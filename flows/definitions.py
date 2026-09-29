"""Dagster: the orchestrator this platform runs on.

Prefect used to be here and is gone. The choice was re-examined on the merits of
*this* workload rather than on popularity, and Dagster won on four specific
properties. They are worth writing down, because "we picked X" ages badly and
"we picked X because of these four things" can be checked, and disagreed with, by
whoever reads this next.

**1. The unit here is a dataset, not a job.** What exists in this platform is an
end-of-day futures table, a tick table, a derived front-month roll map, a derived
back-adjusted continuous series, contract specifications, and model runs.
`@asset` declares exactly that: the thing that exists, the things it is computed
from, and the code that computes it. The task graph falls out of the data graph
instead of being maintained beside it. In Airflow an asset is a URI string with
no properties; in Prefect it is an annotation hung off a task.

**2. The data contracts are asset checks.** This platform already hand-rolls
what `@asset_check(blocking=True)` gives natively: `run_source` validates against
the contract before anything reaches the database, a schema-level failure aborts
the load, and row-level failures quarantine the bad rows and publish the rest.
Under Dagster that becomes a property of the dataset. "eod_bars was deliberately
not updated for 2026-09-25, because the contract version moved" is a state of the
asset with the reason attached, rather than a red task somebody has to open and
interpret. The checks below do not reimplement anything: they call the platform's
`run_source` and translate its `QualityReport` into `AssetCheckResult`s.

**3. Trading-day partitions are one keyword argument.** The partition set below
*is* the MCX trading calendar, because `exclusions` (Dagster 1.12) takes
datetimes and `src/mdp/calendar.py` already has the holiday list. Backfilling
March while skipping holidays is therefore not a script and not a `for` loop with
a calendar lookup in it: the holidays are not partitions, so there is nothing to
skip. `2026-10-02` (Gandhi Jayanti) cannot be materialised because it does not
exist, which is a stronger statement than a job that checks the calendar and
returns early.

**4. Secondary but real: the framework computes the data version.**
`data_version = hash(code_version, input data versions)` is what
`src/mdp/runs.py` and `src/mdp/recipes.py` hand-roll (`code_version()`,
`fingerprint()`). Those stay, because the platform must be able to answer "why
did this number change?" without an orchestrator installed, but Dagster computes
the same thing for free and the assets below hand it the platform's own hashes so
the two agree rather than compete.

Prefect ranked last on the same three tests, which is why keeping it would have
contradicted the reason the choice was re-examined at all: no partitions of any
kind, so "backfill a range" and "replay one day" are `for` loops you write and
maintain yourself; no checks, so a blocked load is a raised exception and
"blocked" versus "broken" is a string in a log line; and its Assets UI is Cloud
only, so the one feature that would have narrowed the gap is not available to a
self-hosted deployment.

Dagster is the only runner here. An Airflow DAG over these same functions used to
sit beside this file as a portability proof, and it was a weak one: the two were
never equivalent - no asset checks, no holiday exclusions - so inviting the
comparison invited that discovery. The claim it was making is now made where it can
be checked and cannot diverge. Nothing in `src/mdp` imports Dagster, Airflow or
anything else orchestration-shaped; `tests/test_audit_fixes.py` parses every module
under `src/mdp` to assert it, and asserts that no orchestrator is importable in the
default test environment either. Every entry point below is also driven by
`src/mdp/cli.py`, which a test likewise asserts, so this file is a shell over
functions anyone can call by hand.

What is in here:

    eod_bars            partitioned by trading day, loaded by `run_source`
    trades              unpartitioned and cursor-driven, by `fetch_incremental`
    contract_reference  the roll map, by `build_reference`
    continuous_series   the back-adjusted splice - a view, not a table
    model_runs          the recipe's scenarios, by `run_recipe`

    eod_file_published  what should be here and is not, by `check_arrivals`
    vendor_not_degrading  by `review_vendor_scores`
    await_eod_publication  the arrival window, by `await_arrival` - an op rather
                        than an asset, for a reason written down at its definition

Install and run:

    make dagster-install                         # into .venv-dagster, not .venv
    make dagster                                 # UI on :3000, with the daemon
    make dagster-materialise                     # one partition, no UI
    make dagster-backfill RANGE=2026-03-01..2026-03-31
"""
# NOTE: no `from __future__ import annotations` here, unlike every other module
# in this repository. Dagster inspects the *runtime* type of an asset function's
# `context` parameter to decide what to pass it, and PEP 563 turns that
# annotation into the string "AssetExecutionContext", which it then rejects with
# "Cannot annotate `context` parameter with type AssetExecutionContext". The
# alternatives were to leave `context` unannotated, which loses the one
# annotation a reader most wants, or to drop the future import in this one file.
# Nothing here needs it: the repository targets 3.12, where `str | None` and
# `list[dict]` are native. Flagged rather than left as a silent inconsistency.
import os
import sys
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from dagster import (  # noqa: E402
    AssetCheckResult,
    AssetCheckSeverity,
    AssetCheckSpec,
    AssetExecutionContext,
    AssetKey,
    AssetMaterialization,
    AssetSelection,
    ConfigurableResource,
    DataVersion,
    Definitions,
    MaterializeResult,
    OpExecutionContext,
    RunRequest,
    ScheduleDefinition,
    ScheduleEvaluationContext,
    SkipReason,
    TimeWindowPartitionsDefinition,
    asset,
    asset_check,
    define_asset_job,
    job,
    op,
    schedule,
)

from mdp.arrival import await_arrival  # noqa: E402
from mdp.calendar import load_calendar  # noqa: E402
from mdp.config import SourceConfig  # noqa: E402
from mdp.fetch import fetch_incremental  # noqa: E402
from mdp.monitor import check_arrivals, review_vendor_scores  # noqa: E402
from mdp.pipeline import build_reference, run_source  # noqa: E402
from mdp.recipes import Recipe, comparison_table, fingerprint, run_recipe  # noqa: E402
from mdp.runs import code_version  # noqa: E402
from mdp.serve import MarketData  # noqa: E402
from mdp.storage import get_store  # noqa: E402

SYNTHETIC = os.getenv("MDP_SYNTHETIC", "1") == "1"
STORE_KIND = os.getenv("MDP_STORE", "duckdb")

EOD = SourceConfig.by_name("mcx_bhavcopy")
TICKS = SourceConfig.by_name("binance_trades")
MCX = load_calendar((EOD.acquire.get("monitor") or {}).get("calendar", "mcx"))
RECIPE = "crudeoil_daily"

# The git sha of the code that computes these assets, from the platform's own
# `code_version()` so the Dagster data version and the `model_runs` registry row
# are derived from the same string rather than from two ideas of what "the code"
# is. This is point 4 of the docstring: Dagster hashes this together with the
# data versions of an asset's inputs, and the platform no longer has to.
CODE_VERSION = code_version()

# DuckDB allows exactly one writer. See MdpStore below for why this is a pool.
WRITER_POOL = "mdp_store_writer"


# ---------------------------------------------------------------------------
# Partitions: the partition set IS the trading calendar
# ---------------------------------------------------------------------------
# Point 3 of the docstring, and the shortest piece of code in this file.
#
# `cron_schedule` gives the shape of a trading week and `exclusions` removes the
# exchange holidays, both read out of `config/calendars/mcx.yml` via
# `load_calendar`, so the calendar has one definition and the orchestrator uses
# it rather than restating it. A holiday is not a partition that gets skipped: it
# is not a partition. `dagster asset backfill --partition-range 2026-03-01...`
# over March therefore materialises 21 days, not 31 with 10 early returns, and
# nobody has to remember that Holi moves.
#
# `end_offset` is left at its default 0, which means the newest partition is the
# most recently *closed* window. That is the correct behaviour here rather than a
# limitation: the window for trading day D closes at 00:00 IST on D+1, and the
# MCX publication deadline for D is 23:55 IST on D — so D becomes materialisable
# five minutes after its file was due, and never before.
_CRON_DOW = ",".join(str((weekday + 1) % 7) for weekday in sorted(MCX.trading_weekdays))

trading_days = TimeWindowPartitionsDefinition(
    start="2026-01-01",
    fmt="%Y-%m-%d",
    cron_schedule=f"0 0 * * {_CRON_DOW}",
    timezone=MCX.timezone,
    # `exclusions` takes datetimes, and the calendar already holds date -> name.
    # Naive datetimes are interpreted in `timezone`, which is what we want.
    exclusions=[
        datetime(day.year, day.month, day.day) for day in sorted(MCX.holidays)
    ],
)


# ---------------------------------------------------------------------------
# The single-writer constraint
# ---------------------------------------------------------------------------
class MdpStore(ConfigurableResource):
    """How an asset reaches the database, and the one place that decides.

    A resource rather than a `get_store()` call in each asset, for the reason the
    Prefect flow discovered the hard way and wrote a whole task about: two
    computations creating the schema at the same time collide. Here the schema is
    ensured on the way to the store, and the pool below guarantees there is only
    ever one of them in flight on DuckDB.
    """

    kind: str = STORE_KIND

    def connect(self):
        store = get_store(self.kind)
        store.ensure_schema()
        return store


# Every asset and every check that opens the store declares `pool=WRITER_POOL`.
# The limit is instance configuration rather than code, which is the point: it is
# a property of the deployment, not of the pipeline. `flows/dagster.yaml` sets it
# to 1 and `make dagster` puts that file where Dagster reads it.
#
# **This is the part that ports.** Every orchestrator worth using has a name for
# "these steps contend for one resource" - Airflow's is a pool with one slot - and in
# each case it is deployment configuration rather than something the pipeline code
# declares, which is why moving the pipeline does not mean re-deriving the
# constraint.
#
# The pool holds across runs rather than only inside one, which is what the Prefect
# flow could not express: it ran sources sequentially *inside* the flow function,
# branching on the store's class name, so a second flow run - or the CLI, or
# somebody's notebook - was free to open the same file and collide. What no
# orchestrator can express is a limit that also binds the CLI, which is why the
# constraint is documented in the schema as well. On QuestDB the limit is raised and
# the writers run concurrently, which is the same decision the Prefect flow was
# making at runtime.
#
# Note "opens the store", not "writes". `continuous_series` and both monitor
# checks only read, and they take the pool too, because DuckDB's file lock is
# exclusive: a reader collides with a writer just as two writers do. That is not a
# guess - the two monitor checks were written without the pool, the multiprocess
# executor ran them at the same time, and the second one died on
# `Could not set lock on file ... Conflicting lock is held in ... (PID 5170)`.
# Which is the argument for expressing the constraint once, in the orchestrator,
# where it can be enforced, instead of once per call site where it can be
# forgotten.


# ---------------------------------------------------------------------------
# Contracts as asset checks
# ---------------------------------------------------------------------------
def declared_checks(cfg: SourceConfig) -> list[str]:
    """The checks this source's config says it runs, in report order.

    Read out of the source YAML rather than listed here, because "a new dataset
    is a YAML file plus a JSON contract" is the platform's central claim and a
    hard-coded check list in the orchestrator would quietly falsify it.

    `schema_version` is prepended because `run_source` prepends it, and
    `expect_rows_min` is appended because `run_checks` appends it whenever the
    config sets a minimum. `reconcile_totals` is a retired name that the platform
    honours as an alias for two real checks, so it is expanded the same way here.
    """
    names = ["schema_version", *cfg.quality.get("checks", ["contract"])]
    if "reconcile_totals" in names:
        names += ["volume_accounted", "expected_coverage"]
    if cfg.quality.get("expect_rows_min") is not None:
        names.append("expect_rows_min")
    return list(dict.fromkeys(names))


def check_specs_for(cfg: SourceConfig, asset_name: str) -> list[AssetCheckSpec]:
    """One spec per check the source declares, all of them blocking.

    `blocking=True` on every spec looks aggressive and is the opposite: Dagster
    blocks on a failed check only at ERROR severity, and WARN-severity failures
    are recorded and let the run continue. So marking everything blocking and
    deriving the severity from the platform's own `pass`/`warn`/`fail` leaves the
    decision where it already lives - in `src/mdp/quality.py` - instead of
    duplicating "which checks are fatal" into a second list that can drift.
    That mapping is exactly the platform's published contract behaviour: a schema
    failure aborts the load, row-level failures quarantine and publish the rest.
    """
    return [
        AssetCheckSpec(
            name=name,
            asset=asset_name,
            blocking=True,
            description=f"src/mdp/quality.py::{name}, as run by run_source",
        )
        for name in declared_checks(cfg)
    ]


_SEVERITY = {"warn": AssetCheckSeverity.WARN, "fail": AssetCheckSeverity.ERROR}


def translate_report(cfg: SourceConfig, report, *, rows_in: int) -> list[AssetCheckResult]:
    """The platform's QualityReport, as Dagster asset checks.

    A translation and nothing more. The checks themselves are not reimplemented
    here and must not be: `run_source` has already validated against the
    contract, quarantined what failed, scored the vendor and written the
    `quality_events` rows. This turns the outcome it returns into the vocabulary
    the orchestrator understands.

    Two wrinkles, both of them Dagster's static-declaration model meeting a
    data-driven check list:

      - A declared check does not always run. `gap_scan` needs a `ts` column and
        at least one row; `freshness` is skipped outright in backfill mode.
        Dagster requires a result for every spec it was told about, or the step
        fails with a missing-output error, so a check that did not run reports as
        passed with `evaluated: false` on it. Marking it failed would page
        somebody about a check that had nothing to look at.
      - A check can run that was not declared. `completeness_vs_expected` only
        appears when a caller passes `expected_rows`. Results with no spec are
        dropped here (Dagster rejects an undeclared check result) and surface as
        materialisation metadata instead, so nothing is silently lost.
    """
    by_name = {result.name: result for result in report.results}
    out: list[AssetCheckResult] = []
    for name in declared_checks(cfg):
        result = by_name.get(name)
        if result is None:
            out.append(AssetCheckResult(
                check_name=name,
                passed=True,
                metadata={"evaluated": False,
                          "detail": "not applicable to this load"},
            ))
            continue
        out.append(AssetCheckResult(
            check_name=name,
            passed=result.status == "pass",
            severity=_SEVERITY.get(result.status, AssetCheckSeverity.WARN),
            description=result.detail,
            metadata={"evaluated": True, "status": result.status,
                      "rows_in": result.rows_in, "rows_out": result.rows_out,
                      "source_rows": rows_in},
        ))
    return out


def undeclared(cfg: SourceConfig, report) -> dict[str, str]:
    """Checks that ran without a spec, so they are visible somewhere."""
    declared = set(declared_checks(cfg))
    return {
        f"unspecced/{r.name}": f"{r.status}: {r.detail}"
        for r in report.results
        if r.name not in declared
    }


def load_metadata(summary: dict[str, Any]) -> dict[str, Any]:
    """The numbers worth seeing on a materialisation without opening the store."""
    return {
        "run_id": summary["run_id"],
        "rows_acquired": summary["rows_acquired"],
        "rows_clean": summary["rows_clean"],
        "rows_quarantined": summary["rows_quarantined"],
        "rows_loaded": summary["rows_loaded"],
        "published": summary["published"],
        "blocked_by": ", ".join(summary["blocked_by"]) or "-",
        "vendor_score": summary["vendor_score"],
        "vendor_status": summary["vendor_status"],
        "quarantine_path": summary["quarantine_path"] or "-",
        "seconds": summary["seconds"],
    }


# ---------------------------------------------------------------------------
# Sources
# ---------------------------------------------------------------------------
@asset(
    name="eod_bars",
    partitions_def=trading_days,
    pool=WRITER_POOL,
    code_version=CODE_VERSION,
    group_name="market_data",
    kinds={STORE_KIND},
    # No output when the day was not published. This is point 2 of the docstring
    # made literal: a blocked load is not a failed task, it is an asset that has
    # no materialisation for that partition, with the check that explains why
    # sitting next to it. Dagster then skips the downstream assets rather than
    # failing them, which is the right shape - rebuilding the roll map from data
    # that did not land is work with a wrong answer at the end of it.
    output_required=False,
    check_specs=check_specs_for(EOD, "eod_bars"),
    description=(
        "MCX end-of-day futures bars, one partition per trading day. "
        "Materialised by mdp.pipeline.run_source; the checks are its QualityReport."
    ),
)
def eod_bars(context: AssetExecutionContext, mdp_store: MdpStore):
    """One trading day of the end-of-day file, through the contract gate.

    The partition key is the trading day, and `days=1` keeps it that way. That
    matters more than it looks: the synthetic generator's default is 750 business
    days, and the Prefect flow's worst bug was a daily run that regenerated and
    reloaded the whole history as a side effect of loading one day twice. A
    partition is a day, so the load asks for a day.

    `now` is the publication deadline for this partition's own day plus its grace
    window, not the wall clock. `run_source` runs the contract's future-event rule
    and the calendar-aware freshness check against whatever clock it is handed, so
    a replay of March evaluated against this morning would fail freshness on every
    partition and teach everybody to pass `--force`. Handing it the deadline of
    the day being materialised makes the question "did this day's file arrive by
    the time it was due", which is answerable for any partition, in any order, at
    any time - which is what makes a backfill reproducible.

    **One load path, and only one.** The platform has two ways into this table:
    `run_source` acquires a named day directly, and `await_arrival` polls from the
    publication deadline and loads the file the moment it lands. The Prefect flow
    ran both for the same source on the same day, which in synthetic mode
    regenerated the entire history as a side effect; its own comments describe
    finding that out. This asset is the first path only. The second one cannot be
    a partitioned asset, for a reason that is a property of the platform rather
    than of Dagster, and `eod_arrival_watch` below is where it lives and where the
    reason is written down.
    """
    day = date.fromisoformat(context.partition_key)
    as_of = MCX.delivery_deadline(day) + timedelta(minutes=MCX.grace_minutes)
    store = mdp_store.connect()

    summary = run_source(
        EOD, store, synthetic=SYNTHETIC, now=as_of, trade_date=day, days=1,
    )
    report = summary["report"]
    context.log.info("%s %s: %s", EOD.name, day,
                     summary["blocked_by"] or f"published {summary['rows_loaded']} rows")

    if summary["published"]:
        yield MaterializeResult(
            metadata={"trading_day": str(day), "as_of": as_of.isoformat(),
                      **load_metadata(summary), **undeclared(EOD, report)},
            # The platform's own run id is the honest data version for a load: two
            # materialisations of the same partition are different loads of the same
            # day, and a consumer that cached the first one needs to know. Dagster
            # hashes it with CODE_VERSION for everything downstream - point 4.
            data_version=DataVersion(summary["run_id"]),
        )
    else:
        context.log.error(
            "%s %s NOT published, blocked by %s. Quarantine: %s",
            EOD.name, day, summary["blocked_by"], summary["quarantine_path"],
        )

    yield from translate_report(EOD, report, rows_in=summary["rows_acquired"])


@asset(
    name="trades",
    pool=WRITER_POOL,
    code_version=CODE_VERSION,
    group_name="market_data",
    kinds={STORE_KIND},
    output_required=False,
    check_specs=check_specs_for(TICKS, "trades"),
    description=(
        "Binance aggregated trades, cursor-driven. Unpartitioned on purpose - "
        "see the docstring; materialised by mdp.fetch.fetch_incremental."
    ),
)
def trades(context: AssetExecutionContext, mdp_store: MdpStore):
    """The continuous tick source. Unpartitioned, and that is the argument.

    The obvious move is to partition this hourly and be symmetrical with
    `eod_bars`. It is the wrong move here, for a reason that is specific to this
    platform rather than to tick data in general: **the high-water mark already
    exists and lives in the database.** `mdp.cursor` stores it, `run_source`
    advances it only to what actually landed, and it is bounded by the window the
    run asked for so one clock-skewed print cannot carry it into the future. A
    partition key would be a second, competing statement of where we stopped, and
    the two would disagree the first time a load was blocked - Dagster would
    consider the partition materialised while the cursor, correctly, had not
    moved, so the gap would be invisible in exactly the place built to make it
    visible. One high-water mark, one owner.

    The other half of it: partitions of a continuous feed are an arbitrary grid
    over something that has no natural one. `binance_trades.yml` declares
    `schedule: {kind: interval, every_seconds: 60}` - the source itself says it is
    polled, not delivered - and the drop-zone window is a rolling
    `first_run_minutes`/`overlap_seconds` pair, not an hour boundary. An hourly
    partition would make "replay 14:00" mean something the source does not mean,
    and the overlap the grain deduplicates would straddle two partitions.

    So: an unpartitioned asset ticked by `ticks_job`'s every-minute schedule,
    which is the `every_seconds: 60` from the config and nothing more. Replaying a
    specific window is `mdp.pipeline.run_source(start=..., end=...)`, which is
    what the CLI already exposes and what `fetch_incremental` calls underneath.
    The eod file is the thing with a natural grain, and it is the thing that is
    partitioned.
    """
    store = mdp_store.connect()
    summary = fetch_incremental(TICKS, store, synthetic=SYNTHETIC)
    report = summary["report"]
    start, end = summary["window"]
    context.log.info(
        "%s: window %s -> %s, %s", TICKS.name, start, end,
        summary["blocked_by"] or f"{summary['rows_loaded']} rows",
    )

    if summary["published"]:
        yield MaterializeResult(
            metadata={
                "window_start": str(start), "window_end": str(end),
                "cursor_before": str(summary["cursor_before"]),
                "cursor_at": str(summary["cursor_at"]),
                **load_metadata(summary), **undeclared(TICKS, report),
            },
            data_version=DataVersion(summary["run_id"]),
        )
    else:
        context.log.error(
            "%s NOT published, blocked by %s", TICKS.name, summary["blocked_by"],
        )

    yield from translate_report(TICKS, report, rows_in=summary["rows_acquired"])


# ---------------------------------------------------------------------------
# Derived datasets
# ---------------------------------------------------------------------------
@asset(
    name="contract_reference",
    deps=[eod_bars],
    pool=WRITER_POOL,
    code_version=CODE_VERSION,
    group_name="reference",
    kinds={STORE_KIND},
    description=(
        "Point-in-time front-month roll map and contract specs, versioned rather "
        "than overwritten. Materialised by mdp.pipeline.build_reference."
    ),
)
def contract_reference(context: AssetExecutionContext, mdp_store: MdpStore):
    """The front-month map. Unpartitioned, downstream of every eod partition.

    `build_reference` reads the whole of `eod_bars` and closes rows rather than
    replacing them, because a settlement corrected last week can change which
    contract the volume rule picks for a day already sitting in a published
    backtest. It is a function of the entire table, not of one day, so it is one
    unpartitioned asset with an all-partitions dependency on `eod_bars` - which is
    Dagster's default for this shape and is also the truth.

    A restatement is surfaced as materialisation metadata rather than logged.
    "Nothing changed" and "forty days were restated" are very different mornings.
    """
    store = mdp_store.connect()
    counts = build_reference(store)
    context.log.info(
        "front-month map: %d opened, %d restated, %d unchanged",
        counts["opened"], counts["closed"], counts["unchanged"],
    )
    if counts["closed"]:
        context.log.warning(
            "roll map restated for %d symbol-days: a correction changed which "
            "contract was front on a day already served, and runs that used the "
            "previous map will not reproduce", counts["closed"],
        )
    return MaterializeResult(metadata={
        "opened": counts["opened"],
        "restated": counts["closed"],
        "unchanged": counts["unchanged"],
        "restatement": "yes - previously served days changed" if counts["closed"] else "no",
    })


@asset(
    name="continuous_series",
    deps=[contract_reference, eod_bars],
    # Reads rather than writes, and still takes the pool: DuckDB's file lock is
    # exclusive, so a reader collides with a writer exactly as two writers do.
    # Found by running it - see the note on WRITER_POOL below.
    pool=WRITER_POOL,
    code_version=CODE_VERSION,
    group_name="reference",
    output_required=False,
    description=(
        "Back-adjusted continuous price series per symbol, spliced on the roll "
        "map. Served on demand by mdp.serve.MarketData.get_continuous."
    ),
)
def continuous_series(context: AssetExecutionContext, mdp_store: MdpStore):
    """The back-adjusted continuous series.

    The one asset here that is a view rather than a table, and it is worth being
    honest about the mismatch: Dagster's mental model of an asset is a thing that
    is stored, and this one is computed on read by `get_continuous` from
    `eod_bars` plus `contract_reference`. Nothing is written when this
    materialises.

    It is still the right thing to declare, because it is what consumers actually
    ask for and what `model_runs` is a function of - leaving it out would mean
    `model_runs` depended on the roll map and the bars and the splicing rule was
    nowhere. What a materialisation asserts is therefore "the view is buildable
    and here is its fingerprint", not "bytes were written". No IO manager is
    involved: `MaterializeResult` carries the metadata and no value.

    The fingerprint is the platform's own `recipes.fingerprint`, handed to Dagster
    as the data version, so `model_runs` invalidates when the series content moves
    and not merely when this asset was last poked.
    """
    store = mdp_store.connect()
    recipe = Recipe.by_name(RECIPE)
    symbol = recipe.input.get("symbol", "CRUDEOIL")
    md = MarketData(store=store, consumer="research")
    prices = md.get_continuous(
        symbol,
        method=recipe.input.get("roll_method", "volume"),
        adjust=recipe.input.get("adjust", "ratio"),
        exchange=recipe.input.get("exchange", "MCX"),
    )
    if prices.empty:
        # Not an error. A fresh database, or a day whose load was blocked, has
        # nothing to splice; skipping is what tells `model_runs` not to bother.
        context.log.warning("no end-of-day data for %s yet, nothing to splice", symbol)
        return

    print_ = fingerprint(prices, recipe)
    context.log.info("%s: %d rows, %s -> %s, series %s", symbol, print_["rows"],
                     print_["first_date"], print_["last_date"], print_["series_sha"])
    yield MaterializeResult(
        metadata={"symbol": symbol, "adjust": print_["adjust"], **print_},
        data_version=DataVersion(print_["series_sha"]),
    )


@asset(
    name="model_runs",
    deps=[continuous_series],
    pool=WRITER_POOL,
    code_version=CODE_VERSION,
    group_name="research",
    kinds={STORE_KIND},
    output_required=False,
    description=(
        f"Walk-forward runs of the {RECIPE} recipe, one registry row and one "
        "artifact directory per scenario. Materialised by mdp.recipes.run_recipe."
    ),
)
def model_runs(context: AssetExecutionContext, mdp_store: MdpStore):
    """Every scenario in the recipe, against the series as it now stands.

    Downstream of `continuous_series` rather than of `eod_bars`, which is the
    dependency that is actually true: a run is a function of the spliced series,
    and the splice is a function of the bars and the roll map. In a task-graph
    runner this is a task that happens to run after the reference refresh, and the
    reason it has to is a comment rather than an edge - which is the single clearest
    thing the asset model buys on this pipeline.

    A recipe run on insufficient history is not a failure. A daily cycle loads a
    day; history comes from a backfill, and on a fresh database there is not
    enough of it for a walk-forward. That is a skip with an explanation, not a
    red run at 6am.
    """
    store = mdp_store.connect()
    try:
        results = run_recipe(Recipe.by_name(RECIPE), store)
    except RuntimeError as exc:
        context.log.warning("%s: %s", RECIPE, exc)
        return
    table = comparison_table(results)
    # `comparison_table` selects whichever of its columns are present, and a run
    # with too little history to produce a fold has no metrics at all - so the
    # absence of the column, not a zero in it, is how "no scenario scored" arrives.
    scored = (
        table.dropna(subset=["skill_vs_baseline"])
        if "skill_vs_baseline" in table.columns
        else table.iloc[0:0]
    )
    if scored.empty:
        context.log.warning(
            "not enough history for a walk-forward on %s. A daily cycle loads a "
            "day; history comes from a backfill (`dagster asset backfill`, or "
            "`mdp backfill --start ... --end ...`).", RECIPE,
        )
        return

    best = scored.iloc[0]
    data = results.attrs.get("data", {})
    context.log.info("%s: best scenario %s, skill vs baseline %.2f%%",
                     RECIPE, best["scenario"], float(best["skill_vs_baseline"]) * 100)
    yield MaterializeResult(
        metadata={
            "recipe": RECIPE,
            "scenarios": int(len(table)),
            "best_scenario": str(best["scenario"]),
            "best_skill_vs_baseline": float(best["skill_vs_baseline"]),
            "run_ids": ", ".join(str(r) for r in table["run_id"]),
            "input_rows": data.get("rows", 0),
            "input_series_sha": data.get("series_sha", "-"),
        },
        # The recipe's own input fingerprint, so a run that produced a different
        # number because the data moved is distinguishable from one that produced
        # a different number because the code moved.
        data_version=DataVersion(f"{data.get('series_sha', '-')}:{RECIPE}"),
    )


# ---------------------------------------------------------------------------
# Checks that are not about one load
# ---------------------------------------------------------------------------
@asset_check(
    asset=eod_bars,
    name="eod_file_published",
    blocking=False,
    pool=WRITER_POOL,
    description=(
        "Which recent trading days are past their publication deadline with "
        "nothing in the table. mdp.monitor.check_arrivals."
    ),
)
def eod_file_published(mdp_store: MdpStore) -> AssetCheckResult:
    """The opposite question to every other check here, and the only one that can
    fire when nothing ran.

    Unpartitioned on purpose, though `eod_bars` is partitioned: a gap is a
    statement about the recent past as a whole, and asking it per partition would
    be asking each day whether it exists, which is not the question. It is also
    the check that catches the case `run_source` structurally cannot - a day whose
    file never arrived produces no load, so no report, so no per-partition check.

    Not blocking. A missing Tuesday is a vendor conversation, not a reason to stop
    serving Monday.
    """
    store = mdp_store.connect()
    gaps = check_arrivals(EOD, store)
    return AssetCheckResult(
        passed=not gaps,
        severity=AssetCheckSeverity.WARN,
        description="; ".join(str(g) for g in gaps) or "every expected delivery is present",
        metadata={"gaps": len(gaps),
                  "days": ", ".join(str(g.trading_day) for g in gaps) or "-"},
    )


@asset_check(
    asset=eod_bars,
    name="vendor_not_degrading",
    blocking=False,
    pool=WRITER_POOL,
    description="Is this source getting quietly worse? mdp.monitor.review_vendor_scores.",
)
def vendor_not_degrading(mdp_store: MdpStore) -> AssetCheckResult:
    """A trend, not an incident, which is why it is separate from the checks that
    can block a load and why it warns rather than fails.

    `review_vendor_scores` returns the *degraded* sources - the latest score per
    source, filtered to those below the amber threshold - not every score. An
    empty frame is therefore the good outcome, which is the opposite of what the
    name suggests and worth stating here, because reading it as "all the scores"
    produced a check that reported "no vendor scores recorded yet" against a table
    with twelve rows in it.
    """
    store = mdp_store.connect()
    degraded = review_vendor_scores(store)
    if degraded.empty:
        return AssetCheckResult(
            passed=True, description="no source is below the amber score threshold",
        )
    worst = degraded.sort_values("score").iloc[0]
    return AssetCheckResult(
        passed=False,
        severity=AssetCheckSeverity.WARN,
        description="; ".join(
            f"{row['source']} {float(row['score']):.3f} ({row['status']})"
            for _, row in degraded.iterrows()
        ),
        metadata={"degraded_sources": int(len(degraded)),
                  "worst_source": str(worst["source"]),
                  "worst_score": float(worst["score"]),
                  "as_of": str(worst["as_of"])},
    )


# ---------------------------------------------------------------------------
# Jobs and schedules
# ---------------------------------------------------------------------------
# One job over the whole daily chain, partitioned by trading day. The
# unpartitioned assets in it run once per run, which is what they are.
daily_job = define_asset_job(
    name="mdp_daily",
    # Subtracting the two standalone monitor checks keeps them out of this job.
    # They are unpartitioned statements about the recent past; running them inside
    # a per-partition job would ask them the same question once per day of a
    # backfill and answer it against today's table every time. The per-load checks
    # declared by `check_specs` stay in, because they are computed by the asset's
    # own step and cannot be separated from it - `without_checks()` is the wrong
    # tool here and Dagster says so, loudly, at definition time.
    selection=AssetSelection.assets(
        eod_bars, contract_reference, continuous_series, model_runs,
    ) - AssetSelection.checks(eod_file_published, vendor_not_degrading),
    # No `partitions_def=` here: Dagster infers it from the selected assets, and
    # passing it is deprecated in 1.13.
    description=(
        "Load one trading day of the end-of-day file, then everything derived "
        "from it. A backfill of a range is the same job over more partitions."
    ),
)

# The schedule fires on the cron the partition set was built from, and targets the
# partition whose window has just closed - which for a trading-day partition and
# a 23:55 publication deadline is the trading day whose file was due five minutes
# ago.
#
# Written out rather than taken from `build_schedule_from_partitioned_job`, and
# the reason is a bug that was found by running it rather than by reading it.
# That helper derives its cron from the partitions definition's `cron_schedule`
# and **drops the `exclusions`**: the schedule it built here was
# `0 0 * * 1,2,3,4,5`, which fires on the morning after a holiday as well as on
# the morning of one. Concretely, with Gandhi Jayanti on Friday 2026-10-02: the
# tick at 00:00 on Friday 2026-10-02 correctly targets 2026-10-01, and the tick at
# 00:00 on Monday 2026-10-05 targets 2026-10-01 *again*, because 10-02 is not a
# partition and 10-05 has not closed yet. That is a second load of a day already
# loaded - exactly the class of duplicate the Prefect flow was carrying.
#
# So the tick asks the instance whether the partition it is about to request has
# already been materialised, and skips if it has. Two consequences worth knowing:
# a Friday partition is picked up on Monday rather than on Saturday, because the
# cron does not run at the weekend and the exchange has nothing to publish then;
# and anything older than the last closed partition is deliberately not chased
# here. Catching up a week is a backfill, and a backfill over a partition set that
# already excludes the holidays is one command with no calendar logic in it, which
# is the whole argument for partitioning by trading day:
#
#     dagster asset backfill --partition-range 2026-03-01...2026-03-31 \
#       -f flows/definitions.py --assets eod_bars
@schedule(
    job=daily_job,
    cron_schedule=f"0 0 * * {_CRON_DOW}",
    execution_timezone=MCX.timezone,
    name="mdp_daily_after_publication",
    description="Load the trading day whose publication deadline has just passed.",
)
def daily_schedule(context: ScheduleEvaluationContext):
    closed = trading_days.get_partition_keys(
        current_time=context.scheduled_execution_time
    )
    if not closed:
        return SkipReason("no trading day has closed yet")
    key = closed[-1]
    done = context.instance.get_materialized_partitions(AssetKey("eod_bars"))
    if key in done:
        return SkipReason(
            f"{key} is already materialised; the most likely reason this tick "
            f"exists at all is an exchange holiday between it and the previous "
            f"trading day. Older gaps are a backfill, not a catch-up run."
        )
    context.log.info("requesting %s", key)
    return RunRequest(partition_key=key, run_key=key)


# ---------------------------------------------------------------------------
# The arrival-window path: an op, not an asset, and the reason why
# ---------------------------------------------------------------------------
@op(pool=WRITER_POOL, description="Wait inside the publication window and load on arrival.")
def await_eod_publication(context: OpExecutionContext, mdp_store: MdpStore) -> dict:
    """React to the end-of-day file landing, instead of waiting for a clock.

    `await_arrival` starts polling when the file is due and keeps looking until it
    appears or the window closes, which is what separates **late** from **missing**
    - two outcomes that look identical to a single scheduled fetch and need
    completely different responses. It is the platform's answer to "was this
    published on time", and it records the answer in `quality_events` whichever way
    it goes.

    **Why this is an op and not a partition of `eod_bars`.** It was written as one
    first, selected by run config, and taken out again after running it. Two things
    make `await_arrival` structurally unable to be a partitioned asset, and both
    are properties of the platform:

      1. **It is directory-scoped, not day-scoped.** `await_arrival` loads by
         calling `watch([cfg], store, once=True)`, and `watch` processes *every*
         unsettled file in the drop directory - which is the right behaviour for a
         watcher, and is why the same path works whether we fetched the file, a
         vendor pushed it, or somebody dropped it there during an incident. It
         means a materialisation of the partition for one day can legitimately load
         three, and a partition that claims to be one day but is not is worse than
         no partition at all. Observed, not theorised: asking for 2026-09-25
         reported "10 rows loaded" with `published=False`, because one pending
         delivery published and another was blocked, and `Arrival.published` is
         `all(deliveries)`.
      2. **Its return value has no report.** `Arrival` carries `status`,
         `rows_loaded` and `published`; the per-check outcomes went to
         `quality_events` inside `watch`. Reading the latest load's rows back out
         works for one delivery and is ambiguous for several - which is the same
         problem as (1) wearing a different hat.

    So the wait stays orchestration and the table stays an asset. This op emits an
    `AssetMaterialization` for the trading day it waited for, which is Dagster's
    own mechanism for "this asset changed outside the asset graph", so the partition
    shows as materialised in the catalog with the publication metadata on it. It
    carries no `AssetCheckResult`s, deliberately: an op cannot emit them, and
    inventing per-check results for a load whose report it never saw would be
    worse than pointing at the `quality_events` rows the platform did write.

    Run this **or** `mdp_daily` for a given day, never both - that is the routing
    the Prefect flow got wrong. In practice: this one between 23:55 and 01:55
    exchange time, when the window is open and lateness is still a live question;
    `mdp_daily` afterwards, for anything already settled.
    """
    store = mdp_store.connect()
    arrival = await_arrival(
        EOD, store, synthetic=SYNTHETIC,
        # Which day: `await_arrival` defaults to the previous trading day from the
        # calendar, which is the right answer for a watcher running at the
        # publication deadline and is not the same question a partition key asks.
        #
        # In synthetic mode the generator produces the file on the first poll, so
        # sleeping through a 15-minute interval proves nothing. In live mode the
        # wait is the entire point. The CLI exposes the same switch, as
        # `mdp await-file --no-sleep`, and for the same reason.
        sleep=not SYNTHETIC,
        max_attempts=3 if SYNTHETIC else None,
    )
    context.log.info(arrival.summary())
    metadata = {
        "trading_day": str(arrival.trading_day),
        "arrival_status": arrival.status,
        "attempts": arrival.attempts,
        "delay_minutes": round(arrival.delay_seconds / 60, 1),
        "rows_loaded": arrival.rows_loaded,
        "published": arrival.published,
        "detail": arrival.detail or "-",
        "checks": (
            "recorded by the platform in quality_events for this load; this path "
            "cannot surface them as asset checks - see the op docstring"
        ),
    }
    if arrival.published:
        context.log_event(AssetMaterialization(
            asset_key=AssetKey("eod_bars"),
            partition=str(arrival.trading_day),
            description="loaded on arrival by the publication watcher",
            metadata=metadata,
        ))
    elif arrival.status == "missing":
        # Not raised. A file that never arrived is a recorded event, and
        # `eod_file_published` is the check that knows what should exist.
        context.log.error(
            "%s %s: window closed with nothing after %d attempts",
            EOD.name, arrival.trading_day, arrival.attempts,
        )
    else:
        context.log.error(
            "%s %s: the file arrived but did not pass its checks; nothing published",
            EOD.name, arrival.trading_day,
        )
    return metadata


@job(
    name="mdp_eod_arrival",
    description="Poll from the publication deadline until the end-of-day file lands.",
)
def arrival_job():
    await_eod_publication()


# 23:55 exchange time on a trading weekday: the publication deadline itself, which
# is when there is first any point in looking. The holidays are not excluded from
# this cron - unlike the partition set - and do not need to be: `await_arrival`
# returns `skipped` with the reason from the calendar on a day the exchange was
# shut, which is cheaper than a second calendar in a cron string.
arrival_schedule = ScheduleDefinition(
    name="mdp_eod_arrival_window",
    job=arrival_job,
    cron_schedule=f"55 23 * * {_CRON_DOW}",
    execution_timezone=MCX.timezone,
)

# The tick feed's own cadence, straight out of binance_trades.yml
# (`schedule: {kind: interval, every_seconds: 60}`). Nothing more: a minute is
# often enough that a missed tick costs a minute of data and rare enough to stay
# far inside the public rate limit.
ticks_job = define_asset_job(
    name="mdp_ticks",
    selection=[trades],
    description="One incremental pass over the tick feed, from the cursor.",
)
ticks_schedule = ScheduleDefinition(
    name="mdp_ticks_every_minute",
    job=ticks_job,
    cron_schedule="* * * * *",
)

# The monitor checks run on their own, because they are the ones that have
# something to say when nothing ran.
monitor_job = define_asset_job(
    name="mdp_monitor",
    selection=AssetSelection.checks(eod_file_published, vendor_not_degrading),
    description="What should be here and is not, and which source is degrading.",
)
monitor_schedule = ScheduleDefinition(
    name="mdp_monitor_hourly",
    job=monitor_job,
    cron_schedule="7 * * * *",
)


defs = Definitions(
    assets=[eod_bars, trades, contract_reference, continuous_series, model_runs],
    asset_checks=[eod_file_published, vendor_not_degrading],
    jobs=[daily_job, arrival_job, ticks_job, monitor_job],
    schedules=[daily_schedule, arrival_schedule, ticks_schedule, monitor_schedule],
    resources={"mdp_store": MdpStore(kind=STORE_KIND)},
)


# Convenience for the non-interactive smoke test in the Makefile, and for anybody
# who wants to see one partition go end to end without a UI. Deliberately using
# the public `dagster.materialize` API against the same definitions the webserver
# loads, so the two cannot drift.
def last_closed_partition() -> str:
    """The newest trading day whose publication window has closed.

    Not `calendar.previous_trading_day()`, which would answer 2026-09-25 on a
    Sunday: that day's partition window runs to Monday 00:00 IST, because an
    excluded day (a weekend or a holiday) is absorbed into the preceding
    partition's window rather than becoming a gap in the timeline. The partition
    set is the authority on what exists, so it is the thing asked.
    """
    keys = trading_days.get_partition_keys(current_time=datetime.now(UTC))
    if not keys:
        raise RuntimeError("no trading day has closed since the partition start date")
    return keys[-1]


def materialise(partition_key: str | None = None, selection: list[str] | None = None) -> bool:
    """Materialise one trading day, or the whole daily chain, in this process."""
    from dagster import materialize

    keys = [AssetKey(name) for name in (selection or
                                        ["eod_bars", "contract_reference",
                                         "continuous_series", "model_runs"])]
    chosen = [defs.get_assets_def(key) for key in keys]
    # `materialize` refuses a partition key when nothing in the selection is
    # partitioned, so a run of only the derived assets does not get one.
    partitioned = any(a.partitions_def is not None for a in chosen)
    result = materialize(
        chosen,
        partition_key=(partition_key or last_closed_partition()) if partitioned else None,
        resources={"mdp_store": MdpStore(kind=STORE_KIND)},
        raise_on_error=False,
    )
    return result.success


def materialise_range(start: str, end: str, selection: list[str] | None = None) -> bool:
    """Backfill a date range: one run per trading day in it, and no calendar code.

    The loop here is over `get_partition_keys_in_range`, which is to say over the
    trading calendar, because that is what the partition set is. `2026-09-10` to
    `2026-09-18` is six runs, not nine: the weekend and Ganesh Chaturthi are not
    partitions, so there is nothing to skip and nothing to remember. That is the
    difference between this and the Prefect backfill it replaces, which was a loop
    over `pd.date_range` with a calendar lookup inside it.

    Why a loop at all: this is what the Dagster daemon does when a backfill is
    launched from the UI or the API, one run per partition so a bad Tuesday is
    retried on its own and shows as one unmaterialised day. The daemon is the
    normal route; this exists because `dagster asset materialize
    --partition-range` refuses a range unless the asset declares
    `BackfillPolicy.single_run()`, and declaring that would collapse the range
    into one run and one set of check results for the whole month, which throws
    away the per-day record that is the reason for partitioning in the first place.
    """
    from dagster import PartitionKeyRange

    keys = trading_days.get_partition_keys_in_range(PartitionKeyRange(start, end))
    if not keys:
        raise RuntimeError(f"no trading days between {start} and {end}")
    print(f"{len(keys)} trading days in {start}..{end}: {', '.join(keys)}")
    return all(materialise(key, selection) for key in keys)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--partition", default=None,
                        help="trading day to materialise (default: the last one)")
    parser.add_argument("--partition-range", default=None, metavar="START..END",
                        help="backfill every trading day in the range")
    parser.add_argument("--assets", default=None,
                        help="comma-separated asset names (default: the daily chain)")
    args = parser.parse_args()
    picked = args.assets.split(",") if args.assets else None
    if args.partition_range:
        first, _, last = args.partition_range.partition("..")
        ok = materialise_range(first, last.lstrip("."), picked)
    else:
        ok = materialise(args.partition, picked)
    raise SystemExit(0 if ok else 1)
