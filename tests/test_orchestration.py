"""Tests for the Dagster code location in flows/definitions.py.

Dagster is the one orchestrator. There used to be three runners here - a Dagster
code location, an Airflow DAG and a cron/flock shell script - and the two that are
gone were a hedge rather than a decision: they were never equivalent to the code
location, and "why do you need all three?" had no good answer. What they were there
to prove is now proved by assertion instead, which is both stronger and cheaper to
keep true. See docs/ARCHITECTURE.md 3.4.

Split deliberately into two halves.

The first half needs nothing installed. It parses `flows/` and `src/mdp/` as text
and asserts the things that are true of the *repository*: that the orchestrated
path and the hand-run CLI path drive the same platform entry points, that no
orchestrator has crept into the runtime or `dev` dependencies, and that the retired
runners are actually gone rather than sitting next door unmaintained. Those hold in
the default test environment, which is the environment CI runs, and they are the
assertions that would otherwise silently stop being checked the moment a runner is
not installed.

The second half needs Dagster and skips without it, because Dagster is an optional
dependency group: a default install that drags in a scheduler has made the
orchestrator load-bearing again, and that coupling is the one thing this platform
claims not to have. It still needs no Dagster *instance*, no daemon and no
webserver - the partition set, the asset graph and the check declarations are all
in-process properties of the definitions, which is one of the better arguments for
the asset model. Run it with:

    uv pip install --python .venv-dagster "dagster>=1.12" -r requirements.txt
    PYTHONPATH=$PWD/src .venv-dagster/bin/python -m pytest tests/test_orchestration.py
"""
from __future__ import annotations

import ast
import importlib
import importlib.util
import sys
import tomllib
from datetime import date
from pathlib import Path

import pytest

from mdp.calendar import load_calendar
from mdp.config import repo_root

FLOWS = repo_root() / "flows"
DAGSTER_DEFS = FLOWS / "definitions.py"
CLI = repo_root() / "src" / "mdp" / "cli.py"

# The runners this repository used to carry and no longer does. Named rather than
# implied, so "we removed it" is a checked statement and not a changelog entry.
RETIRED = (
    FLOWS / "market_data_flow.py",   # the Prefect flow
    FLOWS / "airflow_dag.py",        # the Airflow DAG
    FLOWS / ".airflowignore",        # which existed only to hide the file above
    repo_root() / "scripts" / "run_daily.sh",   # cron and flock
)

# The three the platform is actually driven by. If one of the two ways in stops
# calling one of these it is no longer the same pipeline, whatever its graph
# looks like.
ESSENTIAL = ("run_source", "build_reference", "await_arrival")

# Holidays that must not be partitions, and the trading days either side of them.
# 2026-09-14 is Ganesh Chaturthi and 2026-10-02 is Gandhi Jayanti; both are
# weekdays, which is the whole point - a weekend would be excluded by the cron.
HOLIDAYS = ("2026-09-14", "2026-10-02")
TRADING = ("2026-09-11", "2026-09-15", "2026-10-01", "2026-10-05")


def mdp_imports(path: Path) -> set[str]:
    """Every name imported from `mdp` by a file, without importing it.

    Parsed rather than imported, because importing the code location needs Dagster
    installed and Dagster is not a runtime dependency of the platform. The same
    technique as `tests/test_audit_fixes.py`, on purpose: if the two ever disagree,
    one of them is wrong about the repository.

    Both import spellings count. The code location sits outside the package and
    says `from mdp.pipeline import run_source`; the CLI sits inside it and says
    `from .pipeline import run_source`. A relative import from a module under
    `src/mdp` is an import from `mdp`.
    """
    tree = ast.parse(path.read_text(encoding="utf-8"))
    names: set[str] = set()
    for node in ast.walk(tree):
        if not isinstance(node, ast.ImportFrom):
            continue
        absolute = (node.module or "").startswith("mdp")
        relative = node.level > 0 and path.parent.name == "mdp"
        if absolute or relative:
            names.update(a.name for a in node.names)
    return names


# ---------------------------------------------------------------------------
# No Dagster required
# ---------------------------------------------------------------------------
def test_the_retired_runners_are_gone():
    """Retiring a runner means deleting it, not leaving it next door.

    Worth keeping as one test naming all three, because each was removed for a
    different reason and the shared consequence is the same: a runner nobody
    exercises is a second answer to "how is this scheduled?" that will be believed
    by whoever finds it first, and it rots without telling anyone. The shell script
    proved that on its own - it silently broke when the Prefect flow it invoked was
    deleted, and had to be rewritten before anybody noticed it was dead.

      - The Prefect flow ranked last on this workload: no partitions of any kind,
        no checks, and a Cloud-only Assets UI.
      - The Airflow DAG was kept on the argument that Airflow is what a firm
        probably already runs, which is reasoning from what other people use. It
        was also not equivalent: one DAG and five tasks with no asset checks and no
        holiday exclusions, against a code location declaring five assets, sixteen
        checks, four jobs and four schedules.
      - `scripts/run_daily.sh` existed to show the platform runs with no
        orchestrator. `tests/test_audit_fixes.py` now shows that statically, and a
        static test cannot rot; the CLI shows the rest, since every step the script
        ran is an `mdp` subcommand anybody can type.
    """
    for path in RETIRED:
        assert not path.exists(), (
            f"{path.name} was retired; a runner that exists but is not exercised is "
            f"a claim nobody is checking"
        )
    assert DAGSTER_DEFS.exists(), "flows/definitions.py is the Dagster code location"
    # And `flows/` is now exactly the code location plus its instance config.
    assert {p.name for p in FLOWS.iterdir() if not p.name.startswith("__")} == {
        "definitions.py", "dagster.yaml",
    }


def test_the_code_location_and_the_cli_drive_the_same_entry_points():
    """One orchestrator, one CLI, one set of functions.

    This used to compare the code location against the Airflow DAG. With one
    orchestrator the comparison that still means something is against the hand-run
    path: `flows/definitions.py` and `src/mdp/cli.py` are the two ways this platform
    is driven, and if they diverge, the orchestrated pipeline is not the pipeline
    anybody demonstrates or debugs. The CLI is the better control of the two the DAG
    could have been, because it is exercised by the README, by `make demo` and by
    CI's demo step on every push, so it cannot quietly stop working.

    The same assertion `tests/test_audit_fixes.py` makes. Both are kept on purpose:
    if the two ever disagree, one of them is wrong about the repository.
    """
    dagster_calls = mdp_imports(DAGSTER_DEFS)
    cli_calls = mdp_imports(CLI)
    assert dagster_calls and cli_calls
    shared = dagster_calls & cli_calls
    for essential in ESSENTIAL:
        assert essential in shared, (
            f"{essential} is driven by only one of the code location and the CLI"
        )


def test_the_dagster_file_does_not_import_the_platform_by_side_effect():
    """It reaches the platform through `mdp`, not through the other runner."""
    text = DAGSTER_DEFS.read_text(encoding="utf-8")
    tree = ast.parse(text)
    modules = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            modules.update(a.name.split(".")[0] for a in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            modules.add(node.module.split(".")[0])
    assert "prefect" not in modules
    assert "airflow" not in modules
    assert {"dagster", "mdp"} <= modules


def test_no_orchestrator_is_a_runtime_dependency():
    """The orchestrator lives in the repository; it does not live in the
    dependency tree.

    Dagster being the only orchestrator makes this test more important rather than
    less. With a second runner around, "the pipeline is portable" had a second
    implementation standing in front of it; now the claim rests on exactly two
    things, this and `test_no_pipeline_code_imports_an_orchestrator`. The realistic
    way it dies is somebody promoting the runner they actually use into
    `[project.dependencies]` for convenience. `dev` is excluded for the same reason
    at one remove: `uv sync` installs it by default, so a scheduler there is a
    scheduler everybody has.

    The retired names stay in the loop. A check that only covers today's
    orchestrator would pass a `pyproject.toml` that reinstates yesterday's.
    """
    pyproject = tomllib.loads(
        (repo_root() / "pyproject.toml").read_text(encoding="utf-8")
    )
    groups = pyproject.get("dependency-groups", {})
    runtime = " ".join(pyproject["project"]["dependencies"]).lower()
    dev = " ".join(d.lower() for d in groups.get("dev", []))

    for runner in ("dagster", "airflow", "prefect", "luigi", "temporalio"):
        assert runner not in runtime, f"{runner} is not a runtime dependency"
        assert runner not in dev, (
            f"{runner} belongs in its own optional group, not in dev: a default "
            f"install that drags in a scheduler has made the orchestrator "
            f"load-bearing again"
        )
    # The one that exists has a home, so it can be exercised on purpose.
    assert "dagster" in groups, "the dagster group should exist for running the assets"
    # And the retired ones do not, because a dependency group for a runner with no
    # file is an invitation to write the file again.
    assert "airflow" not in groups, "the Airflow DAG is gone; its group went with it"
    assert "prefect" not in groups, "Prefect was retired; it should have no group"


def test_the_calendar_is_the_only_source_of_holidays():
    """The dates this test asserts about come from the calendar, not from here.

    A test that hard-codes 2026-10-02 as a holiday and a partition set that
    hard-codes it too would agree with each other forever while both being wrong
    about the exchange. So the test checks that the calendar says what it thinks it
    says, and everything below reads the partition set against the calendar.
    """
    calendar = load_calendar("mcx")
    for holiday in HOLIDAYS:
        day = date.fromisoformat(holiday)
        assert day in calendar.holidays, f"{holiday} should be an MCX holiday"
        assert not calendar.is_trading_day(day)
    for trading in TRADING:
        assert calendar.is_trading_day(date.fromisoformat(trading))


# ---------------------------------------------------------------------------
# Dagster required, an instance not
# ---------------------------------------------------------------------------
# A marker plus a lazily-imported fixture rather than a module-level
# `pytest.importorskip`. That call would skip the *whole file*, taking the five
# tests above with it - and those are exactly the ones that must keep running in
# the default environment, where Dagster is deliberately absent. Getting this
# wrong is silent: the file reports "1 skipped" and nothing is checked at all.
needs_dagster = pytest.mark.skipif(
    importlib.util.find_spec("dagster") is None,
    reason="dagster is an optional dependency group; see this module's docstring",
)


@pytest.fixture(scope="module")
def defs_module():
    """The code location, imported in-process. No instance, no daemon, no UI."""
    if str(FLOWS.parent) not in sys.path:
        sys.path.insert(0, str(FLOWS.parent))
    return importlib.import_module("flows.definitions")


@pytest.fixture(scope="module")
def partitions(defs_module):
    return defs_module.trading_days


@needs_dagster
def test_the_partition_set_excludes_exchange_holidays(defs_module, partitions):
    """The headline claim: the partition set IS the trading calendar.

    Not "the job checks the calendar and returns early" - the holiday is not a
    partition, so `dagster asset backfill` over March cannot materialise one and
    nobody has to remember that Holi moves. `2026-10-02` is Gandhi Jayanti and
    `2026-09-14` is Ganesh Chaturthi; both fall on weekdays, so the weekday cron
    alone would have let them through.
    """
    keys = set(partitions.get_partition_keys(
        current_time=_after(max(HOLIDAYS + TRADING))
    ))
    assert keys, "the partition set should not be empty"
    for holiday in HOLIDAYS:
        assert holiday not in keys, (
            f"{holiday} is an exchange holiday and must not be a partition"
        )
    for trading in TRADING:
        assert trading in keys, f"{trading} is a trading day and must be a partition"


@needs_dagster
def test_every_partition_is_a_trading_day(defs_module, partitions):
    """Stronger than checking two holidays: nothing in the set is a closed day.

    This is the assertion that catches a calendar the partitions were built from
    going stale, or a cron whose weekday numbering is off by one - Python counts
    Monday as 0 and cron counts Sunday as 0, and getting that wrong shifts the
    whole trading week by a day while still looking like five days out of seven.
    """
    calendar = load_calendar("mcx")
    keys = partitions.get_partition_keys(current_time=_after("2026-12-31"))
    assert len(keys) > 200, "a year of MCX trading days is roughly 245"
    closed = [k for k in keys if not calendar.is_trading_day(date.fromisoformat(k))]
    assert not closed, f"partitions on non-trading days: {closed[:10]}"

    # And the converse: no trading day in the covered range is missing.
    covered = calendar.trading_days(
        date.fromisoformat(keys[0]), date.fromisoformat(keys[-1])
    )
    missing = [str(d) for d in covered if str(d) not in set(keys)]
    assert not missing, f"trading days that are not partitions: {missing[:10]}"


@needs_dagster
def test_a_range_backfill_skips_the_holidays_without_being_told(defs_module, partitions):
    """`2026-09-10` to `2026-09-18` is six runs, not nine.

    The weekend and Ganesh Chaturthi are absent from the range because they are
    absent from the partition set. This is the operation the Prefect flow could
    only express as a loop over `pd.date_range` with a calendar lookup inside it.
    """
    from dagster import PartitionKeyRange

    keys = partitions.get_partition_keys_in_range(
        PartitionKeyRange("2026-09-10", "2026-09-18")
    )
    assert keys == ["2026-09-10", "2026-09-11", "2026-09-15",
                    "2026-09-16", "2026-09-17", "2026-09-18"]


@needs_dagster
def test_the_asset_graph_matches_the_real_data_dependencies(defs_module):
    """The dependency edges are the ones the platform actually has.

    `contract_reference` is a function of `eod_bars` because `build_reference`
    reads that table. `continuous_series` is a function of both, because the
    splice needs the bars and the roll map. `model_runs` is a function of the
    spliced series, not of the bars - which is the edge the task-graph runners
    cannot state, and record as a comment about ordering instead.
    """
    graph = {
        spec.key.to_user_string(): {d.asset_key.to_user_string() for d in spec.deps}
        for asset in defs_module.defs.assets
        for spec in asset.specs
    }
    assert graph == {
        "eod_bars": set(),
        "trades": set(),
        "contract_reference": {"eod_bars"},
        "continuous_series": {"contract_reference", "eod_bars"},
        "model_runs": {"continuous_series"},
    }


@needs_dagster
def test_only_the_end_of_day_source_is_partitioned(defs_module):
    """The tick feed is unpartitioned on purpose: the high-water mark is the
    cursor in the database, and a partition key would be a second, competing
    statement of where the load stopped. See the asset's docstring."""
    partitioned = {
        spec.key.to_user_string()
        for asset in defs_module.defs.assets
        for spec in asset.specs
        if spec.partitions_def is not None
    }
    assert partitioned == {"eod_bars"}


@needs_dagster
def test_the_checks_are_the_source_configs_own_check_list(defs_module):
    """The asset checks are declared from the source YAML, not listed in the
    orchestrator.

    "A new dataset is a YAML file plus a JSON contract" is the platform's central
    claim, and a hard-coded check list in the code location would quietly falsify
    it: adding a check to `config/sources/*.yml` would leave Dagster unaware of it.
    """
    from mdp.config import SourceConfig

    for asset, cfg_name in ((defs_module.eod_bars, "mcx_bhavcopy"),
                            (defs_module.trades, "binance_trades")):
        cfg = SourceConfig.by_name(cfg_name)
        declared = set(defs_module.declared_checks(cfg))
        assert {spec.name for spec in asset.check_specs} == declared
        # The platform's own checks, plus the two `run_checks` always adds.
        assert set(cfg.quality["checks"]) <= declared
        assert "schema_version" in declared, "run_source prepends this one"
        assert "expect_rows_min" in declared, "run_checks appends this one"


@needs_dagster
def test_every_check_is_blocking_and_the_severity_carries_the_decision(defs_module):
    """Blocking on every spec is not aggression, it is delegation.

    Dagster blocks only on an ERROR-severity failure, so marking everything
    blocking and deriving the severity from the platform's own pass/warn/fail
    leaves "which checks are fatal" in src/mdp/quality.py, where it already lives,
    instead of copying it into a second list that can drift. A warn is recorded
    and the load publishes; a fail blocks.
    """
    from dagster import AssetCheckSeverity

    for asset in (defs_module.eod_bars, defs_module.trades):
        assert asset.check_specs, "the source assets declare checks"
        assert all(spec.blocking for spec in asset.check_specs)

    assert defs_module._SEVERITY["fail"] is AssetCheckSeverity.ERROR
    assert defs_module._SEVERITY["warn"] is AssetCheckSeverity.WARN


@needs_dagster
def test_a_report_translates_into_one_result_per_declared_check(defs_module):
    """Dagster wants a result for every spec it was told about, and the platform
    runs whichever checks the data allows.

    So the translation has to be total: a check that did not run reports as passed
    with `evaluated: false` rather than being omitted, because an omitted result
    fails the step with a missing-output error, and a failed result would page
    somebody about a check that had nothing to look at.
    """
    from mdp.config import SourceConfig
    from mdp.quality import CheckResult, QualityReport

    cfg = SourceConfig.by_name("mcx_bhavcopy")
    report = QualityReport(source=cfg.name, dataset=cfg.dataset, results=[
        CheckResult("schema_version", "pass", "contract v1.0, unchanged"),
        CheckResult("contract", "warn", "3 rows quarantined"),
        CheckResult("uniqueness", "fail", "duplicate grain"),
        # `freshness`, `volume_accounted` and `expected_coverage` are declared by
        # the config and deliberately absent here, as they are on a load that had
        # no rows to look at.
        CheckResult("completeness_vs_expected", "pass", "not declared anywhere"),
    ])

    results = defs_module.translate_report(cfg, report, rows_in=10)
    by_name = {r.check_name: r for r in results}
    assert set(by_name) == set(defs_module.declared_checks(cfg))

    assert by_name["schema_version"].passed
    assert not by_name["contract"].passed
    assert by_name["contract"].severity.value == "WARN"
    assert not by_name["uniqueness"].passed
    assert by_name["uniqueness"].severity.value == "ERROR"

    absent = by_name["freshness"]
    assert absent.passed, "a check that did not run is not a failure"
    assert absent.metadata["evaluated"].value is False

    # The undeclared result is not silently dropped; it becomes metadata.
    assert "unspecced/completeness_vs_expected" in defs_module.undeclared(cfg, report)


@needs_dagster
def test_every_asset_that_opens_the_store_takes_the_writer_pool(defs_module):
    """DuckDB allows one connection to hold the file, reader or writer.

    Expressed once, as a pool, so the constraint is enforced by the orchestrator
    rather than remembered at each call site. The limit itself lives in
    flows/dagster.yaml because it is a property of the deployment, not of the
    pipeline - which is also why it ports: Airflow's idiom for the same fact is a
    pool with one slot, and that is deployment configuration too.
    """
    pools = {
        asset.op.name: asset.op.pool
        for asset in defs_module.defs.assets
    }
    # Every asset here reads or writes the store, so every one of them takes it.
    assert set(pools.values()) == {defs_module.WRITER_POOL}, pools

    instance_config = (FLOWS / "dagster.yaml").read_text(encoding="utf-8")
    assert "default_limit: 1" in instance_config
    # Run granularity would allow two steps inside one run to open the same file.
    assert "granularity: op" in instance_config


@needs_dagster
def test_the_jobs_and_schedules_resolve(defs_module):
    """The definitions are loadable as a code location, which is what `dagster dev`
    does and what `make dagster-check` asserts in CI."""
    names = {job.name for job in defs_module.defs.jobs}
    assert names == {"mdp_daily", "mdp_eod_arrival", "mdp_ticks", "mdp_monitor"}

    schedules = {s.name: s.cron_schedule for s in defs_module.defs.schedules}
    # Trading weekdays, from the calendar rather than typed in here.
    calendar = load_calendar("mcx")
    weekdays = ",".join(str((w + 1) % 7) for w in sorted(calendar.trading_weekdays))
    assert schedules["mdp_daily_after_publication"] == f"0 0 * * {weekdays}"
    # The arrival watcher starts looking at the publication deadline itself, which
    # the calendar owns - so the minute and hour come from the calendar too.
    assert schedules["mdp_eod_arrival_window"] == (
        f"{calendar.deadline.minute} {calendar.deadline.hour} * * {weekdays}"
    )
    # The tick feed's own declared cadence: binance_trades.yml says 60 seconds.
    assert schedules["mdp_ticks_every_minute"] == "* * * * *"


@needs_dagster
def test_the_arrival_window_path_is_an_op_not_a_partitioned_asset(defs_module):
    """`await_arrival` is orchestration, not a partition, and on purpose.

    It loads by calling `watch`, which processes every unsettled file in the drop
    directory rather than the one day it was asked about - so a partition
    materialised through it can legitimately load three days, and a partition that
    claims to be one day and is not is worse than no partition. It also returns an
    `Arrival` with no per-check report. Both are properties of the platform, and
    both are reasons the wait lives in an op that reports an `AssetMaterialization`
    for the day it waited for. See the op's docstring.

    The assertion that matters: `eod_bars` itself takes no run configuration that
    would route it down a second load path. One asset, one loader.
    """
    assert defs_module.await_eod_publication.pool == defs_module.WRITER_POOL
    repo = defs_module.defs.get_repository_def()
    arrival = repo.get_job("mdp_eod_arrival")
    assert [n.name for n in arrival.nodes] == ["await_eod_publication"]
    # And the arrival job is not an asset job: it produces no asset outputs.
    assert not arrival.asset_layer.executable_asset_keys

    assert "await_publication" not in str(
        defs_module.eod_bars.op.config_schema.config_type
    ), "eod_bars must not carry a switch between load paths"


@needs_dagster
def test_the_daily_job_excludes_the_unpartitioned_monitor_checks(defs_module):
    """A per-partition job must not carry a check about the recent past.

    Running `eod_file_published` inside a backfill would ask it the same question
    once per day of the range and answer it against today's table every time.
    """
    # `defs.jobs` holds unresolved job definitions; the repository resolves them
    # against the asset graph, which is where the selected check keys appear.
    repo = defs_module.defs.get_repository_def()
    job = repo.get_job("mdp_daily")
    check_names = {key.name for key in job.asset_layer.asset_graph.asset_check_keys}
    assert "eod_file_published" not in check_names
    assert "vendor_not_degrading" not in check_names
    # The per-load checks are computed by the asset's own step and stay.
    assert "contract" in check_names


def _after(day: str):
    """A moment safely past the end of a day, in the exchange's own timezone.

    The partition set's newest key is the most recently *closed* window, and a
    window that ends at midnight is not closed at midnight of the same day, so
    every "does the set contain X" test has to ask from beyond X.
    """
    from datetime import datetime, timedelta
    from zoneinfo import ZoneInfo

    calendar = load_calendar("mcx")
    return datetime.fromisoformat(day).replace(
        tzinfo=ZoneInfo(calendar.timezone)
    ) + timedelta(days=5)
