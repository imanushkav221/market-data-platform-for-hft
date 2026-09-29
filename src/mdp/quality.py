"""Quality checks and vendor scoring.

Two different jobs that people often conflate:
  - checks answer "is this load safe to publish?"  (block or warn, now)
  - vendor scores answer "is this source getting worse?"  (a trend, and a number
    to put in front of a supplier)
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

import pandas as pd

from .config import SourceConfig, vendor_quality_policy
from .contracts import ValidationResult


@dataclass
class CheckResult:
    name: str
    status: str  # pass | warn | fail
    detail: str
    rows_in: int = 0
    rows_out: int = 0


@dataclass
class QualityReport:
    source: str
    dataset: str
    results: list[CheckResult] = field(default_factory=list)

    @property
    def blocking(self) -> list[CheckResult]:
        return [r for r in self.results if r.status == "fail"]

    @property
    def publishable(self) -> bool:
        return not self.blocking

    def to_frame(self, run_id: str) -> pd.DataFrame:
        return pd.DataFrame(
            [
                {
                    "run_id": run_id,
                    "dataset": self.dataset,
                    "source": self.source,
                    "check_name": r.name,
                    "status": r.status,
                    "rows_in": r.rows_in,
                    "rows_out": r.rows_out,
                    "detail": r.detail,
                    "event_at": datetime.now(UTC),
                }
                for r in self.results
            ]
        )

    def summary(self) -> str:
        icon = {"pass": "ok  ", "warn": "warn", "fail": "FAIL"}
        return "\n".join(f"  [{icon[r.status]}] {r.name}: {r.detail}" for r in self.results)


def run_checks(
    cfg: SourceConfig,
    result: ValidationResult,
    *,
    now: datetime | None = None,
    expected_rows: int | None = None,
    mode: str = "live",
) -> QualityReport:
    now = now or datetime.now(UTC)
    wanted = list(cfg.quality.get("checks", ["contract"]))
    report = QualityReport(source=cfg.name, dataset=cfg.dataset)
    clean = result.clean

    # `reconcile_totals` is a retired name. That check never reconciled against
    # the exchange: both sides of its comparison were built from the same parsed
    # frame, so a truncated file reconciled perfectly against itself and reported
    # a 0.0000% difference. It is honoured as an alias rather than dropped, so a
    # source config written against the old name keeps real checks instead of
    # silently losing one, and it now runs the two that replaced it:
    # `volume_accounted`, which is what the old arithmetic actually measured, and
    # `expected_coverage`, which is the one that can catch a truncated file.
    if "reconcile_totals" in wanted:
        wanted += ["volume_accounted", "expected_coverage"]

    if "contract" in wanted:
        # A format change blocks. Bad rows inside a good format are quarantined
        # and reported, because one broken row should not stop a release.
        if result.blocking_failures:
            status, detail = "fail", "; ".join(f["detail"] for f in result.blocking_failures)
        elif len(result.rejected):
            status = "warn"
            top = result.rejected["_reject_reason"].value_counts().head(3).to_dict()
            detail = f"{len(result.rejected)} rows quarantined: {top}"
        else:
            status, detail = "pass", "all rows satisfy the contract"
        report.results.append(
            CheckResult("contract", status, detail, result.rows_in, len(clean))
        )

    if "uniqueness" in wanted:
        dupes = [f for f in result.failures if f["kind"] == "duplicate"]
        report.results.append(
            CheckResult(
                "uniqueness",
                "warn" if dupes else "pass",
                dupes[0]["detail"] if dupes else f"unique on {cfg.contract.grain}",
                result.rows_in,
                len(clean),
            )
        )

    if "monotonic_trade_id" in wanted and {"symbol", "trade_id", "ts"} <= set(clean.columns):
        offenders = 0
        for _, g in clean.sort_values("ts").groupby("symbol"):
            offenders += int((g["trade_id"].diff() < 0).sum())
        report.results.append(
            CheckResult(
                "monotonic_trade_id",
                "warn" if offenders else "pass",
                f"{offenders} out-of-order ids (reordering is normal, a spike is not)",
                len(clean),
                len(clean),
            )
        )

    if "gap_scan" in wanted and "ts" in clean.columns and len(clean):
        ts = pd.to_datetime(clean["ts"], utc=True).sort_values()
        buckets = ts.dt.floor("min")
        minutes = buckets.nunique()
        span = int((buckets.max() - buckets.min()).total_seconds() // 60) + 1
        missing = max(0, span - minutes)
        report.results.append(
            CheckResult(
                "gap_scan",
                "warn" if missing > 0 else "pass",
                f"{missing} of {span} minutes with no prints",
                len(clean),
                len(clean),
            )
        )

    if "freshness" in wanted and mode == "backfill":
        report.results.append(CheckResult(
            "freshness", "pass",
            "not applicable to a backfill; dataset currency is checked by the "
            "arrival monitor instead",
            len(clean), len(clean),
        ))
    elif "freshness" in wanted and len(clean):
        col = cfg.contract.event_time_column
        latest = pd.to_datetime(clean[col], utc=True, errors="coerce").max()
        calendar_name = (cfg.acquire.get("monitor") or {}).get("calendar")

        if calendar_name:
            # Calendar-aware freshness. A wall-clock threshold is wrong for any
            # end-of-day file: the file for Thursday is published after Thursday
            # closes, so on Friday morning it is already 15 hours old and on
            # Monday it is three days old without anything being wrong. The
            # question is not "how old is this?" but "is this the most recent
            # trading day that should have been published?"
            from .calendar import load_calendar

            cal = load_calendar(calendar_name)
            expected = cal.expected_days(lookback=10, now=now)
            most_recent_due = max(expected) if expected else None
            latest_day = latest.date() if pd.notna(latest) else None
            if most_recent_due is None:
                status, detail = "pass", "nothing due yet today"
            elif latest_day and latest_day >= most_recent_due:
                status = "pass"
                detail = f"latest trading day {latest_day}, most recent due {most_recent_due}"
            else:
                status = "fail"
                detail = (
                    f"latest trading day {latest_day}, but {most_recent_due} "
                    f"is past its publication deadline"
                )
        else:
            age = max((pd.Timestamp(now) - latest).total_seconds(), 0.0)
            sla = float(cfg.quality.get("freshness_sla_seconds", 86400))
            status = "pass" if age <= sla else "fail"
            detail = f"latest event {age / 60:.1f} min old, SLA {sla / 60:.0f} min"

        report.results.append(
            CheckResult("freshness", status, detail, len(clean), len(clean))
        )

    if "volume_accounted" in wanted and {"volume", "symbol"} <= set(clean.columns):
        # How much of the file's own declared volume survived the contract gate,
        # per symbol. This is a real and useful number — 5% of rows rejected but
        # 60% of the volume rejected is a different incident from 5% and 5% — and
        # it is emphatically not reconciliation: every figure here comes out of
        # the file being checked, so it cannot see anything the file does not
        # say about itself.
        tol = float((cfg.quality.get("reconcile") or {}).get("tolerance_pct", 0.0))
        parsed = pd.concat([clean, result.rejected], ignore_index=True)
        declared = parsed.groupby("symbol")["volume"].sum()
        loaded = clean.groupby("symbol")["volume"].sum()
        share = (loaded - declared).abs() / declared.replace(0, pd.NA)
        worst = float(share.max() * 100) if len(share) else 0.0
        report.results.append(
            CheckResult(
                "volume_accounted",
                "pass" if worst <= tol else "warn",
                f"largest per-symbol share of the file's own volume quarantined: "
                f"{worst:.4f}%",
                result.rows_in,
                len(clean),
            )
        )

    if "expected_coverage" in wanted and {"trade_date", "symbol"} <= set(clean.columns):
        report.results.append(_expected_coverage(cfg, clean, result))

    minimum = cfg.quality.get("expect_rows_min")
    if minimum is not None:
        ok = len(clean) >= int(minimum)
        report.results.append(
            CheckResult(
                "expect_rows_min",
                "pass" if ok else "fail",
                f"{len(clean)} rows, expected at least {minimum}",
                result.rows_in,
                len(clean),
            )
        )

    if expected_rows:
        got = len(clean) / expected_rows
        report.results.append(
            CheckResult(
                "completeness_vs_expected",
                "pass" if got >= 0.999 else "warn",
                f"{got:.2%} of expected rows arrived",
                expected_rows,
                len(clean),
            )
        )

    return report


def _expected_coverage(cfg: SourceConfig, clean: pd.DataFrame,
                       result: ValidationResult) -> CheckResult:
    """Is anything missing from the middle or the end of this end-of-day file?

    This is the check that can catch a silently truncated delivery, and it does
    it without an external reference, because this platform has none: it compares
    the file against the trading calendar and against its own shape.

    Two things a truncated file cannot hide. A transfer cut short stops partway
    through the last day it reached, so that day arrives with fewer symbols than
    every other day in the same file, and that raggedness is visible from the
    file alone. A delivery that lost a chunk in the middle keeps a plausible
    first and last day but skips trading days in between, and the calendar knows
    which days those were.

    A breach blocks. A partially delivered file is exactly the half-load this
    platform refuses to do, and publishing 80% of a day is worse than publishing
    none of it: the gap is then invisible to everyone downstream.
    """
    days = pd.to_datetime(clean["trade_date"], errors="coerce").dt.date
    frame = pd.DataFrame({"day": days, "symbol": clean["symbol"]}).dropna()
    if frame.empty:
        return CheckResult("expected_coverage", "pass", "no dated rows to check",
                           result.rows_in, len(clean))

    present = {day: set(g["symbol"]) for day, g in frame.groupby("day")}
    everything = set().union(*present.values())
    problems: list[str] = []

    calendar_name = (cfg.acquire.get("monitor") or {}).get("calendar")
    if calendar_name and len(present) > 1:
        from .calendar import load_calendar

        cal = load_calendar(calendar_name)
        absent = [d for d in cal.trading_days(min(present), max(present))
                  if d not in present]
        if absent:
            problems.append(
                f"{len(absent)} trading day(s) inside the file's own span have no "
                f"rows at all: {absent[:5]}"
            )

    short = {d: sorted(everything - s) for d, s in present.items() if s != everything}
    if short:
        latest = max(short)
        problems.append(
            f"{len(short)} day(s) carry fewer symbols than the rest of the file, "
            f"latest {latest} missing {short[latest]}"
        )

    # Comparing against the symbol list in the source config is the strongest
    # form of this check and it is deliberately opt-in. A vendor legitimately
    # splits a day into one file per commodity — the drop directory in this repo
    # is exercised exactly that way — so requiring every configured symbol in
    # every file would reject correct deliveries. A deployment whose file really
    # does carry the whole list sets quality.coverage.require_configured_symbols.
    if (cfg.quality.get("coverage") or {}).get("require_configured_symbols"):
        missing = sorted(set(cfg.acquire.get("symbols") or []) - everything)
        if missing:
            problems.append(f"configured symbols absent from the file entirely: {missing}")

    if problems:
        return CheckResult("expected_coverage", "fail", "; ".join(problems),
                           result.rows_in, len(clean))
    return CheckResult(
        "expected_coverage", "pass",
        f"{len(present)} day(s), each carrying all {len(everything)} symbol(s) "
        f"the file contains",
        result.rows_in, len(clean),
    )


def _table_exists(store, table: str) -> bool | None:
    """Does this table exist? None when the question itself cannot be answered.

    The three answers are genuinely different and the caller acts differently on
    each, so they are not collapsed into a bool. Both stores speak
    `information_schema`, which is why the probe is not written per backend.
    """
    try:
        found = store.query(
            f"SELECT count(*) AS n FROM information_schema.tables "
            f"WHERE table_name = '{table}'"
        )
    except Exception:
        return None
    if found.empty:
        return None
    return int(found.iloc[0, 0]) > 0


def check_schema_version(cfg: SourceConfig, store, *, accept: bool = False) -> CheckResult:
    """Has the contract version changed since the last load?

    A dataset's contract version changing is not a bug, it is a decision
    somebody made: a new field, a changed type, a different grain. What must not
    happen is that decision arriving silently at 6am and quietly rewriting what
    consumers thought they were reading.

    So a first sighting is recorded and allowed; a change stops the load until a
    person accepts it. `accept` is that person, and their name goes in the table.

    A gate that cannot read its own state fails closed. Treating a failed read as
    an empty table turns a blocked contract change into a first sighting, which
    is then recorded as auto-accepted, which makes every later run pass
    legitimately: one transient database error and the gate is permanently
    disarmed with no way back. So the two cases are told apart. A
    `schema_versions` table that does not exist yet is a real first run and is
    allowed. A read that failed for any other reason blocks, and blocks again on
    the next run, because nothing was written.
    """
    version = cfg.contract.schema_version
    try:
        seen = store.query(
            f"SELECT * FROM schema_versions WHERE dataset = '{cfg.dataset}' "
            f"ORDER BY first_seen DESC"
        )
    except Exception as exc:
        if _table_exists(store, "schema_versions") is False:
            seen = pd.DataFrame()
        else:
            return CheckResult(
                "schema_version", "fail",
                f"cannot read schema_versions, so the contract version of "
                f"{cfg.dataset} is unknown and nothing is published: {exc}",
            )

    known = [] if seen.empty else list(seen["schema_version"])
    if version in known:
        return CheckResult("schema_version", "pass", f"contract v{version}, unchanged")

    if not known or accept:
        store.insert("schema_versions", pd.DataFrame([{
            "dataset": cfg.dataset,
            "schema_version": version,
            "accepted_by": "auto (first sighting)" if not known else "operator",
            "first_seen": datetime.now(UTC),
        }]))
        detail = (
            f"first sighting of contract v{version}"
            if not known
            else f"contract changed {known[0]} -> {version}, accepted"
        )
        return CheckResult("schema_version", "pass", detail)

    return CheckResult(
        "schema_version", "fail",
        f"contract changed {known[0]} -> {version}. Nothing is published until "
        f"someone accepts it: re-run with --accept-schema-change once the "
        f"consumers of this dataset have been told",
    )


def score_vendor(
    cfg: SourceConfig,
    result: ValidationResult,
    report: QualityReport,
    *,
    as_of: datetime | None = None,
    mode: str = "live",
) -> dict[str, Any]:
    """Turn a load into three numbers a supplier conversation can use.

    Freshness is not scored on a backfill. Historical files are old by
    definition, and letting that drag the score down would mean every backfill
    marked a good vendor red, which is how a scoring system stops being read.
    The remaining weights are renormalised so the number stays comparable.
    """
    as_of = as_of or datetime.now(UTC)
    spec = vendor_quality_policy()["vendor_quality"]
    dims, thresholds = spec["dimensions"], spec["thresholds"]

    target = dims["freshness"]["target_seconds"].get(cfg.name, 3600)
    fresh_check = next((r for r in report.results if r.name == "freshness"), None)
    if fresh_check and len(result.clean):
        col = cfg.contract.event_time_column
        latest = pd.to_datetime(result.clean[col], utc=True, errors="coerce").max()
        age = max((pd.Timestamp(as_of) - latest).total_seconds(), 0.0)
        freshness = min(1.0, target / age) if age > target else 1.0
    else:
        freshness = 1.0 if len(result.clean) else 0.0

    rows_in = max(result.rows_in, 1)
    completeness = len(result.clean) / rows_in
    accuracy = 1.0 - (len(result.rejected) / rows_in)

    if mode == "backfill":
        weight = dims["completeness"]["weight"] + dims["accuracy"]["weight"]
        score = (
            dims["completeness"]["weight"] * completeness
            + dims["accuracy"]["weight"] * accuracy
        ) / weight
        freshness = float("nan")
    else:
        score = (
            dims["freshness"]["weight"] * freshness
            + dims["completeness"]["weight"] * completeness
            + dims["accuracy"]["weight"] * accuracy
        )
    status = (
        "green" if score >= thresholds["green"]
        else "amber" if score >= thresholds["amber"]
        else "red"
    )
    return {
        "as_of": as_of.date(),
        "source": cfg.name,
        "freshness": None if mode == "backfill" else round(freshness, 4),
        "completeness": round(completeness, 4),
        "accuracy": round(accuracy, 4),
        "score": round(score, 4),
        "status": status,
        "detail": (
            f"{len(result.rejected)} of {rows_in} rows failed the contract"
            + (", freshness not scored (backfill)" if mode == "backfill" else "")
        ),
    }
