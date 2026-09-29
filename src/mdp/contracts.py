"""Contract validation: the gate every row passes before it reaches the database.

The rule this module exists to enforce: never half-load. A file either meets its
contract and goes in, or the offending rows are quarantined with a reason and a
human is told. A vendor changing their format fails here, loudly, instead of
silently corrupting a table that research is already querying.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Any

import pandas as pd

from .config import Contract

_COERCERS = {
    "string": lambda s: s.astype("string"),
    "int": lambda s: pd.to_numeric(s, errors="coerce").astype("Int64"),
    "float": lambda s: pd.to_numeric(s, errors="coerce").astype("float64"),
    "bool": lambda s: s.astype("boolean"),
    "date": lambda s: pd.to_datetime(s, errors="coerce", utc=False).dt.normalize(),
    "timestamp_ms": lambda s: pd.to_datetime(
        pd.to_numeric(s, errors="coerce"), unit="ms", errors="coerce", utc=True
    )
    if pd.api.types.is_numeric_dtype(s)
    else pd.to_datetime(s, errors="coerce", utc=True),
}


@dataclass
class ValidationResult:
    dataset: str
    clean: pd.DataFrame
    rejected: pd.DataFrame
    failures: list[dict[str, Any]] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return len(self.rejected) == 0 and not self.schema_failures

    @property
    def schema_failures(self) -> list[dict[str, Any]]:
        """Failures that are about the shape of the file, not individual rows.

        These are the ones worth paging someone about: a missing column means the
        provider changed their format.
        """
        return [f for f in self.failures if f["kind"] in ("missing_column", "empty")]

    @property
    def threshold_failures(self) -> list[dict[str, Any]]:
        """Rejections that breached a limit the contract itself declared.

        A few bad rows are quarantined and the load continues, which is the right
        answer and the reason quarantine exists. Past the limit the contract
        states, it stops being a few bad rows and starts being a different file,
        and loading part of a different file is the half-load this module refuses
        to do.
        """
        return [f for f in self.failures if f["kind"] == "reject_rate"]

    @property
    def blocking_failures(self) -> list[dict[str, Any]]:
        """Everything that must stop a load: a format change, or a breached limit."""
        return self.schema_failures + self.threshold_failures

    @property
    def rows_in(self) -> int:
        return len(self.clean) + len(self.rejected)

    def summary(self) -> str:
        bits = [f"{self.dataset}: {len(self.clean)} clean, {len(self.rejected)} rejected"]
        for f in self.failures:
            bits.append(f"  - {f['kind']}: {f['detail']}")
        return "\n".join(bits)


def validate(df: pd.DataFrame, contract: Contract, *,
             now: datetime | None = None) -> ValidationResult:
    now = now or datetime.now(UTC)
    failures: list[dict[str, Any]] = []
    df = df.copy()

    # 1. Shape of the file. A missing required column is a format change, not a bad row.
    missing = [c for c in contract.required_columns if c not in df.columns]
    if missing:
        failures.append(
            {"kind": "missing_column", "detail": f"required columns absent: {sorted(missing)}"}
        )
        return ValidationResult(contract.dataset, df.head(0), df, failures)

    if df.empty:
        failures.append({"kind": "empty", "detail": "source returned no rows"})
        return ValidationResult(contract.dataset, df, df.head(0), failures)

    # Drop anything the contract does not declare. Undeclared columns are not loaded:
    # if a provider adds a field we want, it goes in the contract first.
    extra = [c for c in df.columns if c not in contract.column_names]
    if extra:
        failures.append({"kind": "undeclared_column", "detail": f"ignored: {sorted(extra)}"})
    df = df[[c for c in contract.column_names if c in df.columns]]

    reasons = pd.Series([""] * len(df), index=df.index, dtype="object")
    # Which reject reasons mean "the value was not there". `max_null_fraction` is
    # a limit on those specifically, so they are recorded as they are raised
    # rather than recovered later by matching on the wording of a message.
    null_reasons: set[str] = set()

    def fail(mask: pd.Series, reason: str, *, is_null: bool = False) -> None:
        mask = mask.fillna(False).astype(bool)
        if mask.any():
            reasons.loc[mask & (reasons == "")] = reason
            failures.append({"kind": "row_rule", "detail": f"{reason} on {int(mask.sum())} rows"})
        if is_null:
            null_reasons.add(reason)

    # 2. Types. Anything that will not coerce is a rejected row, not a crash.
    for spec in contract.columns:
        name, ctype = spec["name"], spec["type"]
        if name not in df.columns:
            continue
        before_null = df[name].isna()
        df[name] = _COERCERS[ctype](df[name])
        # A value that will not coerce ends up null, so it counts against the
        # null budget exactly as an absent one does.
        fail(df[name].isna() & ~before_null, f"{name} not coercible to {ctype}", is_null=True)
        if spec.get("required"):
            fail(before_null, f"{name} is required but null", is_null=True)

    # 3. Per column rules.
    for spec in contract.columns:
        name = spec["name"]
        if name not in df.columns:
            continue
        col = df[name]
        if "allowed" in spec:
            fail(col.notna() & ~col.isin(spec["allowed"]), f"{name} outside allowed values")
        if "min" in spec:
            op = col <= spec["min"] if spec.get("exclusive_min") else col < spec["min"]
            fail(col.notna() & op, f"{name} below minimum")
        if "max" in spec:
            fail(col.notna() & (col > spec["max"]), f"{name} above maximum")

    rules = contract.rules or {}

    # 4. Cross field rules, written in the contract rather than in code.
    for rule in rules.get("cross_field", []):
        try:
            holds = df.eval(rule["expr"])
        except Exception as exc:  # a broken rule must not look like clean data
            failures.append({"kind": "rule_error", "detail": f"{rule['name']}: {exc}"})
            continue
        fail(~holds.astype(bool), f"cross-field rule failed: {rule['name']}")

    # 5. Events from the future are a clock problem at the source. Never silently load them.
    horizon = rules.get("reject_future_events_seconds")
    if horizon is not None:
        et = df[contract.event_time_column]
        if pd.api.types.is_datetime64_any_dtype(et):
            cutoff = pd.Timestamp(now + timedelta(seconds=float(horizon)))
            if et.dt.tz is None:
                cutoff = cutoff.tz_localize(None)
            fail(et.notna() & (et > cutoff), "event time in the future")

    rejected = df[reasons != ""].copy()
    rejected["_reject_reason"] = reasons[reasons != ""]
    clean = df[reasons == ""].copy()

    # 6. Uniqueness on the declared grain. Duplicates are expected on a reconnect,
    # so they are dropped and counted rather than treated as corruption.
    unique_on = rules.get("unique_on") or contract.grain
    if unique_on and all(c in clean.columns for c in unique_on):
        dupes = clean.duplicated(subset=unique_on, keep="first")
        if dupes.any():
            failures.append(
                {"kind": "duplicate", "detail": f"{int(dupes.sum())} duplicate rows on {unique_on}"}
            )
            clean = clean[~dupes]

    # 7. Declared limits on how much of the file may be thrown away.
    #
    # A threshold the contract states and the loader then ignores is worse than
    # no threshold: it reads as a guarantee in review and delivers nothing at
    # 6am. So a breach is a `reject_rate` failure, and `reject_rate` blocks the
    # load — see ValidationResult.blocking_failures. Quarantining a handful of
    # bad rows and carrying on is still right and still happens; what cannot
    # happen is publishing 49 rows of a 1000-row file because the other 951 were
    # quietly filed as warnings.
    #
    # The two limits count different things on purpose, because the earlier
    # version counted every rejection under a key named for nulls:
    #   max_null_fraction   - rows lost because a required value was absent or
    #                         would not coerce, which is what the name says and
    #                         is the shape of a mangled or wrongly-delimited file
    #   max_reject_fraction - rows lost for any reason at all, which is the
    #                         blunter limit a source declares when it wants one
    null_rejects = int(reasons[reasons != ""].isin(null_reasons).sum())
    for key, count, what in (
        ("max_null_fraction", null_rejects,
         "of rows rejected for a required value that was absent or uncoercible"),
        ("max_reject_fraction", len(rejected), "of rows rejected"),
    ):
        limit = rules.get(key)
        if limit is None or not len(df):
            continue
        frac = count / len(df)
        if frac > float(limit):
            failures.append(
                {
                    "kind": "reject_rate",
                    "detail": f"{frac:.1%} {what}, contract allows "
                              f"{float(limit):.1%} ({key})",
                }
            )

    return ValidationResult(contract.dataset, clean, rejected, failures)
