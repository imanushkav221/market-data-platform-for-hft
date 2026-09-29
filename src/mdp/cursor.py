"""High-water marks: what we have already taken from each source.

Without this, every fetch asks for a fixed window and you get one of two bugs.
Ask for too little and you lose data whenever a run is late or slow. Ask for too
much and you re-fetch the same rows forever, which looks fine until the source
starts charging per request or rate-limiting you at the worst moment.

A cursor fixes both: fetch from where we got to, not from a guess. The rules that
matter:

  1. **The cursor only moves after a successful publish.** If the load is blocked,
     the position stays put and the next run picks up the same data. A cursor
     advanced on fetch rather than on load is how gaps appear.
  2. **It moves to what we actually loaded**, not to what we asked for, because
     those differ whenever a source truncates a response.
  3. **It overlaps deliberately.** Re-reading a few seconds costs nothing and the
     grain deduplicates; missing a few seconds is silent and permanent.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

import pandas as pd

from .storage import Store

log = logging.getLogger("mdp.cursor")

DDL_DUCKDB = """
CREATE TABLE IF NOT EXISTS ingest_cursors (
    source VARCHAR, position_ts TIMESTAMPTZ, position_id BIGINT,
    rows_at_position BIGINT, updated_at TIMESTAMPTZ
);
"""


@dataclass
class Cursor:
    source: str
    position_ts: datetime | None = None
    position_id: int | None = None

    @property
    def is_start(self) -> bool:
        return self.position_ts is None and self.position_id is None

    def window(self, *, default_minutes: int, overlap_seconds: int,
              now: datetime | None = None) -> tuple[datetime, datetime]:
        """The time range to ask the source for.

        First run has no position, so it takes the configured window. Every run
        after that starts a little before where we got to: the overlap costs one
        deduplicated read and closes the gap that clock skew would otherwise open.
        """
        now = now or datetime.now(UTC)
        if self.position_ts is None:
            return now - timedelta(minutes=default_minutes), now
        return self.position_ts - timedelta(seconds=overlap_seconds), now


def read(store: Store, source: str) -> Cursor:
    try:
        rows = store.query(
            f"SELECT * FROM ingest_cursors WHERE source = '{source}' "
            f"ORDER BY updated_at DESC LIMIT 1"
        )
    except Exception:
        return Cursor(source)
    if rows.empty:
        return Cursor(source)
    row = rows.iloc[0]
    ts = pd.to_datetime(row["position_ts"], utc=True, errors="coerce")
    pid = row["position_id"]
    return Cursor(
        source,
        None if pd.isna(ts) else ts.to_pydatetime(),
        None if pd.isna(pid) else int(pid),
    )


def advance(store: Store, source: str, frame: pd.DataFrame, *,
            time_column: str = "ts", id_column: str | None = "trade_id",
            window_end: datetime | None = None) -> Cursor | None:
    """Move the mark to the furthest row we actually loaded, but never past the
    window we actually asked for.

    Called after a successful publish and nowhere else.

    `window_end` is the cap, and without it this function had a hole that
    produced exactly the permanent silent gap the module docstring promises
    cannot happen. A single row carrying a clock-skewed timestamp two hours
    ahead of the requested window dragged the high-water mark with it. The next
    window then started after it ended, read backwards, and the two hours in
    between were never requested by any run, ever. Not an error, not a retry:
    just missing.

    Capping is the right response rather than rejecting the row, because the
    skewed print is real data that belongs in the table. What it must not do is
    speak for time the pipeline has not yet covered.
    """
    if frame is None or frame.empty or time_column not in frame.columns:
        return None
    times = pd.to_datetime(frame[time_column], utc=True, errors="coerce")
    latest = times.max()
    if pd.isna(latest):
        return None
    if window_end is not None:
        cap = pd.Timestamp(window_end)
        if cap.tzinfo is None:
            cap = cap.tz_localize("UTC")
        if latest > cap:
            log.warning(
                "%s: a row is stamped %s, past the requested window end %s; "
                "capping the cursor so the gap between them is not skipped",
                source, latest, cap,
            )
            latest = cap
    position_id = None
    if id_column and id_column in frame.columns:
        try:
            position_id = int(pd.to_numeric(frame[id_column], errors="coerce").max())
        except (TypeError, ValueError):
            position_id = None

    store.insert(
        "ingest_cursors",
        pd.DataFrame([{
            "source": source,
            "position_ts": latest.to_pydatetime(),
            "position_id": position_id,
            "rows_at_position": len(frame),
            "updated_at": datetime.now(UTC),
        }]),
    )
    return Cursor(source, latest.to_pydatetime(), position_id)
