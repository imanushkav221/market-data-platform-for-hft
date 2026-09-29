"""Automated retrieval: go and get the file, instead of waiting for someone to
put it somewhere.

This closes the loop. The fetcher pulls from the exchange or vendor and lands the
payload in the drop zone; the watcher picks it up from there. They never call each
other.

That indirection is deliberate and is the point worth making in the presentation:
the loader does not care whether a file arrived because we fetched it, because a
vendor pushed it over SFTP, or because somebody dropped it in by hand during an
incident. All three paths are the same path. Replacing the fetcher — a new vendor,
a new protocol, an S3 event instead of a poll — touches nothing downstream.

Writes are atomic: content goes to a `.part` file and is renamed into place, so
the watcher can never see a half-written file.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path

import pandas as pd

from . import cursor as cursor_store
from .acquire import acquire
from .calendar import TradingCalendar, load_calendar
from .config import SourceConfig, repo_root


@dataclass
class FetchResult:
    source: str
    trade_date: date
    path: Path | None
    rows: int
    skipped_reason: str | None = None
    seconds: float = 0.0

    @property
    def fetched(self) -> bool:
        return self.path is not None

    def summary(self) -> str:
        if self.skipped_reason:
            return f"{self.source} {self.trade_date}: skipped ({self.skipped_reason})"
        where = self.path.name if self.path else "nowhere"
        return (
            f"{self.source} {self.trade_date}: {self.rows} rows -> "
            f"{where} in {self.seconds:.1f}s"
        )


def drop_dir(cfg: SourceConfig) -> Path:
    drop = cfg.acquire.get("drop") or {}
    directory = repo_root() / drop.get("dir", f"data/incoming/{cfg.name}")
    directory.mkdir(parents=True, exist_ok=True)
    return directory


def fetch_incremental(cfg: SourceConfig, store, *, synthetic: bool = False,
                      now: datetime | None = None, **params) -> dict:
    """Fetch everything since the last successful load, for a continuous source.

    This is the other half of the platform, and the half people forget. An
    end-of-day file has one arrival a day; a venue has a firehose, and the only
    sane way to read it on a schedule is to remember where you stopped.
    """
    from .pipeline import run_source

    settings = cfg.acquire.get("incremental") or {}
    position = cursor_store.read(store, cfg.name)
    start, end = position.window(
        default_minutes=int(settings.get("first_run_minutes", 60)),
        overlap_seconds=int(settings.get("overlap_seconds", 5)),
        now=now,
    )
    # `now` is passed through deliberately. Without it, `run_source` evaluated
    # the contract's future-event rule and the freshness check against the wall
    # clock rather than against the window that was actually requested, so any
    # replay of a historical window failed freshness and the cursor never moved.
    summary = run_source(
        cfg, store, synthetic=synthetic, start=start, end=end, now=end, **params
    )
    summary["window"] = (start, end)
    summary["cursor_before"] = position.position_ts
    return summary


def fetch_to_drop(
    cfg: SourceConfig,
    trade_date: date | None = None,
    *,
    synthetic: bool = False,
    calendar: TradingCalendar | None = None,
    force: bool = False,
    ignore_deadline: bool = False,
    **params,
) -> FetchResult:
    """Retrieve one day and land it in the drop zone.

    Returns without fetching on a non-trading day, because asking an exchange for
    a file it never published is how you generate alerts nobody reads.
    """
    started = datetime.now(UTC)
    calendar = calendar or _calendar_for(cfg)
    trade_date = trade_date or (
        calendar.previous_trading_day() if calendar else date.today()
    )

    # The future check comes first, and the order is not arbitrary. A date that
    # is both in the future and a Saturday is in the future: that is the more
    # fundamental reason and the one a caller needs to hear. With the calendar
    # check first, `today + 3` reported "weekend" whenever today was a Wednesday
    # or a Thursday, which also made the test that pins this behaviour fail two
    # days in seven, and a test that goes red on a schedule teaches whoever owns
    # it to ignore red.
    if not force and trade_date > date.today():
        return FetchResult(cfg.name, trade_date, None, 0,
                           skipped_reason="in the future")

    if calendar and not force and not calendar.is_trading_day(trade_date):
        return FetchResult(cfg.name, trade_date, None, 0,
                           skipped_reason=calendar.why_closed(trade_date))
    if calendar and not force and not ignore_deadline and not calendar.is_overdue(trade_date):
        return FetchResult(
            cfg.name, trade_date, None, 0,
            skipped_reason=(
                "not published yet, due "
                f"{calendar.delivery_deadline(trade_date):%H:%M %Z}"
            ),
        )

    frame = acquire(cfg, synthetic=synthetic, trade_date=trade_date, days=1, **params)
    if frame is None or frame.empty:
        return FetchResult(cfg.name, trade_date, None, 0, skipped_reason="source returned nothing")

    # Keep only the requested day when the driver hands back more (the synthetic
    # generator does; a real endpoint for one date does not).
    date_column = (cfg.acquire.get("monitor") or {}).get("date_column", "trade_date")
    if date_column in frame.columns:
        as_dates = pd.to_datetime(frame[date_column], errors="coerce").dt.date
        if (as_dates == trade_date).any():
            frame = frame[as_dates == trade_date]

    directory = drop_dir(cfg)
    stamp = trade_date.strftime("%Y%m%d")
    suffix = (cfg.acquire.get("drop") or {}).get("pattern", "*.csv").split(".")[-1]
    target = directory / f"{cfg.name.upper()}_{stamp}.{suffix}"

    # Atomic landing: write aside, then rename. A reader can never catch this
    # halfway, which is the same discipline a careful vendor uses.
    staging = target.with_suffix(target.suffix + ".part")
    if suffix in ("parquet", "pq"):
        frame.to_parquet(staging, index=False)
    else:
        frame.to_csv(staging, index=False)
    staging.replace(target)

    seconds = (datetime.now(UTC) - started).total_seconds()
    return FetchResult(cfg.name, trade_date, target, len(frame), seconds=seconds)


def fetch_range(
    cfg: SourceConfig,
    start: date,
    end: date,
    *,
    synthetic: bool = False,
    calendar: TradingCalendar | None = None,
    **params,
) -> list[FetchResult]:
    """Backfill: every trading day in the range, weekends and holidays skipped.

    Deliberately the same code path as the daily fetch. A backfill that works
    differently from the daily run is a backfill that produces different data.
    """
    calendar = calendar or _calendar_for(cfg)
    # Every calendar day is walked, and the non-trading ones are reported as
    # skipped rather than silently dropped. "Nothing happened on 2 October" and
    # "2 October was a holiday" are different answers to the same question, and
    # only one of them stops somebody investigating.
    days = [d.date() for d in pd.date_range(start, end)]
    return [
        fetch_to_drop(cfg, day, synthetic=synthetic, calendar=calendar, **params)
        for day in days
    ]


def _calendar_for(cfg: SourceConfig) -> TradingCalendar | None:
    name = (cfg.acquire.get("monitor") or {}).get("calendar")
    return load_calendar(name) if name else None
