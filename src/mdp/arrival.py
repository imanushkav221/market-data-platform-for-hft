"""Waiting for a file inside its arrival window.

A single scheduled fetch at a fixed time assumes the exchange publishes at
exactly the time it says it does. It mostly does, and the days it does not are
the days that matter: the file lands forty minutes late, the 06:00 job already
ran and found nothing, and nobody knows until a researcher asks why yesterday is
missing.

So the end-of-day path does what a real one does: it starts polling when the file
is due and keeps looking until it appears or the window closes. Three outcomes,
all of them recorded rather than inferred:

  - **on time** — arrived inside the grace period, nothing to say
  - **late** — arrived, but after the grace period, so the delay is logged and a
    warning goes out. Repeated lateness is a vendor conversation with evidence
  - **missing** — the window closed with nothing, which pages somebody

The distinction between "late" and "missing" is the reason this exists. Both look
identical to a single-shot fetch, and they need completely different responses.
"""
from __future__ import annotations

import time
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta

import pandas as pd

from .alerts import Alert, Notifier, Severity
from .calendar import TradingCalendar, load_calendar
from .config import SourceConfig
from .fetch import fetch_to_drop
from .storage import Store
from .watch import watch


@dataclass
class Arrival:
    source: str
    trading_day: date
    deadline: datetime
    status: str                      # on_time | late | missing | skipped
    attempts: int = 0
    arrived_at: datetime | None = None
    delay_seconds: float = 0.0
    rows_loaded: int = 0
    published: bool = False
    detail: str = ""

    def summary(self) -> str:
        if self.status == "skipped":
            return f"{self.source} {self.trading_day}: skipped ({self.detail})"
        if self.status == "missing":
            return (
                f"{self.source} {self.trading_day}: NOT PUBLISHED after "
                f"{self.attempts} attempts, window closed"
            )
        late = (
            f", {self.delay_seconds / 60:.0f} min after the deadline"
            if self.status == "late" else ""
        )
        return (
            f"{self.source} {self.trading_day}: arrived on attempt {self.attempts}"
            f"{late}, {self.rows_loaded} rows loaded"
        )


def await_arrival(
    cfg: SourceConfig,
    store: Store,
    trading_day: date | None = None,
    *,
    calendar: TradingCalendar | None = None,
    notifier: Notifier | None = None,
    synthetic: bool = False,
    now: datetime | None = None,
    poll_seconds: float | None = None,
    max_wait_minutes: float | None = None,
    max_attempts: int | None = None,
    sleep: bool = True,
) -> Arrival:
    """Poll for one trading day's file from its deadline until the window closes.

    `sleep=False` and `max_attempts` exist so this is testable without waiting
    for real minutes to pass; everything else behaves identically.

    `now` is the clock this whole function runs on, including the arrival time it
    records and the moment it decides the window has closed. Reading the wall
    clock instead would score a replay of last Tuesday's arrival against this
    morning, turning a file that was twenty minutes late into one that is days
    late, which is precisely the mistake the calendar module's docstring warns
    about. Each unsuccessful pass advances that clock by the poll interval it
    waited, so the window means the same thing whether the poll really slept or
    a test asked it not to.
    """
    settings = cfg.acquire.get("schedule") or {}
    monitor = cfg.acquire.get("monitor") or {}
    calendar = calendar or load_calendar(monitor.get("calendar", "mcx"))
    notifier = notifier or Notifier()

    now = now or datetime.now(calendar.tz)
    trading_day = trading_day or calendar.previous_trading_day(now.date() + timedelta(days=1))

    if not calendar.is_trading_day(trading_day):
        return Arrival(cfg.name, trading_day, calendar.delivery_deadline(trading_day),
                       "skipped", detail=calendar.why_closed(trading_day) or "not a trading day")

    deadline = calendar.delivery_deadline(trading_day)
    interval = float(poll_seconds if poll_seconds is not None
                     else settings.get("poll_seconds", 900))
    window = float(max_wait_minutes if max_wait_minutes is not None
                   else settings.get("max_wait_minutes", 120))
    closes_at = deadline + timedelta(minutes=window)

    clock = now
    attempts = 0
    while True:
        attempts += 1
        # `ignore_deadline` because we are deliberately looking before the file is
        # formally overdue: that is the entire point of a window.
        result = fetch_to_drop(
            cfg, trading_day, synthetic=synthetic, calendar=calendar,
            ignore_deadline=True,
        )

        if result.fetched:
            arrived_at = clock
            delay = max((arrived_at - deadline).total_seconds(), 0.0)
            on_time = arrived_at <= deadline + timedelta(minutes=calendar.grace_minutes)

            deliveries = watch([cfg], store, once=True)
            rows = sum(d.rows_loaded for d in deliveries)
            published = all(d.published for d in deliveries) if deliveries else False

            arrival = Arrival(
                cfg.name, trading_day, deadline,
                "on_time" if on_time else "late",
                attempts=attempts, arrived_at=arrived_at, delay_seconds=delay,
                rows_loaded=rows, published=published,
            )
            _record(store, cfg, arrival)
            if not on_time:
                notifier.send(Alert(
                    Severity.WARN,
                    f"{cfg.name}: {trading_day} file published late",
                    f"{delay / 60:.0f} minutes after the {deadline:%H:%M %Z} deadline, "
                    f"on attempt {attempts}. Loaded {rows} rows.",
                    source=cfg.name,
                    context={"trading_day": str(trading_day),
                             "delay_minutes": round(delay / 60, 1)},
                ))
            if not published and deliveries:
                notifier.blocked_load(
                    cfg.name, deliveries[0].blocked_by,
                    "the file arrived but did not pass its checks",
                )
            return arrival

        exhausted = (max_attempts is not None and attempts >= max_attempts)
        if exhausted or clock >= closes_at:
            arrival = Arrival(
                cfg.name, trading_day, deadline, "missing", attempts=attempts,
                detail=result.skipped_reason or "nothing published",
            )
            _record(store, cfg, arrival)
            notifier.missing_delivery(
                cfg.name, trading_day, deadline,
                f"window closed after {attempts} attempts over "
                f"{window:.0f} minutes",
            )
            return arrival

        if sleep:
            time.sleep(interval)
        # Advanced by exactly what was waited, which matches elapsed time when the
        # poll really slept and keeps a replay deterministic when it did not.
        clock += timedelta(seconds=interval)


def _record(store: Store, cfg: SourceConfig, arrival: Arrival) -> None:
    """Publication timeliness belongs in the same table as every other signal.

    A month of these answers "how reliable is this source, really?" with numbers
    instead of impressions.
    """
    status = {"on_time": "pass", "late": "warn", "missing": "fail", "skipped": "pass"}
    store.insert(
        "quality_events",
        pd.DataFrame([{
            "run_id": "arrival",
            "dataset": cfg.dataset,
            "source": cfg.name,
            "check_name": "publication_timeliness",
            "status": status[arrival.status],
            "rows_in": 0,
            "rows_out": arrival.rows_loaded,
            "detail": arrival.summary(),
            "event_at": datetime.now(UTC),
        }]),
    )
