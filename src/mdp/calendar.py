"""Trading calendar.

The cheapest way to make a data platform untrustworthy is to alert on Saturdays.
People stop reading the alerts, and then they miss the Tuesday that mattered.

So the calendar is a first-class part of the pipeline, not an afterthought: it
decides which days a delivery is expected, when it is late, and which day a
backfill should actually cover.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from functools import lru_cache
from zoneinfo import ZoneInfo

import yaml

from .config import repo_root

_WEEKDAYS = {"mon": 0, "tue": 1, "wed": 2, "thu": 3, "fri": 4, "sat": 5, "sun": 6}


@dataclass(frozen=True)
class TradingCalendar:
    name: str
    timezone: str
    trading_weekdays: set[int]
    holidays: dict[date, str]
    deadline: time
    grace_minutes: int
    session_open: time
    session_close: time

    @property
    def tz(self) -> ZoneInfo:
        return ZoneInfo(self.timezone)

    # ---------------- sessions ----------------
    def session_bounds(self, day: date) -> tuple[datetime, datetime] | None:
        """When trading actually opened and closed on a day, in exchange time.

        The config has carried a `session:` block since the beginning and nothing
        parsed it, which meant the module's own claim to know session times was
        not true. It matters for more than tidiness: MCX runs an evening session
        that crosses midnight UTC, so bucketing intraday data by UTC day splits a
        session in half, and every per-day aggregate computed that way is wrong
        in a manner that looks like thin volume rather than like a bug.

        Returns None on a day the exchange was shut, because a closed day has no
        session and returning a plausible pair of times for one would be the same
        class of error this module exists to prevent.
        """
        day = _as_date(day)
        if not self.is_trading_day(day):
            return None
        opened = datetime.combine(day, self.session_open, tzinfo=self.tz)
        closed = datetime.combine(day, self.session_close, tzinfo=self.tz)
        if closed <= opened:
            # A session declared to end before it starts is one that runs past
            # midnight, which is the normal case for commodities.
            closed += timedelta(days=1)
        return opened, closed

    def in_session(self, moment: datetime) -> bool:
        """Whether a timestamp falls inside its own exchange session."""
        local = moment.astimezone(self.tz)
        for candidate in (local.date(), local.date() - timedelta(days=1)):
            bounds = self.session_bounds(candidate)
            if bounds and bounds[0] <= local < bounds[1]:
                return True
        return False

    # ---------------- membership ----------------
    def is_trading_day(self, day: date) -> bool:
        day = _as_date(day)
        return day.weekday() in self.trading_weekdays and day not in self.holidays

    def why_closed(self, day: date) -> str | None:
        """Plain-language reason, for the alert that explains itself."""
        day = _as_date(day)
        if day.weekday() not in self.trading_weekdays:
            return "weekend"
        if day in self.holidays:
            return f"holiday: {self.holidays[day]}"
        return None

    # ---------------- navigation ----------------
    def previous_trading_day(self, day: date | None = None) -> date:
        day = _as_date(day or date.today())
        cursor = day - timedelta(days=1)
        for _ in range(30):
            if self.is_trading_day(cursor):
                return cursor
            cursor -= timedelta(days=1)
        raise RuntimeError(f"no trading day found in the 30 days before {day}")

    def trading_days(self, start: date, end: date) -> list[date]:
        start, end = _as_date(start), _as_date(end)
        out, cursor = [], start
        while cursor <= end:
            if self.is_trading_day(cursor):
                out.append(cursor)
            cursor += timedelta(days=1)
        return out

    # ---------------- expectations ----------------
    def delivery_deadline(self, day: date) -> datetime:
        """When the file for this trading day should exist, in exchange time."""
        return datetime.combine(_as_date(day), self.deadline, tzinfo=self.tz)

    def is_overdue(self, day: date, now: datetime | None = None) -> bool:
        now = now or datetime.now(self.tz)
        if now.tzinfo is None:
            now = now.replace(tzinfo=self.tz)
        cutoff = self.delivery_deadline(day) + timedelta(minutes=self.grace_minutes)
        return now > cutoff

    def expected_days(self, through: date | None = None, lookback: int = 7,
                      now: datetime | None = None) -> list[date]:
        """Trading days in the recent past whose file should already be here.

        The window is anchored to `now` when one is given, not to today's date.
        Getting that wrong makes the monitor untestable and, worse, makes a
        replay of last Tuesday evaluate itself against this morning.
        """
        if through is None:
            through = now.date() if now else date.today()
        through = _as_date(through)
        start = through - timedelta(days=lookback)
        return [d for d in self.trading_days(start, through) if self.is_overdue(d, now)]


def _as_date(value) -> date:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    return date.fromisoformat(str(value)[:10])


@lru_cache(maxsize=8)
def load_calendar(name: str = "mcx") -> TradingCalendar:
    path = repo_root() / "config" / "calendars" / f"{name}.yml"
    raw = yaml.safe_load(path.read_text(encoding="utf-8"))

    holidays: dict[date, str] = {}
    for key, entries in raw.items():
        if not key.startswith("holidays"):
            continue
        for entry in entries or []:
            holidays[_as_date(entry["date"])] = entry.get("name", "holiday")

    publication = raw.get("publication", {})
    session = raw.get("session", {})
    return TradingCalendar(
        name=raw.get("name", name.upper()),
        timezone=raw.get("timezone", "UTC"),
        trading_weekdays={_WEEKDAYS[d] for d in raw.get("trading_days", [])},
        holidays=holidays,
        deadline=time.fromisoformat(publication.get("deadline", "23:55")),
        grace_minutes=int(publication.get("grace_minutes", 30)),
        session_open=time.fromisoformat(session.get("open", "09:00")),
        session_close=time.fromisoformat(session.get("close", "23:30")),
    )
