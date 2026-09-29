"""The data that never arrived.

Every check in `quality.py` looks at data we received. None of them can see the
file that was never sent, and that is the failure that actually costs money: the
desk queries yesterday's numbers all morning because nothing broke loudly enough
to notice.

So this module asks the opposite question. Not "is what arrived correct?" but
"what should be here and is not?" — against the trading calendar, so weekends and
holidays never raise an alert, and a genuinely missed Tuesday always does.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, date, datetime

import pandas as pd

from .alerts import Alert, Notifier, Severity
from .calendar import TradingCalendar, load_calendar
from .config import SourceConfig
from .storage import Store


@dataclass
class Gap:
    """A trading day that is wholly or partly absent.

    The two are different incidents and need different responses, so the caller
    can tell them apart through `kind` rather than by parsing a sentence. A day
    with nothing at all is usually a delivery that never ran. A day missing one
    symbol of five is usually a delivery that ran and was incomplete, which is
    the failure the day-level count could never see: as long as GOLD arrived,
    three days of entirely absent CRUDEOIL looked like a clean week.
    """

    source: str
    trading_day: date
    deadline: datetime
    rows_found: int = 0
    missing_symbols: tuple[str, ...] = ()
    expected_symbols: tuple[str, ...] = ()

    @property
    def kind(self) -> str:
        """`nothing` when the day is empty, `partial` when only some symbols are."""
        return "nothing" if self.rows_found == 0 else "partial"

    def __str__(self) -> str:
        due = f"(due {self.deadline:%Y-%m-%d %H:%M %Z})"
        if self.kind == "nothing":
            return f"{self.source}: nothing for {self.trading_day} {due}"
        return (
            f"{self.source}: {self.trading_day} incomplete, "
            f"{len(self.missing_symbols)} of {len(self.expected_symbols)} symbols "
            f"missing ({', '.join(self.missing_symbols)}) {due}"
        )


def check_arrivals(
    cfg: SourceConfig,
    store: Store,
    *,
    notifier: Notifier | None = None,
    calendar: TradingCalendar | None = None,
    now: datetime | None = None,
    lookback_days: int | None = None,
) -> list[Gap]:
    """Which recent trading days are missing data, past their delivery deadline.

    The question is asked per symbol, because a day-level row count answers a
    question nobody has. The source config lists five MCX symbols; counting rows
    per day means three trading days of entirely absent CRUDEOIL report no gaps
    at all as long as GOLD arrived, and that is the exact shape of the failure
    this module exists to catch.

    A day still produces at most one `Gap`, because one incident should page once
    rather than five times, and the symbols that are missing are named on it.
    """
    settings = cfg.acquire.get("monitor") or {}
    if not settings or settings.get("enabled") is False:
        return []

    calendar = calendar or load_calendar(settings.get("calendar", "mcx"))
    lookback = lookback_days or int(settings.get("lookback_days", 7))
    date_column = settings.get("date_column", "trade_date")
    symbol_column = settings.get("symbol_column", "symbol")
    table = cfg.load_cfg["table"]
    # What the source says it fetches is the only statement of what "complete"
    # means. A source that does not declare one can only be asked the weaker
    # question, and is, rather than being skipped.
    wanted = tuple(cfg.acquire.get("symbols") or ())

    expected = calendar.expected_days(lookback=lookback, now=now)
    if not expected:
        return []

    if wanted:
        present = store.query(
            f"SELECT {date_column} AS d, {symbol_column} AS s, count(*) AS n "
            f"FROM {table} GROUP BY {date_column}, {symbol_column}"
        )
    else:
        present = store.query(
            f"SELECT {date_column} AS d, NULL AS s, count(*) AS n "
            f"FROM {table} GROUP BY {date_column}"
        )

    counts: dict[date, int] = {}
    arrived: dict[date, set[str]] = {}
    for _, row in present.iterrows():
        day = pd.Timestamp(row["d"]).date()
        counts[day] = counts.get(day, 0) + int(row["n"])
        if row["s"] is not None and not pd.isna(row["s"]):
            arrived.setdefault(day, set()).add(str(row["s"]))

    gaps = []
    for day in expected:
        found = counts.get(day, 0)
        missing = tuple(s for s in wanted if s not in arrived.get(day, set()))
        if found and not missing:
            continue
        gaps.append(Gap(cfg.name, day, calendar.delivery_deadline(day), found,
                        missing_symbols=missing, expected_symbols=wanted))

    if gaps:
        _record_gaps(store, cfg, gaps)
        if notifier:
            for gap in gaps:
                _alert(notifier, cfg, gap)
    elif notifier:
        notifier.send(Alert(
            Severity.INFO,
            f"{cfg.name}: all expected deliveries present",
            f"{len(expected)} trading days checked"
            + (f" for {len(wanted)} symbols" if wanted else ""),
            source=cfg.name,
        ))
    return gaps


def _alert(notifier: Notifier, cfg: SourceConfig, gap: Gap) -> None:
    """One alert per gap, named for which of the two incidents it is.

    Both page. An incomplete day is not a lesser problem than an empty one: it is
    the more dangerous of the two, because the dataset looks populated and every
    query over it silently returns a subset. They get different alertnames so
    Alertmanager groups them separately and whoever is woken up knows which they
    are looking at before opening anything.
    """
    if gap.kind == "nothing":
        notifier.missing_delivery(
            cfg.name, gap.trading_day, gap.deadline,
            "no rows loaded for a trading day past its deadline",
        )
        return
    notifier.send(Alert(
        Severity.PAGE,
        f"{cfg.name}: {gap.trading_day} delivered incomplete",
        f"{gap.rows_found} rows loaded, but {list(gap.missing_symbols)} have no "
        f"rows for that day. Expected by {gap.deadline}.",
        source=cfg.name,
        alertname="MdpDeliveryIncomplete",
        context={"trading_day": str(gap.trading_day),
                 "missing_symbols": list(gap.missing_symbols)},
    ))


def _record_gaps(store: Store, cfg: SourceConfig, gaps: list[Gap]) -> None:
    """Missing days belong in the same table as every other quality signal.

    Somebody asking "was Tuesday's data ever loaded?" should get the answer from
    one place, whether the answer is "yes", "yes but late" or "no".
    """
    store.insert(
        "quality_events",
        pd.DataFrame(
            [
                {
                    "run_id": "monitor",
                    "dataset": cfg.dataset,
                    "source": cfg.name,
                    "check_name": "expected_arrival",
                    "status": "fail",
                    "rows_in": 0,
                    "rows_out": 0,
                    "detail": str(gap),
                    "event_at": datetime.now(UTC),
                }
                for gap in gaps
            ]
        ),
    )


def review_vendor_scores(
    store: Store,
    *,
    notifier: Notifier | None = None,
    amber: float = 0.85,
) -> pd.DataFrame:
    """A source getting quietly worse is a trend, not an incident.

    Worth a warning in the morning summary rather than a page at 3am, which is
    why it is separated from the checks that block a load.
    """
    scores = store.query(
        "SELECT * FROM vendor_scores ORDER BY as_of DESC, source"
    )
    if scores.empty:
        return scores
    latest = scores.drop_duplicates(subset=["source"], keep="first")
    degraded = latest[latest["score"] < amber]
    if notifier:
        for _, row in degraded.iterrows():
            notifier.degraded_source(row["source"], float(row["score"]), row["detail"])
    return degraded
