"""Tests for the parts that would fail silently in production.

Not coverage for its own sake. Each test here stands for a way market data goes
wrong without anyone noticing: a vendor changes a column, a feed replays on
reconnect, a clock drifts, a futures roll is handled badly, or a feature quietly
looks at its own future.
"""
from __future__ import annotations

import time
from dataclasses import replace
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from mdp.config import SourceConfig
from mdp.contracts import validate
from mdp.model import make_features, walk_forward
from mdp.pipeline import run_source
from mdp.quality import run_checks, score_vendor
from mdp.reference import continuous_series, front_month_map
from mdp.storage import DuckDBStore

NOW = datetime(2026, 9, 20, 12, 0, tzinfo=UTC)


@pytest.fixture
def trades_cfg():
    return SourceConfig.by_name("binance_trades")


@pytest.fixture
def eod_cfg():
    return SourceConfig.by_name("mcx_bhavcopy")


@pytest.fixture
def store(tmp_path):
    s = DuckDBStore(tmp_path / "test.duckdb")
    s.ensure_schema()
    yield s
    s.close()


# --------------------------------------------------------------------------
# The gate
# --------------------------------------------------------------------------
def test_missing_column_is_a_format_change_not_a_bad_row(trades_cfg):
    """A vendor dropping a column must stop the load, not load nulls."""
    df = pd.DataFrame({"venue": ["binance"], "symbol": ["BTCUSDT"], "trade_id": [1],
                       "ts": [1758369600000], "price": [60000.0]})  # qty and flag gone
    result = validate(df, trades_cfg.contract, now=NOW)
    assert not result.ok
    assert result.schema_failures
    assert len(result.clean) == 0


def test_bad_rows_are_quarantined_with_a_reason(trades_cfg):
    df = pd.DataFrame(
        {
            "venue": ["binance"] * 3,
            "symbol": ["BTCUSDT"] * 3,
            "trade_id": [1, 2, 3],
            "ts": [1758369600000] * 3,
            "price": [60000.0, 0.0, 60010.0],   # a zero print
            "qty": [0.5, 0.5, 0.5],
            "is_buyer_maker": [True, False, True],
        }
    )
    result = validate(df, trades_cfg.contract, now=NOW)
    assert len(result.clean) == 2
    assert len(result.rejected) == 1
    assert "price" in result.rejected["_reject_reason"].iloc[0]


def test_replayed_trades_are_deduplicated_on_the_declared_grain(trades_cfg):
    row = {"venue": "binance", "symbol": "BTCUSDT", "trade_id": 7, "ts": 1758369600000,
           "price": 60000.0, "qty": 0.5, "is_buyer_maker": True}
    df = pd.DataFrame([row, row, row])
    result = validate(df, trades_cfg.contract, now=NOW)
    assert len(result.clean) == 1
    assert any(f["kind"] == "duplicate" for f in result.failures)


def test_a_clock_ahead_of_us_is_rejected(trades_cfg):
    future = int((NOW + timedelta(hours=1)).timestamp() * 1000)
    df = pd.DataFrame([{"venue": "binance", "symbol": "BTCUSDT", "trade_id": 1,
                        "ts": future, "price": 60000.0, "qty": 0.1,
                        "is_buyer_maker": False}])
    result = validate(df, trades_cfg.contract, now=NOW)
    assert len(result.clean) == 0
    assert "future" in result.rejected["_reject_reason"].iloc[0]


def test_impossible_bar_fails_the_cross_field_rule(eod_cfg):
    df = pd.DataFrame([{"exchange": "MCX", "symbol": "GOLD", "expiry": "2026-10-28",
                        "trade_date": "2026-09-18", "open": 100.0, "high": 90.0,
                        "low": 95.0, "close": 99.0, "settle": 99.0, "volume": 10.0,
                        "open_interest": 5.0}])
    result = validate(df, eod_cfg.contract, now=NOW)
    assert len(result.clean) == 0
    assert "high_is_highest" in result.rejected["_reject_reason"].iloc[0]


# --------------------------------------------------------------------------
# Checks and scoring
# --------------------------------------------------------------------------
def test_stale_data_blocks_publication(trades_cfg):
    old = int((NOW - timedelta(hours=6)).timestamp() * 1000)
    df = pd.DataFrame([{"venue": "binance", "symbol": "BTCUSDT", "trade_id": 1, "ts": old,
                        "price": 60000.0, "qty": 0.1, "is_buyer_maker": False}])
    result = validate(df, trades_cfg.contract, now=NOW)
    report = run_checks(trades_cfg, result, now=NOW)
    assert not report.publishable
    assert [c.name for c in report.blocking] == ["freshness"]


def test_vendor_score_falls_when_rows_are_rejected(trades_cfg):
    good = [{"venue": "binance", "symbol": "BTCUSDT", "trade_id": i,
             "ts": int(NOW.timestamp() * 1000), "price": 60000.0 + i, "qty": 0.1,
             "is_buyer_maker": False} for i in range(1, 10)]
    bad = dict(good[0]) | {"trade_id": 99, "price": 0.0}
    clean_score = score_vendor(
        trades_cfg, validate(pd.DataFrame(good), trades_cfg.contract, now=NOW),
        run_checks(trades_cfg, validate(pd.DataFrame(good), trades_cfg.contract, now=NOW), now=NOW),
        as_of=NOW,
    )
    dirty = validate(pd.DataFrame(good + [bad]), trades_cfg.contract, now=NOW)
    dirty_score = score_vendor(trades_cfg, dirty, run_checks(trades_cfg, dirty, now=NOW), as_of=NOW)
    assert dirty_score["score"] < clean_score["score"]
    assert dirty_score["accuracy"] < 1.0


# --------------------------------------------------------------------------
# Reference data and the roll
# --------------------------------------------------------------------------
def _two_contract_frame() -> pd.DataFrame:
    days = pd.bdate_range("2026-01-02", periods=40)
    rows = []
    for i, d in enumerate(days):
        for expiry, base, vol in (("2026-01-28", 100.0, 1000 - i * 30),
                                  ("2026-02-25", 102.0, 100 + i * 30)):
            rows.append({"exchange": "MCX", "symbol": "GOLD", "expiry": expiry,
                         "trade_date": d.date().isoformat(), "open": base + i * 0.1,
                         "high": base + i * 0.1, "low": base + i * 0.1,
                         "close": base + i * 0.1, "settle": base + i * 0.1,
                         "volume": float(max(vol, 1)), "open_interest": 100.0})
    return pd.DataFrame(rows)


def test_front_month_never_rolls_backwards():
    chosen = front_month_map(_two_contract_frame(), method="volume")
    expiries = chosen.sort_values("trade_date")["expiry"].tolist()
    assert expiries == sorted(expiries), "a roll must happen once, not flip back and forth"


def test_ratio_adjustment_removes_the_roll_jump():
    eod = _two_contract_frame()
    raw = continuous_series(eod, symbol="GOLD", adjust="none")
    adj = continuous_series(eod, symbol="GOLD", adjust="ratio")
    raw_jump = (raw.loc[raw["is_roll"], "close"].iloc[0]
                / raw["close"].shift(1)[raw["is_roll"]].iloc[0])
    adj_returns = adj["adj_close"].pct_change().abs()
    assert abs(raw_jump - 1) > 0.01          # the raw series really does jump
    assert adj_returns.max() < 0.01          # the adjusted one does not


def test_as_of_gives_the_series_as_it_stood_then():
    eod = _two_contract_frame()
    early = continuous_series(eod, symbol="GOLD", adjust="ratio", as_of="2026-01-20")
    assert early["trade_date"].max() <= pd.Timestamp("2026-01-20")


# --------------------------------------------------------------------------
# The model
# --------------------------------------------------------------------------
def test_features_never_see_their_own_future():
    """The one test that matters in the model layer.

    Change any price after row t and the features at row t must not move. If this
    fails, every backtest result built on it is fiction.
    """
    rng = np.random.default_rng(0)
    dates = pd.bdate_range("2024-01-01", periods=300)
    px = 100 * np.exp(np.cumsum(rng.normal(0, 0.01, len(dates))))
    base = pd.DataFrame({"trade_date": dates, "adj_close": px})

    tampered = base.copy()
    tampered.loc[200:, "adj_close"] *= 1.5    # rewrite the future

    f1 = make_features(base)
    f2 = make_features(tampered)
    cols = f1.attrs["feature_cols"]
    pd.testing.assert_frame_equal(
        f1.loc[:199, cols].reset_index(drop=True),
        f2.loc[:199, cols].reset_index(drop=True),
    )
    # and the target, which is allowed to look forward, must differ at the boundary:
    # row 199's label is row 200's return, and row 200 is what we tampered with
    assert f1.loc[199, "target"] != pytest.approx(f2.loc[199, "target"])
    assert f1.loc[198, "target"] == pytest.approx(f2.loc[198, "target"])


def test_walk_forward_trains_only_on_the_past():
    rng = np.random.default_rng(1)
    dates = pd.bdate_range("2023-01-01", periods=500)
    px = 100 * np.exp(np.cumsum(rng.normal(0, 0.01, len(dates))))
    feats = make_features(pd.DataFrame({"trade_date": dates, "adj_close": px}))
    result = walk_forward(feats, min_train=250, test_size=21)
    assert result.folds
    for fold in result.folds:
        assert fold.train_end < fold.test_start
    assert "skill_vs_baseline" in result.metrics()


# --------------------------------------------------------------------------
# End to end
# --------------------------------------------------------------------------
def test_pipeline_runs_end_to_end_offline(store, eod_cfg):
    summary = run_source(eod_cfg, store, synthetic=True, days=120, now=None)
    assert summary["rows_acquired"] > 0
    assert summary["rows_loaded"] == summary["rows_clean"]
    assert summary["rows_quarantined"] > 0      # the generator emits real defects
    loaded = store.query("SELECT count(*) AS n FROM eod_bars")["n"].iloc[0]
    assert loaded == summary["rows_loaded"]
    events = store.query("SELECT * FROM quality_events")
    assert len(events) > 0, "every load leaves an audit trail, even a clean one"


def test_bars_match_the_trades_they_came_from(store, trades_cfg):
    run_source(trades_cfg, store, synthetic=True)
    trades = store.query(
        "SELECT DISTINCT ON (venue, symbol, trade_id) symbol, ts, price, qty FROM trades"
    )
    bars = store.bars("BTCUSDT", None, None, "1m", "binance")
    one = trades[trades["symbol"] == "BTCUSDT"]
    assert abs(bars["volume"].sum() - one["qty"].sum()) < 1e-6
    assert bars["trades"].sum() == len(one)
    assert bars["high"].max() == pytest.approx(one["price"].max())
    assert bars["low"].min() == pytest.approx(one["price"].min())


# --------------------------------------------------------------------------
# Event-driven ingestion
# --------------------------------------------------------------------------
def _vendor_csv(path: Path, *, days: int = 5, symbol: str = "GOLD",
                end: date | None = None) -> Path:
    """A file in the exchange's own column names, as it would actually land."""
    end = end or date.today()
    rows = []
    for i in range(days):
        d = end - timedelta(days=i)
        for k, exp in enumerate((end + timedelta(days=20), end + timedelta(days=50))):
            base = 72000.0 + i * 10 + k * 100
            rows.append({"Symbol": symbol, "ExpiryDate": exp.isoformat(),
                         "Date": d.isoformat(), "Open": base, "High": base * 1.002,
                         "Low": base * 0.998, "Close": base * 1.001,
                         "SettlePrice": base * 1.001, "Volume": 1000.0 + i,
                         "OpenInterest": 5000.0})
    path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(rows).to_csv(path, index=False)
    return path


@pytest.fixture
def drop_source(tmp_path, monkeypatch, eod_cfg):
    """An mcx source whose drop directory and ledger are inside tmp_path."""
    from mdp import watch as watch_mod

    monkeypatch.setattr(watch_mod, "LEDGER", tmp_path / "processed.json")
    drop_dir = tmp_path / "incoming"
    drop_dir.mkdir()
    acquire = dict(eod_cfg.acquire)
    acquire["drop"] = dict(acquire.get("drop", {}))
    acquire["drop"]["dir"] = str(drop_dir)
    cfg = replace(eod_cfg, acquire=acquire)
    return cfg, drop_dir


def test_a_dropped_file_is_ingested_on_arrival_with_measured_latency(drop_source, store):
    from mdp.watch import watch

    cfg, drop_dir = drop_source
    _vendor_csv(drop_dir / "BHAVCOPY_20260925.csv")

    handled = watch([cfg], store, once=True)

    assert len(handled) == 1
    delivery = handled[0]
    assert delivery.published
    assert delivery.rows_loaded == 10
    assert delivery.total_latency > 0          # it is measured, not assumed
    assert delivery.processing_latency < 30

    # and the latency is queryable beside the other checks, not only in a log
    events = store.query(
        "SELECT * FROM quality_events WHERE check_name = 'delivery_latency'"
    )
    assert len(events) == 1
    assert "from file write to queryable" in events["detail"].iloc[0]


def test_the_vendors_column_names_are_mapped_from_config(drop_source, store):
    """The file says Symbol and ExpiryDate; the platform speaks its own schema."""
    from mdp.watch import watch

    cfg, drop_dir = drop_source
    _vendor_csv(drop_dir / "vendor.csv")
    watch([cfg], store, once=True)

    loaded = store.query("SELECT * FROM eod_bars")
    assert {"exchange", "symbol", "expiry", "trade_date"} <= set(loaded.columns)
    assert loaded["exchange"].unique().tolist() == ["MCX"]


def test_the_same_delivery_twice_loads_once(drop_source, store):
    from mdp.watch import watch

    cfg, drop_dir = drop_source
    _vendor_csv(drop_dir / "first.csv")
    first = watch([cfg], store, once=True)
    second = watch([cfg], store, once=True)

    assert len(first) == 1 and len(second) == 0


def test_a_resend_under_a_new_name_loads_once(drop_source, store):
    """Vendors resend yesterday's file with today's name. Identity is content."""
    from mdp.watch import watch

    cfg, drop_dir = drop_source
    original = _vendor_csv(drop_dir / "BHAVCOPY_A.csv")
    watch([cfg], store, once=True)

    (drop_dir / "BHAVCOPY_B.csv").write_bytes(original.read_bytes())
    again = watch([cfg], store, once=True)

    assert len(again) == 0
    rows = store.query("SELECT count(*) AS n FROM eod_bars")["n"].iloc[0]
    assert rows == 10


def test_a_file_still_being_written_is_left_alone(drop_source, store):
    """A 400MB upload in progress looks exactly like a small complete file."""
    import threading

    from mdp.watch import watch

    cfg, drop_dir = drop_source
    cfg.acquire["drop"]["settle_seconds"] = 0.4
    path = _vendor_csv(drop_dir / "partial.csv")

    def keep_writing():
        time.sleep(0.15)
        with path.open("a") as handle:
            handle.write("GOLD,2026-12-30,2026-09-25,1,1,1,1,1,1,1\n")

    writer = threading.Thread(target=keep_writing)
    writer.start()
    handled = watch([cfg], store, once=True)
    writer.join()

    assert handled == []                       # skipped while in flight
    assert store.query("SELECT count(*) AS n FROM eod_bars")["n"].iloc[0] == 0
    assert len(watch([cfg], store, once=True)) == 1   # picked up once it settles


def test_a_blocked_delivery_is_retried_not_forgotten(drop_source, store):
    """Stale data blocks the load. The file must stay eligible for another pass,
    so a corrected re-delivery works without anyone editing a ledger by hand."""
    from mdp.watch import watch

    cfg, drop_dir = drop_source
    _vendor_csv(drop_dir / "stale.csv", end=date.today() - timedelta(days=30))

    first = watch([cfg], store, once=True)
    assert first and not first[0].published
    assert first[0].blocked_by == ["freshness"]
    assert store.query("SELECT count(*) AS n FROM eod_bars")["n"].iloc[0] == 0

    second = watch([cfg], store, once=True)
    assert len(second) == 1                    # tried again, not silently skipped


def test_one_unreadable_file_does_not_stop_the_watcher(drop_source, store):
    """A corrupt delivery is one vendor's bad morning, not an outage."""
    from mdp.watch import watch

    cfg, drop_dir = drop_source
    # ragged rows: pandas cannot tokenise this at all, unlike a merely wrong file,
    # which the contract gate would catch further down
    (drop_dir / "corrupt.csv").write_text("a,b,c\n1,2\n3,4,5,6,7,8,9\n")
    _vendor_csv(drop_dir / "good.csv")

    handled = watch([cfg], store, once=True)

    by_name = {d.path.name: d for d in handled}
    assert by_name["corrupt.csv"].blocked_by == ["acquisition_error"]
    assert by_name["good.csv"].published          # the good file still loaded
    failures = store.query(
        "SELECT * FROM quality_events WHERE check_name = 'acquisition'"
    )
    assert "could not read corrupt.csv" in failures["detail"].iloc[0]


def test_a_file_that_keeps_failing_stops_being_retried(drop_source, store):
    """Retrying forever is noise. After the budget, it needs a person."""
    from mdp.watch import watch

    cfg, drop_dir = drop_source
    cfg.acquire["drop"]["max_attempts"] = 2
    (drop_dir / "always_bad.csv").write_text("a,b,c\n1,2\n3,4,5,6,7\n")

    assert len(watch([cfg], store, once=True)) == 1
    assert len(watch([cfg], store, once=True)) == 1
    assert watch([cfg], store, once=True) == []   # budget spent, left for a human


# --------------------------------------------------------------------------
# Trading calendar, automated fetch, and the data that never arrived
# --------------------------------------------------------------------------
def test_the_calendar_knows_when_the_market_is_shut():
    from mdp.calendar import load_calendar

    cal = load_calendar("mcx")
    assert cal.is_trading_day(date(2026, 9, 25))                 # a Friday
    assert not cal.is_trading_day(date(2026, 9, 26))             # Saturday
    assert cal.why_closed(date(2026, 9, 26)) == "weekend"
    assert not cal.is_trading_day(date(2026, 10, 2))             # Gandhi Jayanti
    assert "holiday" in cal.why_closed(date(2026, 10, 2))
    assert cal.previous_trading_day(date(2026, 9, 28)) == date(2026, 9, 25)


def test_a_delivery_is_only_late_once_its_deadline_has_passed():
    from zoneinfo import ZoneInfo

    from mdp.calendar import load_calendar

    cal = load_calendar("mcx")
    ist = ZoneInfo("Asia/Kolkata")
    friday = date(2026, 9, 25)
    assert not cal.is_overdue(friday, datetime(2026, 9, 25, 22, 0, tzinfo=ist))
    assert cal.is_overdue(friday, datetime(2026, 9, 26, 9, 0, tzinfo=ist))


def test_fetch_lands_a_file_the_watcher_then_picks_up(drop_source, store):
    """The whole point: nobody drops anything by hand."""
    from mdp.calendar import load_calendar
    from mdp.fetch import fetch_to_drop
    from mdp.watch import watch

    cfg, drop_dir = drop_source
    # The most recent session, whenever this runs: a fixed date would pass
    # freshness on the day it was written and fail a week later.
    result = fetch_to_drop(cfg, load_calendar("mcx").previous_trading_day(),
                           synthetic=True)

    assert result.fetched
    assert result.path.parent == drop_dir
    assert not list(drop_dir.glob("*.part"))     # written atomically

    deliveries = watch([cfg], store, once=True)
    assert len(deliveries) == 1 and deliveries[0].published


def test_fetch_skips_days_the_market_was_shut(drop_source):
    from mdp.calendar import load_calendar
    from mdp.fetch import fetch_to_drop

    cfg, _ = drop_source
    # Both dates are deliberately in the PAST. A fixed future date makes this
    # test's meaning depend on the day it runs: once the date passes the
    # "in the future" guard it exercises the calendar, and before that it does
    # not, which is how the original version of this test came to assert
    # something it was not testing.
    cal = load_calendar("mcx")
    today = date.today()
    saturday = next(
        d for d in (today - timedelta(days=n) for n in range(1, 10))
        if d.weekday() == 5
    )
    holiday = max(d for d in cal.holidays if d < today)

    saturday_result = fetch_to_drop(cfg, saturday, synthetic=True)
    holiday_result = fetch_to_drop(cfg, holiday, synthetic=True)

    assert not saturday_result.fetched
    assert saturday_result.skipped_reason == "weekend"
    assert not holiday_result.fetched
    assert holiday_result.skipped_reason == cal.why_closed(holiday)
    assert holiday_result.skipped_reason not in ("weekend", "in the future")


def test_fetch_refuses_to_invent_the_future(drop_source):
    """A backfill range running past today is a normal mistake. Generating data
    for days that have not happened is the worst possible response to it."""
    from mdp.fetch import fetch_to_drop

    cfg, _ = drop_source
    ahead = fetch_to_drop(cfg, date.today() + timedelta(days=3), synthetic=True)
    assert not ahead.fetched
    assert ahead.skipped_reason == "in the future"


def test_a_backfill_walks_calendar_days_and_reports_what_it_skipped(drop_source):
    from mdp.fetch import fetch_range

    cfg, _ = drop_source
    # a window entirely in the past, so the result does not depend on the clock
    results = fetch_range(cfg, date(2026, 9, 7), date(2026, 9, 13), synthetic=True)

    assert len(results) == 7                              # every calendar day walked
    reasons = {r.skipped_reason for r in results if not r.fetched}
    assert reasons == {"weekend"}
    assert sum(r.fetched for r in results) == 5           # Mon to Fri


def test_the_monitor_finds_the_day_that_never_arrived(drop_source, store):
    """No check in quality.py can see a file that was never sent. This can."""
    from mdp.alerts import Notifier, Severity
    from mdp.fetch import fetch_to_drop
    from mdp.monitor import check_arrivals
    from mdp.watch import watch

    cfg, _ = drop_source
    cal = load_calendar_for_test()
    recent = [d for d in cal.trading_days(date.today() - timedelta(days=9), date.today())
              if cal.is_overdue(d)]
    assert len(recent) >= 3, "need a few overdue trading days for this test"

    # everything except the middle one
    missing_day = recent[len(recent) // 2]
    for day in recent:
        if day != missing_day:
            fetch_to_drop(cfg, day, synthetic=True)
    watch([cfg], store, once=True, mode="backfill")

    notifier = Notifier(sinks=[])
    gaps = check_arrivals(cfg, store, notifier=notifier, lookback_days=9)

    assert [g.trading_day for g in gaps] == [missing_day]
    assert any(a.severity == Severity.PAGE for a in notifier.sent)
    events = store.query(
        "SELECT * FROM quality_events WHERE check_name = 'expected_arrival'"
    )
    assert len(events) == 1


def test_the_monitor_is_silent_at_the_weekend(drop_source, store):
    """Alerting on a Saturday is how people stop reading alerts."""
    from mdp.monitor import check_arrivals

    cfg, _ = drop_source
    cal = load_calendar_for_test()
    # a window containing only a weekend
    saturday = date(2026, 9, 26)
    gaps = check_arrivals(
        cfg, store, lookback_days=1,
        now=datetime(2026, 9, 27, 12, 0, tzinfo=cal.tz),
    )
    assert gaps == []
    assert not cal.is_trading_day(saturday)


def test_freshness_does_not_block_a_backfill(drop_source, store):
    """The check that is right for the daily run is wrong for history."""
    from mdp.fetch import fetch_to_drop
    from mdp.watch import watch

    cfg, _ = drop_source
    old_day = date(2026, 9, 8)
    fetch_to_drop(cfg, old_day, synthetic=True)

    live = watch([cfg], store, once=True)
    assert live and not live[0].published and live[0].blocked_by == ["freshness"]

    backfilled = watch([cfg], store, once=True, mode="backfill")
    assert backfilled and backfilled[0].published


def load_calendar_for_test():
    from mdp.calendar import load_calendar

    return load_calendar("mcx")


# --------------------------------------------------------------------------
# Cursors: fetch from where we got to, not from a guess
# --------------------------------------------------------------------------
def test_the_cursor_only_moves_after_a_successful_publish(store, trades_cfg):
    from mdp import cursor as cursor_store
    from mdp.pipeline import run_source

    assert cursor_store.read(store, trades_cfg.name).is_start

    ok = run_source(trades_cfg, store, synthetic=True)
    assert ok["rows_loaded"] > 0
    moved = cursor_store.read(store, trades_cfg.name)
    assert moved.position_ts is not None
    assert moved.position_id is not None

    # a blocked load must not advance it, or the skipped data is lost forever
    stale_end = datetime(2020, 1, 1, tzinfo=UTC)
    blocked = run_source(trades_cfg, store, synthetic=True, end=stale_end)
    assert blocked["blocked_by"]
    assert cursor_store.read(store, trades_cfg.name).position_ts == moved.position_ts


def test_the_window_starts_from_the_cursor_with_a_deliberate_overlap():
    from mdp.cursor import Cursor

    now = datetime(2026, 9, 25, 12, 0, tzinfo=UTC)
    cold = Cursor("s").window(default_minutes=60, overlap_seconds=5, now=now)
    assert (now - cold[0]).total_seconds() == 3600

    warm = Cursor("s", position_ts=now - timedelta(minutes=2))
    start, end = warm.window(default_minutes=60, overlap_seconds=5, now=now)
    assert (now - start).total_seconds() == 125      # 2 minutes plus the overlap
    assert end == now


def test_incremental_fetch_only_takes_what_is_new(store, trades_cfg):
    from mdp.fetch import fetch_incremental

    first = fetch_incremental(trades_cfg, store, synthetic=True)
    second = fetch_incremental(trades_cfg, store, synthetic=True)

    assert first["rows_loaded"] > second["rows_loaded"]
    assert second["cursor_before"] is not None


# --------------------------------------------------------------------------
# Entitlements
# --------------------------------------------------------------------------
def test_a_consumer_outside_the_licence_is_refused_and_logged(store):
    from mdp.entitlements import NotEntitled
    from mdp.serve import MarketData

    MarketData(store=store, consumer="research").get_eod("CRUDEOIL")
    with pytest.raises(NotEntitled):
        MarketData(store=store, consumer="external").get_eod("CRUDEOIL")

    log = store.query("SELECT * FROM access_log ORDER BY event_at")
    assert set(log["consumer"]) == {"research", "external"}
    assert list(log.sort_values("consumer")["allowed"]) == [False, True]


def test_an_unknown_dataset_is_denied_rather_than_allowed():
    """A dataset nobody has considered the licence for is the one to stop."""
    from mdp.entitlements import load_policy

    allowed, detail = load_policy().allows("research", "some_new_feed")
    assert not allowed and "no entitlement rule" in detail


# --------------------------------------------------------------------------
# Contract specifications, point in time
# --------------------------------------------------------------------------
def test_the_lot_size_is_the_one_that_applied_on_the_day(store):
    from mdp.pipeline import load_contract_specs
    from mdp.serve import MarketData

    load_contract_specs(store)
    md = MarketData(store=store)

    assert md.get_specs("CRUDEOIL")["lot_size"] == 100
    assert md.get_specs("CRUDEOIL", as_of="2017-06-30")["lot_size"] == 50
    # same price, different notional, because the contract changed
    assert md.notional("CRUDEOIL", 6200) == 620_000
    assert md.notional("CRUDEOIL", 6200, as_of="2017-06-30") == 310_000


# --------------------------------------------------------------------------
# Schema changes are a decision, not an accident
# --------------------------------------------------------------------------
def test_a_contract_version_change_stops_the_load_until_accepted(store, eod_cfg):
    from dataclasses import replace as dc_replace

    from mdp.pipeline import run_source

    first = run_source(eod_cfg, store, synthetic=True, days=30)
    assert first["published"]

    bumped = dc_replace(eod_cfg, contract=dc_replace(eod_cfg.contract, schema_version="2.0"))
    blocked = run_source(bumped, store, synthetic=True, days=30)
    assert blocked["blocked_by"] == ["schema_version"]

    accepted = run_source(bumped, store, synthetic=True, days=30, accept_schema_change=True)
    assert accepted["published"]


# --------------------------------------------------------------------------
# Restore: the backup claim, actually exercised
# --------------------------------------------------------------------------
def test_the_database_can_be_rebuilt_from_the_landing_zone(store, eod_cfg, monkeypatch, tmp_path):
    from mdp import landing
    from mdp.pipeline import restore_from_landing, run_source

    monkeypatch.setattr(landing, "repo_root", lambda: tmp_path)

    loaded = run_source(eod_cfg, store, synthetic=True, days=40)
    original = store.query("SELECT count(*) AS n FROM eod_bars")["n"].iloc[0]
    assert original == loaded["rows_loaded"]

    store.query("DELETE FROM eod_bars")
    assert store.query("SELECT count(*) AS n FROM eod_bars")["n"].iloc[0] == 0

    result = restore_from_landing(eod_cfg, store)
    assert result["rows_restored"] == original
    assert store.query("SELECT count(*) AS n FROM eod_bars")["n"].iloc[0] == original


def test_a_restore_still_goes_through_the_contract_gate(store, eod_cfg, monkeypatch, tmp_path):
    """Otherwise a restore is an efficient way to reinstate bad data."""
    from mdp import landing
    from mdp.pipeline import restore_from_landing, run_source

    monkeypatch.setattr(landing, "repo_root", lambda: tmp_path)
    run_source(eod_cfg, store, synthetic=True, days=40)     # generator adds bad rows
    result = restore_from_landing(eod_cfg, store)
    assert result["rows_rejected"] > 0
    assert result["rows_restored"] == result["rows_in_landing"] - result["rows_rejected"]


# --------------------------------------------------------------------------
# Secrets and metrics
# --------------------------------------------------------------------------
def test_credentials_come_from_the_environment_not_the_config(monkeypatch):
    from mdp.config import redact, resolve_secrets

    monkeypatch.setenv("MDP_TEST_TOKEN", "s3cret")
    resolved = resolve_secrets({"api_key": "${env:MDP_TEST_TOKEN}", "url": "https://x"})
    assert resolved["api_key"] == "s3cret"
    assert redact(resolved)["api_key"] == "***redacted***"
    assert redact(resolved)["url"] == "https://x"

    with pytest.raises(KeyError):
        resolve_secrets("${env:MDP_DEFINITELY_NOT_SET}")
    assert resolve_secrets("${env:MDP_DEFINITELY_NOT_SET:fallback}") == "fallback"


def test_metrics_come_out_in_a_format_a_scraper_understands(store, eod_cfg):
    from mdp.metrics import render
    from mdp.pipeline import run_source

    run_source(eod_cfg, store, synthetic=True, days=30)
    text = render(store)

    assert "# TYPE mdp_rows_total gauge" in text
    assert 'mdp_rows_total{dataset="eod_bars"' in text
    assert "mdp_vendor_score" in text
    for line in text.splitlines():
        if not line.startswith("#") and line.strip():
            assert len(line.rsplit(" ", 1)) == 2      # name{labels} value


# --------------------------------------------------------------------------
# Arrival window: telling "late" apart from "never"
# --------------------------------------------------------------------------
def test_a_file_that_arrives_in_the_window_is_on_time(drop_source, store):
    from mdp.alerts import Notifier
    from mdp.arrival import await_arrival

    cfg, _ = drop_source
    notifier = Notifier(sinks=[])
    result = await_arrival(
        cfg, store, date.today(), synthetic=True, notifier=notifier,
        max_attempts=1, sleep=False,
    )
    # On a closed day the window is never opened, which is itself correct.
    assert result.status in ("on_time", "late", "skipped")
    if result.status == "on_time":
        assert result.attempts == 1
        assert result.rows_loaded > 0
        assert not notifier.sent          # nothing to say when it just works


def test_a_late_file_is_loaded_and_the_delay_is_recorded(drop_source, store):
    """Late is not missing. It loads, and the delay becomes evidence."""
    from mdp.alerts import Notifier, Severity
    from mdp.arrival import await_arrival
    from mdp.calendar import load_calendar

    cfg, _ = drop_source
    cal = load_calendar("mcx")
    yesterday = cal.previous_trading_day()

    notifier = Notifier(sinks=[])
    result = await_arrival(
        cfg, store, yesterday, synthetic=True, notifier=notifier,
        max_attempts=1, sleep=False,
    )

    assert result.status == "late"
    assert result.rows_loaded > 0             # still loaded, because it is here
    assert result.delay_seconds > 0
    assert any(a.severity == Severity.WARN for a in notifier.sent)

    events = store.query(
        "SELECT * FROM quality_events WHERE check_name = 'publication_timeliness'"
    )
    assert events["status"].iloc[0] == "warn"


def test_a_file_that_never_comes_pages_somebody(drop_source, store, monkeypatch):
    from mdp import arrival as arrival_mod
    from mdp.alerts import Notifier, Severity
    from mdp.fetch import FetchResult

    cfg, _ = drop_source
    # A trading day, not "today": today is a Saturday one week in four and the
    # window is never opened on a closed market.
    trading_day = load_calendar_for_test().previous_trading_day()
    monkeypatch.setattr(
        arrival_mod, "fetch_to_drop",
        lambda *a, **k: FetchResult(cfg.name, trading_day, None, 0,
                                    skipped_reason="nothing published"),
    )
    notifier = Notifier(sinks=[])
    # A window still open: it should keep trying until the budget is spent.
    result = arrival_mod.await_arrival(
        cfg, store, trading_day, synthetic=True, notifier=notifier,
        max_attempts=3, max_wait_minutes=10_000, sleep=False,
    )

    assert result.status == "missing"
    assert result.attempts == 3               # it really did keep trying
    assert any(a.severity == Severity.PAGE for a in notifier.sent)
    events = store.query(
        "SELECT * FROM quality_events WHERE check_name = 'publication_timeliness'"
    )
    assert events["status"].iloc[0] == "fail"

    # And a window that closed long ago gives up at once rather than burning the
    # budget on a file that is never coming.
    closed = arrival_mod.await_arrival(
        cfg, store, trading_day, synthetic=True, notifier=Notifier(sinks=[]),
        max_attempts=3, max_wait_minutes=1, sleep=False,
    )
    assert closed.status == "missing" and closed.attempts == 1


def test_the_window_is_not_opened_on_a_closed_day(drop_source, store):
    from mdp.arrival import await_arrival

    cfg, _ = drop_source
    result = await_arrival(cfg, store, date(2026, 10, 2), synthetic=True,
                           max_attempts=1, sleep=False)
    assert result.status == "skipped"
    assert "Gandhi" in result.detail


# --------------------------------------------------------------------------
# The as-of join: the tick path's consumer, and the easiest place to leak
# --------------------------------------------------------------------------
def _seed_trades(store, prices_at_seconds: dict[int, float], *, base=None) -> datetime:
    """Trades at known offsets, so the expected answer is arithmetic not luck."""
    base = base or datetime(2026, 9, 20, 10, 0, tzinfo=UTC)
    rows = [
        {"venue": "binance", "symbol": "TESTUSD", "trade_id": i + 1,
         "ts": base + timedelta(seconds=offset), "price": price, "qty": 1.0,
         "is_buyer_maker": False}
        for i, (offset, price) in enumerate(sorted(prices_at_seconds.items()))
    ]
    store.insert("trades", pd.DataFrame(rows))
    return base


def test_the_as_of_match_is_the_last_trade_at_or_before(store):
    from mdp.research import as_of_prices

    base = _seed_trades(store, {0: 100.0, 10: 101.0, 20: 102.0})
    out = as_of_prices(
        store, "TESTUSD",
        [base + timedelta(seconds=5), base + timedelta(seconds=10),
         base + timedelta(seconds=19)],
    )
    assert list(out["price"]) == [100.0, 101.0, 101.0]
    # exactly at a trade's timestamp, that trade counts: at-or-before, not before
    assert out.iloc[1]["staleness_seconds"] == 0.0


def test_a_trade_after_the_query_time_cannot_change_the_answer(store):
    """The test that matters. If this fails, every backtest built on it is fiction."""
    from mdp.research import as_of_prices

    base = _seed_trades(store, {0: 100.0, 10: 101.0})
    query = [base + timedelta(seconds=12)]
    before = as_of_prices(store, "TESTUSD", query)

    # a print arrives 200ms after the decision moment, which a 'nearest' join
    # would happily return
    store.insert("trades", pd.DataFrame([{
        "venue": "binance", "symbol": "TESTUSD", "trade_id": 999,
        "ts": base + timedelta(seconds=12.2), "price": 500.0, "qty": 1.0,
        "is_buyer_maker": False,
    }]))
    after = as_of_prices(store, "TESTUSD", query)

    assert before["price"].iloc[0] == 101.0
    assert after["price"].iloc[0] == 101.0        # unchanged: no leakage


def test_a_stale_price_comes_back_empty_rather_than_plausible(store):
    """Forty minutes old dressed as current is worse than a hole."""
    from mdp.research import as_of_prices

    base = _seed_trades(store, {0: 100.0})
    out = as_of_prices(
        store, "TESTUSD", [base + timedelta(minutes=40)],
        max_staleness_seconds=60,
    )
    assert len(out) == 1                          # the row survives
    assert pd.isna(out["price"].iloc[0])          # the price does not
    assert bool(out["stale"].iloc[0])
    assert out["staleness_seconds"].iloc[0] == pytest.approx(2400)


def test_a_timestamp_before_any_trade_has_no_price(store):
    from mdp.research import as_of_prices

    base = _seed_trades(store, {60: 100.0})
    out = as_of_prices(store, "TESTUSD", [base])
    assert len(out) == 1
    assert pd.isna(out["price"].iloc[0])
    # A row with no match at all must be flagged stale. It was not: `NaN > x` is
    # False rather than NaN, so the guard meant to catch this never fired and a
    # consumer filtering on `~stale` kept the no-data row as a good one.
    assert bool(out["stale"].iloc[0])


def test_the_volatility_window_is_closed_on_the_right(store, trades_cfg):
    """The bar CONTAINING the query time must not enter the window.

    This test used to put the query exactly on a bar boundary, which is the one
    timestamp where the correct and the incorrect filter agree, so it passed
    against a version that leaked. Real decision times do not land on
    boundaries, and this repo's own demo offsets them by design, so the query
    here is offset too.
    """
    from mdp.pipeline import run_source
    from mdp.research import with_features

    run_source(trades_cfg, store, synthetic=True)
    bars = store.bars("BTCUSDT", None, None, "1m", "binance")
    assert len(bars) > 15

    buckets = pd.to_datetime(bars["bucket"], utc=True)
    boundary = buckets.iloc[12]
    mid_bar = boundary + timedelta(seconds=17.5)
    out = with_features(store, "BTCUSDT", [boundary, mid_bar], vol_window_minutes=10)

    # The rule is full containment: a bar counts only if it both opens at or
    # after the window opens and closes at or before the decision time.
    #
    # On the boundary that is the ten bars before it. Seventeen seconds later
    # the window has slid forward, so the oldest bar now starts before it opens
    # and drops out, and the bar containing the decision has not closed yet and
    # has not come in. Nine is the right answer, and the bar that would make it
    # ten is exactly the one that contains trades from after the decision.
    assert out["bars_in_window"].iloc[0] == 10
    assert out["bars_in_window"].iloc[1] == 9

    # The invariant, stated directly rather than inferred from a count.
    used = bars[(buckets >= mid_bar - timedelta(minutes=10))
                & (buckets + timedelta(minutes=1) <= mid_bar)]
    assert len(used) == out["bars_in_window"].iloc[1]
    assert (pd.to_datetime(used["bucket"], utc=True) + timedelta(minutes=1)
            <= mid_bar).all()
    # And the bar the old code let through is provably in the data and provably
    # not in the window.
    containing = bars[(buckets <= mid_bar) & (buckets + timedelta(minutes=1) > mid_bar)]
    assert len(containing) == 1
    assert containing.index[0] not in used.index


def test_a_decision_time_gets_the_same_answer_alone_as_in_a_batch(store, trades_cfg):
    """The leak was batch-dependent, which is the worst way for one to behave.

    `store.bars` truncated its final bucket, so the last row of a request
    accidentally got the right window and every other row did not. The same
    decision time then produced different volatility depending on which other
    timestamps were asked for alongside it. Measured on the demo data, one
    timestamp differed by a factor of 52 between the two calls.
    """
    from mdp.pipeline import run_source
    from mdp.research import with_features

    run_source(trades_cfg, store, synthetic=True)
    bars = store.bars("BTCUSDT", None, None, "1m", "binance")
    buckets = pd.to_datetime(bars["bucket"], utc=True)
    times = [buckets.iloc[i] + timedelta(seconds=17.5) for i in (14, 18, 22)]

    batched = with_features(store, "BTCUSDT", times, vol_window_minutes=10)
    for i, ts in enumerate(times):
        alone = with_features(store, "BTCUSDT", [ts], vol_window_minutes=10)
        assert alone["realised_vol"].iloc[0] == pytest.approx(
            batched["realised_vol"].iloc[i]
        ), f"{ts} answered differently alone than in a batch"


def test_a_partial_bucket_is_dropped_rather_than_returned_half_built(store):
    """A bar built from part of its interval and labelled as a whole bar is
    worse than a missing one, because the missing one is visible."""
    base = _seed_trades(store, {s: 100.0 + s for s in range(0, 120, 10)})

    whole = store.bars("TESTUSD", None, None, "1m", "binance")
    assert len(whole) == 2

    # Ask from halfway through the first bucket. That bucket must not come back
    # with half its volume and the wrong open.
    mid = base + timedelta(seconds=30)
    partial = store.bars("TESTUSD", mid, None, "1m", "binance")
    kept = pd.to_datetime(partial["bucket"], utc=True).tolist()
    assert pd.Timestamp(base).floor("1min") not in kept


# --------------------------------------------------------------------------
# Model recipes and scenario isolation
# --------------------------------------------------------------------------
def test_features_are_declared_not_hard_coded():
    from mdp.model import FEATURES, make_features

    rng = np.random.default_rng(5)
    dates = pd.bdate_range("2024-01-01", periods=300)
    px = 6200 * np.exp(np.cumsum(rng.normal(0, 0.02, len(dates))))
    prices = pd.DataFrame({"trade_date": dates, "adj_close": px, "volume": 1000.0})

    chosen = ["ret_1", "vol_20", "rsi_14"]
    feats = make_features(prices, features=chosen)
    assert feats.attrs["feature_cols"] == chosen
    assert set(chosen) <= set(feats.columns)
    assert "mom_10" not in feats.columns        # not asked for, not computed

    with pytest.raises(KeyError):
        make_features(prices, features=["ret_1", "not_a_feature"])
    assert len(FEATURES) >= 10


def test_a_scenario_override_changes_only_what_it_names():
    from mdp.recipes import Recipe

    recipe = Recipe.by_name("crudeoil_daily")
    scenario = {"name": "x", "overrides": {"estimator": {"alpha": 99.0}}}
    resolved = recipe.resolve(scenario)

    assert resolved["estimator"]["alpha"] == 99.0
    assert resolved["estimator"]["kind"] == recipe.estimator["kind"]   # merged, not replaced
    assert resolved["features"] == recipe.features
    assert recipe.estimator["alpha"] != 99.0                           # base untouched


def test_every_scenario_gets_its_own_isolated_run(store, eod_cfg):
    """Nothing overwrites anything: that is what makes the comparison meaningful."""
    from mdp.pipeline import build_reference, run_source
    from mdp.recipes import Recipe, run_recipe

    run_source(eod_cfg, store, synthetic=True, days=700)
    build_reference(store)

    recipe = Recipe.by_name("crudeoil_daily")
    results = run_recipe(recipe, store, scenarios=["baseline", "price_only"])

    assert len(results) == 2
    assert results["run_id"].nunique() == 2
    registry = store.query("SELECT * FROM model_runs")
    assert len(registry) == 2
    assert {r.split(":")[1] for r in registry["model"]} == {"baseline", "price_only"}


def test_a_run_records_the_data_it_saw_not_just_its_parameters(store, eod_cfg):
    """Parameters alone do not reproduce a run: the data moves underneath them."""
    import json

    from mdp.pipeline import build_reference, run_source
    from mdp.recipes import Recipe, run_recipe

    run_source(eod_cfg, store, synthetic=True, days=700)
    build_reference(store)
    results = run_recipe(Recipe.by_name("crudeoil_daily"), store, scenarios=["baseline"])

    data = results.attrs["data"]
    assert data["rows"] > 0 and data["series_sha"]
    params = json.loads(store.query("SELECT * FROM model_runs")["params"].iloc[0])
    assert params["data"]["series_sha"] == data["series_sha"]
    assert params["recipe_version"] == Recipe.by_name("crudeoil_daily").version


# --------------------------------------------------------------------------
# Charts, the notebook and the monitoring surface
# --------------------------------------------------------------------------
def test_the_charts_draw_from_real_run_output(store, eod_cfg, tmp_path):
    from mdp.charts import roll_chart
    from mdp.pipeline import build_reference, run_source
    from mdp.serve import MarketData

    run_source(eod_cfg, store, synthetic=True, days=300)
    build_reference(store)
    series = MarketData(store=store).get_continuous("CRUDEOIL")

    path = roll_chart(series, out=tmp_path / "roll.png")
    assert path.exists() and path.stat().st_size > 5_000


def test_the_notebook_runs_against_the_serving_api_only(tmp_path):
    """The research notebook must not reach past MarketData.

    This is the guarantee that makes the platform worth having. If a notebook
    opens a parquet file or writes SQL, the abstraction is decoration and the
    next researcher will copy the shortcut.
    """
    import json

    nb = json.loads(
        (Path(__file__).resolve().parents[1] / "notebooks" / "research.ipynb")
        .read_text(encoding="utf-8")
    )
    code = "\n".join(
        "".join(c["source"]) for c in nb["cells"] if c["cell_type"] == "code"
    )

    assert "MarketData(" in code
    for shortcut in ("read_parquet", "SELECT ", "store.query(", "duckdb.connect",
                     "open("):
        assert shortcut not in code, f"notebook bypasses the serving API: {shortcut}"


def test_the_exporter_serves_metrics_and_survives_a_dead_store():
    """Monitoring that cannot report its own failure is decoration.

    The exporter must keep answering when the store is unreadable, and must say
    so in the payload rather than serving a frozen number as if it were fresh.
    """

    from mdp import metrics as metrics_mod

    calls = {"n": 0}

    class Boom:
        def query(self, *a, **k):
            raise RuntimeError("database is locked")

        def close(self):
            calls["n"] += 1

    exporter = metrics_mod._Exporter(Boom)
    body = exporter.scrape()

    assert "mdp_exporter_scrape_failures_total 1" in body
    assert "mdp_exporter_last_success_timestamp_seconds 0" in body
    assert calls["n"] == 1, "the failing scrape must still close its connection"


def test_pushed_alerts_get_a_stable_alertname():
    """Alertmanager groups on alertname, so interpolating the source into it
    would produce one notification per symbol on a bad morning."""
    from mdp.alerts import Alert, AlertmanagerSink, Severity, _alertname

    assert _alertname("mcx_bhavcopy: load blocked, data not published") == \
        _alertname("binance_trades: load blocked, data not published")

    sink = AlertmanagerSink(url="http://example.invalid")
    payload = sink._payload(Alert(
        Severity.PAGE, "mcx_bhavcopy: no delivery for 2026-09-25", "overdue",
        source="mcx_bhavcopy",
        context={"trading_day": "2026-09-25", "run_id": "abc123", "rows": 9},
    ))[0]

    assert payload["labels"]["severity"] == "page"
    assert payload["labels"]["source"] == "mcx_bhavcopy"
    assert payload["labels"]["trading_day"] == "2026-09-25"
    # High-cardinality context must not become labels.
    assert "run_id" not in payload["labels"] and "rows" not in payload["labels"]
    assert payload["endsAt"] > payload["startsAt"]


def test_importing_charts_does_not_hijack_an_interactive_backend():
    """A library that reconfigures matplotlib globally on import breaks the
    first person who uses it in a notebook, and does it silently: the figures
    simply never appear."""
    import matplotlib

    original = matplotlib.get_backend()
    try:
        matplotlib.use("module://matplotlib_inline.backend_inline", force=True)
    except Exception:  # pragma: no cover - backend not installed
        pytest.skip("inline backend unavailable")
    try:
        import importlib

        import mdp.charts

        importlib.reload(mdp.charts)
        assert "inline" in matplotlib.get_backend().lower()
    finally:
        matplotlib.use(original, force=True)


def test_every_metric_the_dashboards_and_alerts_use_is_actually_emitted(store, eod_cfg):
    """The classic silent monitoring failure.

    Rename a metric in the exporter and nothing breaks: the alert simply never
    fires again and the panel quietly reads "No Data". Nobody notices until the
    morning they needed it. Pinning the contract in a test is cheap; discovering
    the gap during an incident is not.
    """
    import json
    import re

    import yaml

    from mdp.metrics import render
    from mdp.pipeline import run_source

    run_source(eod_cfg, store, synthetic=True, days=30)
    # Match the declared names (the `# HELP` lines), not the series. A metric
    # family with no series yet is still part of the contract: cursor lag has no
    # rows until a streaming source has run, and that is not a broken dashboard.
    payload = render(store)
    emitted = set(re.findall(r"^# HELP (mdp_[a-z_]+)", payload, re.M))
    emitted |= set(re.findall(r"^(mdp_[a-z_]+)", payload, re.M))
    # Exporter self-metrics are added by the endpoint, not by render().
    emitted |= {"mdp_exporter_scrape_failures_total",
                "mdp_exporter_last_success_timestamp_seconds",
                "mdp_exporter_scrape_duration_seconds"}

    root = Path(__file__).resolve().parents[1]
    referenced: set[str] = set()

    rules = yaml.safe_load((root / "monitoring" / "rules" / "mdp.yml").read_text())
    for group in rules["groups"]:
        for rule in group["rules"]:
            referenced |= set(re.findall(r"\b(mdp_[a-z_]+)", rule["expr"]))

    board = json.loads(
        (root / "monitoring" / "grafana" / "dashboards" / "mdp-platform.json").read_text()
    )
    for panel in board["panels"]:
        for target in panel.get("targets", []):
            referenced |= set(re.findall(r"\b(mdp_[a-z_]+)", target["expr"]))

    assert referenced, "no metrics referenced: the cross-check itself is broken"
    assert not (referenced - emitted), (
        f"alerts or panels reference metrics the exporter never emits: "
        f"{sorted(referenced - emitted)}"
    )


# --------------------------------------------------------------------------
# Findings from the pre-presentation audit. Each of these failed before its fix.
# --------------------------------------------------------------------------
def test_a_correction_beats_the_row_it_corrects_on_either_store(store, eod_cfg):
    """`_latest_per_grain` has to be able to tell which row arrived last.

    It could not. The two stores spelled the ingest-time column differently, so
    the sort silently never ran on DuckDB, and separately the DuckDB insert
    wrote an explicit NULL over the column's own DEFAULT, so there was nothing
    to sort by even once the names matched. The two backends resolved the same
    correction in opposite directions.
    """
    from mdp.pipeline import run_source
    from mdp.serve import _latest_per_grain

    run_source(eod_cfg, store, synthetic=True, days=40)
    rows = store.query("SELECT * FROM eod_bars LIMIT 1")
    assert rows["ingested_at"].notna().all(), "the column DEFAULT did not fire"

    original = rows.iloc[0]
    correction = rows.copy()
    correction["close"] = 99999.0
    time.sleep(0.01)
    store.insert("eod_bars", correction.drop(columns=["ingested_at"]))

    both = store.query(
        f"SELECT * FROM eod_bars WHERE symbol = '{original['symbol']}' "
        f"AND trade_date = DATE '{pd.Timestamp(original['trade_date']).date()}' "
        f"AND expiry = DATE '{pd.Timestamp(original['expiry']).date()}'"
    )
    assert len(both) == 2
    kept = _latest_per_grain(both, ["exchange", "symbol", "expiry", "trade_date"])
    assert len(kept) == 1
    assert kept["close"].iloc[0] == 99999.0


def test_duplicates_cannot_be_resolved_without_an_ingest_time(store):
    """Silently keeping whichever row came back last is how a correction loses."""
    from mdp.serve import _latest_per_grain

    df = pd.DataFrame({"symbol": ["X", "X"], "trade_date": ["2026-01-01"] * 2,
                       "close": [1.0, 2.0]})
    with pytest.raises(ValueError, match="ingested_at"):
        _latest_per_grain(df, ["symbol", "trade_date"])


def test_notional_refuses_to_guess_a_lot_size(store, eod_cfg):
    """A silent fallback to 1.0 is a hundredfold understatement of a position,
    produced by the function whose whole job is to prevent exactly that."""
    from mdp.pipeline import build_reference, run_source
    from mdp.serve import MarketData

    run_source(eod_cfg, store, synthetic=True, days=40)
    build_reference(store)
    md = MarketData(store=store)

    assert md.notional("CRUDEOIL", 6200.0, 1) == pytest.approx(620_000.0)
    with pytest.raises(LookupError):
        md.notional("ZINC", 6200.0, 1)              # no spec for this symbol
    with pytest.raises(LookupError):
        md.notional("CRUDEOIL", 6200.0, 1, as_of="2010-01-01")   # before the history


def test_the_operational_views_are_behind_the_same_door(store, eod_cfg):
    """`external` is the consumer the entitlement policy exists to stop, and it
    could read the catalogue, the check history and the access log itself."""
    from mdp.pipeline import run_source
    from mdp.serve import MarketData

    run_source(eod_cfg, store, synthetic=True, days=20)
    outsider = MarketData(store=store, consumer="external")

    assert outsider.catalog().empty          # nothing it may read, so nothing listed
    for view in ("quality", "access_log", "vendor_scores"):
        with pytest.raises(PermissionError):
            getattr(outsider, view)()

    insider = MarketData(store=store, consumer="research")
    assert not insider.catalog().empty


def test_an_unreachable_store_is_not_reported_as_an_empty_catalogue(store):
    """"There is no data" and "I could not ask" are different answers."""
    from mdp.serve import MarketData

    class Broken:
        def query(self, sql):
            raise RuntimeError("connection refused")

    md = MarketData(store=Broken(), consumer="research", enforce=False)
    with pytest.raises(RuntimeError, match="connection refused"):
        md.catalog()


def test_the_roll_map_is_read_back_rather_than_recomputed(store, eod_cfg):
    """`as_of` has to mean "what we believed then", not "re-run today's rule on
    truncated data". Those differ the moment a settlement is corrected."""
    from mdp.pipeline import build_reference, run_source
    from mdp.serve import MarketData

    run_source(eod_cfg, store, synthetic=True, days=400)
    first = build_reference(store, now="2026-06-01")
    assert first["opened"] > 0

    md = MarketData(store=store)
    stored = md._roll_map("CRUDEOIL", as_of="2026-06-01")
    assert not stored.empty, "the map must be readable as of a date"
    assert "trade_date" in stored.columns, "a map without a day answers nothing"

    # Re-running on the same data must not restate anything.
    again = build_reference(store, now="2026-06-02")
    assert again["opened"] == 0 and again["closed"] == 0
    assert again["unchanged"] > 0

    series = md.get_continuous("CRUDEOIL", as_of="2026-06-01")
    assert not series.empty
    assert pd.Timestamp(series["trade_date"].max()) <= pd.Timestamp("2026-06-01")


def test_a_restated_roll_closes_the_old_belief_instead_of_erasing_it(store):
    """The version that deleted and rewrote the table made the point-in-time
    claim false in the one place it mattered: the old answer was simply gone, so
    a run that used it could never be reproduced.

    Built from an explicit frame rather than from the synthetic generator. The
    first version of this test bumped the lowest-volume contract of whatever the
    generator had produced, which worked until the generated shape shifted and
    the bumped contract turned out to expire BEFORE the current front, where the
    monotonic guard correctly refuses to switch. A test whose meaning depends on
    the day it runs is not a test.
    """
    from mdp.pipeline import build_reference

    store.ensure_schema()
    # Two contracts trading side by side. March has the volume on both days, so
    # it is front; May expires later, so promoting it is a legal forward roll.
    rows = []
    for day, mar_vol, may_vol in (("2026-02-02", 900.0, 100.0),
                                  ("2026-02-03", 900.0, 100.0)):
        for expiry, volume in (("2026-03-19", mar_vol), ("2026-05-19", may_vol)):
            rows.append({"exchange": "MCX", "symbol": "CRUDEOIL", "expiry": expiry,
                         "trade_date": day, "open": 6000.0, "high": 6100.0,
                         "low": 5900.0, "close": 6050.0, "settle": 6050.0,
                         "volume": volume, "open_interest": 10.0})
    store.insert("eod_bars", pd.DataFrame(rows))

    first = build_reference(store, now="2026-02-04")
    assert first["opened"] == 2 and first["closed"] == 0

    believed = store.query(
        "SELECT * FROM contract_reference WHERE trade_date = DATE '2026-02-03'"
    )
    assert len(believed) == 1
    assert pd.Timestamp(believed["expiry"].iloc[0]) == pd.Timestamp("2026-03-19")

    # The vendor restates 3 February: May had the volume after all, so the roll
    # happened a day earlier than we published.
    correction = pd.DataFrame([{
        "exchange": "MCX", "symbol": "CRUDEOIL", "expiry": "2026-05-19",
        "trade_date": "2026-02-03", "open": 6000.0, "high": 6100.0, "low": 5900.0,
        "close": 6050.0, "settle": 6050.0, "volume": 5000.0, "open_interest": 10.0,
    }])
    time.sleep(0.01)
    store.insert("eod_bars", correction)

    after = build_reference(store, now="2026-02-10")
    assert after["opened"] == 1, "a changed answer must open a new row"
    assert after["closed"] == 1, "and close the old one rather than delete it"

    history = store.query(
        "SELECT * FROM contract_reference WHERE trade_date = DATE '2026-02-03' "
        "ORDER BY known_from"
    )
    assert len(history) == 2, "both beliefs survive"
    known_to = pd.to_datetime(history["known_to"])
    assert (known_to < pd.Timestamp("2999-12-31")).sum() == 1, "one is closed"
    assert (known_to >= pd.Timestamp("2999-12-31")).sum() == 1, "one is current"

    # And the closed one still says what we used to believe, which is the entire
    # point: a backtest run on 5 February can be reproduced.
    closed = history[known_to < pd.Timestamp("2999-12-31")].iloc[0]
    current = history[known_to >= pd.Timestamp("2999-12-31")].iloc[0]
    assert pd.Timestamp(closed["expiry"]) == pd.Timestamp("2026-03-19")
    assert pd.Timestamp(current["expiry"]) == pd.Timestamp("2026-05-19")


def test_the_cursor_never_runs_past_the_window_that_was_asked_for(store):
    """One clock-skewed print used to drag the high-water mark hours ahead, and
    the next window then started after it ended and read backwards. The time in
    between was never requested by any run: not an error, not a retry, gone."""
    from mdp import cursor as cursor_store

    window_end = pd.Timestamp("2026-09-20 12:00:00", tz="UTC")
    frame = pd.DataFrame({
        "ts": [window_end - timedelta(minutes=5), window_end + timedelta(hours=2)],
        "trade_id": [1, 2],
    })

    moved = cursor_store.advance(
        store, "skewed", frame, window_end=window_end.to_pydatetime()
    )
    assert moved is not None
    assert pd.Timestamp(moved.position_ts) == window_end

    nxt = cursor_store.read(store, "skewed").window(default_minutes=60, overlap_seconds=5)
    assert nxt[0] < nxt[1], "the next window must read forwards"


def test_a_restore_that_cannot_validate_leaves_the_table_alone(store, eod_cfg):
    """A restore is what you reach for when things have already gone wrong. It
    is the last operation that should be able to make them worse - and this one
    emptied the table, put nothing back, and recorded the result as `pass`."""
    from mdp.pipeline import restore_from_landing, run_source

    run_source(eod_cfg, store, synthetic=True, days=40)
    before = int(store.query("SELECT count(*) AS n FROM eod_bars")["n"].iloc[0])
    assert before > 0

    # Corrupt the landing zone the way a vendor rename does: a required column
    # disappears, so nothing survives the contract gate.
    from mdp.config import repo_root

    landing = repo_root() / "data" / "landing" / eod_cfg.dataset
    files = sorted(landing.rglob("*.parquet"))
    assert files
    for f in files:
        df = pd.read_parquet(f)
        df.rename(columns={"settle": "settlement_price"}).to_parquet(f, index=False)

    result = restore_from_landing(eod_cfg, store)
    after = int(store.query("SELECT count(*) AS n FROM eod_bars")["n"].iloc[0])

    assert result["rows_restored"] == 0
    assert after == before, "the existing data must survive a refused restore"
    events = store.query(
        "SELECT * FROM quality_events WHERE check_name = 'restore' "
        "ORDER BY event_at DESC LIMIT 1"
    )
    assert events["status"].iloc[0] == "fail", "and the record must say so"


def test_walk_forward_purges_the_labels_that_resolve_inside_the_test_block():
    """At horizon h the last h training labels are forward returns computed from
    prices inside the test block. Without a purge the five-day scenario leaked
    five times as far as the one-day scenario, and the recipe ranks them against
    each other, so the comparison was partly measuring leakage."""
    from mdp.model import make_features, walk_forward

    rng = np.random.default_rng(11)
    dates = pd.bdate_range("2024-01-01", periods=600)
    px = 6200 * np.exp(np.cumsum(rng.normal(0, 0.015, len(dates))))
    prices = pd.DataFrame({"trade_date": dates, "adj_close": px, "volume": 1000.0})

    for horizon in (1, 5):
        feats = make_features(prices, horizon=horizon)
        out = walk_forward(feats, min_train=250, test_size=21, horizon=horizon)
        assert out.purge == horizon - 1
        for fold in out.folds:
            # The last training label must resolve strictly before the test
            # block opens, which is what the gap buys.
            gap_days = np.busday_count(
                pd.Timestamp(fold.train_end).date(), pd.Timestamp(fold.test_start).date()
            )
            assert gap_days >= horizon, (
                f"horizon {horizon}: train ends {fold.train_end}, test starts "
                f"{fold.test_start}, only {gap_days} business days apart"
            )
