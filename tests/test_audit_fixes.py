"""Regression tests for the defects found in the platform audit.

Each test here is written so that it fails against the behaviour that was
shipped and passes against the behaviour that replaced it. They are deliberately
built from hand-made frames with arithmetic that can be checked by eye, because
a test whose expected value came out of the code it is testing proves nothing.
"""
from __future__ import annotations

import argparse
from datetime import UTC, date, datetime, timedelta

import pandas as pd
import pytest

from mdp.config import SourceConfig
from mdp.reference import continuous_series, front_month_map

NOW = datetime(2026, 9, 20, 12, 0, tzinfo=UTC)

OLD_EXPIRY = "2026-01-28"
NEW_EXPIRY = "2026-02-25"


# --------------------------------------------------------------------------
# Defect 1: the roll day with no print from the expiring contract
# --------------------------------------------------------------------------
def _frame_where_the_old_contract_stops_early(
    *, last_day_of_old: int = 9, days: int = 20, overlap: bool = True
) -> pd.DataFrame:
    """Two contracts, the expiring one going dark before the roll.

    The new contract trades at twice the price of the old one, so an unadjusted
    splice shows a +100% one-day return: a jump big enough that no rounding
    argument can explain it away.
    """
    dates = pd.bdate_range("2026-01-05", periods=days)
    rows = []
    for i, day in enumerate(dates):
        old_trades = i <= last_day_of_old if overlap else i < last_day_of_old
        new_trades = True if overlap else i >= last_day_of_old
        drift = 1.0 + 0.001 * i
        if old_trades:
            rows.append(_bar(OLD_EXPIRY, day, 100.0 * drift, 1000.0))
        if new_trades:
            vol = 100.0 if old_trades else 1000.0
            rows.append(_bar(NEW_EXPIRY, day, 200.0 * drift, vol))
    return pd.DataFrame(rows)


def _bar(expiry: str, day: pd.Timestamp, close: float, volume: float) -> dict:
    return {
        "exchange": "MCX", "symbol": "GOLD", "expiry": expiry,
        "trade_date": day.date().isoformat(), "open": close, "high": close,
        "low": close, "close": close, "settle": close, "volume": volume,
        "open_interest": 100.0,
    }


def test_a_roll_with_no_print_from_the_expiring_contract_is_still_adjusted():
    """The last day both traded is the splice point, not the roll day.

    Before the fix the guard around the ratio fell through, the unchanged factor
    was written to all pre-roll history anyway, and `adj_close` showed a +100%
    return on a day the market moved 0.1%.
    """
    eod = _frame_where_the_old_contract_stops_early()
    adj = continuous_series(eod, symbol="GOLD", adjust="ratio")

    roll = adj.index[adj["is_roll"]].tolist()
    assert len(roll) == 1, "the fixture is meant to produce exactly one roll"

    returns = adj["adj_close"].pct_change().abs()
    assert returns.max() < 0.01, "adjusted returns must be continuous across the roll"
    assert bool(adj.loc[roll[0], "roll_adjusted"]) is True

    # And the factor really was taken from the last shared day, not left at 1.0.
    assert adj.loc[roll[0] - 1, "adj_close"] == pytest.approx(201.8, rel=1e-9)


def test_the_difference_adjustment_uses_the_same_fallback_splice():
    eod = _frame_where_the_old_contract_stops_early()
    adj = continuous_series(eod, symbol="GOLD", adjust="difference")

    steps = adj["adj_close"].diff().abs()
    assert steps.max() < 1.0, "adjusted price differences must be continuous"


def test_a_roll_that_cannot_be_adjusted_is_recorded_rather_than_faked():
    """No day carries both contracts, so there is no honest splice price.

    The old code wrote the carried factor regardless and said nothing. The fix
    leaves the factor alone and marks the roll, so the discontinuity is findable.
    """
    eod = _frame_where_the_old_contract_stops_early(last_day_of_old=10, overlap=False)
    adj = continuous_series(eod, symbol="GOLD", adjust="ratio")

    roll = adj.index[adj["is_roll"]].tolist()
    assert len(roll) == 1
    assert bool(adj.loc[roll[0], "roll_adjusted"]) is False
    # Non-roll rows have nothing to report, so they are null rather than False.
    assert adj["roll_adjusted"].notna().sum() == 1


def test_roll_adjusted_is_null_when_no_adjustment_was_asked_for():
    eod = _frame_where_the_old_contract_stops_early()
    raw = continuous_series(eod, symbol="GOLD", adjust="none")
    assert raw["roll_adjusted"].isna().all()


# --------------------------------------------------------------------------
# Defect 2: the day the new front contract is missing from the file
# --------------------------------------------------------------------------
APRIL, MAY = "2026-04-28", "2026-05-26"
GAP_DAY = "2026-03-05"


def _frame_where_the_new_front_is_absent_for_a_day() -> pd.DataFrame:
    """Both contracts trade every day except one, when May has no row.

    `test_front_month_never_rolls_backwards` in the main suite uses a frame in
    which both contracts trade on every single day, so it never reaches the
    branch that used to let the pick fall back to the expired contract. This one
    does: on 2026-03-05 the front contract is simply not in the file.
    """
    rows = []
    for i, day in enumerate(pd.bdate_range("2026-03-02", periods=5)):
        stamp = day.date().isoformat()
        april_leads = i == 0
        rows.append(_bar(APRIL, day, 100.0, 1000.0 if april_leads else 100.0))
        if stamp != GAP_DAY:
            rows.append(_bar(MAY, day, 102.0, 100.0 if april_leads else 1000.0))
    return pd.DataFrame(rows)


def test_the_front_month_does_not_flip_back_when_the_new_contract_is_absent():
    """Proven before the fix: Apr, May, May, Apr, May. Not monotonic."""
    chosen = front_month_map(_frame_where_the_new_front_is_absent_for_a_day())
    expiries = chosen.sort_values("trade_date")["expiry"].tolist()

    assert expiries == sorted(expiries), "a roll must happen once, not flip back"
    assert expiries[-2] == pd.Timestamp(MAY), "the gap day carries the front forward"


def test_a_day_the_front_contract_did_not_print_is_dropped_not_invented():
    """Carrying the previous close forward would publish a return that did not
    happen, so the day leaves the series instead."""
    series = continuous_series(
        _frame_where_the_new_front_is_absent_for_a_day(), symbol="GOLD", adjust="ratio"
    )
    days = set(series["trade_date"])

    assert pd.Timestamp(GAP_DAY) not in days
    assert len(series) == 4
    assert series["close"].notna().all() and series["adj_close"].notna().all()


# --------------------------------------------------------------------------
# Defect 3: the schema-version gate that failed open
# --------------------------------------------------------------------------
class _BrokenReadStore:
    """A store whose `schema_versions` read fails while the table is there.

    A dead replica, a permission change or a lock timeout all look like this,
    and none of them mean "this dataset has never been loaded".
    """

    def __init__(self, *, table_exists: bool | Exception = True):
        self.table_exists = table_exists
        self.inserted: list[tuple[str, pd.DataFrame]] = []

    def query(self, sql: str) -> pd.DataFrame:
        if "information_schema" in sql:
            if isinstance(self.table_exists, Exception):
                raise self.table_exists
            return pd.DataFrame({"n": [1 if self.table_exists else 0]})
        raise RuntimeError("connection reset by peer")

    def insert(self, table: str, df: pd.DataFrame, **kwargs) -> int:
        self.inserted.append((table, df))
        return len(df)


@pytest.fixture
def eod_cfg():
    return SourceConfig.by_name("mcx_bhavcopy")


def test_a_schema_gate_that_cannot_read_its_state_blocks(eod_cfg):
    """Before the fix this became a 'first sighting', which passed and wrote the
    version as auto-accepted, so every later run passed legitimately."""
    from mdp.quality import check_schema_version

    store = _BrokenReadStore(table_exists=True)
    result = check_schema_version(eod_cfg, store)

    assert result.status == "fail"
    assert not store.inserted, "a failed read must not record an accepted version"

    # And it still blocks on the next run, because nothing was written.
    assert check_schema_version(eod_cfg, store).status == "fail"


def test_a_gate_that_cannot_even_probe_for_its_table_blocks(eod_cfg):
    from mdp.quality import check_schema_version

    store = _BrokenReadStore(table_exists=RuntimeError("information_schema unavailable"))
    result = check_schema_version(eod_cfg, store)

    assert result.status == "fail"
    assert not store.inserted


def test_a_schema_versions_table_that_does_not_exist_yet_is_a_real_first_run(eod_cfg):
    """The one case that may pass: nothing has ever been loaded here."""
    from mdp.quality import check_schema_version

    store = _BrokenReadStore(table_exists=False)
    result = check_schema_version(eod_cfg, store)

    assert result.status == "pass"
    assert "first sighting" in result.detail
    assert [t for t, _ in store.inserted] == ["schema_versions"]


# --------------------------------------------------------------------------
# Defect 4: the check that reconciled a file against itself
# --------------------------------------------------------------------------
MCX_SYMBOLS = ["GOLD", "SILVER", "CRUDEOIL", "NATURALGAS", "COPPER"]


def _bhavcopy(days: int = 20) -> pd.DataFrame:
    """A complete multi-day bhavcopy on real MCX trading days, one row per
    symbol per day, sorted the way an exchange file arrives."""
    from mdp.calendar import load_calendar

    cal = load_calendar("mcx")
    trading = cal.trading_days(date(2026, 6, 1), date(2026, 8, 31))[-days:]
    rows = []
    for day in trading:
        for i, symbol in enumerate(MCX_SYMBOLS):
            px = 1000.0 + i * 100
            rows.append({
                "exchange": "MCX", "symbol": symbol, "expiry": "2026-09-28",
                "trade_date": day.isoformat(), "open": px, "high": px,
                "low": px, "close": px, "settle": px,
                "volume": 500.0 + i, "open_interest": 100.0,
            })
    return pd.DataFrame(rows)


def _check(cfg: SourceConfig, df: pd.DataFrame):
    """Run the quality checks over a frame and return them by name."""
    from mdp.contracts import validate
    from mdp.quality import run_checks

    result = validate(df, cfg.contract, now=NOW)
    report = run_checks(cfg, result, now=NOW, mode="backfill")
    return {c.name: c for c in report.results}, report


def test_a_truncated_file_is_caught(eod_cfg):
    """A transfer cut short leaves its last day short of symbols.

    Before the fix the only check pointed at this compared the parsed file with
    itself, so the full file and the truncated one both reported a largest
    difference of 0.0000% and both published.
    """
    full = _bhavcopy()
    truncated = full.iloc[:-3]                 # the write stopped mid-day

    whole, whole_report = _check(eod_cfg, full)
    cut, cut_report = _check(eod_cfg, truncated)

    assert whole["expected_coverage"].status == "pass"
    assert whole_report.publishable

    assert cut["expected_coverage"].status == "fail"
    assert "fewer symbols" in cut["expected_coverage"].detail
    assert not cut_report.publishable, "a half-delivered file must not publish"

    # The old arithmetic really could not tell the two apart, which is why it
    # had to be renamed rather than tightened.
    assert whole["volume_accounted"].detail == cut["volume_accounted"].detail


def test_a_missing_trading_day_inside_the_file_is_caught(eod_cfg):
    """A delivery that lost a chunk keeps a plausible first and last day."""
    full = _bhavcopy()
    lost = sorted(set(full["trade_date"]))[5]
    gapped = full[full["trade_date"] != lost]

    checks, report = _check(eod_cfg, gapped)

    assert checks["expected_coverage"].status == "fail"
    assert "no rows at all" in checks["expected_coverage"].detail
    assert not report.publishable


def test_nothing_claims_to_reconcile_against_the_exchange_any_more(eod_cfg):
    """The config still says `reconcile_totals`, and it must not resurrect the
    claim that the load was compared with the exchange's own totals."""
    checks, _ = _check(eod_cfg, _bhavcopy())

    assert "reconcile_totals" not in checks
    assert "volume_accounted" in checks and "expected_coverage" in checks
    for check in checks.values():
        assert "reconcil" not in check.detail.lower()
        assert "reconcil" not in check.name.lower()
    assert "the file's own volume" in checks["volume_accounted"].detail


def test_the_configured_symbol_list_is_enforced_when_a_source_asks_for_it(eod_cfg):
    """The strongest form of the check, for a file that really is the whole day."""
    from dataclasses import replace

    quality = dict(eod_cfg.quality)
    quality["coverage"] = {"require_configured_symbols": True}
    strict = replace(eod_cfg, quality=quality)

    one_symbol_short = _bhavcopy()
    one_symbol_short = one_symbol_short[one_symbol_short["symbol"] != "COPPER"]

    checks, report = _check(strict, one_symbol_short)

    assert checks["expected_coverage"].status == "fail"
    assert "COPPER" in checks["expected_coverage"].detail
    assert not report.publishable

    # And the same file passes for a source that has not opted in, because a
    # per-commodity vendor file is a legitimate delivery.
    relaxed, _ = _check(eod_cfg, one_symbol_short)
    assert relaxed["expected_coverage"].status == "pass"


# --------------------------------------------------------------------------
# Defect 5: the declared threshold that was evaluated and then ignored
# --------------------------------------------------------------------------
@pytest.fixture
def trades_cfg():
    return SourceConfig.by_name("binance_trades")


def _trades(n: int = 1000, *, null_rows: int = 0,
            bad_price_rows: int = 0) -> pd.DataFrame:
    base = pd.Timestamp("2026-09-20 10:00", tz="UTC")
    df = pd.DataFrame({
        "venue": "binance",
        "symbol": "BTCUSDT",
        "trade_id": range(1, n + 1),
        "ts": [int((base + pd.Timedelta(milliseconds=i)).timestamp() * 1000)
               for i in range(n)],
        "price": 60000.0,
        "qty": 0.5,
        "is_buyer_maker": False,
    })
    if null_rows:
        df.loc[: null_rows - 1, "qty"] = None        # a required value, absent
    if bad_price_rows:
        df.loc[: bad_price_rows - 1, "price"] = 0.0  # a zero print, not a null
    return df


def test_a_file_that_is_mostly_nulls_blocks_instead_of_half_loading(trades_cfg):
    """The reported symptom: 951 of 1000 rows rejected, publishable True,
    49 rows published. The contract declares max_null_fraction 0.0."""
    from mdp.contracts import validate
    from mdp.quality import run_checks

    result = validate(_trades(1000, null_rows=951), trades_cfg.contract, now=NOW)
    assert len(result.rejected) == 951 and len(result.clean) == 49

    assert result.threshold_failures, "a declared limit was breached"
    assert result.blocking_failures

    report = run_checks(trades_cfg, result, now=NOW, mode="backfill")
    contract = next(c for c in report.results if c.name == "contract")
    assert contract.status == "fail"
    assert "max_null_fraction" in contract.detail
    assert not report.publishable
    assert "contract" in [c.name for c in report.blocking]


def test_a_handful_of_bad_rows_is_still_quarantined_and_the_load_continues(trades_cfg):
    """The module's best idea, and it must survive the fix above."""
    from mdp.contracts import validate
    from mdp.quality import run_checks

    result = validate(_trades(1000, bad_price_rows=3), trades_cfg.contract, now=NOW)
    assert len(result.rejected) == 3

    report = run_checks(trades_cfg, result, now=NOW, mode="backfill")
    contract = next(c for c in report.results if c.name == "contract")
    assert contract.status == "warn"
    assert report.publishable


def test_a_declared_total_reject_limit_blocks_too(trades_cfg):
    """`max_reject_fraction` is the blunt limit, for rejections of any kind."""
    from dataclasses import replace

    from mdp.contracts import validate
    from mdp.quality import run_checks

    rules = dict(trades_cfg.contract.rules, max_reject_fraction=0.001)
    strict = replace(trades_cfg, contract=replace(trades_cfg.contract, rules=rules))

    result = validate(_trades(1000, bad_price_rows=50), strict.contract, now=NOW)
    assert len(result.rejected) == 50
    assert result.threshold_failures

    report = run_checks(strict, result, now=NOW, mode="backfill")
    assert not report.publishable
    detail = next(c for c in report.results if c.name == "contract").detail
    assert "max_reject_fraction" in detail

    # Under the limit it stays a warning, which is the behaviour worth keeping.
    ok = validate(_trades(1000, bad_price_rows=1), strict.contract, now=NOW)
    assert not ok.threshold_failures
    assert run_checks(strict, ok, now=NOW, mode="backfill").publishable


def test_max_null_fraction_counts_nulls_not_every_rejection(trades_cfg):
    """The key is named for nulls and now counts nulls.

    Counting every rejection under it meant the shipped 0.0 blocked any file with
    a single zero print, which is not what a null budget is for.
    """
    from mdp.contracts import validate

    zero_prints = validate(_trades(1000, bad_price_rows=200),
                           trades_cfg.contract, now=NOW)
    assert len(zero_prints.rejected) == 200
    assert not zero_prints.threshold_failures

    absent_values = validate(_trades(1000, null_rows=1), trades_cfg.contract, now=NOW)
    assert absent_values.threshold_failures, "0.0 means no nulls at all"


# --------------------------------------------------------------------------
# Defect 6: the arrival monitor that aggregated across symbols
# --------------------------------------------------------------------------
@pytest.fixture
def store(tmp_path):
    from mdp.storage import DuckDBStore

    s = DuckDBStore(tmp_path / "monitor.duckdb")
    s.ensure_schema()
    yield s
    s.close()


def _load_eod(store, days, symbols) -> None:
    rows = [{
        "exchange": "MCX", "symbol": symbol, "expiry": pd.Timestamp("2026-12-28").date(),
        "trade_date": day, "open": 100.0, "high": 100.0, "low": 100.0,
        "close": 100.0, "settle": 100.0, "volume": 10.0, "open_interest": 5.0,
    } for day in days for symbol in symbols]
    store.insert("eod_bars", pd.DataFrame(rows))


LOOKBACK = 9


def _overdue_days() -> list:
    """Every trading day the monitor will expect over the lookback window."""
    from mdp.calendar import load_calendar

    cal = load_calendar("mcx")
    window = cal.trading_days(date.today() - timedelta(days=LOOKBACK), date.today())
    days = [d for d in window if cal.is_overdue(d)]
    assert len(days) >= 3, "need a few overdue trading days for this test"
    return days


def test_a_symbol_missing_for_days_is_an_incident_not_a_clean_week(eod_cfg, store):
    """Before the fix the monitor counted rows per day with no symbol in the
    query, so CRUDEOIL being absent for three days reported gaps: []."""
    from mdp.alerts import Notifier, Severity
    from mdp.monitor import check_arrivals

    days = _overdue_days()
    all_symbols = list(eod_cfg.acquire["symbols"])
    without_crude = [s for s in all_symbols if s != "CRUDEOIL"]

    _load_eod(store, days[:-3], all_symbols)
    _load_eod(store, days[-3:], without_crude)     # three days with no crude

    notifier = Notifier(sinks=[])
    gaps = check_arrivals(eod_cfg, store, notifier=notifier, lookback_days=LOOKBACK)

    assert [g.trading_day for g in gaps] == days[-3:]
    assert all(g.kind == "partial" for g in gaps)
    assert all(g.missing_symbols == ("CRUDEOIL",) for g in gaps)
    assert all(g.rows_found > 0 for g in gaps)
    assert any(a.severity == Severity.PAGE for a in notifier.sent)


def test_an_empty_day_and_an_incomplete_day_are_told_apart(eod_cfg, store):
    """Different incidents, different responses, so they must be distinguishable
    without parsing a sentence."""
    from mdp.alerts import Notifier
    from mdp.monitor import check_arrivals

    days = _overdue_days()
    all_symbols = list(eod_cfg.acquire["symbols"])

    _load_eod(store, days[:-2], all_symbols)                 # complete
    _load_eod(store, days[-2:-1], all_symbols[:2])           # incomplete
    # the last day is not loaded at all

    notifier = Notifier(sinks=[])
    gaps = {g.trading_day: g for g in
            check_arrivals(eod_cfg, store, notifier=notifier, lookback_days=LOOKBACK)}

    assert set(gaps) == {days[-2], days[-1]}
    assert gaps[days[-2]].kind == "partial"
    assert gaps[days[-1]].kind == "nothing"
    assert "incomplete" in str(gaps[days[-2]])
    assert "nothing" in str(gaps[days[-1]])

    names = {a.alertname for a in notifier.sent}
    assert names == {"MdpDeliveryIncomplete", "MdpDeliveryMissing"}


def test_a_complete_week_still_reports_no_gaps(eod_cfg, store):
    from mdp.monitor import check_arrivals

    _load_eod(store, _overdue_days(), list(eod_cfg.acquire["symbols"]))
    assert check_arrivals(eod_cfg, store, lookback_days=LOOKBACK) == []


# --------------------------------------------------------------------------
# Defect 7: await_arrival ignoring the clock it was handed
# --------------------------------------------------------------------------
@pytest.fixture
def replay_source(tmp_path, monkeypatch, eod_cfg):
    """An mcx source whose drop directory and ledger live under tmp_path."""
    from dataclasses import replace

    from mdp import watch as watch_mod

    monkeypatch.setattr(watch_mod, "LEDGER", tmp_path / "processed.json")
    drop_dir = tmp_path / "incoming"
    drop_dir.mkdir()
    acquire = dict(eod_cfg.acquire)
    acquire["drop"] = dict(acquire.get("drop", {}), dir=str(drop_dir))
    return replace(eod_cfg, acquire=acquire)


def test_a_replayed_arrival_is_scored_against_the_day_it_arrived(replay_source, store):
    """The same file, the same day, judged on the clock it was handed.

    Before the fix `arrived_at` came from `datetime.now()`, so replaying an
    arrival that was five minutes past a deadline days ago reported it as days
    late instead of minutes.
    """
    from mdp.alerts import Notifier
    from mdp.arrival import await_arrival
    from mdp.calendar import load_calendar

    cal = load_calendar("mcx")
    day = cal.previous_trading_day(date.today() - timedelta(days=5))
    deadline = cal.delivery_deadline(day)

    notifier = Notifier(sinks=[])
    result = await_arrival(
        replay_source, store, day, synthetic=True, notifier=notifier,
        now=deadline + timedelta(minutes=5), max_attempts=1, sleep=False,
    )

    assert result.status == "on_time", "five minutes is inside the grace period"
    assert result.arrived_at == deadline + timedelta(minutes=5)
    assert result.delay_seconds == 300
    assert not any("late" in a.title for a in notifier.sent), \
        "nothing arrived late, so nothing may say it did"


def test_the_window_closes_on_the_supplied_clock_not_the_wall_clock(replay_source, store,
                                                                   monkeypatch):
    """A replay must poll for the whole window, not give up at once because the
    real world has moved on."""
    from mdp import arrival as arrival_mod
    from mdp.alerts import Notifier
    from mdp.calendar import load_calendar
    from mdp.fetch import FetchResult

    cal = load_calendar("mcx")
    day = cal.previous_trading_day(date.today() - timedelta(days=5))
    deadline = cal.delivery_deadline(day)

    monkeypatch.setattr(
        arrival_mod, "fetch_to_drop",
        lambda *a, **k: FetchResult(replay_source.name, day, None, 0,
                                    skipped_reason="nothing published"),
    )
    result = arrival_mod.await_arrival(
        replay_source, store, day, synthetic=True, notifier=Notifier(sinks=[]),
        now=deadline + timedelta(minutes=5),
        poll_seconds=900, max_wait_minutes=120, sleep=False,
    )

    # Starting five minutes in and stepping fifteen at a time, the clock first
    # reaches the 120-minute close on the ninth pass.
    assert result.status == "missing"
    assert result.attempts == 9


# --------------------------------------------------------------------------
# Defect 8: acquisition
# --------------------------------------------------------------------------
def test_an_empty_window_is_an_empty_frame_not_a_crash(trades_cfg):
    """`pd.concat([])` raises, the retry policy burns five attempts on it, and a
    recoverable empty window kills the run."""
    from mdp.acquire import acquire

    end = datetime(2026, 9, 20, 12, 0, tzinfo=UTC)
    out = acquire(trades_cfg, synthetic=True, start=end, end=end)

    assert isinstance(out, pd.DataFrame)
    assert out.empty


class _FakeAggTrades:
    """A Binance aggTrades endpoint whose trades all share one millisecond.

    A real venue does this during a liquidation cascade, which is exactly when
    the prints matter most.
    """

    def __init__(self, trades: list[dict], limit: int):
        self.trades = trades
        self.limit = limit
        self.calls: list[dict] = []

    def get(self, url, params=None, timeout=None):
        self.calls.append(dict(params))
        if "fromId" in params:
            picked = [t for t in self.trades if t["a"] >= params["fromId"]]
        else:
            picked = [t for t in self.trades
                      if params["startTime"] <= t["T"] <= params["endTime"]]
        return _FakeResponse(picked[: params["limit"]])


class _FakeResponse:
    def __init__(self, payload):
        self.payload = payload

    def raise_for_status(self):
        return None

    def json(self):
        return self.payload


def test_no_trade_is_skipped_when_a_full_page_shares_one_millisecond(
    trades_cfg, monkeypatch
):
    """`cursor = last_ts + 1` on a full page steps over the rest of that
    millisecond, and every trade in it is lost without a trace."""
    from dataclasses import replace

    import requests

    from mdp.acquire import acquire

    limit = 10
    at = int(datetime(2026, 9, 20, 12, 0, tzinfo=UTC).timestamp() * 1000)
    # 25 trades in one millisecond, so a page of 10 cannot hold them.
    burst = [{"a": i, "T": at, "p": "60000.0", "q": "0.1", "m": False}
             for i in range(1, 26)]
    after = [{"a": 100, "T": at + 5000, "p": "60001.0", "q": "0.2", "m": True}]

    fake = _FakeAggTrades(burst + after, limit)
    monkeypatch.setattr(requests, "get", fake.get)

    acquire_cfg = dict(trades_cfg.acquire, max_rows_per_request=limit)
    cfg = replace(trades_cfg, acquire=dict(acquire_cfg, symbols=["BTCUSDT"]))

    out = acquire(
        cfg,
        start=datetime(2026, 9, 20, 11, 59, tzinfo=UTC),
        end=datetime(2026, 9, 20, 12, 1, tzinfo=UTC),
    )

    got = set(out["trade_id"])
    assert got == {t["a"] for t in burst + after}, "no print may be skipped"
    assert any("fromId" in c for c in fake.calls), \
        "a dense millisecond has to be paged by trade id"


def test_the_calendar_knows_its_own_session_times():
    """The `session:` block had been in the config from the beginning and
    nothing parsed it, so the module's claim to know session times was false.

    It matters beyond tidiness: MCX runs an evening session that crosses
    midnight UTC, so bucketing intraday data by UTC day splits a session in
    half, and every per-day aggregate computed that way is wrong in a way that
    looks like thin volume rather than like a bug.
    """
    from datetime import date, datetime
    from zoneinfo import ZoneInfo

    from mdp.calendar import load_calendar

    cal = load_calendar("mcx")
    bounds = cal.session_bounds(date(2026, 9, 25))     # a Friday
    assert bounds is not None
    opened, closed = bounds
    assert opened.hour == 9 and closed.hour == 23
    assert closed > opened

    # A closed day has no session, and inventing plausible times for one would
    # be the same class of error this module exists to prevent.
    assert cal.session_bounds(date(2026, 9, 26)) is None      # Saturday
    assert cal.session_bounds(date(2026, 10, 2)) is None      # Gandhi Jayanti

    ist = ZoneInfo("Asia/Kolkata")
    assert cal.in_session(datetime(2026, 9, 25, 22, 0, tzinfo=ist))
    assert not cal.in_session(datetime(2026, 9, 25, 8, 0, tzinfo=ist))
    assert not cal.in_session(datetime(2026, 9, 26, 12, 0, tzinfo=ist))


def test_the_vendor_policy_file_only_declares_what_is_read():
    """`golden_sources.yml` opened with a block ranking sources per field that
    nothing read. A config declaring a feature the code does not have is a
    claim, and it is one an interviewer can check in thirty seconds."""
    import yaml

    from mdp.config import repo_root, vendor_quality_policy

    assert not (repo_root() / "config" / "golden_sources.yml").exists()
    path = repo_root() / "config" / "vendor_quality.yml"
    raw = yaml.safe_load(path.read_text(encoding="utf-8"))
    assert set(raw) == {"vendor_quality"}, f"unread top-level keys: {set(raw) - {'vendor_quality'}}"
    assert vendor_quality_policy()["vendor_quality"]["thresholds"]["green"] > 0


# --------------------------------------------------------------------------
# The orchestrator claim, made checkable
# --------------------------------------------------------------------------
# These three tests are the whole of the portability claim, and they are the only
# thing making it. The repository used to make it with a second runner — an Airflow
# DAG over the same functions — and that was weaker: the two were never equivalent
# (the Dagster code location declares asset checks and holiday-excluding partitions,
# the DAG declared neither), and a second implementation diverges quietly the moment
# nobody exercises it, which is exactly what happened to the shell runner it sat
# beside. A static assertion cannot rot. See docs/ARCHITECTURE.md 3.4.
def test_no_pipeline_code_imports_an_orchestrator():
    """The load-bearing test for the portability claim. Nothing else makes it.

    "Porting this to Airflow is a day's work" is now asserted here rather than
    demonstrated by a half-equivalent second DAG. That trade is deliberate: this
    test parses every module under `src/mdp` on every CI run, so it cannot quietly
    stop being true, whereas a second runner is only as honest as the last person
    who ran it. If this test goes red the claim is false, and there is no longer a
    second implementation standing in front of it to soften the news.

    Orchestrator coupling leaks in quietly — a retry decorator on a function
    that then cannot be called without a task context, a parameter that is
    really a Prefect block, state that lives in the run instead of in the
    database. By the time anyone notices, the port is a rewrite.

    The banned set keeps the retired names (prefect, airflow, luigi, temporalio)
    alongside the one that is actually installed somewhere. A ban that only covers
    today's orchestrator would pass a module that imports yesterday's.
    """
    import ast

    from mdp.config import repo_root

    banned = {"prefect", "airflow", "dagster", "luigi", "temporalio"}
    offenders = []
    for path in sorted((repo_root() / "src" / "mdp").glob("*.py")):
        tree = ast.parse(path.read_text(encoding="utf-8"))
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                names = [a.name for a in node.names]
            elif isinstance(node, ast.ImportFrom):
                names = [node.module or ""]
            else:
                continue
            for name in names:
                if name.split(".")[0] in banned:
                    offenders.append(f"{path.name}:{node.lineno} imports {name}")
    assert not offenders, "pipeline code must not know who is calling it: " + "; ".join(offenders)


def test_the_orchestrator_and_the_cli_drive_the_same_entry_points():
    """Replaces `test_both_runners_drive_the_same_functions`, which compared the
    Dagster code location against an Airflow DAG that has since been deleted.

    Keeping the shape of that test and changing what it compares is the right move
    rather than dropping it, because the thing it was really protecting still
    exists: there are two ways to drive this platform, and they must be the same
    pipeline. They are now `flows/definitions.py` and `src/mdp/cli.py` — the
    orchestrated path and the path a person types — and the second is a better
    control than the DAG was. A hand-run command is exercised constantly, by the
    README, by `make demo` and by CI's demo step, so it cannot rot the way an
    unrun second DAG can; and `mdp` subcommands are what anybody porting to another
    orchestrator would actually wrap.

    Parsed rather than imported, because importing the code location needs Dagster
    installed and Dagster is deliberately absent from this environment.
    """
    import ast

    from mdp.config import repo_root

    def mdp_imports(path):
        """Every name the file pulls out of the `mdp` package.

        Handles both spellings: the code location is outside the package and says
        `from mdp.pipeline import run_source`, while the CLI is inside it and says
        `from .pipeline import run_source`. A level-1 import from a module under
        `src/mdp` is an import from `mdp`, so both count.
        """
        tree = ast.parse(path.read_text(encoding="utf-8"))
        names = set()
        for node in ast.walk(tree):
            if not isinstance(node, ast.ImportFrom):
                continue
            absolute = (node.module or "").startswith("mdp")
            relative = node.level > 0 and path.parent.name == "mdp"
            if absolute or relative:
                names.update(a.name for a in node.names)
        return names

    dagster_calls = mdp_imports(repo_root() / "flows" / "definitions.py")
    cli_calls = mdp_imports(repo_root() / "src" / "mdp" / "cli.py")

    assert dagster_calls and cli_calls
    shared = dagster_calls & cli_calls
    # The load path, the reference refresh and the arrival wait are the three that
    # matter: if the orchestrator and the CLI diverge on those, one of them is
    # running a different pipeline and the other is the one people will trust.
    for essential in ("run_source", "build_reference", "await_arrival"):
        assert essential in shared, (
            f"{essential} is driven by only one of the code location and the CLI"
        )


def test_no_orchestrator_is_a_runtime_or_dev_dependency():
    """Generalised from `test_airflow_is_not_a_runtime_dependency`.

    The invariant was never about Airflow. It is that *no* orchestrator is
    installed by a default `uv sync --frozen`, so the environment the tests run in
    is an environment in which the platform has no scheduler — which is what makes
    `test_no_pipeline_code_imports_an_orchestrator` mean something. Asserting it of
    the one runner that happened to be optional at the time would have passed
    happily on the day Dagster was promoted into `[project.dependencies]`, and that
    promotion is the realistic way this claim dies: it is the orchestrator the
    repository actually runs.
    """
    import importlib.util
    import tomllib

    from mdp.config import repo_root

    pyproject = tomllib.loads((repo_root() / "pyproject.toml").read_text(encoding="utf-8"))
    groups = pyproject.get("dependency-groups", {})

    runtime = " ".join(pyproject["project"]["dependencies"]).lower()
    dev = " ".join(d.lower() for d in groups.get("dev", [])).lower()

    for runner in ("dagster", "airflow", "prefect", "luigi", "temporalio"):
        assert runner not in runtime, f"{runner} must not be a runtime dependency"
        # `dev` is installed by a bare `uv sync`, so a scheduler in it is a
        # scheduler everybody has, which has made the orchestrator load-bearing
        # again by a different route.
        assert runner not in dev, (
            f"{runner} belongs in its own optional group, not in dev: a default "
            f"install that drags in a scheduler has made the orchestrator "
            f"load-bearing again"
        )
        # And it must not be importable here, which is what makes the two
        # assertions above mean something rather than describe a file.
        assert importlib.util.find_spec(runner) is None, (
            f"{runner} is importable in the default test environment; the "
            f"platform is supposed to run without an orchestrator installed"
        )

    # Dagster does have a home, so the code location can be exercised on purpose.
    assert "dagster" in groups, "the dagster group should exist for running the assets"
    assert "airflow" not in groups, (
        "the airflow group went with the DAG; a dependency group for a runner that "
        "does not exist is a dangling invitation to reinstate it"
    )
    assert "prefect" not in groups, "Prefect was retired; it should have no group"


def test_the_exporter_resolves_its_store_before_choosing_keyword_arguments(monkeypatch):
    """The metrics endpoint has to work against the store production actually uses.

    `cmd_exporter` builds a factory that passes `read_only=True`, which DuckDB
    accepts and QuestDB does not. It used to decide which of the two it was
    talking to by looking at the --store flag alone:

        kwargs = {"read_only": True} if (kind or "duckdb") == "duckdb" else {}

    With MDP_STORE=questdb and no flag -- the production configuration, and the
    one the compose file and the deployment both use -- `kind` is None, so the
    expression chose "duckdb" and passed an argument QuestDBStore rejects. Every
    scrape then raised, and `_Exporter.scrape` did what it is designed to do with
    a failing read: served the last good payload. From a cold start that payload
    is the empty string.

    So the endpoint returned 200 with no metrics in it, forever, against the only
    store that matters. Prometheus recorded no failure, because the scrape
    succeeded. Nothing alerted, because an absent metric fires no alert -- the
    exact failure the probe query in `metrics.render` was added to prevent,
    reintroduced one layer up by an argument-resolution bug.

    This was found by running the exporter against a live QuestDB rather than by
    reading it.
    """
    from mdp import cli

    seen: dict = {}

    def fake_get_store(kind=None, **kwargs):
        seen["kind"] = kind
        seen["kwargs"] = kwargs
        return object()

    def fake_serve(factory, **_):
        factory()

    monkeypatch.setattr(cli, "get_store", fake_get_store)
    monkeypatch.setattr("mdp.metrics.serve", fake_serve)
    monkeypatch.setenv("MDP_STORE", "questdb")

    args = argparse.Namespace(store=None, host="127.0.0.1", port=9108)
    cli.cmd_exporter(args)

    assert seen["kwargs"] == {}, (
        "with MDP_STORE=questdb the exporter passed read_only=True, which "
        "QuestDBStore rejects; every scrape then failed and the endpoint served "
        "an empty payload that no alert can distinguish from a healthy platform"
    )

    # And the DuckDB path must keep the read-only connection, which is what stops
    # the exporter holding a lock the daily load needs.
    seen.clear()
    monkeypatch.setenv("MDP_STORE", "duckdb")
    cli.cmd_exporter(argparse.Namespace(store=None, host="127.0.0.1", port=9108))
    assert seen["kwargs"] == {"read_only": True}
