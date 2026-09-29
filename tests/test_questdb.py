"""QuestDB tests: the semantics, not the plumbing.

These run only when a QuestDB is reachable at QDB_HOST:QDB_PORT and skip cleanly
otherwise, so CI without one still passes. That is a deliberate shape rather than
a compromise. The interesting behaviour of this backend -- what `ASOF JOIN`
matches at a boundary, whether `DEDUP UPSERT KEYS` collapses at commit or
eventually, what `SAMPLE BY` labels a bucket, how a nanosecond timestamp lands in
a microsecond column -- cannot be asserted against a mock, because a mock would
be asserting what somebody believed the server does. Every one of these was
wrong about something on the first attempt.

Each test gets its own table prefix. QuestDB has no databases and no schemas, so
there is no namespace to isolate a fixture in; without a prefix these tests would
have to write into the same `trades` table a real deployment uses, and the
benchmark in bench/RESULTS.md is loaded into exactly that table on the server this
suite is pointed at.
"""
from __future__ import annotations

import time
import uuid
from datetime import UTC, datetime, timedelta

import pandas as pd
import pytest
import requests

from mdp.config import questdb_settings
from mdp.serve import _latest_per_grain
from mdp.storage import QuestDBStore


def _reachable() -> bool:
    settings = questdb_settings()
    try:
        response = requests.get(
            f"http://{settings['host']}:{settings['port']}/exec",
            params={"query": "SELECT 1"}, timeout=2,
        )
    except requests.RequestException:
        return False
    return response.status_code == 200 and "error" not in response.json()


pytestmark = pytest.mark.skipif(
    not _reachable(),
    reason="no QuestDB at QDB_HOST:QDB_PORT (default 127.0.0.1:9000)",
)


@pytest.fixture
def store():
    """A store whose tables nothing else on the server shares, dropped after."""
    s = QuestDBStore(table_prefix=f"t_pytest_{uuid.uuid4().hex[:10]}_")
    s.ensure_schema()
    try:
        yield s
    finally:
        existing = s.query(
            "SELECT table_name FROM tables() "
            f"WHERE table_name LIKE '{s.table_prefix}%'"
        )
        for name in existing["table_name"]:
            s._exec(f"DROP TABLE IF EXISTS {name}")
        s.close()


def _seed_trades(store, prices_at_seconds, base=None, symbol="TESTUSD"):
    """Trades at known offsets, so the expected answer is arithmetic not luck."""
    base = base or datetime(2026, 9, 20, 10, 0, tzinfo=UTC)
    rows = [
        {"venue": "binance", "symbol": symbol, "trade_id": i + 1,
         "ts": base + timedelta(seconds=offset), "price": price, "qty": 1.0,
         "is_buyer_maker": False}
        for i, (offset, price) in enumerate(sorted(prices_at_seconds.items()))
    ]
    store.insert("trades", pd.DataFrame(rows))
    return base


# --------------------------------------------------------------------------
# The as-of join. The query this platform exists to make correct, and the
# reason QuestDB is the primary store; see bench/RESULTS.md.
# --------------------------------------------------------------------------
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


def test_asof_and_lt_differ_by_exactly_the_decision_instant(store):
    """The one line where a look-ahead bug lives, pinned to the live server.

    QuestDB has two backward joins and they differ at one boundary: `ASOF JOIN`
    takes the last row at or before, `LT JOIN` the last row strictly before. The
    platform's rule is at-or-before, so QuestDBStore.as_of uses ASOF -- but the
    two are one word apart in the SQL and the difference is invisible unless a
    decision time lands exactly on a trade, which is the case this asserts.

    Neither of them leaks the future. LT would silently discard the trade that
    happened at the decision instant, and on a platform where decision times are
    generated from trade times that is not a rare case.
    """
    base = _seed_trades(store, {0: 100.0, 10: 101.0})
    staging = f"t_pytest_lt_{uuid.uuid4().hex[:10]}"
    store._exec(
        f"CREATE TABLE {staging} (query_ts TIMESTAMP, symbol SYMBOL, venue SYMBOL) "
        f"TIMESTAMP(query_ts) PARTITION BY DAY BYPASS WAL"
    )
    try:
        store._imp(
            staging,
            pd.DataFrame([{"query_ts": base + timedelta(seconds=10),
                           "symbol": "TESTUSD", "venue": "binance"}]),
            {"query_ts": "TIMESTAMP", "symbol": "SYMBOL", "venue": "SYMBOL"},
        )
        trades = store.table("trades")
        asof = store.query(
            f"SELECT t.price AS price FROM {staging} q "
            f"ASOF JOIN {trades} t ON (symbol, venue)"
        )
        strictly_before = store.query(
            f"SELECT t.price AS price FROM {staging} q "
            f"LT JOIN {trades} t ON (symbol, venue)"
        )
    finally:
        store._exec(f"DROP TABLE IF EXISTS {staging}")

    assert asof["price"].iloc[0] == 101.0            # the trade AT the instant
    assert strictly_before["price"].iloc[0] == 100.0  # the one before it


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
    # The age survives too, which is the reason QuestDB's TOLERANCE clause is not
    # used to apply this bound even on a server that has it: TOLERANCE nulls the
    # match itself, and a hole with no age on it is a worse answer than this one.
    assert out["staleness_seconds"].iloc[0] == pytest.approx(2400)


def test_a_timestamp_before_any_trade_has_no_price(store):
    from mdp.research import as_of_prices

    base = _seed_trades(store, {60: 100.0})
    out = as_of_prices(store, "TESTUSD", [base])
    assert len(out) == 1
    assert pd.isna(out["price"].iloc[0])
    assert bool(out["stale"].iloc[0])


def test_an_unknown_symbol_is_answered_with_nulls_not_a_timeout(store):
    """QuestDB's ASOF JOIN degenerates on a key that matches nothing.

    Measured against the 20,000,000-row benchmark table: 5,000 decisions answered
    in 0.062 s for a symbol that exists and hit the server's 60-second timeout for
    one that does not, because the backward scan for a key that will never appear
    does not terminate at a partition boundary. The practical shape of that is a
    typo -- one character off a real symbol turns a 30 ms query into a minute and
    then an error, where DuckDB returns NULLs immediately.

    QuestDBStore.as_of probes the key first and answers without the join when it
    has no trades. The answer is identical; the cost is not.
    """
    from mdp.research import as_of_prices

    base = _seed_trades(store, {0: 100.0})
    started = time.perf_counter()
    out = as_of_prices(store, "NOSUCHSYM", [base, base + timedelta(seconds=30)])
    elapsed = time.perf_counter() - started

    assert len(out) == 2                          # a row per timestamp, as always
    assert out["price"].isna().all()
    assert out["stale"].all()
    assert elapsed < 10, (
        "an unknown symbol should not cost a backward scan of the table"
    )


# --------------------------------------------------------------------------
# Bars
# --------------------------------------------------------------------------
def test_a_partial_bucket_is_dropped_rather_than_returned_half_built(store):
    """A bar built from part of its interval and labelled as a whole bar is worse
    than a missing one, because the missing one is visible."""
    base = _seed_trades(store, {s: 100.0 + s for s in range(0, 120, 10)})

    whole = store.bars("TESTUSD", None, None, "1m", "binance")
    assert len(whole) == 2
    assert list(whole.columns) == ["bucket", "open", "high", "low", "close",
                                   "volume", "trades", "notional"]
    assert whole["open"].iloc[0] == 100.0
    assert whole["close"].iloc[0] == 150.0
    assert whole["trades"].iloc[0] == 6

    # Ask from halfway through the first bucket. That bucket must not come back
    # with half its volume and the wrong open.
    mid = base + timedelta(seconds=30)
    partial = store.bars("TESTUSD", mid, None, "1m", "binance")
    kept = pd.to_datetime(partial["bucket"], utc=True).tolist()
    assert pd.Timestamp(base).floor("1min") not in kept
    # And what does come back is whole: the 10:01 bucket with all six of its trades.
    assert partial["trades"].iloc[0] == 6


def test_the_bucket_grid_is_the_calendar_and_not_the_requested_start(store):
    """`SAMPLE BY ... FROM x` anchors the grid on x rather than filtering it.

    Verified against the live server: `SAMPLE BY 1m FROM '10:00:30' ALIGN TO
    CALENDAR` over trades starting at 10:00 returns buckets stamped 10:00:30 and
    10:01:30, and ALIGN TO CALENDAR does not override it. Those are one-minute
    bars of real trades on a grid nobody asked for. QuestDBStore.bars therefore
    filters the bucket in an outer query instead, and this test is what stops
    somebody replacing that with the clause that looks like it was designed for
    the job.
    """
    base = _seed_trades(store, {s: 100.0 + s for s in range(0, 180, 10)})
    bars = store.bars("TESTUSD", base + timedelta(seconds=30), None, "1m", "binance")
    buckets = pd.to_datetime(bars["bucket"], utc=True)
    assert (buckets == buckets.dt.floor("1min")).all(), (
        f"bars must sit on calendar minutes, got {buckets.tolist()}"
    )


# --------------------------------------------------------------------------
# The grain: collapse on replay, keep on correction
# --------------------------------------------------------------------------
def test_a_replayed_trade_on_the_grain_collapses_at_commit(store):
    """DEDUP UPSERT KEYS is why nothing here needs the equivalent of FINAL.

    A feed replays the last N trades on reconnect. On a merge-on-read engine like
    ClickHouse's ReplacingMergeTree those sit in the table as duplicates until a
    merge collapses them, so every as-of read has to pay for `FINAL` to avoid
    matching a superseded print. On QuestDB the collapse happens at commit, so the
    duplicate is never visible to a reader at all.
    """
    prices = {0: 100.0, 10: 101.0, 20: 102.0}
    base = _seed_trades(store, prices)
    trades = store.table("trades")
    after_first = store.query(f"SELECT count() AS n FROM {trades}")["n"].iloc[0]
    assert after_first == 3

    _seed_trades(store, prices, base=base)        # the same three prints again
    after_replay = store.query(f"SELECT count() AS n FROM {trades}")["n"].iloc[0]
    assert after_replay == 3, "a replayed print must not survive as a second row"

    # And a resend that corrects the price of a print replaces it rather than
    # sitting beside it, so the as-of join cannot match the superseded copy.
    store.insert("trades", pd.DataFrame([{
        "venue": "binance", "symbol": "TESTUSD", "trade_id": 2,
        "ts": base + timedelta(seconds=10), "price": 999.0, "qty": 1.0,
        "is_buyer_maker": False,
    }]))
    corrected = store.query(
        f"SELECT price FROM {trades} WHERE trade_id = 2"
    )
    assert len(corrected) == 1
    assert corrected["price"].iloc[0] == 999.0


def test_a_late_correction_to_an_end_of_day_row_is_kept_and_the_newest_wins(store):
    """eod_bars needs the opposite of the trades table's behaviour.

    An exchange restates a settlement days later. That correction must NOT
    overwrite the row it corrects: the original is what a backtest published last
    week saw, and replacing it moves a number that is already in a report with
    nothing recording that it moved. So `ingested_at` is in the dedup key set for
    this table, which makes a correction a new row, and `_latest_per_grain`
    resolves the two on read.
    """
    row = {"exchange": "MCX", "symbol": "GOLD",
           "expiry": pd.Timestamp("2026-10-05"), "trade_date": pd.Timestamp("2026-09-20"),
           "open": 1.0, "high": 2.0, "low": 0.5, "close": 1.5, "settle": 1.5,
           "volume": 10.0, "open_interest": 5.0}
    store.insert("eod_bars", pd.DataFrame([row]))
    eod = store.table("eod_bars")

    stored = store.query(f"SELECT * FROM {eod}")
    assert stored["ingested_at"].notna().all(), (
        "ingested_at must be filled by the store; QuestDB has no column DEFAULT"
    )

    time.sleep(0.01)
    store.insert("eod_bars", pd.DataFrame([{**row, "close": 99999.0}]))

    both = store.query(f"SELECT * FROM {eod} ORDER BY trade_date")
    assert len(both) == 2, "a late correction must be kept, not merged away"

    kept = _latest_per_grain(both, ["exchange", "symbol", "expiry", "trade_date"])
    assert len(kept) == 1
    assert kept["close"].iloc[0] == 99999.0


# --------------------------------------------------------------------------
# The places QuestDB is not like DuckDB, asserted so they stay handled
# --------------------------------------------------------------------------
def test_nanosecond_timestamps_land_as_microseconds_without_moving(store):
    """QuestDB is microsecond-native and pandas is nanosecond-native.

    The conversion truncates, and truncation rather than rounding is deliberate:
    see the note on _qdb_literal. What must not happen is a timestamp arriving at
    a different microsecond than the one it left at, which is what a tz-naive
    round trip or a `%f` on a nanosecond value would produce.
    """
    base = pd.Timestamp("2026-09-20 10:00:00.123456789", tz="UTC")
    store.insert("trades", pd.DataFrame([{
        "venue": "binance", "symbol": "NANOUSD", "trade_id": 1,
        "ts": base, "price": 1.0, "qty": 1.0, "is_buyer_maker": False,
    }]))
    stored = store.query(
        f"SELECT ts FROM {store.table('trades')} WHERE symbol = 'NANOUSD'"
    )
    assert stored["ts"].iloc[0] == base.floor("us")

    # And the truncation is downwards, so a decision at the same microsecond
    # matches and one a nanosecond earlier still does.
    from mdp.research import as_of_prices
    out = as_of_prices(store, "NANOUSD", [base.floor("us")])
    assert out["price"].iloc[0] == 1.0


def test_a_day_comes_back_naive_and_an_instant_comes_back_aware(store):
    """The one thing the Store interface genuinely has to paper over.

    DuckDB has a DATE type distinct from a timestamp, and this schema uses it for
    `trade_date`, `expiry`, `known_from` and the rest; a
    DuckDB read hands those to pandas tz-naive and hands `ts` and `ingested_at`
    back tz-aware. QuestDB has one temporal type, so QuestDBStore resolves it by
    column name. It has to: reference.py compares `trade_date` against a naive
    `pd.Timestamp(as_of)`, and pandas raises TypeError rather than coercing, so
    getting this wrong breaks `get_continuous` and nothing else.
    """
    store.insert("eod_bars", pd.DataFrame([{
        "exchange": "MCX", "symbol": "GOLD", "expiry": pd.Timestamp("2026-10-05"),
        "trade_date": pd.Timestamp("2026-09-20"), "open": 1.0, "high": 2.0,
        "low": 0.5, "close": 1.5, "settle": 1.5, "volume": 10.0,
        "open_interest": 5.0,
    }]))
    eod = store.query(f"SELECT * FROM {store.table('eod_bars')}")
    assert eod["trade_date"].dt.tz is None
    assert eod["expiry"].dt.tz is None
    assert eod["ingested_at"].dt.tz is not None
    # The comparison reference.py makes, which is the reason for all of the above.
    assert (eod["trade_date"] <= pd.Timestamp("2026-09-30")).all()


def test_a_write_is_visible_to_the_very_next_read(store):
    """A WAL write is acknowledged at the sequencer and applied later.

    `/imp` returning does not mean a SELECT will see the rows, so QuestDBStore
    waits for the apply job to catch up before `insert` returns. Without that,
    every read-after-write in this platform becomes a race: `advance_cursor`
    writes a cursor the next window reads back, and the contract gate writes a
    first sighting the next run reads.
    """
    trades = store.table("trades")
    for i in range(12):
        store.insert("trades", pd.DataFrame([{
            "venue": "binance", "symbol": "RACEUSD", "trade_id": i,
            "ts": datetime(2026, 9, 20, 10, 0, i, tzinfo=UTC),
            "price": 1.0 + i, "qty": 1.0, "is_buyer_maker": False,
        }]))
        seen = store.query(
            f"SELECT count() AS n FROM {trades} WHERE symbol = 'RACEUSD'"
        )["n"].iloc[0]
        assert seen == i + 1, f"write {i} was not visible to the next read"


def test_a_rejected_row_is_raised_rather_than_silently_dropped(store):
    """`/imp` answers HTTP 200 "status":"OK" while dropping rows it could not parse.

    `atomicity=abort` does not change that -- the rejection is reported only in
    the response body, in `rowsRejected`. A loader that trusts the status code
    loses rows and says nothing, which is the failure this platform exists to make
    impossible, so `insert` reads the field and raises.

    The unparseable value here is a float in a LONG column, reached by handing
    `insert` a frame whose `trade_id` is a plain object column holding text.
    """
    bad = pd.DataFrame([{
        "venue": "binance", "symbol": "BADUSD", "trade_id": "not-a-number",
        "ts": datetime(2026, 9, 20, 10, 0, tzinfo=UTC),
        "price": 1.0, "qty": 1.0, "is_buyer_maker": False,
    }])
    with pytest.raises(RuntimeError, match="rejected"):
        store.insert("trades", bad)


def test_a_frame_without_the_designated_timestamp_is_a_clear_error(store):
    """QuestDB closes the connection when a CSV omits the designated timestamp.

    No error message, no status code: the caller gets a ConnectionError out of
    `requests` and no hint which column was missing. So `insert` checks first and
    says which one it was.
    """
    with pytest.raises(ValueError, match="ts"):
        store.insert("trades", pd.DataFrame([{
            "venue": "binance", "symbol": "NOTSUSD", "trade_id": 1,
            "price": 1.0, "qty": 1.0, "is_buyer_maker": False,
        }]))


def test_the_schema_declares_every_table_the_platform_writes_to(store):
    """A missing table is not a crash, it is a dataset that quietly never loads.

    serve.py treats "table does not exist" as "not loaded yet" and returns empty,
    on purpose, so a schema file that forgot a table produces a platform that
    looks like it is working and has a hole in it. This is the list DuckDBStore
    creates, and it is checked rather than trusted.
    """
    expected = {
        "trades", "eod_bars", "contract_reference", "quality_events",
        "vendor_scores", "schema_versions", "ingest_cursors", "contract_specs",
        "access_log", "model_runs",
    }
    found = store.query(
        "SELECT table_name FROM tables() "
        f"WHERE table_name LIKE '{store.table_prefix}%'"
    )
    names = {n[len(store.table_prefix):] for n in found["table_name"]}
    assert expected <= names, f"missing from sql/02_questdb.sql: {expected - names}"

    # And every one of them is ordered by a timestamp, which on QuestDB is not
    # decoration: it is what the as-of join binary-searches and what a range
    # filter prunes on.
    ordering = store.query(
        "SELECT table_name, designatedTimestamp FROM tables() "
        f"WHERE table_name LIKE '{store.table_prefix}%'"
    )
    assert ordering["designatedTimestamp"].notna().all()


def test_ensure_schema_is_safe_to_run_twice(store):
    """`make schema` is in the runbook and gets run on a populated database.

    QuestDB has no CREATE OR REPLACE for this and CREATE TABLE IF NOT EXISTS does
    not touch an existing table's data, but the DEDUP clause is part of the same
    statement, so a re-run is the natural place for a schema file to quietly drop
    rows or reset a dedup key. It does not.
    """
    _seed_trades(store, {0: 100.0, 10: 101.0})
    store.ensure_schema()
    kept = store.query(f"SELECT count() AS n FROM {store.table('trades')}")
    assert kept["n"].iloc[0] == 2
    assert store.query(
        "SELECT dedup FROM tables() "
        f"WHERE table_name = '{store.table('trades')}'"
    )["dedup"].iloc[0]


def test_the_store_is_reachable_through_get_store(monkeypatch):
    """MDP_STORE=questdb has to be the whole of what a deployment configures."""
    from mdp.storage import get_store

    settings = questdb_settings()
    monkeypatch.setenv("MDP_STORE", "questdb")
    monkeypatch.setenv("QDB_HOST", settings["host"])
    monkeypatch.setenv("QDB_PORT", str(settings["port"]))
    store = get_store()
    try:
        assert isinstance(store, QuestDBStore)
        assert store.host == settings["host"]
        assert store.port == settings["port"]
        assert store.query("SELECT 1 AS one")["one"].iloc[0] == 1
    finally:
        store.close()


def test_the_connection_comes_from_the_environment_not_a_literal(monkeypatch):
    """Same rule the source configs follow: the name is committed, not the value."""
    monkeypatch.setenv("QDB_HOST", "questdb.internal")
    monkeypatch.setenv("QDB_PORT", "9001")
    assert questdb_settings() == {"host": "questdb.internal", "port": 9001}

    monkeypatch.delenv("QDB_HOST", raising=False)
    monkeypatch.delenv("QDB_PORT", raising=False)
    # The fallbacks are QuestDB's own HTTP defaults, which is what makes a local
    # `docker compose up questdb` need no configuration at all.
    assert questdb_settings() == {"host": "127.0.0.1", "port": 9000}
