"""Storage, kept behind one small interface.

Two implementations:

  QuestDB    the primary production store, chosen on a measurement rather than a
             preference. bench/RESULTS.md times the as-of join -- the query this
             platform exists to make correct -- on 20,000,000 trades: QuestDB
             answered in 0.029 s at 5,000 decisions and 0.081 s at 50,000 with
             +297 MB of resident memory, against ClickHouse's 1.06 s at both
             sizes with +839 MB. ClickHouse's time is flat in the number of
             decisions because it hash-builds the entire right-hand side into
             memory and as-of semantics fix which side that is, so the whole
             trades table goes in every time. QuestDB binary-searches into a
             physically time-ordered column store. The ratio is interesting; the
             slopes are the reason to choose, because one of those two costs
             grows with the table rather than with the question.
  DuckDB     laptop mode, so the whole pipeline, tests and demo run with nothing
             installed.

ClickHouse was a third implementation here and has been removed. The measurement
that rejected it stands: bench/ drives ClickHouse with raw SQL over HTTP rather
than through this module, so the comparison above and in bench/RESULTS.md is
unaffected by the deletion. What would bring it back is written down in
docs/ARCHITECTURE.md and is not a performance argument: it wins the platform's
point-in-time half -- `argMax(..., ingested_at) GROUP BY grain` beats QuestDB's
`LATEST ON` with several partition keys, which scans -- and its open-source build
has replication, TLS and role-based access control, all of which are
Enterprise-only on QuestDB. Against that, a third implementation held the
interface down to what all three stores could express, which is the opposite of
what you want once a store has been chosen for its specific strengths.

The pipeline code above this layer does not know which one it is talking to,
which is what makes the acquisition and storage split worth having.
"""
from __future__ import annotations

import json
import os
import re
import time
import uuid
from abc import ABC, abstractmethod
from datetime import datetime
from pathlib import Path

import pandas as pd

from .config import questdb_settings, repo_root

_FREQ_SECONDS = {"1m": 60, "5m": 300, "15m": 900, "1h": 3600, "1d": 86400}


class Store(ABC):
    @abstractmethod
    def ensure_schema(self) -> None: ...

    @abstractmethod
    def insert(self, table: str, df: pd.DataFrame, *, batch_rows: int = 50_000) -> int: ...

    @abstractmethod
    def query(self, sql: str) -> pd.DataFrame: ...

    @abstractmethod
    def bars(self, symbol: str, start: datetime | str | None, end: datetime | str | None,
             freq: str, venue: str) -> pd.DataFrame: ...

    @abstractmethod
    def as_of(self, times: pd.DataFrame, *, symbol: str, venue: str) -> pd.DataFrame:
        """Last trade at or before each requested timestamp.

        Done in SQL, not pandas: this is the join a column store exists for, and
        an in-memory version stops working at the first real dataset.
        """
        ...

    def close(self) -> None:
        """Release whatever the store holds. Not abstract on purpose.

        A store with nothing to release is a legitimate implementation, and
        forcing every one of them to write an empty override would be ceremony.
        Callers must be able to call this unconditionally, which is what makes
        the metrics exporter able to open and close a connection per scrape.
        """
        return None


# QuestDB's SAMPLE BY unit for each of the platform's bar frequencies, paired
# with the pandas frequency that floors a timestamp onto the same grid. The two
# have to agree or `bars()` filters a calendar-aligned bucket against a boundary
# computed on a different grid, which is the kind of off-by-one that returns a
# plausible answer. `m` is minutes in QuestDB's duration syntax; `M` is months.
_QDB_SAMPLE = {
    "1m": ("1m", "1min"),
    "5m": ("5m", "5min"),
    "15m": ("15m", "15min"),
    "1h": ("1h", "1h"),
    "1d": ("1d", "1D"),
}

# The tables sql/02_questdb.sql declares, in the order it declares them. Used
# only to rewrite the DDL when a table prefix is in play; see the note on
# QuestDBStore's `table_prefix`.
_QDB_CREATE = re.compile(r"(?i)(CREATE\s+TABLE\s+IF\s+NOT\s+EXISTS\s+)([A-Za-z_][A-Za-z0-9_]*)")

_QDB_TIMESTAMP_TYPES = {"TIMESTAMP", "DATE"}
_QDB_INTEGER_TYPES = {"LONG", "INT", "SHORT", "BYTE"}
_QDB_FLOAT_TYPES = {"DOUBLE", "FLOAT"}
_QDB_NUMERIC_TYPES = _QDB_INTEGER_TYPES | _QDB_FLOAT_TYPES | {"LONG256"}

# The columns the DuckDB schema declares as `DEFAULT now()`, and which the
# loaders above this layer therefore never populate. QuestDB has no column
# DEFAULT clause at all, so QuestDBStore.insert fills them, and this is the list
# it fills. It mirrors DuckDBStore.ensure_schema exactly, by table and column,
# because a store that guessed instead -- filling
# any absent timestamp with the current time -- would paper over the case that
# matters: a caller that forgot to stamp `access_log.event_at` would get an audit
# row timed to the write rather than to the read, and nothing would say so.
_QDB_SERVER_CLOCK = {
    "trades": "ingested_at",
    "eod_bars": "ingested_at",
    "quality_events": "event_at",
    "vendor_scores": "computed_at",
}

# The columns that are calendar DAYS rather than instants, and which therefore
# have to come back from a read without a timezone.
#
# This is the one place the Store interface genuinely has to paper over a
# difference, so it is worth being precise about what the difference is. DuckDB
# has a DATE type distinct from a timestamp, and this schema uses it for every one
# of these columns; a DuckDB read hands them to pandas as tz-naive datetime64 and
# hands `ts`, `ingested_at` and the rest back as tz-aware. QuestDB has one
# temporal type. Handing everything back tz-aware would be defensible on its own
# terms and is still wrong here, because the code above this layer was written
# against DuckDB's shape: reference.py compares
# `trade_date` to `pd.Timestamp(as_of)`, which is naive, and pandas raises
# TypeError rather than coercing. So the resolution is by column name, which
# works because these names mean a day everywhere in this schema and nowhere in
# it does one of them mean an instant.
#
# The alternative -- declaring them as QuestDB's own DATE type -- was tried and
# is worse: a QuestDB DATE cannot be a designated timestamp, so `eod_bars` could
# not be ordered or partitioned on `trade_date`, which is the column every read
# of it filters on.
_QDB_DATE_COLUMNS = frozenset({
    "expiry", "trade_date", "first_trade", "last_trade",
    "known_from", "known_to", "as_of", "data_through",
})


class QuestDBStore(Store):
    """The primary production store, spoken to over HTTP.

    No client library. QuestDB's HTTP surface is three endpoints -- `/exec` for
    SQL, `/imp` for bulk CSV ingest, and `/write` for line protocol -- and
    `requests` is already a dependency, so adding a driver would buy nothing but
    another thing to pin. Two properties of that surface are load-bearing and
    were measured against a live 8.2.1 rather than assumed:

      `/exec` is GET only. A POST is answered with 404 "Method not supported",
      so the SQL travels in the URL and is bounded by the server's request header
      buffer -- 64 KB by default. Past that the connection is closed with no
      response at all, which arrives as a ConnectionError rather than as a SQL
      error. That single fact is why `as_of` stages its decision timestamps in a
      table instead of inlining them: 5,000 of them do not fit in a URL, and the
      failure mode if you try is not a message but a dropped socket.

      `/imp` reports rejected rows in its response body and returns HTTP 200
      anyway. `atomicity=abort` does not change that -- a CSV with one
      unparseable field imports the other row, answers `"status":"OK"`, and
      records `"rowsRejected":1`. So `insert` reads that field and raises. A
      loader that trusts the status code silently loses rows, which is the exact
      class of failure this platform is built to make impossible.

    `table_prefix` exists for one reason and it is worth being straight about:
    QuestDB has no databases and no schemas, so there is no namespace to put a
    test fixture in. Without a prefix, tests/test_questdb.py would have to write
    into the same `trades` table the benchmark in bench/RESULTS.md is loaded
    into, and truncating that to get a clean fixture would destroy the evidence
    this whole choice rests on. The prefix is applied by every method that names
    a table itself. `query` is a raw passthrough -- it has to be, because
    serve.py sends it SQL -- so a prefixed store's caller names prefixed tables.
    Production uses no prefix and the question does not arise.
    """

    def __init__(self, host: str | None = None, port: int | None = None,
                 *, timeout: float = 120.0, table_prefix: str = "",
                 commit_timeout: float = 30.0):
        import requests

        settings = questdb_settings()
        self.host = host or settings["host"]
        self.port = int(port or settings["port"])
        self.base = f"http://{self.host}:{self.port}"
        self.timeout = timeout
        self.commit_timeout = commit_timeout
        self.table_prefix = table_prefix
        self.session = requests.Session()
        self._columns: dict[str, list[tuple[str, str]]] = {}
        self._designated: dict[str, str | None] = {}

    # ------------------------------------------------------------------ plumbing
    def table(self, name: str) -> str:
        """This store's physical name for a logical table.

        Public because a caller that has been handed a prefixed store -- which in
        practice means the test suite -- needs it to write SQL for `query`, and
        reaching into a private attribute to build the name by hand would put the
        prefix convention in two places.
        """
        return f"{self.table_prefix}{name}"

    def _exec(self, sql: str) -> dict:
        """One SQL statement over `/exec`, with QuestDB's errors raised as errors.

        QuestDB answers a bad query with HTTP 200 and an `error` key, so a caller
        that only checks the status code sees every failure as success. The
        message is re-raised with the statement attached because a QuestDB parse
        error gives a character offset and nothing else, and an offset into a
        statement you cannot see is not a diagnostic.
        """
        response = self.session.get(
            f"{self.base}/exec", params={"query": sql}, timeout=self.timeout
        )
        try:
            payload = response.json()
        except ValueError:
            response.raise_for_status()
            raise RuntimeError(f"QuestDB returned non-JSON for: {sql[:200]}") from None
        if "error" in payload:
            raise RuntimeError(f"QuestDB: {payload['error']} in: {sql[:500]}")
        return payload

    def _table_columns(self, table: str) -> list[tuple[str, str]]:
        """(name, type) for a table, cached, plus which column is the timestamp."""
        if table not in self._columns:
            rows = self._exec(f"SHOW COLUMNS FROM {table}")["dataset"]
            self._columns[table] = [(r[0], r[1]) for r in rows]
            designated = [r[0] for r in rows if r[6]]
            self._designated[table] = designated[0] if designated else None
        return self._columns[table]

    def _wait_for_commit(self, table: str) -> None:
        """Block until the WAL apply job has caught up with what we just wrote.

        This is the one place QuestDB needs code that DuckDB does not. A write to
        a WAL table is acknowledged when it reaches the sequencer, and becomes
        visible to readers when a background job applies it, so `/imp` returning
        does not mean a SELECT will see the rows. DuckDB is synchronous and so
        read-your-write for free; without this, QuestDB would not be, and the
        platform is full of places that depend on it. `advance_cursor` writes a
        cursor and the next window reads it back; the contract gate writes a first
        sighting and the next run reads it; every test asserts on what it just
        inserted. An eventually-visible write turns all of those into races that
        pass on a laptop and fail under load.

        `suspended` is checked rather than waited on: a suspended WAL never
        catches up, so polling for it is an outage dressed as a slow query.
        """
        deadline = time.monotonic() + self.commit_timeout
        query = (
            "SELECT writerTxn, sequencerTxn, suspended, errorMessage "
            f"FROM wal_tables() WHERE name = '{_quote(table)}'"
        )
        while True:
            rows = self._exec(query)["dataset"]
            if not rows:
                return  # not a WAL table, so the write was already synchronous
            writer, sequencer, suspended, message = rows[0]
            if suspended:
                raise RuntimeError(
                    f"QuestDB WAL for {table} is suspended and will not catch up: "
                    f"{message}"
                )
            if writer is not None and sequencer is not None and writer >= sequencer:
                return
            if time.monotonic() > deadline:
                raise TimeoutError(
                    f"QuestDB WAL for {table} did not apply within "
                    f"{self.commit_timeout}s (writer txn {writer}, "
                    f"sequencer txn {sequencer})"
                )
            time.sleep(0.005)

    # ------------------------------------------------------------------ schema
    def ensure_schema(self) -> None:
        sql = (repo_root() / "sql" / "02_questdb.sql").read_text(encoding="utf-8")
        if self.table_prefix:
            sql = _QDB_CREATE.sub(
                lambda m: f"{m.group(1)}{self.table_prefix}{m.group(2)}", sql
            )
        for stmt in _qdb_statements(sql):
            self._exec(stmt)
        self._columns.clear()
        self._designated.clear()

    # ------------------------------------------------------------------ write
    def insert(self, table: str, df: pd.DataFrame, *, batch_rows: int = 50_000) -> int:
        """Bulk-load a frame through `/imp`, one CSV per batch.

        Only the columns the frame actually carries are sent, named in the header,
        which is what lets a caller hand over a frame with extra or missing
        columns and get the schema's own behaviour for the rest. `/imp` fills the
        columns a CSV omits with nulls and accepts them in any order.

        The columns the DuckDB schema declares `DEFAULT now()` are filled here,
        from a clock read off the QuestDB server, because QuestDB has no column
        DEFAULT clause. It has to be the server's clock, not the loader's:
        otherwise "newest ingest" quietly becomes "newest loader clock", and two
        loaders with a few seconds of drift between them resolve the same
        correction in opposite directions. One clock read per batch keeps that
        property without a round trip per row.
        """
        if df.empty:
            return 0
        target = self.table(table)
        columns = self._table_columns(target)
        types = dict(columns)
        present = [name for name, _ in columns if name in df.columns]

        stamped = _QDB_SERVER_CLOCK.get(table)
        frame = df[present].copy() if present else pd.DataFrame(index=df.index)
        if stamped and stamped in types and stamped not in present:
            frame[stamped] = self._server_now()
            present = present + [stamped]
        if not present:
            return 0

        designated = self._designated.get(target)
        if designated and designated not in present:
            # Not a defensive nicety. A CSV that omits the designated timestamp
            # makes QuestDB 8.2.1 close the connection without a response, so the
            # caller sees a ConnectionError from `requests` and no hint at all
            # about which column was missing.
            raise ValueError(
                f"{target} is ordered by {designated}, which is not in the frame "
                f"({list(df.columns)}); QuestDB cannot store a row without it"
            )

        total = 0
        for start in range(0, len(frame), batch_rows):
            chunk = frame.iloc[start : start + batch_rows]
            total += self._imp(target, chunk, types)
        self._wait_for_commit(target)
        return total

    def _server_now(self) -> pd.Timestamp:
        return pd.Timestamp(self._exec("SELECT now() AS n")["dataset"][0][0])

    def _imp(self, table: str, chunk: pd.DataFrame, types: dict[str, str]) -> int:
        body = _qdb_csv(chunk, types)
        response = self.session.post(
            f"{self.base}/imp",
            params={"name": table, "fmt": "json", "forceHeader": "true",
                    "atomicity": "abort", "overwrite": "false"},
            files={"data": ("data.csv", body, "text/csv")},
            timeout=self.timeout,
        )
        response.raise_for_status()
        payload = response.json()
        rejected = int(payload.get("rowsRejected") or 0)
        if rejected:
            # `atomicity=abort` does not abort: QuestDB 8.2.1 imports the rows it
            # could parse, answers HTTP 200 with "status":"OK", and reports the
            # rest here. Raising is the only way a caller finds out.
            failing = [c["name"] for c in payload.get("columns", []) if c.get("errors")]
            raise RuntimeError(
                f"QuestDB rejected {rejected} of {len(chunk)} rows for {table}; "
                f"columns with errors: {failing or 'unreported'}"
            )
        return int(payload.get("rowsImported") or 0)

    # ------------------------------------------------------------------ read
    def query(self, sql: str) -> pd.DataFrame:
        payload = self._exec(sql)
        return _qdb_frame(payload)

    def bars(self, symbol, start, end, freq="1m", venue="binance") -> pd.DataFrame:
        """OHLCV bars, filtered on the bucket rather than on the raw trade time.

        `SAMPLE BY` reads the trades column directly. There is no bars table and
        no materialised view here, and the reason is in sql/02_questdb.sql: DEDUP
        has already made a replayed print impossible to double count, so the whole
        argument that forces a ClickHouse minute view to be refreshable rather
        than insert-triggered -- an insert-triggered view aggregates each inserted
        block once, so a replayed trade is counted twice and no later merge undoes
        it -- does not arise. No FINAL, no DISTINCT ON, no refresh lag.

        Two QuestDB specifics here, both verified against a live server and both
        of which produce a plausible wrong answer if you get them backwards.

        The first is that the range filter is on the BUCKET and not on `ts`.
        Filtering the raw trade time means a range starting mid-bucket returns
        that bucket built from only the trades inside the range, labelled as a
        whole bar with nothing marking it. A partial bar is worse than a missing
        one, because the missing one is visible. So the `ts` predicate is widened
        to the enclosing bucket boundaries -- which is all it is for, letting
        QuestDB prune partitions and scan an interval instead of a table -- and
        the bucket itself is then filtered in the outer query, exactly as
        DuckDBStore does it.

        The second is why `SAMPLE BY ... FROM ... TO ...` is not used for that,
        despite being the clause that looks designed for it. `FROM` does not
        filter the range on a calendar grid: it ANCHORS the grid. Asked for
        `SAMPLE BY 1m FROM '10:00:30' ALIGN TO CALENDAR` against trades from
        10:00, the live server returned buckets stamped 10:00:30 and 10:01:30.
        `ALIGN TO CALENDAR` did not override it. Those are one-minute bars of
        real trades on a grid nobody asked for, and a consumer joining them to
        anything else on the platform would be joining bars that do not line up.
        Passing a caller's arbitrary start into `FROM` would silently relabel
        every bar in the result.
        """
        sample, floor = _QDB_SAMPLE[freq]
        trades_table = self.table("trades")
        where = [f"symbol = '{_quote(symbol)}'", f"venue = '{_quote(venue)}'"]
        outer = []
        if start:
            begin = pd.Timestamp(start)
            where.append(f"ts >= '{_qdb_literal(begin.floor(floor))}'")
            outer.append(f"bucket >= '{_qdb_literal(begin)}'")
        if end:
            finish = pd.Timestamp(end)
            where.append(f"ts < '{_qdb_literal(finish.ceil(floor))}'")
            outer.append(f"bucket < '{_qdb_literal(finish)}'")
        having = f"WHERE {' AND '.join(outer)}" if outer else ""
        return self.query(
            f"""
            SELECT bucket, open, high, low, close, volume, trades, notional
            FROM (
                SELECT ts AS bucket,
                       first(price) AS open,
                       max(price) AS high,
                       min(price) AS low,
                       last(price) AS close,
                       sum(qty) AS volume,
                       count() AS trades,
                       sum(price * qty) AS notional
                FROM {trades_table}
                WHERE {' AND '.join(where)}
                SAMPLE BY {sample} ALIGN TO CALENDAR
            ) {having}
            ORDER BY bucket
            """
        )

    def as_of(self, times: pd.DataFrame, *, symbol: str, venue: str = "binance") -> pd.DataFrame:
        """Last trade at or before each requested timestamp.

        This is the query the benchmark measured and the reason QuestDB is the
        primary store. Four things about it are deliberate.

        **ASOF, not LT.** QuestDB has both, and they differ by exactly one
        boundary: `ASOF JOIN` takes the last row at or before, `LT JOIN` takes the
        last row strictly before. The platform's rule is at-or-before, so ASOF is
        correct -- but this is the single line where a look-ahead bug lives, so it
        is worth recording what the live server returns for each. Against trades
        at 10:00:00 and 10:00:10 and a decision at exactly 10:00:10, ASOF returned
        the 10:00:10 print and LT returned the 10:00:00 one. Neither leaks the
        future; LT would silently discard a trade that happened at the decision
        instant, which on a platform where decisions are generated from trade
        times is not a rare case.

        **The ON clause is equality only.** There is no inequality term to get
        wrong, because the time relationship is implicit in the two tables'
        designated timestamps. That is a genuinely better shape than ClickHouse's
        `ON ... AND q.query_ts >= t.ts`, where the inequality is written out and
        can therefore be written backwards.

        **The decision timestamps are staged in a table, not inlined.** `/exec`
        is GET only, so the SQL rides in the URL, and the server closes the
        connection without a response once it exceeds the ~64 KB header buffer --
        measured at around 480 decisions for this query's shape. Staging costs a
        table create, a CSV upload and a drop, which measured 36 ms for 3
        timestamps and 76 ms for 5,000 against the live server. That is a fixed
        floor a single-timestamp lookup does not pay on DuckDB, and
        it buys a path whose semantics do not change with N. Two code paths for
        one join is where a divergence hides.

        The staging table is `PARTITION BY DAY BYPASS WAL`: partitioned so
        out-of-order timestamps are accepted (a non-partitioned QuestDB table
        rejects them, and `/imp` reports that as rejected rows rather than as an
        error -- 5,000 unsorted rows imported as 142), and non-WAL so the write
        is synchronous and needs no commit wait.

        **No TOLERANCE.** QuestDB 8.3 adds `TOLERANCE` to bound how far back the
        join will look, which would map onto this platform's
        `max_staleness_seconds` and terminate the backward scan early. 8.2.1
        rejects the token, so it is not used here -- but it is also not simply a
        missing optimisation, and `staleness_bound_seconds` is opt-in for a
        reason. TOLERANCE makes a too-old match come back as NULL, and
        `as_of_prices` in research.py needs the match's timestamp precisely so it
        can report HOW stale the price it refused to return was. Bounding inside
        the join would hand the consumer a hole with no age on it, which is a
        worse answer than the one this platform already gives.
        """
        columns = ["query_ts", "trade_ts", "price", "trade_id"]
        if times.empty:
            return pd.DataFrame({c: pd.Series(dtype="object") for c in columns})

        requested = pd.to_datetime(times["query_ts"], utc=True).sort_values()
        if not self._has_trades(symbol, venue):
            return self._no_matches(requested)

        staging = f"mdp_asof_{uuid.uuid4().hex[:16]}"
        frame = pd.DataFrame({
            "query_ts": requested.reset_index(drop=True),
            "symbol": symbol,
            "venue": venue,
        })
        self._exec(
            f"CREATE TABLE {staging} (query_ts TIMESTAMP, symbol SYMBOL, venue SYMBOL) "
            f"TIMESTAMP(query_ts) PARTITION BY DAY BYPASS WAL"
        )
        try:
            self._imp(staging, frame, {"query_ts": "TIMESTAMP", "symbol": "SYMBOL",
                                       "venue": "SYMBOL"})
            return self.query(
                f"""
                SELECT q.query_ts AS query_ts, t.ts AS trade_ts,
                       t.price AS price, t.trade_id AS trade_id
                FROM {staging} q
                ASOF JOIN {self.table('trades')} t ON (symbol, venue)
                ORDER BY q.query_ts
                """
            )
        finally:
            # A staging table that outlives its query is a leak with a name, and
            # this one is named after a UUID, so nobody would ever find it.
            self._exec(f"DROP TABLE IF EXISTS {staging}")

    def _has_trades(self, symbol: str, venue: str) -> bool:
        """Does this key have any trades at all, ever?

        Not an optimisation. QuestDB's ASOF JOIN degenerates when a join key
        matches nothing: measured against the 20,000,000-row benchmark table, the
        same 5,000 decisions answered in 0.062 s for a symbol that exists and hit
        the server's 60-second timeout for one that does not. The backward scan
        looking for a key that is never going to appear does not terminate at a
        partition boundary, so the cost stops being "per decision, a binary
        search" and becomes "per decision, the table".

        The practical shape of that is a typo. `md.as_of("BTCUSD", ...)` -- one
        character short of a symbol this platform carries -- is a query that takes
        a minute and then fails, where DuckDB returns a column of NULLs
        immediately. So the key is probed first, which costs 2 ms when it
        matches and about 100 ms when it does not, and a key with no trades is
        answered without the join. The answer is identical either way: every match
        would have been NULL.
        """
        found = self._exec(
            f"SELECT ts FROM {self.table('trades')} "
            f"WHERE symbol = '{_quote(symbol)}' AND venue = '{_quote(venue)}' LIMIT 1"
        )
        return bool(found.get("dataset"))

    @staticmethod
    def _no_matches(requested: pd.Series) -> pd.DataFrame:
        """One unmatched row per requested timestamp, shaped like a real answer.

        The row survives and only the price is missing, which is the contract
        `as_of_prices` is written against: a consumer has to be able to see that
        their timestamp had no usable price, and a dropped row is invisible.
        """
        return pd.DataFrame({
            "query_ts": requested.reset_index(drop=True),
            "trade_ts": pd.Series(pd.NaT, index=range(len(requested)),
                                  dtype="datetime64[us, UTC]"),
            "price": pd.Series(float("nan"), index=range(len(requested)), dtype="float64"),
            "trade_id": pd.Series(float("nan"), index=range(len(requested)), dtype="float64"),
        })

    def close(self) -> None:
        self.session.close()


def _qdb_statements(sql: str) -> list[str]:
    """Split a schema file into statements, comments removed first.

    Splitting on `;` with the comments left in works only as long as no comment
    contains a semicolon, and the first sentence that uses one cuts a CREATE TABLE
    in half -- the error the server returns is then a parse error pointing at
    English. sql/02_questdb.sql has several, so the comments come out before the
    split. They are written for whoever reads the file, not for the server.
    """
    stripped = []
    for line in sql.splitlines():
        marker = line.find("--")
        # No string literal in this DDL contains a double dash, so the first
        # occurrence on a line is always the comment.
        stripped.append(line if marker < 0 else line[:marker])
    return [s.strip() for s in "\n".join(stripped).split(";") if s.strip()]


def _qdb_literal(value) -> str:
    """A pandas timestamp as QuestDB's microsecond ISO-8601 literal.

    QuestDB is microsecond-native and pandas is nanosecond-native, so this
    truncates. Truncation rather than rounding is the safe direction for a
    decision timestamp -- moving it earlier can only ever exclude a trade, never
    admit one from the future. It is not the safe direction for a trade
    timestamp, which `insert` also truncates: a print at 12.2000004 s stored as
    12.200000 becomes eligible for a decision at 12.2000001. The exposure is
    under one microsecond and no venue this platform reads publishes sub-
    microsecond times, but it is a real edge and pretending otherwise is how it
    gets rediscovered.
    """
    ts = pd.Timestamp(value)
    if ts.tzinfo is not None:
        ts = ts.tz_convert("UTC").tz_localize(None)
    return ts.strftime("%Y-%m-%dT%H:%M:%S.%f") + "Z"


def _qdb_csv(chunk: pd.DataFrame, types: dict[str, str]) -> str:
    """A frame as the CSV `/imp` wants: ISO-8601 timestamps, empty for null.

    Two column kinds are converted here and the rest are left to pandas' own CSV
    writer, which is both faster than a Python loop over every value and exactly
    as faithful: for a float it emits `repr`, the shortest string that round-trips,
    so a price keeps every tick and a notional keeps every cent.

    Timestamps are converted because pandas writes a tz-aware datetime as
    `2026-09-20 10:00:00+00:00` -- which QuestDB does in fact parse -- but writes a
    NaT in that same column as the literal string `NaT`, which `/imp` counts as a
    rejected row rather than as a null. `first_trade` on contract_reference is NaT
    on every row the reference builder writes, so that is not a hypothetical.

    Booleans are converted because QuestDB has no null boolean, so a missing flag
    has to become one of the two values rather than an empty field. False is right
    for every boolean on this schema -- `is_buyer_maker`, `is_front`, `allowed` --
    and choosing it here keeps the loader's behaviour the same as DuckDB's, which
    stores NULL and reads it falsy.
    """
    out = chunk.copy()
    for column in out.columns:
        kind = (types.get(column) or "").upper()
        series = out[column]
        if kind in _QDB_TIMESTAMP_TYPES or pd.api.types.is_datetime64_any_dtype(series):
            values = pd.to_datetime(series, utc=True, errors="coerce")
            # Microseconds, because QuestDB is microsecond-native; see the note on
            # truncation in _qdb_literal, which formats a single value the same way.
            formatted = values.dt.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
            out[column] = formatted.where(values.notna(), None)
        elif kind == "BOOLEAN":
            out[column] = series.astype("boolean").fillna(False).map(
                {True: "true", False: "false"}
            )
        elif kind in _QDB_INTEGER_TYPES and pd.api.types.is_float_dtype(series):
            # A float column landing in a LONG: `/imp` rejects `7.0`, and it
            # reports the rejection against every column in the row rather than
            # against this one, so the error message points at the timestamp as
            # often as at the number. Nullable Int64 from the contract layer is
            # already written as `7`, so this only catches a frame that lost its
            # integer dtype to a NaN somewhere upstream.
            out[column] = series.round().astype("Int64")
    return out.to_csv(index=False, na_rep="", lineterminator="\n")


def _qdb_frame(payload: dict) -> pd.DataFrame:
    """A `/exec` JSON response as a typed DataFrame.

    The types come from the response's own column metadata rather than from
    pandas inference, so an all-null timestamp column still arrives as a
    timestamp column and an empty result still has the right columns. Callers
    like `_latest_per_grain` check for a column by name and raise when it is
    missing, so an empty frame with no columns reads as a different failure than
    the one that happened.
    """
    columns = [c["name"] for c in payload.get("columns", [])]
    rows = payload.get("dataset") or []
    frame = pd.DataFrame(rows, columns=columns) if columns else pd.DataFrame()
    for column in payload.get("columns", []):
        name, kind = column["name"], column["type"].upper()
        if kind in _QDB_TIMESTAMP_TYPES:
            parsed = pd.to_datetime(frame[name], utc=True, errors="coerce")
            frame[name] = (
                parsed.dt.tz_localize(None) if name in _QDB_DATE_COLUMNS else parsed
            )
        elif kind in _QDB_NUMERIC_TYPES:
            frame[name] = pd.to_numeric(frame[name], errors="coerce")
        elif kind == "BOOLEAN":
            frame[name] = frame[name].astype("boolean")
    return frame


class DuckDBStore(Store):
    """Laptop mode. Same interface, no server, so `make demo` always works."""

    def __init__(self, path: str | Path | None = None, read_only: bool = False):
        import duckdb

        self.path = str(path or repo_root() / "data" / "mdp.duckdb")
        Path(self.path).parent.mkdir(parents=True, exist_ok=True)
        # read_only is for observers: the metrics exporter should be incapable of
        # writing to the thing it is measuring, not merely disinclined to.
        self.con = duckdb.connect(self.path, read_only=read_only)

    def ensure_schema(self) -> None:
        self.con.execute(
            """
            CREATE TABLE IF NOT EXISTS trades (
                venue VARCHAR, symbol VARCHAR, trade_id BIGINT, ts TIMESTAMPTZ,
                price DOUBLE, qty DOUBLE, is_buyer_maker BOOLEAN,
                ingested_at TIMESTAMPTZ DEFAULT now());
            CREATE TABLE IF NOT EXISTS eod_bars (
                exchange VARCHAR, symbol VARCHAR, expiry DATE, trade_date DATE,
                open DOUBLE, high DOUBLE, low DOUBLE, close DOUBLE, settle DOUBLE,
                volume DOUBLE, open_interest DOUBLE, ingested_at TIMESTAMPTZ DEFAULT now());
            -- trade_date is the day the choice applies to; known_from and
            -- known_to are the window during which we believed it. Without the
            -- first of those the table cannot answer "which contract was front
            -- on day X", which is the only question it exists to answer, and
            -- for a long time it did not have the column.
            CREATE TABLE IF NOT EXISTS contract_reference (
                exchange VARCHAR, symbol VARCHAR, expiry DATE, trade_date DATE,
                first_trade DATE, last_trade DATE, is_front BOOLEAN,
                known_from DATE, known_to DATE);
            CREATE TABLE IF NOT EXISTS quality_events (
                run_id VARCHAR, dataset VARCHAR, source VARCHAR, check_name VARCHAR,
                status VARCHAR, rows_in BIGINT, rows_out BIGINT, detail VARCHAR,
                event_at TIMESTAMPTZ DEFAULT now());
            CREATE TABLE IF NOT EXISTS vendor_scores (
                as_of DATE, source VARCHAR, freshness DOUBLE, completeness DOUBLE,
                accuracy DOUBLE, score DOUBLE, status VARCHAR, detail VARCHAR,
                computed_at TIMESTAMPTZ DEFAULT now());
            CREATE TABLE IF NOT EXISTS schema_versions (
                dataset VARCHAR, schema_version VARCHAR, accepted_by VARCHAR,
                first_seen TIMESTAMPTZ);
            CREATE TABLE IF NOT EXISTS ingest_cursors (
                source VARCHAR, position_ts TIMESTAMPTZ, position_id BIGINT,
                rows_at_position BIGINT, updated_at TIMESTAMPTZ);
            CREATE TABLE IF NOT EXISTS contract_specs (
                exchange VARCHAR, symbol VARCHAR, lot_size DOUBLE, tick_size DOUBLE,
                price_unit VARCHAR, quantity_unit VARCHAR,
                known_from DATE, known_to DATE);
            CREATE TABLE IF NOT EXISTS access_log (
                event_at TIMESTAMPTZ, consumer VARCHAR, dataset VARCHAR,
                action VARCHAR, allowed BOOLEAN, detail VARCHAR);
            CREATE TABLE IF NOT EXISTS model_runs (
                run_id VARCHAR, model VARCHAR, params VARCHAR, code_version VARCHAR,
                data_through DATE, metrics VARCHAR, started_at TIMESTAMPTZ,
                finished_at TIMESTAMPTZ);
            """
        )

    def insert(self, table: str, df: pd.DataFrame, *, batch_rows: int = 50_000) -> int:
        if df.empty:
            return 0
        cols = [r[0] for r in self.con.execute(f"DESCRIBE {table}").fetchall()]
        # Insert only the columns the frame actually carries, and name them.
        #
        # The previous version filled every missing column with None and did
        # `INSERT ... SELECT *`, which writes an explicit NULL and therefore
        # overrides the column DEFAULT. The visible consequence was that
        # `ingested_at` was NULL on every row ever loaded, which in turn made
        # `_latest_per_grain` unable to tell a correction from the row it
        # corrected. A default that never fires is worse than no default,
        # because the schema says it is there.
        present = [c for c in cols if c in df.columns]
        if not present:
            return 0
        frame = df[present].copy()
        self.con.register("_incoming", frame)
        column_list = ", ".join(f'"{c}"' for c in present)
        self.con.execute(f"INSERT INTO {table} ({column_list}) SELECT * FROM _incoming")
        self.con.unregister("_incoming")
        return len(frame)

    def query(self, sql: str) -> pd.DataFrame:
        return self.con.execute(sql).fetch_df()

    def bars(self, symbol, start, end, freq="1m", venue="binance") -> pd.DataFrame:
        """OHLCV bars, filtered on the bucket rather than on the raw trade time.

        The distinction matters and used to be wrong here. Filtering raw `ts`
        means a range starting mid-bucket returns that bucket built from only
        the trades inside the range, labelled as a whole bar with no marker on
        it. A caller asking for 10:00:30 onwards got a 10:00 bar with half the
        volume and a wrong open, and nothing said so. QuestDBStore filters on the
        bucket and drops it, so the two stores also disagreed.

        A partial bar is worse than a missing one: the missing one is visible.
        """
        seconds = _FREQ_SECONDS[freq]
        where = [f"symbol = '{symbol}'", f"venue = '{venue}'"]
        bucket_where = []
        if start:
            bucket_where.append(f"bucket >= TIMESTAMPTZ '{pd.Timestamp(start)}'")
        if end:
            bucket_where.append(f"bucket < TIMESTAMPTZ '{pd.Timestamp(end)}'")
        clause = " AND ".join(where)
        having = f"WHERE {' AND '.join(bucket_where)}" if bucket_where else ""
        return self.query(
            f"""
            WITH d AS (
              SELECT time_bucket(INTERVAL {seconds} SECOND, ts) AS bucket, ts, price, qty
              FROM (SELECT DISTINCT ON (venue, symbol, trade_id) venue, symbol, trade_id,
                           ts, price, qty FROM trades WHERE {clause})
            ), b AS (
              SELECT bucket,
                     first(price ORDER BY ts) AS open,
                     max(price) AS high,
                     min(price) AS low,
                     last(price ORDER BY ts) AS close,
                     sum(qty) AS volume,
                     count(*) AS trades,
                     sum(price * qty) AS notional
              FROM d GROUP BY bucket
            )
            SELECT * FROM b {having} ORDER BY bucket
            """
        )

    def as_of(self, times: pd.DataFrame, *, symbol: str, venue: str = "binance") -> pd.DataFrame:
        frame = times.copy()
        frame["query_ts"] = pd.to_datetime(frame["query_ts"], utc=True)
        self.con.register("_queries", frame)
        try:
            # ASOF JOIN with >= takes the last row at or before. Not the nearest:
            # nearest would match a print from after the query time, which is the
            # bug this whole function exists to prevent.
            return self.con.execute(
                """
                SELECT q.query_ts AS query_ts, t.ts AS trade_ts,
                       t.price AS price, t.trade_id AS trade_id
                FROM _queries AS q
                ASOF LEFT JOIN (
                    SELECT DISTINCT ON (trade_id) ts, price, trade_id
                    FROM trades
                    WHERE symbol = ? AND venue = ?
                ) AS t
                ON q.query_ts >= t.ts
                ORDER BY q.query_ts
                """,
                [symbol, venue],
            ).fetch_df()
        finally:
            self.con.unregister("_queries")

    def close(self) -> None:
        self.con.close()


def _quote(value: str) -> str:
    """Escape a value destined for a SQL string literal.

    These values come from config rather than from the open internet, so this is
    not guarding against an attacker. It is guarding against a symbol with an
    apostrophe in it turning a query into a syntax error, which is the failure
    that actually happens and which reads as a database outage rather than as a
    bad symbol.
    """
    return str(value).replace("\\", "\\\\").replace("'", "''")


def get_store(kind: str | None = None, **kwargs) -> Store:
    """Open the requested store, or the default one.

    The default stays `duckdb` even though QuestDB is the primary production
    store, and that is deliberate. This function is what `make demo`, the test
    suite and a new joiner's first `python -m mdp.cli` all go through, and a
    default that needs a server running turns "clone and run" into "clone, read
    the README, start docker, wait, run". Production names its store explicitly
    with MDP_STORE=questdb, which is one line in the compose file and in the
    deployment, and an explicit choice in the place that cares is worth more than
    a default that is right for one of three audiences.
    """
    kind = (kind or os.getenv("MDP_STORE") or "duckdb").lower()
    if kind == "questdb":
        return QuestDBStore(**kwargs)
    if kind == "duckdb":
        return DuckDBStore(**kwargs)
    raise ValueError(f"unknown store {kind!r}")


def json_dumps(obj) -> str:
    return json.dumps(obj, default=str, sort_keys=True)
