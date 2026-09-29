-- Schema for the market data platform on QuestDB, which is the primary
-- production store.
--
-- Why this file exists at all, stated first because the reason is a measurement
-- and not a preference. The query this platform exists to make correct is the
-- as-of join: "the last trade at or before each of N decision timestamps".
-- bench/RESULTS.md measures it on 20,000,000 trades. QuestDB answered in 0.029 s
-- at 5,000 decisions and 0.081 s at 50,000, with +297 MB of resident memory.
-- ClickHouse answered in 1.06 s at both sizes with +839 MB, because it hash-builds
-- the entire right-hand side into memory and as-of semantics fix which side that
-- is, so the whole trades table goes in every time. The ratio at 20M rows is
-- interesting; the shape is the reason to choose. QuestDB's cost grows with the
-- question because it binary-searches a physically time-ordered column store,
-- and ClickHouse's grows with the table. One of those two terms is the one that
-- matters at two billion rows.
--
-- Three things about QuestDB shape this file. Each of them is a real property of
-- QuestDB and a real difference from how these tables would be declared on
-- ClickHouse, which is the engine this schema was measured against, not a
-- translation of another engine's DDL:
--
--   1. Every table has a DESIGNATED TIMESTAMP, and rows are stored physically
--      ordered by it. That is what makes the as-of join a binary search. It is
--      also a constraint: the designated timestamp cannot be null, and it must
--      be a member of any DEDUP key set.
--
--   2. DEDUP UPSERT KEYS on a WAL table enforces the grain at commit, not
--      eventually. This is strictly better than ClickHouse's ReplacingMergeTree
--      for the trades table: there is no window during which a replayed print
--      sits in the table waiting for a merge, so nothing here needs the
--      equivalent of FINAL, and the as-of join cannot match a superseded row.
--      The read path gets faster and more correct at the same time, which is not
--      a trade-off that comes up often.
--
--   3. There is no CREATE DATABASE and no schema namespace. Tables live in one
--      flat space, so these names are unqualified. That is why src/mdp/serve.py
--      can send the same unqualified SQL to both stores.
--
-- QuestDB has no column DEFAULT clause, so `ingested_at` cannot be declared as
-- `DEFAULT now()` the way DuckDBStore.ensure_schema declares it. QuestDBStore.insert
-- fills it instead, from a clock read off this server rather than off the
-- loader's machine, which preserves the property that matters: "newest ingest"
-- means newest ingest and not newest vendor clock.

-- ---------------------------------------------------------------------------
-- Raw trade prints. The table the whole benchmark is about.
--
-- The designated timestamp is `ts` and the partitioning is by day, which is what
-- turns a decision timestamp into a partition lookup followed by a binary search
-- inside one time-ordered column rather than a scan.
--
-- DEDUP UPSERT KEYS is the declared unique grain of a print, plus `ts` because
-- QuestDB requires the designated timestamp in the key set. A feed that replays
-- the last N trades on reconnect therefore writes them and they collapse at
-- commit, which is exactly what ReplacingMergeTree promises eventually and what
-- DuckDB does not do at all.
--
-- The consequence of `ts` being in the key set has to be said out loud, because
-- it is the one place this is weaker than the ClickHouse table. On ClickHouse the
-- sorting key is (venue, symbol, trade_id) and `ts` is deliberately NOT in it, so
-- a resend that CORRECTS the timestamp of a print replaces the original. Here it
-- cannot: a corrected `ts` is a different dedup key, so the correction lands
-- beside the original and both survive. A vendor that corrects trade timestamps
-- needs the old row retired explicitly, and QuestDB's open-source build has no
-- row-level DELETE to do it with — the repair is a partition rewrite. That is a
-- runbook entry, not a schema change, and it is the same class of hole as
-- ClickHouse's "a correction that moves ts across a month boundary never
-- collapses", only wider.
CREATE TABLE IF NOT EXISTS trades (
    venue          SYMBOL CAPACITY 16 CACHE,
    symbol         SYMBOL CAPACITY 4096 CACHE,
    trade_id       LONG,
    ts             TIMESTAMP,
    price          DOUBLE,
    qty            DOUBLE,
    is_buyer_maker BOOLEAN,
    ingested_at    TIMESTAMP
) TIMESTAMP(ts) PARTITION BY DAY WAL
DEDUP UPSERT KEYS(ts, venue, symbol, trade_id);

-- ---------------------------------------------------------------------------
-- No bars table and no materialised view, which is the second deliberate
-- divergence from how the same platform is built on ClickHouse.
--
-- There, a minute bar has to be a maintained table, and the view that maintains it
-- has to be REFRESHABLE rather than insert-triggered. An insert-triggered view
-- sees the raw inserted block, fires before ReplacingMergeTree has collapsed
-- anything, and therefore counts a replayed trade twice in volume, trades and
-- notional forever.
--
-- QuestDB does not need either half of that argument. `SAMPLE BY` aggregates the
-- time-ordered trades column directly at query time, and DEDUP has already made
-- a replayed print impossible to double count, so the bar is computed from one
-- row per print with no merge to wait for. There is no stale-bar window, nothing
-- to refresh, and no second copy of the data to keep consistent. That whole class
-- of bug does not exist here.
--
-- What is given up is a precomputed answer. A minute bar over a wide range costs
-- a scan of the trades in that range rather than a read of an aggregate. On this
-- platform's read patterns that is the better trade, and if it stops being one
-- the fix is a materialised view over the same SAMPLE BY, not a return to
-- insert-triggered aggregation.

-- ---------------------------------------------------------------------------
-- End of day exchange data, one row per contract per day.
--
-- This table needs the OPPOSITE of the trades table's behaviour and the schema
-- has to say so, because the obvious thing to do here is wrong. An exchange
-- restates a settlement days later, and that correction must NOT overwrite the
-- row it corrects: the original is what a backtest published last week saw, and
-- silently replacing it changes an answer that is already in a report with
-- nothing anywhere recording that it moved.
--
-- So `ingested_at` is IN the dedup key set. A correction carries a later ingest
-- time, which makes it a different key, which makes it a new row. Both rows
-- survive and `_latest_per_grain` in src/mdp/serve.py resolves them on read by
-- keeping the highest `ingested_at` per (exchange, symbol, expiry, trade_date).
-- Point-in-time history is kept in the table; the current answer is computed on
-- the way out.
--
-- `trade_date` is the designated timestamp rather than `ingested_at` because
-- every read filters on it — serve.py sends `trade_date >= DATE '...'` — and the
-- designated timestamp is the column that filter can prune on.
CREATE TABLE IF NOT EXISTS eod_bars (
    exchange      SYMBOL CAPACITY 64 CACHE,
    symbol        SYMBOL CAPACITY 4096 CACHE,
    expiry        TIMESTAMP,
    trade_date    TIMESTAMP,
    open          DOUBLE,
    high          DOUBLE,
    low           DOUBLE,
    close         DOUBLE,
    settle        DOUBLE,
    volume        DOUBLE,
    open_interest DOUBLE,
    ingested_at   TIMESTAMP
) TIMESTAMP(trade_date) PARTITION BY YEAR WAL
DEDUP UPSERT KEYS(trade_date, exchange, symbol, expiry, ingested_at);

-- ---------------------------------------------------------------------------
-- Reference data. A futures price series means nothing without the roll rules,
-- and these have to be point in time: a backtest run in June must see June's map.
--
-- `trade_date` is the day the choice applies to; `known_from` and `known_to` are
-- the window during which we believed it, so a settlement corrected later closes
-- the old row rather than overwriting it. Without `trade_date` this table cannot
-- answer "which contract was front on day X", which is the only question it
-- exists to answer.
--
-- The dedup grain is one belief per symbol-day per validity window, enforced at
-- commit, so re-running the reference build is idempotent immediately rather than
-- eventually.
CREATE TABLE IF NOT EXISTS contract_reference (
    exchange    SYMBOL CAPACITY 64 CACHE,
    symbol      SYMBOL CAPACITY 4096 CACHE,
    expiry      TIMESTAMP,
    trade_date  TIMESTAMP,
    first_trade TIMESTAMP,
    last_trade  TIMESTAMP,
    is_front    BOOLEAN,
    known_from  TIMESTAMP,
    known_to    TIMESTAMP
) TIMESTAMP(trade_date) PARTITION BY YEAR WAL
DEDUP UPSERT KEYS(trade_date, exchange, symbol, known_from);

-- ---------------------------------------------------------------------------
-- Operational tables: what ran, what it produced, and how the source behaved.

-- Quality events are an append-only log and get no dedup: two identical checks
-- in the same run are two facts about that run, and collapsing them would hide
-- a retry. `status` is a SYMBOL rather than an enum because QuestDB has no enum
-- type; the values are still only ever pass/warn/fail, and the contract that
-- says so lives in src/mdp/quality.py.
CREATE TABLE IF NOT EXISTS quality_events (
    run_id     VARCHAR,
    dataset    SYMBOL CAPACITY 64 CACHE,
    source     SYMBOL CAPACITY 64 CACHE,
    check_name SYMBOL CAPACITY 128 CACHE,
    status     SYMBOL CAPACITY 8 CACHE,
    rows_in    LONG,
    rows_out   LONG,
    detail     VARCHAR,
    event_at   TIMESTAMP
) TIMESTAMP(event_at) PARTITION BY MONTH WAL;

-- Vendor quality scores. `as_of` is a day and a source can be scored every
-- minute, so many rows share (source, as_of) and the readers -- metrics.py and
-- monitor.py -- sort by `as_of` and take the freshest. `computed_at` is in the
-- dedup key for the same reason `ingested_at` is in eod_bars': the history of
-- what we believed about a vendor is the interesting part, and a score that
-- silently replaced yesterday's would make the vendor look like it had always
-- been this good.
CREATE TABLE IF NOT EXISTS vendor_scores (
    as_of        TIMESTAMP,
    source       SYMBOL CAPACITY 64 CACHE,
    freshness    DOUBLE,
    completeness DOUBLE,
    accuracy     DOUBLE,
    score        DOUBLE,
    status       SYMBOL CAPACITY 8 CACHE,
    detail       VARCHAR,
    computed_at  TIMESTAMP
) TIMESTAMP(computed_at) PARTITION BY MONTH WAL
DEDUP UPSERT KEYS(computed_at, source, as_of);

-- Which contract version each dataset is on. A change is a decision, not an
-- accident: the load stops until somebody accepts it.
CREATE TABLE IF NOT EXISTS schema_versions (
    dataset        SYMBOL CAPACITY 64 CACHE,
    schema_version VARCHAR,
    accepted_by    VARCHAR,
    first_seen     TIMESTAMP
) TIMESTAMP(first_seen) PARTITION BY YEAR WAL
DEDUP UPSERT KEYS(first_seen, dataset, schema_version);

-- Where each source has been read up to. Only advanced after a successful load.
-- The cursor's whole job is to be readable immediately after it is written, so
-- QuestDBStore.insert waits for the WAL commit to be applied before returning;
-- see the comment on _wait_for_commit.
CREATE TABLE IF NOT EXISTS ingest_cursors (
    source           SYMBOL CAPACITY 64 CACHE,
    position_ts      TIMESTAMP,
    position_id      LONG,
    rows_at_position LONG,
    updated_at       TIMESTAMP
) TIMESTAMP(updated_at) PARTITION BY YEAR WAL
DEDUP UPSERT KEYS(updated_at, source);

-- Contract specifications, versioned. A lot size that changed in March means a
-- backtest run over March with today's multiplier is quietly wrong by a constant
-- factor, and constant factors are the hardest errors to notice in a P&L.
--
-- The dedup grain matters here in a way it does not for the log tables.
-- load_contract_specs in src/mdp/pipeline.py reloads the whole YAML file on every
-- run, and it only issues a DELETE first on stores that support one -- which
-- QuestDB, with no row-level DELETE in the open-source build, does not. Without
-- dedup the table would grow a duplicate set of specs per run and
-- MarketData.get_specs would return an arbitrary one of them. With it, the reload
-- is an upsert and the table holds exactly one row per version.
CREATE TABLE IF NOT EXISTS contract_specs (
    exchange      SYMBOL CAPACITY 64 CACHE,
    symbol        SYMBOL CAPACITY 4096 CACHE,
    lot_size      DOUBLE,
    tick_size     DOUBLE,
    price_unit    SYMBOL CAPACITY 32 CACHE,
    quantity_unit SYMBOL CAPACITY 32 CACHE,
    known_from    TIMESTAMP,
    known_to      TIMESTAMP
) TIMESTAMP(known_from) PARTITION BY YEAR WAL
DEDUP UPSERT KEYS(known_from, exchange, symbol);

-- Who read what. Licensed market data usually comes with an obligation to know,
-- and an audit log that can lose a row is not one. No dedup: two reads of the
-- same dataset by the same consumer in the same microsecond are two reads.
CREATE TABLE IF NOT EXISTS access_log (
    event_at TIMESTAMP,
    consumer SYMBOL CAPACITY 128 CACHE,
    dataset  SYMBOL CAPACITY 64 CACHE,
    action   SYMBOL CAPACITY 16 CACHE,
    allowed  BOOLEAN,
    detail   VARCHAR
) TIMESTAMP(event_at) PARTITION BY MONTH WAL;

-- What was run, against which data, with which code. The row that lets somebody
-- reproduce a number six months later.
CREATE TABLE IF NOT EXISTS model_runs (
    run_id       VARCHAR,
    model        SYMBOL CAPACITY 64 CACHE,
    params       VARCHAR,
    code_version VARCHAR,
    data_through TIMESTAMP,
    metrics      VARCHAR,
    started_at   TIMESTAMP,
    finished_at  TIMESTAMP
) TIMESTAMP(started_at) PARTITION BY YEAR WAL
DEDUP UPSERT KEYS(started_at, model, run_id);
