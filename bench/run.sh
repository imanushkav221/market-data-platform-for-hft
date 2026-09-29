#!/usr/bin/env bash
#
# The store decision, reproducible.
#
# Runs the same as-of join against QuestDB, ClickHouse and DuckDB on identical
# data, and prints wall time and peak resident memory for each. Wall time is
# client-side and warm: the first run of each is discarded, because the first one
# measures the page cache rather than the engine.
#
# It assumes the two servers are already up and does not start them, because a
# benchmark that also manages processes tends to measure the process management.
# Exactly how they were started:
#
#   QuestDB    http://127.0.0.1:${QDB_PORT:-9000}
#              docker compose up -d questdb
#   ClickHouse http://127.0.0.1:${CH_PORT:-8123}
#              docker run -d --name bench-clickhouse -p 8123:8123 \
#                --ulimit nofile=262144:262144 clickhouse/clickhouse-server:24.8
#
# ClickHouse is started by hand rather than from docker-compose.yml on purpose: it
# is not part of the platform any more (docs/ARCHITECTURE.md section 3.2 has the
# decision) and it is still the engine this benchmark exists to compare against.
# Nothing here goes through src/mdp/storage.py -- each engine is driven with its own
# raw SQL over HTTP -- so removing the ClickHouse backend did not touch this result.
#
# Usage:  ./run.sh [n_trades] [n_decisions]
set -euo pipefail

TRADES=${1:-20000000}
DECISIONS=${2:-5000}
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DATA="$HERE/data"
QDB_PORT=${QDB_PORT:-9000}
# QuestDB's read_parquet() resolves relative paths against the server's
# cairo.sql.copy.root, not against the client's working directory — and a file it
# cannot find reports as "Parquet file is likely corrupted", which sends you
# looking at the file instead of at the path. Point this at the same directory
# the server has, or set cairo.sql.copy.root to $DATA.
QDB_COPY_ROOT=${QDB_COPY_ROOT:-$DATA}
CH_PORT=${CH_PORT:-8123}
PY=${PY:-python}

# --noproxy matters in any environment with an HTTP proxy configured: without it
# a request to localhost can be routed away and the benchmark measures the proxy.
# The glob in --noproxy has to be quoted at every call site: unquoted, the shell
# expands it against the working directory and curl is handed a filename.
q_questdb() {
    curl -s --noproxy '*' -G "http://127.0.0.1:$QDB_PORT/exec" --data-urlencode "query=$1"
}
q_clickhouse() {
    curl -s --noproxy '*' --data-binary "$1" "http://127.0.0.1:$CH_PORT/"
}

hwm() {  # peak resident set size, in MB, of a process matched by pattern
    local pid
    pid=$(pgrep -f "$1" | head -1) || { echo "n/a"; return; }
    awk '/VmHWM/ {printf "%.0f", $2/1024}' "/proc/$pid/status"
}

timed() {  # run a query three times, print the last two
    local label="$1" fn="$2" sql="$3"
    "$fn" "$sql" >/dev/null                       # warm
    for _ in 1 2; do
        local s e
        s=$(date +%s.%N); "$fn" "$sql" >/dev/null; e=$(date +%s.%N)
        printf "  %-12s %6.3f s\n" "$label" "$(echo "$e - $s" | bc)"
    done
}

echo "== generating $TRADES trades, $DECISIONS decisions =="
mkdir -p "$DATA"
$PY "$HERE/gen.py" "$TRADES" "$DECISIONS" "$DATA"

# QuestDB's read_parquet() is fussy in two undocumented ways: it rejects Arrow
# large_string, and it rejects large row groups — a 1M-row group fails as
# "Parquet file is likely corrupted". Writing plain `string` with 50,000-row
# groups fixes both. It is also sensitive to something in a whole-file write
# that chunked writes do not trip (two files of identical schema and within 72
# bytes of each other, one loads and one does not), so the loader writes chunks
# and inserts them in sequence. That is slower and it is what works.
echo "== rewriting parquet for QuestDB (chunked, 50k row groups, plain string) =="
rm -rf "$DATA/qchunks"
$PY - "$DATA" <<'PYEOF'
import os
import sys

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

data = sys.argv[1]
os.makedirs(f"{data}/qchunks", exist_ok=True)

df = pd.read_parquet(f"{data}/trades.parquet")
df["ts"] = df["ts"].dt.tz_convert("UTC").dt.tz_localize(None).astype("datetime64[us]")
schema = pa.schema([
    pa.field("symbol", pa.string()), pa.field("trade_id", pa.int64()),
    pa.field("ts", pa.timestamp("us")), pa.field("price", pa.float64()),
    pa.field("qty", pa.float64()),
])
chunk = 2_000_000
for i in range(0, len(df), chunk):
    pq.write_table(
        pa.Table.from_pandas(df.iloc[i:i + chunk], schema=schema, preserve_index=False),
        f"{data}/qchunks/t{i // chunk:03d}.parquet",
        compression="none", version="2.4", row_group_size=50_000,
    )
print(f"  {len(os.listdir(f'{data}/qchunks'))} trade chunks")

dec = pd.read_parquet(f"{data}/decisions.parquet")
dec["query_ts"] = (dec["query_ts"].dt.tz_convert("UTC").dt.tz_localize(None)
                   .astype("datetime64[us]"))
dec_schema = pa.schema([pa.field("symbol", pa.string()),
                        pa.field("query_ts", pa.timestamp("us"))])
pq.write_table(pa.Table.from_pandas(dec, schema=dec_schema, preserve_index=False),
               f"{data}/decisions_q.parquet", compression="none",
               version="2.4", row_group_size=50_000)
print("  decisions_q.parquet")
PYEOF

# Put the QuestDB-shaped files where the server can actually see them.
if [ "$QDB_COPY_ROOT" != "$DATA" ]; then
    mkdir -p "$QDB_COPY_ROOT"
    rm -rf "$QDB_COPY_ROOT/qchunks"
    cp -r "$DATA/qchunks" "$QDB_COPY_ROOT/qchunks"
    cp "$DATA/decisions_q.parquet" "$QDB_COPY_ROOT/decisions_q.parquet"
fi

echo
echo "== QuestDB =="
# Probe before loading, so a path problem says so instead of arriving later as
# an empty table and a WAL that never advances.
probe=$(q_questdb "SELECT count() FROM read_parquet('decisions_q.parquet')")
case "$probe" in
  *error*)
    echo "  cannot read parquet from the server's copy root."
    echo "  QuestDB resolves read_parquet() against cairo.sql.copy.root, and"
    echo "  reports a file it cannot find as 'likely corrupted'."
    echo "  Set QDB_COPY_ROOT to the server's copy root, or point"
    echo "  cairo.sql.copy.root at $DATA. Server said: $probe"
    exit 1;;
esac
q_questdb "DROP TABLE IF EXISTS trades" >/dev/null
q_questdb "DROP TABLE IF EXISTS decisions" >/dev/null
q_questdb "CREATE TABLE trades (symbol SYMBOL, trade_id LONG, ts TIMESTAMP, price DOUBLE, qty DOUBLE)
           TIMESTAMP(ts) PARTITION BY DAY WAL DEDUP UPSERT KEYS(ts, symbol, trade_id)" >/dev/null
q_questdb "CREATE TABLE decisions (symbol SYMBOL, query_ts TIMESTAMP)
           TIMESTAMP(query_ts) PARTITION BY DAY WAL" >/dev/null
for f in "$DATA"/qchunks/*.parquet; do
    q_questdb "INSERT INTO trades SELECT symbol, trade_id, ts, price, qty
               FROM read_parquet('qchunks/$(basename "$f")')" >/dev/null
done
q_questdb "INSERT INTO decisions SELECT symbol, query_ts FROM read_parquet('decisions_q.parquet')" >/dev/null
# WAL writes are eventually visible: returning from the insert does not mean a
# select sees the rows, so wait for the applied transaction to catch up.
n=0
for _ in $(seq 1 120); do
    n=$(q_questdb "SELECT count() FROM trades" | grep -oE '\[\[[0-9]+\]\]' | tr -dc '0-9')
    [ "${n:-0}" = "$TRADES" ] && break
    sleep 2
done
[ "${n:-0}" = "$TRADES" ] || { echo "  QuestDB never applied the WAL (saw ${n:-0} of $TRADES)"; exit 1; }
echo "  loaded $n rows, VmHWM before: $(hwm questdb) MB"
timed questdb q_questdb \
  "SELECT count() AS m, round(avg(t.price),4) AS p FROM decisions d ASOF JOIN trades t ON (symbol)"
echo "  VmHWM after: $(hwm questdb) MB"
q_questdb "SELECT count() AS matched, round(avg(t.price),4) AS avg_px FROM decisions d ASOF JOIN trades t ON (symbol)" \
  | grep -oE '\[\[.*\]\]'

echo
echo "== ClickHouse =="
q_clickhouse "DROP TABLE IF EXISTS trades" >/dev/null
q_clickhouse "DROP TABLE IF EXISTS decisions" >/dev/null
q_clickhouse "CREATE TABLE trades (symbol LowCardinality(String), trade_id Int64,
              ts DateTime64(9,'UTC'), price Float64, qty Float64)
              ENGINE = MergeTree ORDER BY (symbol, ts, trade_id)" >/dev/null
q_clickhouse "CREATE TABLE decisions (symbol LowCardinality(String), query_ts DateTime64(9,'UTC'))
              ENGINE = MergeTree ORDER BY (symbol, query_ts)" >/dev/null
# Pushed over HTTP rather than read with file(), which resolves against the
# server's user_files_path and silently inserts nothing when the path is outside
# it — the same failure mode as QuestDB's copy root, with a different name.
curl -s --noproxy '*' --data-binary "@$DATA/trades.parquet" \
     "http://127.0.0.1:$CH_PORT/?query=INSERT%20INTO%20trades%20FORMAT%20Parquet" >/dev/null
curl -s --noproxy '*' --data-binary "@$DATA/decisions.parquet" \
     "http://127.0.0.1:$CH_PORT/?query=INSERT%20INTO%20decisions%20FORMAT%20Parquet" >/dev/null
q_clickhouse "OPTIMIZE TABLE trades FINAL" >/dev/null
ch_rows=$(q_clickhouse "SELECT count() FROM trades" | tr -dc '0-9')
[ "${ch_rows:-0}" = "$TRADES" ] || { echo "  ClickHouse loaded ${ch_rows:-0} of $TRADES"; exit 1; }
echo "  loaded $ch_rows rows, VmHWM before: $(hwm 'clickhouse server') MB"
timed clickhouse q_clickhouse \
  "SELECT count() AS m, round(avg(t.price),4) AS p FROM decisions AS d
   ASOF LEFT JOIN trades AS t ON d.symbol = t.symbol AND d.query_ts >= t.ts"
echo "  VmHWM after: $(hwm 'clickhouse server') MB"
q_clickhouse "SELECT count() AS matched, round(avg(t.price),4) AS avg_px FROM decisions AS d
   ASOF LEFT JOIN trades AS t ON d.symbol = t.symbol AND d.query_ts >= t.ts"

echo
echo "== DuckDB =="
$PY - "$DATA" <<'PYEOF'
import resource, sys, time
import duckdb
data = sys.argv[1]
con = duckdb.connect()
con.execute(f"CREATE TABLE trades AS SELECT * FROM read_parquet('{data}/trades.parquet')")
con.execute(f"CREATE TABLE decisions AS SELECT * FROM read_parquet('{data}/decisions.parquet')")
sql = """SELECT count(*) AS m, round(avg(t.price),4) AS p
         FROM decisions d ASOF LEFT JOIN trades t
         ON d.symbol = t.symbol AND d.query_ts >= t.ts"""
con.execute(sql).fetchone()
for _ in range(2):
    s = time.time(); r = con.execute(sql).fetchone(); e = time.time()
    print(f"  duckdb       {e - s:6.3f} s")
print(f"  peak RSS: {resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024:.0f} MB")
print(f"  {r}")
PYEOF

echo
echo "All three must print the same matched count and average price. If they do"
echo "not, the comparison is measuring a difference in answers, not in speed."
