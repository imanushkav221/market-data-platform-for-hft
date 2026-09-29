"""Generate the benchmark dataset once, as Parquet, so every engine loads the
same bytes and no engine gets an advantage from the generator.

Shape matches the platform's real query: trades for a handful of symbols over a
month, plus a set of arbitrary decision timestamps that deliberately do NOT fall
on any bar boundary or trade time.
"""
import sys

import numpy as np
import pandas as pd

n_trades = int(sys.argv[1])
n_decisions = int(sys.argv[2])
out = sys.argv[3]

SYMBOLS = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "XRPUSDT", "ADAUSDT"]
rng = np.random.default_rng(7)

start = pd.Timestamp("2026-08-01", tz="UTC")
span_ns = int(pd.Timedelta(days=31).value)

# Sorted in time, which is what every one of these engines wants.
offsets = np.sort(rng.integers(0, span_ns, n_trades))
ts = pd.to_datetime(start.value + offsets, utc=True)
sym_idx = rng.integers(0, len(SYMBOLS), n_trades)

trades = pd.DataFrame({
    "symbol": pd.Categorical.from_codes(sym_idx, SYMBOLS).astype(str),
    "trade_id": np.arange(1, n_trades + 1, dtype=np.int64),
    "ts": ts,
    "price": (60000 * np.exp(np.cumsum(rng.normal(0, 0.00002, n_trades)))).astype(np.float64),
    "qty": rng.random(n_trades).astype(np.float64),
})
trades.to_parquet(f"{out}/trades.parquet", index=False, compression="zstd")

# Decision times: offset 17.5s past a minute so they never coincide with a bar
# edge, which is the case that hides an off-by-one.
dec_offsets = np.sort(rng.integers(0, span_ns, n_decisions))
decisions = pd.DataFrame({
    "symbol": pd.Categorical.from_codes(
        rng.integers(0, len(SYMBOLS), n_decisions), SYMBOLS).astype(str),
    "query_ts": pd.to_datetime(start.value + dec_offsets, utc=True).floor("min")
                + pd.Timedelta(seconds=17.5),
})
decisions.to_parquet(f"{out}/decisions.parquet", index=False, compression="zstd")

print(f"{len(trades):,} trades, {len(decisions):,} decisions")
print(f"span {trades['ts'].min()} .. {trades['ts'].max()}")
