"""The as-of join: what was true at this instant, and nothing after it.

This is the operation a quant asks a data platform for more than any other. Given
a list of moments — signal timestamps, order times, model decision points — return
the state of the market as it stood at each one.

It sounds like a lookup and it is the single easiest place to leak the future. Two
mistakes are nearly universal:

  1. **Joining on the nearest timestamp** instead of the last one at or before.
     Nearest will happily hand you a print from 200ms *after* the decision, and a
     backtest built on that looks wonderful and is worthless.
  2. **Silently returning a stale price.** If the last trade was forty minutes
     ago, the honest answer is "no current price", not a forty minute old number
     dressed as current. A staleness bound turns an invisible error into a NULL
     somebody has to handle.

So: a strict at-or-before match, an explicit staleness bound, and the age of the
match returned alongside the price so the consumer can see what they are holding.
Both stores do the match in SQL — QuestDB with `ASOF JOIN`, DuckDB with its own
`ASOF LEFT JOIN` — because this is exactly the join a column store is built for
and doing it in pandas stops working at the first real dataset.
"""
from __future__ import annotations

import pandas as pd

from .storage import Store

DEFAULT_MAX_STALENESS = 60.0


def as_of_prices(
    store: Store,
    symbol: str,
    timestamps,
    *,
    venue: str = "binance",
    max_staleness_seconds: float = DEFAULT_MAX_STALENESS,
) -> pd.DataFrame:
    """The last trade at or before each timestamp.

    Returns one row per requested timestamp with the matched price, the trade's
    own time, and how stale it was. A match older than the bound keeps its
    timestamp but its price comes back NULL: better an obvious hole than a
    plausible wrong number.
    """
    times = _as_frame(timestamps)
    if times.empty:
        return times.assign(price=None, trade_ts=None, staleness_seconds=None)

    matched = store.as_of(times, symbol=symbol, venue=venue)
    matched["staleness_seconds"] = (
        pd.to_datetime(matched["query_ts"], utc=True)
        - pd.to_datetime(matched["trade_ts"], utc=True)
    ).dt.total_seconds()

    # The honesty step. Note what is NOT done: the row is not dropped, because a
    # consumer needs to know their timestamp had no usable price.
    # `NaN > x` is False, not NaN, so a timestamp with no match at all was being
    # flagged `stale=False` and the `.fillna(True)` that was meant to catch it
    # never ran. A consumer filtering on `~stale` kept the no-data rows as if
    # they were good ones, which is the opposite of what this column is for.
    no_match = matched["staleness_seconds"].isna()
    too_old = (matched["staleness_seconds"] > max_staleness_seconds).fillna(False)
    matched.loc[too_old, "price"] = None
    matched["stale"] = too_old | no_match
    return matched


def with_features(
    store: Store,
    symbol: str,
    timestamps,
    *,
    venue: str = "binance",
    vol_window_minutes: int = 30,
    max_staleness_seconds: float = DEFAULT_MAX_STALENESS,
) -> pd.DataFrame:
    """As-of price plus a trailing realised volatility, per timestamp.

    Realised volatility over the window *ending* at each timestamp, computed from
    minute bars rather than raw trades because that is both cheaper and what a
    researcher would use. Same rule as the price: the window is closed at the
    left, open at the right, so nothing after the moment can enter it.
    """
    prices = as_of_prices(
        store, symbol, timestamps, venue=venue,
        max_staleness_seconds=max_staleness_seconds,
    )
    if prices.empty:
        return prices

    span_start = pd.to_datetime(prices["query_ts"], utc=True).min() - pd.Timedelta(
        minutes=vol_window_minutes + 1
    )
    span_end = pd.to_datetime(prices["query_ts"], utc=True).max()
    bars = store.bars(symbol, span_start, span_end, "1m", venue)
    if bars.empty:
        return prices.assign(realised_vol=None, bars_in_window=0)

    bars = bars.copy()
    bars["bucket"] = pd.to_datetime(bars["bucket"], utc=True)
    bars = bars.sort_values("bucket")
    bars["ret"] = bars["close"].pct_change()

    vols, counts = [], []
    window = pd.Timedelta(minutes=vol_window_minutes)
    bar_span = pd.Timedelta(minutes=1)
    for query_ts in pd.to_datetime(prices["query_ts"], utc=True):
        # A bar is stamped with the START of the interval it covers, so the test
        # is on where the bar ENDS, not where it is labelled.
        #
        # The obvious version of this line is `bucket < query_ts`, and it is
        # wrong in a way that is almost impossible to see. A decision at
        # 10:50:17.5 admits the bar stamped 10:50:00, which covers 10:50:00 to
        # 10:51:00 and whose close is therefore built from trades after the
        # decision. Measured on this repo's own demo data, that one bar moved
        # realised volatility by a factor of 52 when a large print landed twelve
        # seconds past the decision time.
        #
        # It also hid from the test suite: a query timestamp that falls exactly
        # on a bar boundary is the single case where the two versions agree, and
        # that is what the original test used.
        in_window = bars[
            (bars["bucket"] >= query_ts - window)
            & (bars["bucket"] + bar_span <= query_ts)
        ]
        returns = in_window["ret"].dropna()
        vols.append(float(returns.std()) if len(returns) > 1 else None)
        counts.append(int(len(in_window)))

    return prices.assign(realised_vol=vols, bars_in_window=counts)


def _as_frame(timestamps) -> pd.DataFrame:
    if isinstance(timestamps, pd.DataFrame):
        column = "query_ts" if "query_ts" in timestamps.columns else timestamps.columns[0]
        series = timestamps[column]
    else:
        series = pd.Series(list(timestamps))
    parsed = pd.to_datetime(series, utc=True, errors="coerce").dropna()
    return pd.DataFrame({"query_ts": sorted(parsed.unique())})
