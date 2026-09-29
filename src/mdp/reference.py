"""Reference data: expiries, the front-month map, and continuous series.

A futures price history is not a time series until someone decides which contract
it refers to on each day. That decision is reference data, it is versioned, and it
has to be point in time: a backtest run in June must see the map as it stood in
June, not as it stands today. Getting this wrong is the most common silent error
in commodities data, and it is invisible until someone asks why a number moved.
"""
from __future__ import annotations

import pandas as pd


def front_month_map(eod: pd.DataFrame, *, method: str = "volume",
                    buffer_days: int = 3) -> pd.DataFrame:
    """Choose the contract each symbol trades as 'front' on each date.

    volume  - the contract with the most volume that day (what a desk actually trades)
    nearest - the nearest expiry at least `buffer_days` away (simple and predictable)
    """
    df = eod.copy()
    df["expiry"] = pd.to_datetime(df["expiry"])
    df["trade_date"] = pd.to_datetime(df["trade_date"])
    df = df[df["expiry"] >= df["trade_date"]]

    if method == "volume":
        idx = df.groupby(["exchange", "symbol", "trade_date"])["volume"].idxmax()
        chosen = df.loc[idx, ["exchange", "symbol", "trade_date", "expiry"]]
    elif method == "nearest":
        ok = df[df["expiry"] >= df["trade_date"] + pd.Timedelta(days=buffer_days)]
        idx = ok.groupby(["exchange", "symbol", "trade_date"])["expiry"].idxmin()
        chosen = ok.loc[idx, ["exchange", "symbol", "trade_date", "expiry"]]
    else:
        raise ValueError(f"unknown front-month method {method!r}")

    chosen = chosen.sort_values(["exchange", "symbol", "trade_date"]).reset_index(drop=True)
    chosen = _forbid_rolling_backwards(chosen)
    # known_from is the day we could have known this: the trade date itself.
    chosen["known_from"] = chosen["trade_date"]
    chosen["is_front"] = True
    return chosen


def _forbid_rolling_backwards(chosen: pd.DataFrame) -> pd.DataFrame:
    """A roll happens once and never reverses.

    Whichever rule picks the front contract, daily volume flips around and a naive
    'most volume today' rule rolls back and forth, which produces a price series
    that jumps between contracts. Real desks roll forward once. Enforcing that
    here turns roughly sixty rolls a year into about twelve, which is what a
    monthly contract should give. This is the kind of thing that looks like a
    modelling bug much later.

    The rule is absolute: the chosen expiry is non-decreasing in trade date, with
    no exception for a day on which the new front contract happens to have no
    print. The earlier version made an exception for exactly that day and so
    produced Apr, May, May, Apr, May, which is the monotonicity failure this
    function exists to prevent. Whether a contract printed on a given day is a
    question about prices, and it is answered in `continuous_series`; this
    function answers only which contract is front, and that answer does not
    un-happen because the exchange published nothing.
    """
    out = []
    for (exchange, symbol), g in chosen.groupby(["exchange", "symbol"], sort=False):
        current = None
        rows = []
        for row in g.itertuples(index=False):
            pick = row.expiry if current is None else max(row.expiry, current)
            current = pick
            rows.append({"exchange": exchange, "symbol": symbol,
                         "trade_date": row.trade_date, "expiry": pick})
        out.append(pd.DataFrame(rows))
    return pd.concat(out, ignore_index=True) if out else chosen


def _splice_prices(
    df: pd.DataFrame,
    roll_date: pd.Timestamp,
    old_exp: pd.Timestamp,
    new_exp: pd.Timestamp,
    price_column: str,
) -> tuple[float, float] | None:
    """The last day on or before the roll on which both contracts printed.

    Normally that is the roll day itself. It is not when the expiring contract
    stopped trading before the new one took over, and that is the ordinary case
    rather than the exotic one: a contract's last trading day is usually before
    its expiry, and the volume rule rolls precisely when the old contract's
    volume collapses. On such a day the expiring contract has no row at all.

    Both legs are taken from the same earlier day on purpose. Pairing the new
    contract's close on the roll day with an older close of the expiring one
    would splice two different dates together and manufacture a return out of
    the days in between, which is the error this whole module exists to avoid.

    Returns None when no day has both, which means the two segments share no
    price and the roll genuinely cannot be adjusted.
    """
    both = df[df["expiry"].isin([old_exp, new_exp]) & (df["trade_date"] <= roll_date)]
    if both.empty:
        return None
    wide = both.pivot_table(
        index="trade_date", columns="expiry", values=price_column, aggfunc="last"
    )
    if old_exp not in wide.columns or new_exp not in wide.columns:
        return None
    # A zero close cannot be a ratio denominator, and a contract quoted at zero is
    # a data error rather than a splice point, so those days are not candidates.
    pair = wide[[old_exp, new_exp]].dropna()
    pair = pair[pair[old_exp] != 0]
    if pair.empty:
        return None
    last = pair.index.max()
    return float(pair.loc[last, old_exp]), float(pair.loc[last, new_exp])


def continuous_series(
    eod: pd.DataFrame,
    *,
    symbol: str,
    method: str = "volume",
    adjust: str = "ratio",
    as_of: str | pd.Timestamp | None = None,
    price_column: str = "close",
    roll_map: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """Build a back-adjusted continuous front-month series.

    adjust:
      none       - splice the raw prices, leaving a jump at every roll
      ratio      - multiply history so returns are continuous (use this for modelling)
      difference - shift history so price differences are continuous

    `as_of` truncates the data first, so the series is exactly what a model run on
    that date would have seen.

    `roll_map` supplies the front-month choice instead of recomputing it, and is
    how the stored point-in-time map in `contract_reference` gets used. The
    difference is not cosmetic. Recomputing answers "which contract would today's
    rule pick from today's data", and a settlement corrected last week silently
    changes the answer for a day that is already in a published backtest. Reading
    the stored map answers "which contract did we say was front on that day",
    which is the question a reproducible run is actually asking. When no map is
    supplied the rule is applied to the data in hand, which is correct for a
    fresh dataset and is what happens before the first `build_reference`.

    The returned frame carries `roll_adjusted`, a nullable boolean that is set on
    roll rows only: True where the two contracts were spliced at a real shared
    price, False where no day carries both and the roll could not be adjusted at
    all. It is null on every non-roll row, and on every row when `adjust="none"`,
    because there is nothing to have succeeded or failed at. An unadjusted roll
    leaves a genuine discontinuity in `adj_close` before that date: the column
    exists so a caller finds out by looking rather than by being surprised.

    A day on which the front contract did not print is absent from the result
    rather than carried forward, so every row here is a price the exchange
    actually published.
    """
    df = eod.copy()
    df["expiry"] = pd.to_datetime(df["expiry"])
    df["trade_date"] = pd.to_datetime(df["trade_date"])
    df = df[df["symbol"] == symbol]
    if as_of is not None:
        df = df[df["trade_date"] <= pd.Timestamp(as_of)]
    if df.empty:
        return pd.DataFrame()

    if roll_map is not None and not roll_map.empty:
        chosen = roll_map.copy()
        chosen["expiry"] = pd.to_datetime(chosen["expiry"])
        chosen["trade_date"] = pd.to_datetime(chosen["trade_date"])
        chosen = chosen[chosen["symbol"] == symbol]
        if as_of is not None:
            chosen = chosen[chosen["trade_date"] <= pd.Timestamp(as_of)]
        chosen = chosen[["exchange", "symbol", "trade_date", "expiry"]]
        chosen = chosen.drop_duplicates(
            subset=["exchange", "symbol", "trade_date"], keep="last"
        )
        chosen = _forbid_rolling_backwards(
            chosen.sort_values(["exchange", "symbol", "trade_date"]).reset_index(drop=True)
        )
    else:
        chosen = front_month_map(df, method=method)
        chosen = chosen[chosen["symbol"] == symbol]
    series = chosen.merge(df, on=["exchange", "symbol", "trade_date", "expiry"], how="left")
    series = series.sort_values("trade_date")

    # The front-month map names a contract for every day the symbol traded at
    # all, including days that contract itself did not print. Those days survive
    # the merge with no price, and there are only two things to do with them.
    # Carrying the previous close forward would publish a flat day the exchange
    # never published, and that invented return goes straight into volatility,
    # into every feature and into a backtest, which is the class of error this
    # module exists to prevent. So the day is dropped: the series then has a
    # calendar gap, which is true, instead of a fabricated price, which is not.
    series = series[series[price_column].notna()]
    if series.empty:
        return pd.DataFrame()
    series = series.reset_index(drop=True)

    # Computed after the drop so a roll is flagged on the first day the new front
    # contract actually printed, which is the day the splice has to anchor to.
    series["is_roll"] = series["expiry"] != series["expiry"].shift(1)
    series.loc[0, "is_roll"] = False

    series["adj_factor"] = 1.0
    series["adj_offset"] = 0.0
    series["roll_adjusted"] = pd.Series(pd.NA, index=series.index, dtype="boolean")
    if adjust != "none":
        roll_idx = series.index[series["is_roll"]].tolist()
        factor, offset = 1.0, 0.0
        # Walk backwards so the most recent segment keeps its true prices, which
        # is what makes today's number match the exchange's today.
        for i in reversed(roll_idx):
            roll_date = series.loc[i, "trade_date"]
            new_exp, old_exp = series.loc[i, "expiry"], series.loc[i - 1, "expiry"]
            pair = _splice_prices(df, roll_date, old_exp, new_exp, price_column)
            if pair is None:
                # No day carries both contracts, so there is no price at which the
                # two segments meet. Writing the factor we happen to be carrying
                # would back-adjust the earlier history by a number derived from a
                # different roll, which shows up as a large return on a day the
                # market did nothing. Leave the factor alone and say so instead.
                series.loc[i, "roll_adjusted"] = False
                continue
            old_close, new_close = pair
            if adjust == "ratio":
                factor *= new_close / old_close
            else:
                offset += new_close - old_close
            series.loc[i, "roll_adjusted"] = True
            series.loc[: i - 1, "adj_factor"] = factor
            series.loc[: i - 1, "adj_offset"] = offset

    if adjust == "ratio":
        series["adj_close"] = series[price_column] * series["adj_factor"]
    elif adjust == "difference":
        series["adj_close"] = series[price_column] + series["adj_offset"]
    else:
        series["adj_close"] = series[price_column]

    cols = ["trade_date", "symbol", "expiry", "is_roll", "roll_adjusted", price_column,
            "adj_close", "volume", "open_interest"]
    return series[[c for c in cols if c in series.columns]].reset_index(drop=True)
