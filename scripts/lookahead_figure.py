"""The lookahead figure: the same feature, computed two ways.

Realised volatility over the window ending at each decision time, on real bars
out of the platform's own store. Two series:

  correct       a bar enters the window only if it ENDS at or before the
                decision:  bucket + bar_span <= query_ts
  contaminated  the obvious version, `bucket < query_ts`, which admits the bar
                the decision falls inside -- a bar whose close is built from
                trades that happened after the decision was made

Nothing else differs. Same bars, same window, same estimator.

Run:  PYTHONPATH=src MDP_STORE=questdb python scripts/lookahead_figure.py
"""
from __future__ import annotations

import os
import sys

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
import pandas as pd  # noqa: E402

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from mdp.storage import get_store  # noqa: E402

SURFACE = "#FDFCF9"
INK = "#14202E"
INK_SOFT = "#4A5568"
INK_MUTED = "#6A7179"
GRID = "#E3E0D8"
CORRECT = "#2a78d6"
CONTAMINATED = "#eb6834"

SYMBOL = "BTCUSDT"
VENUE = "binance"
WINDOW_MINUTES = 10
BAR_SPAN = pd.Timedelta(minutes=1)


def realised_vol(bars: pd.DataFrame, query_ts: pd.Timestamp, window: pd.Timedelta,
                 *, contaminated: bool) -> float | None:
    if contaminated:
        # The obvious version. A bar is stamped with the START of its interval,
        # so this admits the bar the decision falls inside.
        in_window = bars[(bars["bucket"] >= query_ts - window) & (bars["bucket"] < query_ts)]
    else:
        # The bar has to END at or before the decision.
        in_window = bars[
            (bars["bucket"] >= query_ts - window)
            & (bars["bucket"] + BAR_SPAN <= query_ts)
        ]
    returns = in_window["ret"].dropna()
    return float(returns.std()) if len(returns) > 1 else None


def main() -> int:
    store = get_store()
    bars = store.bars(SYMBOL, None, None, "1m", VENUE)
    if bars.empty:
        print("no bars: load the tick source first")
        return 1

    bars = bars.copy()
    bars["bucket"] = pd.to_datetime(bars["bucket"], utc=True)
    bars = bars.sort_values("bucket").reset_index(drop=True)
    bars["ret"] = bars["close"].pct_change()

    # Decision times deliberately OFF a bar boundary. On a boundary the two
    # versions agree exactly, which is how this class of bug survives a test
    # suite: the test picks a round number.
    first = bars["bucket"].min() + pd.Timedelta(minutes=WINDOW_MINUTES + 1)
    last = bars["bucket"].max()
    decisions = pd.date_range(first, last, freq="1min") + pd.Timedelta(seconds=17.5)
    decisions = decisions[decisions <= last]

    window = pd.Timedelta(minutes=WINDOW_MINUTES)
    rows = []
    for q in decisions:
        rows.append({
            "query_ts": q,
            "correct": realised_vol(bars, q, window, contaminated=False),
            "contaminated": realised_vol(bars, q, window, contaminated=True),
        })
    df = pd.DataFrame(rows).dropna()
    if df.empty:
        print("not enough bars for the window")
        return 1

    df["ratio"] = df["contaminated"] / df["correct"]
    worst = df.loc[df["ratio"].idxmax()]
    print(f"decisions: {len(df)}")
    print(f"median ratio: {df['ratio'].median():.3f}")
    print(f"worst ratio:  {worst['ratio']:.2f}x at {worst['query_ts']}")
    print(f"  correct {worst['correct']:.6f}  contaminated {worst['contaminated']:.6f}")
    over = (df["ratio"] > 1.05).mean() * 100
    print(f"decisions overstated by more than 5%: {over:.0f}%")

    fig, ax = plt.subplots(figsize=(13.5, 6.4), dpi=170)
    fig.patch.set_facecolor(SURFACE)
    ax.set_facecolor(SURFACE)

    x = df["query_ts"].dt.tz_convert("UTC")
    ax.plot(x, df["contaminated"] * 100, color=CONTAMINATED, linewidth=2,
            label="Window includes the bar the decision sits inside")
    ax.plot(x, df["correct"] * 100, color=CORRECT, linewidth=2,
            label="Window ends at or before the decision")

    ax.set_ylabel("Realised volatility over the trailing 10 minutes  (%)",
                  color=INK_SOFT, fontsize=12)
    ax.tick_params(colors=INK_MUTED, labelsize=11)
    for side in ("top", "right"):
        ax.spines[side].set_visible(False)
    for side in ("left", "bottom"):
        ax.spines[side].set_color(GRID)
    ax.grid(True, axis="y", color=GRID, linewidth=0.8)
    ax.set_axisbelow(True)

    import matplotlib.dates as mdates
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%H:%M"))

    # Direct labels as well as the legend, so identity never rests on colour.
    last_row = df.iloc[-1]
    ax.annotate("contaminated", xy=(x.iloc[-1], last_row["contaminated"] * 100),
                xytext=(6, 6), textcoords="offset points",
                color=INK_SOFT, fontsize=11, fontweight="bold")
    ax.annotate("correct", xy=(x.iloc[-1], last_row["correct"] * 100),
                xytext=(6, -14), textcoords="offset points",
                color=INK_SOFT, fontsize=11, fontweight="bold")

    # Shade every decision where the two answers differ by more than 5%. That
    # band is the measurable claim: not that the error is enormous, but that it
    # is frequent, and that all of it is information from after the decision.
    disagrees = (df["ratio"] - 1).abs() > 0.05
    ymin, ymax = ax.get_ylim()
    ax.fill_between(x, ymin, ymax, where=disagrees.to_numpy(),
                    color=CONTAMINATED, alpha=0.10, linewidth=0, step="mid")
    ax.set_ylim(ymin, ymax)

    leg = ax.legend(loc="upper left", frameon=False, fontsize=11.5)
    for text in leg.get_texts():
        text.set_color(INK_SOFT)

    ax.annotate(
        f"shaded: the {over:.0f}% of decisions where the two answers differ by more than 5%",
        xy=(0.5, -0.13), xycoords="axes fraction", ha="center",
        color=INK_MUTED, fontsize=11.5,
    )

    fig.subplots_adjust(left=0.08, right=0.88, top=0.96, bottom=0.17)
    out = "docs/figures/lookahead.png"
    fig.savefig(out, facecolor=SURFACE)
    print(f"wrote {out}")

    stats = {
        "decisions": int(len(df)),
        "median_ratio": float(df["ratio"].median()),
        "worst_ratio": float(worst["ratio"]),
        "pct_over_5": float(over),
    }
    pd.Series(stats).to_json("docs/figures/lookahead_stats.json")
    print(stats)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
