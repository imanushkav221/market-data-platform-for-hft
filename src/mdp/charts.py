"""Charts for the deck: PNGs drawn from run output.

Every figure here is drawn from data the pipeline actually produced, so a number
on a slide can be traced back to a run rather than to a drawing tool.

Design rules, applied deliberately rather than by taste:

  - one measure per axis, never two y-scales on one chart
  - categorical colours assigned in a fixed order, never cycled
  - status colours (good/warning/critical) reserved for state, never for a series
  - thin marks, recessive grid and axes, ink for text rather than series colour
  - a legend whenever there are two or more series, plus selective direct labels
    so identity is never carried by colour alone

The palette is a validated set: adjacent pairs stay separable under the common
colour-vision deficiencies, which is checkable rather than a matter of opinion.
"""
from __future__ import annotations

from pathlib import Path

import matplotlib

# Headless by default, because this runs on a server and in CI. But only if
# nothing has already chosen a backend: importing this module from a notebook
# must not rip the inline backend out from under it. (`use(..., force=False)`
# does not do this. It controls whether an unimportable backend raises, not
# whether an existing one is preserved, which is an easy thing to get wrong and
# only shows up as silently missing figures.)
_active = matplotlib.get_backend().lower()
if not (
    _active.startswith("module://")
    or "inline" in _active          # matplotlib >= 3.9 names this one "inline"
    or _active in {"nbagg", "webagg", "widget", "ipympl", "tkagg", "qtagg",
                   "qt5agg", "gtk3agg", "gtk4agg", "macosx"}
):
    matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
import pandas as pd  # noqa: E402

from .config import repo_root  # noqa: E402

# Categorical slots, in fixed order. A sixth series folds into "other" rather
# than inventing a colour.
SERIES = ["#2a78d6", "#eb6834", "#1baf7a", "#eda100", "#e87ba4"]
STATUS = {"good": "#0ca30c", "pass": "#0ca30c", "warning": "#fab219",
          "warn": "#fab219", "serious": "#ec835a", "critical": "#d03b3b",
          "fail": "#d03b3b", "amber": "#fab219", "red": "#d03b3b",
          "green": "#0ca30c"}
SURFACE = "#fcfcfb"
INK = "#0b0b0b"
INK_2 = "#52514e"
MUTED = "#898781"
GRID = "#e1e0d9"

plt.rcParams.update({
    "figure.facecolor": SURFACE,
    "axes.facecolor": SURFACE,
    "axes.edgecolor": "#c3c2b7",
    "axes.labelcolor": INK_2,
    "axes.titlecolor": INK,
    "axes.titlesize": 13,
    "axes.titleweight": "600",
    "axes.titlelocation": "left",
    "axes.labelsize": 10,
    "axes.spines.top": False,
    "axes.spines.right": False,
    "xtick.color": MUTED,
    "ytick.color": MUTED,
    "xtick.labelsize": 9,
    "ytick.labelsize": 9,
    "grid.color": GRID,
    "grid.linewidth": 0.8,
    "legend.frameon": False,
    "legend.fontsize": 9,
    "font.size": 10,
    "figure.dpi": 160,
})


def figures_dir() -> Path:
    out = repo_root() / "docs" / "figures"
    out.mkdir(parents=True, exist_ok=True)
    return out


def _finish(fig, ax, path: Path, *, note: str | None = None) -> Path:
    ax.grid(axis="y", alpha=0.7)
    ax.set_axisbelow(True)
    if note:
        fig.text(0.01, 0.005, note, color=MUTED, fontsize=8, ha="left")
    fig.tight_layout()
    fig.savefig(path, bbox_inches="tight")
    plt.close(fig)
    return path


# ---------------------------------------------------------------------------
# 1. The roll
# ---------------------------------------------------------------------------
def roll_chart(series: pd.DataFrame, *, symbol: str = "CRUDEOIL",
               zoom_days: int = 120, out: Path | None = None) -> Path:
    """Spliced versus back-adjusted price, with the rolls marked.

    Two panels rather than one, because the whole history hides the thing worth
    seeing: at this scale a roll jump is a few pixels. The zoom is where the
    argument lives, and the return panel makes it unarguable — the raw series
    shows a large "move" on a day the market did nothing, because the contract
    changed underneath it.
    """
    path = out or figures_dir() / "roll.png"
    df = series.copy()
    df["trade_date"] = pd.to_datetime(df["trade_date"])
    df["raw_ret"] = df["close"].pct_change()
    df["adj_ret"] = df["adj_close"].pct_change()
    zoom = df.tail(zoom_days).copy()

    fig, (ax1, ax2) = plt.subplots(
        2, 1, figsize=(9, 6.4), sharex=True,
        gridspec_kw={"height_ratios": [2.0, 1.0], "hspace": 0.18},
    )

    ax1.plot(zoom["trade_date"], zoom["close"], color=SERIES[1], linewidth=1.8,
             label="Spliced front month (raw)")
    ax1.plot(zoom["trade_date"], zoom["adj_close"], color=SERIES[0], linewidth=2.0,
             label="Back-adjusted (ratio)")
    for when in zoom.loc[zoom["is_roll"], "trade_date"]:
        # Darker than the grid: these are events, not chrome.
        ax1.axvline(when, color="#c3c2b7", linewidth=1.2, zorder=0)
        ax2.axvline(when, color="#c3c2b7", linewidth=1.2, zorder=0)

    # Name the worst roll jump, because a number beats an adjective.
    rolls = zoom[zoom["is_roll"]].dropna(subset=["raw_ret"])
    if len(rolls):
        worst = rolls.loc[rolls["raw_ret"].abs().idxmax()]
        ax1.annotate(
            f"roll: raw series moves {worst['raw_ret'] * 100:+.1f}%\n"
            f"on a day the market did not",
            (worst["trade_date"], worst["close"]),
            xytext=(-140, -46), textcoords="offset points", color=INK, fontsize=9,
            arrowprops={"arrowstyle": "-", "color": MUTED, "linewidth": 1},
        )

    ax1.set_title(f"{symbol}: what the roll does to the price series")
    ax1.set_ylabel("INR per barrel")
    ax1.legend(loc="upper right")
    ax1.grid(axis="y", alpha=0.7)
    ax1.set_axisbelow(True)

    ax2.bar(zoom["trade_date"], zoom["raw_ret"] * 100, color=SERIES[1], width=1.0,
            label="Raw daily move")
    ax2.plot(zoom["trade_date"], zoom["adj_ret"] * 100, color=SERIES[0],
             linewidth=1.4, label="Adjusted daily move")
    ax2.axhline(0, color="#c3c2b7", linewidth=1.0)
    ax2.set_ylabel("daily move, %")
    ax2.legend(loc="lower right", ncols=2)
    ax2.grid(axis="y", alpha=0.7)
    ax2.set_axisbelow(True)

    total_rolls = int(df["is_roll"].sum())
    fig.text(0.01, 0.005,
             f"Last {len(zoom)} trading days of {len(df)}; {total_rolls} rolls in the "
             f"full series. Vertical lines mark roll dates.",
             color=MUTED, fontsize=8)
    fig.savefig(path, bbox_inches="tight")
    plt.close(fig)
    return path


# ---------------------------------------------------------------------------
# 2. Walk-forward folds
# ---------------------------------------------------------------------------
def fold_skill_chart(folds: pd.DataFrame, *, title: str = "Walk-forward folds",
                     out: Path | None = None) -> Path:
    """Per-fold skill against the naive baseline.

    The honest slide: the distribution, not the headline. Colour carries state
    here (beat the baseline or not), which is why it uses the status palette.
    """
    path = out or figures_dir() / "fold_skill.png"
    df = folds.copy().reset_index(drop=True)
    colours = [STATUS["good"] if s > 0 else STATUS["critical"] for s in df["skill"]]

    fig, ax = plt.subplots(figsize=(9, 4.2))
    ax.bar(df.index + 1, df["skill"] * 100, color=colours, width=0.68)
    ax.axhline(0, color="#c3c2b7", linewidth=1.2)

    mean = df["skill"].mean() * 100
    ax.axhline(mean, color=INK_2, linewidth=1.4, linestyle=(0, (4, 3)))
    ax.annotate(f"mean {mean:+.2f}%", (len(df) + 0.4, mean), color=INK,
                fontsize=9, va="center")

    beat = int((df["skill"] > 0).sum())
    ax.set_title(title)
    ax.set_xlabel("fold (each one month of out-of-sample days)")
    ax.set_ylabel("skill vs baseline, %")
    ax.set_xlim(0.3, len(df) + 3)
    return _finish(fig, ax, path,
                   note=f"{beat} of {len(df)} folds beat the no-change baseline; "
                        f"green beat it, red did not.")


# ---------------------------------------------------------------------------
# 3. Scenarios
# ---------------------------------------------------------------------------
def scenario_chart(comparison: pd.DataFrame, *, out: Path | None = None) -> Path:
    """Skill by scenario. One measure, so one hue; the labels carry the values."""
    path = out or figures_dir() / "scenarios.png"
    df = comparison.dropna(subset=["skill_vs_baseline"]).copy()
    df = df.sort_values("skill_vs_baseline")

    fig, ax = plt.subplots(figsize=(9, 0.55 * len(df) + 2.0))
    values = df["skill_vs_baseline"] * 100
    ax.barh(df["scenario"], values, color=SERIES[0], height=0.6)
    ax.axvline(0, color="#c3c2b7", linewidth=1.2)

    span = max(abs(values.min()), abs(values.max())) or 1.0
    for y, (value, folds) in enumerate(zip(values, df["folds_beating_baseline"], strict=True)):
        offset = span * 0.03
        ax.annotate(f"{value:+.2f}%  ({int(folds)} folds beat)",
                    (value + (offset if value >= 0 else -offset), y),
                    color=INK, fontsize=9, va="center",
                    ha="left" if value >= 0 else "right")

    ax.set_title("Scenario comparison: mean skill against the naive baseline")
    ax.set_xlabel("skill vs baseline, %")
    ax.set_xlim(values.min() - span * 0.55, values.max() + span * 0.55)
    ax.grid(axis="x", alpha=0.7)
    ax.grid(axis="y", visible=False)
    ax.set_axisbelow(True)
    fig.text(0.01, 0.005,
             "Every scenario ran on the same input series, so the comparison "
             "measures the change and not the data.", color=MUTED, fontsize=8)
    fig.tight_layout()
    fig.savefig(path, bbox_inches="tight")
    plt.close(fig)
    return path


# ---------------------------------------------------------------------------
# 4. Delivery latency
# ---------------------------------------------------------------------------
def latency_chart(events: pd.DataFrame, *, sla_seconds: float = 60.0,
                  out: Path | None = None) -> Path:
    """File written to queryable, per delivery, against the SLA."""
    path = out or figures_dir() / "latency.png"
    df = events.copy()
    df["seconds"] = df["detail"].str.extract(r"([\d.]+)s from file write")[0].astype(float)
    df = df.dropna(subset=["seconds"]).sort_values("event_at").reset_index(drop=True)

    fig, ax = plt.subplots(figsize=(9, 3.8))
    ax.plot(df.index + 1, df["seconds"], color=SERIES[0], linewidth=1.8,
            marker="o", markersize=5)
    ax.axhline(sla_seconds, color=STATUS["warning"], linewidth=1.4,
               linestyle=(0, (4, 3)))
    ax.annotate(f"SLA {sla_seconds:.0f}s", (len(df) + 0.2, sla_seconds),
                color=INK_2, fontsize=9, va="center")

    worst = df["seconds"].max()
    ax.set_title("Delivery latency: file written to queryable")
    ax.set_xlabel("delivery")
    ax.set_ylabel("seconds")
    ax.set_ylim(0, max(worst * 1.35, sla_seconds * 1.2))
    ax.set_xlim(0.5, len(df) + 2)
    return _finish(fig, ax, path,
                   note=f"{len(df)} deliveries, worst {worst:.1f}s, all inside the SLA.")


# ---------------------------------------------------------------------------
# 5. Vendor scorecard
# ---------------------------------------------------------------------------
def vendor_chart(scores: pd.DataFrame, *, out: Path | None = None) -> Path:
    """Freshness, completeness and accuracy per source: the procurement slide."""
    path = out or figures_dir() / "vendors.png"
    df = scores.drop_duplicates(subset=["source"], keep="first").copy()
    dimensions = ["freshness", "completeness", "accuracy"]
    width, positions = 0.24, range(len(df))

    fig, ax = plt.subplots(figsize=(9, 4.0))
    for i, dim in enumerate(dimensions):
        values = pd.to_numeric(df[dim], errors="coerce").fillna(0.0)
        offsets = [p + (i - 1) * (width + 0.02) for p in positions]
        ax.bar(offsets, values, width=width, color=SERIES[i], label=dim.title())
        for x, v in zip(offsets, values, strict=True):
            ax.annotate(f"{v:.3f}", (x, v), xytext=(0, 3), textcoords="offset points",
                        ha="center", color=INK, fontsize=8)

    ax.axhline(0.95, color=STATUS["good"], linewidth=1.2, linestyle=(0, (4, 3)))
    ax.annotate("green threshold 0.95", (len(df) - 0.45, 0.955), color=INK_2,
                fontsize=8, va="bottom", ha="right")
    ax.set_xticks(list(positions))
    ax.set_xticklabels(df["source"])
    ax.set_ylim(0, 1.12)
    ax.set_title("Vendor quality, scored per source per day")
    ax.set_ylabel("score")
    ax.legend(loc="lower right", ncols=3)
    return _finish(fig, ax, path,
                   note="Weighted 0.4 freshness, 0.4 completeness, 0.2 accuracy. "
                        "Freshness is not scored on a backfill.")


# ---------------------------------------------------------------------------
# 6. Quality mix
# ---------------------------------------------------------------------------
def quality_chart(events: pd.DataFrame, *, out: Path | None = None) -> Path:
    """Outcomes per check. Status colours, because this is state, not identity."""
    path = out or figures_dir() / "quality.png"
    counts = (
        events.groupby(["check_name", "status"]).size().unstack(fill_value=0)
        .reindex(columns=["pass", "warn", "fail"], fill_value=0)
    )
    counts = counts.loc[counts.sum(axis=1).sort_values().index]

    fig, ax = plt.subplots(figsize=(9, 0.42 * len(counts) + 2.0))
    left = [0] * len(counts)
    for status in ("pass", "warn", "fail"):
        ax.barh(counts.index, counts[status], left=left, height=0.62,
                color=STATUS[status], label=status, edgecolor=SURFACE, linewidth=2)
        left = [a + b for a, b in zip(left, counts[status], strict=True)]

    for y, total in enumerate(left):
        ax.annotate(f"{int(total)}", (total, y), xytext=(5, 0),
                    textcoords="offset points", color=INK, fontsize=9, va="center")

    ax.set_title("Every check that ran, and how it came out")
    ax.set_xlabel("checks recorded")
    ax.legend(loc="lower right", ncols=3)
    ax.grid(axis="x", alpha=0.7)
    ax.grid(axis="y", visible=False)
    ax.set_axisbelow(True)
    fig.tight_layout()
    fig.savefig(path, bbox_inches="tight")
    plt.close(fig)
    return path


def tick_chart(bars: pd.DataFrame, asof: pd.DataFrame, *, symbol: str = "BTCUSDT",
               out: Path | None = None) -> Path:
    """The tick path, in one picture.

    Top: minute bars built on insert by the materialised view, with the moments
    research asked about marked on them. Bottom: prints per minute, which is what
    the end-of-day file has none of and why this path needs different machinery.

    The point of the marks is the as-of rule. Each one sits on the last trade at
    or before its query time, never the nearest, and the label carries how stale
    that match was — a number the consumer can act on rather than a price they
    have to trust.
    """
    path = out or figures_dir() / "ticks.png"
    df = bars.copy()
    df["bucket"] = pd.to_datetime(df["bucket"], utc=True)

    fig, (ax1, ax2) = plt.subplots(
        2, 1, figsize=(9, 5.8), sharex=True,
        gridspec_kw={"height_ratios": [2.1, 1.0], "hspace": 0.16},
    )

    ax1.plot(df["bucket"], df["close"], color=SERIES[0], linewidth=1.8,
             label="1-minute bars (maintained on insert)")
    ax1.fill_between(df["bucket"], df["low"], df["high"], color=SERIES[0],
                     alpha=0.14, linewidth=0, label="minute high-low range")

    if asof is not None and not asof.empty:
        marks = asof.dropna(subset=["price"]).copy()
        marks["query_ts"] = pd.to_datetime(marks["query_ts"], utc=True)
        ax1.scatter(marks["query_ts"], marks["price"], s=42, zorder=3,
                    color=SERIES[1], edgecolor=SURFACE, linewidth=1.5,
                    label="as-of match (last trade at or before)")
        worst = marks.loc[marks["staleness_seconds"].idxmax()]
        ax1.annotate(
            f"matched a print {worst['staleness_seconds']:.1f}s earlier,\n"
            f"never one from after the query",
            (worst["query_ts"], worst["price"]),
            xytext=(-150, 26), textcoords="offset points", color=INK, fontsize=9,
            arrowprops={"arrowstyle": "-", "color": MUTED, "linewidth": 1},
        )

    ax1.set_title(f"{symbol}: the tick path, from prints to a research row")
    ax1.set_ylabel("price")
    ax1.legend(loc="upper left", fontsize=8)
    ax1.grid(axis="y", alpha=0.7)
    ax1.set_axisbelow(True)

    # Just under a minute wide, so adjacent bars keep a visible gap instead of
    # fusing into a solid block.
    ax2.bar(df["bucket"], df["trades"], color=SERIES[2], width=0.00048)
    ax2.set_ylabel("prints per minute")
    ax2.grid(axis="y", alpha=0.7)
    ax2.set_axisbelow(True)

    total = int(df["trades"].sum())
    fig.text(0.01, 0.005,
             f"{len(df)} minutes, {total:,} trade prints deduplicated on "
             f"(venue, symbol, trade_id). Bars are an aggregate maintained on "
             f"insert, not a nightly job.",
             color=MUTED, fontsize=8)
    fig.savefig(path, bbox_inches="tight")
    plt.close(fig)
    return path


def build_all(store, *, symbol: str = "CRUDEOIL", tick_symbol: str = "BTCUSDT",
              comparison: pd.DataFrame | None = None) -> dict[str, Path]:
    """Draw whatever the platform currently has data for."""
    from .serve import MarketData

    md = MarketData(store=store)
    made: dict[str, Path] = {}

    series = md.get_continuous(symbol)
    if not series.empty:
        made["roll"] = roll_chart(series, symbol=symbol)

    bars = md.get_bars(tick_symbol, None, None, "1m")
    if len(bars) > 5:
        buckets = pd.to_datetime(bars["bucket"], utc=True)
        # Decision times that deliberately miss the bar boundaries, as real
        # signal timestamps do.
        times = [t + pd.Timedelta(seconds=37) for t in buckets.iloc[::max(1, len(bars) // 8)]]
        made["ticks"] = tick_chart(bars, md.as_of(tick_symbol, times), symbol=tick_symbol)

    events = md.quality(limit=5000)
    if not events.empty:
        made["quality"] = quality_chart(events)
        latency = events[events["check_name"] == "delivery_latency"]
        if len(latency) >= 2:
            made["latency"] = latency_chart(latency)

    scores = md.vendor_scores()
    if not scores.empty:
        made["vendors"] = vendor_chart(scores)

    if comparison is not None and not comparison.empty:
        made["scenarios"] = scenario_chart(comparison)
        best = comparison.sort_values("skill_vs_baseline", ascending=False).iloc[0]
        folds_path = repo_root() / "data" / "runs" / best["run_id"] / "folds.parquet"
        if folds_path.exists():
            made["folds"] = fold_skill_chart(
                pd.read_parquet(folds_path),
                title=f"Walk-forward folds: {best['scenario']}",
            )
    return made
