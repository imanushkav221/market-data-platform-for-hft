"""The model layer: features, walk-forward evaluation, and an honest baseline.

This exists to show how a model is run and validated on top of the platform, not
to claim a tradable signal. Three things it is built to demonstrate:

  1. Features are computed with an explicit as-of rule, so no row can see its own
     future. The shift is in one place, named, and tested.
  2. Evaluation is walk-forward against a naive baseline. A model that cannot
     beat "tomorrow looks like today" has told you nothing.
  3. Every run is recorded with its parameters and inputs, so a number produced
     in June can be reproduced in December.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import numpy as np
import pandas as pd

# ----------------------------------------------------------------------------
# Features
# ----------------------------------------------------------------------------
# ----------------------------------------------------------------------------
# Feature registry
#
# Features are declared by name in a model recipe, not hard-coded here. Adding
# one is a function plus a line of YAML, and the recipe records exactly which
# features a given run used, which is what makes two runs comparable months
# apart.
#
# Every function takes the price series and returns a series aligned to it. None
# of them may look forward; that is enforced by a test, not by good intentions.
# ----------------------------------------------------------------------------
FEATURES: dict[str, Any] = {}


def feature(name: str):
    def register(fn):
        FEATURES[name] = fn
        return fn

    return register


@feature("ret_1")
def _ret_1(px: pd.Series, **_) -> pd.Series:
    return px.pct_change()


@feature("ret_5")
def _ret_5(px: pd.Series, **_) -> pd.Series:
    return px.pct_change(5)


@feature("ret_20")
def _ret_20(px: pd.Series, **_) -> pd.Series:
    return px.pct_change(20)


@feature("vol_20")
def _vol_20(px: pd.Series, **_) -> pd.Series:
    return px.pct_change().rolling(20).std()


@feature("vol_60")
def _vol_60(px: pd.Series, **_) -> pd.Series:
    return px.pct_change().rolling(60).std()


@feature("mom_10")
def _mom_10(px: pd.Series, **_) -> pd.Series:
    return px / px.shift(10) - 1.0


@feature("mom_60")
def _mom_60(px: pd.Series, **_) -> pd.Series:
    return px / px.shift(60) - 1.0


@feature("dist_ma50")
def _dist_ma50(px: pd.Series, **_) -> pd.Series:
    return px / px.rolling(50).mean() - 1.0


@feature("dist_ma200")
def _dist_ma200(px: pd.Series, **_) -> pd.Series:
    return px / px.rolling(200).mean() - 1.0


@feature("rsi_14")
def _rsi_14(px: pd.Series, **_) -> pd.Series:
    return _rsi(px, 14)


@feature("vol_ratio")
def _vol_ratio(px: pd.Series, **_) -> pd.Series:
    """Short volatility against long: is the market waking up or going quiet."""
    r = px.pct_change()
    return r.rolling(10).std() / r.rolling(60).std() - 1.0


@feature("volume_z")
def _volume_z(px: pd.Series, frame: pd.DataFrame | None = None, **_) -> pd.Series:
    """Volume against its own recent history. Needs the volume column; returns
    NaN when the input has none, rather than pretending."""
    if frame is None or "volume" not in frame.columns:
        return pd.Series(np.nan, index=px.index)
    vol = frame["volume"].astype(float)
    return (vol - vol.rolling(60).mean()) / vol.rolling(60).std()


DEFAULT_FEATURES = ["ret_1", "ret_5", "vol_20", "mom_10", "dist_ma50", "rsi_14"]


def make_features(prices: pd.DataFrame, *, price_col: str = "adj_close",
                  time_col: str = "trade_date", horizon: int = 1,
                  features: list[str] | None = None) -> pd.DataFrame:
    """Build the declared features and the target.

    THE RULE: every feature at row t uses information available at the close of
    t, and the target is the return from t to t+horizon. Nothing else is allowed
    to touch the target. This is the line where look-ahead bias gets in, so it
    lives on its own and is covered by a test.
    """
    names = features or DEFAULT_FEATURES
    unknown = [n for n in names if n not in FEATURES]
    if unknown:
        raise KeyError(f"unknown features {unknown}; registered: {sorted(FEATURES)}")

    keep = [c for c in (time_col, price_col, "volume") if c in prices.columns]
    df = prices[keep].dropna(subset=[price_col]).sort_values(time_col).reset_index(drop=True)
    px = df[price_col]

    for name in names:
        df[name] = FEATURES[name](px, frame=df)

    # Target: forward return. shift(-horizon) is the only forward-looking line
    # in this file, and it is the label, never an input.
    df["target"] = px.shift(-horizon) / px - 1.0

    df.attrs["feature_cols"] = list(names)
    return df


def _rsi(px: pd.Series, window: int) -> pd.Series:
    delta = px.diff()
    up = delta.clip(lower=0).rolling(window).mean()
    down = (-delta.clip(upper=0)).rolling(window).mean()
    rs = up / down.replace(0, np.nan)
    return 100 - (100 / (1 + rs))


# ----------------------------------------------------------------------------
# Walk-forward evaluation
# ----------------------------------------------------------------------------
@dataclass
class FoldResult:
    fold: int
    train_end: Any
    test_start: Any
    test_end: Any
    n_train: int
    n_test: int
    model_rmse: float
    baseline_rmse: float
    model_dir_acc: float
    baseline_dir_acc: float

    @property
    def skill(self) -> float:
        """How much better than the baseline, as a fraction. Negative means worse."""
        if self.baseline_rmse == 0:
            return 0.0
        return 1.0 - (self.model_rmse / self.baseline_rmse)


@dataclass
class WalkForwardResult:
    folds: list[FoldResult] = field(default_factory=list)
    predictions: pd.DataFrame = field(default_factory=pd.DataFrame)
    purge: int = 0      # training rows dropped per fold to stop the label leaking

    def frame(self) -> pd.DataFrame:
        return pd.DataFrame([f.__dict__ | {"skill": f.skill} for f in self.folds])

    def metrics(self) -> dict[str, float]:
        f = self.frame()
        if f.empty:
            return {}
        return {
            "folds": float(len(f)),
            "model_rmse": float(f["model_rmse"].mean()),
            "baseline_rmse": float(f["baseline_rmse"].mean()),
            "skill_vs_baseline": float(f["skill"].mean()),
            "model_dir_acc": float(f["model_dir_acc"].mean()),
            "baseline_dir_acc": float(f["baseline_dir_acc"].mean()),
            "folds_beating_baseline": float((f["skill"] > 0).sum()),
        }

    def verdict(self) -> str:
        m = self.metrics()
        if not m:
            return "no folds evaluated"
        beat = int(m["folds_beating_baseline"])
        total = int(m["folds"])
        if m["skill_vs_baseline"] <= 0:
            # "even though {beat} of {total}" used to be hard-coded into this
            # sentence, which implied a majority. At ten folds of twenty-one it
            # is not one, and a summary line that overstates its own result is
            # the last place to be careless.
            share = (
                "and most folds do not either" if beat * 2 < total
                else f"though {beat} of {total} folds beat it individually"
            )
            return (
                f"Mean skill against the naive baseline is {m['skill_vs_baseline']:.2%}, so on "
                f"average the model does not beat it, {share}. The distribution matters more "
                "than the headline number either way. On daily commodity closes this is the "
                "expected result, and being able to say so is the point of running it this way."
            )
        return (
            f"Mean skill against the baseline is {m['skill_vs_baseline']:.2%} "
            f"({beat}/{total} folds). Before calling that a signal it needs costs, "
            "slippage and a far longer sample."
        )


def walk_forward(
    features: pd.DataFrame,
    *,
    time_col: str = "trade_date",
    min_train: int = 250,
    test_size: int = 21,
    model: str = "ridge",
    alpha: float = 1.0,
    horizon: int = 1,
    purge: int | None = None,
) -> WalkForwardResult:
    """Expanding-window walk forward with a purge gap between train and test.

    Train on the past, test on the next block, move forward. Never a random
    split: rows are not exchangeable in time.

    The purge is the part worth explaining. A target at horizon `h` is a forward
    return, so the label on the last `h` training rows is computed from prices
    that fall inside the test block. Training on them leaks the answer. The
    standard fix is to drop the last `h` rows of each training window, and
    `purge` defaults to exactly that.

    It matters more here than it usually does, because the recipe compares
    scenarios at different horizons side by side. Without a purge, the five-day
    scenario leaked five times as far into its test blocks as the one-day
    scenario, so the ranking was partly measuring leakage rather than the change
    being tested - and it flattered the longer horizon, which is the direction
    that makes a result look good.
    """
    cols = features.attrs.get("feature_cols") or [
        c for c in features.columns if c not in (time_col, "target", "adj_close", "close")
    ]
    df = features.dropna(subset=cols + ["target"]).reset_index(drop=True)
    out = WalkForwardResult()
    preds: list[pd.DataFrame] = []
    if len(df) < min_train + test_size:
        return out

    gap = max(int(horizon) - 1, 0) if purge is None else max(int(purge), 0)
    out.purge = gap
    start = min_train
    fold = 0
    while start + test_size <= len(df):
        # The gap is carved out of the END of training, not the start of test:
        # the test block must stay contiguous or the evaluation is no longer a
        # simulation of trading it.
        train = df.iloc[: max(start - gap, 1)]
        test = df.iloc[start : start + test_size]
        x_train, y_train = train[cols].to_numpy(), train["target"].to_numpy()
        x_test, y_test = test[cols].to_numpy(), test["target"].to_numpy()

        beta = _fit(x_train, y_train, model=model, alpha=alpha)
        y_hat = _predict(x_test, beta)

        # The baseline a forecast has to beat: no change. It is hard to beat, and
        # a model that cannot is not a model.
        y_base = np.zeros_like(y_test)

        out.folds.append(
            FoldResult(
                fold=fold,
                train_end=train[time_col].iloc[-1],
                test_start=test[time_col].iloc[0],
                test_end=test[time_col].iloc[-1],
                n_train=len(train),
                n_test=len(test),
                model_rmse=_rmse(y_test, y_hat),
                baseline_rmse=_rmse(y_test, y_base),
                model_dir_acc=_dir_acc(y_test, y_hat),
                # A no-change forecast has no direction, so its "accuracy" is a
                # convention, not a measurement: 0.5 is the coin-flip a
                # directionless prediction is worth. Named here because it sits
                # next to a measured number in the output and would otherwise
                # look like one.
                baseline_dir_acc=0.5,
            )
        )
        preds.append(
            pd.DataFrame({time_col: test[time_col].to_numpy(), "actual": y_test,
                          "predicted": y_hat, "fold": fold})
        )
        start += test_size
        fold += 1

    out.predictions = pd.concat(preds, ignore_index=True) if preds else pd.DataFrame()
    return out


def _fit(x: np.ndarray, y: np.ndarray, *, model: str, alpha: float) -> np.ndarray:
    """Ridge by normal equations. Deliberately small: no sklearn dependency, and
    nothing here is the interesting part of the presentation."""
    mu, sigma = x.mean(0), x.std(0)
    sigma[sigma == 0] = 1.0
    xs = np.column_stack([np.ones(len(x)), (x - mu) / sigma])
    penalty = np.eye(xs.shape[1]) * (alpha if model == "ridge" else 0.0)
    penalty[0, 0] = 0.0  # never penalise the intercept
    beta = np.linalg.solve(xs.T @ xs + penalty, xs.T @ y)
    return np.concatenate([beta, mu, sigma])


def _predict(x: np.ndarray, packed: np.ndarray) -> np.ndarray:
    n = x.shape[1]
    beta, mu, sigma = packed[: n + 1], packed[n + 1 : 2 * n + 1], packed[2 * n + 1 :]
    xs = np.column_stack([np.ones(len(x)), (x - mu) / sigma])
    return xs @ beta


def _rmse(y: np.ndarray, y_hat: np.ndarray) -> float:
    return float(np.sqrt(np.mean((y - y_hat) ** 2)))


def _dir_acc(y: np.ndarray, y_hat: np.ndarray) -> float:
    mask = y != 0
    if not mask.any():
        return 0.5
    return float((np.sign(y[mask]) == np.sign(y_hat[mask])).mean())
