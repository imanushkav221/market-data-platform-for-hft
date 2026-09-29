"""Acquisition: fetching from a source, and nothing else.

Deliberately separated from storage. Everything source-specific lives here
(endpoints, paging, retries, the shape a provider happens to send); everything
about where bytes end up lives in landing.py and storage.py. Adding a provider
touches one function in this file and one YAML file in config/sources.
"""
from __future__ import annotations

import time
from collections.abc import Callable
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd

from .config import SourceConfig

Driver = Callable[[SourceConfig, dict], pd.DataFrame]
_DRIVERS: dict[str, Driver] = {}


def driver(name: str) -> Callable[[Driver], Driver]:
    def register(fn: Driver) -> Driver:
        _DRIVERS[name] = fn
        return fn

    return register


def acquire(cfg: SourceConfig, **params) -> pd.DataFrame:
    """Fetch one batch for a source, with the retry policy from its config."""
    name = cfg.acquire["driver"]
    if params.get("synthetic"):
        name = "synthetic"
    if params.get("local_file") and cfg.acquire.get("drop_driver"):
        # A file that landed is read by the drop driver, whatever the source
        # normally uses to fetch.
        name = cfg.acquire["drop_driver"]
    if name not in _DRIVERS:
        raise KeyError(f"no acquisition driver named {name!r}")
    policy = cfg.acquire.get("retry", {})
    attempts = int(policy.get("attempts", 1))
    backoff = float(policy.get("backoff_seconds", 1.0))
    last: Exception | None = None
    for attempt in range(1, attempts + 1):
        try:
            return _DRIVERS[name](cfg, params)
        except Exception as exc:  # retry is a property of acquisition, not of the caller
            last = exc
            if attempt == attempts:
                break
            time.sleep(backoff * (2 ** (attempt - 1)))
    raise RuntimeError(f"{cfg.name}: acquisition failed after {attempts} attempts: {last}")


# --------------------------------------------------------------------------
# Real sources
# --------------------------------------------------------------------------
@driver("binance_agg_trades")
def _binance_agg_trades(cfg: SourceConfig, params: dict) -> pd.DataFrame:
    """Public aggregated trades. Paged backwards from `end` until the window is covered."""
    import requests

    base = cfg.acquire["base_url"].rstrip("/")
    symbols = params.get("symbols") or cfg.acquire.get("symbols", [])
    minutes = int(params.get("window_minutes") or cfg.acquire.get("window_minutes", 60))
    limit = int(cfg.acquire.get("max_rows_per_request", 1000))
    end = params.get("end") or datetime.now(UTC)
    # `start` comes from the cursor when there is one, so a run picks up where the
    # last successful load finished instead of guessing a window.
    start = params.get("start") or (end - timedelta(minutes=minutes))

    hour_ms = 60 * 60 * 1000
    frames = []
    for symbol in symbols:
        cursor = int(start.timestamp() * 1000)
        end_ms = int(end.timestamp() * 1000)
        # Set when a full page turns out to sit entirely inside one millisecond,
        # which is the only case a timestamp cursor cannot page through.
        resume_from_id: int | None = None
        while cursor < end_ms:
            query: dict = {"symbol": symbol, "limit": limit}
            if resume_from_id is None:
                # `startTime` and `endTime` are both inclusive, so the boundary
                # millisecond is fetched by two consecutive windows. That is left
                # alone deliberately: the duplicate prints are removed by the
                # contract gate's uniqueness rule on (venue, symbol, trade_id),
                # and shaving a millisecond off the window to avoid them would
                # risk dropping a trade to save nothing.
                query["startTime"] = cursor
                query["endTime"] = min(cursor + hour_ms, end_ms)
            else:
                query["fromId"] = resume_from_id
            resp = requests.get(f"{base}/api/v3/aggTrades", params=query, timeout=20)
            resp.raise_for_status()
            page = resp.json()
            # `fromId` ignores the time window, so the tail past the window we
            # were asked for is dropped here rather than by the endpoint.
            rows = [r for r in page if r["T"] <= end_ms] if resume_from_id else page
            if not rows:
                if resume_from_id is not None:
                    break
                cursor += hour_ms
                continue
            frames.append(
                pd.DataFrame(
                    {
                        "venue": "binance",
                        "symbol": symbol,
                        "trade_id": [r["a"] for r in rows],
                        "ts": [r["T"] for r in rows],
                        "price": [r["p"] for r in rows],
                        "qty": [r["q"] for r in rows],
                        "is_buyer_maker": [r["m"] for r in rows],
                    }
                )
            )
            if len(rows) < len(page):
                break                       # the window ended inside this page
            if len(page) < limit:
                # A short page means the request returned everything it held.
                cursor = cursor + hour_ms if resume_from_id is None else rows[-1]["T"] + 1
                resume_from_id = None
            elif rows[-1]["T"] > rows[0]["T"]:
                # A full page that did advance in time. Resume AT its last
                # timestamp rather than one millisecond after it: trades sharing
                # that millisecond which did not fit on this page would
                # otherwise be skipped outright, and skipped trades are invisible
                # while the repeats this causes are dropped by the gate.
                cursor = rows[-1]["T"]
                resume_from_id = None
            else:
                # A full page entirely inside one millisecond. Moving the cursor
                # past that millisecond loses every trade left in it and leaving
                # it where it is refetches the same page forever, so page by
                # aggregate trade id, which is what the endpoint offers for
                # exactly this.
                cursor = rows[-1]["T"]
                resume_from_id = int(rows[-1]["a"]) + 1
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


@driver("mcx_bhavcopy")
def _mcx_bhavcopy(cfg: SourceConfig, params: dict) -> pd.DataFrame:
    """MCX published end-of-day file for one trading date.

    Only the free published EOD file is used. Intraday and tick products are
    licensed separately; see the contract's lineage block. If the exchange
    endpoint is unreachable, drop the file into data/incoming/ and pass
    `local_file` instead, which is also how a vendor SFTP drop would work.
    """
    import io

    import requests

    when: date = params.get("trade_date") or date.today()
    local = params.get("local_file")
    if local:
        raw = pd.read_csv(local)
    else:
        base = cfg.acquire["base_url"].rstrip("/")
        url = base + cfg.acquire["path_template"].format(date=when)
        resp = requests.get(url, timeout=30, headers={"User-Agent": "market-data-platform/1.0"})
        resp.raise_for_status()
        raw = pd.read_csv(io.StringIO(resp.text))

    cols = {c.lower().strip().replace(" ", "_"): c for c in raw.columns}

    def pick(*names, default=None):
        for n in names:
            if n in cols:
                return raw[cols[n]]
        return default

    out = pd.DataFrame(
        {
            "exchange": "MCX",
            "symbol": pick("symbol", "commodity", "instrument"),
            "expiry": pick("expirydate", "expiry_date", "expiry"),
            "trade_date": pick("date", "tradedate", "trade_date"),
            "open": pick("open", "open_price"),
            "high": pick("high", "high_price"),
            "low": pick("low", "low_price"),
            "close": pick("close", "close_price"),
            "settle": pick("settle", "settlement_price", "settle_price"),
            "volume": pick("volume", "volume_lots", "qty"),
            "open_interest": pick("open_interest", "oi"),
        }
    )
    symbols = params.get("symbols") or cfg.acquire.get("symbols")
    if symbols is not None and "symbol" in out:
        out = out[out["symbol"].isin(symbols)]
    return out.reset_index(drop=True)


@driver("file_drop")
def _file_drop(cfg: SourceConfig, params: dict) -> pd.DataFrame:
    """Read a file that landed in a drop directory.

    This is how most vendor data actually arrives: an SFTP or object-storage
    delivery, not an API call. The watcher hands the path in; everything
    downstream is identical to a fetched source.
    """
    path = Path(params["local_file"])
    suffix = path.suffix.lower()
    if suffix in (".parquet", ".pq"):
        df = pd.read_parquet(path)
    elif suffix in (".csv", ".txt"):
        df = pd.read_csv(path)
    elif suffix == ".json":
        df = pd.read_json(path)
    else:
        raise ValueError(f"no reader for {suffix} files")

    # A vendor's column names are the vendor's business. Mapping them lives in
    # config, so a provider renaming a field is a one-line change rather than a
    # code deploy. Anything not mapped goes to the contract gate as-is and is
    # rejected there if it does not belong.
    drop = cfg.acquire.get("drop") or {}
    column_map = drop.get("column_map") or {}
    if column_map:
        df = df.rename(columns={k: v for k, v in column_map.items() if k in df.columns})
    for field, value in (drop.get("constants") or {}).items():
        df[field] = value
    return df


# --------------------------------------------------------------------------
# Synthetic source: makes the whole pipeline runnable with no network.
# It deliberately emits the defects a real feed emits, so the validation and
# quality stages have something to catch during a live demo.
# --------------------------------------------------------------------------
def _next_expiries(day: pd.Timestamp, day_of_month: int, count: int = 2) -> list[pd.Timestamp]:
    """The next `count` monthly contract expiries on or after `day`.

    Each commodity has its own convention (crude around the 19th of the month
    before delivery, gas the 25th, bullion the 5th). An expiry landing on a
    weekend moves back to the previous business day, which is what makes the
    front-month map worth testing.
    """
    out = []
    cursor = pd.Timestamp(year=day.year, month=day.month, day=1)
    for _ in range(count + 3):
        if day_of_month:
            expiry = pd.Timestamp(year=cursor.year, month=cursor.month, day=day_of_month)
        else:
            expiry = (cursor + pd.offsets.MonthEnd(1)).normalize() - pd.offsets.BDay(3)
        if expiry.weekday() >= 5:
            expiry -= pd.offsets.BDay(1)
        if expiry >= day:
            out.append(expiry)
            if len(out) == count:
                break
        cursor += pd.offsets.MonthBegin(1)
    return out


@driver("synthetic")
def _synthetic(cfg: SourceConfig, params: dict) -> pd.DataFrame:
    seed = int(params.get("seed", 7))
    rng = np.random.default_rng(seed)
    clean_only = bool(params.get("clean", False))

    if cfg.dataset == "binance_trades":
        symbols = params.get("symbols") or cfg.acquire.get("symbols", ["BTCUSDT"])
        minutes = int(params.get("window_minutes") or cfg.acquire.get("window_minutes", 60))
        end = params.get("end") or datetime.now(UTC)
        start = params.get("start")
        if start is not None:
            minutes = max(1, int((end - start).total_seconds() // 60))
        frames = []
        for i, symbol in enumerate(symbols):
            n = minutes * 120
            start_ms = int((start or (end - timedelta(minutes=minutes))).timestamp() * 1000)
            gaps = rng.exponential(scale=500, size=n).cumsum()
            ts = start_ms + gaps.astype("int64")
            # Never emit a print from after the window we were asked for. Without
            # this the cursor lands in the future and the next window reads
            # backwards, which is a confusing way to discover an off-by-one.
            end_ms = int(end.timestamp() * 1000)
            ts = ts[ts <= end_ms]
            n = len(ts)
            if n == 0:
                continue
            px0 = 60000.0 if symbol.startswith("BTC") else 3000.0
            steps = rng.normal(0, px0 * 2e-5, size=n)
            price = px0 * np.exp(np.cumsum(steps) / px0)
            df = pd.DataFrame(
                {
                    "venue": "binance",
                    "symbol": symbol,
                    "trade_id": np.arange(1 + i * 10**7, 1 + i * 10**7 + n, dtype="int64"),
                    "ts": ts,
                    "price": np.round(price, 2),
                    "qty": np.round(rng.lognormal(-3, 1, size=n), 6),
                    "is_buyer_maker": rng.random(n) < 0.5,
                }
            )
            if not clean_only:
                # Defects a real feed produces, so the gate has work to do:
                dupes = df.sample(n=max(2, n // 500), random_state=seed)  # reconnect replay
                bad_price = df.sample(n=max(1, n // 1000), random_state=seed + 1).copy()
                bad_price["price"] = 0.0  # a zero print
                future = df.tail(1).copy()
                future["ts"] = int((end + timedelta(hours=2)).timestamp() * 1000)  # clock skew
                future["trade_id"] = future["trade_id"] + 10**6
                df = pd.concat([df, dupes, bad_price, future], ignore_index=True)
            frames.append(df)
        if not frames:
            # Every symbol produced nothing, which happens whenever the window
            # asked for is shorter than the gap between prints. That is an empty
            # window, not a failure: `pd.concat([])` raises, the retry policy
            # then burns all five attempts on it and the run dies on something
            # the caller could simply have skipped. The MCX branch below already
            # returns an empty frame here, and so does the real Binance driver.
            return pd.DataFrame()
        return pd.concat(frames, ignore_index=True).sort_values("ts").reset_index(drop=True)

    if cfg.dataset == "mcx_bhavcopy":
        symbols = params.get("symbols") or cfg.acquire.get("symbols", ["GOLD"])
        days = int(params.get("days", 750))
        end_day = params.get("trade_date") or date.today()
        rows = []
        # INR per unit, roughly the right order of magnitude for each contract.
        base_px = {"CRUDEOIL": 6200.0, "NATURALGAS": 250.0, "GOLD": 72000.0,
                   "SILVER": 88000.0, "COPPER": 820.0}
        # MCX expiry conventions differ by commodity, and the roll depends on
        # them: crude oil expires around the 19th of the month before delivery,
        # natural gas around the 25th, bullion on the 5th of the delivery month.
        expiry_day = {"CRUDEOIL": 19, "NATURALGAS": 25, "GOLD": 5, "SILVER": 5}
        # Daily volatility: crude moves several times as much as bullion, which
        # is the reason it is the interesting contract for a trading desk.
        daily_vol = {"CRUDEOIL": 0.021, "NATURALGAS": 0.030, "GOLD": 0.009,
                     "SILVER": 0.014, "COPPER": 0.012}
        for symbol in symbols:
            px = base_px.get(symbol, 1000.0)
            sigma = daily_vol.get(symbol, 0.012)
            day_of_month = expiry_day.get(symbol, 0)
            # Monthly contracts, so the roll behaviour is real.
            anchor = np.log(base_px.get(symbol, 1000.0))
            kappa = 0.012      # how hard the price is pulled back to the anchor
            for d in pd.bdate_range(end=pd.Timestamp(end_day), periods=days):
                # Mean-reverting rather than a pure random walk. A three-year
                # random walk on 2% daily vol wanders to double the starting
                # price, which no commodity desk would recognise; crude spends
                # its life oscillating around a range. It also makes the
                # forecasting exercise honest, because mean reversion is a real
                # effect a naive model can partly capture.
                px = float(np.exp(
                    np.log(px) + kappa * (anchor - np.log(px)) + rng.normal(0, sigma)
                ))
                for k, expiry in enumerate(_next_expiries(d, day_of_month, 2)):
                    carry = 1.0 + 0.0015 * k
                    o = px * carry * float(np.exp(rng.normal(0, 0.002)))
                    c = px * carry * float(np.exp(rng.normal(0, 0.004)))
                    h = max(o, c) * float(np.exp(abs(rng.normal(0, 0.003))))
                    lo = min(o, c) * float(np.exp(-abs(rng.normal(0, 0.003))))
                    vol = float(rng.integers(2000, 120000)) / (1 + 3 * k)
                    rows.append(
                        {
                            "exchange": "MCX",
                            "symbol": symbol,
                            "expiry": expiry.date().isoformat(),
                            "trade_date": d.date().isoformat(),
                            "open": round(o, 2),
                            "high": round(h, 2),
                            "low": round(lo, 2),
                            "close": round(c, 2),
                            "settle": round(c, 2),
                            "volume": vol,
                            "open_interest": float(rng.integers(1000, 90000)),
                        }
                    )
        df = pd.DataFrame(rows)
        if not clean_only and len(df) > 20:
            broken = df.sample(n=3, random_state=seed).copy()
            broken["high"] = broken["low"] - 1.0  # high below low: impossible, must be caught
            df = pd.concat([df, broken], ignore_index=True)
        return df.reset_index(drop=True)

    raise KeyError(f"no synthetic generator for dataset {cfg.dataset!r}")
