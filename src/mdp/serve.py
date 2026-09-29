"""The internal application: what a quant developer actually imports.

Nobody outside this file should need to know whether the data is in QuestDB or
DuckDB, how bars are built, which contract was front-month in March, or what the
partition key is. One import, four functions, no SQL:

    from mdp.serve import MarketData
    md = MarketData()
    bars = md.get_bars("BTCUSDT", "2026-09-20", "2026-09-21", freq="5m")
    gold = md.get_continuous("GOLD", adjust="ratio", as_of="2026-06-30")
"""
from __future__ import annotations

from datetime import datetime

import pandas as pd

from . import entitlements
from .reference import continuous_series
from .research import as_of_prices, with_features
from .storage import Store, get_store


def _looks_like_missing_table(exc: Exception) -> bool:
    """Distinguish "this dataset is not loaded yet" from "the store is broken".

    Both stores say so in prose rather than with a typed exception, so this is
    string matching, which is unpleasant. It is still better than the previous
    behaviour of catching everything, because that turned an unreachable
    database into an empty catalogue: the platform reporting confidently that
    there is no data, when what it means is that it could not ask.
    """
    text = str(exc).lower()
    return any(
        phrase in text
        for phrase in ("does not exist", "doesn't exist", "unknown table",
                       "no such table", "table or view not found", "not found")
    )


def _latest_per_grain(df: pd.DataFrame, grain: list[str]) -> pd.DataFrame:
    """Keep one row per grain, the most recently ingested.

    QuestDB deliberately does not collapse a correction on eod_bars -- `ingested_at`
    is in the dedup key precisely so the row a report already saw survives -- and
    DuckDB does not collapse duplicates at all. Consumers should never see a
    duplicate because of where the data happens to live, so the library settles
    it on read.
    """
    if df.empty or not all(c in df.columns for c in grain):
        return df
    if "ingested_at" not in df.columns:
        # Without an ingest time there is no "most recent", and silently keeping
        # whichever row the query happened to return last is how a correction
        # loses to the row it was correcting. This used to happen for real: the
        # two stores spelled the column differently, so the sort never ran on
        # DuckDB and the two backends resolved the same correction in opposite
        # directions. Say so rather than guessing.
        raise ValueError(
            "cannot resolve duplicates without an ingested_at column; "
            f"got {list(df.columns)}"
        )
    if df["ingested_at"].isna().all():
        raise ValueError(
            "ingested_at is null on every row, so the most recent ingest cannot "
            "be identified; the column default did not fire on insert"
        )
    df = df.sort_values("ingested_at", kind="stable")
    return df.drop_duplicates(subset=grain, keep="last").reset_index(drop=True)


class MarketData:
    """The one door every consumer goes through.

    `consumer` decides what this caller may read. It defaults to the policy's
    default rather than to "everything", because a default of everything makes
    the entitlement file decorative.
    """

    def __init__(self, store: Store | None = None, kind: str | None = None,
                 consumer: str | None = None, enforce: bool = True):
        self.store = store or get_store(kind)
        self.consumer = consumer or entitlements.load_policy().default_consumer
        self.enforce = enforce

    def _may_read(self, dataset: str) -> None:
        entitlements.check(
            self.consumer, dataset, store=self.store, enforce=self.enforce
        )

    # ---------------- intraday ----------------
    def get_bars(self, symbol: str, start=None, end=None, freq: str = "1m",
                 venue: str = "binance") -> pd.DataFrame:
        """OHLCV bars at the requested frequency. Minute bars are maintained on
        insert; anything coarser is rolled up from them."""
        self._may_read("binance_trades")
        return self.store.bars(symbol, start, end, freq, venue)

    def get_trades(self, symbol: str, start=None, end=None,
                   venue: str = "binance", limit: int | None = None) -> pd.DataFrame:
        self._may_read("binance_trades")
        where = [f"symbol = '{symbol}'", f"venue = '{venue}'"]
        if start:
            where.append(f"ts >= '{pd.Timestamp(start)}'")
        if end:
            where.append(f"ts < '{pd.Timestamp(end)}'")
        sql = f"SELECT * FROM trades WHERE {' AND '.join(where)} ORDER BY ts"
        if limit:
            sql += f" LIMIT {int(limit)}"
        return self.store.query(sql)

    def as_of(self, symbol: str, timestamps, *, venue: str = "binance",
              max_staleness_seconds: float = 60.0) -> pd.DataFrame:
        """What was the price at each of these moments, and how stale was it.

        The most-asked question in research, and the easiest place to leak the
        future. Strict at-or-before matching, an explicit staleness bound, and
        the age of every match returned so nobody has to trust it blindly.
        """
        self._may_read("binance_trades")
        return as_of_prices(
            self.store, symbol, timestamps, venue=venue,
            max_staleness_seconds=max_staleness_seconds,
        )

    def research_frame(self, symbol: str, timestamps, *, venue: str = "binance",
                       vol_window_minutes: int = 30) -> pd.DataFrame:
        """As-of price plus trailing realised volatility: a research-ready row
        per decision time, with no value in it drawn from after that time."""
        self._may_read("binance_trades")
        return with_features(
            self.store, symbol, timestamps, venue=venue,
            vol_window_minutes=vol_window_minutes,
        )

    # ---------------- end of day ----------------
    def get_eod(self, symbol: str, start=None, end=None, exchange: str = "MCX") -> pd.DataFrame:
        self._may_read("mcx_bhavcopy")
        where = [f"symbol = '{symbol}'", f"exchange = '{exchange}'"]
        if start:
            where.append(f"trade_date >= DATE '{pd.Timestamp(start).date()}'")
        if end:
            where.append(f"trade_date <= DATE '{pd.Timestamp(end).date()}'")
        df = self.store.query(
            f"SELECT * FROM eod_bars WHERE {' AND '.join(where)} ORDER BY trade_date, expiry"
        )
        return _latest_per_grain(df, ["exchange", "symbol", "expiry", "trade_date"])

    def get_continuous(self, symbol: str, *, method: str = "volume", adjust: str = "ratio",
                       as_of: str | datetime | None = None,
                       exchange: str = "MCX") -> pd.DataFrame:
        """A single price series for a futures symbol, with the roll handled.

        `as_of` gives you the series as it stood on that date, which is the only
        honest way to feed a backtest.
        """
        self._may_read("mcx_bhavcopy")
        eod = self.store.query(
            f"SELECT * FROM eod_bars WHERE symbol = '{symbol}' AND exchange = '{exchange}' "
            f"ORDER BY trade_date, expiry"
        )
        if eod.empty:
            return eod
        # Duplicates here would silently corrupt the roll: two rows for the same
        # contract-day make the volume rule pick a contract twice.
        eod = _latest_per_grain(eod, ["exchange", "symbol", "expiry", "trade_date"])
        return continuous_series(
            eod, symbol=symbol, method=method, adjust=adjust, as_of=as_of,
            roll_map=self._roll_map(symbol, as_of=as_of, exchange=exchange),
        )

    def _roll_map(self, symbol: str, *, as_of=None, exchange: str = "MCX") -> pd.DataFrame:
        """The front-month choice as it was believed on a date.

        This is what makes `as_of` mean what the documentation says it means. It
        is tempting to read `as_of` as "truncate the price history and re-run the
        rule", and that is what used to happen, but the two are not the same
        thing. Re-running the rule uses today's settlements, so a correction that
        arrived last week can change which contract was front on a day that is
        already inside a published backtest, and nothing anywhere records that
        the answer moved.

        Returns empty when the map has not been built, and the caller then falls
        back to applying the rule directly, which is right for a fresh dataset.
        """
        day = pd.Timestamp(as_of).date() if as_of is not None else None
        where = [f"symbol = '{symbol}'", f"exchange = '{exchange}'", "is_front"]
        if day is not None:
            # The window is inclusive at both ends: known_from is the first day
            # we held this belief and known_to the last.
            where.append(f"known_from <= DATE '{day}'")
            where.append(f"known_to >= DATE '{day}'")
        try:
            return self.store.query(
                f"SELECT exchange, symbol, trade_date, expiry, known_from, known_to "
                f"FROM contract_reference WHERE {' AND '.join(where)} "
                f"ORDER BY trade_date, known_from"
            )
        except Exception as exc:  # noqa: BLE001
            if _looks_like_missing_table(exc):
                return pd.DataFrame()
            raise

    # ---------------- contract specifications ----------------
    def get_specs(self, symbol: str, *, as_of=None, exchange: str = "MCX") -> dict:
        """Lot size, tick size and units as they stood on a date.

        A lot size that changed in March means a backtest over March using
        today's multiplier is quietly wrong by a constant factor, and constant
        factors are the hardest errors to notice in a P&L.
        """
        self._may_read("mcx_bhavcopy")
        where = [f"symbol = '{symbol}'", f"exchange = '{exchange}'"]
        if as_of is not None:
            day = pd.Timestamp(as_of).date()
            where.append(f"known_from <= DATE '{day}'")
            where.append(f"known_to >= DATE '{day}'")
        rows = self.store.query(
            f"SELECT * FROM contract_specs WHERE {' AND '.join(where)} "
            f"ORDER BY known_from DESC"
        )
        return {} if rows.empty else rows.iloc[0].to_dict()

    def notional(self, symbol: str, price: float, lots: float = 1.0, *,
                 as_of=None, exchange: str = "MCX") -> float:
        """What a position is actually worth, using the spec of the day.

        Raises when no spec version covers the date. The previous behaviour was
        to fall back to a multiplier of 1.0, which is the exact error
        `get_specs` warns about in its own docstring, only worse: a silent
        hundredfold understatement of a position, produced by the function whose
        entire job is to prevent it. An unknown symbol or a date before the spec
        history begins is a question, not a number.
        """
        spec = self.get_specs(symbol, as_of=as_of, exchange=exchange)
        if not spec or spec.get("lot_size") in (None, ""):
            when = f" as of {pd.Timestamp(as_of).date()}" if as_of is not None else ""
            raise LookupError(
                f"no contract specification for {exchange}:{symbol}{when}; "
                f"refusing to guess a lot size"
            )
        return price * float(spec["lot_size"]) * lots

    # ---------------- housekeeping ----------------
    def catalog(self) -> pd.DataFrame:
        """What exists, how fresh it is. The first question every new joiner asks.

        Entitlement-aware: a consumer sees the datasets it may read and no
        others. A catalogue that lists what you cannot have is an index of
        things to go and ask for, which is not what an entitlement boundary is
        supposed to produce.
        """
        frames = []
        for table, dataset, tcol in (
            ("trades", "binance_trades", "ts"),
            ("eod_bars", "mcx_bhavcopy", "trade_date"),
        ):
            try:
                self._may_read(dataset)
            except PermissionError:
                continue
            try:
                frames.append(
                    self.store.query(
                        f"SELECT '{table}' AS dataset, symbol AS symbol, count(*) AS rows, "
                        f"min({tcol}) AS first_event, max({tcol}) AS last_event "
                        f"FROM {table} GROUP BY symbol"
                    )
                )
            except Exception as exc:  # noqa: BLE001
                # A missing table on a fresh install is expected. An unreachable
                # store is not, and returning an empty catalogue for it would
                # say "there is no data" when the truth is "I cannot tell".
                if not _looks_like_missing_table(exc):
                    raise
        return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()

    # The operational views below are themselves a dataset. `platform_metadata`
    # is declared in the entitlement policy alongside the market data, because
    # "who read the licensed feed and when" is exactly the kind of thing that
    # should not be readable by whoever asks.
    def quality(self, limit: int = 20) -> pd.DataFrame:
        self._may_read("platform_metadata")
        return self.store.query(
            f"SELECT * FROM quality_events ORDER BY event_at DESC LIMIT {int(limit)}"
        )

    def access_log(self, limit: int = 20) -> pd.DataFrame:
        """Who read what, and who was refused."""
        self._may_read("platform_metadata")
        return self.store.query(
            f"SELECT * FROM access_log ORDER BY event_at DESC LIMIT {int(limit)}"
        )

    def vendor_scores(self) -> pd.DataFrame:
        self._may_read("platform_metadata")
        return self.store.query("SELECT * FROM vendor_scores ORDER BY as_of DESC, source")
