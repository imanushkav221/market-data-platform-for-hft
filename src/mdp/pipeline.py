"""One source, end to end: acquire, validate, land, check, load, score.

The order matters and is the argument of the whole project:
raw bytes are kept before anything is interpreted, the contract is enforced
before anything is loaded, and nothing is published until the checks pass.
"""
from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

import pandas as pd

from . import cursor as cursor_store
from . import landing
from .acquire import acquire
from .config import SourceConfig
from .contracts import validate
from .quality import check_schema_version, run_checks, score_vendor
from .runs import new_run_id
from .storage import Store


def run_source(cfg: SourceConfig, store: Store, *, synthetic: bool = False,
               publish: bool = True, now: datetime | None = None,
               mode: str = "live", accept_schema_change: bool = False,
               **params) -> dict[str, Any]:
    """Load one source.

    `mode` separates two questions people conflate. A live load must answer "is
    this current?" as well as "is this correct?". A backfill of March can only
    answer the second: March is not current and never will be. Applying the
    freshness rule to it would block every historical load, and teaching people
    to pass --force around a blocked backfill is how a safety check becomes
    decoration. Dataset currency is covered separately, by the arrival monitor.
    """
    now = now or datetime.now(UTC)
    run_id = new_run_id("load")
    started = datetime.now(UTC)

    # 1. Acquire. Source-specific concerns stop here.
    raw = acquire(cfg, synthetic=synthetic, **params)
    raw_path = landing.write_raw(cfg, raw, when=now) if len(raw) else None

    # 2. Validate against the contract before anything reaches the database.
    result = validate(raw, cfg.contract, now=now)
    quarantine_path = landing.write_quarantine(cfg, result.rejected, when=now)

    # 3. Decide whether this load is safe to publish.
    report = run_checks(cfg, result, now=now, mode=mode)
    # Prepended so it reads first in the summary: if the contract moved, nothing
    # else in the report matters until a person has looked at it.
    report.results.insert(
        0, check_schema_version(cfg, store, accept=accept_schema_change)
    )

    rows_loaded = 0
    if publish and report.publishable and len(result.clean):
        frame = result.clean.copy()
        if "is_buyer_maker" in frame.columns:
            frame["is_buyer_maker"] = frame["is_buyer_maker"].astype("boolean")
        rows_loaded = store.insert(
            cfg.load_cfg["table"], frame, batch_rows=int(cfg.load_cfg.get("batch_rows", 50_000))
        )

    # 4. The cursor moves only now, and only to what actually landed. Advancing
    #    it on fetch instead would turn a blocked load into a permanent gap.
    cursor_advanced = None
    incremental = cfg.acquire.get("incremental") or {}
    if rows_loaded and incremental.get("enabled"):
        moved = cursor_store.advance(
            store, cfg.name, result.clean,
            time_column=incremental.get("time_column", "ts"),
            id_column=incremental.get("id_column"),
            # The window this run actually asked for. Without it a single
            # clock-skewed print carries the high-water mark past the end of the
            # window and the time in between is never fetched by anybody.
            window_end=params.get("end") or now,
        )
        cursor_advanced = moved.position_ts if moved else None

    # 5. Record what happened, always, including when nothing was loaded.
    events = report.to_frame(run_id)
    if not events.empty:
        store.insert("quality_events", events)
    score = score_vendor(cfg, result, report, as_of=now, mode=mode)
    store.insert("vendor_scores", pd.DataFrame([score]))

    return {
        "run_id": run_id,
        "source": cfg.name,
        "dataset": cfg.dataset,
        "rows_acquired": len(raw),
        "rows_clean": len(result.clean),
        "rows_quarantined": len(result.rejected),
        "rows_loaded": rows_loaded,
        "published": bool(rows_loaded),
        "blocked_by": [c.name for c in report.blocking],
        "cursor_at": cursor_advanced,
        "vendor_score": score["score"],
        "vendor_status": score["status"],
        "raw_path": str(raw_path) if raw_path else None,
        "quarantine_path": str(quarantine_path) if quarantine_path else None,
        "report": report,
        "validation": result,
        "seconds": (datetime.now(UTC) - started).total_seconds(),
    }


def build_reference(store: Store, *, method: str = "volume", now=None) -> dict[str, int]:
    """Refresh the front-month map, versioned rather than overwritten.

    This used to delete the table and rewrite it, which quietly made the
    point-in-time claim false in the one place it was supposed to be true. A
    settlement corrected last week can change which contract the volume rule
    picks for a day that is already sitting in a published backtest, and a full
    rewrite makes that change invisible: the old answer is gone, and the run
    that used it can never be reproduced.

    So a row is now closed rather than replaced. When today's rule picks a
    different front contract for a past day, the existing row gets
    `known_to = yesterday` and a new row opens with `known_from = today`. Asking
    the map "what did we believe on date X" then has an answer, and
    `get_continuous(as_of=X)` is the query that asks it.

    Returns counts rather than a single number, because "nothing changed" and
    "forty days were restated" are very different mornings and the caller should
    be able to tell them apart.
    """
    from .reference import front_month_map

    today = pd.Timestamp(now or datetime.now(UTC).date()).normalize()
    eod = store.query("SELECT exchange, symbol, expiry, trade_date, volume FROM eod_bars")
    if eod.empty:
        return {"opened": 0, "closed": 0, "unchanged": 0}

    chosen = front_month_map(eod, method=method)[
        ["exchange", "symbol", "trade_date", "expiry", "is_front"]
    ]
    chosen["trade_date"] = pd.to_datetime(chosen["trade_date"])
    chosen["expiry"] = pd.to_datetime(chosen["expiry"])

    try:
        current = store.query(
            "SELECT exchange, symbol, expiry, trade_date, first_trade, last_trade, "
            "is_front, known_from, known_to FROM contract_reference "
            "WHERE known_to >= DATE '2999-12-31'"
        )
    except Exception:  # noqa: BLE001 - a fresh install has no table yet
        current = pd.DataFrame()

    key = ["exchange", "symbol", "trade_date"]
    if current.empty:
        to_open, to_close = chosen, pd.DataFrame()
        unchanged = 0
    else:
        current["trade_date"] = pd.to_datetime(current["trade_date"])
        current["expiry"] = pd.to_datetime(current["expiry"])
        merged = chosen.merge(
            current[key + ["expiry"]].rename(columns={"expiry": "expiry_known"}),
            on=key, how="left",
        )
        changed = merged["expiry_known"].isna() | (
            merged["expiry_known"] != merged["expiry"]
        )
        to_open = merged.loc[changed, chosen.columns]
        unchanged = int((~changed).sum())
        superseded = merged.loc[
            changed & merged["expiry_known"].notna(), key
        ]
        to_close = current.merge(superseded, on=key, how="inner")

    closed = 0
    if not to_close.empty and _supports_delete(store):
        # Close the old row by rewriting it with a bounded known_to. Done as
        # delete-then-insert because the two stores disagree about UPDATE, and
        # the row count is small enough that it does not matter.
        conditions = " OR ".join(
            f"(exchange = '{r.exchange}' AND symbol = '{r.symbol}' "
            f"AND trade_date = DATE '{pd.Timestamp(r.trade_date).date()}' "
            f"AND known_to >= DATE '2999-12-31')"
            for r in to_close.itertuples()
        )
        store.query(f"DELETE FROM contract_reference WHERE {conditions}")
        closed = store.insert(
            "contract_reference",
            to_close.assign(known_to=today - pd.Timedelta(days=1)),
        )

    opened = 0
    if not to_open.empty:
        opened = store.insert("contract_reference", to_open.assign(
            first_trade=pd.NaT,
            last_trade=to_open["expiry"],
            known_from=today,
            known_to=pd.Timestamp("2999-12-31"),
        )[["exchange", "symbol", "expiry", "first_trade", "last_trade",
           "is_front", "known_from", "known_to", "trade_date"]])

    load_contract_specs(store)
    return {"opened": opened, "closed": closed, "unchanged": unchanged}


def restore_from_landing(cfg: SourceConfig, store: Store, *,
                         truncate: bool = True) -> dict[str, Any]:
    """Rebuild a dataset from the raw files, ignoring the database entirely.

    This is the claim "the landing zone is the source of truth" made executable.
    A backup nobody has restored is a hope, so there is a command for it and a
    test that runs it, and the rebuilt table is compared against the original.

    Everything goes through the same contract gate as a live load: a restore that
    skips validation would be a very efficient way to reinstate the bad data you
    were trying to get rid of.
    """
    raw = landing.replay(cfg.dataset)
    if raw.empty:
        return {"source": cfg.name, "rows_restored": 0, "detail": "landing zone is empty"}

    table = cfg.load_cfg["table"]

    # Validate BEFORE touching the existing table.
    #
    # The order used to be the other way around, and it turned a restore into a
    # delete. A landing file that fails the contract at schema level produces an
    # empty `clean`, so the table was emptied and then nothing was put back, and
    # the quality event recorded for it said `pass`. The operational record, the
    # one the monitor and the metrics endpoint read, reported a successful
    # rebuild of a table that no longer had anything in it.
    #
    # A restore is the operation you reach for when things have already gone
    # wrong. It is the last one that should be capable of making them worse.
    result = validate(raw, cfg.contract)
    report = run_checks(cfg, result, mode="backfill")
    blocked = [r.name for r in report.results if r.status == "fail"]
    if result.schema_failures or blocked or result.clean.empty:
        detail = (
            f"refused: {len(raw)} rows in the landing zone did not pass the "
            f"contract ({', '.join(blocked) or 'schema'}); {table} left untouched"
        )
        store.insert("quality_events", pd.DataFrame([{
            "run_id": new_run_id("restore"), "dataset": cfg.dataset, "source": cfg.name,
            "check_name": "restore", "status": "fail", "rows_in": len(raw), "rows_out": 0,
            "detail": detail, "event_at": datetime.now(UTC),
        }]))
        return {
            "source": cfg.name, "rows_in_landing": len(raw), "rows_restored": 0,
            "rows_rejected": len(result.rejected), "table": table,
            "blocked_by": blocked, "detail": detail,
        }

    if truncate and _supports_delete(store):
        store.query(f"DELETE FROM {table}")

    frame = result.clean.copy()
    if "is_buyer_maker" in frame.columns:
        frame["is_buyer_maker"] = frame["is_buyer_maker"].astype("boolean")
    rows = store.insert(table, frame, batch_rows=int(cfg.load_cfg.get("batch_rows", 50_000)))

    store.insert("quality_events", pd.DataFrame([{
        "run_id": new_run_id("restore"), "dataset": cfg.dataset, "source": cfg.name,
        "check_name": "restore", "status": "pass", "rows_in": len(raw), "rows_out": rows,
        "detail": f"rebuilt {table} from {len(raw)} rows in the landing zone",
        "event_at": datetime.now(UTC),
    }]))
    return {
        "source": cfg.name, "rows_in_landing": len(raw), "rows_restored": rows,
        "rows_rejected": len(result.rejected), "table": table, "blocked_by": [],
    }


def load_contract_specs(store: Store) -> int:
    """Seed the point-in-time contract specifications.

    Reference data, like the roll map: versioned by the date it took effect, so
    a backtest over March uses March's lot size and not today's.
    """
    import yaml

    from .config import repo_root

    path = repo_root() / "config" / "contract_specs.yml"
    if not path.exists():
        return 0
    raw = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    rows = []
    for exchange, symbols in raw.items():
        for symbol, versions in (symbols or {}).items():
            for spec in versions:
                rows.append({
                    "exchange": exchange, "symbol": symbol,
                    "lot_size": float(spec["lot_size"]),
                    "tick_size": float(spec["tick_size"]),
                    "price_unit": spec.get("price_unit", ""),
                    "quantity_unit": spec.get("quantity_unit", ""),
                    "known_from": pd.Timestamp(spec["known_from"]).date(),
                    "known_to": pd.Timestamp(spec["known_to"]).date(),
                })
    if not rows:
        return 0
    frame = pd.DataFrame(rows)
    if _supports_delete(store):
        store.query("DELETE FROM contract_specs")
    return store.insert("contract_specs", frame)


def _supports_delete(store: Store) -> bool:
    return store.__class__.__name__ == "DuckDBStore"
