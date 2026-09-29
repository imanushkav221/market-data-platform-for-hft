"""Raw landing zone.

Everything that arrives is written to Parquet before anything else happens to it.
The landing zone, not the database, is the source of truth: if a transform is
wrong we replay from here rather than going back to the vendor. Rejected rows are
written next to it with their reason, which is what makes a vendor conversation
short.
"""
from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import pandas as pd

from .config import SourceConfig, repo_root


def _partition_path(root: Path, cfg: SourceConfig, df: pd.DataFrame, when: datetime) -> Path:
    parts = [root, cfg.dataset]
    if "symbol" in (cfg.landing.get("partition_by") or []) and "symbol" in df.columns:
        symbols = df["symbol"].dropna().unique()
        parts.append(str(symbols[0]) if len(symbols) == 1 else "_multi")
    parts.append(when.strftime("%Y-%m-%d"))
    return Path(*[str(p) for p in parts])


def write_raw(cfg: SourceConfig, df: pd.DataFrame, *, when: datetime | None = None) -> Path:
    when = when or datetime.now(UTC)
    out = _partition_path(repo_root() / "data" / "landing", cfg, df, when)
    out.mkdir(parents=True, exist_ok=True)
    path = out / f"{cfg.name}_{when.strftime('%H%M%S%f')}.parquet"
    df.to_parquet(path, index=False)
    return path


def write_quarantine(cfg: SourceConfig, df: pd.DataFrame, *,
                     when: datetime | None = None) -> Path | None:
    if df.empty:
        return None
    when = when or datetime.now(UTC)
    out = repo_root() / "data" / "quarantine" / cfg.dataset / when.strftime("%Y-%m-%d")
    out.mkdir(parents=True, exist_ok=True)
    path = out / f"{cfg.name}_{when.strftime('%H%M%S%f')}.parquet"
    df.to_parquet(path, index=False)
    return path


def apply_retention(dataset: str | None = None, *, days: int = 90,
                    apply: bool = False) -> dict:
    """Age raw files out of the landing zone.

    Two things make this safe to run: it is a dry run unless told otherwise, and
    it never touches quarantine, because the rejected rows are the evidence in a
    vendor conversation and those conversations are slow.

    Retention length is a policy decision, not a technical one. 90 days is a
    placeholder for a conversation with compliance.
    """
    from datetime import datetime, timedelta

    root = repo_root() / "data" / "landing"
    if dataset:
        root = root / dataset
    cutoff = datetime.now(UTC) - timedelta(days=days)

    candidates, freed = [], 0
    for path in sorted(root.rglob("*.parquet")) if root.exists() else []:
        modified = datetime.fromtimestamp(path.stat().st_mtime, tz=UTC)
        if modified < cutoff:
            candidates.append(path)
            freed += path.stat().st_size
            if apply:
                path.unlink()
    return {
        "dataset": dataset or "all",
        "older_than_days": days,
        "files": len(candidates),
        "bytes": freed,
        "applied": apply,
    }


def replay(dataset: str) -> pd.DataFrame:
    """Rebuild a dataset from the landing zone. Backfill is a parameter, not a script."""
    root = repo_root() / "data" / "landing" / dataset
    files = sorted(root.rglob("*.parquet"))
    if not files:
        return pd.DataFrame()
    return pd.concat([pd.read_parquet(f) for f in files], ignore_index=True)
