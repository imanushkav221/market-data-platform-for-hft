"""Drop a vendor-shaped file into the watched directory.

For the live demo: run `make watch` in one terminal and `make drop` in another.
The file appears, the watcher notices it, validates it against the contract and
loads it, and prints how long the whole thing took.

    python scripts/drop_sample.py                 # a clean file
    python scripts/drop_sample.py --broken        # one the contract will catch
    python scripts/drop_sample.py --stale         # one the freshness check blocks
"""
from __future__ import annotations

import argparse
import sys
from datetime import date, timedelta
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from mdp.config import SourceConfig, repo_root  # noqa: E402


def build(days: int, end: date, *, broken: bool) -> pd.DataFrame:
    """The exchange's own column names, not ours. Mapping happens in config."""
    rng = np.random.default_rng(11)
    rows, px = [], 6200.0
    for i in range(days):
        d = end - timedelta(days=i)
        px *= float(np.exp(rng.normal(0, 0.021)))
        for k, expiry in enumerate((end + timedelta(days=20), end + timedelta(days=50))):
            o = px * (1 + 0.0015 * k)
            c = o * float(np.exp(rng.normal(0, 0.004)))
            rows.append(
                {
                    "Symbol": "CRUDEOIL",
                    "ExpiryDate": expiry.isoformat(),
                    "Date": d.isoformat(),
                    "Open": round(o, 2),
                    "High": round(max(o, c) * 1.002, 2),
                    "Low": round(min(o, c) * 0.998, 2),
                    "Close": round(c, 2),
                    "SettlePrice": round(c, 2),
                    "Volume": float(rng.integers(500, 40000)),
                    "OpenInterest": float(rng.integers(1000, 90000)),
                }
            )
    df = pd.DataFrame(rows)
    if broken:
        # high below low: impossible, and exactly the kind of thing that reaches
        # a researcher unnoticed without a cross-field rule
        df.loc[0, "High"] = df.loc[0, "Low"] - 5
    return df


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", default="mcx_bhavcopy")
    parser.add_argument("--days", type=int, default=8)
    parser.add_argument("--broken", action="store_true", help="include an impossible bar")
    parser.add_argument("--stale", action="store_true", help="old dates, blocks on freshness")
    args = parser.parse_args()

    cfg = SourceConfig.by_name(args.source)
    drop = cfg.acquire.get("drop") or {}
    directory = repo_root() / drop.get("dir", f"data/incoming/{cfg.name}")
    directory.mkdir(parents=True, exist_ok=True)

    end = date.today() - (timedelta(days=30) if args.stale else timedelta(0))
    df = build(args.days, end, broken=args.broken)

    stamp = pd.Timestamp.now().strftime("%Y%m%d_%H%M%S")
    path = directory / f"BHAVCOPY_{stamp}.csv"

    # Write to a temporary name and rename into place, which is what a careful
    # vendor does and what stops the watcher seeing a half-written file.
    staging = path.with_suffix(".csv.part")
    df.to_csv(staging, index=False)
    staging.rename(path)

    print(f"dropped {path.relative_to(repo_root())} ({len(df)} rows)")
    if args.broken:
        print("  contains one impossible bar: the contract gate should quarantine it")
    if args.stale:
        print("  dated 30 days ago: the freshness check should block the load")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
