"""Run registry: what ran, on what, with which code, and what came out.

Every pipeline load and every model run gets an id and a row. This is what makes
"why did this number change?" a two-minute question instead of an afternoon, and
it is the same discipline a published model release needs.
"""
from __future__ import annotations

import json
import subprocess
import uuid
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pandas as pd

from .config import repo_root


def new_run_id(prefix: str = "run") -> str:
    return f"{prefix}-{datetime.now(UTC):%Y%m%dT%H%M%S}-{uuid.uuid4().hex[:6]}"


def code_version() -> str:
    """Git sha when available, so a run points at the code that produced it."""
    try:
        sha = subprocess.run(
            ["git", "rev-parse", "--short", "HEAD"],
            cwd=repo_root(), capture_output=True, text=True, timeout=5,
        )
        if sha.returncode == 0:
            dirty = subprocess.run(
                ["git", "status", "--porcelain"],
                cwd=repo_root(), capture_output=True, text=True, timeout=5,
            ).stdout.strip()
            return sha.stdout.strip() + ("-dirty" if dirty else "")
    except Exception:
        pass
    return "unversioned"


@dataclass
class ModelRun:
    model: str
    params: dict[str, Any]
    data_through: Any
    run_id: str = field(default_factory=lambda: new_run_id("model"))
    code: str = field(default_factory=code_version)
    started_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    finished_at: datetime | None = None
    metrics: dict[str, float] = field(default_factory=dict)

    def finish(self, metrics: dict[str, float]) -> ModelRun:
        self.metrics = metrics
        self.finished_at = datetime.now(UTC)
        return self

    def to_frame(self) -> pd.DataFrame:
        return pd.DataFrame(
            [
                {
                    "run_id": self.run_id,
                    "model": self.model,
                    "params": json.dumps(self.params, sort_keys=True, default=str),
                    "code_version": self.code,
                    "data_through": pd.Timestamp(self.data_through).date(),
                    "metrics": json.dumps(self.metrics, sort_keys=True),
                    "started_at": self.started_at,
                    "finished_at": self.finished_at or datetime.now(UTC),
                }
            ]
        )

    def save_artifacts(self, **frames: pd.DataFrame) -> Path:
        """Predictions and fold results land beside the run, not in a notebook."""
        out = repo_root() / "data" / "runs" / self.run_id
        out.mkdir(parents=True, exist_ok=True)
        (out / "run.json").write_text(
            json.dumps(
                {
                    "run_id": self.run_id,
                    "model": self.model,
                    "params": self.params,
                    "code_version": self.code,
                    "data_through": str(self.data_through),
                    "metrics": self.metrics,
                    "started_at": self.started_at.isoformat(),
                },
                indent=2,
                default=str,
            ),
            encoding="utf-8",
        )
        for name, frame in frames.items():
            if frame is not None and not frame.empty:
                frame.to_parquet(out / f"{name}.parquet", index=False)
        return out
