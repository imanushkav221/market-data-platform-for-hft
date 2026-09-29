"""Event-driven ingestion: react when a file lands, instead of waiting for a clock.

Why this exists. A scheduled pipeline pays for its schedule: a file that arrives
at 06:02 waits until 07:00 to be loaded, and the desk has stale data for 58
minutes that nobody can see. Reacting to arrival removes that wait entirely, and
the latency it replaces it with is measurable, which is the point.

This implementation polls a drop directory, because that runs anywhere with no
dependencies. In production the trigger is the same shape with a different
doorbell: an S3 or GCS object-created event, an inotify watch, or an SFTP server
hook. Everything after the trigger is identical, which is why the watcher only
decides *when*, never *what*.

Three things that look like details and are not:

  1. **Partial writes.** A 400MB vendor upload in progress looks exactly like a
     small complete file. We wait for the size and mtime to stop changing before
     touching it. Loading half a file is worse than loading none.
  2. **Re-delivery.** Vendors resend. A ledger of what has already been processed,
     keyed on content identity rather than filename, makes reprocessing a no-op
     instead of a double load.
  3. **Latency.** Every delivery records the gap between the file's own timestamp
     and the moment its rows were queryable. Without that number, "low latency"
     is a claim rather than a fact.
"""
from __future__ import annotations

import hashlib
import json
import time
from collections.abc import Callable, Iterable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path

import pandas as pd

from .config import SourceConfig, repo_root
from .pipeline import run_source
from .storage import Store

LEDGER = repo_root() / "data" / ".processed.json"


# ---------------------------------------------------------------------------
# Ledger
# ---------------------------------------------------------------------------
MAX_HASH_BYTES = 256 * 1024 * 1024


def _fingerprint(path: Path, max_hash_bytes: int = MAX_HASH_BYTES) -> str:
    """Identify a delivery by what it is, not what it is called.

    Name plus size plus mtime seems sufficient until a vendor resends yesterday's
    file under today's name, which they do, and the load runs twice. So the
    identity is the content: a streamed SHA-256, which costs about a second per
    gigabyte and removes the whole class of problem.

    Above the size threshold that becomes wasteful, so very large deliveries fall
    back to name, size and mtime. The database grain is the second line of
    defence in both cases: QuestDB's DEDUP UPSERT KEYS collapses duplicates on
    the declared grain at commit, and the client library deduplicates on read.
    """
    st = path.stat()
    if st.st_size <= max_hash_bytes:
        digest = hashlib.sha256()
        with path.open("rb") as handle:
            for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                digest.update(chunk)
        return "sha:" + digest.hexdigest()[:16]
    return "stat:" + hashlib.sha256(
        f"{path.name}:{st.st_size}:{int(st.st_mtime)}".encode()
    ).hexdigest()[:16]


def _load_ledger() -> dict:
    if LEDGER.exists():
        try:
            return json.loads(LEDGER.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            return {}          # a corrupt ledger must not stop ingestion
    return {}


def _record(fingerprint: str, entry: dict) -> None:
    ledger = _load_ledger()
    ledger[fingerprint] = entry
    LEDGER.parent.mkdir(parents=True, exist_ok=True)
    LEDGER.write_text(json.dumps(ledger, indent=2, default=str), encoding="utf-8")


def _attempts(fingerprint: str) -> int:
    return int(_load_ledger().get(fingerprint, {}).get("attempts", 0))


def _is_settled(entry: dict, max_attempts: int) -> bool:
    """True when this delivery should not be looked at again.

    Either it loaded, or it has failed enough times that retrying it every pass
    is just noise. A file that keeps failing needs a person, not another attempt.
    """
    if entry.get("status") == "ok":
        return True
    return int(entry.get("attempts", 0)) >= max_attempts


# ---------------------------------------------------------------------------
# Detection
# ---------------------------------------------------------------------------
@dataclass
class Delivery:
    source: str
    path: Path
    fingerprint: str
    file_time: datetime
    detected_at: datetime
    rows_loaded: int = 0
    published: bool = False
    blocked_by: list[str] = field(default_factory=list)
    arrival_latency: float = 0.0     # file timestamp -> we noticed
    processing_latency: float = 0.0  # we noticed -> rows queryable

    @property
    def total_latency(self) -> float:
        return self.arrival_latency + self.processing_latency

    def summary(self) -> str:
        state = "published" if self.published else f"BLOCKED {self.blocked_by}"
        return (
            f"{self.source}: {self.path.name} -> {self.rows_loaded} rows {state} "
            f"(detected {self.arrival_latency:.1f}s after the file was written, "
            f"queryable {self.processing_latency:.1f}s later, "
            f"{self.total_latency:.1f}s end to end)"
        )


def _is_stable(path: Path, settle_seconds: float) -> bool:
    """True once the file has stopped growing.

    The cheapest correct answer. The alternative used by careful vendors is a
    marker file written after the payload, which we would honour instead if one
    existed.
    """
    try:
        first = path.stat()
        time.sleep(settle_seconds)
        second = path.stat()
    except FileNotFoundError:
        return False
    return first.st_size == second.st_size and first.st_mtime == second.st_mtime


def scan(cfg: SourceConfig) -> list[Path]:
    """Files waiting in this source's drop directory that we have not seen."""
    drop = cfg.acquire.get("drop") or {}
    directory = repo_root() / drop.get("dir", f"data/incoming/{cfg.name}")
    directory.mkdir(parents=True, exist_ok=True)
    pattern = drop.get("pattern", "*")
    max_attempts = int(drop.get("max_attempts", 3))
    ledger = _load_ledger()

    waiting = []
    for path in sorted(directory.glob(pattern)):
        if not path.is_file() or path.name.startswith("."):
            continue
        entry = ledger.get(_fingerprint(path))
        if entry and _is_settled(entry, max_attempts):
            continue
        waiting.append(path)
    return waiting


def _log_event(store: Store, cfg: SourceConfig, check: str, status: str, detail: str) -> None:
    store.insert("quality_events", pd.DataFrame([{
        "run_id": "watch", "dataset": cfg.dataset, "source": cfg.name,
        "check_name": check, "status": status, "rows_in": 0, "rows_out": 0,
        "detail": detail, "event_at": datetime.now(UTC),
    }]))


# ---------------------------------------------------------------------------
# Handling one delivery
# ---------------------------------------------------------------------------
def handle(cfg: SourceConfig, path: Path, store: Store, *, mode: str = "live") -> Delivery | None:
    drop = cfg.acquire.get("drop") or {}
    settle = float(drop.get("settle_seconds", 1.0))

    if not _is_stable(path, settle):
        return None  # still being written; we will see it on the next pass

    detected_at = datetime.now(UTC)
    file_time = datetime.fromtimestamp(path.stat().st_mtime, tz=UTC)
    fingerprint = _fingerprint(path)

    max_attempts = int(drop.get("max_attempts", 3))
    attempts = _attempts(fingerprint) + 1

    started = time.monotonic()
    try:
        summary = run_source(cfg, store, local_file=str(path), mode=mode)
    except Exception as exc:
        # An unreadable file is one vendor's bad morning, not an outage. Record
        # it, leave it for another attempt, and carry on with every other source.
        processing = time.monotonic() - started
        _log_event(
            store, cfg, "acquisition", "fail",
            f"could not read {path.name} (attempt {attempts}/{max_attempts}): {exc}",
        )
        _record(fingerprint, {
            "status": "failed", "attempts": attempts, "source": cfg.name,
            "file": str(path), "last_error": str(exc),
            "last_attempt_at": datetime.now(UTC),
        })
        return Delivery(
            source=cfg.name, path=path, fingerprint=fingerprint, file_time=file_time,
            detected_at=detected_at, blocked_by=["acquisition_error"],
            arrival_latency=max((detected_at - file_time).total_seconds(), 0.0),
            processing_latency=processing,
        )
    processing = time.monotonic() - started

    delivery = Delivery(
        source=cfg.name,
        path=path,
        fingerprint=fingerprint,
        file_time=file_time,
        detected_at=detected_at,
        rows_loaded=summary["rows_loaded"],
        published=summary["published"],
        blocked_by=summary["blocked_by"],
        arrival_latency=max((detected_at - file_time).total_seconds(), 0.0),
        processing_latency=processing,
    )

    # The latency goes into the same table as every other quality event, so it is
    # queryable next to the checks rather than living in a log nobody reads.
    store.insert(
        "quality_events",
        pd.DataFrame(
            [
                {
                    "run_id": summary["run_id"],
                    "dataset": cfg.dataset,
                    "source": cfg.name,
                    "check_name": "delivery_latency",
                    "status": "pass" if delivery.total_latency < float(
                        drop.get("latency_sla_seconds", 60)
                    ) else "warn",
                    "rows_in": summary["rows_acquired"],
                    "rows_out": summary["rows_loaded"],
                    "detail": (
                        f"{delivery.total_latency:.2f}s from file write to queryable "
                        f"({delivery.arrival_latency:.2f}s detect + "
                        f"{delivery.processing_latency:.2f}s process)"
                    ),
                    "event_at": datetime.now(UTC),
                }
            ]
        ),
    )

    if summary["published"]:
        _record(fingerprint, {
            "status": "ok",
            "source": cfg.name,
            "file": str(path),
            "rows": summary["rows_loaded"],
            "run_id": summary["run_id"],
            "processed_at": datetime.now(UTC),
            "latency_seconds": round(delivery.total_latency, 3),
        })
    else:
        # A blocked delivery gets another go, up to the retry budget, so a
        # corrected re-delivery works without anyone editing a ledger by hand.
        _record(fingerprint, {
            "status": "failed",
            "attempts": attempts,
            "source": cfg.name,
            "file": str(path),
            "last_error": f"blocked by {summary['blocked_by']}",
            "last_attempt_at": datetime.now(UTC),
        })
    return delivery


# ---------------------------------------------------------------------------
# The loop
# ---------------------------------------------------------------------------
def watch(
    sources: Iterable[SourceConfig],
    store: Store,
    *,
    poll_seconds: float = 2.0,
    once: bool = False,
    on_delivery: Callable[[Delivery], None] | None = None,
    max_iterations: int | None = None,
    mode: str = "live",
) -> list[Delivery]:
    """Watch drop directories and ingest what lands.

    `once` does a single pass, which is what the tests and a cron-driven
    deployment use. Without it this runs until interrupted, which is what a
    service does.
    """
    store.ensure_schema()
    handled: list[Delivery] = []
    iterations = 0

    while True:
        for cfg in sources:
            for path in scan(cfg):
                delivery = handle(cfg, path, store, mode=mode)
                if delivery is None:
                    continue
                handled.append(delivery)
                if on_delivery:
                    on_delivery(delivery)

        iterations += 1
        if once or (max_iterations is not None and iterations >= max_iterations):
            return handled
        time.sleep(poll_seconds)
