"""Who may read what, and a record of who did.

Market data arrives with a licence. It usually says which people and which
purposes may use it, and sometimes that is the difference between a renewal and
a lawyer's letter. "Everyone internal can read everything" is a breach waiting
for an auditor to find it.

The serving layer is the only place this can be enforced, because it is the one
door every consumer goes through. Two deliberate decisions:

  1. **Deny is logged as loudly as allow.** The interesting audit question is
     usually who tried, not who succeeded.
  2. **The policy is data, not code.** A licence change is a config change and a
     review, not a deploy.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from functools import lru_cache

import pandas as pd
import yaml

from .config import repo_root
from .storage import Store


class NotEntitled(PermissionError):
    """Raised when a consumer asks for a dataset its licence does not cover."""


@dataclass(frozen=True)
class Policy:
    default_consumer: str
    datasets: dict
    consumers: dict

    def licence(self, dataset: str) -> str:
        return (self.datasets.get(dataset) or {}).get("licence", "unspecified")

    def allows(self, consumer: str, dataset: str) -> tuple[bool, str]:
        if (self.consumers.get(consumer) or {}).get("allow_nothing"):
            return False, f"{consumer} may not read any dataset"
        rule = self.datasets.get(dataset)
        if rule is None:
            # Unknown datasets are denied, not allowed. A new dataset that nobody
            # has thought about the licence for is exactly the one to stop.
            return False, f"no entitlement rule for {dataset}"
        allowed = rule.get("allow", [])
        if consumer in allowed:
            return True, rule.get("licence", "")
        return False, (
            f"{consumer} is not on the allow list for {dataset} "
            f"({rule.get('licence', 'licence unspecified')})"
        )


@lru_cache(maxsize=1)
def load_policy() -> Policy:
    path = repo_root() / "config" / "entitlements.yml"
    raw = yaml.safe_load(path.read_text(encoding="utf-8"))
    return Policy(
        default_consumer=raw.get("default_consumer", "research"),
        datasets=raw.get("datasets", {}),
        consumers=raw.get("consumers", {}),
    )


def check(consumer: str, dataset: str, *, action: str = "read",
          store: Store | None = None, enforce: bool = True) -> bool:
    policy = load_policy()
    allowed, detail = policy.allows(consumer, dataset)
    if store is not None:
        _audit(store, consumer, dataset, action, allowed, detail)
    if not allowed and enforce:
        raise NotEntitled(detail)
    return allowed


def _audit(store: Store, consumer: str, dataset: str, action: str,
           allowed: bool, detail: str) -> None:
    """Never let the audit trail break the read it is auditing."""
    try:
        store.insert(
            "access_log",
            pd.DataFrame([{
                "event_at": datetime.now(UTC),
                "consumer": consumer,
                "dataset": dataset,
                "action": action,
                "allowed": bool(allowed),
                "detail": detail,
            }]),
        )
    except Exception:  # noqa: BLE001
        pass
