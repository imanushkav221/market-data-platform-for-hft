"""Loading source configs and dataset contracts.

The point of this module: a new dataset is a YAML file plus a JSON contract.
Nothing in src/ changes when a source is added.
"""
from __future__ import annotations

import json
import os
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml


def repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


_SECRET_PATTERN = re.compile(r"\$\{env:([A-Z0-9_]+)(?::([^}]*))?\}")
_SECRET_KEYS = ("key", "token", "secret", "password", "passphrase", "credential")


def resolve_secrets(value):
    """Replace ${env:VAR} with the environment, recursively.

    Credentials never live in a config file that git can see. They are named
    here, resolved at load, and the name is what gets committed. A missing
    variable fails loudly at startup rather than as a confusing 401 at 6am.
    """
    if isinstance(value, str):
        def swap(match: re.Match) -> str:
            name, default = match.group(1), match.group(2)
            if name in os.environ:
                return os.environ[name]
            if default is not None:
                return default
            raise KeyError(
                f"config needs the environment variable {name}, which is not set"
            )
        return _SECRET_PATTERN.sub(swap, value)
    if isinstance(value, dict):
        return {k: resolve_secrets(v) for k, v in value.items()}
    if isinstance(value, list):
        return [resolve_secrets(v) for v in value]
    return value


def redact(config: dict) -> dict:
    """A copy safe to print, log or paste into a ticket."""
    out: dict[Any, Any] = {}
    for key, value in config.items():
        if isinstance(value, dict):
            out[key] = redact(value)
        elif any(hint in key.lower() for hint in _SECRET_KEYS) and value:
            out[key] = "***redacted***"
        else:
            out[key] = value
    return out


@dataclass(frozen=True)
class Contract:
    """The schema and rules a dataset promises to satisfy before it is loaded."""

    dataset: str
    schema_version: str
    grain: list[str]
    event_time_column: str
    columns: list[dict[str, Any]]
    rules: dict[str, Any] = field(default_factory=dict)
    lineage: dict[str, Any] = field(default_factory=dict)
    description: str = ""

    @property
    def column_names(self) -> list[str]:
        return [c["name"] for c in self.columns]

    @property
    def required_columns(self) -> list[str]:
        return [c["name"] for c in self.columns if c.get("required")]

    def column(self, name: str) -> dict[str, Any]:
        for c in self.columns:
            if c["name"] == name:
                return c
        raise KeyError(f"{name} is not in the {self.dataset} contract")

    @classmethod
    def load(cls, path: str | Path) -> Contract:
        p = Path(path)
        if not p.is_absolute():
            p = repo_root() / p
        raw = json.loads(p.read_text(encoding="utf-8"))
        return cls(
            dataset=raw["dataset"],
            schema_version=raw["schema_version"],
            grain=raw["grain"],
            event_time_column=raw["event_time_column"],
            columns=raw["columns"],
            rules=raw.get("rules", {}),
            lineage=raw.get("lineage", {}),
            description=raw.get("description", ""),
        )


@dataclass(frozen=True)
class SourceConfig:
    """One ingestible source. Everything the pipeline needs to know about it."""

    name: str
    dataset: str
    frequency: str
    acquire: dict[str, Any]
    landing: dict[str, Any]
    load_cfg: dict[str, Any]
    quality: dict[str, Any]
    contract: Contract

    @classmethod
    def load(cls, path: str | Path) -> SourceConfig:
        p = Path(path)
        if not p.is_absolute():
            p = repo_root() / p
        raw = resolve_secrets(yaml.safe_load(p.read_text(encoding="utf-8")))
        return cls(
            name=raw["name"],
            dataset=raw["dataset"],
            frequency=raw.get("frequency", "daily"),
            acquire=raw.get("acquire", {}),
            landing=raw.get("landing", {}),
            load_cfg=raw.get("load", {}),
            quality=raw.get("quality", {}),
            contract=Contract.load(raw["contract"]),
        )

    @classmethod
    def by_name(cls, name: str) -> SourceConfig:
        return cls.load(repo_root() / "config" / "sources" / f"{name}.yml")

    @classmethod
    def all(cls) -> list[SourceConfig]:
        d = repo_root() / "config" / "sources"
        return [cls.load(p) for p in sorted(d.glob("*.yml"))]


def vendor_quality_policy() -> dict[str, Any]:
    p = repo_root() / "config" / "vendor_quality.yml"
    return yaml.safe_load(p.read_text(encoding="utf-8"))


# The connection settings for the primary production store, declared in the same
# ${env:VAR} form every source config uses rather than read out of os.environ at
# the point of use.
#
# Two reasons it is spelled this way. The first is that resolve_secrets is the
# one place that decides what a missing variable means, and a second convention
# for the same job is how two halves of a deployment end up disagreeing about
# which host they are talking to. The second is that these defaults are then
# visible and greppable: QDB_PORT's default is QuestDB's own HTTP port, which is
# 9000, and that is a fact somebody will need when the container maps it
# elsewhere, so it belongs in the source rather than in an argument default.
#
# Note that there is no QDB_USER/QDB_PASSWORD pair here. QuestDB's open-source
# build has no role-based access control at all, so a credential in this dict
# would be decorative, and bench/RESULTS.md records that gap as one of the real
# costs of this choice. If this ever runs against QuestDB Enterprise, the
# credentials go here as ${env:...} like everything else.
_QUESTDB_SETTINGS = {
    "host": "${env:QDB_HOST:127.0.0.1}",
    "port": "${env:QDB_PORT:9000}",
}


def questdb_settings() -> dict[str, Any]:
    """Host and port for the QuestDB HTTP endpoint, resolved from the environment."""
    resolved = resolve_secrets(dict(_QUESTDB_SETTINGS))
    return {"host": resolved["host"], "port": int(resolved["port"])}
