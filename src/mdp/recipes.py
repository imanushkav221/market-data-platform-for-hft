"""Model recipes and scenario runs.

Two ideas, both borrowed from how governed datasets and model runs are handled
on a research platform that has to survive audit:

**A recipe is declarative.** Inputs, features, target, validation and outputs
live in YAML with a version on them. The code reads the recipe; it does not
contain the model. So a run records exactly what produced it, two runs months
apart are comparable, and adding a variant is a block of config rather than a
branch in a script that nobody remembers writing.

**Scenarios are isolated.** Each variation gets its own run id, its own
parameters, its own artifacts, and its own row in the registry. Nothing
overwrites anything. That is what lets you ask "is the longer horizon better?"
without destroying the run that answered the previous question, and it is the
same mechanism you would use to shadow a new model version against the live one
before promoting it.

The input fingerprint matters as much as the parameters: rows, date range and
contract version of the data the run actually saw. Parameters alone do not make
a run reproducible, because the data moves underneath them.
"""
from __future__ import annotations

import copy
import hashlib
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import pandas as pd
import yaml

from .config import repo_root
from .model import make_features, walk_forward
from .runs import ModelRun
from .serve import MarketData
from .storage import Store


@dataclass
class Recipe:
    name: str
    version: str
    owner: str
    description: str
    input: dict[str, Any]
    target: dict[str, Any]
    features: list[str]
    estimator: dict[str, Any]
    validation: dict[str, Any]
    scenarios: list[dict[str, Any]] = field(default_factory=list)
    path: Path | None = None

    @classmethod
    def load(cls, path: str | Path) -> Recipe:
        p = Path(path)
        if not p.is_absolute():
            p = repo_root() / p
        raw = yaml.safe_load(p.read_text(encoding="utf-8"))
        meta = raw.get("recipe", {})
        return cls(
            name=meta.get("name", p.stem),
            version=str(meta.get("version", "0")),
            owner=meta.get("owner", "unknown"),
            description=meta.get("description", "").strip(),
            input=raw.get("input", {}),
            target=raw.get("target", {}),
            features=list(raw.get("features", [])),
            estimator=raw.get("estimator", {}),
            validation=raw.get("validation", {}),
            scenarios=raw.get("scenarios", []) or [{"name": "baseline"}],
            path=p,
        )

    @classmethod
    def by_name(cls, name: str) -> Recipe:
        return cls.load(repo_root() / "config" / "models" / f"{name}.yml")

    def as_dict(self) -> dict[str, Any]:
        return {
            "input": self.input, "target": self.target, "features": self.features,
            "estimator": self.estimator, "validation": self.validation,
        }

    def resolve(self, scenario: dict[str, Any]) -> dict[str, Any]:
        """Apply a scenario's overrides to the base recipe.

        Merged one level deep, so a scenario can change `alpha` without having to
        restate the whole estimator block.
        """
        merged = copy.deepcopy(self.as_dict())
        for key, value in (scenario.get("overrides") or {}).items():
            if isinstance(value, dict) and isinstance(merged.get(key), dict):
                merged[key].update(value)
            else:
                merged[key] = value
        return merged


def fingerprint(prices: pd.DataFrame, recipe: Recipe) -> dict[str, Any]:
    """What data this run actually saw.

    Parameters alone do not make a run reproducible: the data moves underneath
    them. Rows, the date range, and a hash of the series pin down the input, so a
    number that changed can be traced to a changed input rather than argued about.
    """
    if prices.empty:
        return {"rows": 0}
    closes = prices["adj_close"].round(6).astype(str).str.cat(sep="|")
    return {
        "rows": int(len(prices)),
        "first_date": str(pd.Timestamp(prices["trade_date"].min()).date()),
        "last_date": str(pd.Timestamp(prices["trade_date"].max()).date()),
        "series_sha": hashlib.sha256(closes.encode()).hexdigest()[:12],
        "recipe_version": recipe.version,
        "adjust": recipe.input.get("adjust"),
    }


def run_scenario(
    recipe: Recipe,
    scenario: dict[str, Any],
    prices: pd.DataFrame,
    store: Store,
    *,
    data_fingerprint: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """One isolated run: its own id, parameters, artifacts and registry row."""
    settings = recipe.resolve(scenario)
    horizon = int(settings["target"].get("horizon_days", 1))
    validation = settings["validation"]

    feats = make_features(prices, horizon=horizon, features=settings["features"])
    run = ModelRun(
        model=f"{recipe.name}:{scenario['name']}",
        params={
            "recipe": recipe.name,
            "recipe_version": recipe.version,
            "scenario": scenario["name"],
            "horizon_days": horizon,
            "features": settings["features"],
            "estimator": settings["estimator"],
            "validation": validation,
            "input": settings["input"],
            "data": data_fingerprint or {},
        },
        data_through=prices["trade_date"].max(),
    )

    result = walk_forward(
        feats,
        min_train=int(validation.get("min_train", 250)),
        test_size=int(validation.get("test_size", 21)),
        model=settings["estimator"].get("kind", "ridge"),
        alpha=float(settings["estimator"].get("alpha", 1.0)),
        # The horizon has to reach walk_forward, not just make_features, or the
        # purge gap cannot size itself and the longer-horizon scenarios in this
        # recipe get compared against the shorter ones while leaking further.
        horizon=horizon,
    )
    metrics = result.metrics()
    run.finish(metrics)

    if metrics:
        store.insert("model_runs", run.to_frame())
        run.save_artifacts(folds=result.frame(), predictions=result.predictions)

    return {
        "scenario": scenario["name"],
        "description": scenario.get("description", ""),
        "run_id": run.run_id,
        "horizon_days": horizon,
        "n_features": len(settings["features"]),
        "alpha": settings["estimator"].get("alpha"),
        "min_train": validation.get("min_train"),
        **metrics,
        "verdict": result.verdict() if metrics else "not enough history",
        "_result": result,
        "_run": run,
    }


def run_recipe(
    recipe: Recipe,
    store: Store,
    *,
    consumer: str = "research",
    scenarios: list[str] | None = None,
) -> pd.DataFrame:
    """Run every scenario in a recipe against the same input, and compare them.

    Same input for every scenario on purpose: if the data moved between runs, the
    comparison measures the data rather than the change you were testing.
    """
    md = MarketData(store=store, consumer=consumer)
    prices = md.get_continuous(
        recipe.input.get("symbol", "CRUDEOIL"),
        method=recipe.input.get("roll_method", "volume"),
        adjust=recipe.input.get("adjust", "ratio"),
        as_of=recipe.input.get("as_of"),
        exchange=recipe.input.get("exchange", "MCX"),
    )
    if prices.empty:
        raise RuntimeError(
            f"no data for {recipe.input.get('symbol')}: ingest the source first"
        )

    data = fingerprint(prices, recipe)
    wanted = [s for s in recipe.scenarios if not scenarios or s["name"] in scenarios]
    rows = [
        run_scenario(recipe, scenario, prices, store, data_fingerprint=data)
        for scenario in wanted
    ]

    frame = pd.DataFrame(rows)
    frame.attrs["data"] = data
    frame.attrs["recipe"] = recipe
    return frame


def comparison_table(results: pd.DataFrame) -> pd.DataFrame:
    """The columns worth putting on a slide, ranked by skill against the baseline."""
    columns = [
        "scenario", "horizon_days", "n_features", "alpha", "folds",
        "skill_vs_baseline", "model_dir_acc", "folds_beating_baseline", "run_id",
    ]
    present = [c for c in columns if c in results.columns]
    out = results[present].copy()
    if "skill_vs_baseline" in out:
        out = out.sort_values("skill_vs_baseline", ascending=False)
    return out.reset_index(drop=True)
