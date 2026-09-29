"""The no-Docker monitoring config is generated, and this proves it stayed that way.

`monitoring/` is written for the compose stack and addresses its neighbours by
service name. Running the same three binaries directly on a laptop needs those
names replaced, and the obvious way to get there -- a second, hand-edited copy of
every file -- is what this repository actually grew, in a folder beside it. The
two drifted immediately and silently: a corrected Grafana panel sat in
`monitoring/` for hours while the running Grafana served the old one, and nothing
anywhere reported a problem, because nothing was comparing them.

`scripts/monitoring_local.py` replaces the copy with a renderer, so every
behavioural decision has one definition. These tests are the half that makes
that claim checkable rather than aspirational: the first asserts the rendered
output differs from its source on no line that does not carry an address, and
the second asserts no compose-only name escapes the substitution map. Together
they mean a threshold, an interval, a routing rule or a panel cannot be changed
for one runtime and not the other, because there is no second place to change it.
"""

from __future__ import annotations

import importlib.util
import re
from pathlib import Path

import pytest


def repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def _load_renderer():
    path = repo_root() / "scripts" / "monitoring_local.py"
    spec = importlib.util.spec_from_file_location("monitoring_local", path)
    assert spec and spec.loader, f"cannot load {path}"
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


ml = _load_renderer()


def test_rendering_changes_only_lines_that_carry_an_address() -> None:
    """Every difference between source and rendered output is an address.

    This is the whole guarantee. If a line changed and carries none of the
    substitution keys, something behavioural has been forked between the two
    runtimes, which is the failure the renderer exists to make impossible.
    """
    subs = ml.substitutions(repo_root() / "monitoring" / "grafana" / "dashboards")

    for rel in ml.RENDERED:
        source = (ml.MONITORING / rel).read_text(encoding="utf-8")
        rendered = ml.render(source, subs)

        src_lines = source.splitlines()
        out_lines = rendered.splitlines()
        assert len(src_lines) == len(out_lines), (
            f"{rel}: rendering changed the line count "
            f"({len(src_lines)} -> {len(out_lines)}). Substitution replaces "
            f"addresses in place; it never adds or removes a line."
        )

        for lineno, (before, after) in enumerate(zip(src_lines, out_lines), 1):
            if before == after:
                continue
            assert any(key in before for key in subs), (
                f"{rel}:{lineno} changed but carries no address from the "
                f"substitution map, so the two runtimes now disagree about "
                f"behaviour rather than about where things live:\n"
                f"  compose: {before.strip()}\n"
                f"  local:   {after.strip()}"
            )


def test_no_compose_only_name_survives_the_render() -> None:
    """Nothing addressable by service name is left in the rendered config.

    A name that resolves only inside the compose network is a config that starts
    and then fails at the first scrape, which looks like a broken exporter rather
    than a broken config. Catching it here costs nothing; catching it at 23:55
    costs the session.
    """
    subs = ml.substitutions(repo_root() / "monitoring" / "grafana" / "dashboards")
    compose_only = ["host.docker.internal", "/etc/prometheus", "/var/lib/grafana"]
    compose_only += [f"{svc}:{port}" for svc, port in
                     (("prometheus", 9090), ("alertmanager", 9093),
                      ("grafana", 3000), ("questdb", 9000), ("mdp", 9108))]

    for rel in ml.RENDERED:
        rendered = ml.render((ml.MONITORING / rel).read_text(encoding="utf-8"), subs)
        for name in compose_only:
            # A commented-out line is documentation, not configuration.
            live = "\n".join(l for l in rendered.splitlines()
                             if not l.lstrip().startswith("#"))
            assert name not in live, (
                f"{rel}: '{name}' survived rendering. Add it to "
                f"ADDRESSES in scripts/monitoring_local.py, or the local stack "
                f"will start and then quietly fail to reach it."
            )


def test_every_substitution_is_an_address_and_not_a_behaviour() -> None:
    """The map may only move things, never retune them.

    The renderer's usefulness rests entirely on the map being addresses. A
    threshold or an interval smuggled in here would be invisible to the test
    above -- it would be an address-carrying line by definition -- so the shape
    of each key is checked directly.
    """
    subs = ml.substitutions(repo_root() / "monitoring" / "grafana" / "dashboards")
    address = re.compile(
        r"""^(
              [A-Za-z0-9_.-]+(:\d+)?          # a host, optionally with a port
            | (/|[A-Za-z]:/).*                # or an absolute path, posix or windows
                                              # (which may contain spaces)
            )$""",
        re.VERBOSE,
    )
    for key, value in subs.items():
        assert address.match(key), f"substitution key is not an address: {key!r}"
        assert address.match(value), f"substitution value is not an address: {value!r}"


def test_the_rendered_rule_files_path_points_at_the_repository() -> None:
    """The alert rules are shared, not copied.

    Thresholds are the most valuable thing in `monitoring/` and the most
    tempting to tweak in a local copy. The local Prometheus reads them from the
    repository directly, so there is no local copy to tweak.
    """
    subs = ml.substitutions(repo_root() / "monitoring" / "grafana" / "dashboards")
    rendered = ml.render((ml.MONITORING / "prometheus.yml").read_text(encoding="utf-8"), subs)
    expected = ml._posix(ml.MONITORING / "rules" / "*.yml")
    assert expected in rendered, (
        "the local Prometheus must glob the repository's own rules directory; "
        f"expected {expected!r} in the rendered config"
    )


def test_the_drift_check_would_actually_catch_a_behavioural_substitution() -> None:
    """The guard above is only worth having if it can fail.

    A test that asserts "the generated file matches the generator" passes
    trivially for any generator, including one that has been quietly taught to
    change a threshold. So this feeds the checker a substitution that retunes
    the scrape interval and asserts it is rejected -- which is the scenario the
    whole arrangement exists to prevent, and the only evidence that the check
    is load-bearing.
    """
    subs = ml.substitutions(repo_root() / "monitoring" / "grafana" / "dashboards")
    sabotaged = dict(subs, **{"scrape_interval: 15s": "scrape_interval: 30s"})

    source = (ml.MONITORING / "prometheus.yml").read_text(encoding="utf-8")
    rendered = ml.render(source, sabotaged)

    offending = [
        (before, after)
        for before, after in zip(source.splitlines(), rendered.splitlines())
        if before != after and not any(key in before for key in subs)
    ]
    assert offending, (
        "a substitution that changes the scrape interval slipped past the same "
        "comparison the real test performs, so that test is not checking anything"
    )
    assert "scrape_interval" in offending[0][0]
