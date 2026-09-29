"""Alerting.

A check that fails into a log file is not an alert. Somebody has to be told, and
the message has to contain enough for them to act without opening the code.

Deliberately small: a log sink that always works, and a webhook sink for wherever
the team actually reads things. The interface is what matters, because the
destination changes and the decision about *what is worth waking someone for*
does not.

The rule encoded here: page on data that is wrong or missing, notify on data that
is merely imperfect, and never alert on a weekend.
"""
from __future__ import annotations

import json
import logging
import os
from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import IntEnum
from typing import Protocol

log = logging.getLogger("mdp.alerts")


class Severity(IntEnum):
    INFO = 10       # worth recording, nobody is told
    WARN = 20       # in the morning summary
    PAGE = 30       # somebody is told now


@dataclass
class Alert:
    severity: Severity
    title: str
    detail: str
    source: str = ""
    # Alertmanager groups, inhibits and silences on this, so it is an identifier
    # rather than a description. Deriving it from the human-readable title works
    # until somebody improves the wording and silently splits an alert group in
    # two, so anything the pipeline raises on purpose names itself.
    alertname: str = ""
    context: dict = field(default_factory=dict)
    at: datetime = field(default_factory=lambda: datetime.now(UTC))

    def format(self) -> str:
        bits = [f"[{self.severity.name}] {self.title}"]
        if self.source:
            bits.append(f"source={self.source}")
        bits.append(self.detail)
        if self.context:
            bits.append(json.dumps(self.context, default=str, sort_keys=True))
        return " | ".join(bits)


class Sink(Protocol):
    def send(self, alert: Alert) -> None: ...


class LogSink:
    """Always available. In production this is what the log shipper picks up."""

    def send(self, alert: Alert) -> None:
        level = {
            Severity.INFO: logging.INFO,
            Severity.WARN: logging.WARNING,
            Severity.PAGE: logging.ERROR,
        }[alert.severity]
        # Emit once. If the process has configured logging (a service, or a
        # Prefect run) the handler owns the output; if it has not (someone
        # running the CLI by hand) fall back to stdout. Alerts that appear twice
        # get trusted half as much.
        if log.handlers or logging.getLogger().handlers:
            log.log(level, alert.format())
        else:
            print(alert.format())


class WebhookSink:
    """Slack, Teams, PagerDuty: they all take a POST.

    Never fails the pipeline. An alerting system that can take down the thing it
    monitors is worse than no alerting system.
    """

    def __init__(self, url: str | None = None, timeout: float = 5.0):
        self.url = url or os.getenv("MDP_ALERT_WEBHOOK", "")
        self.timeout = timeout

    def send(self, alert: Alert) -> None:
        if not self.url:
            return
        try:
            import requests

            requests.post(
                self.url,
                json={"text": alert.format(), "severity": alert.severity.name,
                      "source": alert.source, "context": alert.context},
                timeout=self.timeout,
            )
        except Exception as exc:  # noqa: BLE001
            log.warning("alert webhook failed, continuing: %s", exc)


class AlertmanagerSink:
    """Push pipeline events into the same place rule-based alerts land.

    There are two honest sources of alerts in a data platform and they need
    different machinery. Prometheus rules are right for anything that is a
    *trend or a threshold over time*: a vendor score drifting down, a cursor
    that stopped moving, a file that is now overdue. They are wrong for a
    *discrete event the pipeline observed and the metrics cannot reconstruct*:
    this load was blocked because column `settle` went missing, these 412 rows
    were quarantined for failing a cross-field rule. Exporting that as a gauge
    and re-deriving it in PromQL would be inventing a worse copy of something
    the pipeline already knows.

    So both paths exist and both end at Alertmanager, which means one place to
    silence, one grouping policy, one on-call rotation. The alternative, a
    webhook straight to Slack, works right up until somebody needs to mute a
    known-bad vendor for an afternoon and finds there is nothing to mute.

    The `ttl` is the awkward part and worth being explicit about. Alertmanager
    expires a pushed alert once `endsAt` passes, so a short TTL makes a real
    problem quietly resolve itself while it is still a real problem, and a long
    one leaves a stale alert firing after somebody fixed it. Two hours is chosen
    to comfortably outlive the pipeline's own retry-and-rerun cycle: if the
    condition is still true, the next run pushes again and extends it.
    """

    def __init__(self, url: str | None = None, timeout: float = 5.0,
                 ttl_seconds: int = 7200):
        self.url = (url or os.getenv("MDP_ALERTMANAGER_URL") or "").rstrip("/")
        self.timeout = timeout
        self.ttl_seconds = ttl_seconds

    def _payload(self, alert: Alert) -> list[dict]:
        from datetime import timedelta

        labels = {
            "alertname": alert.alertname or _alertname(alert.title),
            "severity": "page" if alert.severity >= Severity.PAGE else "warn",
            "team": "data-platform",
            "platform": "mdp",
            "origin": "pipeline",     # distinguishes these from rule-based alerts
        }
        if alert.source:
            labels["source"] = alert.source
        # Context becomes labels only where it is low cardinality. Putting a run
        # id or a row count in a label is how a metrics store falls over.
        for key in ("trading_day", "dataset"):
            if key in alert.context:
                labels[key] = str(alert.context[key])
        return [{
            "labels": labels,
            "annotations": {
                "summary": alert.title,
                "description": alert.detail,
                "context": json.dumps(alert.context, default=str, sort_keys=True),
            },
            "startsAt": alert.at.isoformat(),
            "endsAt": (alert.at + timedelta(seconds=self.ttl_seconds)).isoformat(),
        }]

    def send(self, alert: Alert) -> None:
        if not self.url:
            return
        try:
            import requests

            response = requests.post(
                f"{self.url}/api/v2/alerts",
                json=self._payload(alert),
                timeout=self.timeout,
            )
            response.raise_for_status()
        except Exception as exc:  # noqa: BLE001
            # Same rule as the webhook sink: alerting must never be able to take
            # down the pipeline it watches. The LogSink already has the message.
            log.warning("alertmanager push failed, continuing: %s", exc)


def _alertname(title: str) -> str:
    """Fallback for an ad-hoc alert: `mcx_bhavcopy: load blocked, x` -> `MdpLoadBlocked`.

    Everything before the colon is the source, which must not enter the name or
    a bad morning becomes one notification per symbol. Everything after the
    first comma is elaboration, which would make the name drift with the wording.
    """
    tail = title.split(":", 1)[-1].split(",", 1)[0]
    words = [w for w in "".join(
        c if c.isalnum() or c.isspace() else " " for c in tail
    ).split() if w]
    if not words:
        return "MdpPipelineAlert"
    return "Mdp" + "".join(w.capitalize() for w in words[:4])


def default_sinks() -> list[Sink]:
    """Log always; Alertmanager and webhook only if configured."""
    sinks: list[Sink] = [LogSink()]
    if os.getenv("MDP_ALERTMANAGER_URL"):
        sinks.append(AlertmanagerSink())
    if os.getenv("MDP_ALERT_WEBHOOK"):
        sinks.append(WebhookSink())
    return sinks


class Notifier:
    def __init__(self, sinks: list[Sink] | None = None,
                 minimum: Severity = Severity.WARN):
        self.sinks = sinks if sinks is not None else default_sinks()
        self.minimum = minimum
        self.sent: list[Alert] = []

    def send(self, alert: Alert) -> None:
        self.sent.append(alert)
        if alert.severity < self.minimum:
            return
        for sink in self.sinks:
            sink.send(alert)

    # Convenience for the three things worth alerting on in this platform.
    def blocked_load(self, source: str, blocked_by: list[str], detail: str) -> None:
        self.send(Alert(
            Severity.PAGE,
            f"{source}: load blocked, data not published",
            f"failed checks: {blocked_by}. {detail}",
            source=source,
            alertname="MdpLoadBlocked",
            context={"blocked_by": blocked_by},
        ))

    def missing_delivery(self, source: str, day, deadline, detail: str = "") -> None:
        self.send(Alert(
            Severity.PAGE,
            f"{source}: no delivery for {day}",
            f"expected by {deadline}. {detail}".strip(),
            source=source,
            alertname="MdpDeliveryMissing",
            context={"trading_day": str(day), "deadline": str(deadline)},
        ))

    def degraded_source(self, source: str, score: float, detail: str) -> None:
        self.send(Alert(
            Severity.WARN,
            f"{source}: vendor score {score:.3f}",
            detail,
            source=source,
            # Deliberately the same name the Prometheus rule uses: it is the same
            # condition seen from two angles, so they should group into one
            # notification rather than two. The `origin` label keeps them apart
            # for anyone who needs to know which noticed first.
            alertname="MdpVendorDegraded",
            context={"score": score},
        ))
