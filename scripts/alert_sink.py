"""A stand-in for wherever alerts actually go.

Alertmanager's receivers are webhooks, and so are Slack, Teams, PagerDuty and
OpsGenie underneath. This prints what they would have shown, so the whole loop
can be exercised on a laptop: rule fires in Prometheus, routes and groups in
Alertmanager, arrives here already deduplicated.

Swapping this for the real destination is a URL in monitoring/alertmanager.yml.
That is the entire reason the pipeline pushes to Alertmanager rather than posting
to Slack directly.

    python scripts/alert_sink.py            # listens on :9199
"""
from __future__ import annotations

import argparse
import json
from datetime import datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

BOLD, DIM, RED, YELLOW, GREEN, RESET = (
    "\033[1m", "\033[2m", "\033[31m", "\033[33m", "\033[32m", "\033[0m"
)


def _render(route: str, body: dict) -> None:
    status = body.get("status", "?")
    colour = GREEN if status == "resolved" else (
        RED if body.get("commonLabels", {}).get("severity") == "page" else YELLOW
    )
    group = body.get("groupLabels", {})
    alerts = body.get("alerts", [])
    stamp = datetime.now().strftime("%H:%M:%S")
    print(
        f"\n{colour}{BOLD}[{status.upper()}]{RESET} {stamp}  "
        f"{colour}{group.get('alertname', 'alert')}{RESET}"
        f"{DIM}  route={route}  ({len(alerts)} in group){RESET}"
    )
    for alert in alerts:
        labels = alert.get("labels", {})
        notes = alert.get("annotations", {})
        source = labels.get("source", "-")
        print(f"  {BOLD}{notes.get('summary', labels.get('alertname', ''))}{RESET}")
        print(f"    source={source} severity={labels.get('severity', '-')} "
              f"origin={labels.get('origin', 'rule')}")
        if notes.get("description"):
            print(f"    {' '.join(notes['description'].split())}")
        if notes.get("runbook"):
            print(f"    {DIM}runbook: {notes['runbook']}{RESET}")


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def do_POST(self):  # noqa: N802
        length = int(self.headers.get("Content-Length", 0))
        raw = self.rfile.read(length) if length else b"{}"
        try:
            _render(self.path, json.loads(raw))
        except Exception as exc:  # noqa: BLE001
            print(f"could not parse alert payload: {exc}")
        self.send_response(200)
        self.send_header("Content-Length", "0")
        self.end_headers()

    def log_message(self, fmt, *args):
        pass


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--port", type=int, default=9199)
    args = p.parse_args()
    print(f"alert sink listening on http://0.0.0.0:{args.port} "
          f"(routes: /alerts, /alerts/page, /alerts/warn)")
    server = ThreadingHTTPServer(("0.0.0.0", args.port), Handler)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nstopped")
    finally:
        server.server_close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
