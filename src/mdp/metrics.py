"""Metrics, in the format everything speaks.

A status table a person reads is not monitoring. Monitoring is numbers a machine
scrapes, so that a graph exists before anyone needs it and an alert can fire on a
trend rather than on a single bad morning.

Prometheus text format, on an endpoint Prometheus scrapes. Nothing here draws a
chart or decides what is bad: Grafana owns the first and the alert rules own the
second, because those are the tools the on-call rotation already lives in. The
platform's job is to publish honest numbers and get out of the way.
"""
from __future__ import annotations

import logging
import threading
import time
from datetime import UTC, datetime

import pandas as pd

from .storage import Store

log = logging.getLogger("mdp.metrics")


def _line(name: str, value, labels: dict | None = None) -> str:
    if labels:
        rendered = ",".join(f'{k}="{v}"' for k, v in labels.items())
        return f"{name}{{{rendered}}} {value}"
    return f"{name} {value}"


def render(store: Store) -> str:
    """Everything worth graphing, in one scrape."""
    out: list[str] = []
    now = datetime.now(UTC)

    # Fail loudly if the store cannot be read at all.
    #
    # Every section below swallows its own exception, which is right for a table
    # that does not exist yet: one missing dataset should not blank the whole
    # scrape. But those same handlers turn "the database is unreachable" into a
    # clean scrape containing no metrics, and an absent metric does not fire an
    # alert. That is the worst possible failure for a monitoring endpoint: it
    # reports health by saying nothing. One probe query, deliberately outside the
    # handlers, makes the difference between "no data" and "no answer" visible.
    store.query("SELECT 1 AS probe")

    def section(name: str, help_text: str, kind: str = "gauge") -> None:
        out.append(f"# HELP {name} {help_text}")
        out.append(f"# TYPE {name} {kind}")

    # How much data there is, and how stale it is. The two questions a desk asks.
    section("mdp_rows_total", "Rows currently loaded per dataset")
    section("mdp_data_age_seconds", "Age of the newest event per dataset")
    for table, tcol in (("trades", "ts"), ("eod_bars", "trade_date")):
        try:
            rows = store.query(
                f"SELECT symbol, count(*) AS n, max({tcol}) AS latest "
                f"FROM {table} GROUP BY symbol"
            )
        except Exception:
            continue
        for _, row in rows.iterrows():
            labels = {"dataset": table, "symbol": row["symbol"]}
            out.append(_line("mdp_rows_total", int(row["n"]), labels))
            latest = pd.to_datetime(row["latest"], utc=True, errors="coerce")
            if pd.notna(latest):
                age = max((pd.Timestamp(now) - latest).total_seconds(), 0.0)
                out.append(_line("mdp_data_age_seconds", round(age, 1), labels))

    # Check outcomes, so a rising warn rate is visible before it becomes a failure.
    section("mdp_checks_total", "Quality check results", "counter")
    try:
        checks = store.query(
            "SELECT source, check_name, status, count(*) AS n "
            "FROM quality_events GROUP BY source, check_name, status"
        )
        for _, row in checks.iterrows():
            out.append(_line("mdp_checks_total", int(row["n"]), {
                "source": row["source"], "check": row["check_name"],
                "status": row["status"],
            }))
    except Exception:
        pass

    # Vendor health, which is a trend rather than an incident.
    section("mdp_vendor_score", "Latest vendor quality score")
    try:
        scores = store.query("SELECT * FROM vendor_scores ORDER BY as_of DESC")
        for _, row in scores.drop_duplicates(subset=["source"]).iterrows():
            out.append(_line("mdp_vendor_score", row["score"], {"source": row["source"]}))
    except Exception:
        pass

    # Two different questions, and they were being answered by one number under
    # a name that described the other one.
    #
    # `position_age` is how far behind the DATA is: now minus the timestamp of
    # the furthest row loaded. It climbs during a quiet market even when the
    # poller is perfectly healthy.
    #
    # `idle` is how long since the cursor MOVED: now minus the time it was last
    # written. That is the one that tells you a poller has died, because it
    # climbs when nothing is happening regardless of what the market is doing.
    #
    # The metric was computing the first and calling it the second, and the
    # alert on it inherited the error.
    section("mdp_cursor_position_age_seconds",
            "Age of the furthest event each source has loaded")
    section("mdp_cursor_idle_seconds",
            "Time since each source cursor last advanced")
    try:
        cursors = store.query("SELECT * FROM ingest_cursors ORDER BY updated_at DESC")
        for _, row in cursors.drop_duplicates(subset=["source"]).iterrows():
            labels = {"source": row["source"]}
            ts = pd.to_datetime(row["position_ts"], utc=True, errors="coerce")
            if pd.notna(ts):
                out.append(_line("mdp_cursor_position_age_seconds",
                                 round(max((pd.Timestamp(now) - ts).total_seconds(), 0.0), 1),
                                 labels))
            moved = pd.to_datetime(row.get("updated_at"), utc=True, errors="coerce")
            if pd.notna(moved):
                out.append(_line("mdp_cursor_idle_seconds",
                                 round(max((pd.Timestamp(now) - moved).total_seconds(), 0.0), 1),
                                 labels))
    except Exception:
        pass

    # What the exchange calendar says should be here. Without this, an alert on
    # data age fires every Sunday and gets muted, and then it is not an alert.
    section("mdp_trading_day", "1 when the exchange is open right now, else 0")
    try:
        from .calendar import load_calendar

        cal = load_calendar("mcx")
        out.append(_line("mdp_trading_day", int(cal.is_trading_day(now.date())),
                         {"calendar": "mcx"}))
        section("mdp_delivery_deadline_seconds",
                "Seconds until (negative: since) the latest file was due")
        day = (now.date() if cal.is_trading_day(now.date())
               else cal.previous_trading_day(now.date()))
        deadline = cal.delivery_deadline(day)
        out.append(_line("mdp_delivery_deadline_seconds",
                         round((deadline - now).total_seconds(), 1),
                         {"calendar": "mcx", "trading_day": str(day)}))
    except Exception:
        pass

    return "\n".join(out) + "\n"


# ---------------------------------------------------------------------------
# The endpoint
# ---------------------------------------------------------------------------

_HELP_EXPORTER = """# HELP mdp_exporter_scrape_failures_total Scrapes that could not read the store
# TYPE mdp_exporter_scrape_failures_total counter
# HELP mdp_exporter_last_success_timestamp_seconds When the store was last read
# TYPE mdp_exporter_last_success_timestamp_seconds gauge
# HELP mdp_exporter_scrape_duration_seconds How long the last scrape took
# TYPE mdp_exporter_scrape_duration_seconds gauge
"""


class _Exporter:
    """Reads the store on demand, and is honest when it cannot.

    A scrape opens its own read-only connection and closes it again. That matters
    on DuckDB, where a long-lived reader would fight the daily load for the file
    lock, and it is good manners on QuestDB too: the thing that watches the
    pipeline should never be the reason the pipeline stalls.

    When a read fails (lock held, server restarting) the last good payload is
    served again, with a failure counter and a last-success timestamp alongside
    it. Prometheus can then alert on the exporter having gone quiet, which is a
    different incident from the data having gone stale, and the two get confused
    often enough to be worth separating.
    """

    def __init__(self, store_factory):
        self.store_factory = store_factory
        self.failures = 0
        self.last_success = 0.0
        self.last_duration = 0.0
        self.payload = ""
        self.lock = threading.Lock()

    def scrape(self) -> str:
        started = time.time()
        store = None
        try:
            store = self.store_factory()
            body = render(store)
            with self.lock:
                self.payload = body
                self.last_success = time.time()
                self.last_duration = self.last_success - started
        except Exception as exc:  # noqa: BLE001
            with self.lock:
                self.failures += 1
            log.warning("scrape failed, serving last good payload: %s", exc)
        finally:
            if store is not None:
                try:
                    store.close()
                except Exception:  # noqa: BLE001
                    pass
        with self.lock:
            return (
                self.payload
                + _HELP_EXPORTER
                + _line("mdp_exporter_scrape_failures_total", self.failures) + "\n"
                + _line("mdp_exporter_last_success_timestamp_seconds",
                        round(self.last_success, 1)) + "\n"
                + _line("mdp_exporter_scrape_duration_seconds",
                        round(self.last_duration, 4)) + "\n"
            )


def serve(store_factory, *, host: str = "0.0.0.0", port: int = 9108):
    """Block, serving /metrics until interrupted."""
    from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

    exporter = _Exporter(store_factory)

    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def do_GET(self):  # noqa: N802
            if self.path.rstrip("/") in ("/metrics", ""):
                body = exporter.scrape().encode()
                self.send_response(200)
                self.send_header("Content-Type", "text/plain; version=0.0.4")
            elif self.path.rstrip("/") == "/healthz":
                body = b"ok\n"
                self.send_response(200)
                self.send_header("Content-Type", "text/plain")
            else:
                body = b"try /metrics\n"
                self.send_response(404)
                self.send_header("Content-Type", "text/plain")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, fmt, *args):  # quiet: Prometheus scrapes constantly
            log.debug(fmt, *args)

    server = ThreadingHTTPServer((host, port), Handler)
    log.info("metrics on http://%s:%d/metrics", host, port)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
