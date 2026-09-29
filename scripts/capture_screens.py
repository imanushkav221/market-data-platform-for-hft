"""Screenshot the running monitoring stack.

These are pictures of the real thing: Prometheus evaluating the repo's own rule
file against the live exporter, and Alertmanager holding what those rules and the
pipeline pushed at it. Nothing here is mocked or staged for the camera.

    PYTHONPATH=src python scripts/capture_screens.py
"""
from __future__ import annotations

import pathlib
import sys

from playwright.sync_api import sync_playwright

OUT = pathlib.Path("docs/screens")
SHOTS = [
    ("prometheus-alerts", "http://127.0.0.1:9090/alerts", 1600, 1200),
    ("prometheus-rules", "http://127.0.0.1:9090/rules", 1600, 1400),
    ("prometheus-graph",
     "http://127.0.0.1:9090/query?g0.expr=mdp_vendor_score&g0.show_tree=0"
     "&g0.tab=graph&g0.range_input=30m&g0.res_type=auto&g0.display_mode=lines",
     1600, 1000),
    ("alertmanager", "http://127.0.0.1:9093/#/alerts", 1600, 900),
]


def main() -> int:
    OUT.mkdir(parents=True, exist_ok=True)
    with sync_playwright() as p:
        browser = p.chromium.launch(args=["--no-sandbox"])
        for name, url, width, height in SHOTS:
            # locale is explicit: this container reports en-US@posix, which the
            # Prometheus 3.x UI rejects with "Invalid language tag" and renders
            # a blank page. A screenshot of a blank page is worse than no
            # screenshot, because it looks like the service is down.
            page = browser.new_page(viewport={"width": width, "height": height},
                                    device_scale_factor=2, locale="en-US")
            try:
                page.goto(url, wait_until="networkidle", timeout=45000)
            except Exception as exc:  # noqa: BLE001
                print(f"  {name}: navigation issue ({exc.__class__.__name__}), "
                      f"screenshotting anyway")
            page.wait_for_timeout(6000)
            if not page.inner_text("body").strip():
                print(f"  {name}: page rendered empty, not writing a misleading shot")
                page.close()
                continue
            path = OUT / f"{name}.png"
            page.screenshot(path=str(path), full_page=False)
            print(f"  wrote {path}")
            page.close()
        browser.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
