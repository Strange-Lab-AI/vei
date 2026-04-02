#!/usr/bin/env python3
"""One-off: serve Studio on a temp workspace and write docs screenshots."""
from __future__ import annotations

import tempfile
import threading
import time
from pathlib import Path

import uvicorn

from vei.run.api import launch_workspace_run
from vei.ui.api import create_ui_app
from vei.workspace.api import create_workspace_from_template


def main() -> None:
    root = Path(tempfile.mkdtemp()) / "workspace"
    create_workspace_from_template(
        root=root,
        source_kind="example",
        source_ref="acquired_user_cutover",
    )
    launch_workspace_run(root, runner="workflow")
    app = create_ui_app(root)
    port = 9876
    host = "127.0.0.1"

    def serve() -> None:
        uvicorn.run(app, host=host, port=port, log_level="warning")

    threading.Thread(target=serve, daemon=True).start()
    time.sleep(0.8)

    from playwright.sync_api import sync_playwright

    out_dir = Path(__file__).resolve().parents[1] / "docs" / "assets"
    out_dir.mkdir(parents=True, exist_ok=True)

    base = f"http://{host}:{port}"
    with sync_playwright() as p:
        browser = p.chromium.launch()
        page = browser.new_page(viewport={"width": 1440, "height": 900})
        page.goto(f"{base}/", wait_until="networkidle", timeout=60_000)
        page.wait_for_timeout(1500)
        page.screenshot(path=str(out_dir / "vei_studio_hero.png"), full_page=False)
        page.click('button[data-studio-view="outcome"]')
        page.wait_for_timeout(600)
        page.screenshot(path=str(out_dir / "vei_studio_outcome_tab.png"), full_page=False)
        browser.close()

    print("Wrote:", out_dir)


if __name__ == "__main__":
    main()
