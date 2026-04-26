from __future__ import annotations

import argparse
import requests
import socket
import subprocess
import sys
import time
from contextlib import closing
from pathlib import Path

try:
    from playwright.sync_api import Page, sync_playwright
except ModuleNotFoundError:  # pragma: no cover - optional dependency
    sync_playwright = None
    Page = object  # type: ignore[assignment]

try:
    from scripts.enron_example_specs import bundle_specs
except ModuleNotFoundError:
    sys.path.insert(0, str(Path(__file__).resolve().parent))
    from enron_example_specs import bundle_specs


ASSETS_ROOT = Path("docs/assets/enron-whatif")
VIEWPORT = {"width": 1680, "height": 2200}


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Capture stable Studio screenshots for the repo-owned Enron bundles."
    )
    parser.add_argument(
        "--bundle",
        action="append",
        default=[],
        help="Capture only the named bundle slug. Repeat to capture multiple bundles.",
    )
    return parser.parse_args()


def _free_port() -> int:
    with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as sock:
        sock.bind(("127.0.0.1", 0))
        sock.listen(1)
        return int(sock.getsockname()[1])


def _wait_for_server(
    base_url: str,
    *,
    server: subprocess.Popen[str],
    timeout_s: float = 60.0,
) -> None:
    deadline = time.monotonic() + timeout_s
    last_error = ""
    while time.monotonic() < deadline:
        if server.poll() is not None:
            stdout = (server.stdout.read() if server.stdout is not None else "").strip()
            stderr = (server.stderr.read() if server.stderr is not None else "").strip()
            details = stderr or stdout or "no server output"
            raise RuntimeError(f"Studio exited before startup at {base_url}: {details}")
        try:
            response = requests.get(f"{base_url}/api/workspace", timeout=1.5)
            if response.status_code == 200:
                return
        except requests.RequestException as exc:
            last_error = str(exc)
        time.sleep(0.2)
    raise RuntimeError(f"Studio did not come up at {base_url}: {last_error}")


def _start_server(workspace_root: Path, *, port: int) -> subprocess.Popen[str]:
    command = [
        sys.executable,
        "-c",
        (
            "from vei.ui.app import serve_ui; "
            f"serve_ui({str(workspace_root)!r}, host='127.0.0.1', port={port})"
        ),
    ]
    return subprocess.Popen(
        command,
        cwd=str(Path.cwd()),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )


def _open_whatif_view(page: Page) -> None:
    page.locator("#whatif-selection .whatif-scene-shell").wait_for(timeout=30_000)


def _capture_locator(page: Page, selector: str, target: Path, *, nth: int = 0) -> None:
    target.parent.mkdir(parents=True, exist_ok=True)
    locator = page.locator(selector).nth(nth)
    locator.scroll_into_view_if_needed()
    page.wait_for_timeout(200)
    locator.screenshot(path=str(target), animations="disabled")


def _run_saved_experiment(page: Page) -> None:
    page.evaluate("() => window.runWhatIfExperimentFromUI()")
    page.locator("#whatif-experiment-result .whatif-summary-grid").wait_for(
        timeout=30_000
    )
    page.locator("#whatif-experiment-result .is-business-change").first.wait_for(
        timeout=30_000
    )


def _run_saved_ranking(page: Page) -> None:
    page.evaluate("() => window.runRankedWhatIfFromUI()")
    page.locator("#whatif-experiment-result .whatif-ranked-list").wait_for(
        timeout=30_000
    )


def _bundle_targets(bundle_slug: str) -> dict[str, Path]:
    stem = bundle_slug.removeprefix("enron-")
    return {
        "forecast": ASSETS_ROOT / f"{stem}-forecast.png",
        "ranking": ASSETS_ROOT / f"{stem}-ranking.png",
    }


def _generic_master_targets() -> dict[str, Path]:
    return {
        "scene": ASSETS_ROOT / "enron-decision-scene-top.png",
        "public_context": ASSETS_ROOT / "enron-public-context.png",
        "forecast": ASSETS_ROOT / "enron-predicted-business-change.png",
        "macro": ASSETS_ROOT / "enron-macro-outcomes.png",
        "ranking": ASSETS_ROOT / "enron-ranked-comparison.png",
    }


def _capture_bundle(bundle_slug: str, workspace_root: Path) -> dict[str, str]:
    port = _free_port()
    base_url = f"http://127.0.0.1:{port}"
    server = _start_server(workspace_root, port=port)
    try:
        _wait_for_server(base_url, server=server)
        with sync_playwright() as playwright:
            browser = playwright.chromium.launch(headless=True)
            page = browser.new_page(viewport=VIEWPORT, device_scale_factor=1)
            page.goto(base_url, wait_until="networkidle")
            _open_whatif_view(page)

            bundle_targets = _bundle_targets(bundle_slug)

            if bundle_slug == "enron-master-agreement-public-context":
                generic_targets = _generic_master_targets()
                _capture_locator(
                    page,
                    "#whatif-selection .whatif-scene-shell",
                    generic_targets["scene"],
                )
                _capture_locator(
                    page,
                    "#whatif-selection .whatif-scene-panel.is-public-context",
                    generic_targets["public_context"],
                )

            _run_saved_experiment(page)
            _capture_locator(
                page,
                "#whatif-experiment-result .whatif-scene-panel.is-business-change",
                bundle_targets["forecast"],
                nth=0,
            )

            if bundle_slug == "enron-master-agreement-public-context":
                generic_targets = _generic_master_targets()
                _capture_locator(
                    page,
                    "#whatif-experiment-result .whatif-scene-panel.is-business-change",
                    generic_targets["forecast"],
                    nth=0,
                )
                _capture_locator(
                    page,
                    "#whatif-experiment-result .whatif-scene-panel.is-business-change",
                    generic_targets["macro"],
                    nth=1,
                )

            _run_saved_ranking(page)
            _capture_locator(
                page, "#whatif-experiment-result", bundle_targets["ranking"]
            )

            if bundle_slug == "enron-master-agreement-public-context":
                _capture_locator(
                    page,
                    "#whatif-experiment-result",
                    _generic_master_targets()["ranking"],
                )
            browser.close()
    finally:
        server.terminate()
        try:
            server.wait(timeout=5)
        except subprocess.TimeoutExpired:
            server.kill()
            server.wait(timeout=5)

    return {name: str(path) for name, path in _bundle_targets(bundle_slug).items()}


def main() -> None:
    if sync_playwright is None:
        raise SystemExit(
            "playwright is required for Enron screenshot capture. "
            "Install browser extras with `pip install -e '.[browser]'`."
        )

    args = _parse_args()
    selected_bundles = set(args.bundle)
    specs = [
        spec
        for spec in bundle_specs()
        if not selected_bundles or spec.bundle_slug in selected_bundles
    ]
    if not specs:
        raise SystemExit("No matching Enron bundles were selected.")

    ASSETS_ROOT.mkdir(parents=True, exist_ok=True)
    for spec in specs:
        workspace_root = spec.output_root / "workspace"
        _capture_bundle(spec.bundle_slug, workspace_root)
        print(f"Captured screenshots for {spec.bundle_slug}")


if __name__ == "__main__":
    main()
