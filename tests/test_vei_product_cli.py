from __future__ import annotations

import json
import sys
from pathlib import Path

import typer.testing

from vei.cli.vei import app
from vei.cli import vei_ui


def test_product_cli_workspace_run_and_inspect_flow(tmp_path: Path) -> None:
    runner = typer.testing.CliRunner()
    root = tmp_path / "workspace"

    init_result = runner.invoke(
        app,
        [
            "project",
            "init",
            "--root",
            str(root),
            "--example",
            "acquired_user_cutover",
        ],
    )
    assert init_result.exit_code == 0, init_result.output

    contract_result = runner.invoke(
        app,
        ["contract", "validate", "--root", str(root)],
    )
    assert contract_result.exit_code == 0, contract_result.output
    contract_payload = json.loads(contract_result.output)
    assert contract_payload["ok"] is True

    preview_result = runner.invoke(
        app,
        ["scenario", "preview", "--root", str(root)],
    )
    assert preview_result.exit_code == 0, preview_result.output
    preview_payload = json.loads(preview_result.output)
    assert preview_payload["scenario"]["name"] == "default"

    run_result = runner.invoke(
        app,
        ["run", "start", "--root", str(root), "--runner", "workflow"],
    )
    assert run_result.exit_code == 0, run_result.output
    run_payload = json.loads(run_result.output)
    run_id = run_payload["run_id"]
    assert run_payload["status"] == "ok"
    assert run_payload["contract"]["ok"] is True

    events_result = runner.invoke(
        app,
        ["inspect", "events", "--root", str(root), "--run-id", run_id],
    )
    assert events_result.exit_code == 0, events_result.output
    events_payload = json.loads(events_result.output)
    assert any(item["kind"] == "workflow_step" for item in events_payload["events"])

    graphs_result = runner.invoke(
        app,
        [
            "inspect",
            "graphs",
            "--root",
            str(root),
            "--run-id",
            run_id,
            "--domain",
            "identity_graph",
        ],
    )
    assert graphs_result.exit_code == 0, graphs_result.output
    graphs_payload = json.loads(graphs_result.output)
    assert graphs_payload["domain"] == "identity_graph"
    assert graphs_payload["graph"]["policies"][0]["policy_id"] == "POL-WAVE2"

    timeline_path = root / "runs" / run_id / "timeline.json"
    timeline_path.unlink()
    fallback_events_result = runner.invoke(
        app,
        ["inspect", "events", "--root", str(root), "--run-id", run_id],
    )
    assert fallback_events_result.exit_code == 0, fallback_events_result.output
    fallback_payload = json.loads(fallback_events_result.output)
    assert fallback_payload["events"]


def test_product_cli_rejects_invalid_runner(tmp_path: Path) -> None:
    runner = typer.testing.CliRunner()
    root = tmp_path / "workspace"
    init_result = runner.invoke(
        app,
        [
            "project",
            "init",
            "--root",
            str(root),
            "--example",
            "acquired_user_cutover",
        ],
    )
    assert init_result.exit_code == 0, init_result.output

    run_result = runner.invoke(
        app,
        ["run", "start", "--root", str(root), "--runner", "nonsense"],
    )
    assert run_result.exit_code != 0
    assert "runner must be workflow, scripted, bc, or llm" in run_result.output


def test_standalone_vei_ui_main_accepts_serve_alias(monkeypatch) -> None:
    captured: dict[str, object] = {}

    def fake_app() -> None:
        captured["argv"] = sys.argv[1:]

    monkeypatch.setattr(vei_ui, "app", fake_app)
    monkeypatch.setattr(sys, "argv", ["vei-ui", "serve", "--root", "workspace"])

    vei_ui.main()

    assert captured["argv"] == ["--root", "workspace"]
