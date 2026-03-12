from __future__ import annotations

import json
import sys
from pathlib import Path

import typer.testing

from vei.cli.vei import app
from vei.cli import vei_ui
from vei.imports.api import get_import_package_example_path


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


def test_product_cli_import_flow_supports_generation_and_provenance(
    tmp_path: Path,
) -> None:
    runner = typer.testing.CliRunner()
    root = tmp_path / "workspace"
    package_path = get_import_package_example_path("macrocompute_identity_export")

    validate_result = runner.invoke(
        app,
        ["project", "validate-import", "--package", str(package_path)],
    )
    assert validate_result.exit_code == 0, validate_result.output
    validate_payload = json.loads(validate_result.output)
    assert validate_payload["ok"] is True

    normalize_result = runner.invoke(
        app,
        ["project", "normalize", "--package", str(package_path)],
    )
    assert normalize_result.exit_code == 0, normalize_result.output
    normalize_payload = json.loads(normalize_result.output)
    assert normalize_payload["package"]["name"] == "macrocompute_identity_export"
    assert len(normalize_payload["generated_scenarios"]) >= 6

    import_result = runner.invoke(
        app,
        [
            "project",
            "import",
            "--root",
            str(root),
            "--package",
            str(package_path),
        ],
    )
    assert import_result.exit_code == 0, import_result.output
    import_payload = json.loads(import_result.output)
    assert import_payload["imports"]["package_name"] == "macrocompute_identity_export"

    generate_result = runner.invoke(
        app,
        ["scenario", "generate", "--root", str(root)],
    )
    assert generate_result.exit_code == 0, generate_result.output
    generate_payload = json.loads(generate_result.output)
    assert any(item["name"] == "oversharing_remediation" for item in generate_payload)

    bootstrap_result = runner.invoke(
        app,
        [
            "contract",
            "bootstrap",
            "--root",
            str(root),
            "--scenario-name",
            "oversharing_remediation",
            "--overwrite",
        ],
    )
    assert bootstrap_result.exit_code == 0, bootstrap_result.output
    bootstrap_payload = json.loads(bootstrap_result.output)
    assert bootstrap_payload["metadata"]["import_policy_id"] == "POL-WAVE2"

    run_result = runner.invoke(
        app,
        [
            "run",
            "start",
            "--root",
            str(root),
            "--runner",
            "workflow",
            "--scenario-name",
            "oversharing_remediation",
        ],
    )
    assert run_result.exit_code == 0, run_result.output
    run_payload = json.loads(run_result.output)
    assert run_payload["contract"]["ok"] is True

    provenance_result = runner.invoke(
        app,
        [
            "inspect",
            "provenance",
            "--root",
            str(root),
            "--object-ref",
            "drive_share:GDRIVE-2201",
        ],
    )
    assert provenance_result.exit_code == 0, provenance_result.output
    provenance_payload = json.loads(provenance_result.output)
    assert provenance_payload["provenance"][0]["origin"] == "imported"
