from __future__ import annotations

import json
from pathlib import Path

import pytest
import typer.testing

from vei.cli.vei_scenarios import app as scenarios_app

pytestmark = pytest.mark.integration


def test_compile_command_reads_dsl(tmp_path: Path) -> None:
    spec = {
        "meta": {"name": "dsl-demo"},
        "budget": {"cap_usd": 3100},
        "documents": [
            {"doc_id": "DOC-1", "title": "Guide", "body": "Content"},
        ],
    }
    cfg_path = tmp_path / "scene.json"
    cfg_path.write_text(json.dumps(spec), encoding="utf-8")

    runner = typer.testing.CliRunner()
    result = runner.invoke(scenarios_app, ["compile", str(cfg_path)])
    assert result.exit_code == 0, result.output
    data = json.loads(result.stdout)
    assert data["metadata"]["name"] == "dsl-demo"
    assert "DOC-1" in data["documents"]


def test_dump_existing_scenario() -> None:
    runner = typer.testing.CliRunner()
    result = runner.invoke(scenarios_app, ["dump", "macrocompute_default"])
    assert result.exit_code == 0, result.output
    data = json.loads(result.stdout)
    assert data["budget_cap_usd"] == 3500


def test_manifest_lists_entries() -> None:
    runner = typer.testing.CliRunner()
    result = runner.invoke(scenarios_app, ["manifest"])
    assert result.exit_code == 0, result.output
    data = json.loads(result.stdout)
    assert isinstance(data, list)
    assert any(item["name"] == "multi_channel" for item in data)


def test_manifest_for_named_scenario() -> None:
    runner = typer.testing.CliRunner()
    result = runner.invoke(scenarios_app, ["manifest", "--name", "identity_access"])
    assert result.exit_code == 0, result.output
    data = json.loads(result.stdout)
    assert data["name"] == "identity_access"
    assert "okta" in data["tool_families"]
    assert "servicedesk" in data["tool_families"]
