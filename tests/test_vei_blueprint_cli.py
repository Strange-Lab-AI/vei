from __future__ import annotations

import json

import typer.testing

from vei.cli.vei_blueprint import app


def test_vei_blueprint_list_and_show_family() -> None:
    runner = typer.testing.CliRunner()

    list_result = runner.invoke(app, ["list"])
    show_result = runner.invoke(
        app, ["show", "--family", "security_containment", "--indent", "2"]
    )

    assert list_result.exit_code == 0, list_result.output
    assert "security_containment.blueprint" in list_result.output
    assert show_result.exit_code == 0, show_result.output
    payload = json.loads(show_result.output)
    assert payload["name"] == "security_containment.blueprint"
    assert payload["contract"]["name"] == "security_containment.contract"


def test_vei_blueprint_asset_and_compile_commands() -> None:
    runner = typer.testing.CliRunner()

    asset_result = runner.invoke(
        app,
        [
            "asset",
            "--family",
            "revenue_incident_mitigation",
            "--workflow-variant",
            "revenue_ops_flightdeck",
        ],
    )
    compile_result = runner.invoke(
        app,
        [
            "compile",
            "--family",
            "revenue_incident_mitigation",
            "--workflow-variant",
            "revenue_ops_flightdeck",
        ],
    )

    assert asset_result.exit_code == 0, asset_result.output
    assert compile_result.exit_code == 0, compile_result.output
    asset_payload = json.loads(asset_result.output)
    compile_payload = json.loads(compile_result.output)
    assert asset_payload["workflow_variant"] == "revenue_ops_flightdeck"
    assert compile_payload["workflow_variant"] == "revenue_ops_flightdeck"
    assert compile_payload["asset"]["scenario_name"] == "checkout_spike_mitigation"
    assert any(item["name"] == "spreadsheet" for item in compile_payload["facades"])


def test_vei_blueprint_facades_filters_by_domain() -> None:
    runner = typer.testing.CliRunner()
    result = runner.invoke(app, ["facades", "--domain", "obs_graph"])

    assert result.exit_code == 0, result.output
    payload = json.loads(result.output)
    names = {item["name"] for item in payload}
    assert {"siem", "datadog", "pagerduty"} <= names
