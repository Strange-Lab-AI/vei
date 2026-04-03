from __future__ import annotations

import json
from pathlib import Path

from typer.testing import CliRunner

from vei.cli import vei_pilot
from vei.cli.vei import app
from vei.pilot.models import (
    PilotManifest,
    PilotOutcomeSummary,
    PilotRuntime,
    PilotServiceRecord,
    PilotStatus,
)
from vei.twin.models import CompatibilitySurfaceSpec


def test_pilot_cli_commands_are_wired_into_root_app(
    tmp_path: Path,
    monkeypatch,
) -> None:
    runner = CliRunner()
    root = tmp_path / "pilot"

    monkeypatch.setattr(
        vei_pilot,
        "start_pilot",
        lambda *args, **kwargs: _sample_status(root),
    )
    monkeypatch.setattr(
        vei_pilot,
        "build_pilot_status",
        lambda *args, **kwargs: _sample_status(root),
    )
    monkeypatch.setattr(
        vei_pilot,
        "stop_pilot",
        lambda *args, **kwargs: _sample_status(root, services_ready=False),
    )

    up_result = runner.invoke(app, ["pilot", "up", "--root", str(root)])
    assert up_result.exit_code == 0, up_result.output
    up_payload = json.loads(up_result.output)
    assert up_payload["manifest"]["organization_name"] == "Pinnacle Analytics"

    status_result = runner.invoke(app, ["pilot", "status", "--root", str(root)])
    assert status_result.exit_code == 0, status_result.output
    status_payload = json.loads(status_result.output)
    assert status_payload["active_run"] == "external_renewal_run"

    down_result = runner.invoke(app, ["pilot", "down", "--root", str(root)])
    assert down_result.exit_code == 0, down_result.output
    down_payload = json.loads(down_result.output)
    assert down_payload["services_ready"] is False


def test_pilot_up_forwards_orchestrator_options(tmp_path: Path, monkeypatch) -> None:
    runner = CliRunner()
    root = tmp_path / "pilot"
    captured: dict[str, object] = {}

    def fake_start_pilot(*args, **kwargs):
        captured.update(kwargs)
        return _sample_status(root)

    monkeypatch.setattr(vei_pilot, "start_pilot", fake_start_pilot)

    result = runner.invoke(
        app,
        [
            "pilot",
            "up",
            "--root",
            str(root),
            "--orchestrator",
            "paperclip",
            "--orchestrator-url",
            "http://paperclip.local",
            "--orchestrator-company-id",
            "company-1",
            "--orchestrator-api-key-env",
            "PAPERCLIP_TEST_KEY",
        ],
    )

    assert result.exit_code == 0, result.output
    assert captured["orchestrator"] == "paperclip"
    assert captured["orchestrator_url"] == "http://paperclip.local"
    assert captured["orchestrator_company_id"] == "company-1"
    assert captured["orchestrator_api_key_env"] == "PAPERCLIP_TEST_KEY"


def _sample_status(root: Path, *, services_ready: bool = True) -> PilotStatus:
    manifest = PilotManifest(
        workspace_root=root,
        workspace_name="pilot",
        organization_name="Pinnacle Analytics",
        organization_domain="pinnacle.example.com",
        archetype="b2b_saas",
        crisis_name="Renewal save",
        studio_url="http://127.0.0.1:3011",
        pilot_console_url="http://127.0.0.1:3011/pilot",
        gateway_url="http://127.0.0.1:3020",
        gateway_status_url="http://127.0.0.1:3020/api/twin",
        bearer_token="pilot-token",
        supported_surfaces=[
            CompatibilitySurfaceSpec(
                name="slack",
                title="Slack",
                base_path="/slack/api",
            )
        ],
        recommended_first_exercise="Read Slack and Jira, then post one customer-safe update.",
        sample_client_path="/tmp/pilot_client.py",
    )
    runtime = PilotRuntime(
        workspace_root=root,
        services=[
            PilotServiceRecord(
                name="gateway",
                host="127.0.0.1",
                port=3020,
                url="http://127.0.0.1:3020",
                pid=4101 if services_ready else None,
                state="running" if services_ready else "stopped",
            ),
            PilotServiceRecord(
                name="studio",
                host="127.0.0.1",
                port=3011,
                url="http://127.0.0.1:3011",
                pid=4102 if services_ready else None,
                state="running" if services_ready else "stopped",
            ),
        ],
        started_at="2026-03-25T18:00:00+00:00",
        updated_at="2026-03-25T18:05:00+00:00",
    )
    return PilotStatus(
        manifest=manifest,
        runtime=runtime,
        active_run="external_renewal_run",
        twin_status="running",
        request_count=4,
        services_ready=services_ready,
        outcome=PilotOutcomeSummary(
            status="running",
            contract_ok=False,
            issue_count=2,
            summary="The renewal is still at risk and needs another action.",
            latest_tool="slack.send_message",
            current_tension="Customer trust is slipping.",
            affected_surfaces=["Email", "Slack"],
        ),
    )
