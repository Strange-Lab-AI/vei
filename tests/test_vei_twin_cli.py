from __future__ import annotations

import json
from pathlib import Path

import pytest
from typer.testing import CliRunner

from vei.cli.vei import app
from vei.cli import vei_twin
from vei.context.models import ContextSnapshot, ContextSourceResult
from vei.twin.models import (
    TwinLaunchManifest,
    TwinLaunchRuntime,
    TwinLaunchStatus,
    TwinOutcomeSummary,
    TwinServiceRecord,
)
from vei.twin.models import (
    CompatibilitySurfaceSpec,
    CustomerTwinBundle,
    WorkspaceGovernorStatus,
)

pytestmark = pytest.mark.integration


def test_twin_cli_builds_and_reports_status(tmp_path: Path) -> None:
    runner = CliRunner()
    root = tmp_path / "customer_twin"
    snapshot_path = tmp_path / "snapshot.json"
    snapshot_path.write_text(
        _sample_snapshot().model_dump_json(indent=2),
        encoding="utf-8",
    )

    build_result = runner.invoke(
        app,
        [
            "twin",
            "build",
            "--root",
            str(root),
            "--snapshot",
            str(snapshot_path),
            "--organization-domain",
            "acme.ai",
        ],
    )
    assert build_result.exit_code == 0, build_result.output
    build_payload = json.loads(build_result.output)
    assert build_payload["organization_name"] == "Acme Cloud"
    assert build_payload["organization_domain"] == "acme.ai"

    status_result = runner.invoke(
        app,
        [
            "twin",
            "status",
            "--root",
            str(root),
        ],
    )
    assert status_result.exit_code == 0, status_result.output
    status_payload = json.loads(status_result.output)
    assert status_payload["bundle"]["workspace_name"]
    assert status_payload["bundle"]["gateway"]["surfaces"][0]["name"] == "slack"


def test_twin_cli_lifecycle_commands_use_shared_runtime_surface(
    tmp_path: Path,
    monkeypatch,
) -> None:
    runner = CliRunner()
    root = tmp_path / "governed_twin"

    def _sample_bundle():
        return {
            "version": "1",
            "workspace_root": str(root),
            "workspace_name": "governed_twin",
            "organization_name": "Acme Cloud",
            "organization_domain": "acme.ai",
            "mold": {"archetype": "service_ops"},
            "context_snapshot_path": "context_snapshot.json",
            "blueprint_asset_path": "sources/blueprint_asset.json",
            "gateway": {
                "host": "127.0.0.1",
                "port": 3020,
                "auth_token": "token-123",
                "surfaces": [
                    {"name": "slack", "title": "Slack", "base_path": "/slack/api"}
                ],
                "ui_command": None,
            },
            "summary": "Acme Cloud twin",
            "metadata": {"governor": {"connector_mode": "sim"}},
        }

    monkeypatch.setattr(
        vei_twin,
        "load_customer_twin",
        lambda _root: CustomerTwinBundle.model_validate(_sample_bundle()),
    )
    monkeypatch.setattr(
        vei_twin,
        "build_workspace_governor_status",
        lambda _root, **_kwargs: WorkspaceGovernorStatus(
            governor={"config": {"connector_mode": "sim", "demo_mode": False}},
            outcome={"status": "running"},
            twin_status="running",
            services_ready=True,
        ),
    )
    monkeypatch.setattr(
        vei_twin,
        "build_twin_status",
        lambda _root: _sample_pilot_status(root),
    )

    calls: list[tuple[str, tuple, dict]] = []

    def _record(name):
        def inner(*args, **kwargs):
            calls.append((name, args, kwargs))
            return _sample_pilot_status(root)

        return inner

    monkeypatch.setattr(vei_twin, "start_twin", _record("up"))
    monkeypatch.setattr(vei_twin, "stop_twin", _record("down"))
    monkeypatch.setattr(vei_twin, "reset_twin", _record("reset"))
    monkeypatch.setattr(vei_twin, "finalize_twin", _record("finalize"))
    monkeypatch.setattr(vei_twin, "sync_twin", _record("sync"))

    for command in ("up", "down", "reset", "finalize", "sync"):
        result = runner.invoke(app, ["twin", command, "--root", str(root)])
        assert result.exit_code == 0, result.output
        payload = json.loads(result.output)
        assert payload["bundle"]["workspace_name"] == "governed_twin"
        assert payload["status"]["studio_url"] == "http://127.0.0.1:3011"

    assert [name for name, _args, _kwargs in calls] == [
        "up",
        "down",
        "reset",
        "finalize",
        "sync",
    ]


def _sample_snapshot() -> ContextSnapshot:
    return ContextSnapshot(
        organization_name="Acme Cloud",
        organization_domain="acme.ai",
        captured_at="2026-03-24T16:00:00+00:00",
        sources=[
            ContextSourceResult(
                provider="slack",
                captured_at="2026-03-24T16:00:00+00:00",
                status="ok",
                data={
                    "channels": [
                        {
                            "channel": "#revops-war-room",
                            "unread": 1,
                            "messages": [
                                {
                                    "ts": "1710300000.000100",
                                    "user": "maya.ops",
                                    "text": "We need a customer-safe recovery update today.",
                                }
                            ],
                        }
                    ]
                },
            ),
            ContextSourceResult(
                provider="jira",
                captured_at="2026-03-24T16:00:00+00:00",
                status="ok",
                data={
                    "issues": [
                        {
                            "ticket_id": "ACME-101",
                            "title": "Renewal blocker: onboarding API still timing out",
                            "status": "open",
                            "assignee": "maya.ops",
                            "description": "Customer onboarding export is timing out.",
                        }
                    ]
                },
            ),
            ContextSourceResult(
                provider="gmail",
                captured_at="2026-03-24T16:00:00+00:00",
                status="ok",
                data={
                    "threads": [
                        {
                            "thread_id": "thr-001",
                            "subject": "Renewal risk review",
                            "messages": [
                                {
                                    "from": "jordan.blake@apexfinancial.example.com",
                                    "to": "support@acme.ai",
                                    "subject": "Renewal risk review",
                                    "snippet": "Need a clear owner and a confirmed timeline.",
                                    "labels": ["IMPORTANT"],
                                    "unread": True,
                                }
                            ],
                        }
                    ]
                },
            ),
        ],
    )


def _sample_pilot_status(root: Path) -> TwinLaunchStatus:
    return TwinLaunchStatus(
        manifest=TwinLaunchManifest(
            workspace_root=root,
            workspace_name="governed_twin",
            organization_name="Acme Cloud",
            organization_domain="acme.ai",
            archetype="service_ops",
            crisis_name="Dispatch overload",
            studio_url="http://127.0.0.1:3011",
            control_room_url="http://127.0.0.1:3011/",
            gateway_url="http://127.0.0.1:3020",
            gateway_status_url="http://127.0.0.1:3020/api/twin",
            bearer_token="token-123",
            supported_surfaces=[
                CompatibilitySurfaceSpec(
                    name="slack",
                    title="Slack",
                    base_path="/slack/api",
                )
            ],
            recommended_first_move="Read the queue before acting.",
            sample_client_path="/tmp/governor_client.py",
        ),
        runtime=TwinLaunchRuntime(
            workspace_root=root,
            services=[
                TwinServiceRecord(
                    name="gateway",
                    host="127.0.0.1",
                    port=3020,
                    url="http://127.0.0.1:3020",
                    state="running",
                ),
                TwinServiceRecord(
                    name="studio",
                    host="127.0.0.1",
                    port=3011,
                    url="http://127.0.0.1:3011",
                    state="running",
                ),
            ],
        ),
        active_run="external-run",
        twin_status="running",
        request_count=3,
        services_ready=True,
        outcome=TwinOutcomeSummary(
            status="running",
            summary="Outside work is active.",
        ),
    )
