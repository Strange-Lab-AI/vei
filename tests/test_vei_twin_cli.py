from __future__ import annotations

import json
from pathlib import Path
import zipfile

import pytest
from typer.testing import CliRunner

from vei.cli.vei import app
from vei.cli import vei_twin
from vei.context.api import CanonicalHistoryReadinessReport
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


def test_twin_cli_onboard_builds_provider_configs_and_reports_timeline(
    tmp_path: Path,
    monkeypatch,
) -> None:
    runner = CliRunner()
    root = tmp_path / "onboarded"

    captured: dict[str, object] = {}

    def _fake_build_customer_twin(*args, **kwargs):
        captured["args"] = args
        captured["kwargs"] = kwargs
        bundle = CustomerTwinBundle.model_validate(
            {
                "version": "1",
                "workspace_root": str(root),
                "workspace_name": "onboarded",
                "organization_name": "Acme",
                "organization_domain": "acme.ai",
                "mold": {"archetype": "b2b_saas"},
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
                "summary": "Acme twin",
                "metadata": {},
            }
        )
        return bundle

    monkeypatch.setattr(vei_twin, "build_customer_twin", _fake_build_customer_twin)
    monkeypatch.setattr(
        vei_twin,
        "build_canonical_history_readiness",
        lambda _path: CanonicalHistoryReadinessReport(
            available=True,
            organization_name="Acme",
            organization_domain="acme.ai",
            source_providers=["github", "clickup"],
            event_count=180,
            case_count=9,
            surface_count=2,
            exact_timestamp_count=170,
            stitched_event_count=160,
            high_confidence_stitch_count=120,
            surface_counts={"tickets": 120, "docs": 60},
            readiness_label="ready",
            ready_for_world_modeling=True,
            notes=["ready"],
        ),
    )

    result = runner.invoke(
        app,
        [
            "twin",
            "onboard",
            "--root",
            str(root),
            "--org",
            "Acme",
            "--domain",
            "acme.ai",
            "--provider",
            "github",
            "--provider",
            "clickup",
            "--filter",
            "github:repo=acme/platform",
            "--filter",
            "clickup:list_id=list-1",
        ],
    )

    assert result.exit_code == 0, result.output
    payload = json.loads(result.output)
    assert payload["timeline"]["readiness"]["ready_for_world_modeling"] is True
    assert payload["capture"]["providers"][0]["filters"]["repo"] == "acme/platform"
    kwargs = captured["kwargs"]
    assert kwargs["organization_name"] == "Acme"
    configs = kwargs["provider_configs"]
    assert len(configs) == 2
    assert configs[0].provider == "github"
    assert configs[1].filters["list_id"] == "list-1"


def test_twin_cli_onboard_smoke_writes_canonical_timeline_sidecars(
    tmp_path: Path,
) -> None:
    runner = CliRunner()
    root = tmp_path / "dispatch_onboarded"
    gmail_zip = tmp_path / "dispatch-gmail.zip"
    notion_zip = tmp_path / "dispatch-notion.zip"

    _write_gmail_takeout_zip(gmail_zip)
    _write_notion_export_zip(notion_zip)

    result = runner.invoke(
        app,
        [
            "twin",
            "onboard",
            "--root",
            str(root),
            "--org",
            "Dispatch",
            "--domain",
            "dispatch.ai",
            "--provider",
            "gmail",
            "--provider",
            "notion",
            "--base-url",
            f"gmail={gmail_zip}",
            "--base-url",
            f"notion={notion_zip}",
        ],
    )

    assert result.exit_code == 0, result.output
    payload = json.loads(result.output)
    assert payload["bundle"]["organization_name"] == "Dispatch"
    assert payload["timeline"]["readiness"]["available"] is True
    assert payload["timeline"]["readiness"]["event_count"] >= 3
    assert (root / "context_snapshot.json").exists()
    assert (root / "canonical_events.jsonl").exists()
    assert (root / "canonical_event_index.json").exists()

    timeline_result = runner.invoke(
        app,
        [
            "context",
            "timeline",
            "--root",
            str(root),
            "--limit",
            "5",
            "--format",
            "plain",
        ],
    )
    assert timeline_result.exit_code == 0, timeline_result.output
    assert "Dispatch" in timeline_result.output

    readiness_result = runner.invoke(
        app,
        [
            "context",
            "readiness",
            "--root",
            str(root),
            "--format",
            "plain",
        ],
    )
    assert readiness_result.exit_code == 0, readiness_result.output
    assert "Readiness:" in readiness_result.output
    assert "World model:" in readiness_result.output


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


def _write_gmail_takeout_zip(path: Path) -> None:
    export_root = path.parent / "gmail_export" / "Takeout" / "Mail"
    export_root.mkdir(parents=True)
    mbox_path = export_root / "All mail Including Spam and Trash.mbox"
    mbox_path.write_text(
        "\n".join(
            [
                "From founder@dispatch.ai Mon Mar 10 10:00:00 2025",
                "From: founder@dispatch.ai",
                "To: ops@dispatch.ai",
                "Subject: Weekly sync",
                "Date: Mon, 10 Mar 2025 10:00:00 +0000",
                "Message-ID: <dispatch-1@dispatch.ai>",
                "",
                "Weekly notes.",
                "",
                "From ops@dispatch.ai Mon Mar 10 11:00:00 2025",
                "From: ops@dispatch.ai",
                "To: founder@dispatch.ai",
                "Subject: Re: Weekly sync",
                "Date: Mon, 10 Mar 2025 11:00:00 +0000",
                "Message-ID: <dispatch-2@dispatch.ai>",
                "In-Reply-To: <dispatch-1@dispatch.ai>",
                "References: <dispatch-1@dispatch.ai>",
                "",
                "Follow-up actions.",
                "",
            ]
        ),
        encoding="utf-8",
    )
    with zipfile.ZipFile(path, "w") as archive:
        archive.write(
            mbox_path,
            arcname="Takeout/Mail/All mail Including Spam and Trash.mbox",
        )


def _write_notion_export_zip(path: Path) -> None:
    notion_root = (
        path.parent / "notion_export" / "Private & Shared" / "Central Dispatch"
    )
    notion_root.mkdir(parents=True)
    (notion_root / "Weekly priorities 74fdd6b1c536473aa670c3373f5e7f89.md").write_text(
        "\n".join(
            [
                "# Weekly priorities - 2024-05-06T16:30:00Z",
                "",
                "Owner: Dispatch Ops",
                "Last edited time: May 6, 2024 9:57 AM",
                "Created time: May 6, 2024 9:57 AM",
                "",
                "Transcript starts here.",
            ]
        ),
        encoding="utf-8",
    )
    (
        notion_root / "Internal Ops Tasks 3d140d68e2a842b582878efb0c8be893.csv"
    ).write_text(
        "\n".join(
            [
                "Name,Assign,Status",
                "Weekly cron for LLM reports based on GH,Robb Chen-Ware,Done",
                "New Demo Video,,Not started",
            ]
        ),
        encoding="utf-8",
    )

    inner_zip = path.parent / "dispatch_inner.zip"
    with zipfile.ZipFile(inner_zip, "w") as archive:
        for file_path in sorted((path.parent / "notion_export").rglob("*")):
            if not file_path.is_file():
                continue
            archive.write(
                file_path,
                arcname=str(file_path.relative_to(path.parent / "notion_export")),
            )

    with zipfile.ZipFile(path, "w") as archive:
        archive.write(inner_zip, arcname="Dispatch-Export.zip")


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
