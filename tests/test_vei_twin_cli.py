from __future__ import annotations

import json
from pathlib import Path

from typer.testing import CliRunner

from vei.cli.vei import app
from vei.context.models import ContextSnapshot, ContextSourceResult


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
    assert status_payload["workspace_name"]
    assert status_payload["gateway"]["surfaces"][0]["name"] == "slack"


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
