"""Tests for the quickstart workspace -> what-if context_snapshot bridge."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from vei.context.api import build_canonical_history_readiness, canonical_history_paths
from vei.whatif.api import (
    export_workspace_history_snapshot,
    load_world,
    materialize_episode,
    search_events,
)


def _write_blueprint(workspace_root: Path) -> None:
    sources_dir = workspace_root / "sources"
    sources_dir.mkdir(parents=True, exist_ok=True)
    blueprint = {
        "name": "test_demo.blueprint",
        "title": "Test Demo Co",
        "capability_graphs": {
            "organization_name": "Test Demo Co",
            "organization_domain": "demo.example.com",
            "comm_graph": {
                "slack_channels": [
                    {
                        "channel": "#ops",
                        "messages": [
                            {
                                "ts": "1712000000.000100",
                                "user": "alice.ops",
                                "text": "VIP outage just landed",
                                "thread_ts": None,
                            },
                            {
                                "ts": "1712000060.000200",
                                "user": "bob.dispatch",
                                "text": "On it, dispatching tech",
                                "thread_ts": "1712000000.000100",
                            },
                        ],
                    }
                ],
                "mail_threads": [
                    {
                        "thread_id": "MAIL-1",
                        "title": "Urgent: outage at site",
                        "category": "customer",
                        "messages": [
                            {
                                "from_address": "vip@customer.example.com",
                                "to_address": "support@demo.example.com",
                                "subject": "Outage",
                                "body_text": "Our site is down.",
                                "time_ms": 1712000900000,
                                "unread": True,
                            }
                        ],
                    }
                ],
            },
            "doc_graph": {
                "documents": [
                    {
                        "doc_id": "DOC-1",
                        "title": "Runbook",
                        "body": "Standard operating procedure.",
                        "tags": ["runbook"],
                    }
                ]
            },
            "work_graph": {
                "tickets": [
                    {
                        "ticket_id": "TKT-1",
                        "title": "VIP outage triage",
                        "status": "open",
                        "assignee": "bob.dispatch",
                        "description": "Stabilize the site.",
                    }
                ]
            },
        },
    }
    (sources_dir / "blueprint_asset.json").write_text(
        json.dumps(blueprint), encoding="utf-8"
    )


def _write_run_trace(workspace_root: Path) -> None:
    run_root = workspace_root / "runs" / "workflow_baseline"
    artifacts_dir = run_root / "artifacts"
    artifacts_dir.mkdir(parents=True, exist_ok=True)
    (run_root / "run_manifest.json").write_text(
        json.dumps(
            {
                "run_id": "workflow_baseline",
                "runner": "workflow",
                "workflow_variant": "service_day_collision",
                "started_at": "2026-04-18T18:00:00Z",
                "completed_at": "2026-04-18T18:00:05Z",
            }
        ),
        encoding="utf-8",
    )
    (artifacts_dir / "trace.jsonl").write_text(
        "\n".join(
            [
                json.dumps(
                    {
                        "type": "call",
                        "tool": "service_ops.assign_dispatch",
                        "args": {
                            "work_order_id": "WO-CFS-100",
                            "appointment_id": "APT-CFS-100",
                            "technician_id": "TECH-CFS-02",
                            "note": "Backup controls technician assigned for CFS-100.",
                        },
                        "response": {
                            "work_order_id": "WO-CFS-100",
                            "technician_id": "TECH-CFS-02",
                            "dispatch_status": "assigned",
                        },
                        "time_ms": 1200,
                    }
                ),
                json.dumps(
                    {
                        "type": "call",
                        "tool": "docs.update",
                        "args": {
                            "doc_id": "DOC-1",
                            "body": "CFS-100 dispatch and billing notes synchronized.",
                        },
                        "response": {
                            "doc_id": "DOC-1",
                            "status": "ACTIVE",
                        },
                        "time_ms": 2200,
                    }
                ),
            ]
        )
        + "\n",
        encoding="utf-8",
    )


def test_export_writes_multi_source_context_snapshot(tmp_path: Path) -> None:
    workspace = tmp_path / "demo_workspace"
    workspace.mkdir()
    _write_blueprint(workspace)

    out = export_workspace_history_snapshot(workspace)

    assert out == workspace / "context_snapshot.json"
    payload = json.loads(out.read_text(encoding="utf-8"))
    providers = sorted(s["provider"] for s in payload["sources"])
    assert providers == ["google", "jira", "mail_archive", "slack"]
    assert payload["organization_name"] == "Test Demo Co"
    assert payload["metadata"]["snapshot_role"] == "company_history_bundle"
    history_paths = canonical_history_paths(out)
    assert history_paths.events_path.exists()
    assert history_paths.index_path.exists()
    readiness = build_canonical_history_readiness(out)
    assert readiness.available is True
    assert readiness.event_count > 0


def test_exported_snapshot_loads_as_company_history_world(tmp_path: Path) -> None:
    workspace = tmp_path / "demo_workspace"
    workspace.mkdir()
    _write_blueprint(workspace)
    out = export_workspace_history_snapshot(workspace)

    world = load_world(source="company_history", source_dir=out)

    assert world.summary.organization_name == "Test Demo Co"
    assert world.summary.event_count > 0
    assert any(e.surface == "slack" for e in world.events)
    assert any(e.surface == "mail" for e in world.events)


def test_exported_snapshot_supports_search_and_materialize(tmp_path: Path) -> None:
    workspace = tmp_path / "demo_workspace"
    workspace.mkdir()
    _write_blueprint(workspace)
    out = export_workspace_history_snapshot(workspace)
    world = load_world(source="company_history", source_dir=out)

    result = search_events(world, query="outage", limit=10)
    assert result.match_count >= 1

    branch_event = result.matches[0].event
    materialize_root = tmp_path / "whatif_workspace"
    mat = materialize_episode(
        world, root=materialize_root, event_id=branch_event.event_id
    )

    assert (materialize_root / "context_snapshot.json").exists()
    assert (materialize_root / "episode_manifest.json").exists()
    assert mat.thread_id == branch_event.thread_id


def test_export_rejects_workspace_without_blueprint(tmp_path: Path) -> None:
    workspace = tmp_path / "empty_workspace"
    workspace.mkdir()

    with pytest.raises(ValueError, match="blueprint_asset.json"):
        export_workspace_history_snapshot(workspace)


def test_export_rejects_blueprint_without_supported_sources(tmp_path: Path) -> None:
    workspace = tmp_path / "bare_workspace"
    sources_dir = workspace / "sources"
    sources_dir.mkdir(parents=True)
    (sources_dir / "blueprint_asset.json").write_text(
        json.dumps(
            {
                "capability_graphs": {
                    "organization_name": "Bare Co",
                    "comm_graph": {},
                    "work_graph": {},
                    "doc_graph": {},
                }
            }
        ),
        encoding="utf-8",
    )

    with pytest.raises(
        ValueError, match="did not yield any what-if compatible sources"
    ):
        export_workspace_history_snapshot(workspace)


def test_export_writes_to_custom_output_path(tmp_path: Path) -> None:
    workspace = tmp_path / "demo_workspace"
    workspace.mkdir()
    _write_blueprint(workspace)
    custom_out = tmp_path / "custom_dir" / "snapshot.json"

    out = export_workspace_history_snapshot(workspace, output_path=custom_out)

    assert out == custom_out
    assert custom_out.exists()
    payload = json.loads(custom_out.read_text(encoding="utf-8"))
    assert payload["organization_name"] == "Test Demo Co"


def test_export_merges_workspace_run_activity_into_snapshot(tmp_path: Path) -> None:
    workspace = tmp_path / "demo_workspace"
    workspace.mkdir()
    _write_blueprint(workspace)
    _write_run_trace(workspace)

    out = export_workspace_history_snapshot(workspace)

    payload = json.loads(out.read_text(encoding="utf-8"))
    slack_source = next(
        source for source in payload["sources"] if source["provider"] == "slack"
    )
    jira_source = next(
        source for source in payload["sources"] if source["provider"] == "jira"
    )
    google_source = next(
        source for source in payload["sources"] if source["provider"] == "google"
    )

    slack_messages = [
        message["text"]
        for channel in slack_source["data"]["channels"]
        for message in channel.get("messages", [])
    ]
    assert any("WO-CFS-100 reassigned." in text for text in slack_messages)

    ticket_comments = [
        comment["body"]
        for issue in jira_source["data"]["issues"]
        for comment in issue.get("comments", [])
    ]
    assert any("WO-CFS-100 reassigned." in body for body in ticket_comments)

    doc_comments = [
        comment["body"]
        for document in google_source["data"]["documents"]
        for comment in document.get("comments", [])
    ]
    assert any("DOC-1 updated." in body for body in doc_comments)
