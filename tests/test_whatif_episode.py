from __future__ import annotations

import json
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
from typer.testing import CliRunner

from vei.cli.vei import app as cli_app
from vei.data.models import VEIDataset
from vei.twin import load_customer_twin
from vei.whatif import (
    load_world,
    load_episode_manifest,
    materialize_episode,
    replay_episode_baseline,
    search_events,
)
from vei.whatif.counterfactual import (
    estimate_counterfactual_delta,
    run_llm_counterfactual,
)
from vei.whatif.decision import build_decision_scene, build_saved_decision_scene
from vei.llm.providers import PlanResult, PlanUsage

materialize_episode_module = materialize_episode
load_episode_manifest_module = load_episode_manifest
replay_episode_baseline_module = replay_episode_baseline
build_decision_scene_module = build_decision_scene
build_saved_decision_scene_module = build_saved_decision_scene
run_llm_counterfactual_module = run_llm_counterfactual
run_ejepa_proxy_counterfactual_module = estimate_counterfactual_delta


def _write_rosetta_fixture(root: Path) -> None:
    root.mkdir(parents=True, exist_ok=True)
    metadata_rows = [
        {
            "event_id": "evt-001",
            "timestamp": "2001-05-01T10:00:00Z",
            "actor_id": "vince.kaminski@enron.com",
            "target_id": "sara.shackleton@enron.com",
            "event_type": "message",
            "thread_task_id": "thr-legal-trading",
            "artifacts": json.dumps(
                {
                    "subject": "Gas Position Limits",
                    "norm_subject": "gas position limits",
                    "to_recipients": ["sara.shackleton@enron.com"],
                    "to_count": 1,
                    "consult_legal_specialist": True,
                    "custodian_id": "kaminski-v",
                }
            ),
        },
        {
            "event_id": "evt-002",
            "timestamp": "2001-05-01T10:00:01Z",
            "actor_id": "sara.shackleton@enron.com",
            "target_id": "mark.taylor@enron.com",
            "event_type": "reply",
            "thread_task_id": "thr-legal-trading",
            "artifacts": json.dumps(
                {
                    "subject": "Gas Position Limits",
                    "norm_subject": "gas position limits",
                    "to_recipients": ["mark.taylor@enron.com"],
                    "to_count": 1,
                    "consult_trading_specialist": True,
                    "is_forward": True,
                    "custodian_id": "shackleton-s",
                }
            ),
        },
        {
            "event_id": "evt-003",
            "timestamp": "2001-05-01T10:00:02Z",
            "actor_id": "mark.taylor@enron.com",
            "target_id": "ops.review@enron.com",
            "event_type": "assignment",
            "thread_task_id": "thr-legal-trading",
            "artifacts": json.dumps(
                {
                    "subject": "Gas Position Limits",
                    "norm_subject": "gas position limits",
                    "to_recipients": ["ops.review@enron.com"],
                    "to_count": 1,
                    "custodian_id": "taylor-m",
                }
            ),
        },
        {
            "event_id": "evt-004",
            "timestamp": "2001-05-01T10:00:03Z",
            "actor_id": "assistant@enron.com",
            "target_id": "kenneth.lay@enron.com",
            "event_type": "escalation",
            "thread_task_id": "thr-exec",
            "artifacts": json.dumps(
                {
                    "subject": "Escalate to leadership",
                    "to_recipients": ["kenneth.lay@enron.com"],
                    "to_count": 1,
                    "is_escalation": True,
                }
            ),
        },
        {
            "event_id": "evt-005",
            "timestamp": "2001-05-01T10:00:04Z",
            "actor_id": "jeff.skilling@enron.com",
            "target_id": "outside@lawfirm.com",
            "event_type": "message",
            "thread_task_id": "thr-external",
            "artifacts": json.dumps(
                {
                    "subject": "Draft term sheet",
                    "to_recipients": ["outside@lawfirm.com"],
                    "to_count": 1,
                    "has_attachment_reference": True,
                }
            ),
        },
    ]
    content_rows = [
        {"event_id": "evt-001", "content": "Need legal eyes on this position update."},
        {"event_id": "evt-002", "content": "Forwarding with trading context attached."},
        {"event_id": "evt-003", "content": "Assigning ops review before we proceed."},
        {"event_id": "evt-004", "content": "Escalating to executive review."},
        {"event_id": "evt-005", "content": "External draft attached for review."},
    ]
    pq.write_table(
        pa.Table.from_pylist(metadata_rows),
        root / "enron_rosetta_events_metadata.parquet",
    )
    pq.write_table(
        pa.Table.from_pylist(content_rows),
        root / "enron_rosetta_events_content.parquet",
    )


def _write_mail_archive_fixture(root: Path) -> Path:
    root.mkdir(parents=True, exist_ok=True)
    archive_path = root / "context_snapshot.json"
    archive_path.write_text(
        json.dumps(
            {
                "organization_name": "Py Corp",
                "organization_domain": "pycorp.example.com",
                "captured_at": "2026-03-01T09:15:00Z",
                "threads": [
                    {
                        "thread_id": "py-legal-001",
                        "subject": "Pricing addendum",
                        "category": "historical",
                        "messages": [
                            {
                                "message_id": "py-msg-001",
                                "from": "emma@pycorp.example.com",
                                "to": "legal@pycorp.example.com",
                                "subject": "Pricing addendum",
                                "body_text": "Please review before we send this draft to Redwood.",
                                "timestamp": "2026-03-01T09:00:00Z",
                            },
                            {
                                "message_id": "py-msg-002",
                                "from": "legal@pycorp.example.com",
                                "to": "emma@pycorp.example.com",
                                "subject": "Re: Pricing addendum",
                                "body_text": "Hold for one markup round. Counsel wants one more pass.",
                                "timestamp": "2026-03-01T09:05:00Z",
                            },
                            {
                                "message_id": "py-msg-003",
                                "from": "emma@pycorp.example.com",
                                "to": "partner@redwoodcapital.com",
                                "subject": "Pricing addendum",
                                "body_text": "Sharing the draft addendum now.",
                                "timestamp": "2026-03-01T09:10:00Z",
                                "has_attachment_reference": True,
                            },
                        ],
                    }
                ],
                "actors": [
                    {
                        "actor_id": "emma@pycorp.example.com",
                        "email": "emma@pycorp.example.com",
                        "display_name": "Emma Rowan",
                    },
                    {
                        "actor_id": "legal@pycorp.example.com",
                        "email": "legal@pycorp.example.com",
                        "display_name": "Legal Team",
                    },
                ],
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    return archive_path


def _write_company_history_fixture(root: Path) -> Path:
    root.mkdir(parents=True, exist_ok=True)
    snapshot_path = root / "context_snapshot.json"
    snapshot_path.write_text(
        json.dumps(
            {
                "version": "1",
                "organization_name": "Py Corp",
                "organization_domain": "pycorp.example.com",
                "captured_at": "2026-03-01T10:15:00Z",
                "sources": [
                    {
                        "provider": "slack",
                        "captured_at": "2026-03-01T10:15:00Z",
                        "status": "ok",
                        "record_counts": {"channels": 1, "messages": 3, "users": 2},
                        "data": {
                            "channels": [
                                {
                                    "channel": "#deal-desk",
                                    "channel_id": "C001",
                                    "unread": 0,
                                    "messages": [
                                        {
                                            "ts": "2026-03-01T09:00:00Z",
                                            "user": "emma@pycorp.example.com",
                                            "text": "Need a clean internal review thread for LEGAL-7 before we update Redwood.",
                                        },
                                        {
                                            "ts": "2026-03-01T09:05:00Z",
                                            "user": "legal@pycorp.example.com",
                                            "text": "Hold LEGAL-7 internally until legal signs off.",
                                            "thread_ts": "2026-03-01T09:00:00Z",
                                        },
                                        {
                                            "ts": "2026-03-01T09:10:00Z",
                                            "user": "emma@pycorp.example.com",
                                            "text": "I will send Redwood a short LEGAL-7 status note after that review.",
                                            "thread_ts": "2026-03-01T09:00:00Z",
                                        },
                                    ],
                                }
                            ],
                            "users": [
                                {
                                    "id": "U001",
                                    "name": "emma",
                                    "real_name": "Emma Rowan",
                                    "email": "emma@pycorp.example.com",
                                },
                                {
                                    "id": "U002",
                                    "name": "legal",
                                    "real_name": "Legal Team",
                                    "email": "legal@pycorp.example.com",
                                },
                            ],
                        },
                    },
                    {
                        "provider": "jira",
                        "captured_at": "2026-03-01T10:15:00Z",
                        "status": "ok",
                        "record_counts": {"issues": 1, "projects": 1},
                        "data": {
                            "issues": [
                                {
                                    "ticket_id": "LEGAL-7",
                                    "title": "LEGAL-7 pricing addendum review",
                                    "status": "in_progress",
                                    "assignee": "emma@pycorp.example.com",
                                    "description": "Check LEGAL-7 pricing addendum before any outside update.",
                                    "updated": "2026-03-01T10:00:00Z",
                                    "comments": [
                                        {
                                            "id": "c1",
                                            "author": "legal@pycorp.example.com",
                                            "body": "Need one more LEGAL-7 markup pass before we send anything outside.",
                                            "created": "2026-03-01T09:02:00Z",
                                        },
                                        {
                                            "id": "c2",
                                            "author": "emma@pycorp.example.com",
                                            "body": "Holding the LEGAL-7 response until the markup is done.",
                                            "created": "2026-03-01T09:06:00Z",
                                        },
                                    ],
                                }
                            ],
                            "projects": [{"key": "LEGAL", "name": "Legal"}],
                        },
                    },
                    {
                        "provider": "google",
                        "captured_at": "2026-03-01T10:15:00Z",
                        "status": "ok",
                        "record_counts": {"documents": 1},
                        "data": {
                            "documents": [
                                {
                                    "doc_id": "DOC-LEGAL-7",
                                    "title": "LEGAL-7 markup tracker",
                                    "mime_type": "application/vnd.google-apps.document",
                                    "body": "Markup notes for LEGAL-7 pricing addendum review.",
                                }
                            ]
                        },
                    },
                    {
                        "provider": "salesforce",
                        "captured_at": "2026-03-01T10:15:00Z",
                        "status": "ok",
                        "record_counts": {"deals": 1},
                        "data": {
                            "deals": [
                                {
                                    "id": "DEAL-LEGAL-7",
                                    "name": "LEGAL-7 Redwood renewal",
                                    "stage": "legal_review",
                                    "owner": "emma@pycorp.example.com",
                                    "amount": 240000,
                                }
                            ]
                        },
                    },
                ],
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    return snapshot_path


def test_materialize_episode_builds_mail_only_workspace_and_replay(
    tmp_path: Path,
) -> None:
    rosetta_dir = tmp_path / "rosetta"
    _write_rosetta_fixture(rosetta_dir)
    world = load_world(source="enron", rosetta_dir=rosetta_dir)

    workspace_root = tmp_path / "episode"
    materialization = materialize_episode(
        world,
        root=workspace_root,
        thread_id="thr-legal-trading",
    )
    manifest = load_episode_manifest(workspace_root)
    bundle = load_customer_twin(workspace_root)
    dataset = VEIDataset.model_validate_json(
        materialization.baseline_dataset_path.read_text(encoding="utf-8")
    )
    replay = replay_episode_baseline(workspace_root, tick_ms=1500)

    assert materialization.history_message_count == 1
    assert materialization.future_event_count == 2
    assert manifest.thread_id == "thr-legal-trading"
    assert manifest.branch_event_id == "evt-002"
    assert manifest.branch_event.actor_id == "sara.shackleton@enron.com"
    assert "Forwarding with trading context attached." in manifest.branch_event.snippet
    assert [surface.name for surface in bundle.gateway.surfaces] == ["graph"]
    assert bundle.organization_domain == "enron.com"
    assert len(dataset.events) == 2
    assert dataset.events[0].payload["thread_id"] == "thr-legal-trading"
    assert replay.scheduled_event_count == 2
    assert replay.delivered_event_count == 2
    assert replay.inbox_count >= 3


def test_load_mail_archive_world_and_materialize_episode(
    tmp_path: Path,
) -> None:
    archive_path = _write_mail_archive_fixture(tmp_path / "mail_archive")
    world = load_world(source="auto", source_dir=archive_path)

    search_result = search_events(world, query="Redwood draft", limit=5)
    workspace_root = tmp_path / "py_episode"
    materialization = materialize_episode(
        world,
        root=workspace_root,
        thread_id="py-legal-001",
    )
    manifest = load_episode_manifest(workspace_root)
    bundle = load_customer_twin(workspace_root)
    replay = replay_episode_baseline(workspace_root, tick_ms=400_000)

    assert world.source == "mail_archive"
    assert world.summary.organization_name == "Py Corp"
    assert world.summary.organization_domain == "pycorp.example.com"
    assert world.summary.thread_count == 1
    assert search_result.match_count >= 1
    assert materialization.organization_name == "Py Corp"
    assert materialization.organization_domain == "pycorp.example.com"
    assert (workspace_root / "context_snapshot.json").exists()
    assert manifest.source == "mail_archive"
    assert manifest.branch_event_id == "py-msg-002"
    assert bundle.organization_name == "Py Corp"
    assert bundle.organization_domain == "pycorp.example.com"
    assert replay.scheduled_event_count == 2
    assert replay.delivered_event_count == 2


def test_load_company_history_world_materialize_slack_branch_and_replay(
    tmp_path: Path,
) -> None:
    snapshot_path = _write_company_history_fixture(tmp_path / "company_history")
    world = load_world(source="auto", source_dir=snapshot_path)

    slack_thread = next(thread for thread in world.threads if thread.surface == "slack")
    search_result = search_events(world, query="legal signs off", limit=5)
    workspace_root = tmp_path / "company_history_episode"
    materialization = materialize_episode(
        world,
        root=workspace_root,
        thread_id=slack_thread.thread_id,
    )
    manifest = load_episode_manifest(workspace_root)
    bundle = load_customer_twin(workspace_root)
    replay = replay_episode_baseline(workspace_root, tick_ms=400_000)
    dataset = VEIDataset.model_validate_json(
        materialization.baseline_dataset_path.read_text(encoding="utf-8")
    )

    assert world.source == "company_history"
    assert {thread.surface for thread in world.threads} >= {"slack", "tickets"}
    assert search_result.match_count >= 1
    assert materialization.surface == "slack"
    assert manifest.source == "company_history"
    assert manifest.surface == "slack"
    assert manifest.history_preview[0].surface == "slack"
    assert (workspace_root / "context_snapshot.json").exists()
    assert bundle.organization_name == "Py Corp"
    assert replay.surface == "slack"
    assert replay.visible_item_count >= 2
    assert dataset.events[0].channel == "slack"


def test_load_company_history_world_materialize_ticket_branch_and_replay(
    tmp_path: Path,
) -> None:
    snapshot_path = _write_company_history_fixture(tmp_path / "company_history_tickets")
    world = load_world(source="company_history", source_dir=snapshot_path)

    ticket_thread = next(
        thread for thread in world.threads if thread.surface == "tickets"
    )
    workspace_root = tmp_path / "company_history_ticket_episode"
    materialization = materialize_episode(
        world,
        root=workspace_root,
        thread_id=ticket_thread.thread_id,
    )
    replay = replay_episode_baseline(workspace_root, tick_ms=400_000)
    dataset = VEIDataset.model_validate_json(
        materialization.baseline_dataset_path.read_text(encoding="utf-8")
    )

    assert materialization.surface == "tickets"
    assert replay.surface == "tickets"
    assert replay.visible_item_count >= 1
    assert dataset.events[0].channel == "tickets"


def test_materialize_episode_auto_selects_thread_from_situation_graph(
    tmp_path: Path,
) -> None:
    snapshot_path = _write_company_history_fixture(tmp_path / "company_history_auto")
    world = load_world(source="company_history", source_dir=snapshot_path)

    workspace_root = tmp_path / "company_history_auto_episode"
    materialization = materialize_episode(world, root=workspace_root)

    assert materialization.thread_id == "slack:#deal-desk:1772355600000"
    assert materialization.situation_context is not None
    assert {
        thread.surface for thread in materialization.situation_context.related_threads
    } >= {
        "docs",
        "crm",
    }


def test_materialize_episode_defaults_to_generic_archive_domain_when_missing(
    tmp_path: Path,
) -> None:
    root = tmp_path / "nameless_archive"
    root.mkdir(parents=True, exist_ok=True)
    archive_path = root / "context_snapshot.json"
    archive_path.write_text(
        json.dumps(
            {
                "threads": [
                    {
                        "thread_id": "plain-001",
                        "subject": "Plain text thread",
                        "messages": [
                            {
                                "message_id": "plain-msg-001",
                                "from": "Legal Team",
                                "to": "Operations",
                                "subject": "Plain text thread",
                                "body_text": "Please hold this internally.",
                                "timestamp": "2026-03-03T08:00:00Z",
                            },
                            {
                                "message_id": "plain-msg-002",
                                "from": "Operations",
                                "to": "Legal Team",
                                "subject": "Re: Plain text thread",
                                "body_text": "Holding for review.",
                                "timestamp": "2026-03-03T08:05:00Z",
                            },
                        ],
                    }
                ]
            },
            indent=2,
        ),
        encoding="utf-8",
    )

    world = load_world(source="auto", source_dir=archive_path)
    materialization = materialize_episode(
        world,
        root=tmp_path / "plain_episode",
        thread_id="plain-001",
    )

    assert materialization.organization_domain == "archive.local"


def test_vei_whatif_cli_explore_and_open(tmp_path: Path) -> None:
    rosetta_dir = tmp_path / "rosetta"
    _write_rosetta_fixture(rosetta_dir)
    runner = CliRunner()

    explore_result = runner.invoke(
        cli_app,
        [
            "whatif",
            "explore",
            "--rosetta-dir",
            str(rosetta_dir),
            "--scenario",
            "external_dlp",
        ],
    )
    assert explore_result.exit_code == 0, explore_result.output
    explore_payload = json.loads(explore_result.output)
    assert explore_payload["affected_thread_count"] == 1
    assert explore_payload["matched_event_count"] == 1

    workspace_root = tmp_path / "episode_cli"
    open_result = runner.invoke(
        cli_app,
        [
            "whatif",
            "open",
            "--rosetta-dir",
            str(rosetta_dir),
            "--root",
            str(workspace_root),
            "--thread-id",
            "thr-legal-trading",
        ],
    )
    assert open_result.exit_code == 0, open_result.output
    open_payload = json.loads(open_result.output)
    assert open_payload["future_event_count"] == 2
    assert open_payload["branch_event"]["event_id"] == "evt-002"

    replay_result = runner.invoke(
        cli_app,
        [
            "whatif",
            "replay",
            "--root",
            str(workspace_root),
            "--tick-ms",
            "1500",
        ],
    )
    assert replay_result.exit_code == 0, replay_result.output
    replay_payload = json.loads(replay_result.output)
    assert replay_payload["scheduled_event_count"] == 2
    assert replay_payload["delivered_event_count"] == 2

    events_result = runner.invoke(
        cli_app,
        [
            "whatif",
            "events",
            "--rosetta-dir",
            str(rosetta_dir),
            "--actor",
            "jeff.skilling",
            "--query",
            "draft term sheet",
            "--flagged-only",
        ],
    )
    assert events_result.exit_code == 0, events_result.output
    events_payload = json.loads(events_result.output)
    assert events_payload["match_count"] == 1
    assert events_payload["matches"][0]["event"]["event_id"] == "evt-005"


def test_materialize_episode_can_branch_from_explicit_event_id(tmp_path: Path) -> None:
    rosetta_dir = tmp_path / "rosetta"
    _write_rosetta_fixture(rosetta_dir)
    world = load_world(source="enron", rosetta_dir=rosetta_dir)

    workspace_root = tmp_path / "episode_by_event"
    materialization = materialize_episode(
        world,
        root=workspace_root,
        event_id="evt-005",
    )

    assert materialization.thread_id == "thr-external"
    assert materialization.branch_event_id == "evt-005"
    assert materialization.history_message_count == 0
    assert materialization.future_event_count == 1


def test_split_modules_support_episode_scene_and_counterfactual_flow(
    tmp_path: Path,
    monkeypatch,
) -> None:
    rosetta_dir = tmp_path / "rosetta_split"
    _write_rosetta_fixture(rosetta_dir)
    world = load_world(source="enron", rosetta_dir=rosetta_dir)

    async def fake_plan_once_with_usage(**_: object) -> PlanResult:
        return PlanResult(
            plan={
                "tool": "emit_counterfactual",
                "args": {
                    "summary": "Sara keeps the thread inside Enron for one more review.",
                    "messages": [
                        {
                            "actor_id": "sara.shackleton@enron.com",
                            "to": "mark.taylor@enron.com",
                            "subject": "Re: Gas Position Limits",
                            "body_text": "Please keep this inside until compliance clears it.",
                            "delay_ms": 1000,
                        }
                    ],
                },
            },
            usage=PlanUsage(provider="openai", model="gpt-5"),
        )

    monkeypatch.setattr(
        "vei.whatif.counterfactual.providers.plan_once_with_usage",
        fake_plan_once_with_usage,
    )

    workspace_root = tmp_path / "split_episode"
    materialization = materialize_episode_module(
        world,
        root=workspace_root,
        thread_id="thr-legal-trading",
    )
    manifest = load_episode_manifest_module(workspace_root)
    replay = replay_episode_baseline_module(workspace_root, tick_ms=400_000)
    live_scene = build_decision_scene_module(world, thread_id="thr-legal-trading")
    saved_scene = build_saved_decision_scene_module(workspace_root)
    llm_result = run_llm_counterfactual_module(
        workspace_root,
        prompt="Hold the forward and keep this internal.",
    )
    forecast_result = run_ejepa_proxy_counterfactual_module(
        workspace_root,
        prompt="Hold the forward and keep this internal.",
    )

    assert materialization.branch_event_id == "evt-002"
    assert manifest.branch_event_id == "evt-002"
    assert replay.delivered_event_count >= 1
    assert live_scene.branch_event_id == "evt-002"
    assert saved_scene.branch_event_id == "evt-002"
    assert llm_result.status == "ok"
    assert llm_result.delivered_event_count == 1
    assert forecast_result.status == "ok"
    assert forecast_result.business_state_change is not None
