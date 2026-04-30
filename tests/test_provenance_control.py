from __future__ import annotations

import json
from pathlib import Path

from typer.testing import CliRunner

from vei.cli.vei import app
from vei.events.api import (
    ActorRef,
    CanonicalEvent,
    EventContext,
    ObjectRef,
    WorkspaceEventStore,
    build_llm_call_event,
    build_tool_call_event,
    drain_spine,
)
from vei.ingest.agent_activity.agent_activity_jsonl import AgentActivityJsonlAdapter
from vei.ingest.agent_activity.api import (
    ingest_agent_activity,
    load_workspace_canonical_events,
)
from vei.ingest.agent_activity.mcp_transcript import McpTranscriptAdapter
from vei.ingest.agent_activity.openai_org import OpenAIOrgAdapter
from vei.provenance.api import access_review, blast_radius, build_activity_graph
from vei.provenance.api import replay_policy
from vei.provenance.exporters.otel_genai import export_otel_genai
from vei.router.api import create_router


def test_provenance_event_builders_preserve_canonical_v1() -> None:
    envelope_fields = set(CanonicalEvent.model_fields)
    event = build_tool_call_event(
        kind="tool.call.completed",
        tool_name="slack.post_message",
        actor_ref=ActorRef(actor_id="agent-1"),
        object_refs=[ObjectRef(object_id="C1", domain="comm_graph", kind="channel")],
        args={"text": "secret"},
        response={"ok": True},
        source_id="unit",
        context=EventContext(agent_id="agent-1", trace_id="trace-1"),
        links=[{"kind": "completed_by", "event_id": "evt-request"}],
    )

    assert set(CanonicalEvent.model_fields) == envelope_fields
    assert event.schema_version == 1
    assert event.kind == "tool.call.completed"
    assert event.delta is not None
    assert "args_handle" in event.delta.data
    assert "args" not in event.delta.data
    assert event.delta.data["context"]["agent_id"] == "agent-1"
    assert event.delta.data["links"] == [
        {"kind": "completed_by", "event_id": "evt-request"}
    ]
    assert event.delta.data["link_refs"] == ["evt-request"]
    assert event.hash

    llm = build_llm_call_event(
        kind="llm.call.completed",
        provider="openai",
        model="gpt-5",
        prompt="private prompt",
        response="private response",
        source_id="unit",
    )
    assert llm.text_handle is not None
    assert llm.delta is not None
    assert "prompt_handle" in llm.delta.data
    assert "private prompt" not in llm.model_dump_json()


def test_agent_activity_jsonl_ingest_is_idempotent_and_reportable(
    tmp_path: Path,
) -> None:
    source = tmp_path / "activity.jsonl"
    source.write_text(
        json.dumps(
            {
                "id": "rec-1",
                "ts_ms": 100,
                "actor_id": "agent-1",
                "tool": "docs.read",
                "args": {"doc_id": "doc-1"},
                "response": {"title": "Plan"},
                "status": "completed",
            }
        )
        + "\n",
        encoding="utf-8",
    )
    workspace = tmp_path / "workspace"
    adapter = AgentActivityJsonlAdapter(source, tenant_id="acme")

    first = ingest_agent_activity(adapter=adapter, workspace=workspace)
    second = ingest_agent_activity(adapter=adapter, workspace=workspace)

    assert first.event_count == 1
    assert second.event_count == 0
    assert second.skipped_duplicate_count == 1
    events = load_workspace_canonical_events(workspace)
    assert len(events) == 1
    graph = build_activity_graph(events)
    assert graph.node_count >= 2
    review = access_review(events, agent_id="agent-1")
    assert review.tools_used == ["docs.read"]
    assert review.touched_objects == ["doc-1"]


def test_workspace_event_store_reads_workspace_spine(tmp_path: Path) -> None:
    workspace = tmp_path / "workspace"
    store = WorkspaceEventStore(workspace, source="router", batch_id="test")
    event = build_tool_call_event(
        kind="tool.call.completed",
        tool_name="docs.read",
        actor_ref=ActorRef(actor_id="agent-1"),
        args={"doc_id": "doc-1"},
        object_refs=[ObjectRef(object_id="doc-1", domain="doc_graph", kind="document")],
        source_id="unit",
    )

    first = store.append(event)
    second = store.append(event)
    events = load_workspace_canonical_events(workspace)

    assert first.event_id == second.event_id
    assert len(events) == 1
    assert store.get(event.event_id) is not None


def test_mcp_transcript_ingest_reconstructs_tool_call(tmp_path: Path) -> None:
    transcript = tmp_path / "mcp.jsonl"
    transcript.write_text(
        "\n".join(
            [
                json.dumps(
                    {
                        "ts_ms": 10,
                        "message": {
                            "jsonrpc": "2.0",
                            "id": "1",
                            "method": "tools/call",
                            "params": {"name": "mail.search", "arguments": {"q": "x"}},
                        },
                    }
                ),
                json.dumps(
                    {
                        "ts_ms": 20,
                        "message": {
                            "jsonrpc": "2.0",
                            "id": "1",
                            "result": {"ok": True},
                        },
                    }
                ),
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    workspace = tmp_path / "workspace"
    result = ingest_agent_activity(
        adapter=McpTranscriptAdapter(transcript), workspace=workspace
    )
    events = load_workspace_canonical_events(workspace)

    assert result.event_count == 2
    assert {event.kind for event in events} == {
        "tool.call.requested",
        "tool.call.completed",
    }
    assert all(
        event.delta.data["source_granularity"] == "transcript"
        for event in events
        if event.delta
    )
    completed = next(event for event in events if event.kind == "tool.call.completed")
    requested = next(event for event in events if event.kind == "tool.call.requested")
    assert completed.delta is not None
    assert completed.delta.data["links"] == [
        {"kind": "completed_by", "event_id": requested.event_id}
    ]
    assert completed.delta.data["link_refs"] == [requested.event_id]


def test_openai_org_usage_stays_aggregate(tmp_path: Path) -> None:
    adapter = OpenAIOrgAdapter(
        records=[
            {
                "id": "bucket-1",
                "start_time": 100,
                "end_time": 200,
                "results": [{"amount": {"value": 1.2}}],
            }
        ]
    )
    workspace = tmp_path / "workspace"
    ingest_agent_activity(adapter=adapter, workspace=workspace)
    events = load_workspace_canonical_events(workspace)

    assert len(events) == 1
    assert events[0].kind == "llm.usage.observed"
    assert events[0].delta is not None
    assert events[0].delta.data["source_granularity"] == "aggregate"
    report = blast_radius(events, anchor_event_id=events[0].event_id)
    assert report.unknowns


def test_cli_ingest_and_provenance_commands(tmp_path: Path) -> None:
    source = tmp_path / "activity.jsonl"
    source.write_text(
        json.dumps({"id": "rec-1", "actor_id": "agent-1", "tool": "docs.read"}) + "\n",
        encoding="utf-8",
    )
    workspace = tmp_path / "workspace"
    runner = CliRunner()

    result = runner.invoke(
        app,
        [
            "ingest",
            "agent-activity",
            "--source",
            "agent_activity_jsonl",
            "--path",
            str(source),
            "--workspace",
            str(workspace),
            "--format",
            "json",
        ],
    )
    assert result.exit_code == 0, result.output

    result = runner.invoke(
        app,
        [
            "provenance",
            "access-review",
            "--agent-id",
            "agent-1",
            "--workspace",
            str(workspace),
            "--format",
            "json",
        ],
    )
    assert result.exit_code == 0, result.output
    assert "docs.read" in result.output


def test_router_dispatch_emits_tool_events() -> None:
    drain_spine()
    router = create_router(seed=1)
    router.call_and_step("vei.state", {})
    events = drain_spine()

    assert "tool.call.requested" in {event.kind for event in events}
    assert "tool.call.completed" in {event.kind for event in events}


def test_router_dispatch_persists_workspace_events(tmp_path: Path) -> None:
    drain_spine()
    router = create_router(seed=1, artifacts_dir=str(tmp_path))
    created = router.call_and_step(
        "docs.create", {"title": "Plan", "body": "Body", "tags": ["test"]}
    )

    events = load_workspace_canonical_events(tmp_path)
    assert {event.kind for event in events} >= {
        "tool.call.requested",
        "tool.call.completed",
    }
    review = access_review(events, agent_id="")
    assert created["doc_id"] in review.touched_objects


def test_policy_replay_uses_policy_evaluator_when_reconstructable() -> None:
    event = build_tool_call_event(
        kind="tool.call.requested",
        tool_name="docs.read",
        actor_ref=ActorRef(actor_id="agent-1"),
        source_id="unit",
    )
    report = replay_policy(
        [event],
        policy={
            "name": "surface-lockdown",
            "governor": {
                "config": {"connector_mode": "sim"},
                "agents": [
                    {
                        "agent_id": "agent-1",
                        "name": "Agent One",
                        "allowed_surfaces": ["mail"],
                    }
                ],
            },
        },
    )

    assert report.hit_count == 1
    assert report.hits[0].replay_decision == "deny"
    assert "surface" in report.hits[0].reason


def test_otel_export_preserves_vei_ids() -> None:
    event = build_tool_call_event(
        kind="tool.call.completed",
        tool_name="mail.search",
        args={"q": "contract"},
        source_id="unit",
        context=EventContext(
            trace_id="trace-1",
            parent_event_id="evt-parent",
            jsonrpc_request_id="1",
            mcp_session_id="sess-1",
        ),
    )
    exported = export_otel_genai([event])
    span = exported["resourceSpans"][0]["scopeSpans"][0]["spans"][0]
    attrs = {
        item["key"]: next(iter(item["value"].values())) for item in span["attributes"]
    }
    assert span["traceId"]
    assert span["spanId"]
    assert span["parentSpanId"]
    assert attrs["vei.event_id"] == event.event_id
    assert attrs["vei.event_hash"] == event.hash
    assert attrs["gen_ai.operation.name"] == "execute_tool"
    assert attrs["gen_ai.tool.name"] == "mail.search"
    assert attrs["jsonrpc.request.id"] == "1"
