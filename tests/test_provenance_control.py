from __future__ import annotations

import json
from pathlib import Path

from typer.testing import CliRunner

from vei.cli.vei import app
from vei.events.api import (
    ActorRef,
    ObjectRef,
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
from vei.provenance.exporters.otel_genai import export_otel_genai
from vei.router.api import create_router


def test_provenance_event_builders_preserve_canonical_v1() -> None:
    event = build_tool_call_event(
        kind="tool.call.completed",
        tool_name="slack.post_message",
        actor_ref=ActorRef(actor_id="agent-1"),
        object_refs=[ObjectRef(object_id="C1", domain="comm_graph", kind="channel")],
        args={"text": "secret"},
        response={"ok": True},
        source_id="unit",
    )

    assert event.schema_version == 1
    assert event.kind == "tool.call.completed"
    assert event.delta is not None
    assert "args_handle" in event.delta.data
    assert "args" not in event.delta.data
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


def test_otel_export_preserves_vei_ids() -> None:
    event = build_tool_call_event(
        kind="tool.call.completed",
        tool_name="mail.search",
        source_id="unit",
    )
    exported = export_otel_genai([event])
    span = exported["resourceSpans"][0]["scopeSpans"][0]["spans"][0]
    attrs = {item["key"]: item["value"]["stringValue"] for item in span["attributes"]}
    assert attrs["vei.event_id"] == event.event_id
    assert attrs["vei.event_hash"] == event.hash
