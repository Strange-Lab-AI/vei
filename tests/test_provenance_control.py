from __future__ import annotations

import json
from pathlib import Path

from fastapi.testclient import TestClient
from typer.testing import CliRunner

from vei.cli.vei import app
from vei.events.api import (
    ActorRef,
    CanonicalEvent,
    EventContext,
    ExecutionPrincipal,
    ObjectRef,
    WorkspaceEventStore,
    build_llm_call_event,
    build_tool_call_event,
    extract_object_refs,
    drain_spine,
)
from vei.ingest.agent_activity.agent_activity_jsonl import AgentActivityJsonlAdapter
from vei.ingest.agent_activity.api import (
    ingest_agent_activity,
    load_workspace_canonical_events,
)
from vei.ingest.agent_activity.mcp_transcript import McpTranscriptAdapter
from vei.ingest.agent_activity.openai_org import OpenAIOrgAdapter
from vei.provenance.api import (
    access_review,
    blast_radius,
    build_activity_graph,
    build_evidence_pack,
)
from vei.provenance.api import replay_policy
from vei.provenance.exporters.otel_genai import export_otel_genai
from vei.router.api import create_router
from vei.ui import api as ui_api


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


def test_execution_principal_maps_identity_to_event_context() -> None:
    principal = ExecutionPrincipal.from_mapping(
        {
            "tenant_id": "tenant-1",
            "workspace_id": "workspace-1",
            "human_user_id": "human-1",
            "agent_id": "agent-1",
            "agent_version": "v2",
            "service_principal": "svc-1",
            "delegated_credential_id": "cred-1",
            "mcp_session_id": "sess-1",
            "jsonrpc_request_id": "rpc-1",
        },
        source="mcp",
    )
    context = principal.to_event_context(
        source_id="source-1",
        source_granularity="transcript",
        trace_id="trace-1",
    )

    compact = context.compact()
    assert compact["tenant_id"] == "tenant-1"
    assert compact["agent_id"] == "agent-1"
    assert compact["human_user_id"] == "human-1"
    assert compact["service_principal"] == "svc-1"
    assert compact["delegated_credential_id"] == "cred-1"
    assert compact["mcp_session_id"] == "sess-1"
    assert compact["jsonrpc_request_id"] == "rpc-1"
    assert compact["source_granularity"] == "transcript"


def test_cli_graph_agent_filter_uses_context_actor_id(tmp_path: Path) -> None:
    workspace = tmp_path / "workspace"
    store = WorkspaceEventStore(workspace, source="unit", batch_id="context-agent")
    store.append(
        build_tool_call_event(
            kind="tool.call.completed",
            tool_name="docs.read",
            context=EventContext(agent_id="context-agent"),
            source_id="unit",
        )
    )
    runner = CliRunner()

    result = runner.invoke(
        app,
        [
            "provenance",
            "graph",
            "--workspace",
            str(workspace),
            "--agent-id",
            "context-agent",
            "--format",
            "json",
        ],
    )

    assert result.exit_code == 0, result.output
    payload = json.loads(result.output)
    assert any(node["id"] == "context-agent" for node in payload["nodes"])


def test_access_review_v2_observed_configured_and_evidence_quality() -> None:
    event = build_tool_call_event(
        kind="tool.call.completed",
        tool_name="docs.read",
        actor_ref=ActorRef(actor_id="agent-1"),
        object_refs=[ObjectRef(object_id="doc-1", domain="doc_graph", kind="document")],
        source_id="unit",
        source_granularity="per_call",
    )

    report = access_review(
        [event],
        agent_id="agent-1",
        configured_access=[
            {"kind": "tool", "id": "docs.read"},
            {"kind": "tool", "id": "slack.post_message"},
        ],
    )

    assert report.tools_used == ["docs.read"]
    assert ("tool", "docs.read") in {
        (item.kind, item.id) for item in report.observed_access
    }
    assert ("tool", "slack.post_message") in {
        (item.kind, item.id) for item in report.unused_permissions
    }
    assert report.recommended_revocations
    assert report.evidence_quality[0].identity_confidence == "verified"


def test_semantic_graph_and_blast_radius_use_typed_links() -> None:
    requested = build_tool_call_event(
        kind="tool.call.requested",
        event_id="evt-request",
        tool_name="docs.read",
        actor_ref=ActorRef(actor_id="agent-1"),
        source_id="unit",
    )
    completed = build_tool_call_event(
        kind="tool.call.completed",
        event_id="evt-completed",
        tool_name="docs.read",
        actor_ref=ActorRef(actor_id="agent-1"),
        object_refs=[ObjectRef(object_id="doc-1", domain="doc_graph", kind="document")],
        links=[{"kind": "completed_by", "event_id": requested.event_id}],
        source_id="unit",
    )

    graph = build_activity_graph([requested, completed])
    assert any(edge.kind == "completed_by" for edge in graph.edges)
    report = blast_radius([requested, completed], anchor_event_id=requested.event_id)
    assert completed.event_id in report.observed
    assert report.inferred == ["evt-completed completed_by evt-request"]


def test_object_ref_registry_connector_specific_mappings() -> None:
    refs = extract_object_refs(tool_name="slack.post_message", args={"channel": "C1"})
    refs += extract_object_refs(tool_name="jira.issue.update", args={"issue_id": "J1"})
    refs += extract_object_refs(
        tool_name="salesforce.get_account", args={"account_id": "A1"}
    )
    refs += extract_object_refs(tool_name="snowflake.query", args={"table": "T1"})
    refs += extract_object_refs(
        tool_name="mcp.resource.read", args={"resource_uri": "file://x"}
    )

    triples = {(ref.domain, ref.kind, ref.object_id) for ref in refs}
    assert ("comm_graph", "channel", "C1") in triples
    assert ("work_graph", "ticket", "J1") in triples
    assert ("revenue_graph", "account", "A1") in triples
    assert ("data_graph", "table", "T1") in triples
    assert ("data_graph", "resource", "file://x") in triples


def test_workspace_event_store_manifest_is_tamper_evident(tmp_path: Path) -> None:
    workspace = tmp_path / "workspace"
    first_store = WorkspaceEventStore(workspace, source="unit", batch_id="batch-1")
    second_store = WorkspaceEventStore(workspace, source="unit", batch_id="batch-2")
    first_store.append(
        build_tool_call_event(
            kind="tool.call.completed",
            tool_name="docs.read",
            source_id="unit:1",
        )
    )
    second_store.append(
        build_tool_call_event(
            kind="tool.call.completed",
            tool_name="docs.read",
            source_id="unit:2",
        )
    )

    first_manifest = json.loads(first_store.manifest_path.read_text(encoding="utf-8"))
    second_manifest = json.loads(second_store.manifest_path.read_text(encoding="utf-8"))
    assert first_manifest["batch_event_count"] == 1
    assert first_manifest["batch_hash"]
    assert first_manifest["manifest_hash"]
    assert second_manifest["previous_batch_hash"] == first_manifest["batch_hash"]


def test_evidence_pack_cli_and_ui_routes(tmp_path: Path) -> None:
    workspace = tmp_path / "workspace"
    store = WorkspaceEventStore(workspace, source="unit", batch_id="evidence")
    event = store.append(
        build_tool_call_event(
            kind="tool.call.completed",
            tool_name="docs.read",
            actor_ref=ActorRef(actor_id="agent-1", display_name="Agent One"),
            object_refs=[
                ObjectRef(object_id="doc-1", domain="doc_graph", kind="document")
            ],
            source_id="unit",
        )
    )
    pack = build_evidence_pack(
        load_workspace_canonical_events(workspace),
        agent_id="agent-1",
        anchor_event_id=event.event_id,
    )
    assert pack.timeline.event_count == 1
    assert pack.agents[0].agent_id == "agent-1"

    runner = CliRunner()
    output = tmp_path / "pack.json"
    result = runner.invoke(
        app,
        [
            "provenance",
            "export",
            "--format",
            "evidence-pack",
            "--workspace",
            str(workspace),
            "--output",
            str(output),
        ],
    )
    assert result.exit_code == 0, result.output
    assert (
        json.loads(output.read_text(encoding="utf-8"))["schema_version"]
        == "evidence_pack_v1"
    )
    result = runner.invoke(
        app,
        [
            "provenance",
            "evidence-pack",
            "--workspace",
            str(workspace),
            "--agent-id",
            "agent-1",
            "--event-id",
            event.event_id,
            "--format",
            "json",
        ],
    )
    assert result.exit_code == 0, result.output
    assert "evidence_pack_v1" in result.output

    client = TestClient(ui_api.create_ui_app(workspace))
    assert client.get("/api/workspace/provenance/agents").status_code == 200
    access_response = client.get(
        "/api/workspace/provenance/agents/agent-1/access-review"
    )
    assert access_response.status_code == 200
    assert access_response.json()["agent_id"] == "agent-1"
    blast_response = client.get(
        f"/api/workspace/provenance/events/{event.event_id}/blast-radius"
    )
    assert blast_response.status_code == 200
    replay_response = client.post(
        "/api/workspace/provenance/policy-replay",
        json={"policy": {"name": "test", "deny_event_kinds": ["tool.call.completed"]}},
    )
    assert replay_response.status_code == 200
    assert replay_response.json()["hit_count"] == 1
    pack_response = client.get("/api/workspace/provenance/evidence-pack")
    assert pack_response.status_code == 200
    assert pack_response.json()["schema_version"] == "evidence_pack_v1"
