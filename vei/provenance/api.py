"""Public provenance projection API."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Iterable

from vei.events.api import CanonicalEvent
from vei.ingest.api import load_agent_activity_events

from .models import (
    AccessReviewReport,
    ActivityEdge,
    ActivityNode,
    BlastRadiusReport,
    CompanyActivityGraph,
    PolicyReplayHit,
    PolicyReplayReport,
    ProvenanceTimeline,
    TimelineItem,
)


def load_workspace_events(workspace: str | Path) -> list[CanonicalEvent]:
    return load_agent_activity_events(str(workspace))


def _delta(event: CanonicalEvent) -> dict:
    return event.delta.data if event.delta is not None else {}


def _link_refs(event: CanonicalEvent) -> list[str]:
    refs = _delta(event).get("link_refs", [])
    return [str(ref) for ref in refs] if isinstance(refs, list) else []


def _source_granularity(event: CanonicalEvent) -> str:
    return str(_delta(event).get("source_granularity", ""))


def inspect_timeline(events: Iterable[CanonicalEvent]) -> ProvenanceTimeline:
    items: list[TimelineItem] = []
    warnings: list[str] = []
    for event in sorted(events, key=lambda item: (item.ts_ms, item.event_id)):
        object_ids = [
            f"{ref.domain}:{ref.kind}:{ref.object_id}".strip(":")
            for ref in event.object_refs
        ]
        granularity = _source_granularity(event)
        if granularity in {"aggregate", "audit_only"}:
            warnings.append(
                f"{event.event_id} is {granularity}; it is not per-call evidence"
            )
        items.append(
            TimelineItem(
                event_id=event.event_id,
                ts_ms=event.ts_ms,
                kind=event.kind,
                actor_id=event.actor_ref.actor_id if event.actor_ref else "",
                object_ids=object_ids,
                source_id=event.provenance.source_id,
                source_granularity=granularity,
                summary=_event_summary(event),
                link_refs=_link_refs(event),
            )
        )
    return ProvenanceTimeline(
        event_count=len(items),
        items=items,
        warnings=sorted(set(warnings)),
    )


def build_activity_graph(events: Iterable[CanonicalEvent]) -> CompanyActivityGraph:
    nodes: dict[str, ActivityNode] = {}
    edges: dict[tuple[str, str, str], ActivityEdge] = {}
    warnings: set[str] = set()

    def add_node(node_id: str, kind: str, label: str, event_id: str) -> None:
        if not node_id:
            return
        node = nodes.setdefault(
            node_id, ActivityNode(id=node_id, kind=kind, label=label)
        )
        if event_id not in node.event_ids:
            node.event_ids.append(event_id)

    def add_edge(source: str, target: str, kind: str, event_id: str) -> None:
        if not source or not target:
            return
        key = (source, target, kind)
        edge = edges.setdefault(
            key, ActivityEdge(source=source, target=target, kind=kind)
        )
        if event_id not in edge.event_ids:
            edge.event_ids.append(event_id)

    for event in events:
        actor_id = event.actor_ref.actor_id if event.actor_ref else ""
        if actor_id:
            add_node(
                actor_id,
                "actor",
                event.actor_ref.display_name or actor_id,
                event.event_id,
            )
        tool_name = str(_delta(event).get("tool_name", ""))
        if tool_name:
            tool_id = f"tool:{tool_name}"
            add_node(tool_id, "tool", tool_name, event.event_id)
            add_edge(actor_id, tool_id, "used_tool", event.event_id)
        provider = str(_delta(event).get("provider", ""))
        model = str(_delta(event).get("model", ""))
        if provider or model:
            model_id = f"model:{provider}:{model}"
            add_node(
                model_id, "model", f"{provider}/{model}".strip("/"), event.event_id
            )
            add_edge(actor_id, model_id, "called_model", event.event_id)
        for ref in event.object_refs:
            object_id = f"object:{ref.domain}:{ref.kind}:{ref.object_id}"
            add_node(object_id, "object", ref.label or ref.object_id, event.event_id)
            add_edge(
                actor_id or tool_id if tool_name else actor_id,
                object_id,
                "touched_object",
                event.event_id,
            )
        if event.kind.startswith("governance."):
            policy_id = f"policy:{event.event_id}"
            add_node(
                policy_id,
                "policy",
                _delta(event).get("decision", event.kind),
                event.event_id,
            )
            add_edge(actor_id, policy_id, "policy_event", event.event_id)
        granularity = _source_granularity(event)
        if granularity in {"aggregate", "audit_only"}:
            warnings.add(f"{granularity} evidence present; some links are coarse")

    return CompanyActivityGraph(
        node_count=len(nodes),
        edge_count=len(edges),
        nodes=sorted(nodes.values(), key=lambda item: item.id),
        edges=sorted(
            edges.values(), key=lambda item: (item.source, item.target, item.kind)
        ),
        warnings=sorted(warnings),
    )


def blast_radius(
    events: Iterable[CanonicalEvent],
    *,
    anchor_event_id: str,
) -> BlastRadiusReport:
    event_list = list(events)
    graph = build_activity_graph(event_list)
    related_ids = {anchor_event_id}
    changed = True
    while changed:
        changed = False
        for event in event_list:
            refs = set(_link_refs(event))
            if event.event_id in related_ids or refs & related_ids:
                before = len(related_ids)
                related_ids.add(event.event_id)
                related_ids.update(refs)
                changed = len(related_ids) != before
    reached_nodes = [node for node in graph.nodes if set(node.event_ids) & related_ids]
    reached_edges = [edge for edge in graph.edges if set(edge.event_ids) & related_ids]
    read_objects: set[str] = set()
    written_objects: set[str] = set()
    artifacts: set[str] = set()
    policies: set[str] = set()
    approvals: set[str] = set()
    unknowns: set[str] = set()
    for event in event_list:
        if event.event_id not in related_ids:
            continue
        refs = [ref.object_id for ref in event.object_refs]
        if event.kind.endswith(".read") or ".read" in event.kind:
            read_objects.update(refs)
        if event.kind.endswith(".written") or ".write" in event.kind:
            written_objects.update(refs)
        if event.kind.startswith("artifact."):
            artifacts.add(event.event_id)
        if event.kind.startswith("governance.policy."):
            policies.add(event.event_id)
        if event.kind.startswith("governance.approval."):
            approvals.add(event.event_id)
        if _source_granularity(event) in {"aggregate", "audit_only"}:
            unknowns.add(
                f"{event.event_id} is {_source_granularity(event)}; exact downstream per-call blast radius is unknown"
            )
    return BlastRadiusReport(
        anchor_event_id=anchor_event_id,
        reached_nodes=reached_nodes,
        reached_edges=reached_edges,
        read_objects=sorted(read_objects),
        written_objects=sorted(written_objects),
        artifacts=sorted(artifacts),
        policies=sorted(policies),
        approvals=sorted(approvals),
        unknowns=sorted(unknowns),
    )


def access_review(
    events: Iterable[CanonicalEvent],
    *,
    agent_id: str,
) -> AccessReviewReport:
    touched_objects: set[str] = set()
    tools_used: set[str] = set()
    policy_decisions: set[str] = set()
    granularities: set[str] = set()
    warnings: set[str] = set()
    for event in events:
        actor_id = event.actor_ref.actor_id if event.actor_ref else ""
        if actor_id != agent_id:
            continue
        touched_objects.update(ref.object_id for ref in event.object_refs)
        tool_name = str(_delta(event).get("tool_name", ""))
        if tool_name:
            tools_used.add(tool_name)
        if event.kind.startswith("governance."):
            decision = str(_delta(event).get("decision", event.kind))
            policy_decisions.add(decision)
        granularity = _source_granularity(event)
        if granularity:
            granularities.add(granularity)
        if granularity in {"aggregate", "audit_only"}:
            warnings.add(f"{granularity} source cannot prove exact object reach")
    return AccessReviewReport(
        agent_id=agent_id,
        touched_objects=sorted(touched_objects),
        tools_used=sorted(tools_used),
        policy_decisions=sorted(policy_decisions),
        source_granularities=sorted(granularities),
        warnings=sorted(warnings),
    )


def replay_policy(
    events: Iterable[CanonicalEvent],
    *,
    policy: dict,
) -> PolicyReplayReport:
    event_list = list(events)
    denied_kinds = {str(item) for item in policy.get("deny_event_kinds", [])}
    hold_tools = {str(item) for item in policy.get("hold_tools", [])}
    deny_granularities = {
        str(item) for item in policy.get("deny_source_granularities", [])
    }
    hits: list[PolicyReplayHit] = []
    for event in event_list:
        replay_decision = ""
        reason = ""
        tool_name = str(_delta(event).get("tool_name", ""))
        granularity = _source_granularity(event)
        if event.kind in denied_kinds:
            replay_decision = "deny"
            reason = f"policy denies event kind {event.kind}"
        elif tool_name in hold_tools:
            replay_decision = "hold"
            reason = f"policy holds tool {tool_name}"
        elif granularity in deny_granularities:
            replay_decision = "deny"
            reason = f"policy denies {granularity} evidence"
        if replay_decision:
            hits.append(
                PolicyReplayHit(
                    event_id=event.event_id,
                    original_decision=str(_delta(event).get("decision", "")),
                    replay_decision=replay_decision,
                    reason=reason,
                    event_kind=event.kind,
                )
            )
    return PolicyReplayReport(
        policy_name=str(policy.get("name", "")),
        event_count=len(event_list),
        hit_count=len(hits),
        hits=hits,
    )


def load_policy_file(path: str | Path) -> dict:
    return json.loads(Path(path).expanduser().read_text(encoding="utf-8"))


def export_otel(events: Iterable[CanonicalEvent]) -> dict:
    from vei.provenance.exporters.otel_genai import export_otel_genai

    return export_otel_genai(list(events))


def _event_summary(event: CanonicalEvent) -> str:
    data = _delta(event)
    if event.kind.startswith("tool.call."):
        return f"{event.kind} {data.get('tool_name', '')}".strip()
    if event.kind.startswith("llm."):
        return (
            f"{event.kind} {data.get('provider', '')}/{data.get('model', '')}".strip()
        )
    if event.kind.startswith("governance."):
        return f"{event.kind} {data.get('decision', '')}".strip()
    return event.kind
