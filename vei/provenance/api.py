"""Public provenance projection API."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Iterable

from vei.events.api import CanonicalEvent, ObjectRef, link_event_ids, typed_event_links
from vei.ingest.api import load_agent_activity_events

from .models import (
    AccessReviewReport,
    AccessItem,
    ActivityEdge,
    ActivityNode,
    AgentInventoryItem,
    BlastRadiusReport,
    CompanyActivityGraph,
    EvidencePack,
    EvidenceQuality,
    PolicyReplayHit,
    PolicyReplayReport,
    ProvenanceTimeline,
    TimelineItem,
)


def load_workspace_events(workspace: str | Path) -> list[CanonicalEvent]:
    return load_agent_activity_events(str(workspace))


def _delta(event: CanonicalEvent) -> dict:
    return event.delta.data if event.delta is not None else {}


def _context(event: CanonicalEvent) -> dict:
    context = _delta(event).get("context", {})
    return context if isinstance(context, dict) else {}


def _link_refs(event: CanonicalEvent) -> list[str]:
    return link_event_ids(_delta(event))


def _typed_links(event: CanonicalEvent) -> list:
    return typed_event_links(_delta(event))


def _source_granularity(event: CanonicalEvent) -> str:
    data = _delta(event)
    return str(
        data.get("source_granularity") or _context(event).get("source_granularity", "")
    )


def provenance_actor_id(event: CanonicalEvent) -> str:
    if event.actor_ref:
        return event.actor_ref.actor_id
    context = _context(event)
    return str(context.get("agent_id") or context.get("human_user_id") or "")


def _event_actor_id(event: CanonicalEvent) -> str:
    return provenance_actor_id(event)


def evidence_quality(event: CanonicalEvent) -> EvidenceQuality:
    context = _context(event)
    granularity = _source_granularity(event)
    warnings: list[str] = []
    if granularity in {"aggregate", "audit_only"}:
        warnings.append(f"{granularity} source is not per-call evidence")
    links = _typed_links(event)
    object_confidence = "exact" if event.object_refs else "absent"
    if event.object_refs and any(
        _delta(event).get(key) for key in ("args_handle", "response_handle")
    ):
        object_confidence = "extracted"
    quality = EvidenceQuality(
        source_granularity=granularity,
        source_integrity=_source_integrity(event),
        time_confidence="exact" if event.ts_ms else "missing",
        object_confidence=object_confidence,
        link_confidence=(
            "exact" if links else ("unknown" if _link_refs(event) else "absent")
        ),
        identity_confidence=(
            "verified"
            if event.actor_ref
            else (
                "inferred"
                if context.get("agent_id") or context.get("human_user_id")
                else "absent"
            )
        ),
        warnings=warnings,
    )
    return quality


def _source_integrity(event: CanonicalEvent) -> str:
    origin = getattr(event.provenance.origin, "value", str(event.provenance.origin))
    if origin == "simulated":
        return "simulated"
    if event.provenance.source_id.startswith(("openai", "mcp_transcript")):
        return "api_fetched"
    if origin == "derived":
        return "derived"
    return "imported"


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
                actor_id=_event_actor_id(event),
                object_ids=object_ids,
                source_id=event.provenance.source_id,
                source_granularity=granularity,
                summary=_event_summary(event),
                link_refs=_link_refs(event),
                evidence_quality=evidence_quality(event),
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

    def add_edge(
        source: str,
        target: str,
        kind: str,
        event_id: str,
        *,
        link_kind: str = "",
        confidence: str = "",
    ) -> None:
        if not source or not target:
            return
        key = (source, target, kind)
        edge = edges.setdefault(
            key,
            ActivityEdge(
                source=source,
                target=target,
                kind=kind,
                link_kind=link_kind,
                confidence=confidence,
            ),
        )
        if event_id not in edge.event_ids:
            edge.event_ids.append(event_id)

    for event in events:
        event_node_id = f"event:{event.event_id}"
        add_node(event_node_id, "event", event.kind, event.event_id)
        actor_id = _event_actor_id(event)
        if actor_id:
            add_node(
                actor_id,
                "actor",
                (event.actor_ref.display_name if event.actor_ref else "") or actor_id,
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
            add_edge(tool_id or actor_id, object_id, "touched_object", event.event_id)
        if event.kind.startswith("governance."):
            policy_id = f"policy:{event.event_id}"
            add_node(
                policy_id,
                "policy",
                _delta(event).get("decision", event.kind),
                event.event_id,
            )
            add_edge(actor_id, policy_id, "policy_event", event.event_id)
        for link in _typed_links(event):
            linked_event_node_id = f"event:{link.event_id}"
            add_node(linked_event_node_id, "event", link.event_id, link.event_id)
            source_node = linked_event_node_id
            target_node = event_node_id
            if link.kind in {"completed_by", "failed_by", "resolved_by"}:
                source_node = linked_event_node_id
                target_node = event_node_id
            add_edge(
                source_node,
                target_node,
                link.kind or "linked",
                event.event_id,
                link_kind=link.kind,
                confidence="exact",
            )
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
    inferred: set[str] = set()
    changed = True
    while changed:
        changed = False
        for event in event_list:
            refs = set(_link_refs(event))
            if event.event_id in related_ids or refs & related_ids:
                before = len(related_ids)
                related_ids.add(event.event_id)
                related_ids.update(refs)
                for link in _typed_links(event):
                    if link.event_id in related_ids or event.event_id in related_ids:
                        inferred.add(
                            f"{event.event_id} {link.kind or 'linked'} {link.event_id}"
                        )
                changed = len(related_ids) != before
    reached_nodes = [node for node in graph.nodes if set(node.event_ids) & related_ids]
    reached_edges = [edge for edge in graph.edges if set(edge.event_ids) & related_ids]
    read_objects: set[str] = set()
    written_objects: set[str] = set()
    artifacts: set[str] = set()
    policies: set[str] = set()
    approvals: set[str] = set()
    unknowns: set[str] = set()
    qualities: list[EvidenceQuality] = []
    for event in event_list:
        if event.event_id not in related_ids:
            continue
        qualities.append(evidence_quality(event))
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
        observed=sorted(related_ids),
        inferred=sorted(inferred),
        evidence_quality=qualities,
    )


def access_review(
    events: Iterable[CanonicalEvent],
    *,
    agent_id: str,
    configured_access: Iterable[dict] | None = None,
) -> AccessReviewReport:
    touched_objects: set[str] = set()
    tools_used: set[str] = set()
    policy_decisions: set[str] = set()
    granularities: set[str] = set()
    warnings: set[str] = set()
    observed_items: dict[tuple[str, str], AccessItem] = {}
    qualities: list[EvidenceQuality] = []
    for event in events:
        actor_id = _event_actor_id(event)
        if actor_id != agent_id:
            continue
        qualities.append(evidence_quality(event))
        touched_objects.update(ref.object_id for ref in event.object_refs)
        tool_name = str(_delta(event).get("tool_name", ""))
        if tool_name:
            tools_used.add(tool_name)
            _merge_access_item(
                observed_items,
                AccessItem(
                    kind="tool",
                    id=tool_name,
                    label=tool_name,
                    source="observed",
                    event_ids=[event.event_id],
                ),
            )
        for ref in event.object_refs:
            _merge_access_item(
                observed_items,
                _object_access_item(ref, source="observed", event_id=event.event_id),
            )
        if event.kind.startswith("governance."):
            decision = str(_delta(event).get("decision", event.kind))
            policy_decisions.add(decision)
        granularity = _source_granularity(event)
        if granularity:
            granularities.add(granularity)
        if granularity in {"aggregate", "audit_only"}:
            warnings.add(f"{granularity} source cannot prove exact object reach")
    configured_items = [
        _configured_access_item(item) for item in configured_access or []
    ]
    if configured_access is None:
        warnings.add("configured access unavailable for this workspace")
    observed = sorted(observed_items.values(), key=lambda item: (item.kind, item.id))
    configured = sorted(configured_items, key=lambda item: (item.kind, item.id))
    observed_keys = {(item.kind, item.id) for item in observed}
    configured_keys = {(item.kind, item.id) for item in configured}
    unused = [item for item in configured if (item.kind, item.id) not in observed_keys]
    new_access = [
        item for item in observed if (item.kind, item.id) not in configured_keys
    ]
    recommended = [
        item
        for item in unused
        if item.kind in {"tool", "object", "scope", "permission"}
    ]
    return AccessReviewReport(
        agent_id=agent_id,
        touched_objects=sorted(touched_objects),
        tools_used=sorted(tools_used),
        policy_decisions=sorted(policy_decisions),
        source_granularities=sorted(granularities),
        observed_access=observed,
        configured_access=configured,
        reachable_sensitive_assets=[
            item for item in configured if "sensitive" in item.label.lower()
        ],
        unused_permissions=recommended if configured else [],
        new_access_since_last_review=new_access if configured else observed,
        recommended_revocations=recommended,
        evidence_quality=qualities,
        warnings=sorted(warnings),
    )


def agent_inventory(
    events: Iterable[CanonicalEvent],
    *,
    configured_agents: Iterable[dict] | None = None,
) -> list[AgentInventoryItem]:
    event_list = list(events)
    configured_by_id: dict[str, list[AccessItem]] = {}
    for agent in configured_agents or []:
        agent_id = str(agent.get("agent_id") or agent.get("id") or "")
        if not agent_id:
            continue
        configured_by_id[agent_id] = [
            _configured_access_item(item) for item in _configured_access_entries(agent)
        ]

    grouped: dict[str, list[CanonicalEvent]] = {}
    for event in event_list:
        actor_id = _event_actor_id(event)
        if actor_id:
            grouped.setdefault(actor_id, []).append(event)
    for agent_id in configured_by_id:
        grouped.setdefault(agent_id, [])

    inventory: list[AgentInventoryItem] = []
    for agent_id, agent_events in sorted(grouped.items()):
        review = access_review(
            agent_events,
            agent_id=agent_id,
            configured_access=[
                item.model_dump(mode="json")
                for item in configured_by_id.get(agent_id, [])
            ],
        )
        display_name = ""
        for event in agent_events:
            if event.actor_ref and event.actor_ref.display_name:
                display_name = event.actor_ref.display_name
                break
        inventory.append(
            AgentInventoryItem(
                agent_id=agent_id,
                display_name=display_name,
                event_count=len(agent_events),
                tools_used=review.tools_used,
                touched_objects=review.touched_objects,
                configured_access=configured_by_id.get(agent_id, []),
                evidence_quality=review.evidence_quality,
            )
        )
    return inventory


def build_evidence_pack(
    events: Iterable[CanonicalEvent],
    *,
    agent_id: str | None = None,
    anchor_event_id: str | None = None,
    policy: dict | None = None,
    configured_agents: Iterable[dict] | None = None,
) -> EvidencePack:
    event_list = list(events)
    agents = agent_inventory(event_list, configured_agents=configured_agents)
    agent_ids = [agent_id] if agent_id else [agent.agent_id for agent in agents[:5]]
    configured_by_id = _configured_access_by_agent(configured_agents or [])
    reviews = [
        access_review(
            event_list,
            agent_id=item,
            configured_access=configured_by_id.get(item),
        )
        for item in agent_ids
        if item
    ]
    blast = (
        blast_radius(event_list, anchor_event_id=anchor_event_id)
        if anchor_event_id
        else None
    )
    replay = replay_policy(event_list, policy=policy) if policy else None
    warnings = set(inspect_timeline(event_list).warnings)
    for review in reviews:
        warnings.update(review.warnings)
    if blast:
        warnings.update(blast.unknowns)
    if replay:
        warnings.update(replay.warnings)
    return EvidencePack(
        timeline=inspect_timeline(event_list),
        agents=agents,
        access_reviews=reviews,
        blast_radius=blast,
        policy_replay=replay,
        warnings=sorted(warnings),
    )


def _merge_access_item(
    items: dict[tuple[str, str], AccessItem], item: AccessItem
) -> None:
    key = (item.kind, item.id)
    existing = items.get(key)
    if existing is None:
        items[key] = item
        return
    for event_id in item.event_ids:
        if event_id not in existing.event_ids:
            existing.event_ids.append(event_id)


def _object_access_item(
    ref: ObjectRef, *, source: str, event_id: str = ""
) -> AccessItem:
    return AccessItem(
        kind="object",
        id=f"{ref.domain}:{ref.kind}:{ref.object_id}".strip(":"),
        label=ref.label or ref.object_id,
        source=source,
        event_ids=[event_id] if event_id else [],
    )


def _configured_access_item(item: dict | AccessItem) -> AccessItem:
    if isinstance(item, AccessItem):
        return item
    kind = str(item.get("kind") or item.get("type") or "permission")
    item_id = str(item.get("id") or item.get("name") or item.get("tool") or "")
    if not item_id and item.get("surface"):
        item_id = str(item["surface"])
        kind = "surface"
    return AccessItem(
        kind=kind,
        id=item_id,
        label=str(item.get("label") or item.get("display_name") or item_id),
        source=str(item.get("source") or "configured"),
    )


def _configured_access_entries(agent: dict) -> list[dict]:
    entries: list[dict] = []
    for key, kind in (
        ("allowed_tools", "tool"),
        ("tools", "tool"),
        ("allowed_surfaces", "surface"),
        ("surfaces", "surface"),
        ("scopes", "scope"),
        ("oauth_scopes", "scope"),
        ("permissions", "permission"),
    ):
        values = agent.get(key)
        if isinstance(values, list):
            entries.extend({"kind": kind, "id": str(value)} for value in values)
    return entries


def _configured_access_by_agent(
    configured_agents: Iterable[dict],
) -> dict[str, list[dict]]:
    mapping: dict[str, list[dict]] = {}
    for agent in configured_agents:
        agent_id = str(agent.get("agent_id") or agent.get("id") or "")
        if agent_id:
            mapping[agent_id] = _configured_access_entries(agent)
    return mapping


def replay_policy(
    events: Iterable[CanonicalEvent],
    *,
    policy: dict,
) -> PolicyReplayReport:
    event_list = list(events)
    evaluator_report = _replay_with_policy_evaluator(event_list, policy)
    if evaluator_report is not None:
        return evaluator_report
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


def _replay_with_policy_evaluator(
    events: list[CanonicalEvent],
    policy: dict,
) -> PolicyReplayReport | None:
    governor_policy = policy.get("governor") or {}
    if not isinstance(governor_policy, dict):
        return None
    config_payload = governor_policy.get("config", policy.get("config"))
    agents_payload = governor_policy.get("agents", policy.get("agents"))
    if not config_payload and not agents_payload:
        return None

    from vei.governor import (
        GovernorAgentSpec,
        GovernorIngestEvent,
        GovernorWorkspaceConfig,
        resolve_governor_policy_profile,
    )
    from vei.twin.api import PolicyEvaluator

    config = GovernorWorkspaceConfig.model_validate(config_payload or {})
    connector_mode = str(
        governor_policy.get("connector_mode")
        or policy.get("connector_mode")
        or config.connector_mode
    )
    agents: dict[str, GovernorAgentSpec] = {}
    for item in agents_payload or []:
        agent = GovernorAgentSpec.model_validate(item)
        agent.resolved_policy_profile = resolve_governor_policy_profile(
            agent.policy_profile_id
        )
        agents[agent.agent_id] = agent

    evaluator = PolicyEvaluator(config=config, connector_mode=connector_mode)
    hits: list[PolicyReplayHit] = []
    warnings: set[str] = set()
    for event in events:
        if not event.kind.startswith("tool.call."):
            continue
        data = _delta(event)
        tool_name = str(data.get("tool_name", ""))
        if not tool_name:
            continue
        agent_id = _event_actor_id(event) or str(_context(event).get("agent_id", ""))
        if not agent_id:
            warnings.add(f"{event.event_id}: cannot reconstruct agent identity")
            continue
        agent = agents.get(agent_id)
        if agent is None:
            warnings.add(f"{event.event_id}: no replay agent config for {agent_id}")
            continue
        replay_event = GovernorIngestEvent(
            event_id=event.event_id,
            agent_id=agent_id,
            external_tool=tool_name,
            resolved_tool=tool_name,
            args=dict(data.get("args") or {}),
        )
        evaluation = evaluator.evaluate(event=replay_event, agent=agent)
        if evaluation.decision != "allow":
            hits.append(
                PolicyReplayHit(
                    event_id=event.event_id,
                    original_decision=str(data.get("decision", "")),
                    replay_decision=evaluation.decision,
                    reason=evaluation.reason or evaluation.reason_code,
                    event_kind=event.kind,
                )
            )
    return PolicyReplayReport(
        policy_name=str(policy.get("name", "")),
        event_count=len(events),
        hit_count=len(hits),
        hits=hits,
        warnings=sorted(warnings),
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
