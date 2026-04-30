from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from fastapi import FastAPI, HTTPException, Request

from vei.ingest.api import agent_activity_ingest_status
from vei.provenance.api import (
    access_review,
    agent_inventory,
    blast_radius,
    build_activity_graph,
    build_evidence_pack,
    inspect_timeline,
    load_workspace_events,
    replay_policy,
)


def register_provenance_routes(app: FastAPI, workspace_root: Path) -> None:
    @app.get("/api/workspace/provenance/control")
    def provenance_control() -> dict:
        events = load_workspace_events(workspace_root)
        timeline = inspect_timeline(events)
        graph = build_activity_graph(events)
        configured_agents = _load_configured_agents(workspace_root)
        agents = agent_inventory(events, configured_agents=configured_agents)
        selected_agent = agents[0].agent_id if agents else ""
        selected_event = timeline.items[-1].event_id if timeline.items else ""
        return {
            "available": True,
            "event_count": timeline.event_count,
            "ingest": agent_activity_ingest_status(str(workspace_root)),
            "timeline": [item.model_dump(mode="json") for item in timeline.items[-20:]],
            "graph": graph.model_dump(mode="json"),
            "agents": [item.model_dump(mode="json") for item in agents],
            "access_review": (
                access_review(
                    events,
                    agent_id=selected_agent,
                    configured_access=_configured_access_for(
                        configured_agents, selected_agent
                    ),
                ).model_dump(mode="json")
                if selected_agent
                else None
            ),
            "blast_radius": (
                blast_radius(events, anchor_event_id=selected_event).model_dump(
                    mode="json"
                )
                if selected_event
                else None
            ),
            "evidence_pack": build_evidence_pack(
                events,
                agent_id=selected_agent or None,
                anchor_event_id=selected_event or None,
                configured_agents=configured_agents,
            ).model_dump(mode="json"),
            "warnings": timeline.warnings + graph.warnings,
        }

    @app.get("/api/workspace/provenance/agents")
    def provenance_agents() -> dict:
        events = load_workspace_events(workspace_root)
        configured_agents = _load_configured_agents(workspace_root)
        return {
            "agents": [
                item.model_dump(mode="json")
                for item in agent_inventory(events, configured_agents=configured_agents)
            ]
        }

    @app.get("/api/workspace/provenance/agents/{agent_id}/access-review")
    def provenance_agent_access_review(agent_id: str) -> dict:
        events = load_workspace_events(workspace_root)
        configured_agents = _load_configured_agents(workspace_root)
        return access_review(
            events,
            agent_id=agent_id,
            configured_access=_configured_access_for(configured_agents, agent_id),
        ).model_dump(mode="json")

    @app.get("/api/workspace/provenance/events/{event_id}/blast-radius")
    def provenance_event_blast_radius(event_id: str) -> dict:
        events = load_workspace_events(workspace_root)
        if not any(event.event_id == event_id for event in events):
            raise HTTPException(status_code=404, detail="event not found")
        return blast_radius(events, anchor_event_id=event_id).model_dump(mode="json")

    @app.post("/api/workspace/provenance/policy-replay")
    async def provenance_policy_replay(request: Request) -> dict:
        events = load_workspace_events(workspace_root)
        payload = await request.json()
        policy = payload.get("policy", payload) if isinstance(payload, dict) else {}
        if not isinstance(policy, dict):
            raise HTTPException(status_code=400, detail="policy must be an object")
        return replay_policy(events, policy=policy).model_dump(mode="json")

    @app.get("/api/workspace/provenance/evidence-pack")
    def provenance_evidence_pack(
        agent_id: str | None = None,
        event_id: str | None = None,
    ) -> dict:
        events = load_workspace_events(workspace_root)
        configured_agents = _load_configured_agents(workspace_root)
        return build_evidence_pack(
            events,
            agent_id=agent_id,
            anchor_event_id=event_id,
            configured_agents=configured_agents,
        ).model_dump(mode="json")


def _load_configured_agents(workspace_root: Path) -> list[dict[str, Any]]:
    candidates = [
        workspace_root / "vei_project.json",
        workspace_root / "twin_manifest.json",
        workspace_root / "workspace" / "vei_project.json",
        workspace_root / "workspace" / "twin_manifest.json",
    ]
    agents: list[dict[str, Any]] = []
    for path in candidates:
        if not path.exists():
            continue
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, ValueError, TypeError):
            continue
        agents.extend(_extract_agent_specs(payload))
    deduped: dict[str, dict[str, Any]] = {}
    for agent in agents:
        agent_id = str(agent.get("agent_id") or agent.get("id") or "")
        if agent_id:
            deduped[agent_id] = {**deduped.get(agent_id, {}), **agent}
    return sorted(deduped.values(), key=lambda item: str(item.get("agent_id", "")))


def _extract_agent_specs(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, list):
        items: list[dict[str, Any]] = []
        for item in payload:
            items.extend(_extract_agent_specs(item))
        return items
    if not isinstance(payload, dict):
        return []
    agents: list[dict[str, Any]] = []
    for key in ("agents", "agent_specs", "governor_agents"):
        value = payload.get(key)
        if isinstance(value, list):
            agents.extend(item for item in value if isinstance(item, dict))
    for container in ("governor", "mirror", "twin", "workspace"):
        nested = payload.get(container)
        if isinstance(nested, dict):
            agents.extend(_extract_agent_specs(nested))
    return agents


def _configured_access_for(
    configured_agents: list[dict[str, Any]], agent_id: str
) -> list[dict[str, Any]] | None:
    for agent in configured_agents:
        if str(agent.get("agent_id") or agent.get("id") or "") != agent_id:
            continue
        entries: list[dict[str, Any]] = []
        for key, kind in (
            ("allowed_tools", "tool"),
            ("tools", "tool"),
            ("allowed_surfaces", "surface"),
            ("surfaces", "surface"),
            ("permissions", "permission"),
            ("scopes", "scope"),
            ("oauth_scopes", "scope"),
        ):
            values = agent.get(key)
            if isinstance(values, list):
                entries.extend({"kind": kind, "id": str(value)} for value in values)
        return entries
    return None
