"""Governor-backed policy replay over canonical tool events."""

from __future__ import annotations

from typing import TYPE_CHECKING

from vei.events.api import CanonicalEvent

from vei.governor._config import resolve_governor_policy_profile
from vei.governor.models import (
    GovernorAgentSpec,
    GovernorIngestEvent,
    GovernorWorkspaceConfig,
    Policy,
)

if TYPE_CHECKING:
    from vei.provenance.models import PolicyReplayReport


def _evt_delta(event: CanonicalEvent) -> dict:
    return event.delta.data if event.delta is not None else {}


def _evt_context(event: CanonicalEvent) -> dict:
    context = _evt_delta(event).get("context", {})
    return context if isinstance(context, dict) else {}


def _replay_actor_id(event: CanonicalEvent) -> str:
    if event.actor_ref:
        return str(event.actor_ref.actor_id)
    context = _evt_context(event)
    return str(context.get("agent_id") or context.get("human_user_id") or "")


def replay_policy_with_evaluator(
    events: list[CanonicalEvent],
    policy: Policy,
) -> PolicyReplayReport | None:
    """Re-run governor policy evaluation on ingested tool events (if configured)."""

    from vei.provenance.models import PolicyReplayHit, PolicyReplayReport

    if policy.governor_config is None and not policy.governor_agents:
        return None

    config = policy.governor_config or GovernorWorkspaceConfig()
    connector_mode = (
        policy.connector_mode.strip() or str(config.connector_mode)
    ).strip()

    agents: dict[str, GovernorAgentSpec] = {}
    for agent in policy.governor_agents:
        agent_copy = GovernorAgentSpec.model_validate(
            agent.model_dump(mode="json"),
        )
        agent_copy.resolved_policy_profile = resolve_governor_policy_profile(
            agent_copy.policy_profile_id,
        )
        agents[agent_copy.agent_id] = agent_copy

    from vei.twin.api import PolicyEvaluator

    evaluator = PolicyEvaluator(config=config, connector_mode=connector_mode)
    hits: list[PolicyReplayHit] = []
    warnings: set[str] = set()
    for event in events:
        if not event.kind.startswith("tool.call."):
            continue
        data = _evt_delta(event)
        tool_name = str(data.get("tool_name", ""))
        if not tool_name:
            continue
        agent_id = _replay_actor_id(event) or str(
            _evt_context(event).get("agent_id", ""),
        )
        if not agent_id:
            warnings.add(f"{event.event_id}: cannot reconstruct agent identity")
            continue
        replay_agent = agents.get(agent_id)
        if replay_agent is None:
            warnings.add(f"{event.event_id}: no replay agent config for {agent_id}")
            continue
        replay_event = GovernorIngestEvent(
            event_id=event.event_id,
            agent_id=agent_id,
            external_tool=tool_name,
            resolved_tool=tool_name,
            args=dict(data.get("args") or {}),
        )
        evaluation = evaluator.evaluate(event=replay_event, agent=replay_agent)
        if evaluation.decision != "allow":
            hits.append(
                PolicyReplayHit(
                    event_id=event.event_id,
                    original_decision=str(data.get("decision", "")),
                    replay_decision=evaluation.decision,
                    reason=evaluation.reason or evaluation.reason_code,
                    event_kind=event.kind,
                ),
            )

    return PolicyReplayReport(
        policy_name=policy.name,
        event_count=len(events),
        hit_count=len(hits),
        hits=hits,
        warnings=sorted(warnings),
    )
