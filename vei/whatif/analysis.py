from __future__ import annotations

from pathlib import Path
from typing import Any, Sequence

from vei.context.models import ContextSnapshot

from .cases import build_case_context
from .corpus import (
    detect_whatif_source,
    display_name,
    choose_branch_event,
    event_by_id,
    event_reason_labels,
    has_external_recipients,
    load_history_snapshot,
    load_company_history_world,
    load_mail_archive_world,
    load_enron_world,
    search_events as search_world_events,
    thread_events,
    touches_executive,
)
from .models import (
    WhatIfActorImpact,
    WhatIfBranchCandidate,
    WhatIfBranchCandidateResult,
    WhatIfConsequence,
    WhatIfEvent,
    WhatIfResult,
    WhatIfScenarioId,
    WhatIfThreadImpact,
    WhatIfThreadSummary,
    WhatIfWorld,
)
from .ranking import list_objective_packs as list_historical_objective_packs
from .scenario_registry import (
    list_supported_scenarios,
    resolve_scenario as _resolve_scenario,
    resolve_scenario_from_specific_event as _resolve_scenario_from_specific_event,
)
from .situations import build_situation_context, recommend_branch_thread


def list_objective_packs():
    return list_historical_objective_packs()


def list_branch_candidates(
    world: WhatIfWorld,
    *,
    limit: int = 10,
) -> WhatIfBranchCandidateResult:
    resolved_limit = max(1, int(limit))
    source_snapshot = _source_snapshot_for_world(world)
    ranked: list[WhatIfBranchCandidate] = []
    for thread in world.threads:
        history = thread_events(world.events, thread.thread_id)
        if not history:
            continue
        branch_event = choose_branch_event(history, requested_event_id=None)
        branch_index = next(
            (
                index
                for index, event in enumerate(history)
                if event.event_id == branch_event.event_id
            ),
            None,
        )
        if branch_index is None:
            continue
        past_events = list(history[:branch_index])
        future_events = list(history[branch_index:])
        case_context = build_case_context(
            snapshot=source_snapshot,
            events=world.events,
            case_id=branch_event.case_id,
            branch_thread_id=thread.thread_id,
            branch_timestamp_ms=branch_event.timestamp_ms,
        )
        situation_context = build_situation_context(
            world,
            branch_thread_id=thread.thread_id,
            branch_timestamp_ms=branch_event.timestamp_ms,
        )
        score, reasons = _branch_candidate_score(
            world=world,
            thread=thread,
            branch_event=branch_event,
            past_events=past_events,
            future_events=future_events,
            case_context=case_context,
            situation_context=situation_context,
        )
        ranked.append(
            WhatIfBranchCandidate(
                thread_id=thread.thread_id,
                branch_event_id=branch_event.event_id,
                subject=thread.subject,
                surface=thread.surface,
                history_event_count=len(past_events),
                future_event_count=len(future_events),
                case_id=branch_event.case_id,
                situation_surface_count=(
                    len(set(situation_context.surfaces))
                    if situation_context is not None
                    else 0
                ),
                linked_record_count=(
                    len(case_context.records) if case_context is not None else 0
                ),
                score=round(score, 3),
                reasons=reasons,
            )
        )
    ranked.sort(
        key=lambda item: (
            -item.score,
            -item.history_event_count,
            -item.future_event_count,
            item.thread_id,
        )
    )
    return WhatIfBranchCandidateResult(
        source=world.source,
        returned_count=min(len(ranked), resolved_limit),
        truncated=len(ranked) > resolved_limit,
        candidates=ranked[:resolved_limit],
    )


def load_world(
    *,
    source: str,
    source_dir: str | Path | None = None,
    rosetta_dir: str | Path | None = None,
    time_window: tuple[str, str] | None = None,
    custodian_filter: Sequence[str] | None = None,
    max_events: int | None = None,
    include_content: bool = False,
) -> WhatIfWorld:
    if source_dir is None and rosetta_dir is None:
        raise ValueError("source_dir is required")
    resolved_source_dir = (
        Path(source_dir if source_dir is not None else rosetta_dir)
        .expanduser()
        .resolve()
    )
    normalized_source = (source or "auto").strip().lower()
    if normalized_source in {"", "auto"}:
        normalized_source = detect_whatif_source(resolved_source_dir)
    if normalized_source == "enron":
        return load_enron_world(
            rosetta_dir=resolved_source_dir,
            scenarios=list_supported_scenarios(),
            time_window=time_window,
            custodian_filter=custodian_filter,
            max_events=max_events,
            include_content=include_content,
        )
    if normalized_source == "mail_archive":
        return load_mail_archive_world(
            source_dir=resolved_source_dir,
            scenarios=list_supported_scenarios(),
            time_window=time_window,
            max_events=max_events,
            include_content=include_content,
        )
    if normalized_source == "company_history":
        return load_company_history_world(
            source_dir=resolved_source_dir,
            scenarios=list_supported_scenarios(),
            time_window=time_window,
            max_events=max_events,
            include_content=include_content,
        )
    raise ValueError(f"unsupported what-if source: {source}")


def search_events(
    world: WhatIfWorld,
    *,
    actor: str | None = None,
    participant: str | None = None,
    thread_id: str | None = None,
    event_type: str | None = None,
    query: str | None = None,
    flagged_only: bool = False,
    limit: int = 20,
):
    return search_world_events(
        world,
        actor=actor,
        participant=participant,
        thread_id=thread_id,
        event_type=event_type,
        query=query,
        flagged_only=flagged_only,
        limit=limit,
    )


def run_whatif(
    world: WhatIfWorld,
    *,
    scenario: str | None = None,
    prompt: str | None = None,
) -> WhatIfResult:
    resolved = _resolve_scenario(scenario=scenario, prompt=prompt)
    thread_by_id = {thread.thread_id: thread for thread in world.threads}
    matched_events = _matched_events_for_scenario(
        world.events,
        thread_by_id,
        resolved.scenario_id,
        organization_domain=world.summary.organization_domain,
    )
    matched_thread_ids = sorted(
        {event.thread_id for event in matched_events if event.thread_id}
    )
    matched_actor_ids = sorted(
        {
            actor_id
            for event in matched_events
            for actor_id in {event.actor_id, event.target_id}
            if actor_id
        }
    )
    actor_impacts = _build_actor_impacts(
        matched_events,
        organization_domain=world.summary.organization_domain,
    )
    thread_impacts = _build_thread_impacts(
        matched_events,
        thread_by_id,
        resolved.scenario_id,
        organization_domain=world.summary.organization_domain,
    )
    consequences = _build_consequences(thread_impacts, actor_impacts)

    blocked_forward_count = sum(1 for event in matched_events if event.flags.is_forward)
    blocked_escalation_count = sum(
        1
        for event in matched_events
        if event.flags.is_escalation or event.event_type == "escalation"
    )
    delayed_assignment_count = sum(
        1 for event in matched_events if event.event_type == "assignment"
    )

    return WhatIfResult(
        scenario=resolved,
        prompt=prompt,
        world_summary=world.summary,
        matched_event_count=len(matched_events),
        affected_thread_count=len(matched_thread_ids),
        affected_actor_count=len(matched_actor_ids),
        blocked_forward_count=blocked_forward_count,
        blocked_escalation_count=blocked_escalation_count,
        delayed_assignment_count=delayed_assignment_count,
        timeline_impact=_timeline_impact(resolved.scenario_id, matched_events),
        top_actors=actor_impacts[:5],
        top_threads=thread_impacts[:5],
        top_consequences=consequences[:5],
        decision_branches=list(resolved.decision_branches),
    )


def select_specific_event(
    world: WhatIfWorld,
    *,
    thread_id: str | None,
    event_id: str | None,
    prompt: str,
) -> WhatIfResult:
    if event_id:
        event = event_by_id(world.events, event_id)
        if event is None:
            raise ValueError(f"event not found in world: {event_id}")
        selected_thread_id = event.thread_id
    elif thread_id:
        selected_thread_id = thread_id
        event = None
    else:
        selected_thread = recommend_branch_thread(world)
        selected_thread_id = selected_thread.thread_id
        event = None

    scenario = _resolve_scenario_from_specific_event(
        prompt=prompt,
        event=event,
        organization_domain=world.summary.organization_domain,
    )
    matching_thread = next(
        (thread for thread in world.threads if thread.thread_id == selected_thread_id),
        None,
    )
    if matching_thread is None:
        raise ValueError(f"thread not found in world: {selected_thread_id}")
    matching_events = [
        item for item in world.events if item.thread_id == selected_thread_id
    ]
    return WhatIfResult(
        scenario=scenario,
        prompt=prompt,
        world_summary=world.summary,
        matched_event_count=len(matching_events),
        affected_thread_count=1,
        affected_actor_count=len(matching_thread.actor_ids),
        blocked_forward_count=sum(
            1 for item in matching_events if item.flags.is_forward
        ),
        blocked_escalation_count=sum(
            1
            for item in matching_events
            if item.flags.is_escalation or item.event_type == "escalation"
        ),
        delayed_assignment_count=sum(
            1 for item in matching_events if item.event_type == "assignment"
        ),
        timeline_impact="Counterfactual replay from one explicit historical event.",
        top_threads=[
            WhatIfThreadImpact(
                thread_id=matching_thread.thread_id,
                subject=matching_thread.subject,
                affected_event_count=matching_thread.event_count,
                participant_count=len(matching_thread.actor_ids),
                reasons=["explicit_branch_point"],
            )
        ],
        top_actors=[
            WhatIfActorImpact(
                actor_id=actor_id,
                display_name=display_name(actor_id),
                affected_event_count=sum(
                    1
                    for item in matching_events
                    if actor_id in {item.actor_id, item.target_id}
                ),
                affected_thread_count=1,
                reasons=["explicit_branch_point"],
            )
            for actor_id in matching_thread.actor_ids[:5]
        ],
        top_consequences=[
            WhatIfConsequence(
                thread_id=matching_thread.thread_id,
                subject=matching_thread.subject,
                detail="This experiment was pinned to one explicit branch point.",
                severity="medium",
            )
        ],
        decision_branches=list(scenario.decision_branches),
    )


def _matched_events_for_scenario(
    events: Sequence[WhatIfEvent],
    thread_by_id: dict[str, WhatIfThreadSummary],
    scenario_id: WhatIfScenarioId,
    *,
    organization_domain: str,
) -> list[WhatIfEvent]:
    if scenario_id == "compliance_gateway":
        matched_threads = {
            thread_id
            for thread_id, thread in thread_by_id.items()
            if thread.legal_event_count > 0 and thread.trading_event_count > 0
        }
        return [event for event in events if event.thread_id in matched_threads]

    if scenario_id == "escalation_firewall":
        return [
            event
            for event in events
            if touches_executive(event)
            and (
                event.flags.is_escalation
                or event.flags.is_forward
                or event.event_type == "escalation"
            )
        ]

    if scenario_id == "external_dlp":
        return [
            event
            for event in events
            if event.flags.has_attachment_reference
            and has_external_recipients(
                event.flags.to_recipients,
                organization_domain=organization_domain,
            )
        ]

    if scenario_id == "approval_chain_enforcement":
        matched_threads = {
            thread_id
            for thread_id, thread in thread_by_id.items()
            if thread.assignment_event_count > 0 and thread.approval_event_count == 0
        }
        return [
            event
            for event in events
            if event.thread_id in matched_threads and event.event_type == "assignment"
        ]

    raise ValueError(f"unsupported what-if scenario: {scenario_id}")


def _source_snapshot_for_world(world: WhatIfWorld) -> ContextSnapshot | None:
    if world.source not in {"mail_archive", "company_history"}:
        return None
    try:
        return load_history_snapshot(world.source_dir)
    except Exception:  # noqa: BLE001
        return None


def _branch_candidate_score(
    *,
    world: WhatIfWorld,
    thread: WhatIfThreadSummary,
    branch_event: WhatIfEvent,
    past_events: Sequence[WhatIfEvent],
    future_events: Sequence[WhatIfEvent],
    case_context,
    situation_context,
) -> tuple[float, list[str]]:
    score = 0.0
    reasons: list[str] = []

    if past_events:
        score += min(len(past_events), 4) * 1.4
        reasons.append(f"history:{len(past_events)}")
    else:
        score -= 1.0
        reasons.append("thin_history")

    if future_events:
        score += min(len(future_events), 5) * 1.6
        reasons.append(f"future:{len(future_events)}")
    else:
        score -= 2.0
        reasons.append("thin_future")

    signal_labels = event_reason_labels(
        branch_event,
        organization_domain=world.summary.organization_domain,
    )
    if signal_labels:
        score += min(len(signal_labels), 4) * 1.1
        reasons.extend(signal_labels)

    if branch_event.event_type in {"assignment", "approval", "reply", "escalation"}:
        score += 1.0
        reasons.append(f"decision:{branch_event.event_type}")

    if case_context is not None and case_context.records:
        score += min(len(case_context.records), 3) * 0.8
        reasons.append(f"linked_records:{len(case_context.records)}")

    if situation_context is not None and len(set(situation_context.surfaces)) > 1:
        score += min(len(set(situation_context.surfaces)), 4) * 0.9
        reasons.append(f"cross_surface:{len(set(situation_context.surfaces))}")

    if thread.actor_ids:
        score += min(len(thread.actor_ids), 4) * 0.25

    return score, list(dict.fromkeys(reasons))


def _build_actor_impacts(
    events: Sequence[WhatIfEvent],
    *,
    organization_domain: str,
) -> list[WhatIfActorImpact]:
    counts: dict[str, dict[str, Any]] = {}
    for event in events:
        bucket = counts.setdefault(
            event.actor_id,
            {"count": 0, "threads": set(), "reasons": set()},
        )
        bucket["count"] += 1
        bucket["threads"].add(event.thread_id)
        bucket["reasons"].update(
            event_reason_labels(
                event,
                organization_domain=organization_domain,
            )
        )
    impacts = [
        WhatIfActorImpact(
            actor_id=actor_id,
            display_name=display_name(actor_id),
            affected_event_count=payload["count"],
            affected_thread_count=len(payload["threads"]),
            reasons=sorted(payload["reasons"]),
        )
        for actor_id, payload in counts.items()
        if actor_id
    ]
    return sorted(
        impacts,
        key=lambda item: (-item.affected_event_count, item.actor_id),
    )


def _build_thread_impacts(
    events: Sequence[WhatIfEvent],
    thread_by_id: dict[str, WhatIfThreadSummary],
    scenario_id: WhatIfScenarioId,
    *,
    organization_domain: str,
) -> list[WhatIfThreadImpact]:
    counts: dict[str, dict[str, Any]] = {}
    for event in events:
        bucket = counts.setdefault(
            event.thread_id,
            {"count": 0, "reasons": set()},
        )
        bucket["count"] += 1
        bucket["reasons"].update(
            event_reason_labels(
                event,
                organization_domain=organization_domain,
            )
        )
    impacts: list[WhatIfThreadImpact] = []
    for thread_id, payload in counts.items():
        thread = thread_by_id.get(thread_id)
        if thread is None:
            continue
        reasons = sorted(
            payload["reasons"] or _thread_reason_labels(thread, scenario_id)
        )
        impacts.append(
            WhatIfThreadImpact(
                thread_id=thread_id,
                subject=thread.subject,
                affected_event_count=payload["count"],
                participant_count=len(thread.actor_ids),
                reasons=reasons,
            )
        )
    return sorted(
        impacts,
        key=lambda item: (-item.affected_event_count, item.thread_id),
    )


def _build_consequences(
    thread_impacts: Sequence[WhatIfThreadImpact],
    actor_impacts: Sequence[WhatIfActorImpact],
) -> list[WhatIfConsequence]:
    consequences: list[WhatIfConsequence] = []
    for impact in thread_impacts[:3]:
        detail = (
            f"{impact.affected_event_count} events across {impact.participant_count} "
            f"participants would move under the alternate rule."
        )
        consequences.append(
            WhatIfConsequence(
                thread_id=impact.thread_id,
                subject=impact.subject,
                detail=detail,
                severity="high" if impact.affected_event_count >= 3 else "medium",
            )
        )
    for impact in actor_impacts[:2]:
        detail = (
            f"{impact.display_name} appears in {impact.affected_event_count} matched "
            "events and would likely see their thread flow change."
        )
        consequences.append(
            WhatIfConsequence(
                thread_id="",
                subject=impact.display_name,
                actor_id=impact.actor_id,
                detail=detail,
                severity="medium",
            )
        )
    return consequences


def _timeline_impact(
    scenario_id: WhatIfScenarioId,
    events: Sequence[WhatIfEvent],
) -> str:
    if not events:
        return "No historical events matched this rule."
    if scenario_id == "compliance_gateway":
        return "Adds a review gate before forwarding or escalation on matched threads."
    if scenario_id == "escalation_firewall":
        return "Introduces one extra approval hop before executive escalation."
    if scenario_id == "external_dlp":
        return "Holds external attachment sends until review completes."
    return "Requires approval before the next assignment handoff proceeds."


def _thread_reason_labels(
    thread: WhatIfThreadSummary,
    scenario_id: WhatIfScenarioId,
) -> list[str]:
    if scenario_id == "compliance_gateway":
        return ["legal", "trading"]
    if scenario_id == "escalation_firewall":
        return ["executive_escalation"]
    if scenario_id == "external_dlp":
        return ["attachment", "external_recipient"]
    return ["assignment_without_approval"]
