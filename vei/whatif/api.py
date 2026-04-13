from __future__ import annotations

import asyncio
import json
from pathlib import Path
import shutil
from typing import Any, Sequence

from vei.project_settings import default_model_for_provider
from vei.blueprint.api import create_world_session_from_blueprint
from vei.blueprint.models import BlueprintAsset
from vei.context.models import ContextSnapshot, ContextSourceResult
from vei.data.models import BaseEvent, DatasetMetadata, VEIDataset
from vei.llm import providers
from vei.twin import load_customer_twin
from vei.twin.api import build_customer_twin
from vei.twin.models import ContextMoldConfig

try:
    from dotenv import load_dotenv
except Exception:  # pragma: no cover

    def load_dotenv(*args: object, **kwargs: object) -> None:
        return None


from .models import (
    WhatIfActorImpact,
    WhatIfCaseContext,
    WhatIfCandidateIntervention,
    WhatIfCandidateRanking,
    WhatIfConsequence,
    WhatIfDecisionOption,
    WhatIfDecisionScene,
    WhatIfEpisodeManifest,
    WhatIfEpisodeMaterialization,
    WhatIfEvent,
    WhatIfEventReference,
    WhatIfEventSearchResult,
    WhatIfForecast,
    WhatIfForecastBackend,
    WhatIfForecastDelta,
    WhatIfForecastResult,
    WhatIfExperimentArtifacts,
    WhatIfExperimentMode,
    WhatIfExperimentResult,
    WhatIfObjectivePackId,
    WhatIfOutcomeSignals,
    WhatIfPublicContext,
    WhatIfReplaySummary,
    WhatIfRankedExperimentArtifacts,
    WhatIfRankedExperimentResult,
    WhatIfRankedRolloutResult,
    WhatIfShadowOutcomeScore,
    WhatIfInterventionSpec,
    WhatIfLLMGeneratedMessage,
    WhatIfLLMReplayResult,
    WhatIfLLMUsage,
    WhatIfResult,
    WhatIfScenarioId,
    WhatIfThreadImpact,
    WhatIfThreadSummary,
    WhatIfWorld,
)
from .corpus import (
    CONTENT_NOTICE,
    ENRON_DOMAIN,
    load_history_snapshot,
    choose_branch_event,
    detect_whatif_source,
    display_name,
    event_by_id,
    event_reason_labels,
    event_reference,
    has_external_recipients,
    hydrate_event_snippets,
    load_company_history_world,
    load_mail_archive_world,
    load_enron_world,
    safe_int,
    search_events as search_world_events,
    thread_events,
    thread_subject,
    touches_executive,
)
from .cases import build_case_context, case_context_prompt_lines
from .ejepa import default_forecast_backend, run_ejepa_counterfactual
from .interventions import intervention_tags
from .public_context import (
    public_context_prompt_lines,
    slice_public_context_to_branch,
)
from .ranking import (
    aggregate_outcome_signals,
    get_objective_pack,
    list_objective_packs as list_historical_objective_packs,
    recommendation_reason,
    score_outcome_signals,
    sort_candidates_for_rank,
    summarize_forecast_branch,
    summarize_llm_branch,
)
from .artifacts import (
    render_experiment_overview as _render_experiment_overview,
    render_ranked_experiment_overview as _render_ranked_experiment_overview,
    slug_artifact_label as _slug,
)
from .business_state import (
    assess_historical_business_state,
    describe_forecast_business_change,
)
from .scenario_registry import (
    list_supported_scenarios,
    resolve_scenario as _resolve_scenario,
    resolve_scenario_from_specific_event as _resolve_scenario_from_specific_event,
)


def list_objective_packs():
    return list_historical_objective_packs()


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
) -> WhatIfEventSearchResult:
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


def materialize_episode(
    world: WhatIfWorld,
    *,
    root: str | Path,
    thread_id: str | None = None,
    event_id: str | None = None,
    organization_name: str | None = None,
    organization_domain: str | None = None,
) -> WhatIfEpisodeMaterialization:
    workspace_root = Path(root).expanduser().resolve()
    resolved_organization_name = (
        (organization_name or "").strip()
        or world.summary.organization_name
        or "Historical Archive"
    )
    resolved_organization_domain = (
        (organization_domain or "").strip().lower()
        or world.summary.organization_domain
        or "archive.local"
    )
    (
        selected_thread_id,
        thread_history,
        branch_event,
        past_events,
        future_events,
        selected_thread_subject,
    ) = _resolve_thread_branch(
        world,
        thread_id=thread_id,
        event_id=event_id,
    )
    branch_public_context = slice_public_context_to_branch(
        world.public_context,
        branch_timestamp=branch_event.timestamp,
    )
    source_snapshot = _source_snapshot_for_world(world)
    case_context = build_case_context(
        snapshot=source_snapshot,
        events=world.events,
        case_id=branch_event.case_id,
        branch_thread_id=selected_thread_id,
        branch_timestamp_ms=branch_event.timestamp_ms,
    )
    forecast = forecast_episode(
        future_events,
        organization_domain=resolved_organization_domain,
    )
    historical_business_state = assess_historical_business_state(
        branch_event=event_reference(branch_event),
        forecast=forecast,
        organization_domain=resolved_organization_domain,
        public_context=branch_public_context,
    )
    history_preview = [event_reference(event) for event in past_events[-12:]]
    snapshot = _episode_context_snapshot(
        thread_history=thread_history,
        past_events=past_events,
        thread_id=selected_thread_id,
        thread_subject=selected_thread_subject,
        organization_name=resolved_organization_name,
        organization_domain=resolved_organization_domain,
        world=world,
        branch_event=branch_event,
        public_context=branch_public_context,
        case_context=case_context,
        historical_business_state=historical_business_state,
        source_snapshot=source_snapshot,
    )
    included_surfaces = _included_surfaces_for_thread(
        thread_history,
        case_context=case_context,
    )
    bundle = build_customer_twin(
        workspace_root,
        snapshot=snapshot,
        organization_name=resolved_organization_name,
        organization_domain=resolved_organization_domain,
        mold=ContextMoldConfig(
            archetype="b2b_saas",
            density_level="medium",
            named_team_expansion="minimal",
            included_surfaces=included_surfaces,
            synthetic_expansion_strength="light",
        ),
        overwrite=True,
    )
    baseline_dataset = _baseline_dataset(
        thread_subject=selected_thread_subject,
        branch_event=branch_event,
        future_events=future_events,
        organization_domain=resolved_organization_domain,
        source_name=world.source,
    )
    baseline_dataset_path = workspace_root / "whatif_baseline_dataset.json"
    baseline_dataset_path.write_text(
        baseline_dataset.model_dump_json(indent=2),
        encoding="utf-8",
    )
    _persist_workspace_historical_source(world, workspace_root)
    _persist_workspace_public_context(world, workspace_root)
    manifest = WhatIfEpisodeManifest(
        source=world.source,
        source_dir=world.source_dir,
        workspace_root=workspace_root,
        organization_name=resolved_organization_name,
        organization_domain=resolved_organization_domain,
        thread_id=selected_thread_id,
        thread_subject=selected_thread_subject,
        case_id=branch_event.case_id,
        surface=branch_event.surface,
        branch_event_id=branch_event.event_id,
        branch_timestamp=branch_event.timestamp,
        branch_event=event_reference(branch_event),
        history_message_count=len(past_events),
        future_event_count=len(future_events),
        baseline_dataset_path=str(baseline_dataset_path.relative_to(workspace_root)),
        content_notice=str(world.metadata.get("content_notice", CONTENT_NOTICE)),
        actor_ids=sorted(
            {
                actor_id
                for event in thread_history
                for actor_id in {event.actor_id, event.target_id}
                if actor_id
            }
        ),
        history_preview=history_preview,
        baseline_future_preview=[event_reference(event) for event in future_events[:5]],
        forecast=forecast,
        public_context=branch_public_context,
        case_context=case_context,
        historical_business_state=historical_business_state,
    )
    manifest_path = workspace_root / "whatif_episode_manifest.json"
    manifest_path.write_text(manifest.model_dump_json(indent=2), encoding="utf-8")
    return WhatIfEpisodeMaterialization(
        manifest_path=manifest_path,
        bundle_path=workspace_root / "twin_manifest.json",
        context_snapshot_path=workspace_root / bundle.context_snapshot_path,
        baseline_dataset_path=baseline_dataset_path,
        workspace_root=workspace_root,
        organization_name=resolved_organization_name,
        organization_domain=resolved_organization_domain,
        thread_id=selected_thread_id,
        case_id=branch_event.case_id,
        surface=branch_event.surface,
        branch_event_id=branch_event.event_id,
        branch_event=manifest.branch_event,
        history_message_count=len(past_events),
        future_event_count=len(future_events),
        history_preview=history_preview,
        baseline_future_preview=list(manifest.baseline_future_preview),
        forecast=forecast,
        public_context=branch_public_context,
        case_context=case_context,
        historical_business_state=historical_business_state,
    )


def build_decision_scene(
    world: WhatIfWorld,
    *,
    thread_id: str | None = None,
    event_id: str | None = None,
    history_limit: int = 6,
    future_limit: int = 5,
) -> WhatIfDecisionScene:
    (
        selected_thread_id,
        _thread_history,
        branch_event,
        past_events,
        future_events,
        selected_thread_subject,
    ) = _resolve_thread_branch(
        world,
        thread_id=thread_id,
        event_id=event_id,
    )
    organization_name = world.summary.organization_name or "Historical Archive"
    organization_domain = world.summary.organization_domain or "archive.local"
    branch_reference = event_reference(branch_event)
    branch_public_context = slice_public_context_to_branch(
        world.public_context,
        branch_timestamp=branch_event.timestamp,
    )
    case_context = build_case_context(
        snapshot=_source_snapshot_for_world(world),
        events=world.events,
        case_id=branch_event.case_id,
        branch_thread_id=selected_thread_id,
        branch_timestamp_ms=branch_event.timestamp_ms,
    )
    forecast = forecast_episode(
        future_events,
        organization_domain=organization_domain,
    )
    historical_business_state = assess_historical_business_state(
        branch_event=branch_reference,
        forecast=forecast,
        organization_domain=organization_domain,
        public_context=branch_public_context,
    )
    return WhatIfDecisionScene(
        source=world.source,
        organization_name=organization_name,
        organization_domain=organization_domain,
        thread_id=selected_thread_id,
        thread_subject=selected_thread_subject,
        case_id=branch_reference.case_id,
        surface=branch_reference.surface,
        branch_event_id=branch_event.event_id,
        branch_event=branch_reference,
        history_message_count=len(past_events),
        future_event_count=len(future_events),
        content_notice=str(world.metadata.get("content_notice", CONTENT_NOTICE)),
        branch_summary=_decision_branch_summary(
            branch_reference,
            thread_subject=selected_thread_subject,
            organization_domain=organization_domain,
        ),
        historical_action_summary=_historical_action_summary(
            branch_reference,
            thread_subject=selected_thread_subject,
            organization_domain=organization_domain,
        ),
        historical_outcome_summary=_historical_outcome_summary(forecast),
        stakes_summary=_decision_stakes_summary(
            branch_reference,
            forecast,
            organization_domain=organization_domain,
        ),
        decision_question=_decision_question(selected_thread_subject),
        history_preview=[
            event_reference(event) for event in past_events[-max(1, history_limit) :]
        ],
        historical_future_preview=[
            event_reference(event) for event in future_events[: max(1, future_limit)]
        ],
        candidate_options=_decision_options_for_branch(
            branch_reference,
            thread_subject=selected_thread_subject,
            organization_name=organization_name,
            organization_domain=organization_domain,
        ),
        public_context=branch_public_context,
        case_context=case_context,
        historical_business_state=historical_business_state,
    )


def build_saved_decision_scene(
    root: str | Path,
    *,
    history_limit: int = 6,
    future_limit: int = 5,
) -> WhatIfDecisionScene:
    workspace_root = Path(root).expanduser().resolve()
    manifest = load_episode_manifest(workspace_root)
    history_preview = _history_preview_from_saved_context(
        workspace_root,
        manifest=manifest,
        history_limit=history_limit,
    )
    return WhatIfDecisionScene(
        source=manifest.source,
        organization_name=manifest.organization_name,
        organization_domain=manifest.organization_domain,
        thread_id=manifest.thread_id,
        thread_subject=manifest.thread_subject,
        case_id=manifest.case_id,
        surface=manifest.surface,
        branch_event_id=manifest.branch_event_id,
        branch_event=manifest.branch_event,
        history_message_count=manifest.history_message_count,
        future_event_count=manifest.future_event_count,
        content_notice=manifest.content_notice,
        branch_summary=_decision_branch_summary(
            manifest.branch_event,
            thread_subject=manifest.thread_subject,
            organization_domain=manifest.organization_domain,
        ),
        historical_action_summary=_historical_action_summary(
            manifest.branch_event,
            thread_subject=manifest.thread_subject,
            organization_domain=manifest.organization_domain,
        ),
        historical_outcome_summary=_historical_outcome_summary(manifest.forecast),
        stakes_summary=_decision_stakes_summary(
            manifest.branch_event,
            manifest.forecast,
            organization_domain=manifest.organization_domain,
        ),
        decision_question=_decision_question(manifest.thread_subject),
        history_preview=history_preview,
        historical_future_preview=list(manifest.baseline_future_preview[:future_limit]),
        candidate_options=_decision_options_for_branch(
            manifest.branch_event,
            thread_subject=manifest.thread_subject,
            organization_name=manifest.organization_name,
            organization_domain=manifest.organization_domain,
        ),
        public_context=manifest.public_context,
        case_context=manifest.case_context,
        historical_business_state=manifest.historical_business_state,
    )


def _resolve_thread_branch(
    world: WhatIfWorld,
    *,
    thread_id: str | None = None,
    event_id: str | None = None,
) -> tuple[
    str, list[WhatIfEvent], WhatIfEvent, list[WhatIfEvent], list[WhatIfEvent], str
]:
    selected_thread_id = thread_id
    if selected_thread_id is None:
        if not event_id:
            raise ValueError("provide thread_id or event_id")
        selected_event = event_by_id(world.events, event_id)
        if selected_event is None:
            raise ValueError(f"event not found in world: {event_id}")
        selected_thread_id = selected_event.thread_id

    thread_history = thread_events(world.events, selected_thread_id)
    if not thread_history:
        raise ValueError(f"thread not found in world: {selected_thread_id}")
    if world.source == "enron":
        thread_history = hydrate_event_snippets(
            rosetta_dir=world.source_dir,
            events=thread_history,
        )

    branch_event = choose_branch_event(thread_history, requested_event_id=event_id)
    branch_index = next(
        (
            index
            for index, event in enumerate(thread_history)
            if event.event_id == branch_event.event_id
        ),
        None,
    )
    if branch_index is None:
        raise ValueError(f"branch event not found in thread: {branch_event.event_id}")

    return (
        selected_thread_id,
        thread_history,
        branch_event,
        list(thread_history[:branch_index]),
        list(thread_history[branch_index:]),
        thread_subject(
            world.threads,
            selected_thread_id,
            fallback=branch_event.subject,
        ),
    )


def _episode_context_snapshot(
    *,
    thread_history: Sequence[WhatIfEvent],
    past_events: Sequence[WhatIfEvent],
    thread_id: str,
    thread_subject: str,
    organization_name: str,
    organization_domain: str,
    world: WhatIfWorld,
    branch_event: WhatIfEvent,
    public_context: WhatIfPublicContext | None,
    case_context: WhatIfCaseContext | None,
    historical_business_state,
    source_snapshot: ContextSnapshot | None,
) -> ContextSnapshot:
    metadata = _episode_snapshot_metadata(
        world=world,
        thread_id=thread_id,
        branch_event=branch_event,
        public_context=public_context,
        case_context=case_context,
        historical_business_state=historical_business_state,
    )
    actor_payload = _thread_actor_payload(world, thread_history=thread_history)
    surface = branch_event.surface or "mail"
    if surface == "mail":
        snapshot = _mail_episode_snapshot(
            past_events=past_events,
            thread_id=thread_id,
            thread_subject=thread_subject,
            organization_name=organization_name,
            organization_domain=organization_domain,
            actor_payload=actor_payload,
            metadata=metadata,
        )
        return _append_case_context_sources(
            snapshot=snapshot,
            case_context=case_context,
            source_snapshot=source_snapshot,
        )
    if surface == "slack":
        snapshot = _chat_episode_snapshot(
            past_events=past_events,
            branch_event=branch_event,
            thread_id=thread_id,
            thread_subject=thread_subject,
            organization_name=organization_name,
            organization_domain=organization_domain,
            actor_payload=actor_payload,
            metadata=metadata,
        )
        return _append_case_context_sources(
            snapshot=snapshot,
            case_context=case_context,
            source_snapshot=source_snapshot,
        )
    if surface == "tickets":
        snapshot = _ticket_episode_snapshot(
            past_events=past_events,
            branch_event=branch_event,
            thread_id=thread_id,
            thread_subject=thread_subject,
            organization_name=organization_name,
            organization_domain=organization_domain,
            actor_payload=actor_payload,
            metadata=metadata,
        )
        return _append_case_context_sources(
            snapshot=snapshot,
            case_context=case_context,
            source_snapshot=source_snapshot,
        )
    raise ValueError(f"unsupported historical surface: {surface}")


def _episode_snapshot_metadata(
    *,
    world: WhatIfWorld,
    thread_id: str,
    branch_event: WhatIfEvent,
    public_context: WhatIfPublicContext | None,
    case_context: WhatIfCaseContext | None,
    historical_business_state,
) -> dict[str, Any]:
    return {
        "whatif": {
            "source": world.source,
            "thread_id": thread_id,
            "case_id": branch_event.case_id,
            "branch_event_id": branch_event.event_id,
            "branch_surface": branch_event.surface,
            "content_notice": str(world.metadata.get("content_notice", CONTENT_NOTICE)),
            "public_context": (
                public_context.model_dump(mode="json")
                if public_context is not None
                else None
            ),
            "case_context": (
                case_context.model_dump(mode="json")
                if case_context is not None
                else None
            ),
            "historical_business_state": (
                historical_business_state.model_dump(mode="json")
                if historical_business_state is not None
                else None
            ),
        }
    }


def _thread_actor_payload(
    world: WhatIfWorld,
    *,
    thread_history: Sequence[WhatIfEvent],
) -> list[dict[str, str]]:
    actor_ids = {
        actor_id
        for event in thread_history
        for actor_id in {event.actor_id, event.target_id}
        if actor_id
    }
    return [
        {
            "actor_id": actor.actor_id,
            "email": actor.email,
            "display_name": actor.display_name,
        }
        for actor in world.actors
        if actor.actor_id in actor_ids
    ]


def _mail_episode_snapshot(
    *,
    past_events: Sequence[WhatIfEvent],
    thread_id: str,
    thread_subject: str,
    organization_name: str,
    organization_domain: str,
    actor_payload: Sequence[dict[str, str]],
    metadata: dict[str, Any],
) -> ContextSnapshot:
    archive_threads = [
        {
            "thread_id": thread_id,
            "subject": thread_subject,
            "category": "historical",
            "messages": [
                _archive_message_payload(
                    event,
                    base_time_ms=index * 1000,
                    organization_domain=organization_domain,
                )
                for index, event in enumerate(past_events)
            ],
        }
    ]
    return ContextSnapshot(
        organization_name=organization_name,
        organization_domain=organization_domain,
        sources=[
            ContextSourceResult(
                provider="mail_archive",
                captured_at="",
                status="ok",
                record_counts={
                    "threads": len(archive_threads),
                    "messages": sum(
                        len(thread["messages"]) for thread in archive_threads
                    ),
                    "actors": len(actor_payload),
                },
                data={
                    "threads": archive_threads,
                    "actors": list(actor_payload),
                    "profile": {},
                },
            )
        ],
        metadata=metadata,
    )


def _chat_episode_snapshot(
    *,
    past_events: Sequence[WhatIfEvent],
    branch_event: WhatIfEvent,
    thread_id: str,
    thread_subject: str,
    organization_name: str,
    organization_domain: str,
    actor_payload: Sequence[dict[str, str]],
    metadata: dict[str, Any],
) -> ContextSnapshot:
    provider = (
        branch_event.flags.source
        if branch_event.flags.source in {"slack", "teams"}
        else "slack"
    )
    channel_name = _chat_channel_name(branch_event)
    channel_messages = [
        {
            "ts": _chat_message_ts(event, fallback_index=index + 1),
            "user": event.actor_id,
            "text": _historical_chat_text(event),
            "thread_ts": (
                event.conversation_anchor
                if event.conversation_anchor
                and event.conversation_anchor
                != _chat_message_ts(event, fallback_index=index + 1)
                else None
            ),
        }
        for index, event in enumerate(past_events)
    ]
    return ContextSnapshot(
        organization_name=organization_name,
        organization_domain=organization_domain,
        sources=[
            ContextSourceResult(
                provider=provider,
                captured_at="",
                status="ok",
                record_counts={
                    "channels": 1,
                    "messages": len(channel_messages),
                    "users": len(actor_payload),
                },
                data={
                    "channels": [
                        {
                            "channel": channel_name,
                            "channel_id": channel_name,
                            "unread": 0,
                            "messages": channel_messages,
                        }
                    ],
                    "users": [
                        {
                            "id": actor["actor_id"],
                            "name": actor["actor_id"],
                            "real_name": actor["display_name"] or actor["actor_id"],
                            "email": actor["email"],
                        }
                        for actor in actor_payload
                    ],
                    "profile": {
                        "thread_id": thread_id,
                        "thread_subject": thread_subject,
                    },
                },
            )
        ],
        metadata=metadata,
    )


def _ticket_episode_snapshot(
    *,
    past_events: Sequence[WhatIfEvent],
    branch_event: WhatIfEvent,
    thread_id: str,
    thread_subject: str,
    organization_name: str,
    organization_domain: str,
    actor_payload: Sequence[dict[str, str]],
    metadata: dict[str, Any],
) -> ContextSnapshot:
    latest_state = past_events[-1] if past_events else branch_event
    comments = [
        {
            "id": event.event_id,
            "author": event.actor_id,
            "body": event.snippet,
            "created": event.timestamp,
        }
        for event in past_events
        if event.event_type in {"reply", "message"}
    ]
    return ContextSnapshot(
        organization_name=organization_name,
        organization_domain=organization_domain,
        sources=[
            ContextSourceResult(
                provider="jira",
                captured_at="",
                status="ok",
                record_counts={
                    "issues": 1,
                    "comments": len(comments),
                    "actors": len(actor_payload),
                },
                data={
                    "issues": [
                        {
                            "ticket_id": thread_id.split(":", 1)[-1],
                            "title": thread_subject,
                            "status": _ticket_status_for_event(latest_state),
                            "assignee": latest_state.actor_id or branch_event.actor_id,
                            "description": latest_state.snippet or thread_subject,
                            "updated": latest_state.timestamp or branch_event.timestamp,
                            "comments": comments,
                        }
                    ],
                    "projects": [],
                },
            )
        ],
        metadata=metadata,
    )


def _source_snapshot_for_world(world: WhatIfWorld) -> ContextSnapshot | None:
    if world.source not in {"mail_archive", "company_history"}:
        return None
    try:
        return load_history_snapshot(world.source_dir)
    except Exception:  # noqa: BLE001
        return None


def _append_case_context_sources(
    *,
    snapshot: ContextSnapshot,
    case_context: WhatIfCaseContext | None,
    source_snapshot: ContextSnapshot | None,
) -> ContextSnapshot:
    if case_context is None:
        return snapshot

    extra_sources: list[ContextSourceResult] = []
    extra_sources.extend(_case_history_source_results(case_context))
    if source_snapshot is not None:
        extra_sources.extend(
            _case_record_source_results(
                case_context=case_context,
                source_snapshot=source_snapshot,
            )
        )

    if not extra_sources:
        return snapshot

    merged_sources = list(snapshot.sources)
    for source in extra_sources:
        existing_index = next(
            (
                index
                for index, existing in enumerate(merged_sources)
                if existing.provider == source.provider
            ),
            None,
        )
        if existing_index is None:
            merged_sources.append(source)
            continue
        merged_sources[existing_index] = _merge_context_source_result(
            merged_sources[existing_index],
            source,
        )
    return snapshot.model_copy(update={"sources": merged_sources})


def _case_history_source_results(
    case_context: WhatIfCaseContext,
) -> list[ContextSourceResult]:
    references_by_provider: dict[str, list[WhatIfEventReference]] = {}
    for reference in case_context.related_history:
        provider = _history_provider_for_reference(reference)
        if not provider:
            continue
        references_by_provider.setdefault(provider, []).append(reference)

    sources: list[ContextSourceResult] = []
    for provider, references in references_by_provider.items():
        if provider in {"slack", "teams"}:
            source = _chat_case_history_source(provider=provider, references=references)
        elif provider == "jira":
            source = _ticket_case_history_source(references)
        elif provider == "mail_archive":
            source = _mail_case_history_source(references)
        else:
            source = None
        if source is not None:
            sources.append(source)
    return sources


def _history_provider_for_reference(reference: WhatIfEventReference) -> str | None:
    if reference.surface == "tickets":
        return "jira"
    if reference.surface == "mail":
        return "mail_archive"
    if reference.surface == "slack":
        provider = reference.thread_id.split(":", 1)[0].strip().lower()
        if provider in {"slack", "teams"}:
            return provider
        return "slack"
    return None


def _mail_case_history_source(
    references: Sequence[WhatIfEventReference],
) -> ContextSourceResult | None:
    grouped: dict[str, list[WhatIfEventReference]] = {}
    for reference in sorted(
        references, key=lambda item: (item.timestamp, item.event_id)
    ):
        grouped.setdefault(reference.thread_id, []).append(reference)
    if not grouped:
        return None

    actor_payload = _history_actor_payload_from_references(references)
    threads = []
    for thread_id, thread_references in grouped.items():
        subject = next(
            (
                reference.subject
                for reference in thread_references
                if reference.subject.strip()
            ),
            thread_id,
        )
        messages = [
            {
                "message_id": reference.event_id,
                "from": reference.actor_id,
                "to": _reference_primary_recipient(reference),
                "subject": reference.subject or subject,
                "body_text": _reference_body(reference),
                "unread": False,
                "time_ms": index * 1000,
            }
            for index, reference in enumerate(thread_references, start=1)
        ]
        threads.append(
            {
                "thread_id": thread_id,
                "subject": subject,
                "category": "historical",
                "messages": messages,
            }
        )
    data = {
        "threads": threads,
        "actors": actor_payload,
        "profile": {},
    }
    return ContextSourceResult(
        provider="mail_archive",
        captured_at="",
        status="ok",
        record_counts=_context_source_record_counts("mail_archive", data),
        data=data,
    )


def _chat_case_history_source(
    *,
    provider: str,
    references: Sequence[WhatIfEventReference],
) -> ContextSourceResult | None:
    grouped: dict[str, list[WhatIfEventReference]] = {}
    for reference in sorted(
        references, key=lambda item: (item.timestamp, item.event_id)
    ):
        grouped.setdefault(_chat_channel_name_from_reference(reference), []).append(
            reference
        )
    if not grouped:
        return None

    channels = []
    for channel_name, channel_references in grouped.items():
        messages = []
        for index, reference in enumerate(channel_references, start=1):
            root_anchor = reference.conversation_anchor or str(index * 1000)
            messages.append(
                {
                    "ts": (
                        root_anchor
                        if not reference.is_reply
                        else f"{root_anchor}.{index}"
                    ),
                    "user": reference.actor_id,
                    "text": _reference_body(reference),
                    "thread_ts": root_anchor if reference.is_reply else None,
                }
            )
        channels.append(
            {
                "channel": channel_name,
                "channel_id": channel_name,
                "unread": 0,
                "messages": messages,
            }
        )
    data = {
        "channels": channels,
        "users": _history_chat_users_from_references(references),
        "profile": {},
    }
    return ContextSourceResult(
        provider=provider,
        captured_at="",
        status="ok",
        record_counts=_context_source_record_counts(provider, data),
        data=data,
    )


def _ticket_case_history_source(
    references: Sequence[WhatIfEventReference],
) -> ContextSourceResult | None:
    grouped: dict[str, list[WhatIfEventReference]] = {}
    for reference in sorted(
        references, key=lambda item: (item.timestamp, item.event_id)
    ):
        ticket_id = reference.thread_id.split(":", 1)[-1].strip()
        if not ticket_id:
            continue
        grouped.setdefault(ticket_id, []).append(reference)
    if not grouped:
        return None

    issues = []
    for ticket_id, ticket_references in grouped.items():
        latest = ticket_references[-1]
        comments = [
            {
                "id": reference.event_id,
                "author": reference.actor_id,
                "body": _reference_body(reference),
                "created": reference.timestamp,
            }
            for reference in ticket_references
            if reference.event_type in {"reply", "message", "escalation"}
            or reference.snippet.strip()
        ]
        issues.append(
            {
                "ticket_id": ticket_id,
                "title": latest.subject or ticket_id,
                "status": _ticket_status_for_reference(latest),
                "assignee": latest.actor_id,
                "description": _reference_body(ticket_references[0]),
                "updated": latest.timestamp,
                "comments": comments,
            }
        )
    data = {
        "issues": issues,
        "projects": [],
    }
    return ContextSourceResult(
        provider="jira",
        captured_at="",
        status="ok",
        record_counts=_context_source_record_counts("jira", data),
        data=data,
    )


def _history_actor_payload_from_references(
    references: Sequence[WhatIfEventReference],
) -> list[dict[str, str]]:
    actors: dict[str, dict[str, str]] = {}
    for reference in references:
        for actor_id in {reference.actor_id, reference.target_id}:
            normalized = str(actor_id or "").strip()
            if not normalized:
                continue
            actors.setdefault(
                normalized,
                {
                    "actor_id": normalized,
                    "email": normalized,
                    "display_name": display_name(normalized),
                },
            )
    return list(actors.values())


def _history_chat_users_from_references(
    references: Sequence[WhatIfEventReference],
) -> list[dict[str, str]]:
    users: dict[str, dict[str, str]] = {}
    for reference in references:
        actor_id = str(reference.actor_id or "").strip()
        if not actor_id:
            continue
        users.setdefault(
            actor_id,
            {
                "id": actor_id,
                "name": actor_id,
                "real_name": display_name(actor_id),
                "email": actor_id,
            },
        )
    return list(users.values())


def _reference_primary_recipient(reference: WhatIfEventReference) -> str:
    recipients = [item for item in reference.to_recipients if item]
    if recipients:
        return recipients[0]
    if reference.target_id:
        return reference.target_id
    return ""


def _reference_body(reference: WhatIfEventReference) -> str:
    if reference.snippet.strip():
        return reference.snippet
    return reference.subject or reference.thread_id


def _ticket_status_for_reference(reference: WhatIfEventReference) -> str:
    if reference.event_type == "approval":
        return "resolved"
    if reference.event_type == "escalation":
        return "blocked"
    if reference.event_type == "assignment":
        return "in_progress"
    return "open"


def _case_record_source_results(
    *,
    case_context: WhatIfCaseContext,
    source_snapshot: ContextSnapshot,
) -> list[ContextSourceResult]:
    record_ids_by_provider: dict[str, set[str]] = {}
    for record in case_context.records:
        provider = record.provider.strip().lower()
        record_id = record.record_id.strip()
        if not provider or not record_id:
            continue
        record_ids_by_provider.setdefault(provider, set()).add(record_id)

    sources: list[ContextSourceResult] = []
    google_source = _filtered_google_record_source(
        source_snapshot=source_snapshot,
        record_ids=record_ids_by_provider.get("google", set()),
    )
    if google_source is not None:
        sources.append(google_source)
    for provider in ("crm", "salesforce"):
        source = _filtered_crm_record_source(
            source_snapshot=source_snapshot,
            provider=provider,
            record_ids=record_ids_by_provider.get(provider, set()),
        )
        if source is not None:
            sources.append(source)
    return sources


def _filtered_google_record_source(
    *,
    source_snapshot: ContextSnapshot,
    record_ids: set[str],
) -> ContextSourceResult | None:
    if not record_ids:
        return None
    google_source = source_snapshot.source_for("google")
    if google_source is None or not isinstance(google_source.data, dict):
        return None
    documents = [
        item
        for item in google_source.data.get("documents", [])
        if isinstance(item, dict) and str(item.get("doc_id", "")).strip() in record_ids
    ]
    if not documents:
        return None
    data = {"documents": documents}
    return ContextSourceResult(
        provider="google",
        captured_at=google_source.captured_at,
        status=google_source.status,
        record_counts=_context_source_record_counts("google", data),
        data=data,
    )


def _filtered_crm_record_source(
    *,
    source_snapshot: ContextSnapshot,
    provider: str,
    record_ids: set[str],
) -> ContextSourceResult | None:
    if not record_ids:
        return None
    source = source_snapshot.source_for(provider)
    if source is None or not isinstance(source.data, dict):
        return None
    deals = [
        item
        for item in source.data.get("deals", [])
        if isinstance(item, dict)
        and str(item.get("id", item.get("deal_id", ""))).strip() in record_ids
    ]
    if not deals:
        return None
    data = {"deals": deals}
    return ContextSourceResult(
        provider=provider,
        captured_at=source.captured_at,
        status=source.status,
        record_counts=_context_source_record_counts(provider, data),
        data=data,
    )


def _merge_context_source_result(
    existing: ContextSourceResult,
    extra: ContextSourceResult,
) -> ContextSourceResult:
    merged_data = _merge_context_source_data(
        provider=existing.provider,
        existing=existing.data,
        extra=extra.data,
    )
    return existing.model_copy(
        update={
            "status": _merge_context_source_status(existing.status, extra.status),
            "record_counts": _context_source_record_counts(
                existing.provider, merged_data
            ),
            "data": merged_data,
            "error": existing.error or extra.error,
        }
    )


def _merge_context_source_status(
    left: str,
    right: str,
) -> str:
    statuses = {left, right}
    if "error" in statuses:
        return "partial"
    if "partial" in statuses:
        return "partial"
    return "ok"


def _merge_context_source_data(
    *,
    provider: str,
    existing: dict[str, Any],
    extra: dict[str, Any],
) -> dict[str, Any]:
    if provider in {"mail_archive", "gmail"}:
        return {
            "threads": _merge_mail_threads(
                existing.get("threads", []),
                extra.get("threads", []),
            ),
            "actors": _merge_keyed_dict_items(
                existing.get("actors", []),
                extra.get("actors", []),
                key_names=("actor_id", "email"),
            ),
            "profile": _merge_mapping(existing.get("profile"), extra.get("profile")),
        }
    if provider in {"slack", "teams"}:
        return {
            "channels": _merge_chat_channels(
                existing.get("channels", []),
                extra.get("channels", []),
            ),
            "users": _merge_keyed_dict_items(
                existing.get("users", []),
                extra.get("users", []),
                key_names=("id", "email", "name"),
            ),
            "profile": _merge_mapping(existing.get("profile"), extra.get("profile")),
        }
    if provider == "jira":
        return {
            "issues": _merge_jira_issues(
                existing.get("issues", []),
                extra.get("issues", []),
            ),
            "projects": _merge_keyed_dict_items(
                existing.get("projects", []),
                extra.get("projects", []),
                key_names=("key", "id", "name"),
            ),
        }
    if provider == "google":
        return {
            "documents": _merge_keyed_dict_items(
                existing.get("documents", []),
                extra.get("documents", []),
                key_names=("doc_id", "id", "title"),
            ),
        }
    if provider in {"crm", "salesforce"}:
        return {
            "deals": _merge_keyed_dict_items(
                existing.get("deals", []),
                extra.get("deals", []),
                key_names=("id", "deal_id", "name"),
            ),
        }
    merged = dict(existing)
    merged.update(extra)
    return merged


def _merge_mail_threads(
    existing: Any,
    extra: Any,
) -> list[dict[str, Any]]:
    merged: dict[str, dict[str, Any]] = {}
    for payload in list(existing or []) + list(extra or []):
        if not isinstance(payload, dict):
            continue
        thread_id = str(payload.get("thread_id", "")).strip()
        if not thread_id:
            continue
        messages = _merge_keyed_dict_items(
            merged.get(thread_id, {}).get("messages", []),
            payload.get("messages", []),
            key_names=("message_id", "id", "time_ms", "subject"),
        )
        merged[thread_id] = {
            "thread_id": thread_id,
            "subject": str(payload.get("subject", "")).strip()
            or merged.get(thread_id, {}).get("subject", thread_id),
            "category": str(payload.get("category", "historical") or "historical"),
            "messages": messages,
        }
    return list(merged.values())


def _merge_chat_channels(
    existing: Any,
    extra: Any,
) -> list[dict[str, Any]]:
    merged: dict[str, dict[str, Any]] = {}
    for payload in list(existing or []) + list(extra or []):
        if not isinstance(payload, dict):
            continue
        channel_id = str(payload.get("channel_id", payload.get("channel", ""))).strip()
        if not channel_id:
            continue
        messages = _merge_keyed_dict_items(
            merged.get(channel_id, {}).get("messages", []),
            payload.get("messages", []),
            key_names=("ts", "id"),
        )
        merged[channel_id] = {
            "channel": str(payload.get("channel", channel_id)).strip() or channel_id,
            "channel_id": channel_id,
            "unread": int(payload.get("unread", 0) or 0),
            "messages": messages,
        }
    return list(merged.values())


def _merge_jira_issues(
    existing: Any,
    extra: Any,
) -> list[dict[str, Any]]:
    merged: dict[str, dict[str, Any]] = {}
    for payload in list(existing or []) + list(extra or []):
        if not isinstance(payload, dict):
            continue
        ticket_id = str(payload.get("ticket_id", "")).strip()
        if not ticket_id:
            continue
        current = merged.get(ticket_id, {})
        merged[ticket_id] = {
            "ticket_id": ticket_id,
            "title": str(payload.get("title", "")).strip()
            or current.get("title", ticket_id),
            "status": str(payload.get("status", "")).strip()
            or current.get("status", "open"),
            "assignee": str(payload.get("assignee", "")).strip()
            or current.get("assignee", ""),
            "description": str(payload.get("description", "")).strip()
            or current.get("description", ""),
            "updated": str(payload.get("updated", "")).strip()
            or current.get("updated", ""),
            "comments": _merge_keyed_dict_items(
                current.get("comments", []),
                payload.get("comments", []),
                key_names=("id", "created", "body"),
            ),
        }
    return list(merged.values())


def _merge_keyed_dict_items(
    existing: Any,
    extra: Any,
    *,
    key_names: Sequence[str],
) -> list[dict[str, Any]]:
    merged: dict[str, dict[str, Any]] = {}
    ordered_items = list(existing or []) + list(extra or [])
    for index, item in enumerate(ordered_items):
        if not isinstance(item, dict):
            continue
        key = _dict_item_key(item, key_names=key_names)
        if not key:
            key = f"item-{index + 1}"
        merged.setdefault(key, item)
    return list(merged.values())


def _dict_item_key(
    item: dict[str, Any],
    *,
    key_names: Sequence[str],
) -> str:
    for key_name in key_names:
        value = str(item.get(key_name, "")).strip()
        if value:
            return value
    return ""


def _merge_mapping(
    left: Any,
    right: Any,
) -> dict[str, Any]:
    merged = dict(left or {})
    merged.update(dict(right or {}))
    return merged


def _context_source_record_counts(
    provider: str,
    data: dict[str, Any],
) -> dict[str, int]:
    if provider in {"mail_archive", "gmail"}:
        threads = [item for item in data.get("threads", []) if isinstance(item, dict)]
        return {
            "threads": len(threads),
            "messages": sum(
                len(thread.get("messages", []))
                for thread in threads
                if isinstance(thread.get("messages", []), list)
            ),
            "actors": len(
                [item for item in data.get("actors", []) if isinstance(item, dict)]
            ),
        }
    if provider in {"slack", "teams"}:
        channels = [item for item in data.get("channels", []) if isinstance(item, dict)]
        return {
            "channels": len(channels),
            "messages": sum(
                len(channel.get("messages", []))
                for channel in channels
                if isinstance(channel.get("messages", []), list)
            ),
            "users": len(
                [item for item in data.get("users", []) if isinstance(item, dict)]
            ),
        }
    if provider == "jira":
        issues = [item for item in data.get("issues", []) if isinstance(item, dict)]
        return {
            "issues": len(issues),
            "comments": sum(
                len(issue.get("comments", []))
                for issue in issues
                if isinstance(issue.get("comments", []), list)
            ),
        }
    if provider == "google":
        return {
            "documents": len(
                [item for item in data.get("documents", []) if isinstance(item, dict)]
            ),
        }
    if provider in {"crm", "salesforce"}:
        return {
            "deals": len(
                [item for item in data.get("deals", []) if isinstance(item, dict)]
            ),
        }
    return {}


def _included_surfaces_for_thread(
    events: Sequence[WhatIfEvent],
    *,
    case_context: WhatIfCaseContext | None = None,
) -> list[str]:
    surfaces = {event.surface or "mail" for event in events}
    if case_context is not None:
        surfaces.update(
            reference.surface
            for reference in case_context.related_history
            if reference.surface
        )
        surfaces.update(
            record.surface for record in case_context.records if record.surface
        )
    included: list[str] = ["identity"]
    if "mail" in surfaces:
        included.insert(0, "mail")
    if "slack" in surfaces:
        included.insert(0, "slack")
    if "tickets" in surfaces:
        included.insert(0, "tickets")
    if "docs" in surfaces:
        included.insert(0, "docs")
    if "crm" in surfaces:
        included.insert(0, "crm")
    return included


def _history_preview_from_saved_context(
    workspace_root: Path,
    *,
    manifest: WhatIfEpisodeManifest,
    history_limit: int,
) -> list[WhatIfEventReference]:
    if manifest.history_preview:
        return list(manifest.history_preview[-max(1, history_limit) :])
    try:
        context = _load_episode_context(workspace_root)
    except Exception:  # noqa: BLE001
        return []

    for thread in context.get("threads", []):
        if (
            not isinstance(thread, dict)
            or thread.get("thread_id") != manifest.thread_id
        ):
            continue
        messages = thread.get("messages", [])
        if not isinstance(messages, list):
            return []
        preview_messages = messages[-max(1, history_limit) :]
        return [
            WhatIfEventReference(
                event_id=f"{manifest.thread_id}:history:{index}",
                timestamp=str(message.get("timestamp", "")),
                actor_id=str(message.get("from", "")),
                target_id=str(message.get("to", "")),
                event_type="history",
                thread_id=manifest.thread_id,
                subject=str(message.get("subject", manifest.thread_subject)),
                snippet=str(message.get("body_text", ""))[:600],
                to_recipients=[str(message.get("to", ""))] if message.get("to") else [],
            )
            for index, message in enumerate(preview_messages, start=1)
            if isinstance(message, dict)
        ]
    return []


def _decision_branch_summary(
    branch_event: WhatIfEventReference,
    *,
    thread_subject: str,
    organization_domain: str,
) -> str:
    actor = display_name(branch_event.actor_id)
    verb = _historical_action_verb(branch_event, tense="present")
    subject = (
        thread_subject
        or branch_event.subject
        or branch_event.thread_id
        or "this thread"
    )
    recipient = _branch_recipient_label(
        branch_event,
        organization_domain=organization_domain,
    )
    if branch_event.surface == "slack":
        return f'{actor} is about to {verb} in {recipient} on "{subject}".'
    if branch_event.surface == "tickets":
        return f'{actor} is about to {verb} ticket "{subject}".'
    return f'{actor} is about to {verb} "{subject}" to {recipient}.'


def _historical_action_summary(
    branch_event: WhatIfEventReference,
    *,
    thread_subject: str,
    organization_domain: str,
) -> str:
    actor = display_name(branch_event.actor_id)
    verb = _historical_action_verb(branch_event, tense="past")
    recipient = _branch_recipient_label(
        branch_event,
        organization_domain=organization_domain,
    )
    details: list[str] = []
    if _branch_has_external_sharing(
        branch_event,
        organization_domain=organization_domain,
    ):
        details.append("outside recipient in scope")
    if branch_event.has_attachment_reference:
        details.append("attachment reference present")
    if branch_event.is_forward:
        details.append("forward metadata present")
    if branch_event.is_escalation:
        details.append("escalation signal present")
    suffix = f" ({', '.join(details)})" if details else ""
    subject = (
        thread_subject
        or branch_event.subject
        or branch_event.thread_id
        or "this thread"
    )
    if branch_event.surface == "slack":
        return f'Historically, {actor} {verb} in {recipient} on "{subject}"{suffix}.'
    if branch_event.surface == "tickets":
        return f'Historically, {actor} {verb} ticket "{subject}"{suffix}.'
    return f'Historically, {actor} {verb} "{subject}" to {recipient}{suffix}.'


def _historical_outcome_summary(forecast: WhatIfForecast) -> str:
    return (
        f"The recorded future had {forecast.future_event_count} follow-up events, "
        f"{forecast.future_external_event_count} outside-addressed sends, and "
        f"{forecast.future_escalation_count} escalations."
    )


def _decision_stakes_summary(
    branch_event: WhatIfEventReference,
    forecast: WhatIfForecast,
    *,
    organization_domain: str,
) -> str:
    notes: list[str] = []
    if _branch_has_external_sharing(
        branch_event,
        organization_domain=organization_domain,
    ):
        notes.append("This move reaches outside the company.")
    if branch_event.has_attachment_reference:
        notes.append("The thread carries document-sharing risk.")
    if branch_event.is_escalation or forecast.future_escalation_count > 0:
        notes.append("Leadership or escalation pressure is visible around this thread.")
    if forecast.future_event_count >= 6:
        notes.append(
            "The recorded future stayed active long enough to create coordination load."
        )
    if branch_event.surface == "slack" and not notes:
        notes.append(
            "This moment changes who stays in the channel thread and how much internal coordination follows."
        )
    if branch_event.surface == "tickets" and not notes:
        notes.append(
            "This moment changes ticket ownership, resolution pace, and escalation pressure."
        )
    if not notes:
        notes.append(
            "This moment changes who stays in the loop, how fast the thread moves, and how much follow-up work appears."
        )
    return " ".join(notes[:3])


def _decision_question(thread_subject: str) -> str:
    subject = thread_subject or "this thread"
    return f'What should the company do at this point in "{subject}"?'


def _decision_options_for_branch(
    branch_event: WhatIfEventReference,
    *,
    thread_subject: str,
    organization_name: str,
    organization_domain: str,
) -> list[WhatIfDecisionOption]:
    subject = (
        thread_subject
        or branch_event.subject
        or branch_event.thread_id
        or "this thread"
    )
    counterparty = _branch_recipient_label(
        branch_event,
        organization_domain=organization_domain,
    )
    company_label = organization_name or "the company"
    if branch_event.is_escalation or branch_event.event_type == "escalation":
        return [
            WhatIfDecisionOption(
                option_id="fact_gather",
                label="Pause and gather facts",
                summary="Keep the thread narrow, collect the facts, and name one owner before the next escalation.",
                prompt=(
                    f'Hold the escalation on "{subject}", gather the key facts in one internal note, '
                    "and assign one owner before anyone widens the leadership loop."
                ),
            ),
            WhatIfDecisionOption(
                option_id="single_owner_escalation",
                label="Escalate through one owner",
                summary="Move the thread upward, but through one clear owner and a tighter review path.",
                prompt=(
                    f'Escalate "{subject}" through one named owner, keep distribution narrow, '
                    "and ask for one clear decision instead of a broad leadership blast."
                ),
            ),
            WhatIfDecisionOption(
                option_id="broad_escalation",
                label="Open a broad leadership loop",
                summary="Push for speed by widening the escalation quickly across leaders.",
                prompt=(
                    f'Forward "{subject}" broadly across leadership, ask for rapid views, '
                    "and keep the loop open until a consensus forms."
                ),
            ),
        ]

    if (
        _branch_has_external_sharing(
            branch_event,
            organization_domain=organization_domain,
        )
        or branch_event.has_attachment_reference
        or branch_event.is_forward
    ):
        return [
            WhatIfDecisionOption(
                option_id="internal_review",
                label="Hold for internal review",
                summary="Tightest risk posture. Keep the material inside the company for one more review pass.",
                prompt=(
                    f'Keep "{subject}" inside {company_label}, ask legal or the internal owner for one more review, '
                    "and hold the outside send until one owner clears it."
                ),
            ),
            WhatIfDecisionOption(
                option_id="narrow_status",
                label="Send a narrow status note",
                summary="Keep the relationship warm without sending the full material yet.",
                prompt=(
                    f"Send {counterparty} a short no-attachment status note, promise a clean update soon, "
                    "and keep one internal owner on the next step."
                ),
            ),
            WhatIfDecisionOption(
                option_id="fast_turnaround",
                label="Push for fast turnaround",
                summary="Bias toward speed. Keep the outside loop active and widen circulation for fast comments.",
                prompt=(
                    f'Send "{subject}" now, keep the outside recipient loop active, '
                    "and widen circulation for rapid comments and turnaround."
                ),
            ),
        ]

    if branch_event.event_type == "assignment":
        return [
            WhatIfDecisionOption(
                option_id="single_owner",
                label="Assign one owner",
                summary="Keep the loop tight and make one person responsible for the next step.",
                prompt=(
                    f'Rewrite the next step on "{subject}" into one internal note with one named owner '
                    "and one required action."
                ),
            ),
            WhatIfDecisionOption(
                option_id="focused_review",
                label="Route through focused review",
                summary="Get a stronger answer before the thread broadens.",
                prompt=(
                    f'Route "{subject}" through one focused internal review path before anyone widens the thread, '
                    "then respond with a single consolidated answer."
                ),
            ),
            WhatIfDecisionOption(
                option_id="broad_coordination",
                label="Open a broader coordination loop",
                summary="Trade more coordination for more input and speed.",
                prompt=(
                    f'Forward "{subject}" to a broader cross-functional group, ask for quick comments, '
                    "and keep the thread moving in parallel."
                ),
            ),
        ]

    return [
        WhatIfDecisionOption(
            option_id="tight_loop",
            label="Keep the loop tight",
            summary="Use a narrow internal path and reduce follow-up sprawl.",
            prompt=(
                f'Keep "{subject}" in a tight internal loop, name one owner, '
                "and avoid widening the thread until the next step is clear."
            ),
        ),
        WhatIfDecisionOption(
            option_id="clear_reply",
            label="Reply with a clear next step",
            summary="Balance speed and control with one direct response and one owner.",
            prompt=(
                f'Reply on "{subject}" with one clear next step, one named owner, '
                "and one concrete commitment on timing."
            ),
        ),
        WhatIfDecisionOption(
            option_id="widen_loop",
            label="Widen the loop for speed",
            summary="Invite more people in quickly to accelerate the thread.",
            prompt=(
                f'Widen the participant loop on "{subject}", ask for rapid comments, '
                "and keep the thread moving with parallel follow-up."
            ),
        ),
    ]


def _historical_action_verb(
    branch_event: WhatIfEventReference,
    *,
    tense: str,
) -> str:
    if branch_event.is_escalation or branch_event.event_type == "escalation":
        return "escalated" if tense == "past" else "escalate"
    if branch_event.surface == "slack":
        return "replied" if tense == "past" else "reply"
    if branch_event.surface == "tickets":
        return "updated" if tense == "past" else "update"
    if branch_event.event_type == "assignment" and _branch_looks_like_mail_send(
        branch_event
    ):
        return "sent" if tense == "past" else "send"
    if branch_event.event_type == "assignment":
        return "assigned" if tense == "past" else "assign"
    if branch_event.is_forward:
        return "forwarded" if tense == "past" else "forward"
    if branch_event.is_reply or branch_event.event_type == "reply":
        return "replied on" if tense == "past" else "reply on"
    return "sent" if tense == "past" else "send"


def _branch_looks_like_mail_send(branch_event: WhatIfEventReference) -> bool:
    if branch_event.surface != "mail":
        return False
    recipients = [item for item in branch_event.to_recipients if item]
    if not recipients and branch_event.target_id:
        recipients = [branch_event.target_id]
    return any("@" in recipient for recipient in recipients)


def _branch_recipient_label(
    branch_event: WhatIfEventReference,
    *,
    organization_domain: str,
) -> str:
    if branch_event.surface == "tickets":
        return branch_event.thread_id.split(":", 1)[-1]
    recipients = [item for item in branch_event.to_recipients if item]
    if not recipients and branch_event.target_id:
        recipients = [branch_event.target_id]
    if not recipients:
        if branch_event.surface == "slack":
            return "the current channel"
        if branch_event.surface == "tickets":
            return branch_event.thread_id
        return "the current thread"

    display_recipients = recipients[:2]
    label = ", ".join(
        item if item.startswith("#") else display_name(item)
        for item in display_recipients
    )
    if len(recipients) > 2:
        label = f"{label}, and {len(recipients) - 2} more"
    if has_external_recipients(
        recipients,
        organization_domain=organization_domain,
    ):
        return label
    return label


def _branch_has_external_sharing(
    branch_event: WhatIfEventReference,
    *,
    organization_domain: str,
) -> bool:
    recipients = [item for item in branch_event.to_recipients if item]
    if not recipients and branch_event.target_id:
        recipients = [branch_event.target_id]
    if not recipients:
        return False
    return has_external_recipients(
        recipients,
        organization_domain=organization_domain,
    )


def _persist_workspace_historical_source(
    world: WhatIfWorld,
    workspace_root: Path,
) -> None:
    if world.source not in {"mail_archive", "company_history"}:
        return
    source_file = _historical_source_file(world.source_dir)
    if source_file is None or not source_file.exists():
        return
    target_name = (
        "whatif_mail_archive.json"
        if world.source == "mail_archive"
        else "whatif_company_history.json"
    )
    target = workspace_root / target_name
    if source_file.resolve() == target.resolve():
        return
    shutil.copyfile(source_file, target)


def _persist_workspace_public_context(
    world: WhatIfWorld,
    workspace_root: Path,
) -> None:
    from .public_context import discover_public_context_path

    context_path = discover_public_context_path(
        source_dir=world.source_dir,
        metadata=getattr(world, "metadata", None),
    )
    if context_path is None or not context_path.exists():
        return
    target = workspace_root / "whatif_public_context.json"
    if context_path.resolve() == target.resolve():
        return
    shutil.copyfile(context_path, target)


def _historical_source_file(source_dir: Path) -> Path | None:
    resolved = source_dir.expanduser().resolve()
    if resolved.is_file():
        return resolved
    for filename in (
        "whatif_company_history.json",
        "company_history_bundle.json",
        "whatif_mail_archive.json",
        "historical_mail_archive.json",
        "mail_archive.json",
        "context_snapshot.json",
    ):
        candidate = resolved / filename
        if candidate.exists():
            return candidate
    return None


def load_episode_manifest(root: str | Path) -> WhatIfEpisodeManifest:
    workspace_root = Path(root).expanduser().resolve()
    manifest_path = workspace_root / "whatif_episode_manifest.json"
    if not manifest_path.exists():
        raise ValueError(f"what-if episode manifest not found: {manifest_path}")
    return WhatIfEpisodeManifest.model_validate_json(
        manifest_path.read_text(encoding="utf-8")
    )


def replay_episode_baseline(
    root: str | Path,
    *,
    tick_ms: int = 0,
    seed: int = 42042,
) -> WhatIfReplaySummary:
    workspace_root = Path(root).expanduser().resolve()
    manifest = load_episode_manifest(workspace_root)
    bundle = load_customer_twin(workspace_root)
    asset_path = workspace_root / bundle.blueprint_asset_path
    dataset_path = workspace_root / manifest.baseline_dataset_path
    asset = BlueprintAsset.model_validate_json(asset_path.read_text(encoding="utf-8"))
    dataset = VEIDataset.model_validate_json(dataset_path.read_text(encoding="utf-8"))
    session = create_world_session_from_blueprint(asset, seed=seed)
    replay_result = session.replay(mode="overlay", dataset_events=dataset.events)

    delivered_event_count = 0
    current_time_ms = session.router.bus.clock_ms
    pending_events = session.pending()
    if tick_ms > 0:
        tick_result = session.router.tick(dt_ms=tick_ms)
        delivered_event_count = sum(tick_result.get("delivered", {}).values())
        current_time_ms = int(tick_result.get("time_ms", current_time_ms))
        pending_events = dict(tick_result.get("pending", {}))

    inbox_count = 0
    top_subjects: list[str] = []
    visible_item_count = 0
    top_items: list[str] = []
    if manifest.surface == "mail":
        inbox = session.call_tool("mail.list", {})
        inbox_count = len(inbox)
        top_subjects = [
            str(item.get("subj", ""))
            for item in inbox[:5]
            if isinstance(item, dict) and item.get("subj")
        ]
        visible_item_count = inbox_count
        top_items = list(top_subjects)
    elif manifest.surface == "slack":
        channel_name = _chat_channel_name_from_reference(manifest.branch_event)
        channel_payload = session.call_tool(
            "slack.open_channel",
            {"channel": channel_name},
        )
        channel_messages = (
            channel_payload.get("messages", [])
            if isinstance(channel_payload, dict)
            else []
        )
        messages = _slack_thread_messages(
            channel_messages,
            conversation_anchor=manifest.branch_event.conversation_anchor,
        )
        visible_item_count = len(messages)
        top_items = [
            str(item.get("text", ""))
            for item in messages[:5]
            if isinstance(item, dict) and item.get("text")
        ]
    elif manifest.surface == "tickets":
        tickets_payload = session.call_tool("tickets.list", {})
        tickets = tickets_payload if isinstance(tickets_payload, list) else []
        visible_item_count = len(tickets)
        top_items = [
            str(item.get("title", ""))
            for item in tickets[:5]
            if isinstance(item, dict) and item.get("title")
        ]
    return WhatIfReplaySummary(
        workspace_root=workspace_root,
        baseline_dataset_path=dataset_path,
        surface=manifest.surface,
        scheduled_event_count=int(replay_result.get("scheduled", 0)),
        delivered_event_count=delivered_event_count,
        current_time_ms=current_time_ms,
        pending_events=pending_events,
        inbox_count=inbox_count,
        top_subjects=top_subjects,
        visible_item_count=visible_item_count,
        top_items=top_items,
        baseline_future_preview=list(manifest.baseline_future_preview),
        forecast=manifest.forecast,
    )


def forecast_episode(
    events: Sequence[WhatIfEvent],
    *,
    organization_domain: str = ENRON_DOMAIN,
) -> WhatIfForecast:
    future_event_count = len(events)
    future_escalation_count = sum(
        1
        for event in events
        if event.flags.is_escalation or event.event_type == "escalation"
    )
    future_assignment_count = sum(
        1 for event in events if event.event_type == "assignment"
    )
    future_approval_count = sum(1 for event in events if event.event_type == "approval")
    future_external_event_count = sum(
        1
        for event in events
        if has_external_recipients(
            event.flags.to_recipients,
            organization_domain=organization_domain,
        )
    )
    risk_score = min(
        1.0,
        (
            (future_escalation_count * 0.25)
            + (future_assignment_count * 0.15)
            + (future_external_event_count * 0.2)
            + max(0, future_event_count - future_approval_count) * 0.02
        ),
    )
    summary = (
        f"{future_event_count} future events remain, including "
        f"{future_escalation_count} escalations and {future_external_event_count} "
        "externally addressed messages."
    )
    return WhatIfForecast(
        backend="historical",
        future_event_count=future_event_count,
        future_escalation_count=future_escalation_count,
        future_assignment_count=future_assignment_count,
        future_approval_count=future_approval_count,
        future_external_event_count=future_external_event_count,
        risk_score=round(risk_score, 3),
        summary=summary,
    )


def run_llm_counterfactual(
    root: str | Path,
    *,
    prompt: str,
    provider: str = "openai",
    model: str = default_model_for_provider("openai"),
    seed: int = 42042,
) -> WhatIfLLMReplayResult:
    load_dotenv(override=True)
    workspace_root = Path(root).expanduser().resolve()
    manifest = load_episode_manifest(workspace_root)
    snapshot = _load_episode_snapshot(workspace_root)
    session = _session_for_episode(workspace_root, seed=seed)
    allowed_actors, allowed_recipients = _allowed_thread_participants(
        snapshot=snapshot,
        manifest=manifest,
    )
    recipient_scope, recipient_notes = _apply_recipient_scope(
        allowed_recipients,
        organization_domain=manifest.organization_domain,
        tags=intervention_tags(prompt),
    )
    system = (
        "You are simulating a bounded counterfactual continuation on a historical "
        "enterprise thread. Return strict JSON with keys tool and args. "
        "Use tool='emit_counterfactual'. In args, include summary, notes, and "
        "messages. messages must be a list of 1 to 3 objects with actor_id, surface, "
        "to, subject, body_text, delay_ms, rationale, and optional conversation_anchor. "
        "Only use the listed actors and allowed targets. Keep messages plausible, concise, and clearly tied "
        "to the intervention prompt."
    )
    user = _llm_counterfactual_prompt(
        snapshot=snapshot,
        manifest=manifest,
        prompt=prompt,
        allowed_actors=allowed_actors,
        allowed_recipients=recipient_scope,
    )
    try:
        response = asyncio.run(
            providers.plan_once_with_usage(
                provider=provider,
                model=model,
                system=system,
                user=user,
                timeout_s=90,
            )
        )
        messages, notes = _normalize_llm_messages(
            _counterfactual_args(response.plan),
            manifest=manifest,
            allowed_actors=allowed_actors,
            allowed_recipients=recipient_scope,
        )
        if not messages:
            raise ValueError("LLM returned no usable messages")
    except Exception as exc:  # noqa: BLE001
        return WhatIfLLMReplayResult(
            status="error",
            provider=provider,
            model=model,
            prompt=prompt,
            summary="LLM counterfactual generation failed.",
            error=str(exc),
            notes=["The forecast path can still be used without live LLM output."],
        )

    max_delay = max(message.delay_ms for message in messages)
    replay_result = session.replay(
        mode="overlay",
        dataset_events=[
            _llm_replay_event(message, manifest=manifest) for message in messages
        ],
    )
    tick_result = session.router.tick(dt_ms=max_delay + 1000)
    inbox_count = 0
    top_subjects: list[str] = []
    if manifest.surface == "mail":
        inbox = session.call_tool("mail.list", {})
        inbox_count = len(inbox)
        top_subjects = [
            str(item.get("subj", ""))
            for item in inbox[:5]
            if isinstance(item, dict) and item.get("subj")
        ]
    plan_args = _counterfactual_args(response.plan)
    summary = str(plan_args.get("summary", "") or "").strip()
    if not summary:
        summary = (
            f"{len(messages)} counterfactual actions were generated across "
            f"{len({message.actor_id for message in messages})} participants."
        )
    return WhatIfLLMReplayResult(
        status="ok",
        provider=provider,
        model=model,
        prompt=prompt,
        summary=summary,
        messages=messages,
        usage=WhatIfLLMUsage(
            provider=response.usage.provider,
            model=response.usage.model,
            prompt_tokens=response.usage.prompt_tokens,
            completion_tokens=response.usage.completion_tokens,
            total_tokens=response.usage.total_tokens,
            estimated_cost_usd=response.usage.estimated_cost_usd,
        ),
        scheduled_event_count=int(replay_result.get("scheduled", 0)),
        delivered_event_count=sum(tick_result.get("delivered", {}).values()),
        inbox_count=inbox_count,
        top_subjects=top_subjects,
        notes=recipient_notes + notes + _counterfactual_notes(plan_args),
    )


def run_ejepa_proxy_counterfactual(
    root: str | Path,
    *,
    prompt: str,
) -> WhatIfForecastResult:
    workspace_root = Path(root).expanduser().resolve()
    manifest = load_episode_manifest(workspace_root)
    baseline = manifest.forecast.model_copy(deep=True)
    predicted = manifest.forecast.model_copy(
        update={"backend": "e_jepa_proxy"},
        deep=True,
    )
    tags = intervention_tags(prompt)
    notes: list[str] = []

    event_shift = 0
    escalation_shift = 0
    assignment_shift = 0
    approval_shift = 0
    external_shift = 0
    risk_shift = 0.0

    if {"legal", "compliance"} & tags:
        escalation_shift -= max(1, predicted.future_escalation_count // 2)
        approval_shift += 1
        risk_shift -= 0.18
        notes.append("Compliance involvement reduces uncontrolled escalation.")
    if {"hold", "pause_forward"} & tags:
        external_shift -= max(1, predicted.future_external_event_count)
        event_shift -= max(0, predicted.future_event_count // 3)
        risk_shift -= 0.2
        notes.append("Holding or pausing the thread cuts external exposure.")
    if {"reply_immediately", "clarify_owner"} & tags:
        event_shift -= 1
        assignment_shift -= max(0, predicted.future_assignment_count // 2)
        risk_shift -= 0.12
        notes.append("Fast clarification usually shortens the follow-up tail.")
    if "status_only" in tags:
        external_shift -= max(1, predicted.future_external_event_count // 4)
        event_shift -= max(0, predicted.future_event_count // 8)
        risk_shift -= 0.08
        notes.append(
            "A status-only outside note reduces document exposure while keeping contact warm."
        )
    if {"executive_gate"} & tags:
        escalation_shift -= max(1, predicted.future_escalation_count // 2)
        approval_shift += 1
        risk_shift -= 0.14
        notes.append("Routing through an executive gate lowers escalation spread.")
    if "attachment_removed" in tags and "external_removed" not in tags:
        risk_shift -= 0.16
        notes.append("Keeping the attachment inside lowers sharing risk.")
    if "external_removed" in tags:
        if predicted.future_external_event_count > 0:
            external_shift -= predicted.future_external_event_count
            risk_shift -= 0.24
            notes.append("Removing the outside recipient sharply lowers leak risk.")
        else:
            notes.append(
                "The recorded path already stays internal, so removing outside recipients changes little."
            )
    if {"send_now", "widen_loop"} & tags:
        event_shift += max(1, predicted.future_event_count // 12)
        assignment_shift += max(1, max(predicted.future_assignment_count, 1) // 8)
        external_shift += max(1, max(predicted.future_external_event_count, 1) // 6)
        risk_shift += 0.12
        notes.append(
            "Keeping the outside loop active increases spread and coordination pressure."
        )

    predicted.future_event_count = max(0, predicted.future_event_count + event_shift)
    predicted.future_escalation_count = max(
        0,
        predicted.future_escalation_count + escalation_shift,
    )
    predicted.future_assignment_count = max(
        0,
        predicted.future_assignment_count + assignment_shift,
    )
    predicted.future_approval_count = max(
        0,
        predicted.future_approval_count + approval_shift,
    )
    predicted.future_external_event_count = max(
        0,
        predicted.future_external_event_count + external_shift,
    )
    predicted.risk_score = round(
        max(0.0, min(1.0, predicted.risk_score + risk_shift)),
        3,
    )
    predicted.summary = _forecast_summary_from_counts(predicted)

    delta = WhatIfForecastDelta(
        risk_score_delta=round(predicted.risk_score - baseline.risk_score, 3),
        future_event_delta=predicted.future_event_count - baseline.future_event_count,
        escalation_delta=(
            predicted.future_escalation_count - baseline.future_escalation_count
        ),
        assignment_delta=(
            predicted.future_assignment_count - baseline.future_assignment_count
        ),
        approval_delta=predicted.future_approval_count - baseline.future_approval_count,
        external_event_delta=(
            predicted.future_external_event_count - baseline.future_external_event_count
        ),
    )
    result = WhatIfForecastResult(
        status="ok",
        backend="e_jepa_proxy",
        prompt=prompt,
        summary=_forecast_delta_summary(delta),
        baseline=baseline,
        predicted=predicted,
        delta=delta,
        notes=notes
        or [
            "No specific intervention tags were detected; forecast remained close to baseline."
        ],
    )
    return _attach_business_state_to_forecast_result(
        result,
        branch_event=manifest.branch_event,
        organization_domain=manifest.organization_domain,
        public_context=manifest.public_context,
    )


def _attach_business_state_to_forecast_result(
    forecast_result: WhatIfForecastResult,
    *,
    branch_event: WhatIfEventReference | None,
    organization_domain: str,
    public_context: WhatIfPublicContext | None,
) -> WhatIfForecastResult:
    if branch_event is None or forecast_result.status != "ok":
        return forecast_result
    forecast_result.business_state_change = describe_forecast_business_change(
        branch_event=branch_event,
        forecast_result=forecast_result,
        organization_domain=organization_domain,
        public_context=public_context,
    )
    return forecast_result


def run_counterfactual_experiment(
    world: WhatIfWorld,
    *,
    artifacts_root: str | Path,
    label: str,
    counterfactual_prompt: str,
    selection_scenario: str | None = None,
    selection_prompt: str | None = None,
    thread_id: str | None = None,
    event_id: str | None = None,
    mode: WhatIfExperimentMode = "both",
    forecast_backend: WhatIfForecastBackend | None = None,
    allow_proxy_fallback: bool = True,
    provider: str = "openai",
    model: str = default_model_for_provider("openai"),
    seed: int = 42042,
    ejepa_epochs: int = 4,
    ejepa_batch_size: int = 64,
    ejepa_force_retrain: bool = False,
    ejepa_device: str | None = None,
) -> WhatIfExperimentResult:
    selection = (
        run_whatif(
            world,
            scenario=selection_scenario,
            prompt=selection_prompt,
        )
        if selection_scenario or selection_prompt
        else _selection_for_specific_event(
            world,
            thread_id=thread_id,
            event_id=event_id,
            prompt=counterfactual_prompt,
        )
    )
    selected_thread_id = thread_id
    if selected_thread_id is None and event_id:
        selected_event = event_by_id(world.events, event_id)
        if selected_event is None:
            raise ValueError(f"event not found in world: {event_id}")
        selected_thread_id = selected_event.thread_id
    if selected_thread_id is None:
        selected_thread_id = (
            selection.top_threads[0].thread_id if selection.top_threads else None
        )
    if not selected_thread_id:
        raise ValueError("no matching thread available for the counterfactual run")

    root = Path(artifacts_root).expanduser().resolve() / _slug(label)
    workspace_root = root / "workspace"
    materialization = materialize_episode(
        world,
        root=workspace_root,
        thread_id=selected_thread_id,
        event_id=event_id,
    )
    baseline = replay_episode_baseline(
        workspace_root,
        tick_ms=_baseline_tick_ms(materialization.baseline_dataset_path),
        seed=seed,
    )
    llm_result: WhatIfLLMReplayResult | None = None
    if mode in {"llm", "both"}:
        llm_result = run_llm_counterfactual(
            workspace_root,
            prompt=counterfactual_prompt,
            provider=provider,
            model=model,
            seed=seed,
        )
    forecast_result: WhatIfForecastResult | None = None
    resolved_forecast_backend = forecast_backend or (
        mode if mode in {"e_jepa", "e_jepa_proxy"} else default_forecast_backend()
    )
    if mode in {"e_jepa", "e_jepa_proxy", "both"}:
        if resolved_forecast_backend == "e_jepa":
            forecast_result = run_ejepa_counterfactual(
                workspace_root,
                prompt=counterfactual_prompt,
                source=world.source,
                source_dir=world.source_dir,
                thread_id=selected_thread_id,
                branch_event_id=materialization.branch_event_id,
                llm_messages=llm_result.messages if llm_result is not None else None,
                epochs=ejepa_epochs,
                batch_size=ejepa_batch_size,
                force_retrain=ejepa_force_retrain,
                device=ejepa_device,
            )
            if forecast_result.status == "error" and allow_proxy_fallback:
                proxy_result = run_ejepa_proxy_counterfactual(
                    workspace_root,
                    prompt=counterfactual_prompt,
                )
                proxy_result.notes.insert(
                    0,
                    "Real E-JEPA forecast failed, so this experiment fell back to the proxy forecast.",
                )
                if forecast_result.error:
                    proxy_result.notes.append(
                        f"Original E-JEPA error: {forecast_result.error}"
                    )
                forecast_result = proxy_result
        else:
            forecast_result = run_ejepa_proxy_counterfactual(
                workspace_root,
                prompt=counterfactual_prompt,
            )
    if forecast_result is not None:
        forecast_result = _attach_business_state_to_forecast_result(
            forecast_result,
            branch_event=materialization.branch_event,
            organization_domain=materialization.organization_domain,
            public_context=materialization.public_context,
        )

    result_path = root / "whatif_experiment_result.json"
    overview_path = root / "whatif_experiment_overview.md"
    llm_path = root / "whatif_llm_result.json" if llm_result is not None else None
    forecast_path = None
    if forecast_result is not None:
        forecast_filename = (
            "whatif_ejepa_result.json"
            if forecast_result.backend == "e_jepa"
            else "whatif_ejepa_proxy_result.json"
        )
        forecast_path = root / forecast_filename
    root.mkdir(parents=True, exist_ok=True)

    artifacts = WhatIfExperimentArtifacts(
        root=root,
        result_json_path=result_path,
        overview_markdown_path=overview_path,
        llm_json_path=llm_path,
        forecast_json_path=forecast_path,
    )
    result = WhatIfExperimentResult(
        mode=mode,
        label=label,
        intervention=WhatIfInterventionSpec(
            label=label,
            prompt=counterfactual_prompt,
            objective=(
                selection.scenario.description
                if selection.scenario.description
                else "counterfactual replay"
            ),
            scenario_id=selection.scenario.scenario_id,
            thread_id=selected_thread_id,
            branch_event_id=materialization.branch_event_id,
        ),
        selection=selection,
        materialization=materialization,
        baseline=baseline,
        llm_result=llm_result,
        forecast_result=forecast_result,
        artifacts=artifacts,
    )
    result_path.write_text(result.model_dump_json(indent=2), encoding="utf-8")
    if llm_result is not None and llm_path is not None:
        llm_path.write_text(llm_result.model_dump_json(indent=2), encoding="utf-8")
    if forecast_result is not None and forecast_path is not None:
        forecast_path.write_text(
            forecast_result.model_dump_json(indent=2),
            encoding="utf-8",
        )
    overview_path.write_text(
        _render_experiment_overview(result),
        encoding="utf-8",
    )
    return result


def run_ranked_counterfactual_experiment(
    world: WhatIfWorld,
    *,
    artifacts_root: str | Path,
    label: str,
    objective_pack_id: WhatIfObjectivePackId | str,
    candidate_interventions: Sequence[str | WhatIfCandidateIntervention],
    selection_scenario: str | None = None,
    selection_prompt: str | None = None,
    thread_id: str | None = None,
    event_id: str | None = None,
    rollout_count: int = 4,
    provider: str = "openai",
    model: str = default_model_for_provider("openai"),
    seed: int = 42042,
    shadow_forecast_backend: WhatIfForecastBackend | None = None,
    allow_proxy_fallback: bool = True,
    ejepa_epochs: int = 4,
    ejepa_batch_size: int = 64,
    ejepa_force_retrain: bool = False,
    ejepa_device: str | None = None,
) -> WhatIfRankedExperimentResult:
    if rollout_count < 1 or rollout_count > 16:
        raise ValueError("rollout_count must be between 1 and 16")

    normalized_candidates = _normalize_candidate_interventions(candidate_interventions)
    if not normalized_candidates:
        raise ValueError("at least one candidate intervention is required")
    if len(normalized_candidates) > 5:
        raise ValueError("ranked what-if supports at most 5 candidate interventions")

    objective_pack = get_objective_pack(str(objective_pack_id))
    selection = (
        run_whatif(
            world,
            scenario=selection_scenario,
            prompt=selection_prompt,
        )
        if selection_scenario or selection_prompt
        else _selection_for_specific_event(
            world,
            thread_id=thread_id,
            event_id=event_id,
            prompt=normalized_candidates[0].prompt,
        )
    )
    selected_thread_id = thread_id
    if selected_thread_id is None and event_id:
        selected_event = event_by_id(world.events, event_id)
        if selected_event is None:
            raise ValueError(f"event not found in world: {event_id}")
        selected_thread_id = selected_event.thread_id
    if selected_thread_id is None:
        selected_thread_id = (
            selection.top_threads[0].thread_id if selection.top_threads else None
        )
    if not selected_thread_id:
        raise ValueError("no matching thread available for the counterfactual run")

    root = Path(artifacts_root).expanduser().resolve() / _slug(label)
    workspace_root = root / "workspace"
    materialization = materialize_episode(
        world,
        root=workspace_root,
        thread_id=selected_thread_id,
        event_id=event_id,
    )
    baseline = replay_episode_baseline(
        workspace_root,
        tick_ms=_baseline_tick_ms(materialization.baseline_dataset_path),
        seed=seed,
    )

    candidate_results: list[WhatIfCandidateRanking] = []
    resolved_shadow_backend = shadow_forecast_backend or default_forecast_backend()
    for candidate_index, intervention in enumerate(normalized_candidates):
        rollouts: list[WhatIfRankedRolloutResult] = []
        rollout_signals: list[WhatIfOutcomeSignals] = []
        first_rollout: WhatIfLLMReplayResult | None = None
        for rollout_index in range(rollout_count):
            rollout_seed = seed + (candidate_index * 100) + rollout_index
            llm_result = run_llm_counterfactual(
                workspace_root,
                prompt=intervention.prompt,
                provider=provider,
                model=model,
                seed=rollout_seed,
            )
            if first_rollout is None:
                first_rollout = llm_result
            outcome_signals = summarize_llm_branch(
                branch_event=materialization.branch_event,
                llm_result=llm_result,
                organization_domain=materialization.organization_domain,
            )
            outcome_score = score_outcome_signals(
                pack=objective_pack,
                outcome=outcome_signals,
            )
            rollout_signals.append(outcome_signals)
            rollouts.append(
                WhatIfRankedRolloutResult(
                    rollout_index=rollout_index + 1,
                    seed=rollout_seed,
                    llm_result=llm_result,
                    outcome_signals=outcome_signals,
                    outcome_score=outcome_score,
                )
            )

        average_signals = aggregate_outcome_signals(rollout_signals)
        outcome_score = score_outcome_signals(
            pack=objective_pack,
            outcome=average_signals,
        )
        shadow = _run_ranked_shadow_score(
            world=world,
            workspace_root=workspace_root,
            materialization=materialization,
            objective_pack=objective_pack,
            prompt=intervention.prompt,
            llm_result=first_rollout,
            forecast_backend=resolved_shadow_backend,
            allow_proxy_fallback=allow_proxy_fallback,
            ejepa_epochs=ejepa_epochs,
            ejepa_batch_size=ejepa_batch_size,
            ejepa_force_retrain=ejepa_force_retrain,
            ejepa_device=ejepa_device,
        )
        candidate_results.append(
            WhatIfCandidateRanking(
                intervention=intervention,
                rollout_count=len(rollouts),
                average_outcome_signals=average_signals,
                outcome_score=outcome_score,
                reason="",
                rollouts=rollouts,
                shadow=shadow,
                business_state_change=shadow.forecast_result.business_state_change,
            )
        )

    ordered_labels = sort_candidates_for_rank(
        [
            (
                item.intervention.label,
                item.average_outcome_signals,
                item.outcome_score,
            )
            for item in candidate_results
        ]
    )
    rank_map = {label: index + 1 for index, label in enumerate(ordered_labels)}
    recommended_label = ordered_labels[0] if ordered_labels else ""
    for item in candidate_results:
        item.rank = rank_map[item.intervention.label]
        item.reason = _candidate_ranking_reason(
            candidate=item,
            objective_pack_id=objective_pack.pack_id,
            is_best=item.intervention.label == recommended_label,
        )
    candidate_results.sort(key=lambda item: item.rank)

    result_path = root / "whatif_ranked_result.json"
    overview_path = root / "whatif_ranked_overview.md"
    root.mkdir(parents=True, exist_ok=True)
    artifacts = WhatIfRankedExperimentArtifacts(
        root=root,
        result_json_path=result_path,
        overview_markdown_path=overview_path,
    )
    result = WhatIfRankedExperimentResult(
        label=label,
        objective_pack=objective_pack,
        selection=selection,
        materialization=materialization,
        baseline=baseline,
        candidates=candidate_results,
        recommended_candidate_label=recommended_label,
        artifacts=artifacts,
    )
    result_path.write_text(result.model_dump_json(indent=2), encoding="utf-8")
    overview_path.write_text(
        _render_ranked_experiment_overview(result),
        encoding="utf-8",
    )
    return result


def load_experiment_result(root: str | Path) -> WhatIfExperimentResult:
    result_path = Path(root).expanduser().resolve() / "whatif_experiment_result.json"
    if not result_path.exists():
        raise ValueError(f"what-if experiment result not found: {result_path}")
    return WhatIfExperimentResult.model_validate_json(
        result_path.read_text(encoding="utf-8")
    )


def load_ranked_experiment_result(root: str | Path) -> WhatIfRankedExperimentResult:
    result_path = Path(root).expanduser().resolve() / "whatif_ranked_result.json"
    if not result_path.exists():
        raise ValueError(f"ranked what-if result not found: {result_path}")
    return WhatIfRankedExperimentResult.model_validate_json(
        result_path.read_text(encoding="utf-8")
    )


def _normalize_candidate_interventions(
    values: Sequence[str | WhatIfCandidateIntervention],
) -> list[WhatIfCandidateIntervention]:
    normalized: list[WhatIfCandidateIntervention] = []
    for index, value in enumerate(values, start=1):
        if isinstance(value, WhatIfCandidateIntervention):
            prompt = value.prompt.strip()
            label = value.label.strip() or _candidate_label(prompt, index=index)
        else:
            prompt = str(value).strip()
            label = _candidate_label(prompt, index=index)
        if not prompt:
            continue
        normalized.append(
            WhatIfCandidateIntervention(
                label=label,
                prompt=prompt,
            )
        )
    return normalized


def _candidate_label(prompt: str, *, index: int) -> str:
    cleaned = " ".join(prompt.split())
    if not cleaned:
        return f"Option {index}"
    words = cleaned.split()
    preview = " ".join(words[:5])
    if len(words) > 5:
        preview += "..."
    return preview


def _run_ranked_shadow_score(
    *,
    world: WhatIfWorld,
    workspace_root: Path,
    materialization: WhatIfEpisodeMaterialization,
    objective_pack,
    prompt: str,
    llm_result: WhatIfLLMReplayResult | None,
    forecast_backend: WhatIfForecastBackend,
    allow_proxy_fallback: bool,
    ejepa_epochs: int,
    ejepa_batch_size: int,
    ejepa_force_retrain: bool,
    ejepa_device: str | None,
) -> WhatIfShadowOutcomeScore:
    if forecast_backend == "e_jepa":
        forecast_result = run_ejepa_counterfactual(
            workspace_root,
            prompt=prompt,
            source=world.source,
            source_dir=world.source_dir,
            thread_id=materialization.thread_id,
            branch_event_id=materialization.branch_event_id,
            llm_messages=llm_result.messages if llm_result is not None else None,
            epochs=ejepa_epochs,
            batch_size=ejepa_batch_size,
            force_retrain=ejepa_force_retrain,
            device=ejepa_device,
        )
        if forecast_result.status == "error" and allow_proxy_fallback:
            proxy_result = run_ejepa_proxy_counterfactual(
                workspace_root,
                prompt=prompt,
            )
            proxy_result.notes.insert(
                0,
                "Real E-JEPA shadow scoring failed, so this candidate used the proxy forecast.",
            )
            if forecast_result.error:
                proxy_result.notes.append(
                    f"Original E-JEPA error: {forecast_result.error}"
                )
            forecast_result = proxy_result
    else:
        forecast_result = run_ejepa_proxy_counterfactual(
            workspace_root,
            prompt=prompt,
        )

    outcome_signals = summarize_forecast_branch(forecast_result)
    outcome_score = score_outcome_signals(
        pack=objective_pack,
        outcome=outcome_signals,
    )
    forecast_result = _attach_business_state_to_forecast_result(
        forecast_result,
        branch_event=materialization.branch_event,
        organization_domain=materialization.organization_domain,
        public_context=materialization.public_context,
    )
    return WhatIfShadowOutcomeScore(
        backend=forecast_result.backend,
        outcome_signals=outcome_signals,
        outcome_score=outcome_score,
        forecast_result=forecast_result,
    )


def _candidate_ranking_reason(
    *,
    candidate: WhatIfCandidateRanking,
    objective_pack_id: WhatIfObjectivePackId,
    is_best: bool,
) -> str:
    if is_best:
        objective_pack = get_objective_pack(objective_pack_id)
        return recommendation_reason(
            pack=objective_pack,
            outcome=candidate.average_outcome_signals,
            score=candidate.outcome_score,
            rollout_count=candidate.rollout_count,
        )
    if objective_pack_id == "contain_exposure":
        return "Lower-ranked because it leaves more exposure in the simulated branches."
    if objective_pack_id == "reduce_delay":
        return "Lower-ranked because it still carries a slower follow-up pattern."
    return "Lower-ranked because it protects the relationship less consistently."


def _selection_for_specific_event(
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
        raise ValueError("provide selection criteria or an explicit event/thread")

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


def _archive_message_payload(
    event: WhatIfEvent,
    *,
    base_time_ms: int,
    organization_domain: str,
) -> dict[str, Any]:
    recipient = _primary_recipient(event)
    return {
        "from": event.actor_id
        or _historical_archive_address(organization_domain, "unknown"),
        "to": recipient,
        "subject": event.subject or event.thread_id,
        "body_text": _historical_body(event),
        "unread": False,
        "time_ms": base_time_ms,
    }


def _baseline_event_payload(
    event: WhatIfEvent,
    *,
    branch_event: WhatIfEvent,
    thread_subject: str,
    organization_domain: str,
) -> BaseEvent:
    delay_ms = max(1, event.timestamp_ms - branch_event.timestamp_ms)
    if event.surface == "slack":
        return BaseEvent(
            time_ms=delay_ms,
            actor_id=event.actor_id,
            channel="slack",
            type=event.event_type,
            correlation_id=event.thread_id,
            payload={
                "channel": _chat_channel_name(event),
                "text": _historical_chat_text(event),
                "thread_ts": event.conversation_anchor or None,
                "user": event.actor_id,
            },
        )
    if event.surface == "tickets":
        return BaseEvent(
            time_ms=delay_ms,
            actor_id=event.actor_id,
            channel="tickets",
            type=event.event_type,
            correlation_id=event.thread_id,
            payload=_ticket_event_payload(event),
        )
    return BaseEvent(
        time_ms=delay_ms,
        actor_id=event.actor_id,
        channel="mail",
        type=event.event_type,
        correlation_id=event.thread_id,
        payload={
            "from": event.actor_id
            or _historical_archive_address(organization_domain, "unknown"),
            "to": _primary_recipient(event),
            "subj": event.subject or thread_subject,
            "body_text": _historical_body(event),
            "thread_id": event.thread_id,
            "category": "historical",
        },
    )


def _chat_channel_name(event: WhatIfEvent) -> str:
    recipients = [item for item in event.flags.to_recipients if item]
    if recipients:
        return recipients[0]
    if event.target_id:
        return event.target_id
    return "#history"


def _chat_channel_name_from_reference(event: WhatIfEventReference) -> str:
    recipients = [item for item in event.to_recipients if item]
    if recipients:
        return recipients[0]
    if event.target_id:
        return event.target_id
    return "#history"


def _chat_message_ts(event: WhatIfEvent, *, fallback_index: int) -> str:
    if event.timestamp_ms > 0:
        return str(event.timestamp_ms)
    return str(max(1, fallback_index))


def _historical_chat_text(event: WhatIfEvent) -> str:
    if event.snippet:
        return event.snippet
    return f"[Historical {event.event_type}] {event.subject or event.thread_id}"


def _ticket_event_payload(event: WhatIfEvent) -> dict[str, Any]:
    ticket_id = event.thread_id.split(":", 1)[-1]
    if event.event_type == "assignment":
        return {
            "ticket_id": ticket_id,
            "assignee": event.actor_id,
            "description": event.snippet or event.subject,
        }
    if event.event_type == "approval":
        return {
            "ticket_id": ticket_id,
            "status": "resolved",
        }
    if event.event_type == "escalation":
        return {
            "ticket_id": ticket_id,
            "status": "blocked",
        }
    return {
        "ticket_id": ticket_id,
        "comment": event.snippet or event.subject,
        "author": event.actor_id,
    }


def _slack_thread_messages(
    messages: Sequence[dict[str, Any]],
    *,
    conversation_anchor: str,
) -> list[dict[str, Any]]:
    if not conversation_anchor:
        return [item for item in messages if isinstance(item, dict)]
    return [
        item
        for item in messages
        if isinstance(item, dict)
        and str(item.get("thread_ts") or item.get("ts") or "").split(".", 1)[0]
        == conversation_anchor
    ] or [item for item in messages if isinstance(item, dict)]


def _ticket_status_for_event(event: WhatIfEvent) -> str:
    if event.event_type == "approval":
        return "resolved"
    if event.event_type == "escalation":
        return "blocked"
    if event.event_type == "assignment":
        return "in_progress"
    return "open"


def _baseline_dataset(
    *,
    thread_subject: str,
    branch_event: WhatIfEvent,
    future_events: Sequence[WhatIfEvent],
    organization_domain: str,
    source_name: str,
) -> VEIDataset:
    baseline_events = [
        _baseline_event_payload(
            event,
            branch_event=branch_event,
            thread_subject=thread_subject,
            organization_domain=organization_domain,
        )
        for event in future_events
    ]
    return VEIDataset(
        metadata=DatasetMetadata(
            name=f"whatif-baseline-{branch_event.thread_id}",
            description="Historical future events scheduled after the branch point.",
            tags=["whatif", "baseline", "historical"],
            source=(
                "enron_rosetta"
                if source_name == "enron"
                else (
                    "historical_mail_archive"
                    if source_name == "mail_archive"
                    else "historical_company_history"
                )
            ),
        ),
        events=baseline_events,
    )


def _historical_body(event: WhatIfEvent) -> str:
    lines: list[str] = []
    if event.snippet:
        lines.append("[Historical email excerpt]")
        lines.append(event.snippet.strip())
        lines.append("")
        lines.append("[Excerpt limited by source data. Original body may be longer.]")
    else:
        lines.append("[Historical event recorded without body text excerpt]")
    notes = [f"Event type: {event.event_type}"]
    if event.flags.is_forward:
        notes.append("Forward detected in source metadata.")
    if event.flags.is_escalation:
        notes.append("Escalation detected in source metadata.")
    if event.flags.consult_legal_specialist:
        notes.append("Legal specialist signal present.")
    if event.flags.consult_trading_specialist:
        notes.append("Trading specialist signal present.")
    if event.flags.cc_count:
        notes.append(f"CC count: {event.flags.cc_count}.")
    if event.flags.bcc_count:
        notes.append(f"BCC count: {event.flags.bcc_count}.")
    return "\n".join(lines + ["", *notes]).strip()


def _history_prompt_line(event: WhatIfEventReference) -> str:
    target = ", ".join(event.to_recipients) or event.target_id or event.thread_id
    if event.surface == "slack":
        return (
            f"- Actor: {event.actor_id}\n"
            f"  Channel: {target}\n"
            f"  Type: {event.event_type}\n"
            f"  Thread: {event.subject}\n"
            f"  Text: {event.snippet}"
        )
    if event.surface == "tickets":
        return (
            f"- Actor: {event.actor_id}\n"
            f"  Ticket: {event.thread_id.split(':', 1)[-1]}\n"
            f"  Type: {event.event_type}\n"
            f"  Title: {event.subject}\n"
            f"  Detail: {event.snippet}"
        )
    return (
        f"- From: {event.actor_id}\n"
        f"  To: {target}\n"
        f"  Type: {event.event_type}\n"
        f"  Subject: {event.subject}\n"
        f"  Body: {event.snippet}"
    )


def _llm_surface_instructions(manifest: WhatIfEpisodeManifest) -> str:
    if manifest.surface == "slack":
        channel_name = _chat_channel_name_from_reference(manifest.branch_event)
        return (
            "Use surface='slack'. Set 'to' to the channel name, keep body_text as the chat text, "
            f"and keep conversation_anchor as '{manifest.branch_event.conversation_anchor or ''}' "
            f"for replies in {channel_name}."
        )
    if manifest.surface == "tickets":
        return (
            "Use surface='tickets'. Set 'to' to the ticket id, keep body_text as the ticket comment or update note, "
            "and keep the action on the same ticket."
        )
    return (
        "Use surface='mail'. Set 'to' to one allowed address, keep subject as a realistic email subject, "
        "and keep body_text as the email body."
    )


def _llm_replay_event(
    message: WhatIfLLMGeneratedMessage,
    *,
    manifest: WhatIfEpisodeManifest,
) -> BaseEvent:
    if message.surface == "slack":
        return BaseEvent(
            time_ms=message.delay_ms,
            actor_id=message.actor_id,
            channel="slack",
            type="counterfactual_chat",
            correlation_id=manifest.thread_id,
            payload={
                "channel": message.to,
                "text": message.body_text,
                "thread_ts": message.conversation_anchor or None,
                "user": message.actor_id,
            },
        )
    if message.surface == "tickets":
        return BaseEvent(
            time_ms=message.delay_ms,
            actor_id=message.actor_id,
            channel="tickets",
            type="counterfactual_ticket",
            correlation_id=manifest.thread_id,
            payload={
                "ticket_id": message.to,
                "comment": message.body_text,
                "author": message.actor_id,
            },
        )
    return BaseEvent(
        time_ms=message.delay_ms,
        actor_id=message.actor_id,
        channel="mail",
        type="counterfactual_email",
        correlation_id=manifest.thread_id,
        payload={
            "from": message.actor_id,
            "to": message.to,
            "subj": message.subject,
            "body_text": message.body_text,
            "thread_id": manifest.thread_id,
            "category": "counterfactual",
        },
    )


def _primary_recipient(event: WhatIfEvent) -> str:
    recipients = [item for item in event.flags.to_recipients if item]
    if recipients:
        return recipients[0]
    if event.target_id:
        return event.target_id
    return _historical_archive_address("", "archive")


def _historical_archive_address(organization_domain: str, local_part: str) -> str:
    normalized_domain = organization_domain.strip().lower()
    if not normalized_domain:
        return f"{local_part}@archive.local"
    return f"{local_part}@{normalized_domain}"


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


def _load_episode_snapshot(root: Path) -> dict[str, Any]:
    snapshot_path = root / "context_snapshot.json"
    if not snapshot_path.exists():
        raise ValueError(f"context snapshot not found: {snapshot_path}")
    return json.loads(snapshot_path.read_text(encoding="utf-8"))


def _load_episode_context(root: Path) -> dict[str, Any]:
    payload = _load_episode_snapshot(root)
    sources = payload.get("sources", [])
    for source in sources:
        if not isinstance(source, dict):
            continue
        data = source.get("data", {})
        if isinstance(data, dict):
            return data
    raise ValueError("what-if episode is missing a supported context source")


def _session_for_episode(
    root: Path,
    *,
    seed: int,
):
    bundle = load_customer_twin(root)
    asset_path = root / bundle.blueprint_asset_path
    asset = BlueprintAsset.model_validate_json(asset_path.read_text(encoding="utf-8"))
    return create_world_session_from_blueprint(asset, seed=seed)


def _coerce_episode_snapshot(
    *,
    snapshot: dict[str, Any] | None,
    context: dict[str, Any] | None,
) -> dict[str, Any]:
    if snapshot is not None:
        return snapshot
    if context is not None:
        return context
    raise ValueError("what-if episode is missing a saved context snapshot")


def _allowed_thread_participants(
    *,
    snapshot: dict[str, Any] | None = None,
    context: dict[str, Any] | None = None,
    manifest: WhatIfEpisodeManifest,
) -> tuple[list[str], list[str]]:
    _coerce_episode_snapshot(snapshot=snapshot, context=context)
    actors = sorted(
        {str(actor_id) for actor_id in manifest.actor_ids if str(actor_id).strip()}
    )
    recipients: set[str] = set(actors)
    for event in list(manifest.history_preview) + [manifest.branch_event]:
        if event.actor_id:
            recipients.add(event.actor_id)
        if event.target_id:
            recipients.add(event.target_id)
        for recipient in event.to_recipients:
            if recipient:
                recipients.add(recipient)
    if manifest.surface == "slack":
        recipients.add(_chat_channel_name_from_reference(manifest.branch_event))
    if manifest.surface == "tickets":
        recipients.add(manifest.thread_id.split(":", 1)[-1])
    return actors, sorted(recipients)


def _llm_counterfactual_prompt(
    *,
    snapshot: dict[str, Any] | None = None,
    context: dict[str, Any] | None = None,
    manifest: WhatIfEpisodeManifest,
    prompt: str,
    allowed_actors: Sequence[str],
    allowed_recipients: Sequence[str],
) -> str:
    _coerce_episode_snapshot(snapshot=snapshot, context=context)
    history_lines: list[str] = []
    for event in manifest.history_preview[-8:]:
        history_lines.append(_history_prompt_line(event))
    if not history_lines:
        history_lines.append(
            "- No earlier thread history was saved before this branch point."
        )
    surface_instructions = _llm_surface_instructions(manifest)
    prompt_lines = [
        f"Thread subject: {manifest.thread_subject}",
        f"Surface: {manifest.surface}",
        f"Case id: {manifest.case_id or manifest.thread_id}",
        f"Branch event id: {manifest.branch_event_id}",
        "Historical event being changed:",
        _history_prompt_line(manifest.branch_event),
    ]
    prompt_lines.extend(case_context_prompt_lines(manifest.case_context))
    prompt_lines.extend(public_context_prompt_lines(manifest.public_context))
    prompt_lines.extend(
        [
            "Allowed actors:",
            ", ".join(allowed_actors),
            "Allowed targets:",
            ", ".join(allowed_recipients),
            "Historical thread so far:",
            "\n".join(history_lines[:8]),
            "Surface instructions:",
            surface_instructions,
            "Counterfactual prompt:",
            prompt,
            "Generate only what happens on this thread after the divergence.",
        ]
    )
    return "\n".join(prompt_lines)


def _normalize_llm_messages(
    plan_args: dict[str, Any],
    *,
    manifest: WhatIfEpisodeManifest,
    allowed_actors: Sequence[str],
    allowed_recipients: Sequence[str],
) -> tuple[list[WhatIfLLMGeneratedMessage], list[str]]:
    raw_messages = plan_args.get("messages", plan_args.get("emails", []))
    if not isinstance(raw_messages, list):
        raw_messages = []
    normalized: list[WhatIfLLMGeneratedMessage] = []
    raw_notes = plan_args.get("notes", [])
    notes = (
        [str(item) for item in raw_notes if str(item).strip()]
        if isinstance(raw_notes, list)
        else []
    )
    actor_fallback = (
        allowed_actors[0]
        if allowed_actors
        else _historical_archive_address(
            manifest.organization_domain,
            "counterfactual",
        )
    )
    recipient_fallback = _preferred_recipient_fallback(
        allowed_recipients,
        organization_domain=manifest.organization_domain,
        default=actor_fallback,
    )

    for index, raw in enumerate(raw_messages[:3]):
        if not isinstance(raw, dict):
            continue
        surface = (
            str(raw.get("surface", manifest.surface) or manifest.surface)
            .strip()
            .lower()
        )
        if surface != manifest.surface:
            surface = manifest.surface
            notes.append(
                f"Message {index + 1} used a different surface; it was clamped to {surface}."
            )
        actor_id = str(raw.get("actor_id", actor_fallback)).strip()
        if actor_id not in allowed_actors:
            resolved_actor = _resolve_allowed_identity(actor_id, allowed_actors)
            actor_id = resolved_actor or actor_fallback
            notes.append(
                f"Message {index + 1} used a non-participant actor; it was clamped to {actor_id}."
            )
        recipient = str(raw.get("to", recipient_fallback)).strip()
        if recipient not in allowed_recipients:
            resolved_recipient = _resolve_allowed_identity(
                recipient, allowed_recipients
            )
            recipient = resolved_recipient or recipient_fallback
            notes.append(
                f"Message {index + 1} used a non-thread recipient; it was clamped to {recipient}."
            )
        body_text = str(raw.get("body_text", "")).strip()
        if not body_text:
            continue
        delay_ms = max(1000, safe_int(raw.get("delay_ms", (index + 1) * 1000)))
        conversation_anchor = str(
            raw.get(
                "conversation_anchor",
                raw.get("thread_anchor", manifest.branch_event.conversation_anchor),
            )
            or ""
        ).strip()
        normalized.append(
            WhatIfLLMGeneratedMessage(
                actor_id=actor_id,
                surface=surface,
                to=recipient,
                subject=_message_subject(
                    raw.get("subject"),
                    fallback=manifest.thread_subject,
                ),
                body_text=body_text,
                delay_ms=delay_ms,
                conversation_anchor=conversation_anchor if surface == "slack" else "",
                rationale=str(raw.get("rationale", "")).strip(),
            )
        )
    return normalized, notes


def _message_subject(value: Any, *, fallback: str) -> str:
    subject = str(value or "").strip()
    if subject:
        return subject
    if fallback.lower().startswith("re:"):
        return fallback
    return f"Re: {fallback}"


def _preferred_recipient_fallback(
    recipients: Sequence[str],
    *,
    organization_domain: str,
    default: str,
) -> str:
    for recipient in recipients:
        if (
            recipient
            and organization_domain
            and recipient.lower().endswith(f"@{organization_domain.lower()}")
            and not recipient.lower().startswith("group:")
        ):
            return recipient
    return recipients[0] if recipients else default


def _resolve_allowed_identity(
    raw_value: str,
    allowed_values: Sequence[str],
) -> str | None:
    normalized = raw_value.strip().lower()
    if not normalized:
        return None
    for allowed in allowed_values:
        if normalized == allowed.lower():
            return allowed

    wanted_tokens = _identity_tokens(normalized)
    if not wanted_tokens:
        return None

    best_match: str | None = None
    best_score = 0
    for allowed in allowed_values:
        candidate_tokens = _identity_tokens(allowed.lower())
        overlap = len(wanted_tokens & candidate_tokens)
        if overlap == 0:
            continue
        if normalized in allowed.lower() or allowed.lower() in normalized:
            overlap += 2
        if overlap > best_score:
            best_match = allowed
            best_score = overlap
    return best_match


def _identity_tokens(value: str) -> set[str]:
    cleaned = (
        value.replace("@", " ")
        .replace(".", " ")
        .replace("_", " ")
        .replace("-", " ")
        .replace("<", " ")
        .replace(">", " ")
    )
    return {token for token in cleaned.split() if len(token) >= 2}


def _counterfactual_args(plan: dict[str, Any]) -> dict[str, Any]:
    raw_args = plan.get("args")
    if isinstance(raw_args, dict):
        return raw_args
    return plan


def _counterfactual_notes(plan_args: dict[str, Any]) -> list[str]:
    raw_notes = plan_args.get("notes", [])
    if not isinstance(raw_notes, list):
        return []
    return [str(item) for item in raw_notes if str(item).strip()]


def _apply_recipient_scope(
    recipients: Sequence[str],
    *,
    organization_domain: str,
    tags: set[str],
) -> tuple[list[str], list[str]]:
    result = [str(item).strip() for item in recipients if str(item).strip()]
    internal_recipients = [
        recipient
        for recipient in result
        if organization_domain
        and recipient.lower().endswith(f"@{organization_domain.lower()}")
    ]
    internal_only = bool(
        {
            "hold",
            "pause_forward",
            "external_removed",
            "attachment_removed",
            "legal",
            "compliance",
        }
        & tags
    )
    if not internal_only or not internal_recipients:
        return result, []
    note = "Recipient scope was clamped to internal participants on this archive."
    if organization_domain.strip().lower() == ENRON_DOMAIN:
        note = "Recipient scope was clamped to internal Enron participants."
    return (
        internal_recipients,
        [note],
    )


def _baseline_tick_ms(dataset_path: Path) -> int:
    dataset = VEIDataset.model_validate_json(dataset_path.read_text(encoding="utf-8"))
    if not dataset.events:
        return 0
    return max(event.time_ms for event in dataset.events) + 1000


def _forecast_summary_from_counts(forecast: WhatIfForecast) -> str:
    return (
        f"{forecast.future_event_count} follow-up events remain, with "
        f"{forecast.future_escalation_count} escalations and "
        f"{forecast.future_external_event_count} external sends."
    )


def _forecast_delta_summary(delta: WhatIfForecastDelta) -> str:
    direction = (
        "down"
        if delta.risk_score_delta < 0
        else "up" if delta.risk_score_delta > 0 else "flat"
    )
    return (
        f"Predicted risk moves {direction} by {abs(delta.risk_score_delta):.3f}, "
        f"with escalation delta {delta.escalation_delta} and external-send delta "
        f"{delta.external_event_delta}."
    )
