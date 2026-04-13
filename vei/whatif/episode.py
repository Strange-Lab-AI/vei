from __future__ import annotations

import logging
import shutil
from pathlib import Path
from typing import Any, Sequence

from vei.blueprint.api import create_world_session_from_blueprint
from vei.blueprint.models import BlueprintAsset
from vei.context.models import ContextSnapshot, ContextSourceResult
from vei.data.models import BaseEvent, DatasetMetadata, VEIDataset
from vei.twin import load_customer_twin
from vei.twin.api import build_customer_twin
from vei.twin.models import ContextMoldConfig

from .models import (
    WhatIfCaseContext,
    WhatIfEpisodeManifest,
    WhatIfEpisodeMaterialization,
    WhatIfEvent,
    WhatIfEventReference,
    WhatIfHistoricalScore,
    WhatIfPublicContext,
    WhatIfReplaySummary,
    WhatIfSituationContext,
    WhatIfWorld,
)
from .corpus import (
    CONTENT_NOTICE,
    ENRON_DOMAIN,
    _load_history_snapshot,
    choose_branch_event,
    display_name,
    event_by_id,
    event_reference,
    has_external_recipients,
    hydrate_event_snippets,
    thread_events,
    thread_subject,
)
from .cases import build_case_context
from .public_context import slice_public_context_to_branch
from .business_state import assess_historical_business_state
from ._helpers import (
    chat_channel_name as _chat_channel_name,
    chat_channel_name_from_reference as _chat_channel_name_from_reference,
    historical_archive_address as _historical_archive_address,
    load_episode_context as _load_episode_context,
    primary_recipient as _primary_recipient,
    reference_primary_recipient as _reference_primary_recipient,
)
from .situations import build_situation_context, recommend_branch_thread

logger = logging.getLogger(__name__)


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
    ) = resolve_thread_branch(
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
    situation_context = build_situation_context(
        world,
        branch_thread_id=selected_thread_id,
        branch_timestamp_ms=branch_event.timestamp_ms,
    )
    forecast = score_historical_tail(
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
        situation_context=situation_context,
        historical_business_state=historical_business_state,
        source_snapshot=source_snapshot,
    )
    included_surfaces = _included_surfaces_for_thread(
        thread_history,
        case_context=case_context,
        situation_context=situation_context,
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
        situation_context=situation_context,
        historical_business_state=historical_business_state,
    )
    manifest_path = workspace_root / "episode_manifest.json"
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
        situation_context=situation_context,
        historical_business_state=historical_business_state,
    )


def resolve_thread_branch(
    world: WhatIfWorld,
    *,
    thread_id: str | None = None,
    event_id: str | None = None,
) -> tuple[
    str, list[WhatIfEvent], WhatIfEvent, list[WhatIfEvent], list[WhatIfEvent], str
]:
    selected_thread_id = thread_id
    if selected_thread_id is None:
        if event_id:
            selected_event = event_by_id(world.events, event_id)
            if selected_event is None:
                raise ValueError(f"event not found in world: {event_id}")
            selected_thread_id = selected_event.thread_id
        else:
            selected_thread_id = recommend_branch_thread(world).thread_id

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
    situation_context: WhatIfSituationContext | None,
    historical_business_state,
    source_snapshot: ContextSnapshot | None,
) -> ContextSnapshot:
    metadata = _episode_snapshot_metadata(
        world=world,
        thread_id=thread_id,
        branch_event=branch_event,
        public_context=public_context,
        case_context=case_context,
        situation_context=situation_context,
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
            situation_context=situation_context,
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
            situation_context=situation_context,
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
            situation_context=situation_context,
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
    situation_context: WhatIfSituationContext | None,
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
            "situation_context": (
                situation_context.model_dump(mode="json")
                if situation_context is not None
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
        return _load_history_snapshot(world.source_dir)
    except Exception as exc:  # noqa: BLE001
        logger.warning(
            "whatif source snapshot load failed for %s (%s)",
            world.source,
            type(exc).__name__,
            extra={
                "source": "episode",
                "provider": world.source,
                "file_path": str(world.source_dir),
                "exception_type": type(exc).__name__,
            },
            exc_info=True,
        )
        return None


def _append_case_context_sources(
    *,
    snapshot: ContextSnapshot,
    case_context: WhatIfCaseContext | None,
    situation_context: WhatIfSituationContext | None,
    source_snapshot: ContextSnapshot | None,
) -> ContextSnapshot:
    if case_context is None and situation_context is None:
        return snapshot

    extra_sources: list[ContextSourceResult] = []
    if case_context is not None:
        extra_sources.extend(_case_history_source_results(case_context))
    if situation_context is not None:
        extra_sources.extend(_situation_history_source_results(situation_context))
    if source_snapshot is not None:
        if case_context is not None:
            extra_sources.extend(
                _case_record_source_results(
                    case_context=case_context,
                    source_snapshot=source_snapshot,
                )
            )
        if situation_context is not None:
            extra_sources.extend(
                _situation_record_source_results(
                    situation_context=situation_context,
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


def _situation_history_source_results(
    situation_context: WhatIfSituationContext,
) -> list[ContextSourceResult]:
    temporary_case_context = WhatIfCaseContext(
        case_id=situation_context.situation_id,
        title=situation_context.label,
        related_history=list(situation_context.related_history),
    )
    return _case_history_source_results(temporary_case_context)


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


def _situation_record_source_results(
    *,
    situation_context: WhatIfSituationContext,
    source_snapshot: ContextSnapshot,
) -> list[ContextSourceResult]:
    google_record_ids: set[str] = set()
    crm_record_ids_by_provider: dict[str, set[str]] = {
        "crm": set(),
        "salesforce": set(),
    }
    for thread in situation_context.related_threads:
        provider, _, record_id = thread.thread_id.partition(":")
        normalized_provider = provider.strip().lower()
        normalized_record_id = record_id.strip()
        if not normalized_record_id:
            continue
        if normalized_provider == "docs":
            google_record_ids.add(normalized_record_id)
        elif normalized_provider in crm_record_ids_by_provider:
            crm_record_ids_by_provider[normalized_provider].add(normalized_record_id)

    sources: list[ContextSourceResult] = []
    google_source = _filtered_google_record_source(
        source_snapshot=source_snapshot,
        record_ids=google_record_ids,
    )
    if google_source is not None:
        sources.append(google_source)
    for provider, record_ids in crm_record_ids_by_provider.items():
        source = _filtered_crm_record_source(
            source_snapshot=source_snapshot,
            provider=provider,
            record_ids=record_ids,
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
    situation_context: WhatIfSituationContext | None = None,
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
    if situation_context is not None:
        surfaces.update(
            thread.surface
            for thread in situation_context.related_threads
            if thread.surface
        )
        surfaces.update(
            reference.surface
            for reference in situation_context.related_history
            if reference.surface
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
    except Exception as exc:  # noqa: BLE001
        logger.warning(
            "whatif saved episode context load failed for %s (%s)",
            manifest.thread_id,
            type(exc).__name__,
            extra={
                "source": "episode",
                "provider": "context_snapshot",
                "file_path": str(workspace_root / "context_snapshot.json"),
                "exception_type": type(exc).__name__,
            },
            exc_info=True,
        )
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


def _persist_workspace_historical_source(
    world: WhatIfWorld,
    workspace_root: Path,
) -> None:
    if world.source not in {"mail_archive", "company_history"}:
        return
    source_file = _historical_source_file(world.source_dir)
    if source_file is None or not source_file.exists():
        return
    target = workspace_root / "context_snapshot.json"
    if source_file.resolve() == target.resolve():
        return
    shutil.copyfile(source_file, target)


def _historical_source_file(source_dir: Path) -> Path | None:
    resolved = source_dir.expanduser().resolve()
    if resolved.is_file():
        return resolved
    for filename in ("context_snapshot.json",):
        candidate = resolved / filename
        if candidate.exists():
            return candidate
    return None


def load_episode_manifest(root: str | Path) -> WhatIfEpisodeManifest:
    workspace_root = Path(root).expanduser().resolve()
    manifest_path = workspace_root / "episode_manifest.json"
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


def score_historical_tail(
    events: Sequence[WhatIfEvent],
    *,
    organization_domain: str = ENRON_DOMAIN,
) -> WhatIfHistoricalScore:
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
    return WhatIfHistoricalScore(
        backend="historical",
        future_event_count=future_event_count,
        future_escalation_count=future_escalation_count,
        future_assignment_count=future_assignment_count,
        future_approval_count=future_approval_count,
        future_external_event_count=future_external_event_count,
        risk_score=round(risk_score, 3),
        summary=summary,
    )


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
