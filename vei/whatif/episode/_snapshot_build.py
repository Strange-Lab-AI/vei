from __future__ import annotations

from typing import Any, Sequence

from vei.context.api import ContextSnapshot, ContextSourceResult

from .._helpers import chat_channel_name as _chat_channel_name
from ..models import (
    WhatIfCaseContext,
    WhatIfEvent,
    WhatIfPublicContext,
    WhatIfSituationContext,
    WhatIfWorld,
)
from ._snapshot_history import (
    _case_history_source_results,
    _case_record_source_results,
    _situation_history_source_results,
    _situation_record_source_results,
)
from ._snapshot_merge import _merge_context_source_result
from ._snapshot_shared import (
    _archive_message_payload,
    _chat_message_ts,
    _episode_snapshot_metadata,
    _historical_chat_text,
    _thread_actor_payload,
    _ticket_status_for_event,
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
    historical_business_state: Any,
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


__all__ = ["_episode_context_snapshot"]
