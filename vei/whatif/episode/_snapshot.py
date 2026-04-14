from __future__ import annotations

import json
import logging
import shutil
from pathlib import Path
from typing import Any, Sequence

from pydantic import ValidationError
from vei.context.models import ContextSnapshot, ContextSourceResult

from ..models import (
    WhatIfCaseContext,
    WhatIfEpisodeManifest,
    WhatIfEvent,
    WhatIfEventReference,
    WhatIfPublicContext,
    WhatIfSituationContext,
    WhatIfWorld,
)
from ..corpus import (
    CONTENT_NOTICE,
    _load_history_snapshot,
    display_name,
)
from .._helpers import (
    chat_channel_name as _chat_channel_name,
    chat_channel_name_from_reference as _chat_channel_name_from_reference,
    historical_archive_address as _historical_archive_address,
    load_episode_context as _load_episode_context,
    primary_recipient as _primary_recipient,
    reference_primary_recipient as _reference_primary_recipient,
)
from ._dataset import _historical_body, _historical_chat_text

logger = logging.getLogger(__name__)


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
    except (OSError, json.JSONDecodeError, ValueError, ValidationError) as exc:
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
    except (OSError, json.JSONDecodeError, ValueError) as exc:
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


def _chat_message_ts(event: WhatIfEvent, *, fallback_index: int) -> str:
    if event.timestamp_ms > 0:
        return str(event.timestamp_ms)
    return str(max(1, fallback_index))


def _ticket_status_for_event(event: WhatIfEvent) -> str:
    if event.event_type == "approval":
        return "resolved"
    if event.event_type == "escalation":
        return "blocked"
    if event.event_type == "assignment":
        return "in_progress"
    return "open"
