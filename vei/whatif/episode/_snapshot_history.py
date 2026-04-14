from __future__ import annotations

from typing import Sequence

from vei.context.api import (
    ContextSnapshot,
    ContextSourceResult,
    CrmSourceData,
    GoogleSourceData,
    source_payload,
)

from .._helpers import (
    chat_channel_name_from_reference as _chat_channel_name_from_reference,
    reference_primary_recipient as _reference_primary_recipient,
)
from ..corpus import display_name
from ..models import (
    WhatIfCaseContext,
    WhatIfEventReference,
    WhatIfSituationContext,
)
from ._snapshot_merge import _context_source_record_counts


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
        references,
        key=lambda item: (item.timestamp, item.event_id),
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
        references,
        key=lambda item: (item.timestamp, item.event_id),
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
        references,
        key=lambda item: (item.timestamp, item.event_id),
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
    google_data = source_payload(google_source, GoogleSourceData)
    if google_source is None or google_data is None:
        return None
    documents = [
        item
        for item in google_data.documents
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
    data = source_payload(source, CrmSourceData)
    if source is None or data is None:
        return None
    deals = [
        item
        for item in data.deals
        if isinstance(item, dict)
        and str(item.get("id", item.get("deal_id", ""))).strip() in record_ids
    ]
    if not deals:
        return None
    filtered_data = {"deals": deals}
    return ContextSourceResult(
        provider=provider,
        captured_at=source.captured_at,
        status=source.status,
        record_counts=_context_source_record_counts(provider, filtered_data),
        data=filtered_data,
    )


__all__ = [
    "_case_history_source_results",
    "_case_record_source_results",
    "_situation_history_source_results",
    "_situation_record_source_results",
]
