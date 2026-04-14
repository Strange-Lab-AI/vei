from __future__ import annotations

import logging
from collections import Counter
from typing import Any, Sequence

from ..models import (
    WhatIfActorProfile,
    WhatIfEvent,
    WhatIfEventMatch,
    WhatIfEventReference,
    WhatIfEventSearchResult,
    WhatIfThreadSummary,
    WhatIfWorld,
)
from .._helpers import event_reference as event_reference
from ._time import display_name
from ._util import _email_domain

logger = logging.getLogger(__name__)

_ENRON_DOMAIN = "enron.com"


def build_thread_summaries(
    events: Sequence[WhatIfEvent],
    *,
    organization_domain: str = _ENRON_DOMAIN,
) -> list[WhatIfThreadSummary]:
    buckets: dict[str, dict[str, Any]] = {}
    for event in events:
        bucket = buckets.setdefault(
            event.thread_id,
            {
                "thread_id": event.thread_id,
                "subject": event.subject or event.thread_id,
                "case_ids": Counter(),
                "surface": event.surface or "mail",
                "event_count": 0,
                "actor_ids": set(),
                "first_timestamp": event.timestamp,
                "last_timestamp": event.timestamp,
                "legal_event_count": 0,
                "trading_event_count": 0,
                "escalation_event_count": 0,
                "assignment_event_count": 0,
                "approval_event_count": 0,
                "forward_event_count": 0,
                "attachment_event_count": 0,
                "external_recipient_event_count": 0,
                "event_type_counts": Counter(),
            },
        )
        bucket["event_count"] += 1
        if event.case_id:
            bucket["case_ids"][event.case_id] += 1
        bucket["actor_ids"].add(event.actor_id)
        if event.target_id:
            bucket["actor_ids"].add(event.target_id)
        bucket["last_timestamp"] = event.timestamp
        if event.flags.consult_legal_specialist:
            bucket["legal_event_count"] += 1
        if event.flags.consult_trading_specialist:
            bucket["trading_event_count"] += 1
        if event.flags.is_escalation or event.event_type == "escalation":
            bucket["escalation_event_count"] += 1
        if event.event_type == "assignment":
            bucket["assignment_event_count"] += 1
        if event.event_type == "approval":
            bucket["approval_event_count"] += 1
        if event.flags.is_forward:
            bucket["forward_event_count"] += 1
        if event.flags.has_attachment_reference:
            bucket["attachment_event_count"] += 1
        if has_external_recipients(
            event.flags.to_recipients,
            organization_domain=organization_domain,
        ):
            bucket["external_recipient_event_count"] += 1
        bucket["event_type_counts"][event.event_type] += 1

    threads = [
        WhatIfThreadSummary(
            thread_id=payload["thread_id"],
            subject=payload["subject"],
            case_id=_primary_case_id(payload["case_ids"]),
            surface=payload["surface"],
            event_count=payload["event_count"],
            actor_ids=sorted(actor_id for actor_id in payload["actor_ids"] if actor_id),
            first_timestamp=payload["first_timestamp"],
            last_timestamp=payload["last_timestamp"],
            legal_event_count=payload["legal_event_count"],
            trading_event_count=payload["trading_event_count"],
            escalation_event_count=payload["escalation_event_count"],
            assignment_event_count=payload["assignment_event_count"],
            approval_event_count=payload["approval_event_count"],
            forward_event_count=payload["forward_event_count"],
            attachment_event_count=payload["attachment_event_count"],
            external_recipient_event_count=payload["external_recipient_event_count"],
            event_type_counts=dict(payload["event_type_counts"]),
        )
        for payload in buckets.values()
    ]
    return sorted(threads, key=lambda item: (-item.event_count, item.thread_id))


def build_actor_profiles(
    events: Sequence[WhatIfEvent],
    *,
    organization_domain: str = _ENRON_DOMAIN,
) -> list[WhatIfActorProfile]:
    buckets: dict[str, dict[str, Any]] = {}
    for event in events:
        touch_actor(
            buckets,
            actor_id=event.actor_id,
            sent=True,
            flagged=event_is_flagged(
                event,
                organization_domain=organization_domain,
            ),
            custodian_id=event.flags.custodian_id,
        )
        touch_actor(
            buckets,
            actor_id=event.target_id,
            received=True,
        )
    actors = [
        WhatIfActorProfile(
            actor_id=actor_id,
            email=actor_id,
            display_name=display_name(actor_id),
            custodian_ids=sorted(payload["custodian_ids"]),
            event_count=payload["event_count"],
            sent_count=payload["sent_count"],
            received_count=payload["received_count"],
            flagged_event_count=payload["flagged_event_count"],
        )
        for actor_id, payload in buckets.items()
        if actor_id
    ]
    return sorted(actors, key=lambda item: (-item.event_count, item.actor_id))


def touch_actor(
    buckets: dict[str, dict[str, Any]],
    *,
    actor_id: str,
    sent: bool = False,
    received: bool = False,
    flagged: bool = False,
    custodian_id: str = "",
) -> None:
    if not actor_id:
        return
    bucket = buckets.setdefault(
        actor_id,
        {
            "event_count": 0,
            "sent_count": 0,
            "received_count": 0,
            "flagged_event_count": 0,
            "custodian_ids": set(),
        },
    )
    bucket["event_count"] += 1
    if sent:
        bucket["sent_count"] += 1
    if received:
        bucket["received_count"] += 1
    if flagged:
        bucket["flagged_event_count"] += 1
    if custodian_id:
        bucket["custodian_ids"].add(custodian_id)


def thread_events(events: Sequence[WhatIfEvent], thread_id: str) -> list[WhatIfEvent]:
    return [
        event
        for event in sorted(events, key=lambda item: (item.timestamp_ms, item.event_id))
        if event.thread_id == thread_id
    ]


def event_by_id(events: Sequence[WhatIfEvent], event_id: str) -> WhatIfEvent | None:
    for event in events:
        if event.event_id == event_id:
            return event
    return None


def choose_branch_event(
    events: Sequence[WhatIfEvent],
    *,
    requested_event_id: str | None,
) -> WhatIfEvent:
    if not events:
        raise ValueError("cannot choose a branch event from an empty thread")
    if requested_event_id:
        selected = event_by_id(events, requested_event_id)
        if selected is None:
            raise ValueError(f"branch event not found in thread: {requested_event_id}")
        return selected
    if len(events) == 1:
        return events[0]
    prioritized = [
        event
        for event in events[:-1]
        if event.flags.is_escalation
        or event.flags.is_forward
        or event.event_type in {"assignment", "approval", "reply"}
    ]
    if prioritized:
        return prioritized[0]
    return events[max(0, (len(events) // 2) - 1)]


def thread_subject(
    threads: Sequence[WhatIfThreadSummary],
    thread_id: str,
    *,
    fallback: str,
) -> str:
    for thread in threads:
        if thread.thread_id == thread_id:
            return thread.subject
    return fallback or thread_id


def event_reason_labels(
    event: WhatIfEvent,
    *,
    organization_domain: str = _ENRON_DOMAIN,
) -> list[str]:
    labels: list[str] = []
    if event.flags.consult_legal_specialist:
        labels.append("legal")
    if event.flags.consult_trading_specialist:
        labels.append("trading")
    if event.flags.has_attachment_reference:
        labels.append("attachment")
    if event.flags.is_forward:
        labels.append("forward")
    if event.flags.is_escalation or event.event_type == "escalation":
        labels.append("escalation")
    if event.event_type == "assignment":
        labels.append("assignment")
    if event.event_type == "approval":
        labels.append("approval")
    if has_external_recipients(
        event.flags.to_recipients,
        organization_domain=organization_domain,
    ):
        labels.append("external_recipient")
    return labels


def _primary_case_id(counts: Counter[str]) -> str:
    if not counts:
        return ""
    return sorted(counts.items(), key=lambda item: (-item[1], item[0]))[0][0]


def event_is_flagged(
    event: WhatIfEvent,
    *,
    organization_domain: str = _ENRON_DOMAIN,
) -> bool:
    return bool(
        event_reason_labels(
            event,
            organization_domain=organization_domain,
        )
    )


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
    actor_token = (actor or "").strip().lower()
    participant_token = (participant or "").strip().lower()
    thread_token = (thread_id or "").strip()
    event_type_token = (event_type or "").strip().lower()
    query_token = (query or "").strip().lower()
    query_terms = _query_terms(query_token)
    effective_limit = max(1, int(limit))
    thread_by_id = {thread.thread_id: thread for thread in world.threads}

    raw_matches: list[tuple[WhatIfEvent, list[str], list[str], int, int]] = []
    total_match_count = 0
    for event in world.events:
        match_reasons: list[str] = []
        if actor_token:
            if actor_token not in event.actor_id.lower():
                continue
            match_reasons.append("actor")
        if participant_token:
            participants = [
                event.actor_id,
                event.target_id,
                *event.flags.to_recipients,
                *event.flags.cc_recipients,
            ]
            if not any(
                participant_token in item.lower() for item in participants if item
            ):
                continue
            match_reasons.append("participant")
        if thread_token:
            if event.thread_id != thread_token:
                continue
            match_reasons.append("thread")
        if event_type_token:
            if event.event_type.lower() != event_type_token:
                continue
            match_reasons.append("event_type")
        if query_terms:
            haystack = " ".join(
                [
                    event.event_id,
                    event.thread_id,
                    event.subject,
                    event.actor_id,
                    display_name(event.actor_id),
                    event.target_id,
                    display_name(event.target_id),
                    " ".join(event.flags.to_recipients),
                    " ".join(display_name(item) for item in event.flags.to_recipients),
                    " ".join(event.flags.cc_recipients),
                    " ".join(display_name(item) for item in event.flags.cc_recipients),
                    event.snippet,
                ]
            ).lower()
            if not all(term in haystack for term in query_terms):
                continue
            match_reasons.append("query")
        reason_labels = event_reason_labels(
            event,
            organization_domain=world.summary.organization_domain,
        )
        if flagged_only and not reason_labels:
            continue
        if flagged_only:
            match_reasons.append("flagged")

        total_match_count += 1
        if len(raw_matches) >= effective_limit:
            continue
        thread = thread_by_id.get(event.thread_id)
        raw_matches.append(
            (
                event,
                match_reasons,
                reason_labels,
                thread.event_count if thread is not None else 0,
                len(thread.actor_ids) if thread is not None else 0,
            )
        )

    if world.source == "enron":
        from ._enron import hydrate_event_snippets

        hydrated_events = hydrate_event_snippets(
            rosetta_dir=world.source_dir,
            events=[event for event, *_ in raw_matches],
        )
    else:
        hydrated_events = [event for event, *_ in raw_matches]
    matches = [
        WhatIfEventMatch(
            event=event_reference(event),
            match_reasons=match_reasons,
            reason_labels=reason_labels,
            thread_event_count=thread_event_count,
            participant_count=participant_count,
        )
        for event, (
            _,
            match_reasons,
            reason_labels,
            thread_event_count,
            participant_count,
        ) in zip(
            hydrated_events,
            raw_matches,
        )
    ]

    filters: dict[str, str | int | bool] = {"limit": effective_limit}
    if actor_token:
        filters["actor"] = actor_token
    if participant_token:
        filters["participant"] = participant_token
    if thread_token:
        filters["thread_id"] = thread_token
    if event_type_token:
        filters["event_type"] = event_type_token
    if query_token:
        filters["query"] = query_token
    if flagged_only:
        filters["flagged_only"] = True
    return WhatIfEventSearchResult(
        source=world.source,
        filters=filters,
        match_count=total_match_count,
        truncated=total_match_count > len(matches),
        matches=matches,
    )


def _query_terms(query: str) -> list[str]:
    if not query:
        return []
    normalized = query.replace("@", " ").replace(".", " ").replace("_", " ").strip()
    parts = [part for part in normalized.split() if len(part) >= 2]
    return parts or [query]


def has_external_recipients(
    recipients: Sequence[str],
    *,
    organization_domain: str = _ENRON_DOMAIN,
) -> bool:
    return (
        external_recipient_count(
            recipients,
            organization_domain=organization_domain,
        )
        > 0
    )


def branch_has_external_sharing(
    branch_event: WhatIfEventReference,
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


def is_internal_recipient(
    recipient: str,
    *,
    organization_domain: str = _ENRON_DOMAIN,
) -> bool:
    normalized_recipient = str(recipient or "").strip().lower()
    normalized_domain = str(organization_domain or "").strip().lower()
    if not normalized_recipient:
        return True
    if "@" not in normalized_recipient:
        return True
    if not normalized_domain:
        recipient_domain = _email_domain(normalized_recipient)
        if not recipient_domain:
            return True
        return recipient_domain.endswith(".local")
    return normalized_recipient.endswith(f"@{normalized_domain}")


def external_recipient_count(
    recipients: Sequence[str],
    *,
    organization_domain: str = _ENRON_DOMAIN,
) -> int:
    return sum(
        1
        for recipient in recipients
        if recipient
        and not is_internal_recipient(
            recipient,
            organization_domain=organization_domain,
        )
    )


def internal_recipient_count(
    recipients: Sequence[str],
    *,
    organization_domain: str = _ENRON_DOMAIN,
) -> int:
    return sum(
        1
        for recipient in recipients
        if is_internal_recipient(
            recipient,
            organization_domain=organization_domain,
        )
    )


def recipient_scope(
    recipients: Sequence[str],
    *,
    organization_domain: str = _ENRON_DOMAIN,
    empty_value: str = "unknown",
) -> str:
    cleaned = [
        str(recipient).strip() for recipient in recipients if str(recipient).strip()
    ]
    if not cleaned:
        return empty_value
    external = external_recipient_count(
        cleaned,
        organization_domain=organization_domain,
    )
    if external == 0:
        return "internal"
    if external == len(cleaned):
        return "external"
    return "mixed"


def matches_custodian_filter(
    event: WhatIfEvent,
    tokens: set[str],
) -> bool:
    if event.flags.custodian_id.lower() in tokens:
        return True
    return event.actor_id.lower() in tokens or event.target_id.lower() in tokens
