from __future__ import annotations

import logging
from typing import Any, Sequence

from vei.data.models import BaseEvent, DatasetMetadata, VEIDataset

from ..models import WhatIfEvent
from .._helpers import (
    chat_channel_name as _chat_channel_name,
    historical_archive_address as _historical_archive_address,
    primary_recipient as _primary_recipient,
)

logger = logging.getLogger(__name__)


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
