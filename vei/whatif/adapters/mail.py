from __future__ import annotations


from vei.context.models import ContextSnapshot

from ..corpus import (
    _archive_thread_id,
    _archive_thread_subject,
    _archive_threads_from_snapshot,
    _company_history_thread_id,
    build_archive_event,
)
from ..models import WhatIfEvent


def build_mail_events(
    *,
    snapshot: ContextSnapshot,
    organization_domain: str,
    include_content: bool,
) -> list[WhatIfEvent]:
    try:
        threads_payload = _archive_threads_from_snapshot(snapshot)
    except Exception:  # noqa: BLE001
        return []

    events: list[WhatIfEvent] = []
    for thread_index, thread in enumerate(threads_payload):
        if not isinstance(thread, dict):
            continue
        raw_thread_id = _archive_thread_id(thread, index=thread_index)
        normalized_thread_id = _company_history_thread_id("mail", raw_thread_id)
        thread_subject = _archive_thread_subject(thread, fallback=normalized_thread_id)
        messages = [
            item for item in (thread.get("messages") or []) if isinstance(item, dict)
        ]
        for message_index, message in enumerate(messages):
            event = build_archive_event(
                message=message,
                thread_id=normalized_thread_id,
                thread_subject=thread_subject,
                organization_domain=organization_domain,
                thread_index=thread_index,
                message_index=message_index,
                include_content=include_content,
            )
            if event is None:
                continue
            events.append(
                event.model_copy(
                    update={
                        "surface": "mail",
                        "conversation_anchor": raw_thread_id,
                        "flags": event.flags.model_copy(
                            update={"source": "mail_archive"}
                        ),
                    }
                )
            )
    return events
