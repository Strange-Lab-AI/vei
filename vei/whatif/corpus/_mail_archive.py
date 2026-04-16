from __future__ import annotations

import json
import logging
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Sequence

from vei.context.api import legacy_threads_payload_to_snapshot
from vei.context.models import (
    ContextSnapshot,
    GmailSourceData,
    MailArchiveSourceData,
    source_payload,
)

from ..cases import assign_case_ids, build_case_summaries
from ..models import (
    WhatIfArtifactFlags,
    WhatIfEvent,
    WhatIfScenario,
    WhatIfWorld,
    WhatIfWorldSummary,
)
from ..public_context import resolve_world_public_context
from ..situations import build_situation_graph
from ._aggregation import build_actor_profiles, build_thread_summaries
from ._time import resolve_time_window, safe_int, timestamp_to_ms, timestamp_to_text
from ._util import (
    _archive_event_type,
    _company_history_thread_id,
    _contains_keyword,
    _fallback_internal_address,
    _has_attachment_reference,
    _message_flag,
    _organization_domain_from_threads,
    _organization_name_from_domain,
    _override_actor_profiles,
    _recipient_list,
    _truncate_snippet,
)

logger = logging.getLogger(__name__)

MAIL_ARCHIVE_CONTENT_NOTICE = (
    "Historical email bodies come from the supplied mail archive snapshot. "
    "They reflect the available archive text for each message."
)
MAIL_ARCHIVE_FILE_NAMES = ("context_snapshot.json",)
MAIL_SOURCE_PROVIDERS = {"mail_archive", "gmail"}


def load_mail_archive_world(
    *,
    source_dir: str | Path,
    scenarios: Sequence[WhatIfScenario] | None = None,
    time_window: tuple[str, str] | None = None,
    max_events: int | None = None,
    include_content: bool = False,
) -> WhatIfWorld:
    resolved_source_dir = Path(source_dir).expanduser().resolve()
    snapshot = _load_mail_archive_snapshot(resolved_source_dir)
    source_payload = _mail_archive_source_payload(snapshot)
    threads_payload = _archive_threads_from_snapshot(snapshot)
    if not threads_payload:
        raise ValueError("mail archive source does not contain any threads")

    organization_name = str(snapshot.organization_name or "").strip()
    organization_domain = str(snapshot.organization_domain or "").strip().lower()
    if not organization_domain:
        organization_domain = _organization_domain_from_threads(threads_payload)
    if not organization_name:
        organization_name = _organization_name_from_domain(organization_domain)

    time_bounds = resolve_time_window(time_window)
    events: list[WhatIfEvent] = []
    for thread_index, thread in enumerate(threads_payload):
        if not isinstance(thread, dict):
            continue
        thread_id = _archive_thread_id(thread, index=thread_index)
        thread_subject = _archive_thread_subject(thread, fallback=thread_id)
        messages = [
            item for item in (thread.get("messages") or []) if isinstance(item, dict)
        ]
        for message_index, message in enumerate(messages):
            event = build_archive_event(
                message=message,
                thread_id=thread_id,
                thread_subject=thread_subject,
                organization_domain=organization_domain,
                thread_index=thread_index,
                message_index=message_index,
                include_content=include_content,
            )
            if event is None:
                continue
            if time_bounds is not None and not (
                time_bounds[0] <= event.timestamp_ms <= time_bounds[1]
            ):
                continue
            events.append(event)

    events.sort(key=lambda item: (item.timestamp_ms, item.event_id))
    events = assign_case_ids(events)
    if max_events is not None:
        events = events[: max(0, int(max_events))]

    archive_actor_payload = source_payload.get("actors", [])
    actors = _override_actor_profiles(
        build_actor_profiles(events, organization_domain=organization_domain),
        actor_payload=(
            archive_actor_payload if isinstance(archive_actor_payload, list) else []
        ),
    )
    threads = build_thread_summaries(
        events,
        organization_domain=organization_domain,
    )
    cases = build_case_summaries(events)
    situation_graph = build_situation_graph(
        threads=threads,
        cases=cases,
        events=events,
    )
    summary = WhatIfWorldSummary(
        source="mail_archive",
        organization_name=organization_name,
        organization_domain=organization_domain,
        event_count=len(events),
        thread_count=len(threads),
        actor_count=len(actors),
        custodian_count=0,
        first_timestamp=events[0].timestamp if events else "",
        last_timestamp=events[-1].timestamp if events else "",
        event_type_counts=dict(Counter(event.event_type for event in events)),
        key_actor_ids=[actor.actor_id for actor in actors[:5]],
    )
    public_context = resolve_world_public_context(
        source="mail_archive",
        source_dir=resolved_source_dir,
        organization_name=summary.organization_name,
        organization_domain=summary.organization_domain,
        window_start=summary.first_timestamp,
        window_end=summary.last_timestamp,
        metadata=snapshot.metadata,
    )
    return WhatIfWorld(
        source="mail_archive",
        source_dir=resolved_source_dir,
        summary=summary,
        scenarios=list(scenarios or []),
        actors=actors,
        threads=threads,
        cases=cases,
        events=events,
        situation_graph=situation_graph,
        metadata={"content_notice": MAIL_ARCHIVE_CONTENT_NOTICE},
        public_context=public_context,
    )


def _load_mail_archive_snapshot(path: Path) -> ContextSnapshot:
    return load_history_snapshot(path)


def load_history_snapshot(path: Path) -> ContextSnapshot:
    if path.is_file():
        return _snapshot_from_json_payload(path)
    for filename in MAIL_ARCHIVE_FILE_NAMES:
        candidate = path / filename
        if candidate.exists():
            return _snapshot_from_json_payload(candidate)
    raise ValueError(f"mail archive snapshot not found under: {path}")


_load_history_snapshot = load_history_snapshot


def _snapshot_from_json_payload(path: Path) -> ContextSnapshot:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if isinstance(payload, dict) and isinstance(payload.get("sources"), list):
        return ContextSnapshot.model_validate(payload)
    if isinstance(payload, dict) and isinstance(payload.get("threads"), list):
        return _snapshot_from_archive_payload(payload)
    raise ValueError(f"unsupported mail archive payload: {path}")


def _snapshot_from_archive_payload(payload: dict[str, Any]) -> ContextSnapshot:
    organization_domain = str(payload.get("organization_domain", "") or "").strip()
    return legacy_threads_payload_to_snapshot(
        payload,
        fallback_organization_name=_organization_name_from_domain(organization_domain),
        include_payload_metadata=True,
    )


def _mail_archive_source_payload(snapshot: ContextSnapshot) -> dict[str, Any]:
    mail_archive_source = snapshot.source_for("mail_archive")
    gmail_source = snapshot.source_for("gmail")
    archive_data = source_payload(mail_archive_source, MailArchiveSourceData)
    gmail_data = source_payload(gmail_source, GmailSourceData)
    archive_payload = (
        archive_data.model_dump(mode="python") if archive_data is not None else None
    )
    gmail_payload = (
        gmail_data.model_dump(mode="python") if gmail_data is not None else None
    )
    if archive_payload is not None or gmail_payload is not None:
        return {
            "threads": _merge_archive_thread_payloads(
                archive_payload.get("threads", []) if archive_payload else [],
                (
                    _archive_threads_from_gmail_payload(gmail_payload)
                    if gmail_payload
                    else []
                ),
            ),
            "actors": archive_payload.get("actors", []) if archive_payload else [],
        }
    raise ValueError("snapshot does not contain a mail archive or gmail mail source")


def _mail_archive_source_payload_or_empty(snapshot: ContextSnapshot) -> dict[str, Any]:
    try:
        return _mail_archive_source_payload(snapshot)
    except ValueError as exc:
        logger.warning(
            "whatif mail archive payload unavailable (%s)",
            type(exc).__name__,
            extra={
                "source": "company_history",
                "provider": "mail_archive",
                "file_path": "",
                "exception_type": type(exc).__name__,
            },
            exc_info=True,
        )
        return {}


def _archive_threads_from_snapshot(snapshot: ContextSnapshot) -> list[dict[str, Any]]:
    source_payload = _mail_archive_source_payload(snapshot)
    threads = source_payload.get("threads", [])
    return [thread for thread in threads if isinstance(thread, dict)]


def _merge_archive_thread_payloads(
    archive_threads: Sequence[Any],
    gmail_threads: Sequence[Any],
) -> list[dict[str, Any]]:
    merged: list[dict[str, Any]] = []
    seen_thread_ids: set[str] = set()
    for thread_group in (archive_threads, gmail_threads):
        for thread in thread_group:
            if not isinstance(thread, dict):
                continue
            thread_id = str(thread.get("thread_id", "") or "").strip()
            if thread_id:
                if thread_id in seen_thread_ids:
                    continue
                seen_thread_ids.add(thread_id)
            merged.append(thread)
    return merged


def _archive_threads_from_gmail_payload(
    payload: dict[str, Any],
) -> list[dict[str, Any]]:
    threads: list[dict[str, Any]] = []
    for thread in payload.get("threads", []):
        if not isinstance(thread, dict):
            continue
        messages: list[dict[str, Any]] = []
        for message in thread.get("messages", []):
            if not isinstance(message, dict):
                continue
            messages.append(
                {
                    "message_id": str(
                        message.get("message_id", message.get("id", "")) or ""
                    ),
                    "from": str(message.get("from", "") or ""),
                    "to": str(message.get("to", "") or ""),
                    "subject": str(
                        message.get("subject", thread.get("subject", "")) or ""
                    ),
                    "body_text": str(
                        message.get("snippet", message.get("body_text", "")) or ""
                    ),
                    "date": str(message.get("date", "") or ""),
                    "time_ms": safe_int(message.get("internal_date", 0)),
                    "unread": bool(message.get("unread", False)),
                }
            )
        if not messages:
            continue
        threads.append(
            {
                "thread_id": str(thread.get("thread_id", "") or ""),
                "subject": str(thread.get("subject", "") or ""),
                "category": "historical",
                "messages": messages,
            }
        )
    return threads


def build_archive_event(
    *,
    message: dict[str, Any],
    thread_id: str,
    thread_subject: str,
    organization_domain: str,
    thread_index: int,
    message_index: int,
    include_content: bool,
) -> WhatIfEvent | None:
    actor_id = str(message.get("from", "") or "").strip()
    recipients = _recipient_list(message.get("to"))
    subject = _archive_subject(message, fallback=thread_subject)
    body_text = _archive_body_text(message)
    cc_recipients = _recipient_list(message.get("cc"))
    event_id = _archive_event_id(
        message=message,
        thread_id=thread_id,
        thread_index=thread_index,
        message_index=message_index,
    )
    if not actor_id and not recipients:
        return None

    timestamp_ms, timestamp_text = _archive_timestamp(
        message=message,
        thread_index=thread_index,
        message_index=message_index,
    )
    is_forward = _message_flag(
        message,
        key="is_forward",
        subject=subject,
        prefixes=("fw:", "fwd:"),
    )
    is_reply = _message_flag(
        message,
        key="is_reply",
        subject=subject,
        prefixes=("re:",),
    )
    is_escalation = bool(message.get("is_escalation", False)) or _contains_keyword(
        body_text,
        ("escalate", "urgent", "executive", "leadership"),
    )
    event_type = _archive_event_type(
        message=message,
        subject=subject,
        body_text=body_text,
        is_forward=is_forward,
        is_reply=is_reply,
        is_escalation=is_escalation,
    )

    snippet = body_text if include_content else _truncate_snippet(body_text)
    flags = WhatIfArtifactFlags(
        consult_legal_specialist=_contains_keyword(
            " ".join(
                [subject, body_text, " ".join(recipients), " ".join(cc_recipients)]
            ),
            ("legal", "counsel", "attorney", "compliance", "regulatory"),
        ),
        consult_trading_specialist=_contains_keyword(
            " ".join(
                [subject, body_text, " ".join(recipients), " ".join(cc_recipients)]
            ),
            ("trading", "trade", "desk", "market"),
        ),
        has_attachment_reference=_has_attachment_reference(message, body_text),
        is_escalation=is_escalation,
        is_forward=is_forward,
        is_reply=is_reply,
        cc_count=len(cc_recipients),
        to_count=len(recipients),
        to_recipients=recipients,
        cc_recipients=cc_recipients,
        subject=subject,
        norm_subject=subject.lower().strip(),
        body_sha1=str(message.get("body_sha1", "") or ""),
        custodian_id=str(message.get("custodian_id", "") or ""),
        message_id=str(
            message.get("message_id", message.get("id", message.get("mid", ""))) or ""
        ),
        folder=str(message.get("folder", "") or ""),
        source="mail_archive",
    )
    return WhatIfEvent(
        event_id=event_id,
        timestamp=timestamp_text,
        timestamp_ms=timestamp_ms,
        actor_id=actor_id or _fallback_internal_address(organization_domain, "unknown"),
        target_id=recipients[0] if recipients else "",
        event_type=event_type,
        thread_id=thread_id,
        surface="mail",
        subject=subject,
        snippet=snippet,
        flags=flags,
    )


def _archive_thread_id(thread: dict[str, Any], *, index: int) -> str:
    thread_id = str(thread.get("thread_id", "") or "").strip()
    if thread_id:
        return thread_id
    return f"archive-thread-{index + 1:04d}"


def _archive_thread_subject(thread: dict[str, Any], *, fallback: str) -> str:
    subject = str(thread.get("subject", thread.get("title", "")) or "").strip()
    return subject or fallback


def _archive_subject(message: dict[str, Any], *, fallback: str) -> str:
    subject = str(message.get("subject", message.get("subj", "")) or "").strip()
    return subject or fallback


def _archive_body_text(message: dict[str, Any]) -> str:
    return str(
        message.get(
            "body_text",
            message.get("snippet", message.get("content", "")),
        )
        or ""
    ).strip()


def _archive_event_id(
    *,
    message: dict[str, Any],
    thread_id: str,
    thread_index: int,
    message_index: int,
) -> str:
    for key in ("event_id", "message_id", "id", "mid"):
        value = str(message.get(key, "") or "").strip()
        if value:
            return value
    safe_thread_id = thread_id.replace(" ", "-")
    return f"archive_{safe_thread_id}_{thread_index + 1}_{message_index + 1}"


def _archive_timestamp(
    *,
    message: dict[str, Any],
    thread_index: int,
    message_index: int,
) -> tuple[int, str]:
    for key in ("timestamp", "sent_at", "date", "created_at"):
        value = message.get(key)
        if value:
            return (timestamp_to_ms(value), timestamp_to_text(value))
    raw_time_ms = message.get("time_ms")
    if raw_time_ms not in {None, ""}:
        numeric = safe_int(raw_time_ms)
        return (
            numeric,
            timestamp_to_text(datetime.fromtimestamp(numeric / 1000, tz=timezone.utc)),
        )
    synthetic_time_ms = ((thread_index + 1) * 1_000_000) + ((message_index + 1) * 1_000)
    return (
        synthetic_time_ms,
        timestamp_to_text(
            datetime.fromtimestamp(synthetic_time_ms / 1000, tz=timezone.utc)
        ),
    )


def _company_history_mail_events(
    *,
    snapshot: ContextSnapshot,
    organization_domain: str,
    include_content: bool,
) -> list[WhatIfEvent]:
    try:
        threads_payload = _archive_threads_from_snapshot(snapshot)
    except ValueError as exc:
        logger.warning(
            "whatif mail history fallback failed for provider %s (%s)",
            "mail_archive",
            type(exc).__name__,
            extra={
                "source": "company_history",
                "provider": "mail_archive",
                "file_path": "",
                "exception_type": type(exc).__name__,
            },
            exc_info=True,
        )
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
