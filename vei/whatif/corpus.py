from __future__ import annotations

import json
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Sequence

from .models import (
    WhatIfActorProfile,
    WhatIfArtifactFlags,
    WhatIfEvent,
    WhatIfEventMatch,
    WhatIfEventReference,
    WhatIfEventSearchResult,
    WhatIfScenario,
    WhatIfThreadSummary,
    WhatIfWorld,
    WhatIfWorldSummary,
)

ENRON_DOMAIN = "enron.com"
CONTENT_NOTICE = (
    "Historical email bodies are built from Rosetta excerpts and event metadata. "
    "They are grounded, but they are not full original messages."
)
EXECUTIVE_MARKERS = ("skilling", "lay", "fastow", "kean")


def load_enron_world(
    *,
    rosetta_dir: str | Path,
    scenarios: Sequence[WhatIfScenario] | None = None,
    time_window: tuple[str, str] | None = None,
    custodian_filter: Sequence[str] | None = None,
    max_events: int | None = None,
    include_content: bool = False,
) -> WhatIfWorld:
    try:
        import pyarrow.parquet as pq
    except ImportError as exc:  # pragma: no cover - guarded by dependency
        raise RuntimeError(
            "pyarrow is required for `vei whatif` parquet loading"
        ) from exc

    base = Path(rosetta_dir).expanduser().resolve()
    metadata_path = base / "enron_rosetta_events_metadata.parquet"
    content_path = base / "enron_rosetta_events_content.parquet"
    if not metadata_path.exists():
        raise ValueError(f"metadata parquet not found: {metadata_path}")
    if not content_path.exists():
        raise ValueError(f"content parquet not found: {content_path}")

    metadata_rows = pq.read_table(
        metadata_path,
        columns=[
            "event_id",
            "timestamp",
            "actor_id",
            "target_id",
            "event_type",
            "thread_task_id",
            "artifacts",
        ],
    ).to_pylist()
    content_by_id = (
        load_content_by_event_ids(
            rosetta_dir=base,
            event_ids=[
                str(row.get("event_id", ""))
                for row in metadata_rows
                if str(row.get("event_id", "")).strip()
            ],
        )
        if include_content
        else {}
    )

    time_bounds = resolve_time_window(time_window)
    custodian_tokens = {item.strip().lower() for item in custodian_filter or [] if item}
    events: list[WhatIfEvent] = []
    for row in metadata_rows:
        event = build_event(row, content_by_id.get(str(row.get("event_id", "")), ""))
        if event is None:
            continue
        if time_bounds is not None and not (
            time_bounds[0] <= event.timestamp_ms <= time_bounds[1]
        ):
            continue
        if custodian_tokens and not matches_custodian_filter(event, custodian_tokens):
            continue
        events.append(event)

    events.sort(key=lambda item: (item.timestamp_ms, item.event_id))
    if max_events is not None:
        events = events[: max(0, int(max_events))]

    threads = build_thread_summaries(events)
    actors = build_actor_profiles(events)
    summary = WhatIfWorldSummary(
        source="enron",
        event_count=len(events),
        thread_count=len(threads),
        actor_count=len(actors),
        custodian_count=len(
            {
                custodian
                for actor in actors
                for custodian in actor.custodian_ids
                if custodian
            }
        ),
        first_timestamp=events[0].timestamp if events else "",
        last_timestamp=events[-1].timestamp if events else "",
        event_type_counts=dict(Counter(event.event_type for event in events)),
        key_actor_ids=[actor.actor_id for actor in actors[:5]],
    )
    return WhatIfWorld(
        source="enron",
        rosetta_dir=base,
        summary=summary,
        scenarios=list(scenarios or []),
        actors=actors,
        threads=threads,
        events=events,
        metadata={"content_notice": CONTENT_NOTICE},
    )


def load_content_by_event_ids(
    *,
    rosetta_dir: str | Path,
    event_ids: Sequence[str],
) -> dict[str, str]:
    unique_event_ids = sorted(
        {str(item).strip() for item in event_ids if str(item).strip()}
    )
    if not unique_event_ids:
        return {}
    try:
        import pyarrow.parquet as pq
    except ImportError as exc:  # pragma: no cover - guarded by dependency
        raise RuntimeError(
            "pyarrow is required for `vei whatif` parquet loading"
        ) from exc

    content_path = (
        Path(rosetta_dir).expanduser().resolve()
        / "enron_rosetta_events_content.parquet"
    )
    if not content_path.exists():
        return {}
    content_rows = pq.read_table(
        content_path,
        columns=["event_id", "content"],
        filters=[("event_id", "in", unique_event_ids)],
    ).to_pylist()
    return {
        str(row.get("event_id", "")): str(row.get("content", "") or "")
        for row in content_rows
        if str(row.get("event_id", "")).strip()
    }


def hydrate_event_snippets(
    *,
    rosetta_dir: str | Path,
    events: Sequence[WhatIfEvent],
) -> list[WhatIfEvent]:
    missing_ids = [event.event_id for event in events if not event.snippet]
    if not missing_ids:
        return list(events)
    content_by_id = load_content_by_event_ids(
        rosetta_dir=rosetta_dir,
        event_ids=missing_ids,
    )
    hydrated: list[WhatIfEvent] = []
    for event in events:
        snippet = content_by_id.get(event.event_id, event.snippet)
        hydrated.append(event.model_copy(update={"snippet": snippet}))
    return hydrated


def build_event(row: dict[str, Any], content: str) -> WhatIfEvent | None:
    event_id = str(row.get("event_id", "")).strip()
    if not event_id:
        return None
    timestamp = row.get("timestamp")
    timestamp_ms = timestamp_to_ms(timestamp)
    timestamp_text = timestamp_to_text(timestamp)
    artifacts = artifact_flags(row.get("artifacts"))
    thread_id = str(row.get("thread_task_id", "") or event_id)
    subject = artifacts.subject or artifacts.norm_subject or thread_id
    return WhatIfEvent(
        event_id=event_id,
        timestamp=timestamp_text,
        timestamp_ms=timestamp_ms,
        actor_id=str(row.get("actor_id", "") or ""),
        target_id=str(row.get("target_id", "") or ""),
        event_type=str(row.get("event_type", "") or ""),
        thread_id=thread_id,
        subject=subject,
        snippet=str(content or ""),
        flags=artifacts,
    )


def artifact_flags(raw: Any) -> WhatIfArtifactFlags:
    if isinstance(raw, str):
        try:
            payload = json.loads(raw)
        except json.JSONDecodeError:
            payload = {}
    elif isinstance(raw, dict):
        payload = dict(raw)
    else:
        payload = {}

    return WhatIfArtifactFlags(
        consult_legal_specialist=bool(payload.get("consult_legal_specialist", False)),
        consult_trading_specialist=bool(
            payload.get("consult_trading_specialist", False)
        ),
        has_attachment_reference=bool(payload.get("has_attachment_reference", False)),
        is_escalation=bool(payload.get("is_escalation", False)),
        is_forward=bool(payload.get("is_forward", False)),
        is_reply=bool(payload.get("is_reply", False)),
        cc_count=safe_int(payload.get("cc_count")),
        bcc_count=safe_int(payload.get("bcc_count")),
        to_count=safe_int(payload.get("to_count")),
        to_recipients=string_list(payload.get("to_recipients")),
        cc_recipients=string_list(payload.get("cc_recipients")),
        subject=str(payload.get("subject", "") or ""),
        norm_subject=str(payload.get("norm_subject", "") or ""),
        body_sha1=str(payload.get("body_sha1", "") or ""),
        custodian_id=str(payload.get("custodian_id", "") or ""),
        message_id=str(payload.get("message_id", "") or ""),
        folder=str(payload.get("folder", "") or ""),
        source=str(payload.get("source", "") or ""),
    )


def build_thread_summaries(events: Sequence[WhatIfEvent]) -> list[WhatIfThreadSummary]:
    buckets: dict[str, dict[str, Any]] = {}
    for event in events:
        bucket = buckets.setdefault(
            event.thread_id,
            {
                "thread_id": event.thread_id,
                "subject": event.subject or event.thread_id,
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
        if has_external_recipients(event.flags.to_recipients):
            bucket["external_recipient_event_count"] += 1
        bucket["event_type_counts"][event.event_type] += 1

    threads = [
        WhatIfThreadSummary(
            thread_id=payload["thread_id"],
            subject=payload["subject"],
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


def build_actor_profiles(events: Sequence[WhatIfEvent]) -> list[WhatIfActorProfile]:
    buckets: dict[str, dict[str, Any]] = {}
    for event in events:
        touch_actor(
            buckets,
            actor_id=event.actor_id,
            sent=True,
            flagged=event_is_flagged(event),
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


def event_reference(event: WhatIfEvent) -> WhatIfEventReference:
    return WhatIfEventReference(
        event_id=event.event_id,
        timestamp=event.timestamp,
        actor_id=event.actor_id,
        target_id=event.target_id,
        event_type=event.event_type,
        thread_id=event.thread_id,
        subject=event.subject,
        snippet=event.snippet,
        to_recipients=list(event.flags.to_recipients),
        cc_recipients=list(event.flags.cc_recipients),
        has_attachment_reference=event.flags.has_attachment_reference,
        is_forward=event.flags.is_forward,
        is_reply=event.flags.is_reply,
        is_escalation=event.flags.is_escalation,
    )


def event_reason_labels(event: WhatIfEvent) -> list[str]:
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
    if has_external_recipients(event.flags.to_recipients):
        labels.append("external_recipient")
    return labels


def event_is_flagged(event: WhatIfEvent) -> bool:
    return bool(event_reason_labels(event))


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
        reason_labels = event_reason_labels(event)
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

    hydrated_events = hydrate_event_snippets(
        rosetta_dir=world.rosetta_dir,
        events=[event for event, *_ in raw_matches],
    )
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


def touches_executive(event: WhatIfEvent) -> bool:
    haystack = " ".join(
        [
            event.actor_id.lower(),
            event.target_id.lower(),
            " ".join(value.lower() for value in event.flags.to_recipients),
            " ".join(value.lower() for value in event.flags.cc_recipients),
        ]
    )
    return any(marker in haystack for marker in EXECUTIVE_MARKERS)


def has_external_recipients(recipients: Sequence[str]) -> bool:
    for recipient in recipients:
        if "@" not in recipient:
            continue
        if not recipient.lower().endswith(f"@{ENRON_DOMAIN}"):
            return True
    return False


def matches_custodian_filter(
    event: WhatIfEvent,
    tokens: set[str],
) -> bool:
    if event.flags.custodian_id.lower() in tokens:
        return True
    return event.actor_id.lower() in tokens or event.target_id.lower() in tokens


def resolve_time_window(
    time_window: tuple[str, str] | None,
) -> tuple[int, int] | None:
    if time_window is None:
        return None
    start_raw, end_raw = time_window
    return (parse_time_value(start_raw), parse_time_value(end_raw))


def parse_time_value(value: str) -> int:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    return int(parsed.timestamp() * 1000)


def timestamp_to_ms(value: Any) -> int:
    if value is None:
        return 0
    if isinstance(value, datetime):
        return int(value.timestamp() * 1000)
    if isinstance(value, (int, float)):
        return int(value)
    text = str(value)
    if not text:
        return 0
    parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    return int(parsed.timestamp() * 1000)


def timestamp_to_text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
    text = str(value)
    if not text:
        return ""
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return text
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def display_name(actor_id: str) -> str:
    token = actor_id.split("@", 1)[0].replace(".", " ").replace("_", " ").strip()
    if not token:
        return actor_id
    return " ".join(part.capitalize() for part in token.split())


def safe_int(value: Any) -> int:
    try:
        return int(value)
    except Exception:
        return 0


def string_list(value: Any) -> list[str]:
    if isinstance(value, list):
        return [str(item) for item in value if str(item)]
    if value in (None, ""):
        return []
    return [str(value)]
