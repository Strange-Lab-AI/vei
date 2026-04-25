from __future__ import annotations

import logging
from collections import Counter
from pathlib import Path
from typing import Any, Sequence

from vei.context.api import (
    ContextSnapshot,
    load_canonical_history_bundle,
    resolve_world_public_context,
)

from ..cases import assign_case_ids, build_case_summaries
from ..models import (
    WhatIfArtifactFlags,
    WhatIfEvent,
    WhatIfScenario,
    WhatIfWorld,
    WhatIfWorldSummary,
)
from ..situations import build_situation_graph
from ._aggregation import build_actor_profiles, build_thread_summaries
from ._mail_archive import (
    MAIL_SOURCE_PROVIDERS,
    _mail_archive_source_payload_or_empty,
    load_history_snapshot,
)
from ._time import display_name, resolve_time_window, timestamp_to_text
from ._util import (
    _channel_event_type,
    _channel_message_timestamp_ms,
    _channel_subject,
    _company_history_event_id,
    _company_history_thread_id,
    _contains_keyword,
    _jira_issue_event_type,
    _jira_issue_snippet,
    _normalized_actor_id,
    _organization_domain_from_snapshot,
    _organization_name_from_domain,
    _override_actor_profiles,
    _timestamp_text_from_ms,
    _truncate_snippet,
    _history_timestamp_ms,
)

logger = logging.getLogger(__name__)

COMPANY_HISTORY_CONTENT_NOTICE = (
    "Historical excerpts come from the normalized company history bundle. "
    "They reflect the available source text for each recorded surface."
)
CHAT_SOURCE_PROVIDERS = {"slack", "teams"}
WORK_SOURCE_PROVIDERS = {"jira", "linear", "github", "gitlab", "clickup"}
DOC_SOURCE_PROVIDERS = {"google", "notion", "granola"}
CRM_SOURCE_PROVIDERS = {"crm", "salesforce"}
STATE_CONTEXT_PROVIDERS = {"google", "crm", "salesforce"}
EVENT_HISTORY_PROVIDERS = (
    MAIL_SOURCE_PROVIDERS
    | CHAT_SOURCE_PROVIDERS
    | WORK_SOURCE_PROVIDERS
    | DOC_SOURCE_PROVIDERS
    | CRM_SOURCE_PROVIDERS
)
SUPPORTED_HISTORY_PROVIDERS = EVENT_HISTORY_PROVIDERS | STATE_CONTEXT_PROVIDERS


def load_company_history_world(
    *,
    source_dir: str | Path,
    scenarios: Sequence[WhatIfScenario] | None = None,
    time_window: tuple[str, str] | None = None,
    max_events: int | None = None,
    include_content: bool = False,
    include_situation_graph: bool = True,
) -> WhatIfWorld:
    resolved_source_dir = Path(source_dir).expanduser().resolve()
    canonical_world = load_company_history_world_from_canonical(
        source_dir=resolved_source_dir,
        scenarios=scenarios,
        time_window=time_window,
        max_events=max_events,
        include_content=include_content,
        include_situation_graph=include_situation_graph,
    )
    if canonical_world is not None:
        return canonical_world
    snapshot = load_history_snapshot(resolved_source_dir)
    provider_names = _supported_history_provider_names(snapshot)
    event_provider_names = _event_history_provider_names(snapshot)
    if not provider_names or not event_provider_names:
        raise ValueError(
            "company history snapshot does not contain any supported sources"
        )

    organization_name, organization_domain = _history_snapshot_identity(snapshot)
    time_bounds = resolve_time_window(time_window)
    events = _build_company_history_events(
        snapshot=snapshot,
        organization_domain=organization_domain,
        include_content=include_content,
    )
    if time_bounds is not None:
        events = [
            event
            for event in events
            if time_bounds[0] <= event.timestamp_ms <= time_bounds[1]
        ]
    events.sort(key=lambda item: (item.timestamp_ms, item.event_id))
    events = assign_case_ids(events)
    if max_events is not None:
        events = events[: max(0, int(max_events))]

    actors = _override_actor_profiles(
        build_actor_profiles(events, organization_domain=organization_domain),
        actor_payload=_history_actor_payload(
            snapshot,
            organization_domain=organization_domain,
        ),
    )
    threads = build_thread_summaries(
        events,
        organization_domain=organization_domain,
    )
    cases = build_case_summaries(events)
    situation_graph = (
        build_situation_graph(
            threads=threads,
            cases=cases,
            events=events,
        )
        if include_situation_graph
        else None
    )
    summary = WhatIfWorldSummary(
        source="company_history",
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
        source="company_history",
        source_dir=resolved_source_dir,
        organization_name=summary.organization_name,
        organization_domain=summary.organization_domain,
        window_start=summary.first_timestamp,
        window_end=summary.last_timestamp,
        metadata=snapshot.metadata,
    )
    return WhatIfWorld(
        source="company_history",
        source_dir=resolved_source_dir,
        summary=summary,
        scenarios=list(scenarios or []),
        actors=actors,
        threads=threads,
        cases=cases,
        events=events,
        situation_graph=situation_graph,
        metadata={
            "content_notice": COMPANY_HISTORY_CONTENT_NOTICE,
            "source_providers": ",".join(provider_names),
        },
        public_context=public_context,
    )


def load_company_history_world_from_canonical(
    *,
    source_dir: str | Path,
    scenarios: Sequence[WhatIfScenario] | None = None,
    time_window: tuple[str, str] | None = None,
    max_events: int | None = None,
    include_content: bool = False,
    include_situation_graph: bool = True,
) -> WhatIfWorld | None:
    resolved_source_dir = Path(source_dir).expanduser().resolve()
    bundle = load_canonical_history_bundle(resolved_source_dir)
    if bundle is None:
        return None

    snapshot = _load_history_snapshot_if_present(resolved_source_dir)
    time_bounds = resolve_time_window(time_window)
    events = [
        _canonical_row_to_whatif_event(row, include_content=include_content)
        for row in bundle.index.rows
    ]
    if time_bounds is not None:
        events = [
            event
            for event in events
            if time_bounds[0] <= event.timestamp_ms <= time_bounds[1]
        ]
    events.sort(key=lambda item: (item.timestamp_ms, item.event_id))
    if max_events is not None:
        events = events[: max(0, int(max_events))]

    organization_name = bundle.index.organization_name or "Historical Archive"
    organization_domain = bundle.index.organization_domain or "archive.local"
    actors = build_actor_profiles(events, organization_domain=organization_domain)
    if snapshot is not None:
        actors = _override_actor_profiles(
            actors,
            actor_payload=_history_actor_payload(
                snapshot,
                organization_domain=organization_domain,
            ),
        )
    threads = build_thread_summaries(
        events,
        organization_domain=organization_domain,
    )
    cases = build_case_summaries(events)
    situation_graph = (
        build_situation_graph(
            threads=threads,
            cases=cases,
            events=events,
        )
        if include_situation_graph
        else None
    )
    summary = WhatIfWorldSummary(
        source="company_history",
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
        source="company_history",
        source_dir=resolved_source_dir,
        organization_name=summary.organization_name,
        organization_domain=summary.organization_domain,
        window_start=summary.first_timestamp,
        window_end=summary.last_timestamp,
        metadata=snapshot.metadata if snapshot is not None else {},
    )
    return WhatIfWorld(
        source="company_history",
        source_dir=resolved_source_dir,
        summary=summary,
        scenarios=list(scenarios or []),
        actors=actors,
        threads=threads,
        cases=cases,
        events=events,
        situation_graph=situation_graph,
        metadata={
            "content_notice": COMPANY_HISTORY_CONTENT_NOTICE,
            "source_providers": ",".join(bundle.index.source_providers),
            "timeline_source": "canonical_history_sidecar",
        },
        public_context=public_context,
    )


def _build_company_history_events(
    *,
    snapshot: ContextSnapshot,
    organization_domain: str,
    include_content: bool,
) -> list[WhatIfEvent]:
    from ..adapters import build_company_history_events

    return build_company_history_events(
        snapshot=snapshot,
        organization_domain=organization_domain,
        include_content=include_content,
    )


def _load_history_snapshot_if_present(path: Path) -> ContextSnapshot | None:
    try:
        return load_history_snapshot(path)
    except Exception:  # noqa: BLE001
        return None


def _canonical_row_to_whatif_event(
    row,
    *,
    include_content: bool,
) -> WhatIfEvent:
    snippet = row.snippet if include_content else _truncate_snippet(row.snippet)
    surface = row.surface
    if surface == "crm" and row.kind == "deal_change":
        event_type = "assignment" if row.metadata.get("field") == "owner" else "deal"
    elif surface == "crm":
        event_type = "deal"
    elif row.kind in {"comment", "reply"}:
        event_type = "reply"
    elif row.kind in {"merge_request", "issue", "task"}:
        event_type = "assignment"
    else:
        event_type = row.kind
    return WhatIfEvent(
        event_id=row.event_id,
        timestamp=row.timestamp,
        timestamp_ms=row.ts_ms,
        actor_id=row.actor_id or f"{row.provider}-actor",
        target_id=row.target_id,
        event_type=event_type,
        thread_id=row.thread_ref or f"{surface}:{row.event_id}",
        case_id=row.case_id,
        surface=surface,
        conversation_anchor=row.conversation_anchor,
        subject=row.subject or row.thread_ref,
        snippet=snippet,
        flags=WhatIfArtifactFlags(
            is_reply=event_type == "reply",
            to_count=len(row.participant_ids),
            to_recipients=list(row.participant_ids),
            subject=row.subject or row.thread_ref,
            norm_subject=row.normalized_subject,
            source=row.provider,
        ),
    )


def _company_history_chat_events(
    *,
    snapshot: ContextSnapshot,
    provider: str,
    organization_domain: str,
    include_content: bool,
) -> list[WhatIfEvent]:
    source = snapshot.source_for(provider)
    if source is None:
        return []
    payload = source.typed_data()
    channels = payload.get("channels", [])
    if not isinstance(channels, list):
        return []
    user_lookup = _history_chat_user_lookup(payload)

    events: list[WhatIfEvent] = []
    for channel_index, channel in enumerate(channels):
        if not isinstance(channel, dict):
            continue
        channel_name = str(
            channel.get(
                "channel", channel.get("channel_id", f"channel-{channel_index + 1}")
            )
        ).strip()
        if not channel_name:
            continue
        messages = [
            item for item in (channel.get("messages") or []) if isinstance(item, dict)
        ]
        ordered_messages = sorted(
            messages,
            key=lambda item: _channel_message_timestamp_ms(
                item.get("ts"),
                fallback_index=channel_index + len(events),
            ),
        )
        for message_index, message in enumerate(ordered_messages):
            ts_value = str(message.get("ts", "") or "").strip()
            raw_anchor = str(message.get("thread_ts", ts_value) or ts_value).strip()
            conversation_anchor = str(
                _channel_message_timestamp_ms(
                    raw_anchor,
                    fallback_index=(channel_index + 1) * 1000 + message_index,
                )
            )
            thread_id = _company_history_thread_id(
                provider,
                f"{channel_name}:{conversation_anchor}",
            )
            body_text = str(message.get("text", "") or "").strip()
            actor_id = _normalized_actor_id(
                _resolved_chat_actor_value(
                    message.get("user"),
                    user_lookup=user_lookup,
                ),
                organization_domain=organization_domain,
                fallback=f"{provider}-user-{message_index + 1}",
            )
            timestamp_ms = _channel_message_timestamp_ms(
                message.get("ts"),
                fallback_index=(channel_index + 1) * 1000 + message_index,
            )
            timestamp_text = _timestamp_text_from_ms(timestamp_ms)
            is_reply = conversation_anchor != str(
                _channel_message_timestamp_ms(
                    ts_value,
                    fallback_index=(channel_index + 1) * 1000 + message_index,
                )
            )
            subject = _channel_subject(
                channel_name=channel_name,
                conversation_anchor=conversation_anchor,
                messages=ordered_messages,
            )
            snippet = body_text if include_content else _truncate_snippet(body_text)
            event_type = _channel_event_type(
                body_text=body_text,
                is_reply=is_reply,
            )
            flags = WhatIfArtifactFlags(
                consult_legal_specialist=_contains_keyword(
                    " ".join([channel_name, body_text]),
                    ("legal", "counsel", "compliance", "regulatory"),
                ),
                consult_trading_specialist=_contains_keyword(
                    " ".join([channel_name, body_text]),
                    ("trading", "trade", "desk", "market"),
                ),
                has_attachment_reference=_contains_keyword(
                    body_text,
                    ("attach", "attachment", "draft", ".pdf", ".doc"),
                ),
                is_escalation=_contains_keyword(
                    body_text,
                    ("escalate", "urgent", "leadership", "executive"),
                ),
                is_reply=is_reply,
                to_count=1,
                to_recipients=[channel_name],
                subject=subject,
                norm_subject=subject.lower().strip(),
                message_id=str(message.get("id", "") or ""),
                source=provider,
            )
            events.append(
                WhatIfEvent(
                    event_id=_company_history_event_id(
                        provider=provider,
                        raw_event_id=ts_value or "",
                        fallback_parts=(
                            channel_name,
                            conversation_anchor,
                            str(message_index + 1),
                        ),
                    ),
                    timestamp=timestamp_text,
                    timestamp_ms=timestamp_ms,
                    actor_id=actor_id,
                    event_type=event_type,
                    thread_id=thread_id,
                    surface="slack",
                    conversation_anchor=conversation_anchor,
                    subject=subject,
                    snippet=snippet,
                    flags=flags,
                )
            )
    return events


def _history_chat_user_lookup(payload: dict[str, Any]) -> dict[str, str]:
    lookup: dict[str, str] = {}
    users = payload.get("users", [])
    if not isinstance(users, list):
        return lookup
    for user in users:
        if not isinstance(user, dict):
            continue
        canonical = str(user.get("email", "") or user.get("name", "") or "").strip()
        if not canonical:
            continue
        for key in (
            user.get("id"),
            user.get("name"),
            user.get("real_name"),
            user.get("email"),
        ):
            text = str(key or "").strip()
            if text:
                lookup[text.lower()] = canonical
    return lookup


def _resolved_chat_actor_value(
    value: Any,
    *,
    user_lookup: dict[str, str],
) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    return user_lookup.get(text.lower(), text)


def _company_history_jira_events(
    *,
    snapshot: ContextSnapshot,
    organization_domain: str,
    include_content: bool,
) -> list[WhatIfEvent]:
    source = snapshot.source_for("jira")
    if source is None:
        return []
    issues = source.typed_data().get("issues", [])
    if not isinstance(issues, list):
        return []

    events: list[WhatIfEvent] = []
    for issue_index, issue in enumerate(issues):
        if not isinstance(issue, dict):
            continue
        ticket_id = str(issue.get("ticket_id", "") or "").strip()
        if not ticket_id:
            continue
        thread_id = _company_history_thread_id("jira", ticket_id)
        title = str(issue.get("title", ticket_id) or ticket_id).strip()
        assignee = _normalized_actor_id(
            issue.get("assignee"),
            organization_domain=organization_domain,
            fallback=f"jira-assignee-{issue_index + 1}",
        )
        updated_raw = issue.get("updated") or ""
        if assignee:
            events.append(
                WhatIfEvent(
                    event_id=_company_history_event_id(
                        provider="jira",
                        raw_event_id=f"{ticket_id}:state",
                        fallback_parts=(ticket_id, "state"),
                    ),
                    timestamp=timestamp_to_text(updated_raw),
                    timestamp_ms=_history_timestamp_ms(
                        updated_raw,
                        fallback_index=(issue_index + 1) * 1000,
                    ),
                    actor_id=assignee,
                    event_type=_jira_issue_event_type(issue),
                    thread_id=thread_id,
                    surface="tickets",
                    subject=title,
                    snippet=_jira_issue_snippet(issue, include_content=include_content),
                    flags=WhatIfArtifactFlags(
                        consult_legal_specialist=_contains_keyword(
                            " ".join([title, str(issue.get("description", "") or "")]),
                            ("legal", "counsel", "compliance", "contract"),
                        ),
                        is_escalation=_contains_keyword(
                            " ".join([title, str(issue.get("status", "") or "")]),
                            ("blocked", "urgent", "critical", "escalat"),
                        ),
                        to_count=1,
                        to_recipients=[ticket_id],
                        subject=title,
                        norm_subject=title.lower().strip(),
                        source="jira",
                    ),
                )
            )
        comments = issue.get("comments", [])
        if not isinstance(comments, list):
            continue
        for comment_index, comment in enumerate(comments):
            if not isinstance(comment, dict):
                continue
            body_text = str(comment.get("body", "") or "").strip()
            author = _normalized_actor_id(
                comment.get("author"),
                organization_domain=organization_domain,
                fallback=assignee or f"jira-commenter-{comment_index + 1}",
            )
            timestamp_raw = comment.get("created") or updated_raw
            events.append(
                WhatIfEvent(
                    event_id=_company_history_event_id(
                        provider="jira",
                        raw_event_id=str(comment.get("id", "") or ""),
                        fallback_parts=(ticket_id, "comment", str(comment_index + 1)),
                    ),
                    timestamp=timestamp_to_text(timestamp_raw),
                    timestamp_ms=_history_timestamp_ms(
                        timestamp_raw,
                        fallback_index=(issue_index + 1) * 1000 + comment_index + 1,
                    ),
                    actor_id=author,
                    event_type="reply",
                    thread_id=thread_id,
                    surface="tickets",
                    subject=title,
                    snippet=(
                        body_text if include_content else _truncate_snippet(body_text)
                    ),
                    flags=WhatIfArtifactFlags(
                        consult_legal_specialist=_contains_keyword(
                            " ".join([title, body_text]),
                            ("legal", "counsel", "compliance", "contract"),
                        ),
                        is_escalation=_contains_keyword(
                            body_text,
                            ("blocked", "urgent", "critical", "escalat"),
                        ),
                        is_reply=True,
                        to_count=1,
                        to_recipients=[ticket_id],
                        subject=title,
                        norm_subject=title.lower().strip(),
                        source="jira",
                    ),
                )
            )
    return events


def _history_actor_payload(
    snapshot: ContextSnapshot,
    *,
    organization_domain: str,
) -> list[dict[str, Any]]:
    actor_payload: dict[str, dict[str, str]] = {}
    for actor in _mail_archive_source_payload_or_empty(snapshot).get("actors", []):
        if not isinstance(actor, dict):
            continue
        actor_id = str(actor.get("actor_id", actor.get("email", "")) or "").strip()
        if not actor_id:
            continue
        actor_payload[actor_id] = {
            "actor_id": actor_id,
            "email": str(actor.get("email", actor_id) or actor_id).strip(),
            "display_name": str(actor.get("display_name", "") or "").strip(),
        }
    for provider in ("slack", "teams"):
        source = snapshot.source_for(provider)
        if source is None:
            continue
        users = source.typed_data().get("users", [])
        if not isinstance(users, list):
            continue
        for user in users:
            if not isinstance(user, dict):
                continue
            actor_id = _normalized_actor_id(
                user.get("email") or user.get("name") or user.get("real_name"),
                organization_domain=organization_domain,
                fallback="",
            )
            if not actor_id:
                continue
            actor_payload.setdefault(
                actor_id,
                {
                    "actor_id": actor_id,
                    "email": str(user.get("email", actor_id) or actor_id).strip(),
                    "display_name": str(
                        user.get("real_name", user.get("name", actor_id)) or actor_id
                    ).strip(),
                },
            )
    google_source = snapshot.source_for("google")
    if google_source is not None:
        users = google_source.typed_data().get("users", [])
        if isinstance(users, list):
            for user in users:
                if not isinstance(user, dict):
                    continue
                actor_id = _normalized_actor_id(
                    user.get("email") or user.get("name"),
                    organization_domain=organization_domain,
                    fallback="",
                )
                if not actor_id:
                    continue
                actor_payload.setdefault(
                    actor_id,
                    {
                        "actor_id": actor_id,
                        "email": str(user.get("email", actor_id) or actor_id).strip(),
                        "display_name": str(
                            user.get("name", user.get("email", actor_id)) or actor_id
                        ).strip(),
                    },
                )
    for provider in ("crm", "salesforce"):
        source = snapshot.source_for(provider)
        if source is None:
            continue
        source_data = source.typed_data()
        contacts = source_data.get("contacts", [])
        if isinstance(contacts, list):
            for contact in contacts:
                if not isinstance(contact, dict):
                    continue
                email = str(contact.get("email") or "").strip()
                actor_id = _normalized_actor_id(
                    email
                    or " ".join(
                        part
                        for part in (
                            str(contact.get("first_name") or "").strip(),
                            str(contact.get("last_name") or "").strip(),
                        )
                        if part
                    ),
                    organization_domain=organization_domain,
                    fallback="",
                )
                if not actor_id:
                    continue
                contact_display_name = " ".join(
                    part
                    for part in (
                        str(contact.get("first_name") or "").strip(),
                        str(contact.get("last_name") or "").strip(),
                    )
                    if part
                ).strip()
                actor_payload.setdefault(
                    actor_id,
                    {
                        "actor_id": actor_id,
                        "email": email or actor_id,
                        "display_name": contact_display_name or email or actor_id,
                    },
                )
        deals = source_data.get("deals", [])
        if not isinstance(deals, list):
            continue
        for deal in deals:
            if not isinstance(deal, dict):
                continue
            actor_id = _normalized_actor_id(
                deal.get("owner"),
                organization_domain=organization_domain,
                fallback="",
            )
            if not actor_id:
                continue
            actor_payload.setdefault(
                actor_id,
                {
                    "actor_id": actor_id,
                    "email": actor_id,
                    "display_name": display_name(actor_id),
                },
            )
    return list(actor_payload.values())


def _supported_history_provider_names(snapshot: ContextSnapshot) -> set[str]:
    return {
        str(source.provider).strip().lower()
        for source in snapshot.sources
        if source.status != "error"
        and str(source.provider).strip().lower() in SUPPORTED_HISTORY_PROVIDERS
    }


def _event_history_provider_names(snapshot: ContextSnapshot) -> set[str]:
    return {
        str(source.provider).strip().lower()
        for source in snapshot.sources
        if source.status != "error"
        and str(source.provider).strip().lower() in EVENT_HISTORY_PROVIDERS
    }


def _history_snapshot_identity(snapshot: ContextSnapshot) -> tuple[str, str]:
    organization_domain = str(snapshot.organization_domain or "").strip().lower()
    if not organization_domain:
        organization_domain = _organization_domain_from_snapshot(snapshot)
    organization_name = str(snapshot.organization_name or "").strip()
    if not organization_name:
        organization_name = _organization_name_from_domain(organization_domain)
    return (
        organization_name or "Historical Archive",
        organization_domain or "archive.local",
    )
