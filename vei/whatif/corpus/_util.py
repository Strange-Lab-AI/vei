from __future__ import annotations

import logging
from collections import Counter
from datetime import datetime, timezone
from typing import Any, Sequence

from vei.context.models import ContextSnapshot

from ..models import WhatIfActorProfile
from ._time import (
    timestamp_to_ms,
    timestamp_to_text,
)

logger = logging.getLogger(__name__)


def _contains_keyword(text: str, keywords: Sequence[str]) -> bool:
    lowered = text.lower()
    return any(keyword in lowered for keyword in keywords)


def _truncate_snippet(value: str, *, max_chars: int = 280) -> str:
    cleaned = " ".join(value.split()).strip()
    if len(cleaned) <= max_chars:
        return cleaned
    return cleaned[: max_chars - 1].rstrip() + "\u2026"


def _organization_domain_from_threads(threads: Sequence[dict[str, Any]]) -> str:
    sender_counts: Counter[str] = Counter()
    participant_counts: Counter[str] = Counter()
    for thread in threads:
        if not isinstance(thread, dict):
            continue
        for message in thread.get("messages", []):
            if not isinstance(message, dict):
                continue
            sender_domain = _email_domain(str(message.get("from", "") or ""))
            if sender_domain:
                sender_counts[sender_domain] += 1
                participant_counts[sender_domain] += 1
            for key in ("to", "cc"):
                for recipient in _recipient_list(message.get(key)):
                    domain = _email_domain(recipient)
                    if domain:
                        participant_counts[domain] += 1
    if sender_counts:
        return sorted(
            sender_counts.items(),
            key=lambda item: (-item[1], item[0]),
        )[
            0
        ][0]
    if participant_counts:
        return sorted(
            participant_counts.items(),
            key=lambda item: (-item[1], item[0]),
        )[0][0]
    return ""


def _organization_domain_from_snapshot(snapshot: ContextSnapshot) -> str:
    from ._mail_archive import _mail_archive_source_payload_or_empty

    source_payload = _mail_archive_source_payload_or_empty(snapshot)
    mail_threads = source_payload.get("threads", [])
    if not isinstance(mail_threads, list):
        mail_threads = []
    domain = _organization_domain_from_threads(mail_threads)
    if domain:
        return domain

    domains: Counter[str] = Counter()
    for provider in ("slack", "teams"):
        source = snapshot.source_for(provider)
        if source is None or not isinstance(source.data, dict):
            continue
        users = source.data.get("users", [])
        if not isinstance(users, list):
            continue
        for user in users:
            if not isinstance(user, dict):
                continue
            email_domain = _email_domain(str(user.get("email", "") or ""))
            if email_domain:
                domains[email_domain] += 1
    jira_source = snapshot.source_for("jira")
    if jira_source is not None and isinstance(jira_source.data, dict):
        issues = jira_source.data.get("issues", [])
        if isinstance(issues, list):
            for issue in issues:
                if not isinstance(issue, dict):
                    continue
                assignee_domain = _email_domain(str(issue.get("assignee", "") or ""))
                if assignee_domain:
                    domains[assignee_domain] += 1
                comments = issue.get("comments", [])
                if not isinstance(comments, list):
                    continue
                for comment in comments:
                    if not isinstance(comment, dict):
                        continue
                    author_domain = _email_domain(str(comment.get("author", "") or ""))
                    if author_domain:
                        domains[author_domain] += 1
    if not domains:
        return ""
    return sorted(domains.items(), key=lambda item: (-item[1], item[0]))[0][0]


def _organization_name_from_domain(domain: str) -> str:
    cleaned = str(domain or "").strip().lower()
    if not cleaned:
        return "Historical Archive"
    token = cleaned.split(".", 1)[0].replace("-", " ").replace("_", " ")
    label = " ".join(part.capitalize() for part in token.split() if part)
    return label or "Historical Archive"


def _company_history_thread_id(provider: str, raw_id: str) -> str:
    normalized_provider = provider.strip().lower() or "history"
    normalized_raw_id = str(raw_id or "").strip() or "thread"
    return f"{normalized_provider}:{normalized_raw_id}"


def _company_history_event_id(
    *,
    provider: str,
    raw_event_id: str,
    fallback_parts: Sequence[str],
) -> str:
    normalized_provider = provider.strip().lower() or "history"
    normalized_raw_event_id = str(raw_event_id or "").strip()
    if normalized_raw_event_id:
        return f"{normalized_provider}:{normalized_raw_event_id}"
    joined = ":".join(str(part).strip() for part in fallback_parts if str(part).strip())
    return f"{normalized_provider}:{joined or 'event'}"


def _normalized_actor_id(
    value: Any,
    *,
    organization_domain: str,
    fallback: str,
) -> str:
    text = str(value or "").strip()
    if not text:
        return _fallback_internal_address(organization_domain, fallback or "unknown")
    if "@" in text:
        return text.lower()
    cleaned = text.replace(" ", ".").strip(".").lower()
    if organization_domain:
        return f"{cleaned}@{organization_domain}"
    return cleaned


def _history_timestamp_ms(value: Any, *, fallback_index: int) -> int:
    if value in {None, ""}:
        return fallback_index * 1000
    try:
        return timestamp_to_ms(value)
    except (ValueError, TypeError):
        pass
    text = str(value).strip()
    try:
        return int(float(text) * 1000)
    except (ValueError, TypeError):
        return fallback_index * 1000


def _channel_message_timestamp_ms(value: Any, *, fallback_index: int) -> int:
    return _history_timestamp_ms(value, fallback_index=fallback_index)


def _timestamp_text_from_ms(value: int) -> str:
    return timestamp_to_text(datetime.fromtimestamp(value / 1000, tz=timezone.utc))


def _channel_subject(
    *,
    channel_name: str,
    conversation_anchor: str,
    messages: Sequence[dict[str, Any]],
) -> str:
    for message in messages:
        if not isinstance(message, dict):
            continue
        message_anchor = str(
            _channel_message_timestamp_ms(
                message.get("thread_ts", message.get("ts", "")),
                fallback_index=0,
            )
        )
        if message_anchor != conversation_anchor:
            continue
        root_text = str(message.get("text", "") or "").strip()
        if root_text:
            return _truncate_snippet(root_text, max_chars=80)
    return channel_name


def _channel_event_type(*, body_text: str, is_reply: bool) -> str:
    if _contains_keyword(
        body_text, ("approve", "approved", "ship it", ":white_check_mark:")
    ):
        return "approval"
    if _contains_keyword(body_text, ("assign", "owner", "handoff")):
        return "assignment"
    if _contains_keyword(body_text, ("escalate", "urgent", "leadership", "executive")):
        return "escalation"
    if is_reply:
        return "reply"
    return "message"


def _jira_issue_event_type(issue: dict[str, Any]) -> str:
    status = str(issue.get("status", "") or "").strip().lower()
    title = str(issue.get("title", "") or "").strip().lower()
    if "approv" in status or "approv" in title:
        return "approval"
    if "block" in status or "urgent" in title:
        return "escalation"
    if issue.get("assignee"):
        return "assignment"
    return "message"


def _jira_issue_snippet(
    issue: dict[str, Any],
    *,
    include_content: bool,
) -> str:
    description = str(issue.get("description", "") or "").strip()
    status = str(issue.get("status", "") or "").strip()
    assignee = str(issue.get("assignee", "") or "").strip()
    parts = [
        part
        for part in (
            description,
            f"Status: {status}" if status else "",
            f"Assignee: {assignee}" if assignee else "",
        )
        if part
    ]
    text = "\n".join(parts).strip()
    if include_content:
        return text
    return _truncate_snippet(text)


def _fallback_internal_address(domain: str, local_part: str) -> str:
    normalized_domain = str(domain or "").strip().lower()
    if not normalized_domain:
        return f"{local_part}@archive.local"
    return f"{local_part}@{normalized_domain}"


def _email_domain(value: str) -> str:
    email = str(value or "").strip().lower()
    if "@" not in email:
        return ""
    return email.split("@", 1)[1]


def _override_actor_profiles(
    actors: Sequence[WhatIfActorProfile],
    *,
    actor_payload: Sequence[dict[str, Any]],
) -> list[WhatIfActorProfile]:
    directory: dict[str, dict[str, str]] = {}
    for actor in actor_payload:
        if not isinstance(actor, dict):
            continue
        actor_id = str(actor.get("actor_id", actor.get("email", "")) or "").strip()
        if not actor_id:
            continue
        directory[actor_id] = {
            "email": str(actor.get("email", actor_id) or actor_id).strip(),
            "display_name": str(actor.get("display_name", "") or "").strip(),
        }

    updated: list[WhatIfActorProfile] = []
    for actor in actors:
        override = directory.get(actor.actor_id)
        if not override:
            updated.append(actor)
            continue
        updated.append(
            actor.model_copy(
                update={
                    "email": override["email"] or actor.email,
                    "display_name": override["display_name"] or actor.display_name,
                }
            )
        )
    return updated


def _recipient_list(value: Any) -> list[str]:
    if isinstance(value, list):
        return [str(item).strip() for item in value if str(item).strip()]
    text = str(value or "").strip()
    if not text:
        return []
    normalized = text.replace(";", ",")
    return [item.strip() for item in normalized.split(",") if item.strip()]


def _message_flag(
    message: dict[str, Any],
    *,
    key: str,
    subject: str,
    prefixes: Sequence[str],
) -> bool:
    if bool(message.get(key, False)):
        return True
    lowered = subject.lower().strip()
    return any(lowered.startswith(prefix) for prefix in prefixes)


def _has_attachment_reference(message: dict[str, Any], body_text: str) -> bool:
    if bool(message.get("has_attachment_reference", False)):
        return True
    attachments = message.get("attachments")
    if isinstance(attachments, list) and attachments:
        return True
    attachment_names = message.get("attachment_names")
    if isinstance(attachment_names, list) and attachment_names:
        return True
    return _contains_keyword(
        body_text, ("attach", "attachment", "draft", ".pdf", ".doc")
    )


def _archive_event_type(
    *,
    message: dict[str, Any],
    subject: str,
    body_text: str,
    is_forward: bool,
    is_reply: bool,
    is_escalation: bool,
) -> str:
    explicit = str(message.get("event_type", "") or "").strip().lower()
    if explicit:
        return explicit
    if is_escalation:
        return "escalation"
    if _contains_keyword(" ".join([subject, body_text]), ("approval", "approved")):
        return "approval"
    if _contains_keyword(
        " ".join([subject, body_text]), ("assign", "owner", "handoff")
    ):
        return "assignment"
    if is_forward:
        return "forward"
    if is_reply:
        return "reply"
    return "message"
