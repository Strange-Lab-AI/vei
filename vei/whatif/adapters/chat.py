from __future__ import annotations

from typing import Any, Sequence

from vei.context.models import ContextSnapshot

from ..corpus import (
    _channel_message_timestamp_ms,
    _company_history_event_id,
    _company_history_thread_id,
    _contains_keyword,
    _normalized_actor_id,
    _timestamp_text_from_ms,
    _truncate_snippet,
)
from ..models import WhatIfArtifactFlags, WhatIfEvent


def build_chat_events(
    *,
    snapshot: ContextSnapshot,
    provider: str,
    organization_domain: str,
    include_content: bool,
) -> list[WhatIfEvent]:
    source = snapshot.source_for(provider)
    if source is None or not isinstance(source.data, dict):
        return []
    channels = source.data.get("channels", [])
    if not isinstance(channels, list):
        return []
    user_lookup = _chat_user_lookup(source.data)

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
                    surface=provider,
                    conversation_anchor=conversation_anchor,
                    subject=subject,
                    snippet=snippet,
                    flags=flags,
                )
            )
    return events


def _chat_user_lookup(payload: dict[str, Any]) -> dict[str, str]:
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
