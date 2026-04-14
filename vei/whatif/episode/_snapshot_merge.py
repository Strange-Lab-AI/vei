from __future__ import annotations

from typing import Any, Sequence


def _merge_context_source_result(existing, extra):
    from vei.context.api import ContextSourceResult

    merged_data = _merge_context_source_data(
        provider=existing.provider,
        existing=existing.data,
        extra=extra.data,
    )
    return ContextSourceResult.model_validate(
        {
            **existing.model_dump(mode="python"),
            "status": _merge_context_source_status(existing.status, extra.status),
            "record_counts": _context_source_record_counts(
                existing.provider,
                merged_data,
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
    existing: Any,
    extra: Any,
) -> dict[str, Any]:
    existing_mapping = _context_source_mapping(existing)
    extra_mapping = _context_source_mapping(extra)
    if provider in {"mail_archive", "gmail"}:
        return {
            "threads": _merge_mail_threads(
                existing_mapping.get("threads", []),
                extra_mapping.get("threads", []),
            ),
            "actors": _merge_keyed_dict_items(
                existing_mapping.get("actors", []),
                extra_mapping.get("actors", []),
                key_names=("actor_id", "email"),
            ),
            "profile": _merge_mapping(
                existing_mapping.get("profile"),
                extra_mapping.get("profile"),
            ),
        }
    if provider in {"slack", "teams"}:
        return {
            "channels": _merge_chat_channels(
                existing_mapping.get("channels", []),
                extra_mapping.get("channels", []),
            ),
            "users": _merge_keyed_dict_items(
                existing_mapping.get("users", []),
                extra_mapping.get("users", []),
                key_names=("id", "email", "name"),
            ),
            "profile": _merge_mapping(
                existing_mapping.get("profile"),
                extra_mapping.get("profile"),
            ),
        }
    if provider == "jira":
        return {
            "issues": _merge_jira_issues(
                existing_mapping.get("issues", []),
                extra_mapping.get("issues", []),
            ),
            "projects": _merge_keyed_dict_items(
                existing_mapping.get("projects", []),
                extra_mapping.get("projects", []),
                key_names=("key", "id", "name"),
            ),
        }
    if provider == "google":
        return {
            "documents": _merge_keyed_dict_items(
                existing_mapping.get("documents", []),
                extra_mapping.get("documents", []),
                key_names=("doc_id", "id", "title"),
            ),
        }
    if provider in {"crm", "salesforce"}:
        return {
            "deals": _merge_keyed_dict_items(
                existing_mapping.get("deals", []),
                extra_mapping.get("deals", []),
                key_names=("id", "deal_id", "name"),
            ),
        }
    merged = dict(existing_mapping)
    merged.update(extra_mapping)
    return merged


def _context_source_mapping(value: Any) -> dict[str, Any]:
    if hasattr(value, "model_dump"):
        return value.model_dump(mode="python")
    if isinstance(value, dict):
        return value
    return {}


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


__all__ = [
    "_context_source_record_counts",
    "_merge_context_source_result",
]
