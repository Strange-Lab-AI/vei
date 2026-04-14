from __future__ import annotations

import logging
from email.utils import getaddresses, parseaddr
from typing import Any

from vei.context.models import ContextSnapshot, ContextSourceResult, with_snapshot_role

logger = logging.getLogger(__name__)


def cleanup_normalized_snapshot(snapshot: ContextSnapshot) -> ContextSnapshot:
    cleaned_sources: list[ContextSourceResult] = []
    cleanup_summary: dict[str, dict[str, int]] = {}
    for source in snapshot.sources:
        cleaned_source, summary = _cleanup_source_result(source)
        cleaned_sources.append(cleaned_source)
        if summary:
            cleanup_summary[source.provider] = summary

    cleaned_snapshot = with_snapshot_role(
        snapshot.model_copy(update={"sources": cleaned_sources}),
        "company_history_bundle",
    )
    metadata = dict(cleaned_snapshot.metadata)
    if cleanup_summary:
        metadata["normalization_cleanup"] = cleanup_summary
    dedup_map = deduplicate_actors(cleaned_snapshot)
    if dedup_map:
        metadata["actor_dedup_map"] = dedup_map
    return cleaned_snapshot.model_copy(update={"metadata": metadata})


def cleanup_workspace_seed_snapshot(snapshot: ContextSnapshot) -> ContextSnapshot:
    return with_snapshot_role(snapshot, "workspace_seed")


def normalized_email(value: object) -> str:
    _display_name, address = parseaddr(str(value or "").strip())
    candidate = address or str(value or "").strip()
    if "@" not in candidate:
        return ""
    return candidate.strip().lower()


def normalized_name(value: object) -> str:
    text = str(value or "").strip()
    if not text or "@" in text:
        return ""
    return " ".join(text.lower().split())


def address_tokens(value: object) -> list[str]:
    text = str(value or "").replace(";", ",")
    return [item.strip() for item in text.split(",") if item.strip()]


def deduplicate_actors(snapshot: ContextSnapshot) -> dict[str, str]:
    records = _extract_all_actors(snapshot)
    if not records:
        return {}

    all_ids = [record[0] for record in records]
    parent = {actor_id: actor_id for actor_id in all_ids}
    rank = {actor_id: 0 for actor_id in all_ids}

    def find(actor_id: str) -> str:
        while parent[actor_id] != actor_id:
            parent[actor_id] = parent[parent[actor_id]]
            actor_id = parent[actor_id]
        return actor_id

    def union(left: str, right: str) -> None:
        left_root = find(left)
        right_root = find(right)
        if left_root == right_root:
            return
        if rank[left_root] < rank[right_root]:
            left_root, right_root = right_root, left_root
        parent[right_root] = left_root
        if rank[left_root] == rank[right_root]:
            rank[left_root] += 1

    email_to_ids: dict[str, list[str]] = {}
    for actor_id, email, _name in records:
        if email:
            email_to_ids.setdefault(email, []).append(actor_id)
    for ids in email_to_ids.values():
        for index in range(1, len(ids)):
            union(ids[0], ids[index])

    name_to_ids: dict[str, list[str]] = {}
    for actor_id, email, name in records:
        if not email and name:
            name_to_ids.setdefault(name, []).append(actor_id)
    for ids in name_to_ids.values():
        for index in range(1, len(ids)):
            union(ids[0], ids[index])

    groups: dict[str, list[tuple[str, str, str]]] = {}
    for actor_id, email, name in records:
        root = find(actor_id)
        groups.setdefault(root, []).append((actor_id, email, name))

    dedup_map: dict[str, str] = {}
    for group in groups.values():
        if len(group) < 2:
            continue
        email_counts: dict[str, int] = {}
        for _actor_id, email, _name in group:
            if email:
                email_counts[email] = email_counts.get(email, 0) + 1
        if email_counts:
            best_email = max(email_counts, key=lambda email: email_counts[email])
            canonical = next(
                actor_id for actor_id, email, _ in group if email == best_email
            )
        else:
            canonical = group[0][0]
        for actor_id, _email, _name in group:
            if actor_id != canonical:
                dedup_map[actor_id] = canonical

    if dedup_map:
        logger.info(
            "actors_deduplicated",
            extra={
                "dedup_count": len(dedup_map),
                "group_count": sum(1 for group in groups.values() if len(group) >= 2),
            },
        )
    return dedup_map


def _cleanup_source_result(
    source: ContextSourceResult,
) -> tuple[ContextSourceResult, dict[str, int]]:
    summary: dict[str, int] = {}
    if source.provider in {"gmail", "mail_archive"}:
        data = _source_data_mapping(source)
        cleaned_threads, thread_changes = _cleanup_mail_threads(data.get("threads", []))
        cleaned_actors, actor_changes = _cleanup_mail_actors(data.get("actors", []))
        cleaned_data = {
            **data,
            "threads": cleaned_threads,
            "actors": cleaned_actors,
        }
        summary.update(thread_changes)
        summary.update(actor_changes)
        return (
            ContextSourceResult.model_validate(
                {
                    **source.model_dump(mode="python"),
                    "data": cleaned_data,
                }
            ),
            _nonzero_summary(summary),
        )
    return source, summary


def _cleanup_mail_threads(
    threads: Any,
) -> tuple[list[dict[str, Any]], dict[str, int]]:
    cleaned_threads_by_id: dict[str, dict[str, Any]] = {}
    summary = {
        "thread_ids_normalized": 0,
        "message_ids_normalized": 0,
        "address_fields_normalized": 0,
        "duplicate_threads_merged": 0,
    }
    for index, raw_thread in enumerate(threads or []):
        if not isinstance(raw_thread, dict):
            continue
        thread = dict(raw_thread)
        normalized_thread_id = _normalize_thread_like_id(thread.get("thread_id"))
        if not normalized_thread_id:
            normalized_thread_id = f"thread-{index + 1}"
        if normalized_thread_id != str(thread.get("thread_id") or ""):
            summary["thread_ids_normalized"] += 1
        thread["thread_id"] = normalized_thread_id

        cleaned_messages: list[dict[str, Any]] = []
        for raw_message in thread.get("messages", []):
            if not isinstance(raw_message, dict):
                continue
            message = dict(raw_message)
            normalized_message_id = _normalize_thread_like_id(
                message.get("message_id") or message.get("id")
            )
            if normalized_message_id and normalized_message_id != str(
                message.get("message_id") or message.get("id") or ""
            ):
                summary["message_ids_normalized"] += 1
            if "message_id" in message or normalized_message_id:
                message["message_id"] = normalized_message_id
            for key, many in (
                ("from", False),
                ("to", True),
                ("cc", True),
                ("bcc", True),
            ):
                original = message.get(key)
                cleaned = _normalize_mail_address_field(original, many=many)
                if cleaned != str(original or "").strip():
                    if str(original or "").strip() or cleaned:
                        summary["address_fields_normalized"] += 1
                if cleaned:
                    message[key] = cleaned
                elif key in message:
                    message[key] = ""
            cleaned_messages.append(message)

        existing_thread = cleaned_threads_by_id.get(normalized_thread_id)
        if existing_thread is not None:
            summary["duplicate_threads_merged"] += 1
            existing_thread["messages"] = _merge_thread_messages(
                existing_thread.get("messages", []),
                cleaned_messages,
            )
            if not str(existing_thread.get("subject") or "").strip():
                existing_thread["subject"] = str(thread.get("subject") or "").strip()
            continue
        thread["messages"] = cleaned_messages
        cleaned_threads_by_id[normalized_thread_id] = thread
    return list(cleaned_threads_by_id.values()), summary


def _cleanup_mail_actors(
    actors: Any,
) -> tuple[list[dict[str, Any]], dict[str, int]]:
    cleaned_actors: list[dict[str, Any]] = []
    summary = {"actor_fields_normalized": 0}
    for raw_actor in actors or []:
        if not isinstance(raw_actor, dict):
            continue
        actor = dict(raw_actor)
        original_email = str(actor.get("email") or "").strip()
        original_name = str(
            actor.get("display_name") or actor.get("name") or ""
        ).strip()
        cleaned_email = normalized_email(original_email or actor.get("actor_id"))
        cleaned_name = _clean_single_display_value(original_name)
        if cleaned_email and cleaned_email != original_email:
            summary["actor_fields_normalized"] += 1
        if cleaned_name and cleaned_name != original_name:
            summary["actor_fields_normalized"] += 1
        if cleaned_email:
            actor["email"] = cleaned_email
        if cleaned_name:
            actor["display_name"] = cleaned_name
        cleaned_actors.append(actor)
    return cleaned_actors, _nonzero_summary(summary)


def _normalize_thread_like_id(value: object) -> str:
    return " ".join(str(value or "").replace("\r", " ").replace("\n", " ").split())


def _normalize_mail_address_field(value: object, *, many: bool) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    addresses = getaddresses([text.replace(";", ",")])
    cleaned_values: list[str] = []
    for display_name, address in addresses:
        cleaned_address = normalized_email(address or display_name)
        if cleaned_address:
            cleaned_values.append(cleaned_address)
            continue
        cleaned_name = _clean_single_display_value(display_name or address)
        if cleaned_name:
            cleaned_values.append(cleaned_name)
    if not cleaned_values:
        return _clean_single_display_value(text)
    if many:
        return ", ".join(dict.fromkeys(cleaned_values))
    return cleaned_values[0]


def _clean_single_display_value(value: object) -> str:
    text = " ".join(str(value or "").split())
    if not text:
        return ""
    return text


def _nonzero_summary(summary: dict[str, int]) -> dict[str, int]:
    return {key: value for key, value in summary.items() if value > 0}


def _merge_thread_messages(
    left: Any,
    right: Any,
) -> list[dict[str, Any]]:
    merged: dict[str, dict[str, Any]] = {}
    for index, item in enumerate(list(left or []) + list(right or [])):
        if not isinstance(item, dict):
            continue
        key = _normalize_thread_like_id(item.get("message_id") or item.get("id"))
        if not key:
            key = f"message-{index + 1}"
        merged.setdefault(key, item)
    return list(merged.values())


def _extract_all_actors(snapshot: ContextSnapshot) -> list[tuple[str, str, str]]:
    records: list[tuple[str, str, str]] = []
    for source in snapshot.sources:
        records.extend(_actors_from_source(source))
    return records


def _actors_from_source(source: ContextSourceResult) -> list[tuple[str, str, str]]:
    data = _source_data_mapping(source)
    provider = source.provider
    records: list[tuple[str, str, str]] = []

    if provider == "slack":
        for user in data.get("users", []):
            if not isinstance(user, dict):
                continue
            user_id = str(user.get("id") or "").strip()
            if not user_id:
                continue
            email = normalized_email(user.get("email"))
            name = normalized_name(user.get("real_name") or user.get("name"))
            records.append((f"slack:{user_id}", email, name))
        return records

    if provider in {"mail_archive", "gmail"}:
        for actor in data.get("actors", []):
            if not isinstance(actor, dict):
                continue
            actor_id = str(actor.get("actor_id") or actor.get("id") or "").strip()
            if not actor_id:
                continue
            email = normalized_email(actor.get("email") or actor_id)
            name = normalized_name(
                actor.get("display_name") or actor.get("name") or actor_id
            )
            records.append((f"{provider}:{actor_id}", email, name))
        seen_sender_values: set[str] = set()
        for thread in data.get("threads", []):
            if not isinstance(thread, dict):
                continue
            for message in thread.get("messages", []):
                if not isinstance(message, dict):
                    continue
                from_field = str(message.get("from") or "").strip()
                if not from_field or from_field in seen_sender_values:
                    continue
                seen_sender_values.add(from_field)
                email = normalized_email(from_field)
                if not email:
                    continue
                actor_key = f"{provider}:{email}"
                if any(record[0] == actor_key for record in records):
                    continue
                name_part, _ = parseaddr(from_field)
                name = normalized_name(name_part) if name_part else ""
                records.append((actor_key, email, name))
        return records

    if provider == "google":
        for user in data.get("users", []):
            if not isinstance(user, dict):
                continue
            user_id = str(user.get("id") or "").strip()
            if not user_id:
                continue
            email = normalized_email(user.get("email"))
            name = normalized_name(user.get("name") or user.get("display_name"))
            records.append((f"google:{user_id}", email, name))
        return records

    if provider == "jira":
        seen_assignees: set[str] = set()
        for issue in data.get("issues", []):
            if not isinstance(issue, dict):
                continue
            assignee = str(issue.get("assignee") or "").strip()
            if not assignee or assignee in seen_assignees:
                continue
            seen_assignees.add(assignee)
            email = normalized_email(assignee) if "@" in assignee else ""
            name = normalized_name(assignee) if "@" not in assignee else ""
            records.append((f"jira:{assignee.lower()}", email, name))
        return records

    if provider in {"crm", "salesforce"}:
        for contact in data.get("contacts", []):
            if not isinstance(contact, dict):
                continue
            contact_id = str(contact.get("id") or "").strip()
            if not contact_id:
                continue
            email = normalized_email(contact.get("email"))
            name = normalized_name(
                " ".join(
                    part
                    for part in (
                        str(contact.get("first_name") or "").strip(),
                        str(contact.get("last_name") or "").strip(),
                    )
                    if part
                )
            )
            records.append((f"{provider}:{contact_id}", email, name))
    return records


def _source_data_mapping(source: ContextSourceResult) -> dict[str, Any]:
    if hasattr(source.data, "model_dump"):
        return source.data.model_dump(mode="python")
    if isinstance(source.data, dict):
        return source.data
    return {}
