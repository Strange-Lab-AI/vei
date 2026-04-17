from __future__ import annotations

from typing import Any, Dict

from .models import LivingSurfaceItem, LivingSurfacePanel
from ._surface_panels_shared import (
    approval_sort_rank,
    build_panel,
    compact_badges,
    dict_list,
    dict_records,
    slack_ts,
    ticket_sort_rank,
    truncate,
)


def build_slack_panel(
    components: Dict[str, Dict[str, Any]],
    context: Dict[str, Any],
) -> LivingSurfacePanel | None:
    del context
    slack = components.get("slack", {})
    channels = slack.get("channels")
    if not isinstance(channels, dict) or not channels:
        return None

    rows: list[tuple[float, str, Dict[str, Any], int]] = []
    for channel_name, payload in channels.items():
        if not isinstance(payload, dict):
            continue
        unread = int(payload.get("unread", 0) or 0)
        for message in payload.get("messages", []):
            if isinstance(message, dict):
                rows.append(
                    (slack_ts(message.get("ts")), channel_name, message, unread)
                )

    if not rows:
        return None

    rows.sort(key=lambda item: item[0], reverse=True)
    items = [
        LivingSurfaceItem(
            item_id=f"chat:{channel_name}:{message.get('ts', index)}",
            title=str(message.get("user", "team member")),
            subtitle=channel_name,
            body=truncate(str(message.get("text", "")), 160),
            status=("attention" if unread else "ok"),
            badges=compact_badges(
                [
                    channel_name,
                    "thread reply" if message.get("thread_ts") else "",
                    f"{unread} unread" if unread else "",
                ]
            ),
            highlight_ref=f"slack:{channel_name}:{message.get('ts', index)}",
        )
        for index, (_, channel_name, message, unread) in enumerate(rows[:8], start=1)
    ]
    unread_total = sum(
        int(payload.get("unread", 0) or 0)
        for payload in channels.values()
        if isinstance(payload, dict)
    )
    return build_panel(
        surface="slack",
        kind="chat",
        title="Team Chat",
        accent="#36c5f0",
        headline=f"{len(channels)} channels · {len(rows)} messages",
        items=items,
        fallback_status=("attention" if unread_total else "ok"),
    )


def build_mail_panel(
    components: Dict[str, Dict[str, Any]],
    context: Dict[str, Any],
) -> LivingSurfacePanel | None:
    del context
    mail = components.get("mail", {})
    messages = mail.get("messages")
    if not isinstance(messages, dict) or not messages:
        return None

    threads: Dict[str, list[Dict[str, Any]]] = {}
    for message in messages.values():
        if not isinstance(message, dict):
            continue
        thread_id = str(message.get("thread_id") or message.get("subj") or "mail")
        threads.setdefault(thread_id, []).append(message)

    if not threads:
        return None

    rows: list[tuple[int, str, list[Dict[str, Any]]]] = []
    for thread_id, thread_messages in threads.items():
        latest_time = max(int(item.get("time_ms", 0) or 0) for item in thread_messages)
        rows.append((latest_time, thread_id, thread_messages))
    rows.sort(key=lambda item: item[0], reverse=True)

    items = []
    unread_total = 0
    for _, thread_id, thread_messages in rows[:6]:
        ordered = sorted(
            thread_messages,
            key=lambda item: int(item.get("time_ms", 0) or 0),
            reverse=True,
        )
        latest = ordered[0]
        unread_count = sum(1 for item in thread_messages if item.get("unread"))
        unread_total += unread_count
        items.append(
            LivingSurfaceItem(
                item_id=f"mail:{thread_id}",
                title=str(latest.get("subj", thread_id)),
                subtitle=str(latest.get("from", "inbox")),
                body=truncate(str(latest.get("body_text", "")), 160),
                status=("attention" if unread_count else "ok"),
                badges=compact_badges(
                    [
                        str(latest.get("category", "")),
                        f"{len(thread_messages)} messages",
                        f"{unread_count} unread" if unread_count else "",
                    ]
                ),
                highlight_ref=f"mail:{thread_id}",
            )
        )

    return build_panel(
        surface="mail",
        kind="mail",
        title="Email",
        accent="#ffb454",
        headline=f"{len(threads)} threads · {unread_total} unread",
        items=items,
        fallback_status=("attention" if unread_total else "ok"),
    )


def build_ticket_panel(
    components: Dict[str, Dict[str, Any]],
    context: Dict[str, Any],
) -> LivingSurfacePanel | None:
    del context
    tickets = components.get("tickets", {})
    payload = tickets.get("tickets")
    if not isinstance(payload, dict) or not payload:
        return None

    ordered = sorted(
        (item for item in payload.values() if isinstance(item, dict)),
        key=lambda item: (
            ticket_sort_rank(str(item.get("status", ""))),
            str(item.get("ticket_id", "")),
        ),
    )
    items = [
        LivingSurfaceItem(
            item_id=f"ticket:{item.get('ticket_id', index)}",
            title=str(item.get("title", item.get("ticket_id", "ticket"))),
            subtitle=str(item.get("assignee", "unassigned")),
            body=truncate(str(item.get("description", "")), 140),
            status=str(item.get("status", "")),
            badges=compact_badges(
                [str(item.get("status", "")), str(item.get("ticket_id", ""))]
            ),
            highlight_ref=f"ticket:{item.get('ticket_id', index)}",
        )
        for index, item in enumerate(ordered[:8], start=1)
    ]
    return build_panel(
        surface="tickets",
        kind="queue",
        title="Work Tracker",
        accent="#ff6d5e",
        headline=f"{len(payload)} active tickets",
        items=items,
    )


def build_docs_panel(
    components: Dict[str, Dict[str, Any]],
    context: Dict[str, Any],
) -> LivingSurfacePanel | None:
    del context
    docs = components.get("docs", {})
    google_admin = components.get("google_admin", {})
    payload = dict_records(docs, "docs")
    if not payload:
        return None

    metadata = dict_records(docs, "metadata")
    shares = dict_records(google_admin, "drive_shares")
    ordered = sorted(
        payload.values(),
        key=lambda item: int(
            metadata.get(str(item.get("doc_id")), {}).get("updated_ms", 0) or 0
        ),
        reverse=True,
    )

    items = []
    for index, item in enumerate(ordered[:6], start=1):
        doc_id = str(item.get("doc_id", index))
        share = shares.get(doc_id)
        tags = (
            [str(tag) for tag in item.get("tags", [])]
            if isinstance(item.get("tags"), list)
            else []
        )
        items.append(
            LivingSurfaceItem(
                item_id=f"doc:{doc_id}",
                title=str(item.get("title", doc_id)),
                subtitle=doc_id,
                body=truncate(str(item.get("body", "")), 160),
                status="ok",
                badges=compact_badges(
                    tags[:2]
                    + (
                        [str(share.get("visibility", ""))]
                        if isinstance(share, dict)
                        else []
                    )
                ),
                highlight_ref=f"doc:{doc_id}",
            )
        )

    return build_panel(
        surface="docs",
        kind="document",
        title="Documents",
        accent="#1aa88d",
        headline=f"{len(payload)} artifacts in circulation",
        items=items,
    )


def build_notes_panel(
    components: Dict[str, Dict[str, Any]],
    context: Dict[str, Any],
) -> LivingSurfacePanel | None:
    del context
    notes = components.get("notes", {})
    payload = dict_records(notes, "entries")
    if not payload:
        return None

    ordered = sorted(
        payload.values(),
        key=lambda item: int(item.get("updated_ms", 0) or 0),
        reverse=True,
    )
    items = [
        LivingSurfaceItem(
            item_id=f"note:{item.get('entry_id', index)}",
            title=str(item.get("title", item.get("entry_id", "note"))),
            subtitle=str(item.get("entry_id", index)),
            body=truncate(str(item.get("body", "")), 160),
            status="ok",
            badges=compact_badges(
                [str(tag) for tag in list(item.get("tags") or [])[:2]]
            ),
            highlight_ref=f"note:{item.get('entry_id', index)}",
        )
        for index, item in enumerate(ordered[:6], start=1)
        if isinstance(item, dict)
    ]
    return build_panel(
        surface="notes",
        kind="document",
        title="Notes",
        accent="#5d84ff",
        headline=f"{len(payload)} captured notes",
        items=items,
    )


def build_knowledge_panel(
    components: Dict[str, Dict[str, Any]],
    context: Dict[str, Any],
) -> LivingSurfacePanel | None:
    del context
    knowledge = components.get("knowledge", {})
    payload = dict_records(knowledge, "assets")
    if not payload:
        return None

    ordered = sorted(
        payload.values(),
        key=lambda item: (
            str(item.get("status", "active")) == "active",
            int(item.get("metadata", {}).get("captured_at_ms", 0) or 0),
        ),
        reverse=True,
    )
    items = [
        LivingSurfaceItem(
            item_id=f"knowledge:{item.get('asset_id', index)}",
            title=str(item.get("title", item.get("asset_id", "knowledge asset"))),
            subtitle=str(item.get("kind", "knowledge")),
            body=truncate(str(item.get("summary", item.get("body", ""))), 160),
            status=str(item.get("status", "active")),
            badges=compact_badges(
                [str(item.get("status", "active"))]
                + [str(tag) for tag in list(item.get("tags") or [])[:2]]
                + (["composed"] if isinstance(item.get("composition"), dict) else [])
            ),
            highlight_ref=f"knowledge:{item.get('asset_id', index)}",
        )
        for index, item in enumerate(ordered[:8], start=1)
        if isinstance(item, dict)
    ]
    active_assets = sum(
        1 for item in payload.values() if str(item.get("status", "active")) == "active"
    )
    return build_panel(
        surface="knowledge",
        kind="document",
        title="Knowledge",
        accent="#5d84ff",
        headline=f"{active_assets} active assets · {len(payload)} total",
        items=items,
        fallback_status=("attention" if active_assets < len(payload) else "ok"),
    )


def build_approval_panel(
    components: Dict[str, Dict[str, Any]],
    context: Dict[str, Any],
) -> LivingSurfacePanel | None:
    del context
    servicedesk = components.get("servicedesk", {})
    requests = dict_records(servicedesk, "requests")
    if not requests:
        return None

    ordered = sorted(
        requests.values(),
        key=lambda item: (
            approval_sort_rank(str(item.get("status", ""))),
            str(item.get("request_id", "")),
        ),
    )
    items = []
    for index, item in enumerate(ordered[:8], start=1):
        approval_list = dict_list(item, "approvals")
        pending_count = sum(
            1
            for approval in approval_list
            if str(approval.get("status", "")).upper() == "PENDING"
        )
        items.append(
            LivingSurfaceItem(
                item_id=f"request:{item.get('request_id', index)}",
                title=str(item.get("title", item.get("request_id", "request"))),
                subtitle=str(item.get("requester", "requester")),
                body=truncate(str(item.get("description", "")), 140),
                status=str(item.get("status", "")),
                badges=compact_badges(
                    [
                        str(item.get("status", "")),
                        f"{pending_count} pending" if pending_count else "",
                    ]
                ),
                highlight_ref=f"request:{item.get('request_id', index)}",
            )
        )

    pending_total = sum(
        1
        for item in ordered
        if str(item.get("status", "")).lower()
        in {"pending_approval", "pending", "review"}
    )
    return build_panel(
        surface="approvals",
        kind="approval",
        title="Approvals",
        accent="#9b7bff",
        headline=f"{len(requests)} routed requests",
        items=items,
        fallback_status=("warning" if pending_total else "ok"),
    )


def build_workforce_panel(workforce: Dict[str, Any]) -> LivingSurfacePanel | None:
    if not isinstance(workforce, dict):
        return None
    summary = workforce.get("summary")
    snapshot = workforce.get("snapshot")
    if not isinstance(summary, dict) or not isinstance(snapshot, dict):
        return None

    items: list[LivingSurfaceItem] = []
    for agent in (snapshot.get("agents") or [])[:2]:
        if not isinstance(agent, dict):
            continue
        items.append(
            LivingSurfaceItem(
                item_id=f"workforce-agent:{agent.get('agent_id', '')}",
                title=str(agent.get("name", agent.get("agent_id", "agent"))),
                subtitle=str(agent.get("status", "unknown")),
                body=truncate(
                    f"Mode {agent.get('integration_mode', 'observe')} · "
                    f"{len(agent.get('task_ids') or [])} task(s)",
                    160,
                ),
                status=str(agent.get("status", "")),
                badges=compact_badges(
                    [
                        str(agent.get("integration_mode", "")),
                        str(agent.get("policy_profile_id", "")),
                    ]
                ),
                highlight_ref=str(agent.get("agent_id", "")) or None,
            )
        )

    for task in (snapshot.get("tasks") or [])[:2]:
        if not isinstance(task, dict):
            continue
        items.append(
            LivingSurfaceItem(
                item_id=f"workforce-task:{task.get('task_id', '')}",
                title=str(task.get("identifier") or task.get("title", "task")),
                subtitle=str(task.get("status", "unknown")),
                body=truncate(
                    str(
                        task.get("latest_comment_preview") or task.get("summary") or ""
                    ),
                    160,
                ),
                status=str(task.get("status", "")),
                badges=compact_badges(
                    [
                        f"{len(task.get('linked_approval_ids') or [])} approvals",
                        str(task.get("priority", "")),
                    ]
                ),
                highlight_ref=str(task.get("task_id", "")) or None,
            )
        )

    for approval in (snapshot.get("approvals") or [])[:2]:
        if not isinstance(approval, dict):
            continue
        items.append(
            LivingSurfaceItem(
                item_id=f"workforce-approval:{approval.get('approval_id', '')}",
                title=str(
                    approval.get("summary")
                    or approval.get("approval_type")
                    or approval.get("approval_id", "approval")
                ),
                subtitle=str(approval.get("status", "unknown")),
                body=truncate(str(approval.get("decision_note") or ""), 160),
                status=str(approval.get("status", "")),
                badges=compact_badges(
                    [
                        str(approval.get("requested_by_name", "")),
                        f"{len(approval.get('task_ids') or [])} tasks",
                    ]
                ),
                highlight_ref=str(approval.get("approval_id", "")) or None,
            )
        )

    if not items:
        return None

    pending_total = int(summary.get("pending_approval_count", 0) or 0)
    governable_total = int(summary.get("governable_agent_count", 0) or 0)
    return build_panel(
        surface="workforce",
        kind="queue",
        title="Control Room",
        accent="#4b8dff",
        headline=(
            f"{summary.get('observed_agent_count', 0)} agents · "
            f"{summary.get('task_count', 0)} tasks · "
            f"{pending_total} waiting decisions"
        ),
        items=items[:6],
        fallback_status=(
            "warning" if pending_total else ("ok" if governable_total else "attention")
        ),
    )
