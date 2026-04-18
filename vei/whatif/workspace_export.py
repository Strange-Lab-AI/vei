"""Export a quickstart / playable workspace into a what-if compatible context_snapshot.json.

The playable workspace produced by ``vei quickstart`` (or any vertical run) does not
write a ``context_snapshot.json`` by default. Its company graph lives inside the
blueprint asset under ``capability_graphs.{comm_graph,doc_graph,work_graph,...}``.

This module projects that capability graph into the multi-source ``ContextSnapshot``
shape that the what-if ``company_history`` loader understands, so users can branch
historical what-ifs from any quickstart workspace without re-ingesting external data.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from vei.context.api import ContextSnapshot, ContextSourceResult
from vei.whatif.filenames import CONTEXT_SNAPSHOT_FILE


def _iso_now() -> str:
    return datetime.now(UTC).isoformat().replace("+00:00", "Z")


def _ts_to_seconds(value: Any) -> str:
    """Best-effort coercion of various timestamp representations to a Slack-style ts string."""
    if value is None or value == "":
        return ""
    if isinstance(value, (int, float)):
        # Heuristic: ms vs s
        if value > 1e12:
            return f"{value / 1000.0:.6f}"
        return f"{float(value):.6f}"
    text = str(value).strip()
    if not text:
        return ""
    return text


def _slack_payload(comm_graph: dict[str, Any]) -> dict[str, Any] | None:
    channels = comm_graph.get("slack_channels") or []
    if not channels:
        return None
    normalized_channels: list[dict[str, Any]] = []
    user_ids: set[str] = set()
    for channel in channels:
        if not isinstance(channel, dict):
            continue
        channel_name = str(
            channel.get("channel") or channel.get("channel_id") or ""
        ).strip()
        if not channel_name:
            continue
        messages: list[dict[str, Any]] = []
        for msg in channel.get("messages") or []:
            if not isinstance(msg, dict):
                continue
            user = str(msg.get("user") or "").strip()
            if user:
                user_ids.add(user)
            ts = _ts_to_seconds(msg.get("ts"))
            messages.append(
                {
                    "ts": ts,
                    "user": user,
                    "text": str(msg.get("text") or ""),
                    "thread_ts": _ts_to_seconds(msg.get("thread_ts")) or None,
                }
            )
        normalized_channels.append(
            {
                "channel": channel_name,
                "channel_id": channel_name,
                "messages": messages,
            }
        )
    if not normalized_channels:
        return None
    users = [{"id": uid, "name": uid.split(".")[0] or uid} for uid in sorted(user_ids)]
    return {"channels": normalized_channels, "users": users}


def _mail_archive_payload(comm_graph: dict[str, Any]) -> dict[str, Any] | None:
    threads = comm_graph.get("mail_threads") or []
    if not threads:
        return None
    normalized_threads: list[dict[str, Any]] = []
    actors: dict[str, dict[str, str]] = {}
    for thread in threads:
        if not isinstance(thread, dict):
            continue
        thread_id = str(thread.get("thread_id") or "").strip()
        if not thread_id:
            continue
        normalized_messages: list[dict[str, Any]] = []
        for msg in thread.get("messages") or []:
            if not isinstance(msg, dict):
                continue
            from_addr = str(msg.get("from_address") or msg.get("from") or "").strip()
            to_addr = str(msg.get("to_address") or msg.get("to") or "").strip()
            if from_addr:
                actors.setdefault(
                    from_addr,
                    {"address": from_addr, "name": from_addr.split("@")[0]},
                )
            if to_addr:
                actors.setdefault(
                    to_addr,
                    {"address": to_addr, "name": to_addr.split("@")[0]},
                )
            time_ms = msg.get("time_ms") or msg.get("timestamp_ms")
            timestamp = ""
            if isinstance(time_ms, (int, float)):
                timestamp = (
                    datetime.fromtimestamp(float(time_ms) / 1000.0, UTC)
                    .isoformat()
                    .replace("+00:00", "Z")
                )
            normalized_messages.append(
                {
                    "from": from_addr,
                    "to": to_addr,
                    "from_address": from_addr,
                    "to_address": to_addr,
                    "subject": str(msg.get("subject") or thread.get("title") or ""),
                    "body_text": str(msg.get("body_text") or msg.get("body") or ""),
                    "time_ms": int(time_ms) if isinstance(time_ms, (int, float)) else 0,
                    "timestamp": timestamp,
                    "unread": bool(msg.get("unread", False)),
                }
            )
        if not normalized_messages:
            continue
        normalized_threads.append(
            {
                "thread_id": thread_id,
                "subject": str(thread.get("title") or thread_id),
                "category": str(thread.get("category") or "archive"),
                "messages": normalized_messages,
            }
        )
    if not normalized_threads:
        return None
    return {
        "threads": normalized_threads,
        "actors": list(actors.values()),
        "profile": {},
    }


def _jira_payload(
    work_graph: dict[str, Any],
    *,
    fallback_timestamp: str,
) -> dict[str, Any] | None:
    tickets = work_graph.get("tickets") or []
    if not tickets:
        return None
    issues: list[dict[str, Any]] = []
    for ticket in tickets:
        if not isinstance(ticket, dict):
            continue
        ticket_id = str(ticket.get("ticket_id") or "").strip()
        if not ticket_id:
            continue
        issues.append(
            {
                "ticket_id": ticket_id,
                "title": str(ticket.get("title") or ticket_id),
                "status": str(ticket.get("status") or "open"),
                "assignee": str(ticket.get("assignee") or ""),
                "summary": str(ticket.get("description") or ""),
                "updated": str(ticket.get("updated") or fallback_timestamp),
                "comments": list(ticket.get("comments") or []),
            }
        )
    if not issues:
        return None
    return {"issues": issues, "projects": [], "parse_warnings": []}


def _google_payload(
    doc_graph: dict[str, Any],
    *,
    fallback_timestamp: str,
) -> dict[str, Any] | None:
    documents = doc_graph.get("documents") or []
    if not documents:
        return None
    normalized_docs: list[dict[str, Any]] = []
    for doc in documents:
        if not isinstance(doc, dict):
            continue
        doc_id = str(doc.get("doc_id") or doc.get("id") or "").strip()
        if not doc_id:
            continue
        normalized_docs.append(
            {
                "doc_id": doc_id,
                "title": str(doc.get("title") or doc_id),
                "body": str(doc.get("body") or ""),
                "tags": list(doc.get("tags") or []),
                "updated": str(doc.get("updated") or fallback_timestamp),
                "owner": str(doc.get("owner") or ""),
            }
        )
    if not normalized_docs:
        return None
    return {
        "documents": normalized_docs,
        "users": [],
        "drive_shares": list(doc_graph.get("drive_shares") or []),
        "parse_warnings": [],
    }


def _load_blueprint_capability_graphs(workspace_root: Path) -> dict[str, Any]:
    """Locate the workspace's blueprint asset and return its capability_graphs block."""
    candidates = [
        workspace_root / "sources" / "blueprint_asset.json",
        workspace_root / "blueprint_asset.json",
    ]
    runs_dir = workspace_root / "runs"
    if runs_dir.is_dir():
        for run_dir in sorted(runs_dir.iterdir()):
            if run_dir.is_dir():
                candidates.append(run_dir / "artifacts" / "blueprint_asset.json")
    for path in candidates:
        if path.exists():
            payload = json.loads(path.read_text(encoding="utf-8"))
            cg = payload.get("capability_graphs") or {}
            if isinstance(cg, dict):
                return cg
    raise ValueError(
        f"workspace does not contain a blueprint_asset.json with capability_graphs: "
        f"{workspace_root}"
    )


def export_workspace_history_snapshot(
    workspace_root: str | Path,
    *,
    output_path: str | Path | None = None,
) -> Path:
    """Project a quickstart/playable workspace into a what-if context_snapshot.json.

    Reads ``capability_graphs`` from the workspace's blueprint asset (under
    ``sources/`` or ``runs/*/artifacts/``) and writes a multi-source
    ``ContextSnapshot`` to ``output_path`` (defaults to ``<workspace>/context_snapshot.json``).

    The resulting snapshot is consumable by ``vei whatif open --source company_history --source-dir <output_path>``.
    """
    resolved_root = Path(workspace_root).expanduser().resolve()
    capability_graphs = _load_blueprint_capability_graphs(resolved_root)

    organization_name = str(
        capability_graphs.get("organization_name") or "Demo Company"
    )
    organization_domain = str(
        capability_graphs.get("organization_domain") or "demo.example"
    )
    captured_at = _iso_now()

    sources: list[ContextSourceResult] = []

    comm_graph = capability_graphs.get("comm_graph") or {}
    slack_data = _slack_payload(comm_graph)
    if slack_data is not None:
        sources.append(
            ContextSourceResult(
                provider="slack",
                captured_at=captured_at,
                status="ok",
                record_counts={
                    "channels": len(slack_data["channels"]),
                    "messages": sum(
                        len(c.get("messages", [])) for c in slack_data["channels"]
                    ),
                    "users": len(slack_data["users"]),
                },
                data=slack_data,
            )
        )

    mail_data = _mail_archive_payload(comm_graph)
    if mail_data is not None:
        sources.append(
            ContextSourceResult(
                provider="mail_archive",
                captured_at=captured_at,
                status="ok",
                record_counts={
                    "threads": len(mail_data["threads"]),
                    "messages": sum(
                        len(t.get("messages", [])) for t in mail_data["threads"]
                    ),
                    "actors": len(mail_data["actors"]),
                },
                data=mail_data,
            )
        )

    work_graph = capability_graphs.get("work_graph") or {}
    jira_data = _jira_payload(work_graph, fallback_timestamp=captured_at)
    if jira_data is not None:
        sources.append(
            ContextSourceResult(
                provider="jira",
                captured_at=captured_at,
                status="ok",
                record_counts={"issues": len(jira_data["issues"])},
                data=jira_data,
            )
        )

    doc_graph = capability_graphs.get("doc_graph") or {}
    google_data = _google_payload(doc_graph, fallback_timestamp=captured_at)
    if google_data is not None:
        sources.append(
            ContextSourceResult(
                provider="google",
                captured_at=captured_at,
                status="ok",
                record_counts={"documents": len(google_data["documents"])},
                data=google_data,
            )
        )

    if not sources:
        raise ValueError(
            "blueprint capability_graphs did not yield any what-if compatible sources "
            "(slack, mail_archive, jira, google)"
        )

    snapshot = ContextSnapshot(
        organization_name=organization_name,
        organization_domain=organization_domain,
        captured_at=captured_at,
        sources=sources,
        metadata={
            "snapshot_role": "company_history_bundle",
            "exported_from": "workspace_blueprint",
        },
    )

    out_path = (
        Path(output_path).expanduser().resolve()
        if output_path is not None
        else resolved_root / CONTEXT_SNAPSHOT_FILE
    )
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(snapshot.model_dump_json(indent=2), encoding="utf-8")
    return out_path


__all__ = ["export_workspace_history_snapshot"]
