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
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from vei.context.api import (
    ContextSnapshot,
    ContextSourceResult,
    write_canonical_history_sidecars,
)
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


def _parse_datetime(value: Any) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00")).astimezone(UTC)
    except ValueError:
        return None


def _iso_from_dt(value: datetime) -> str:
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _dt_to_slack_ts(value: datetime) -> str:
    return f"{value.timestamp():.6f}"


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


def _run_activity_payloads(
    workspace_root: Path,
    *,
    captured_at: str,
    slack_data: dict[str, Any] | None,
    jira_data: dict[str, Any] | None,
    google_data: dict[str, Any] | None,
) -> tuple[dict[str, Any] | None, dict[str, Any] | None, dict[str, Any] | None]:
    runs_dir = workspace_root / "runs"
    if not runs_dir.is_dir():
        return slack_data, jira_data, google_data

    call_events = list(_iter_run_calls(runs_dir))
    if not call_events:
        return slack_data, jira_data, google_data

    enriched_slack = _ensure_slack_payload(slack_data)
    enriched_jira = _ensure_jira_payload(jira_data)
    enriched_google = _ensure_google_payload(google_data)

    for event in call_events:
        summary = _run_call_summary(event)
        if not summary:
            continue

        channel_name = _run_channel_name(event)
        if channel_name:
            _append_slack_message(
                enriched_slack,
                channel_name=channel_name,
                user=event["actor_id"],
                text=summary,
                timestamp=event["timestamp"],
            )

        ticket_id = _run_ticket_id(event)
        if ticket_id:
            _append_ticket_comment(
                enriched_jira,
                ticket_id=ticket_id,
                title=_run_ticket_title(ticket_id),
                author=event["actor_id"],
                body=summary,
                timestamp=event["timestamp"],
                comment_id=event["comment_id"],
            )

        doc_id = _run_doc_id(event)
        if doc_id:
            _append_document_comment(
                enriched_google,
                doc_id=doc_id,
                title=_run_doc_title(doc_id),
                author=event["actor_id"],
                body=summary,
                timestamp=event["timestamp"],
                comment_id=event["comment_id"],
            )

    return enriched_slack, enriched_jira, enriched_google


def _iter_run_calls(runs_dir: Path) -> list[dict[str, str]]:
    events: list[dict[str, str]] = []
    for manifest_path in sorted(runs_dir.glob("*/run_manifest.json")):
        manifest_payload = json.loads(manifest_path.read_text(encoding="utf-8"))
        trace_path = manifest_path.parent / "artifacts" / "trace.jsonl"
        if not trace_path.exists():
            continue
        started_at = _parse_datetime(
            manifest_payload.get("started_at") or manifest_payload.get("completed_at")
        )
        if started_at is None:
            continue
        run_id = manifest_path.parent.name
        runner = str(manifest_payload.get("runner") or run_id).strip() or run_id
        variant = (
            str(manifest_payload.get("workflow_variant") or "").strip()
            or str(manifest_payload.get("scenario_name") or "").strip()
            or "service_ops"
        )
        with trace_path.open("r", encoding="utf-8") as handle:
            for index, raw_line in enumerate(handle, start=1):
                line = raw_line.strip()
                if not line:
                    continue
                payload = json.loads(line)
                if payload.get("type") != "call":
                    continue
                time_ms = int(payload.get("time_ms") or 0)
                timestamp = started_at + timedelta(milliseconds=time_ms)
                events.append(
                    {
                        "run_id": run_id,
                        "runner": runner,
                        "variant": variant,
                        "timestamp": _iso_from_dt(timestamp),
                        "tool": str(payload.get("tool") or "").strip(),
                        "actor_id": _run_actor_id(runner, run_id),
                        "comment_id": f"{run_id}-call-{index}",
                        "args_json": json.dumps(
                            payload.get("args") or {}, sort_keys=True
                        ),
                        "response_json": json.dumps(
                            payload.get("response") or {}, sort_keys=True
                        ),
                    }
                )
    return events


def _run_actor_id(runner: str, run_id: str) -> str:
    normalized_runner = runner.replace("_", "-").strip().lower() or "run"
    normalized_run_id = run_id.replace("_", "-").strip().lower() or "session"
    return f"{normalized_runner}.{normalized_run_id}"


def _run_call_summary(event: dict[str, str]) -> str:
    tool = event["tool"]
    args = json.loads(event["args_json"])
    response = json.loads(event["response_json"])
    if tool == "servicedesk.update_request":
        request_id = str(args.get("request_id") or "service request").strip()
        status = str(
            response.get("status") or args.get("approval_status") or ""
        ).strip()
        comment = str(args.get("comment") or "").strip()
        return " ".join(
            part
            for part in [
                f"{request_id} dispatch approval updated.",
                f"Status: {status}." if status else "",
                comment,
            ]
            if part
        )
    if tool == "service_ops.assign_dispatch":
        work_order_id = str(args.get("work_order_id") or "work order").strip()
        technician_id = str(
            response.get("technician_id") or args.get("technician_id") or ""
        ).strip()
        note = str(args.get("note") or "").strip()
        return " ".join(
            part
            for part in [
                f"{work_order_id} reassigned.",
                f"Technician: {technician_id}." if technician_id else "",
                note,
            ]
            if part
        )
    if tool == "service_ops.hold_billing":
        billing_case_id = str(args.get("billing_case_id") or "billing case").strip()
        reason = str(args.get("reason") or "").strip()
        return " ".join(
            part
            for part in [
                f"{billing_case_id} placed on billing hold.",
                reason,
            ]
            if part
        )
    if tool == "service_ops.clear_exception":
        exception_id = str(args.get("exception_id") or "exception").strip()
        note = str(args.get("resolution_note") or "").strip()
        return " ".join(
            part
            for part in [
                f"{exception_id} cleared.",
                note,
            ]
            if part
        )
    if tool == "docs.update":
        doc_id = str(args.get("doc_id") or "document").strip()
        body = str(args.get("body") or "").strip()
        return " ".join(
            part
            for part in [
                f"{doc_id} updated.",
                body,
            ]
            if part
        )
    if not tool:
        return ""
    return f"{tool} executed."


def _run_channel_name(event: dict[str, str]) -> str:
    tool = event["tool"]
    if tool == "service_ops.hold_billing":
        return "#billing-ops"
    if tool == "service_ops.clear_exception":
        return "#vip-escalations"
    if tool == "docs.update":
        return "#exec-brief"
    if tool in {"service_ops.assign_dispatch", "servicedesk.update_request"}:
        return "#clearwater-dispatch"
    return "#clearwater-dispatch"


def _run_ticket_id(event: dict[str, str]) -> str:
    args = json.loads(event["args_json"])
    tool = event["tool"]
    if tool == "service_ops.hold_billing":
        return "JRA-CFS-12"
    if tool == "service_ops.assign_dispatch":
        return "JRA-CFS-11"
    if tool == "service_ops.clear_exception":
        return "JRA-CFS-10"
    if tool == "servicedesk.update_request":
        return "JRA-CFS-10"
    if tool == "docs.update":
        return "JRA-CFS-10"
    request_id = str(args.get("request_id") or "").strip()
    if request_id:
        return "JRA-CFS-10"
    return ""


def _run_ticket_title(ticket_id: str) -> str:
    titles = {
        "JRA-CFS-10": "VIP outage command thread",
        "JRA-CFS-11": "Backup dispatch routing",
        "JRA-CFS-12": "Billing dispute follow-through",
    }
    return titles.get(ticket_id, ticket_id)


def _run_doc_id(event: dict[str, str]) -> str:
    tool = event["tool"]
    args = json.loads(event["args_json"])
    if tool == "docs.update":
        return str(args.get("doc_id") or "DOC-CFS-HANDOFF").strip()
    if tool == "service_ops.assign_dispatch":
        return "DOC-CFS-DISPATCH"
    if tool == "service_ops.hold_billing":
        return "DOC-CFS-BILLING"
    if tool == "service_ops.clear_exception":
        return "DOC-CFS-HANDOFF"
    if tool == "servicedesk.update_request":
        return "DOC-CFS-RUNBOOK"
    return ""


def _run_doc_title(doc_id: str) -> str:
    titles = {
        "DOC-CFS-BILLING": "Clearwater Billing Notes",
        "DOC-CFS-DISPATCH": "Morning Dispatch Board",
        "DOC-CFS-HANDOFF": "Field-To-Billing Handoff Note",
        "DOC-CFS-RUNBOOK": "Clearwater Medical Response Runbook",
        "DOC-CFS-VIP": "VIP Account Brief",
    }
    return titles.get(doc_id, doc_id)


def _ensure_slack_payload(slack_data: dict[str, Any] | None) -> dict[str, Any]:
    if slack_data is not None:
        slack_data.setdefault("channels", [])
        slack_data.setdefault("users", [])
        return slack_data
    return {"channels": [], "users": []}


def _ensure_jira_payload(jira_data: dict[str, Any] | None) -> dict[str, Any]:
    if jira_data is not None:
        jira_data.setdefault("issues", [])
        jira_data.setdefault("projects", [])
        jira_data.setdefault("parse_warnings", [])
        return jira_data
    return {"issues": [], "projects": [], "parse_warnings": []}


def _ensure_google_payload(google_data: dict[str, Any] | None) -> dict[str, Any]:
    if google_data is not None:
        google_data.setdefault("documents", [])
        google_data.setdefault("users", [])
        google_data.setdefault("drive_shares", [])
        google_data.setdefault("parse_warnings", [])
        return google_data
    return {"documents": [], "users": [], "drive_shares": [], "parse_warnings": []}


def _append_slack_message(
    slack_data: dict[str, Any],
    *,
    channel_name: str,
    user: str,
    text: str,
    timestamp: str,
) -> None:
    channels = slack_data.setdefault("channels", [])
    users = slack_data.setdefault("users", [])
    channel = next(
        (
            item
            for item in channels
            if str(item.get("channel") or item.get("channel_id") or "").strip()
            == channel_name
        ),
        None,
    )
    if channel is None:
        channel = {"channel": channel_name, "channel_id": channel_name, "messages": []}
        channels.append(channel)
    messages = channel.setdefault("messages", [])
    dt = _parse_datetime(timestamp)
    messages.append(
        {
            "ts": _dt_to_slack_ts(dt) if dt is not None else _ts_to_seconds(timestamp),
            "user": user,
            "text": text,
            "thread_ts": None,
        }
    )
    if user and not any(str(item.get("id") or "").strip() == user for item in users):
        users.append({"id": user, "name": user.split(".")[0] or user})


def _append_ticket_comment(
    jira_data: dict[str, Any],
    *,
    ticket_id: str,
    title: str,
    author: str,
    body: str,
    timestamp: str,
    comment_id: str,
) -> None:
    issues = jira_data.setdefault("issues", [])
    issue = next(
        (
            item
            for item in issues
            if str(item.get("ticket_id") or item.get("key") or "").strip() == ticket_id
        ),
        None,
    )
    if issue is None:
        issue = {
            "ticket_id": ticket_id,
            "title": title,
            "status": "open",
            "assignee": author,
            "summary": "",
            "updated": timestamp,
            "comments": [],
        }
        issues.append(issue)
    comments = issue.setdefault("comments", [])
    comments.append(
        {
            "id": comment_id,
            "author": author,
            "body": body,
            "created": timestamp,
        }
    )
    issue["updated"] = timestamp


def _append_document_comment(
    google_data: dict[str, Any],
    *,
    doc_id: str,
    title: str,
    author: str,
    body: str,
    timestamp: str,
    comment_id: str,
) -> None:
    documents = google_data.setdefault("documents", [])
    document = next(
        (
            item
            for item in documents
            if str(item.get("doc_id") or item.get("id") or "").strip() == doc_id
        ),
        None,
    )
    if document is None:
        document = {
            "doc_id": doc_id,
            "title": title,
            "body": "",
            "tags": ["generated"],
            "updated": timestamp,
            "owner": author,
            "comments": [],
        }
        documents.append(document)
    comments = document.setdefault("comments", [])
    comments.append(
        {
            "id": comment_id,
            "author": author,
            "body": body,
            "created": timestamp,
        }
    )
    document["updated"] = timestamp


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

    comm_graph = capability_graphs.get("comm_graph") or {}
    slack_data = _slack_payload(comm_graph)
    mail_data = _mail_archive_payload(comm_graph)
    work_graph = capability_graphs.get("work_graph") or {}
    jira_data = _jira_payload(work_graph, fallback_timestamp=captured_at)
    doc_graph = capability_graphs.get("doc_graph") or {}
    google_data = _google_payload(doc_graph, fallback_timestamp=captured_at)
    slack_data, jira_data, google_data = _run_activity_payloads(
        resolved_root,
        captured_at=captured_at,
        slack_data=slack_data,
        jira_data=jira_data,
        google_data=google_data,
    )

    sources: list[ContextSourceResult] = []
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
    write_canonical_history_sidecars(snapshot, out_path)
    return out_path


__all__ = ["export_workspace_history_snapshot"]
