from __future__ import annotations

from typing import Any

from vei.context.models import ContextSnapshot

from ..corpus import (
    _company_history_event_id,
    _company_history_thread_id,
    _contains_keyword,
    _history_timestamp_ms,
    _normalized_actor_id,
    _truncate_snippet,
    timestamp_to_text,
)
from ..models import WhatIfArtifactFlags, WhatIfEvent


def build_jira_events(
    *,
    snapshot: ContextSnapshot,
    organization_domain: str,
    include_content: bool,
) -> list[WhatIfEvent]:
    source = snapshot.source_for("jira")
    if source is None or not isinstance(source.data, dict):
        return []
    issues = source.data.get("issues", [])
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
