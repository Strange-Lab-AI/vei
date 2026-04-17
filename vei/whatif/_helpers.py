from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

from vei.whatif_filenames import CONTEXT_SNAPSHOT_FILE
from .models import (
    WhatIfEvent,
    WhatIfEventReference,
    WhatIfScenarioId,
    WhatIfThreadSummary,
)


def chat_channel_name_from_reference(
    event: WhatIfEventReference,
    *,
    default: str = "#history",
) -> str:
    recipients = [item for item in event.to_recipients if item]
    if recipients:
        return recipients[0]
    if event.target_id:
        return event.target_id
    return default


def chat_channel_name(
    event: WhatIfEvent,
    *,
    default: str = "#history",
) -> str:
    recipients = [item for item in event.flags.to_recipients if item]
    if recipients:
        return recipients[0]
    if event.target_id:
        return event.target_id
    return default


def primary_recipient(
    event: WhatIfEvent,
    *,
    default: str | None = None,
) -> str:
    recipients = [item for item in event.flags.to_recipients if item]
    if recipients:
        return recipients[0]
    if event.target_id:
        return event.target_id
    if default is not None:
        return default
    return historical_archive_address("", "archive")


def reference_primary_recipient(
    event: WhatIfEventReference,
    *,
    default: str = "",
) -> str:
    recipients = [item for item in event.to_recipients if item]
    if recipients:
        return recipients[0]
    if event.target_id:
        return event.target_id
    return default


def historical_archive_address(organization_domain: str, local_part: str) -> str:
    normalized_domain = organization_domain.strip().lower()
    if not normalized_domain:
        return f"{local_part}@archive.local"
    return f"{local_part}@{normalized_domain}"


def event_reference(event: WhatIfEvent) -> WhatIfEventReference:
    return WhatIfEventReference(
        event_id=event.event_id,
        timestamp=event.timestamp,
        actor_id=event.actor_id,
        target_id=event.target_id,
        event_type=event.event_type,
        thread_id=event.thread_id,
        case_id=event.case_id,
        surface=event.surface,
        conversation_anchor=event.conversation_anchor,
        subject=event.subject,
        snippet=event.snippet,
        to_recipients=list(event.flags.to_recipients),
        cc_recipients=list(event.flags.cc_recipients),
        has_attachment_reference=event.flags.has_attachment_reference,
        is_forward=event.flags.is_forward,
        is_reply=event.flags.is_reply,
        is_escalation=event.flags.is_escalation,
    )


def thread_reason_labels(
    thread: WhatIfThreadSummary,
    scenario_id: WhatIfScenarioId,
) -> list[str]:
    del thread
    if scenario_id == "compliance_gateway":
        return ["legal", "trading"]
    if scenario_id == "escalation_firewall":
        return ["executive_escalation"]
    if scenario_id == "external_dlp":
        return ["attachment", "external_recipient"]
    return ["assignment_without_approval"]


def load_episode_snapshot(root: Path) -> dict[str, Any]:
    snapshot_path = resolve_context_snapshot_path(root)
    if not snapshot_path.exists():
        raise ValueError(f"context snapshot not found: {snapshot_path}")
    return json.loads(snapshot_path.read_text(encoding="utf-8"))


def resolve_context_snapshot_path(root: Path) -> Path:
    if root.is_file():
        return root
    return root / CONTEXT_SNAPSHOT_FILE


def load_episode_context(root: Path) -> dict[str, Any]:
    payload = load_episode_snapshot(root)
    sources = payload.get("sources", [])
    for source in sources:
        if not isinstance(source, dict):
            continue
        data = source.get("data", {})
        if isinstance(data, dict):
            return data
    raise ValueError("what-if episode is missing a supported context source")


_LEGAL = re.compile(r"\b(?:legal|compliance)\b")
_HOLD = re.compile(r"\b(?:hold|pause|stop forward|freeze)\b")
_STATUS = re.compile(
    r"\b(?:status note|short update|clean update soon"
    r"|no-attachment|no attachment|without attachment)\b"
)
_REPLY_IMMEDIATELY = re.compile(
    r"\b(?:reply immediately|respond immediately|same day|right away)\b"
)
_OWNER = re.compile(r"\b(?:owner|ownership|clarify owner)\b")
_EXEC_GATE = re.compile(r"\b(?:executive gate|route through|sign-off|approval)\b")
_ATTACHMENT_REMOVED = re.compile(
    r"\b(?:remove attachment|remove the attachment"
    r"|strip attachment|strip the attachment"
    r"|keep the attachment inside"
    r"|keep the original attachment internal)\b"
)
_EXTERNAL_REMOVED = re.compile(
    r"\b(?:remove external|remove outside recipient"
    r"|remove the outside recipient|pull the outside recipient"
    r"|internal only|keep this internal|keep it internal"
    r"|keep the issue internal|hold the outside send)\b"
)
_SEND_NOW = re.compile(
    r"\b(?:send now|send immediately|outside loop active"
    r"|widen circulation|broader loop|rapid comments"
    r"|parallel follow-up|fast turnaround)\b"
)


def intervention_tags(prompt: str) -> set[str]:
    lowered = prompt.strip().lower()
    tags: set[str] = set()
    if _LEGAL.search(lowered):
        tags.update({"legal", "compliance"})
    if _HOLD.search(lowered):
        tags.update({"hold", "pause_forward"})
    if _STATUS.search(lowered):
        tags.update({"status_only", "attachment_removed"})
    if _REPLY_IMMEDIATELY.search(lowered):
        tags.add("reply_immediately")
    if _OWNER.search(lowered):
        tags.add("clarify_owner")
    if _EXEC_GATE.search(lowered):
        tags.add("executive_gate")
    if _ATTACHMENT_REMOVED.search(lowered):
        tags.add("attachment_removed")
    if _EXTERNAL_REMOVED.search(lowered):
        tags.add("external_removed")
    if _SEND_NOW.search(lowered):
        tags.update({"send_now", "widen_loop"})
    return tags
