from __future__ import annotations

import re

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
