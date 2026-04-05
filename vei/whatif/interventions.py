from __future__ import annotations


def intervention_tags(prompt: str) -> set[str]:
    lowered = prompt.strip().lower()
    tags: set[str] = set()
    if any(token in lowered for token in ("legal", "compliance")):
        tags.update({"legal", "compliance"})
    if any(token in lowered for token in ("hold", "pause", "stop forward", "freeze")):
        tags.update({"hold", "pause_forward"})
    if any(
        token in lowered
        for token in (
            "reply immediately",
            "respond immediately",
            "same day",
            "right away",
        )
    ):
        tags.add("reply_immediately")
    if any(token in lowered for token in ("owner", "ownership", "clarify owner")):
        tags.add("clarify_owner")
    if any(
        token in lowered
        for token in ("executive gate", "route through", "sign-off", "approval")
    ):
        tags.add("executive_gate")
    if any(token in lowered for token in ("remove attachment", "strip attachment")):
        tags.add("attachment_removed")
    if any(
        token in lowered
        for token in (
            "remove external",
            "pull the outside recipient",
            "internal only",
            "outside recipient",
        )
    ):
        tags.add("external_removed")
    return tags
