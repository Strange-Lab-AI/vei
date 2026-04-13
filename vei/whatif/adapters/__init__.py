from __future__ import annotations

from vei.context.models import ContextSnapshot

from ..models import WhatIfEvent
from .chat import build_chat_events
from .jira import build_jira_events
from .mail import build_mail_events


def build_company_history_events(
    *,
    snapshot: ContextSnapshot,
    organization_domain: str,
    include_content: bool,
) -> list[WhatIfEvent]:
    events: list[WhatIfEvent] = []
    events.extend(
        build_mail_events(
            snapshot=snapshot,
            organization_domain=organization_domain,
            include_content=include_content,
        )
    )
    events.extend(
        build_chat_events(
            snapshot=snapshot,
            provider="slack",
            organization_domain=organization_domain,
            include_content=include_content,
        )
    )
    events.extend(
        build_chat_events(
            snapshot=snapshot,
            provider="teams",
            organization_domain=organization_domain,
            include_content=include_content,
        )
    )
    events.extend(
        build_jira_events(
            snapshot=snapshot,
            organization_domain=organization_domain,
            include_content=include_content,
        )
    )
    return events


__all__ = [
    "build_chat_events",
    "build_company_history_events",
    "build_jira_events",
    "build_mail_events",
]
