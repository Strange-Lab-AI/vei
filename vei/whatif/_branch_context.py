from __future__ import annotations

from dataclasses import dataclass

from vei.context.api import ContextSnapshot

from ._branch_selection import resolve_thread_branch
from ._source_snapshot import source_snapshot_for_world
from .business_state import assess_historical_business_state
from .cases import build_case_context
from .corpus import event_reference
from .episode._replay import score_historical_tail
from .models import (
    WhatIfBusinessStateAssessment,
    WhatIfCaseContext,
    WhatIfEvent,
    WhatIfEventReference,
    WhatIfHistoricalScore,
    WhatIfPublicContext,
    WhatIfSituationContext,
    WhatIfWorld,
)
from vei.context.api import slice_public_context_to_branch
from .situations import build_situation_context


@dataclass(frozen=True)
class WhatIfBranchContext:
    thread_id: str
    thread_history: list[WhatIfEvent]
    branch_event: WhatIfEvent
    past_events: list[WhatIfEvent]
    future_events: list[WhatIfEvent]
    thread_subject: str
    source_snapshot: ContextSnapshot | None
    branch_reference: WhatIfEventReference
    public_context: WhatIfPublicContext | None
    case_context: WhatIfCaseContext | None
    situation_context: WhatIfSituationContext | None
    forecast: WhatIfHistoricalScore
    historical_business_state: WhatIfBusinessStateAssessment | None


def build_branch_context(
    world: WhatIfWorld,
    *,
    thread_id: str | None = None,
    event_id: str | None = None,
    organization_domain: str | None = None,
) -> WhatIfBranchContext:
    resolved_organization_domain = (
        (organization_domain or "").strip().lower()
        or world.summary.organization_domain
        or "archive.local"
    )
    (
        selected_thread_id,
        thread_history,
        branch_event,
        past_events,
        future_events,
        selected_thread_subject,
    ) = resolve_thread_branch(
        world,
        thread_id=thread_id,
        event_id=event_id,
    )
    source_snapshot = source_snapshot_for_world(world)
    branch_reference = event_reference(branch_event)
    branch_public_context = slice_public_context_to_branch(
        world.public_context,
        branch_timestamp=branch_event.timestamp,
    )
    case_context = build_case_context(
        snapshot=source_snapshot,
        events=world.events,
        case_id=branch_event.case_id,
        branch_thread_id=selected_thread_id,
        branch_timestamp_ms=branch_event.timestamp_ms,
    )
    situation_context = build_situation_context(
        world,
        branch_thread_id=selected_thread_id,
        branch_timestamp_ms=branch_event.timestamp_ms,
    )
    forecast = score_historical_tail(
        future_events,
        organization_domain=resolved_organization_domain,
        branch_timestamp=branch_event.timestamp,
        public_context=branch_public_context,
    )
    historical_business_state = assess_historical_business_state(
        branch_event=branch_reference,
        forecast=forecast,
        organization_domain=resolved_organization_domain,
        public_context=branch_public_context,
    )
    return WhatIfBranchContext(
        thread_id=selected_thread_id,
        thread_history=thread_history,
        branch_event=branch_event,
        past_events=past_events,
        future_events=future_events,
        thread_subject=selected_thread_subject,
        source_snapshot=source_snapshot,
        branch_reference=branch_reference,
        public_context=branch_public_context,
        case_context=case_context,
        situation_context=situation_context,
        forecast=forecast,
        historical_business_state=historical_business_state,
    )


__all__ = ["WhatIfBranchContext", "build_branch_context"]
