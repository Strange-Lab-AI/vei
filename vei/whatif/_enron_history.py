from __future__ import annotations

import json
import math
import re
from datetime import UTC, datetime
from functools import lru_cache
from pathlib import Path
from typing import Any, Sequence

from pydantic import BaseModel, Field

from vei.context.api import (
    CanonicalHistoryIndexRow,
    WhatIfPublicContext,
    slice_public_context_to_branch,
)
from vei.events.api import EventDomain, InternalExternal

from .models import WhatIfArtifactFlags, WhatIfEvent

_NON_ALNUM_PATTERN = re.compile(r"[^a-z0-9]+")
_ENRON_RECORD_HISTORY_PATH = (
    Path(__file__).resolve().parent
    / "fixtures"
    / "enron_record_history"
    / "enron_record_history_v1.json"
)


class EnronRecordHistoryRow(BaseModel):
    record_id: str
    timestamp: str
    source_family: str
    surface: str
    category: str
    headline: str
    summary: str = ""
    source_url: str = ""
    source_label: str = ""


class EnronRecordHistory(BaseModel):
    version: str = "1"
    pack_name: str = "enron_record_history"
    organization_name: str = "Enron Corporation"
    organization_domain: str = "enron.com"
    prepared_at: str = ""
    records: list[EnronRecordHistoryRow] = Field(default_factory=list)


class EnronPublicTimelineItem(BaseModel):
    event: WhatIfEvent
    provider: str
    source_family: str
    timestamp_quality: str = "exact"
    provider_object_refs: list[str] = Field(default_factory=list)
    metadata: dict[str, Any] = Field(default_factory=dict)


def build_enron_branch_history(
    *,
    public_context: WhatIfPublicContext | None,
    branch_event: WhatIfEvent,
    past_events: Sequence[WhatIfEvent],
    min_history_events: int = 30,
    max_history_events: int = 36,
) -> list[WhatIfEvent]:
    trimmed_past_events = list(past_events[-12:])
    if len(trimmed_past_events) >= max_history_events:
        return trimmed_past_events[-max_history_events:]

    public_items = build_enron_public_timeline_items(
        public_context=public_context,
        branch_timestamp=branch_event.timestamp,
        current_history_count=len(trimmed_past_events),
        min_history_events=min_history_events,
        max_public_events=max_history_events,
    )
    public_events = [item.event for item in public_items]
    public_slots = max(0, max_history_events - len(trimmed_past_events))
    selected_public_events = public_events[-public_slots:]
    history = list(trimmed_past_events) + list(selected_public_events)
    history.sort(key=lambda item: (item.timestamp_ms, item.event_id))
    return history[-max_history_events:]


def build_enron_public_canonical_rows(
    *,
    public_context: WhatIfPublicContext | None,
    branch_timestamp: str,
    organization_domain: str,
    current_history_count: int = 0,
    min_history_events: int = 30,
    max_public_events: int = 36,
) -> list[CanonicalHistoryIndexRow]:
    items = build_enron_public_timeline_items(
        public_context=public_context,
        branch_timestamp=branch_timestamp,
        current_history_count=current_history_count,
        min_history_events=min_history_events,
        max_public_events=max_public_events,
    )
    return [
        _canonical_row_from_item(
            item=item,
            organization_domain=organization_domain,
        )
        for item in items
    ]


def build_enron_canonical_rows(
    *,
    public_context: WhatIfPublicContext | None,
    branch_event: WhatIfEvent,
    past_events: Sequence[WhatIfEvent],
    organization_domain: str,
    min_history_events: int = 30,
    max_history_events: int = 36,
) -> list[CanonicalHistoryIndexRow]:
    history_events = build_enron_branch_history(
        public_context=public_context,
        branch_event=branch_event,
        past_events=past_events,
        min_history_events=min_history_events,
        max_history_events=max_history_events,
    )
    public_items = build_enron_public_timeline_items(
        public_context=public_context,
        branch_timestamp=branch_event.timestamp,
        current_history_count=len(list(past_events[-12:])),
        min_history_events=min_history_events,
        max_public_events=max_history_events,
    )
    rows_by_event_id = {
        item.event.event_id: _canonical_row_from_item(
            item=item,
            organization_domain=organization_domain,
        )
        for item in public_items
    }
    for event in list(history_events) + [branch_event]:
        if event.event_id in rows_by_event_id:
            continue
        rows_by_event_id[event.event_id] = _canonical_row_from_event(
            event=event,
            organization_domain=organization_domain,
        )
    return sorted(
        rows_by_event_id.values(),
        key=lambda row: (row.ts_ms, row.event_id),
    )


def build_enron_public_timeline_items(
    *,
    public_context: WhatIfPublicContext | None,
    branch_timestamp: str,
    current_history_count: int = 0,
    min_history_events: int = 30,
    max_public_events: int = 36,
) -> list[EnronPublicTimelineItem]:
    if public_context is None:
        return []

    branch_context = slice_public_context_to_branch(
        public_context,
        branch_timestamp=branch_timestamp,
    )
    if branch_context is None:
        return []

    items = _record_history_items(branch_context)
    items.extend(_financial_snapshot_items(branch_context))
    items.extend(_public_news_items(branch_context))
    items.extend(_credit_history_items(branch_context))
    items.extend(_ferc_history_items(branch_context))

    non_stock_count = len(items)
    stock_target = _stock_target_count(
        branch_context=branch_context,
        current_history_count=current_history_count,
        non_stock_count=non_stock_count,
        min_history_events=min_history_events,
    )
    if stock_target > 0:
        items.extend(_stock_history_items(branch_context, target_count=stock_target))

    deduped_items: dict[str, EnronPublicTimelineItem] = {}
    for item in items:
        deduped_items[item.event.event_id] = item
    ordered = sorted(
        deduped_items.values(),
        key=lambda item: (item.event.timestamp_ms, item.event.event_id),
    )
    return ordered[-max_public_events:]


@lru_cache(maxsize=1)
def load_enron_record_history() -> EnronRecordHistory:
    payload = json.loads(_ENRON_RECORD_HISTORY_PATH.read_text(encoding="utf-8"))
    return EnronRecordHistory.model_validate(payload)


def _record_history_items(
    public_context: WhatIfPublicContext,
) -> list[EnronPublicTimelineItem]:
    branch_ts_ms = _timestamp_ms(public_context.branch_timestamp)
    items: list[EnronPublicTimelineItem] = []
    for record in load_enron_record_history().records:
        record_ts_ms = _timestamp_ms(record.timestamp)
        if record_ts_ms <= 0 or record_ts_ms > branch_ts_ms:
            continue
        items.append(
            EnronPublicTimelineItem(
                event=_build_event(
                    event_id=record.record_id,
                    timestamp=record.timestamp,
                    actor_id=_actor_id_for_family(record.source_family),
                    target_id="enron@enron.com",
                    event_type=record.category,
                    thread_id=f"public:{record.source_family}",
                    case_id=f"enron-public-{record.source_family}",
                    surface=record.surface,
                    subject=record.headline,
                    snippet=record.summary,
                ),
                provider="enron_record_history",
                source_family=record.source_family,
                provider_object_refs=[record.record_id],
                metadata={
                    "source_family": record.source_family,
                    "source_label": record.source_label,
                    "source_url": record.source_url,
                    "category": record.category,
                },
            )
        )
    return items


def _financial_snapshot_items(
    public_context: WhatIfPublicContext,
) -> list[EnronPublicTimelineItem]:
    items: list[EnronPublicTimelineItem] = []
    for snapshot in public_context.financial_snapshots:
        source_ids = list(snapshot.source_ids)
        item_id = snapshot.snapshot_id or f"financial:{snapshot.as_of}:{snapshot.kind}"
        items.append(
            EnronPublicTimelineItem(
                event=_build_event(
                    event_id=item_id,
                    timestamp=snapshot.as_of,
                    actor_id="investor_relations@enron.com",
                    target_id="market@public.enron.example",
                    event_type="financial_snapshot",
                    thread_id="public:financial",
                    case_id="enron-public-financial",
                    surface="governance",
                    subject=snapshot.label,
                    snippet=snapshot.summary,
                ),
                provider="enron_public_context",
                source_family="financial",
                provider_object_refs=source_ids or [item_id],
                metadata={
                    "source_family": "financial",
                    "source_label": snapshot.kind,
                    "source_ids": source_ids,
                },
            )
        )
    return items


def _public_news_items(
    public_context: WhatIfPublicContext,
) -> list[EnronPublicTimelineItem]:
    items: list[EnronPublicTimelineItem] = []
    for event in public_context.public_news_events:
        source_ids = list(event.source_ids)
        items.append(
            EnronPublicTimelineItem(
                event=_build_event(
                    event_id=event.event_id,
                    timestamp=event.timestamp,
                    actor_id="news_wire@public.enron.example",
                    target_id="enron@enron.com",
                    event_type=event.category or "public_news",
                    thread_id="public:news",
                    case_id="enron-public-news",
                    surface="governance",
                    subject=event.headline,
                    snippet=event.summary,
                ),
                provider="enron_public_context",
                source_family="news",
                provider_object_refs=source_ids or [event.event_id],
                metadata={
                    "source_family": "news",
                    "source_label": "Public news event",
                    "source_ids": source_ids,
                    "internally_known_date": event.internally_known_date or "",
                },
            )
        )
    return items


def _stock_history_items(
    public_context: WhatIfPublicContext,
    *,
    target_count: int,
) -> list[EnronPublicTimelineItem]:
    stock_rows = list(public_context.stock_history)
    if target_count <= 0 or not stock_rows:
        return []

    sampled_rows = _sample_rows(stock_rows, target_count=target_count)
    items: list[EnronPublicTimelineItem] = []
    for row in sampled_rows:
        event_id = f"enron_stock_{row.as_of[:10]}"
        items.append(
            EnronPublicTimelineItem(
                event=_build_event(
                    event_id=event_id,
                    timestamp=row.as_of,
                    actor_id="market_observer@public.enron.example",
                    target_id="enron@enron.com",
                    event_type="stock_close",
                    thread_id="market:stock",
                    case_id="enron-market-stock",
                    surface="market",
                    subject=f"Enron stock closes at ${row.close:.2f}",
                    snippet=row.summary or row.label or "Daily Enron stock close.",
                ),
                provider="enron_stock_history",
                source_family="market",
                provider_object_refs=[event_id, *list(row.source_ids)],
                metadata={
                    "source_family": "market",
                    "source_label": row.label or "Daily trading close",
                    "close": row.close,
                    "open": row.open,
                    "high": row.high,
                    "low": row.low,
                    "volume": row.volume,
                    "source_ids": list(row.source_ids),
                },
            )
        )
    return items


def _credit_history_items(
    public_context: WhatIfPublicContext,
) -> list[EnronPublicTimelineItem]:
    items: list[EnronPublicTimelineItem] = []
    for event in public_context.credit_history:
        source_ids = list(event.source_ids)
        items.append(
            EnronPublicTimelineItem(
                event=_build_event(
                    event_id=event.event_id,
                    timestamp=event.as_of,
                    actor_id="ratings_observer@public.enron.example",
                    target_id="enron@enron.com",
                    event_type=event.category or "credit_action",
                    thread_id="market:credit",
                    case_id="enron-market-credit",
                    surface="market",
                    subject=event.headline,
                    snippet=event.summary,
                ),
                provider="enron_credit_history",
                source_family="credit",
                provider_object_refs=source_ids or [event.event_id],
                metadata={
                    "source_family": "credit",
                    "source_label": event.agency,
                    "source_ids": source_ids,
                    "from_rating": event.from_rating,
                    "to_rating": event.to_rating,
                    "outlook": event.outlook,
                    "watch_status": event.watch_status,
                },
            )
        )
    return items


def _ferc_history_items(
    public_context: WhatIfPublicContext,
) -> list[EnronPublicTimelineItem]:
    items: list[EnronPublicTimelineItem] = []
    for event in public_context.ferc_history:
        source_ids = list(event.source_ids)
        items.append(
            EnronPublicTimelineItem(
                event=_build_event(
                    event_id=event.event_id,
                    timestamp=event.timestamp,
                    actor_id="regulator@public.enron.example",
                    target_id="enron@enron.com",
                    event_type=event.category or "regulatory_event",
                    thread_id="public:regulatory",
                    case_id="enron-public-regulatory",
                    surface="governance",
                    subject=event.headline,
                    snippet=event.summary,
                ),
                provider="enron_ferc_history",
                source_family="regulatory",
                provider_object_refs=source_ids or [event.event_id],
                metadata={
                    "source_family": "regulatory",
                    "source_label": event.agency,
                    "source_ids": source_ids,
                },
            )
        )
    return items


def _stock_target_count(
    *,
    branch_context: WhatIfPublicContext,
    current_history_count: int,
    non_stock_count: int,
    min_history_events: int,
) -> int:
    if not branch_context.stock_history:
        return 0

    current_market_count = len(branch_context.credit_history)
    needed = max(0, min_history_events - current_history_count - non_stock_count)
    minimum_market_rows = 0
    if current_market_count == 0:
        minimum_market_rows = min(4, len(branch_context.stock_history))
    target = max(minimum_market_rows, needed)
    return min(len(branch_context.stock_history), min(30, target))


def _sample_rows(rows: Sequence[Any], *, target_count: int) -> list[Any]:
    if target_count <= 0:
        return []
    if len(rows) <= target_count:
        return list(rows)
    if target_count == 1:
        return [rows[-1]]

    last_index = len(rows) - 1
    indexes = {
        min(last_index, max(0, int(round(step * last_index / (target_count - 1)))))
        for step in range(target_count)
    }
    return [rows[index] for index in sorted(indexes)]


def _build_event(
    *,
    event_id: str,
    timestamp: str,
    actor_id: str,
    target_id: str,
    event_type: str,
    thread_id: str,
    case_id: str,
    surface: str,
    subject: str,
    snippet: str,
) -> WhatIfEvent:
    return WhatIfEvent(
        event_id=event_id,
        timestamp=timestamp,
        timestamp_ms=_timestamp_ms(timestamp),
        actor_id=actor_id,
        target_id=target_id,
        event_type=event_type,
        thread_id=thread_id,
        case_id=case_id,
        surface=surface,
        subject=subject,
        snippet=snippet,
        flags=WhatIfArtifactFlags(
            subject=subject,
            norm_subject=_normalize_subject(subject),
            source=surface,
        ),
    )


def _canonical_row_from_item(
    *,
    item: EnronPublicTimelineItem,
    organization_domain: str,
) -> CanonicalHistoryIndexRow:
    subject = item.event.subject or item.event.thread_id
    snippet = item.event.snippet or subject
    surface = item.event.surface or "governance"
    return CanonicalHistoryIndexRow(
        event_id=item.event.event_id,
        timestamp=item.event.timestamp,
        ts_ms=item.event.timestamp_ms,
        timestamp_quality=item.timestamp_quality,  # type: ignore[arg-type]
        surface=surface,
        provider=item.provider,
        kind=f"{surface}.{item.event.event_type}",
        domain=_domain_for_surface(surface).value,
        case_id=item.event.case_id,
        thread_ref=item.event.thread_id,
        conversation_anchor=item.event.conversation_anchor,
        actor_id=item.event.actor_id,
        target_id=item.event.target_id,
        participant_ids=[],
        subject=subject,
        normalized_subject=_normalize_subject(subject),
        snippet=snippet,
        search_terms=_search_terms(subject, snippet, item.source_family),
        provider_object_refs=list(item.provider_object_refs),
        stitch_confidence=1.0,
        stitch_basis="public_record",
        internal_external=InternalExternal.EXTERNAL.value,
        metadata={
            "organization_domain": organization_domain,
            "source_family": item.source_family,
            **item.metadata,
        },
    )


def _canonical_row_from_event(
    *,
    event: WhatIfEvent,
    organization_domain: str,
) -> CanonicalHistoryIndexRow:
    subject = event.subject or event.thread_id
    snippet = event.snippet or subject
    surface = event.surface or "mail"
    provider = _provider_for_surface(surface)
    recipients = list(event.flags.to_recipients) + list(event.flags.cc_recipients)
    return CanonicalHistoryIndexRow(
        event_id=event.event_id,
        timestamp=event.timestamp,
        ts_ms=event.timestamp_ms,
        timestamp_quality="exact",
        surface=surface,
        provider=provider,
        kind=f"{surface}.{event.event_type}",
        domain=_domain_for_surface(surface).value,
        case_id=event.case_id,
        thread_ref=event.thread_id,
        conversation_anchor=event.conversation_anchor,
        actor_id=event.actor_id,
        target_id=event.target_id,
        participant_ids=[participant for participant in recipients if participant],
        subject=subject,
        normalized_subject=_normalize_subject(subject),
        snippet=snippet,
        search_terms=_search_terms(subject, snippet, surface),
        provider_object_refs=[event.event_id],
        stitch_confidence=1.0,
        stitch_basis="branch_history",
        internal_external=_internal_external_value(
            organization_domain=organization_domain,
            actor_id=event.actor_id,
            recipients=recipients,
        ),
        metadata={
            "organization_domain": organization_domain,
            "source_family": "mail" if provider == "mail_archive" else surface,
            "event_type": event.event_type,
        },
    )


def _domain_for_surface(surface: str) -> EventDomain:
    normalized = str(surface or "").strip().lower()
    if normalized == "market":
        return EventDomain.OBS_GRAPH
    if normalized == "docs":
        return EventDomain.DOC_GRAPH
    if normalized == "governance":
        return EventDomain.GOVERNANCE
    return EventDomain.INTERNAL


def _provider_for_surface(surface: str) -> str:
    normalized = str(surface or "").strip().lower()
    if normalized == "mail":
        return "mail_archive"
    if normalized == "market":
        return "enron_stock_history"
    if normalized == "governance":
        return "enron_public_context"
    if normalized == "docs":
        return "enron_record_history"
    return "enron_public_context"


def _actor_id_for_family(source_family: str) -> str:
    normalized = str(source_family or "").strip().lower()
    if normalized == "disclosure":
        return "investor_relations@enron.com"
    if normalized == "filing":
        return "sec_record@public.enron.example"
    if normalized == "hearing":
        return "hearing_record@public.enron.example"
    if normalized == "exhibit":
        return "public_exhibit@public.enron.example"
    if normalized == "governance":
        return "governance_record@public.enron.example"
    return "public_record@public.enron.example"


def _normalize_subject(value: str) -> str:
    lowered = value.strip().lower()
    if not lowered:
        return ""
    return _NON_ALNUM_PATTERN.sub(" ", lowered).strip()


def _search_terms(*parts: str) -> list[str]:
    terms: list[str] = []
    seen: set[str] = set()
    for part in parts:
        normalized = _normalize_subject(part)
        if not normalized:
            continue
        for token in normalized.split():
            if len(token) < 3:
                continue
            if token in seen:
                continue
            seen.add(token)
            terms.append(token)
    return terms[:16]


def _internal_external_value(
    *,
    organization_domain: str,
    actor_id: str,
    recipients: Sequence[str],
) -> str:
    domain = str(organization_domain or "").strip().lower()
    addresses = [actor_id, *recipients]
    if not domain or not addresses:
        return InternalExternal.UNKNOWN.value
    if any(
        "@" in address and not address.strip().lower().endswith(f"@{domain}")
        for address in addresses
        if address
    ):
        return InternalExternal.EXTERNAL.value
    return InternalExternal.INTERNAL.value


def _timestamp_ms(value: str) -> int:
    text = str(value or "").strip()
    if not text:
        return 0
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return 0
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return math.floor(parsed.timestamp() * 1000)


__all__ = [
    "EnronPublicTimelineItem",
    "build_enron_branch_history",
    "build_enron_canonical_rows",
    "build_enron_public_canonical_rows",
    "build_enron_public_timeline_items",
    "load_enron_record_history",
]
