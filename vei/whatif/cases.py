from __future__ import annotations

import re
from collections import Counter
from typing import Any, Iterable, Sequence

from vei.context.models import (
    ContextSnapshot,
    CrmSourceData,
    GoogleSourceData,
    source_payload,
)

from ._helpers import event_reference
from .models import (
    WhatIfCaseContext,
    WhatIfCaseRecord,
    WhatIfCaseSummary,
    WhatIfEvent,
)

_CASE_TOKEN_PATTERN = re.compile(r"\b[A-Z][A-Z0-9]{1,12}-\d{1,8}\b")
_DOC_TOKEN_PATTERN = re.compile(r"\bDOC[A-Z0-9-]{2,}\b")
_DEAL_TOKEN_PATTERN = re.compile(r"\bDEAL-[A-Z0-9-]{2,}\b")


def assign_case_ids(events: Sequence[WhatIfEvent]) -> list[WhatIfEvent]:
    thread_tokens = _primary_thread_tokens(events)
    resolved: list[WhatIfEvent] = []
    for event in events:
        explicit_tokens = _event_anchor_tokens(event)
        primary_token = (
            explicit_tokens[0]
            if explicit_tokens
            else thread_tokens.get(
                event.thread_id,
                "",
            )
        )
        case_id = _case_id_from_token(primary_token, thread_id=event.thread_id)
        resolved.append(event.model_copy(update={"case_id": case_id}))
    return resolved


def build_case_summaries(events: Sequence[WhatIfEvent]) -> list[WhatIfCaseSummary]:
    buckets: dict[str, dict[str, Any]] = {}
    for event in events:
        if not event.case_id:
            continue
        bucket = buckets.setdefault(
            event.case_id,
            {
                "case_id": event.case_id,
                "title": event.subject or _title_from_case_id(event.case_id),
                "event_count": 0,
                "thread_ids": set(),
                "surfaces": set(),
                "first_timestamp": event.timestamp,
                "last_timestamp": event.timestamp,
                "anchor_tokens": set(_event_anchor_tokens(event)),
            },
        )
        bucket["event_count"] += 1
        bucket["thread_ids"].add(event.thread_id)
        bucket["surfaces"].add(event.surface or "mail")
        bucket["last_timestamp"] = event.timestamp
        if event.timestamp < bucket["first_timestamp"]:
            bucket["first_timestamp"] = event.timestamp
        if event.subject and not bucket["title"]:
            bucket["title"] = event.subject
        bucket["anchor_tokens"].update(_event_anchor_tokens(event))

    summaries = [
        WhatIfCaseSummary(
            case_id=payload["case_id"],
            title=payload["title"],
            event_count=payload["event_count"],
            thread_count=len(payload["thread_ids"]),
            surfaces=sorted(payload["surfaces"]),
            thread_ids=sorted(payload["thread_ids"]),
            first_timestamp=payload["first_timestamp"],
            last_timestamp=payload["last_timestamp"],
            anchor_tokens=sorted(payload["anchor_tokens"]),
        )
        for payload in buckets.values()
    ]
    return sorted(summaries, key=lambda item: (-item.event_count, item.case_id))


def build_case_context(
    *,
    snapshot: ContextSnapshot | None,
    events: Sequence[WhatIfEvent],
    case_id: str,
    branch_thread_id: str,
    branch_timestamp_ms: int,
    limit: int = 6,
) -> WhatIfCaseContext | None:
    if not case_id:
        return None
    related_history = [
        event_reference(event)
        for event in events
        if event.case_id == case_id
        and event.thread_id != branch_thread_id
        and event.timestamp_ms <= branch_timestamp_ms
    ]
    related_history = sorted(
        related_history,
        key=lambda item: (item.timestamp, item.event_id),
    )[-limit:]
    records = (
        _related_case_records(snapshot, case_id=case_id) if snapshot is not None else []
    )
    if not related_history and not records:
        return None
    return WhatIfCaseContext(
        case_id=case_id,
        title=_title_from_case_id(case_id),
        related_history=related_history,
        records=records,
    )


def case_context_prompt_lines(context: WhatIfCaseContext | None) -> list[str]:
    if context is None:
        return []
    lines = [
        "Related case context known by the branch date:",
    ]
    if context.related_history:
        lines.append("Cross-surface case activity:")
        for item in context.related_history[:6]:
            subject = item.subject or item.thread_id
            lines.append(
                f"- [{item.surface}] {item.timestamp}: {item.actor_id} -> {subject}"
            )
    if context.records:
        lines.append("Linked records:")
        for record in context.records[:6]:
            lines.append(
                f"- [{record.surface or record.provider}] {record.label}: {record.summary}"
            )
    if len(lines) == 1:
        return []
    return lines


def _primary_thread_tokens(events: Sequence[WhatIfEvent]) -> dict[str, str]:
    tokens_by_thread: dict[str, Counter[str]] = {}
    for event in events:
        for token in _event_anchor_tokens(event):
            tokens_by_thread.setdefault(event.thread_id, Counter())[token] += 1
    resolved: dict[str, str] = {}
    for thread_id, counts in tokens_by_thread.items():
        if not counts:
            continue
        resolved[thread_id] = sorted(
            counts.items(),
            key=lambda item: (-item[1], item[0]),
        )[0][0]
    return resolved


def _case_id_from_token(token: str, *, thread_id: str) -> str:
    normalized = token.strip().upper()
    if normalized:
        return f"case:{normalized}"
    fallback = thread_id.strip() or "history"
    return f"thread:{fallback}"


def _title_from_case_id(case_id: str) -> str:
    if ":" not in case_id:
        return case_id
    return case_id.split(":", 1)[-1]


def _event_anchor_tokens(event: WhatIfEvent) -> list[str]:
    tokens: list[str] = []
    if event.surface == "tickets":
        ticket_id = event.thread_id.split(":", 1)[-1].strip().upper()
        if ticket_id:
            tokens.append(ticket_id)
    text_parts = [
        event.thread_id,
        event.subject,
        event.snippet,
        event.actor_id,
        event.target_id,
        *event.flags.to_recipients,
        *event.flags.cc_recipients,
    ]
    tokens.extend(
        _anchor_tokens_from_text(" ".join(part for part in text_parts if part))
    )
    return _dedupe(tokens)


def _anchor_tokens_from_text(text: str) -> list[str]:
    if not text:
        return []
    tokens = [
        *[item.upper() for item in _CASE_TOKEN_PATTERN.findall(text)],
        *[item.upper() for item in _DOC_TOKEN_PATTERN.findall(text)],
        *[item.upper() for item in _DEAL_TOKEN_PATTERN.findall(text)],
    ]
    return _dedupe(tokens)


def _related_case_records(
    snapshot: ContextSnapshot,
    *,
    case_id: str,
) -> list[WhatIfCaseRecord]:
    tokens = _anchor_tokens_from_text(case_id)
    if not tokens and case_id.startswith("case:"):
        tokens = [case_id.split(":", 1)[-1].upper()]
    if not tokens:
        return []
    records: list[WhatIfCaseRecord] = []
    records.extend(_google_case_records(snapshot, tokens=tokens))
    records.extend(_crm_case_records(snapshot, provider="crm", tokens=tokens))
    records.extend(_crm_case_records(snapshot, provider="salesforce", tokens=tokens))
    return records


def _google_case_records(
    snapshot: ContextSnapshot,
    *,
    tokens: Sequence[str],
) -> list[WhatIfCaseRecord]:
    data = source_payload(snapshot.source_for("google"), GoogleSourceData)
    if data is None:
        return []
    records: list[WhatIfCaseRecord] = []
    for item in data.documents:
        if not isinstance(item, dict):
            continue
        doc_id = str(item.get("doc_id", "")).strip()
        title = str(item.get("title", "")).strip()
        summary = str(item.get("body", "") or item.get("mime_type", "")).strip()
        if not _record_matches_tokens([doc_id, title, summary], tokens=tokens):
            continue
        records.append(
            WhatIfCaseRecord(
                record_id=doc_id or title,
                provider="google",
                surface="docs",
                label=title or doc_id or "Document",
                summary=summary or "Linked document record",
                related_ids=[
                    token
                    for token in _anchor_tokens_from_text(f"{doc_id} {title} {summary}")
                    if token in tokens
                ],
            )
        )
    return records


def _crm_case_records(
    snapshot: ContextSnapshot,
    *,
    provider: str,
    tokens: Sequence[str],
) -> list[WhatIfCaseRecord]:
    data = source_payload(snapshot.source_for(provider), CrmSourceData)
    if data is None:
        return []
    records: list[WhatIfCaseRecord] = []
    for deal in data.deals:
        if not isinstance(deal, dict):
            continue
        deal_id = str(deal.get("id", deal.get("deal_id", ""))).strip()
        label = str(deal.get("name", deal.get("title", ""))).strip()
        stage = str(deal.get("stage", "")).strip()
        owner = str(deal.get("owner", "")).strip()
        if not _record_matches_tokens([deal_id, label, stage, owner], tokens=tokens):
            continue
        summary_parts = [part for part in [stage, owner] if part]
        records.append(
            WhatIfCaseRecord(
                record_id=deal_id or label,
                provider=provider,
                surface="crm",
                label=label or deal_id or "Deal",
                summary=" | ".join(summary_parts) or "Linked deal record",
                related_ids=[
                    token
                    for token in _anchor_tokens_from_text(
                        f"{deal_id} {label} {stage} {owner}"
                    )
                    if token in tokens
                ],
            )
        )
    return records


def _record_matches_tokens(fields: Iterable[str], *, tokens: Sequence[str]) -> bool:
    haystack = " ".join(item for item in fields if item).upper()
    return any(token in haystack for token in tokens)


def _dedupe(values: Sequence[str]) -> list[str]:
    seen: set[str] = set()
    ordered: list[str] = []
    for value in values:
        normalized = value.strip().upper()
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        ordered.append(normalized)
    return ordered
