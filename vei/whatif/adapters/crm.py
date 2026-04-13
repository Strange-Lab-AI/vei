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


def build_crm_events(
    *,
    snapshot: ContextSnapshot,
    provider: str,
    organization_domain: str,
    include_content: bool,
) -> list[WhatIfEvent]:
    source = snapshot.source_for(provider)
    if source is None or not isinstance(source.data, dict):
        return []

    companies = source.data.get("companies", [])
    contacts = source.data.get("contacts", [])
    deals = source.data.get("deals", [])
    if not isinstance(companies, list):
        companies = []
    if not isinstance(contacts, list):
        contacts = []
    if not isinstance(deals, list):
        deals = []

    company_lookup = {
        str(company.get("id") or "").strip(): company
        for company in companies
        if isinstance(company, dict) and str(company.get("id") or "").strip()
    }
    contact_lookup = {
        str(contact.get("id") or "").strip(): contact
        for contact in contacts
        if isinstance(contact, dict) and str(contact.get("id") or "").strip()
    }

    events: list[WhatIfEvent] = []
    for company_index, company in enumerate(companies):
        if not isinstance(company, dict):
            continue
        company_id = str(company.get("id") or "").strip()
        if not company_id:
            continue
        name = str(company.get("name") or company_id).strip()
        created_at = company.get("created_ms") or company.get("created")
        actor_id = _normalized_actor_id(
            company.get("owner") or name,
            organization_domain=organization_domain,
            fallback=f"{provider}-company-{company_index + 1}",
        )
        summary = f"Company record created for {name}"
        events.append(
            WhatIfEvent(
                event_id=_company_history_event_id(
                    provider=provider,
                    raw_event_id=f"{company_id}:company",
                    fallback_parts=(company_id, "company"),
                ),
                timestamp=timestamp_to_text(created_at),
                timestamp_ms=_history_timestamp_ms(
                    created_at,
                    fallback_index=(company_index + 1) * 1000,
                ),
                actor_id=actor_id,
                event_type="message",
                thread_id=_company_history_thread_id(provider, company_id),
                surface="crm",
                conversation_anchor=company_id,
                subject=name,
                snippet=summary,
                flags=_crm_flags(
                    title=name,
                    text=summary,
                    related_ids=[company_id],
                    source=provider,
                ),
            )
        )

    for contact_index, contact in enumerate(contacts):
        if not isinstance(contact, dict):
            continue
        contact_id = str(contact.get("id") or "").strip()
        if not contact_id:
            continue
        company_id = str(contact.get("company_id") or "").strip()
        if not company_id:
            continue
        company = company_lookup.get(company_id, {})
        company_name = str(company.get("name") or company_id).strip()
        email = str(contact.get("email") or "").strip()
        contact_name = " ".join(
            part
            for part in (
                str(contact.get("first_name") or "").strip(),
                str(contact.get("last_name") or "").strip(),
            )
            if part
        ).strip()
        actor_id = _normalized_actor_id(
            email or contact_name,
            organization_domain=organization_domain,
            fallback=f"{provider}-contact-{contact_index + 1}",
        )
        created_at = contact.get("created_ms") or contact.get("created")
        summary = f"Linked {contact_name or email or contact_id} to {company_name}"
        events.append(
            WhatIfEvent(
                event_id=_company_history_event_id(
                    provider=provider,
                    raw_event_id=f"{contact_id}:link",
                    fallback_parts=(contact_id, company_id, "link"),
                ),
                timestamp=timestamp_to_text(created_at),
                timestamp_ms=_history_timestamp_ms(
                    created_at,
                    fallback_index=(contact_index + 1) * 1000 + 100,
                ),
                actor_id=actor_id,
                event_type="assignment",
                thread_id=_company_history_thread_id(provider, company_id),
                surface="crm",
                conversation_anchor=company_id,
                subject=company_name,
                snippet=summary,
                flags=_crm_flags(
                    title=company_name,
                    text=summary,
                    related_ids=[company_id, contact_id],
                    source=provider,
                ),
            )
        )

    for deal_index, deal in enumerate(deals):
        if not isinstance(deal, dict):
            continue
        deal_id = str(deal.get("id") or "").strip()
        if not deal_id:
            continue
        events.extend(
            _deal_events(
                deal=deal,
                deal_index=deal_index,
                provider=provider,
                organization_domain=organization_domain,
                company_lookup=company_lookup,
                contact_lookup=contact_lookup,
                include_content=include_content,
            )
        )
    return events


def _deal_events(
    *,
    deal: dict[str, Any],
    deal_index: int,
    provider: str,
    organization_domain: str,
    company_lookup: dict[str, dict[str, Any]],
    contact_lookup: dict[str, dict[str, Any]],
    include_content: bool,
) -> list[WhatIfEvent]:
    deal_id = str(deal.get("id") or "").strip()
    title = str(deal.get("name") or deal_id).strip()
    stage = str(deal.get("stage") or "open").strip()
    owner = _normalized_actor_id(
        deal.get("owner"),
        organization_domain=organization_domain,
        fallback=f"{provider}-owner-{deal_index + 1}",
    )
    company_id = str(deal.get("company_id") or "").strip()
    contact_id = str(deal.get("contact_id") or "").strip()
    company_name = str(
        company_lookup.get(company_id, {}).get("name") or company_id or "Account"
    ).strip()
    contact = contact_lookup.get(contact_id, {})
    contact_label = (
        " ".join(
            part
            for part in (
                str(contact.get("first_name") or "").strip(),
                str(contact.get("last_name") or "").strip(),
            )
            if part
        ).strip()
        or str(contact.get("email") or contact_id).strip()
    )
    thread_key = deal_id or company_id or title
    thread_id = _company_history_thread_id(provider, thread_key)
    participants = [item for item in (company_id, contact_id) if item]

    events: list[WhatIfEvent] = []
    created_at = deal.get("created_ms") or deal.get("created")
    created_summary = f"Created deal {title} in stage {stage}"
    if company_name:
        created_summary = f"{created_summary} for {company_name}"
    events.append(
        WhatIfEvent(
            event_id=_company_history_event_id(
                provider=provider,
                raw_event_id=f"{deal_id}:create",
                fallback_parts=(deal_id, "create"),
            ),
            timestamp=timestamp_to_text(created_at),
            timestamp_ms=_history_timestamp_ms(
                created_at,
                fallback_index=(deal_index + 1) * 10_000,
            ),
            actor_id=owner,
            event_type="message",
            thread_id=thread_id,
            surface="crm",
            conversation_anchor=deal_id,
            subject=title,
            snippet=_crm_snippet(created_summary, include_content=include_content),
            flags=_crm_flags(
                title=title,
                text=created_summary,
                related_ids=participants,
                source=provider,
            ),
        )
    )

    if contact_label:
        link_summary = f"Linked {contact_label} to deal {title}"
        events.append(
            WhatIfEvent(
                event_id=_company_history_event_id(
                    provider=provider,
                    raw_event_id=f"{deal_id}:contact",
                    fallback_parts=(deal_id, "contact"),
                ),
                timestamp=timestamp_to_text(created_at),
                timestamp_ms=_history_timestamp_ms(
                    created_at,
                    fallback_index=(deal_index + 1) * 10_000 + 1,
                ),
                actor_id=owner,
                event_type="assignment",
                thread_id=thread_id,
                surface="crm",
                conversation_anchor=deal_id,
                subject=title,
                snippet=link_summary,
                flags=_crm_flags(
                    title=title,
                    text=link_summary,
                    related_ids=participants,
                    source=provider,
                ),
            )
        )

    updated_at = deal.get("updated_ms") or deal.get("updated")
    latest_summary = f"Deal {title} now in stage {stage}"
    events.append(
        WhatIfEvent(
            event_id=_company_history_event_id(
                provider=provider,
                raw_event_id=f"{deal_id}:stage",
                fallback_parts=(deal_id, "stage"),
            ),
            timestamp=timestamp_to_text(updated_at or created_at),
            timestamp_ms=_history_timestamp_ms(
                updated_at or created_at,
                fallback_index=(deal_index + 1) * 10_000 + 2,
            ),
            actor_id=owner,
            event_type="message",
            thread_id=thread_id,
            surface="crm",
            conversation_anchor=deal_id,
            subject=title,
            snippet=latest_summary,
            flags=_crm_flags(
                title=title,
                text=latest_summary,
                related_ids=participants,
                source=provider,
            ),
        )
    )

    for history_index, row in enumerate(_history_rows(deal)):
        events.append(
            _history_event(
                deal=deal,
                row=row,
                history_index=history_index,
                provider=provider,
                organization_domain=organization_domain,
                deal_index=deal_index,
                thread_id=thread_id,
                title=title,
                default_actor=owner,
                participants=participants,
            )
        )

    closed_at = deal.get("closed_ms") or deal.get("closed_at") or deal.get("close_date")
    if closed_at:
        outcome = _closed_outcome_text(stage=stage, deal=deal)
        events.append(
            WhatIfEvent(
                event_id=_company_history_event_id(
                    provider=provider,
                    raw_event_id=f"{deal_id}:closed",
                    fallback_parts=(deal_id, "closed"),
                ),
                timestamp=timestamp_to_text(closed_at),
                timestamp_ms=_history_timestamp_ms(
                    closed_at,
                    fallback_index=(deal_index + 1) * 10_000 + 9_000,
                ),
                actor_id=owner,
                event_type="approval" if "won" in outcome.lower() else "message",
                thread_id=thread_id,
                surface="crm",
                conversation_anchor=deal_id,
                subject=title,
                snippet=outcome,
                flags=_crm_flags(
                    title=title,
                    text=outcome,
                    related_ids=participants,
                    source=provider,
                ),
            )
        )
    return events


def _history_event(
    *,
    deal: dict[str, Any],
    row: dict[str, Any],
    history_index: int,
    provider: str,
    organization_domain: str,
    deal_index: int,
    thread_id: str,
    title: str,
    default_actor: str,
    participants: list[str],
) -> WhatIfEvent:
    field_name = str(row.get("field") or row.get("property") or "").strip().lower()
    before = str(row.get("from") or row.get("old_value") or "").strip()
    after = str(row.get("to") or row.get("new_value") or "").strip()
    timestamp = row.get("timestamp") or row.get("timestamp_ms") or row.get("changed_at")
    actor = _normalized_actor_id(
        row.get("changed_by") or row.get("owner") or default_actor,
        organization_domain=organization_domain,
        fallback=default_actor,
    )
    event_type = "message"
    summary = f"Updated {title}"

    if field_name in {"stage", "dealstage"}:
        event_type = "message"
        summary = f"Moved {title} from {before or 'unknown'} to {after or 'unknown'}"
    elif field_name in {"owner", "owner_id"}:
        event_type = "assignment"
        summary = (
            f"Reassigned {title} from {before or 'unknown'} to {after or 'unknown'}"
        )
    elif field_name in {"amount", "amount_usd"}:
        summary = (
            f"Changed {title} amount from {before or 'unknown'} to {after or 'unknown'}"
        )
    elif field_name in {"close_date", "closedate"}:
        summary = f"Moved {title} close date from {before or 'unknown'} to {after or 'unknown'}"
    elif field_name in {"status", "is_closed"}:
        summary = (
            f"Updated {title} status from {before or 'unknown'} to {after or 'unknown'}"
        )
    else:
        summary = (
            f"Changed {field_name or 'deal'} on {title} "
            f"from {before or 'unknown'} to {after or 'unknown'}"
        )

    return WhatIfEvent(
        event_id=_company_history_event_id(
            provider=provider,
            raw_event_id=str(row.get("id") or ""),
            fallback_parts=(
                str(deal.get("id") or ""),
                field_name or "history",
                str(history_index + 1),
            ),
        ),
        timestamp=timestamp_to_text(timestamp),
        timestamp_ms=_history_timestamp_ms(
            timestamp,
            fallback_index=(deal_index + 1) * 10_000 + 100 + history_index,
        ),
        actor_id=actor,
        event_type=event_type,
        thread_id=thread_id,
        surface="crm",
        conversation_anchor=str(deal.get("id") or ""),
        subject=title,
        snippet=summary,
        flags=_crm_flags(
            title=title,
            text=summary,
            related_ids=participants,
            source=provider,
            is_assignment=event_type == "assignment",
        ),
    )


def _history_rows(deal: dict[str, Any]) -> list[dict[str, Any]]:
    history = deal.get("history", [])
    if not isinstance(history, list):
        return []
    return [row for row in history if isinstance(row, dict)]


def _closed_outcome_text(*, stage: str, deal: dict[str, Any]) -> str:
    stage_text = stage.strip().lower()
    if "won" in stage_text:
        return f"Closed won: {deal.get('name') or deal.get('id')}"
    if "lost" in stage_text or "closed" in stage_text:
        return f"Closed {stage_text}: {deal.get('name') or deal.get('id')}"
    return f"Recorded close outcome for {deal.get('name') or deal.get('id')}"


def _crm_snippet(text: str, *, include_content: bool) -> str:
    if include_content:
        return text
    return _truncate_snippet(text)


def _crm_flags(
    *,
    title: str,
    text: str,
    related_ids: list[str],
    source: str,
    is_assignment: bool = False,
) -> WhatIfArtifactFlags:
    joined = " ".join([title, text])
    return WhatIfArtifactFlags(
        consult_legal_specialist=_contains_keyword(
            joined,
            ("legal", "contract", "msa", "counsel", "procurement"),
        ),
        consult_trading_specialist=_contains_keyword(
            joined,
            ("pricing", "quote", "trading", "desk"),
        ),
        is_escalation=_contains_keyword(
            joined,
            ("urgent", "escalate", "executive", "blocker"),
        ),
        is_reply=False,
        to_count=len(related_ids) or 1,
        to_recipients=related_ids or [title],
        subject=title,
        norm_subject=title.lower().strip(),
        source=source,
    )
