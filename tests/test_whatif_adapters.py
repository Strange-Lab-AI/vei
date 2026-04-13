from __future__ import annotations

from vei.context.models import ContextSnapshot, ContextSourceResult
from vei.whatif.adapters.crm import build_crm_events
from vei.whatif.adapters.docs import build_docs_events


def test_build_crm_events_emits_grouped_deal_timeline() -> None:
    snapshot = ContextSnapshot(
        organization_name="Acme Cloud",
        organization_domain="acme.example.com",
        captured_at="2026-03-01T10:00:00Z",
        sources=[
            ContextSourceResult(
                provider="crm",
                captured_at="2026-03-01T10:00:00Z",
                status="ok",
                record_counts={"companies": 1, "contacts": 1, "deals": 1},
                data={
                    "companies": [
                        {
                            "id": "acct-1",
                            "name": "Acme Buyer",
                            "created_ms": 1_772_329_200_000,
                        }
                    ],
                    "contacts": [
                        {
                            "id": "contact-1",
                            "email": "buyer@acmebuyer.example.com",
                            "first_name": "Buyer",
                            "last_name": "One",
                            "company_id": "acct-1",
                            "created_ms": 1_772_329_260_000,
                        }
                    ],
                    "deals": [
                        {
                            "id": "deal-1",
                            "name": "Acme expansion",
                            "stage": "closed_won",
                            "owner": "maya@acme.example.com",
                            "company_id": "acct-1",
                            "contact_id": "contact-1",
                            "created_ms": 1_772_329_200_000,
                            "updated_ms": 1_772_329_560_000,
                            "closed_ms": 1_772_329_860_000,
                            "history": [
                                {
                                    "id": "h-stage",
                                    "field": "stage",
                                    "from": "qualification",
                                    "to": "legal_review",
                                    "changed_by": "maya@acme.example.com",
                                    "timestamp": "2026-03-01T09:05:00Z",
                                },
                                {
                                    "id": "h-owner",
                                    "field": "owner",
                                    "from": "maya@acme.example.com",
                                    "to": "vp.sales@acme.example.com",
                                    "changed_by": "ceo@acme.example.com",
                                    "timestamp": "2026-03-01T09:06:00Z",
                                },
                                {
                                    "id": "h-amount",
                                    "field": "amount",
                                    "from": "$100,000",
                                    "to": "$125,000",
                                    "changed_by": "finance@acme.example.com",
                                    "timestamp": "2026-03-01T09:07:00Z",
                                },
                                {
                                    "id": "h-close",
                                    "field": "close_date",
                                    "from": "2026-03-15",
                                    "to": "2026-03-22",
                                    "changed_by": "ops@acme.example.com",
                                    "timestamp": "2026-03-01T09:08:00Z",
                                },
                            ],
                        }
                    ],
                },
            )
        ],
    )

    events = build_crm_events(
        snapshot=snapshot,
        provider="crm",
        organization_domain="acme.example.com",
        include_content=True,
    )

    deal_events = [event for event in events if event.conversation_anchor == "deal-1"]

    assert deal_events
    assert len({event.thread_id for event in deal_events}) == 1
    assert any(
        event.event_type == "assignment"
        and "Reassigned Acme expansion" in event.snippet
        for event in deal_events
    )
    assert any(
        "Changed Acme expansion amount from $100,000 to $125,000" in event.snippet
        for event in deal_events
    )
    assert any(
        "Moved Acme expansion close date from 2026-03-15 to 2026-03-22" in event.snippet
        for event in deal_events
    )
    assert any(
        event.event_type == "approval" and event.snippet == "Closed won: Acme expansion"
        for event in deal_events
    )


def test_build_crm_events_handles_missing_provider_and_bad_timestamps() -> None:
    empty_snapshot = ContextSnapshot(
        organization_name="Acme Cloud",
        organization_domain="acme.example.com",
        captured_at="2026-03-01T10:00:00Z",
        sources=[],
    )
    assert (
        build_crm_events(
            snapshot=empty_snapshot,
            provider="crm",
            organization_domain="acme.example.com",
            include_content=False,
        )
        == []
    )

    bad_snapshot = ContextSnapshot(
        organization_name="Acme Cloud",
        organization_domain="acme.example.com",
        captured_at="2026-03-01T10:00:00Z",
        sources=[
            ContextSourceResult(
                provider="crm",
                captured_at="2026-03-01T10:00:00Z",
                status="ok",
                record_counts={"deals": 1},
                data={
                    "deals": [
                        {
                            "id": "deal-bad",
                            "name": "Fallback timeline",
                            "stage": "legal_review",
                            "owner": "maya@acme.example.com",
                            "created": "not-a-date",
                            "updated": "also-not-a-date",
                            "history": [
                                {
                                    "id": "h-bad",
                                    "field": "stage",
                                    "from": "draft",
                                    "to": "legal_review",
                                    "changed_by": "maya@acme.example.com",
                                    "timestamp": "broken",
                                }
                            ],
                        }
                    ]
                },
            )
        ],
    )

    bad_events = build_crm_events(
        snapshot=bad_snapshot,
        provider="crm",
        organization_domain="acme.example.com",
        include_content=False,
    )

    assert len(bad_events) >= 2
    assert all(event.timestamp_ms >= 0 for event in bad_events)
    assert all(
        event.timestamp in {"not-a-date", "also-not-a-date", "broken"}
        for event in bad_events
    )
    assert any(event.timestamp == "broken" for event in bad_events)


def test_build_docs_events_emits_document_comment_version_and_share_history() -> None:
    snapshot = ContextSnapshot(
        organization_name="Acme Cloud",
        organization_domain="acme.example.com",
        captured_at="2026-03-01T10:00:00Z",
        sources=[
            ContextSourceResult(
                provider="google",
                captured_at="2026-03-01T10:00:00Z",
                status="ok",
                record_counts={"documents": 1, "drive_shares": 1},
                data={
                    "documents": [
                        {
                            "doc_id": "doc-1",
                            "title": "Renewal plan",
                            "body": "Internal legal review stays open.",
                            "owner": "maya@acme.example.com",
                            "created_time": "2026-03-01T09:00:00Z",
                            "modified_time": "2026-03-01T09:10:00Z",
                            "comments": [
                                {
                                    "id": "comment-1",
                                    "author": "legal@acme.example.com",
                                    "body": "Need one more legal pass.",
                                    "created": "2026-03-01T09:05:00Z",
                                }
                            ],
                            "versions": [
                                {
                                    "version_id": "v2",
                                    "summary": "Version 2 shared for review.",
                                    "modified_by": "maya@acme.example.com",
                                    "modified_time": "2026-03-01T09:06:00Z",
                                }
                            ],
                            "permissions": [
                                {
                                    "id": "perm-1",
                                    "shared_with": ["legal@acme.example.com"],
                                    "granted_by": "maya@acme.example.com",
                                    "created": "2026-03-01T09:07:00Z",
                                }
                            ],
                        }
                    ],
                    "drive_shares": [
                        {
                            "doc_id": "doc-1",
                            "shared_with": ["legal@acme.example.com"],
                        }
                    ],
                },
            )
        ],
    )

    events = build_docs_events(
        snapshot=snapshot,
        organization_domain="acme.example.com",
        include_content=True,
    )

    assert events
    assert len({event.thread_id for event in events}) == 1
    assert any(
        event.event_type == "reply" and "Need one more legal pass." in event.snippet
        for event in events
    )
    assert any("Version 2 shared for review." in event.snippet for event in events)
    assert any(
        event.event_type == "share"
        and event.snippet == "Shared with legal@acme.example.com"
        for event in events
    )


def test_build_docs_events_skips_missing_docs_and_falls_back_on_bad_timestamps() -> (
    None
):
    snapshot = ContextSnapshot(
        organization_name="Acme Cloud",
        organization_domain="acme.example.com",
        captured_at="2026-03-01T10:00:00Z",
        sources=[
            ContextSourceResult(
                provider="google",
                captured_at="2026-03-01T10:00:00Z",
                status="ok",
                record_counts={"documents": 2},
                data={
                    "documents": [
                        {
                            "title": "Missing id should be skipped",
                            "body": "skip me",
                        },
                        {
                            "doc_id": "doc-bad",
                            "title": "Broken timestamps",
                            "body": "Still keep the record.",
                            "owner": "maya@acme.example.com",
                            "modified_time": "not-a-date",
                            "comments": [
                                {
                                    "id": "comment-bad",
                                    "author": "legal@acme.example.com",
                                    "body": "Still review this.",
                                    "created": "also-not-a-date",
                                }
                            ],
                        },
                    ]
                },
            )
        ],
    )

    events = build_docs_events(
        snapshot=snapshot,
        organization_domain="acme.example.com",
        include_content=False,
    )

    assert len(events) == 2
    assert all(event.conversation_anchor == "doc-bad" for event in events)
    assert all(event.timestamp_ms >= 0 for event in events)
    assert events[0].timestamp == "not-a-date"
