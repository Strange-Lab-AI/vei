from __future__ import annotations

from datetime import datetime

from vei.context.api import load_enron_public_context
from vei.whatif._enron_history import (
    build_enron_branch_history,
    build_enron_public_canonical_rows,
)
from vei.whatif.models import WhatIfArtifactFlags, WhatIfEvent


def test_enron_branch_history_reaches_flagship_threshold() -> None:
    public_context = load_enron_public_context()
    branch_event = _mail_event(
        event_id="branch-event",
        timestamp="2000-09-27T14:30:00Z",
        subject="Master agreement draft",
    )
    past_events = [
        _mail_event(
            event_id=f"mail-{index}",
            timestamp=f"2000-09-{20 + index:02d}T14:30:00Z",
            subject=f"Draft update {index}",
        )
        for index in range(1, 7)
    ]

    history = build_enron_branch_history(
        public_context=public_context,
        branch_event=branch_event,
        past_events=past_events,
    )

    assert len(history) >= 30
    assert {"mail", "governance", "market"} <= {event.surface for event in history}


def test_enron_public_canonical_rows_keep_source_family_metadata() -> None:
    rows = build_enron_public_canonical_rows(
        public_context=load_enron_public_context(),
        branch_timestamp="2001-10-30T17:45:00Z",
        organization_domain="enron.com",
        current_history_count=0,
    )

    assert len(rows) >= 30
    source_families = {
        str(row.metadata.get("source_family") or "").strip()
        for row in rows
        if str(row.metadata.get("source_family") or "").strip()
    }
    assert {"credit", "financial", "news"} <= source_families
    assert any(
        row.provider == "enron_record_history"
        and str(row.metadata.get("source_url") or "").strip()
        for row in rows
    )


def _mail_event(
    *,
    event_id: str,
    timestamp: str,
    subject: str,
) -> WhatIfEvent:
    return WhatIfEvent(
        event_id=event_id,
        timestamp=timestamp,
        timestamp_ms=_timestamp_ms(timestamp),
        actor_id="debra.perlingiere@enron.com",
        target_id="gerald.nemec@enron.com",
        event_type="message",
        thread_id="thr-master-agreement",
        case_id="master_agreement",
        surface="mail",
        subject=subject,
        snippet=subject,
        flags=WhatIfArtifactFlags(
            subject=subject,
            norm_subject=subject.lower(),
            to_recipients=["gerald.nemec@enron.com"],
        ),
    )


def _timestamp_ms(value: str) -> int:
    return int(datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp() * 1000)
