from __future__ import annotations

from vei.context.api import diff_snapshots
from vei.context.models import ContextSnapshot, ContextSourceResult


def _snap(
    channels=None,
    issues=None,
    mail_threads=None,
    teams_channels=None,
    crm=None,
    salesforce=None,
    mail_archive=None,
) -> ContextSnapshot:
    sources = []
    if channels is not None:
        sources.append(
            ContextSourceResult(
                provider="slack",
                captured_at="2024-01-01T00:00:00+00:00",
                data={"channels": channels},
            )
        )
    if issues is not None:
        sources.append(
            ContextSourceResult(
                provider="jira",
                captured_at="2024-01-01T00:00:00+00:00",
                data={"issues": issues},
            )
        )
    if mail_threads is not None:
        sources.append(
            ContextSourceResult(
                provider="gmail",
                captured_at="2024-01-01T00:00:00+00:00",
                data={"threads": mail_threads},
            )
        )
    if teams_channels is not None:
        sources.append(
            ContextSourceResult(
                provider="teams",
                captured_at="2024-01-01T00:00:00+00:00",
                data={"channels": teams_channels},
            )
        )
    if crm is not None:
        sources.append(
            ContextSourceResult(
                provider="crm",
                captured_at="2024-01-01T00:00:00+00:00",
                data=crm,
            )
        )
    if salesforce is not None:
        sources.append(
            ContextSourceResult(
                provider="salesforce",
                captured_at="2024-01-01T00:00:00+00:00",
                data=salesforce,
            )
        )
    if mail_archive is not None:
        sources.append(
            ContextSourceResult(
                provider="mail_archive",
                captured_at="2024-01-01T00:00:00+00:00",
                data=mail_archive,
            )
        )
    return ContextSnapshot(
        organization_name="Test",
        captured_at="2024-01-01T00:00:00+00:00",
        sources=sources,
    )


def test_diff_detects_added_channel() -> None:
    before = _snap(channels=[])
    after = _snap(channels=[{"channel": "#new-channel", "messages": []}])
    result = diff_snapshots(before, after)
    assert len(result.added) == 1
    assert result.added[0].domain == "channels"
    assert result.added[0].item_id == "#new-channel"


def test_diff_detects_removed_issue() -> None:
    before = _snap(issues=[{"ticket_id": "PROJ-1", "title": "Bug"}])
    after = _snap(issues=[])
    result = diff_snapshots(before, after)
    assert len(result.removed) == 1
    assert result.removed[0].item_id == "PROJ-1"


def test_diff_detects_changed_issue() -> None:
    before = _snap(issues=[{"ticket_id": "PROJ-1", "title": "Bug", "status": "open"}])
    after = _snap(issues=[{"ticket_id": "PROJ-1", "title": "Bug", "status": "closed"}])
    result = diff_snapshots(before, after)
    assert len(result.changed) == 1
    assert result.changed[0].item_id == "PROJ-1"


def test_diff_no_changes() -> None:
    data = [{"ticket_id": "PROJ-1", "title": "Bug"}]
    before = _snap(issues=data)
    after = _snap(issues=data)
    result = diff_snapshots(before, after)
    assert result.summary == "no changes"
    assert len(result.entries) == 0


def test_diff_detects_gmail_thread_changes() -> None:
    before = _snap(mail_threads=[{"thread_id": "thread-1", "subject": "Budget"}])
    after = _snap(mail_threads=[{"thread_id": "thread-2", "subject": "Budget"}])
    result = diff_snapshots(before, after)
    added = [entry for entry in result.entries if entry.kind == "added"]
    removed = [entry for entry in result.entries if entry.kind == "removed"]
    assert added[0].domain == "mail_threads"
    assert added[0].item_id == "thread-2"
    assert removed[0].domain == "mail_threads"
    assert removed[0].item_id == "thread-1"


def test_diff_detects_teams_channel_changes() -> None:
    before = _snap(teams_channels=[{"channel": "#Engineering/General"}])
    after = _snap(teams_channels=[{"channel": "#Sales/Pipeline"}])
    result = diff_snapshots(before, after)
    added = [entry for entry in result.entries if entry.kind == "added"]
    removed = [entry for entry in result.entries if entry.kind == "removed"]
    assert added[0].domain == "teams_channels"
    assert added[0].item_id == "#Sales/Pipeline"
    assert removed[0].domain == "teams_channels"
    assert removed[0].item_id == "#Engineering/General"


# --- CRM ---


def test_diff_crm_added_company() -> None:
    before = _snap(crm={"companies": [], "contacts": [], "deals": []})
    after = _snap(
        crm={"companies": [{"id": "c1", "name": "Acme"}], "contacts": [], "deals": []}
    )
    result = diff_snapshots(before, after)
    assert len(result.added) == 1
    assert result.added[0].domain == "companies"
    assert result.added[0].item_id == "c1"


def test_diff_crm_removed_contact() -> None:
    before = _snap(
        crm={
            "companies": [],
            "contacts": [{"id": "ct1", "email": "a@b.com"}],
            "deals": [],
        }
    )
    after = _snap(crm={"companies": [], "contacts": [], "deals": []})
    result = diff_snapshots(before, after)
    assert len(result.removed) == 1
    assert result.removed[0].domain == "contacts"
    assert result.removed[0].item_id == "ct1"


def test_diff_crm_changed_deal() -> None:
    before = _snap(
        crm={
            "companies": [],
            "contacts": [],
            "deals": [{"id": "d1", "stage": "prospect"}],
        }
    )
    after = _snap(
        crm={
            "companies": [],
            "contacts": [],
            "deals": [{"id": "d1", "stage": "closed"}],
        }
    )
    result = diff_snapshots(before, after)
    assert len(result.changed) == 1
    assert result.changed[0].domain == "deals"
    assert result.changed[0].item_id == "d1"


# --- Salesforce ---


def test_diff_salesforce_added_deal() -> None:
    before = _snap(salesforce={"companies": [], "contacts": [], "deals": []})
    after = _snap(
        salesforce={
            "companies": [],
            "contacts": [],
            "deals": [{"id": "sf-d1", "amount": 5000}],
        }
    )
    result = diff_snapshots(before, after)
    assert len(result.added) == 1
    assert result.added[0].domain == "deals"
    assert result.added[0].item_id == "sf-d1"


def test_diff_salesforce_no_changes() -> None:
    data = {"companies": [{"id": "sf-c1", "name": "Corp"}], "contacts": [], "deals": []}
    before = _snap(salesforce=data)
    after = _snap(salesforce=data)
    result = diff_snapshots(before, after)
    assert result.summary == "no changes"


# --- Mail Archive ---


def test_diff_mail_archive_added_thread() -> None:
    before = _snap(mail_archive={"threads": [], "actors": []})
    after = _snap(
        mail_archive={
            "threads": [{"thread_id": "t1", "subject": "Hello"}],
            "actors": [],
        }
    )
    result = diff_snapshots(before, after)
    assert len(result.added) == 1
    assert result.added[0].domain == "archive_threads"
    assert result.added[0].item_id == "t1"


def test_diff_mail_archive_changed_actor() -> None:
    before = _snap(
        mail_archive={"threads": [], "actors": [{"actor_id": "a1", "role": "sender"}]}
    )
    after = _snap(
        mail_archive={
            "threads": [],
            "actors": [{"actor_id": "a1", "role": "recipient"}],
        }
    )
    result = diff_snapshots(before, after)
    assert len(result.changed) == 1
    assert result.changed[0].domain == "actors"
    assert result.changed[0].item_id == "a1"


def test_diff_mail_archive_removed_thread_and_actor() -> None:
    before = _snap(
        mail_archive={
            "threads": [{"thread_id": "t1", "subject": "Old"}],
            "actors": [{"actor_id": "a1", "role": "sender"}],
        }
    )
    after = _snap(mail_archive={"threads": [], "actors": []})
    result = diff_snapshots(before, after)
    assert len(result.removed) == 2
    domains = {e.domain for e in result.removed}
    assert domains == {"archive_threads", "actors"}
