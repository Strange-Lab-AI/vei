from __future__ import annotations

from vei.context.api import diff_snapshots
from vei.context.models import ContextSnapshot, ContextSourceResult


def _snap(
    channels=None,
    issues=None,
    mail_threads=None,
    teams_channels=None,
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
