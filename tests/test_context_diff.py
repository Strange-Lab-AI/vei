from __future__ import annotations

from vei.context.api import diff_snapshots
from vei.context.models import ContextSnapshot, ContextSourceResult


def _snap(channels=None, issues=None) -> ContextSnapshot:
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
