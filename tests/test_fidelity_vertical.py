from __future__ import annotations

from unittest.mock import MagicMock

from vei.fidelity.api import _check_vertical_surface


def test_unknown_vertical_returns_skipped_case() -> None:
    """Unknown vertical names produce a safe 'skipped' fidelity case
    instead of silently falling through to property checks."""
    session = MagicMock()
    result = _check_vertical_surface(session, "unknown_vertical_xyz")
    assert result.status == "ok"
    assert "skipped" in result.title.lower()
    assert result.checks == []


def test_empty_vertical_returns_skipped_case() -> None:
    """An empty vertical name is treated as unknown, not as real_estate."""
    session = MagicMock()
    result = _check_vertical_surface(session, "")
    assert result.status == "ok"
    assert result.checks == []
