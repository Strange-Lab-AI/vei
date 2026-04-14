from __future__ import annotations

import pytest

from vei.router.core import MCPError, Router

pytestmark = pytest.mark.integration


def test_tool_latency_advances_clock() -> None:
    router = Router(seed=42, artifacts_dir=None)
    start = router.bus.clock_ms
    router.call_and_step(
        "mail.compose",
        {"to": "sales@example.com", "subj": "Quote", "body_text": "Please advise."},
    )
    assert router.bus.clock_ms >= start + 1000


def test_vei_observe_does_not_double_advance_clock() -> None:
    router = Router(seed=100, artifacts_dir=None)
    start = router.bus.clock_ms
    router.observe()
    assert router.bus.clock_ms == start + 1000


def test_fault_override_injects_error(monkeypatch) -> None:
    router = Router(seed=55, artifacts_dir=None)
    router._fault_overrides["mail.compose"] = 1.0
    with pytest.raises(MCPError) as err:
        router.call_and_step(
            "mail.compose",
            {"to": "sales@example.com", "subj": "Quote", "body_text": "Please advise."},
        )
    assert err.value.code == "fault.injected"
