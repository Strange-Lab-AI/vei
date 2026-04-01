"""Tests for the policy engine module."""

from __future__ import annotations

from vei.monitors.models import MonitorFinding
from vei.policy import (
    DEFAULT_RULES,
    PolicyEngine,
    PolicyFinding,
    PromoteMonitorRule,
)


def _finding(
    code: str,
    *,
    severity: str = "warning",
    tool: str | None = None,
    time_ms: int = 1000,
) -> MonitorFinding:
    return MonitorFinding(
        monitor="test",
        code=code,
        message=f"test finding {code}",
        severity=severity,
        time_ms=time_ms,
        tool=tool,
        metadata={"test": True},
    )


def test_promote_monitor_rule_matches() -> None:
    rule = PromoteMonitorRule("slack.approval_missing_amount", severity="warning")
    finding = _finding("slack.approval_missing_amount", tool="slack.send")
    result = rule.match(finding)
    assert result is not None
    assert isinstance(result, PolicyFinding)
    assert result.code == "slack.approval_missing_amount"
    assert result.severity == "warning"
    assert result.tool == "slack.send"
    assert result.time_ms == 1000
    assert result.metadata == {"test": True}


def test_promote_monitor_rule_no_match() -> None:
    rule = PromoteMonitorRule("pii.leak", severity="error")
    finding = _finding("slack.approval_missing_amount")
    assert rule.match(finding) is None


def test_promote_monitor_rule_custom_message() -> None:
    rule = PromoteMonitorRule("pii.leak", severity="error", message="PII detected")
    finding = _finding("pii.leak")
    result = rule.match(finding)
    assert result is not None
    assert result.message == "PII detected"


def test_promote_monitor_rule_default_message() -> None:
    rule = PromoteMonitorRule("pii.leak")
    assert "pii.leak" in rule.message


def test_engine_evaluates_multiple_findings() -> None:
    rules = [
        PromoteMonitorRule("slack.approval_missing_amount", severity="warning"),
        PromoteMonitorRule("pii.leak", severity="error"),
    ]
    engine = PolicyEngine(rules)

    findings = [
        _finding("slack.approval_missing_amount"),
        _finding("pii.leak"),
        _finding("unrelated.code"),
    ]
    results = engine.evaluate(findings)
    assert len(results) == 2
    codes = {r.code for r in results}
    assert codes == {"slack.approval_missing_amount", "pii.leak"}


def test_engine_empty_findings() -> None:
    engine = PolicyEngine([PromoteMonitorRule("pii.leak")])
    assert engine.evaluate([]) == []


def test_engine_no_rules() -> None:
    engine = PolicyEngine([])
    assert engine.evaluate([_finding("anything")]) == []


def test_engine_finding_matches_multiple_rules() -> None:
    rules = [
        PromoteMonitorRule("pii.leak", severity="error"),
        PromoteMonitorRule("pii.leak", severity="warning", message="also this"),
    ]
    engine = PolicyEngine(rules)
    results = engine.evaluate([_finding("pii.leak")])
    assert len(results) == 2


def test_default_rules_defined() -> None:
    assert len(DEFAULT_RULES) >= 3
    codes = {r.code for r in DEFAULT_RULES}
    assert "pii.leak" in codes
    assert "slack.approval_missing_amount" in codes


def test_engine_with_default_rules() -> None:
    engine = PolicyEngine(DEFAULT_RULES)
    findings = [
        _finding("pii.leak"),
        _finding("mail.outbound_volume"),
        _finding("unknown.code"),
    ]
    results = engine.evaluate(findings)
    codes = {r.code for r in results}
    assert "pii.leak" in codes
    assert "mail.outbound_volume" in codes
    assert "unknown.code" not in codes


def test_policy_finding_fields() -> None:
    pf = PolicyFinding(
        code="test",
        message="msg",
        severity="info",
        time_ms=500,
        tool="slack.send",
        metadata={"key": "value"},
    )
    assert pf.code == "test"
    assert pf.time_ms == 500
    assert pf.tool == "slack.send"
    assert pf.metadata["key"] == "value"
