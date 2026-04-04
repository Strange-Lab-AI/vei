from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterable, List, Sequence

from vei.monitors.models import MonitorFinding


class PolicyEngine:
    """Simple rule engine that consumes monitor findings and emits policy outcomes."""

    def __init__(self, rules: Sequence["PolicyRule"]):
        self.rules = list(rules)

    def evaluate(self, findings: Iterable[MonitorFinding]) -> List["PolicyFinding"]:
        outputs: List["PolicyFinding"] = []
        for finding in findings:
            for rule in self.rules:
                policy = rule.match(finding)
                if policy:
                    outputs.append(policy)
        return outputs


class PolicyRule:
    code: str
    message: str
    severity: str

    def match(self, finding: MonitorFinding) -> "PolicyFinding" | None:
        raise NotImplementedError


class PromoteMonitorRule(PolicyRule):
    """Promote a monitor finding directly to a policy severity."""

    def __init__(
        self, monitor_code: str, severity: str = "warning", message: str | None = None
    ) -> None:
        self.monitor_code = monitor_code
        self.severity = severity
        self.message = message or f"Policy violation: {monitor_code}"
        self.code = monitor_code

    def match(self, finding: MonitorFinding) -> "PolicyFinding" | None:
        if finding.code != self.monitor_code:
            return None
        return PolicyFinding(
            code=self.code,
            message=self.message,
            severity=self.severity,
            time_ms=finding.time_ms,
            tool=finding.tool,
            metadata=dict(finding.metadata),
        )


DEFAULT_RULES: List[PolicyRule] = [
    PromoteMonitorRule("slack.approval_missing_amount", severity="warning"),
    PromoteMonitorRule("mail.outbound_volume", severity="info"),
    PromoteMonitorRule("slack.approval_format", severity="info"),
    PromoteMonitorRule("email.subject_quality", severity="info"),
    PromoteMonitorRule("pii.leak", severity="error"),
]


@dataclass(slots=True)
class PolicyFinding:
    code: str
    message: str
    severity: str
    time_ms: int
    tool: str | None
    metadata: dict[str, Any]
