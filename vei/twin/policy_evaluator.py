"""Pure policy evaluator for twin/governor decisions and replay."""

from __future__ import annotations

from pydantic import BaseModel

from vei.governor import GovernorAgentSpec, GovernorIngestEvent, GovernorWorkspaceConfig

from ._governance import (
    check_approval_rules,
    check_connector_safety,
    check_policy_profile,
    check_surface_access,
    event_surface,
    mirror_operation_class,
)


class PolicyEvaluation(BaseModel):
    action: str = "dispatch"
    surface: str = ""
    resolved_tool: str = ""
    operation_class: str = "read"
    decision: str = "allow"
    reason_code: str = ""
    reason: str = ""


class PolicyEvaluator:
    """Side-effect-free mirror policy evaluator."""

    def __init__(
        self,
        *,
        config: GovernorWorkspaceConfig,
        connector_mode: str,
    ) -> None:
        self.config = config
        self.connector_mode = connector_mode

    def evaluate(
        self,
        *,
        event: GovernorIngestEvent,
        agent: GovernorAgentSpec,
        approval_granted: bool = False,
    ) -> PolicyEvaluation:
        action = "dispatch" if event.resolved_tool else "inject"
        tool_name = str(event.resolved_tool or event.external_tool or "")
        surface = event_surface(event)
        operation_class = mirror_operation_class(tool_name)
        if operation_class is None and action == "inject":
            operation_class = "write_safe"
        if operation_class is None:
            return PolicyEvaluation(
                action=action,
                surface=surface,
                resolved_tool=tool_name,
                operation_class="read",
                decision="deny",
                reason_code="mirror.unknown_operation_class",
                reason=f"mirror does not have an operation class for '{tool_name}' yet",
            )

        surface_denial = check_surface_access(agent, surface)
        if surface_denial is not None:
            return PolicyEvaluation(
                action=action,
                surface=surface,
                resolved_tool=tool_name,
                operation_class=operation_class,
                decision="deny",
                reason_code="mirror.surface_denied",
                reason=surface_denial,
            )

        profile_decision = check_policy_profile(
            agent=agent,
            operation_class=operation_class,
            approval_granted=approval_granted,
        )
        if profile_decision is not None:
            return PolicyEvaluation(
                action=action,
                surface=surface,
                resolved_tool=tool_name,
                operation_class=operation_class,
                decision=profile_decision["decision"],
                reason_code=profile_decision["code"],
                reason=profile_decision["reason"],
            )

        approval_rule_decision = check_approval_rules(
            config=self.config,
            tool_name=tool_name,
            surface=surface,
            operation_class=operation_class,
            approval_granted=approval_granted,
        )
        if approval_rule_decision is not None:
            return PolicyEvaluation(
                action=action,
                surface=surface,
                resolved_tool=tool_name,
                operation_class=operation_class,
                decision=approval_rule_decision["decision"],
                reason_code=approval_rule_decision["code"],
                reason=approval_rule_decision["reason"],
            )

        connector_decision = check_connector_safety(
            connector_mode=self.connector_mode,
            tool_name=tool_name,
            surface=surface,
            operation_class=operation_class,
            approval_granted=approval_granted,
        )
        if connector_decision is not None:
            return PolicyEvaluation(
                action=action,
                surface=surface,
                resolved_tool=tool_name,
                operation_class=operation_class,
                decision=connector_decision["decision"],
                reason_code=connector_decision["code"],
                reason=connector_decision["reason"],
            )

        return PolicyEvaluation(
            action=action,
            surface=surface,
            resolved_tool=tool_name,
            operation_class=operation_class,
        )
