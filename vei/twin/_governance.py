from __future__ import annotations

import os

from vei.blueprint import resolve_tool_operation_class
from vei.connectors import TOOL_ROUTES
from vei.governor import (
    GovernorAgentSpec,
    GovernorConnectorStatus,
    GovernorIngestEvent,
    GovernorWorkspaceConfig,
)

_MIRROR_OPERATION_CLASS_BY_TOOL: dict[str, str] = {
    "service_ops.list_overview": "read",
    "service_ops.assign_dispatch": "write_safe",
    "service_ops.reschedule_dispatch": "write_safe",
    "service_ops.hold_billing": "write_safe",
    "service_ops.clear_exception": "write_safe",
    "service_ops.update_policy": "write_risky",
    "jira.list_issues": "read",
    "jira.get_issue": "read",
    "jira.create_issue": "write_safe",
    "jira.update_issue": "write_safe",
    "jira.add_comment": "write_safe",
    "jira.transition_issue": "write_risky",
    "salesforce.opportunity.list": "read",
    "salesforce.opportunity.get": "read",
    "salesforce.opportunity.create": "write_safe",
    "salesforce.opportunity.update": "write_safe",
    "salesforce.contact.get": "read",
    "salesforce.contact.list": "read",
    "salesforce.account.get": "read",
    "salesforce.account.list": "read",
    "salesforce.activity.log": "write_safe",
}

_PROFILE_ACTIONS = {
    "observer": {
        "read": "allow",
        "write_safe": "deny",
        "write_risky": "deny",
    },
    "operator": {
        "read": "allow",
        "write_safe": "allow",
        "write_risky": "require_approval",
    },
    "approver": {
        "read": "allow",
        "write_safe": "allow",
        "write_risky": "require_approval",
    },
    "admin": {
        "read": "allow",
        "write_safe": "allow",
        "write_risky": "allow",
    },
}


def mirror_operation_class(tool_name: str) -> str | None:
    route = TOOL_ROUTES.get(tool_name)
    if route is not None:
        return route.operation_class.value
    resolved = resolve_tool_operation_class(tool_name)
    if resolved is not None:
        return resolved
    return _MIRROR_OPERATION_CLASS_BY_TOOL.get(tool_name)


def event_surface(event: GovernorIngestEvent) -> str:
    if event.target:
        return _normalize_surface(str(event.target))
    tool_name = str(event.resolved_tool or event.external_tool or "")
    if tool_name.startswith("service_ops."):
        return "service_ops"
    if tool_name.startswith("jira."):
        return "tickets"
    if tool_name.startswith("salesforce."):
        return "crm"
    if tool_name.startswith("mail."):
        return "mail"
    if tool_name.startswith("calendar."):
        return "calendar"
    if tool_name.startswith("slack."):
        return "slack"
    return _normalize_surface(
        str(event.focus_hint or tool_name.split(".")[0] or "world")
    )


def check_surface_access(
    agent: GovernorAgentSpec,
    surface: str,
) -> str | None:
    if not agent.allowed_surfaces:
        return None
    normalized = _normalize_surface(surface)
    for allowed_surface in agent.allowed_surfaces:
        if normalized in _surface_alias_set(allowed_surface):
            return None
    return (
        f"agent '{agent.agent_id}' denied access to surface '{surface}' "
        f"(allowed: {', '.join(agent.allowed_surfaces)})"
    )


def check_policy_profile(
    *,
    agent: GovernorAgentSpec,
    operation_class: str,
    approval_granted: bool,
) -> dict[str, str] | None:
    if approval_granted:
        return None
    profile_id = str(agent.policy_profile_id or "admin").strip().lower() or "admin"
    profile_rules = _PROFILE_ACTIONS.get(profile_id, _PROFILE_ACTIONS["admin"])
    action = profile_rules.get(operation_class, "deny")
    if action == "allow":
        return None
    if action == "require_approval":
        return {
            "decision": "approval_required",
            "code": "mirror.approval_required",
            "reason": (
                f"policy profile '{profile_id}' requires approval for "
                f"{operation_class.replace('_', ' ')} actions"
            ),
        }
    return {
        "decision": "deny",
        "code": "mirror.profile_denied",
        "reason": (
            f"policy profile '{profile_id}' does not allow "
            f"{operation_class.replace('_', ' ')} actions"
        ),
    }


def check_approval_rules(
    *,
    config: GovernorWorkspaceConfig,
    tool_name: str,
    surface: str,
    operation_class: str,
    approval_granted: bool,
) -> dict[str, str] | None:
    if approval_granted or operation_class == "read":
        return None
    normalized_surface = _normalize_surface(surface)
    for rule in config.approval_rules:
        if rule.operation_classes and operation_class not in rule.operation_classes:
            continue
        if rule.surface:
            allowed_surfaces = _surface_alias_set(rule.surface)
            if normalized_surface not in allowed_surfaces:
                continue
        if rule.resolved_tools and tool_name not in rule.resolved_tools:
            continue
        return {
            "decision": "approval_required",
            "code": rule.reason_code,
            "reason": rule.reason,
        }
    return None


def check_connector_safety(
    *,
    connector_mode: str,
    tool_name: str,
    surface: str,
    operation_class: str,
    approval_granted: bool,
) -> dict[str, str] | None:
    if connector_mode != "live":
        return None
    if surface == "service_ops":
        return None
    if surface == "slack":
        if not os.environ.get("VEI_LIVE_SLACK_TOKEN", "").strip():
            return {
                "decision": "deny",
                "code": "mirror.connector_degraded",
                "reason": "Live Slack passthrough is not configured in this environment.",
            }
        blocked = _blocked_live_operations()
        if tool_name in blocked:
            return {
                "decision": "deny",
                "code": "mirror.unsupported_live_write",
                "reason": f"Live policy blocks the operation '{tool_name}'.",
            }
        if operation_class == "read":
            return None
        if operation_class == "write_safe":
            if approval_granted or _env_bool("VEI_LIVE_ALLOW_WRITE_SAFE"):
                return None
            return {
                "decision": "approval_required",
                "code": "mirror.approval_required",
                "reason": "Live safe-write requires approval in this workspace.",
            }
        if _env_bool("VEI_LIVE_ALLOW_WRITE_RISKY"):
            return None
        return {
            "decision": "deny",
            "code": "mirror.unsupported_live_write",
            "reason": "Live risky writes are blocked in this workspace.",
        }
    if operation_class == "read":
        return None
    return {
        "decision": "deny",
        "code": "mirror.unsupported_live_write",
        "reason": (
            f"Live writes are not enabled for the '{surface}' surface in this workspace."
        ),
    }


def connector_statuses(
    connector_mode: str, *, checked_at: str
) -> list[GovernorConnectorStatus]:
    if connector_mode != "live":
        return [
            GovernorConnectorStatus(
                surface="slack",
                source_mode="sim",
                availability="healthy",
                write_capability="interactive",
                reason="Simulated Slack surface is interactive.",
                last_checked_at=checked_at,
            ),
            GovernorConnectorStatus(
                surface="jira",
                source_mode="sim",
                availability="healthy",
                write_capability="interactive",
                reason="Simulated Jira surface is interactive.",
                last_checked_at=checked_at,
            ),
            GovernorConnectorStatus(
                surface="graph",
                source_mode="sim",
                availability="healthy",
                write_capability="interactive",
                reason="Simulated Graph surface is interactive.",
                last_checked_at=checked_at,
            ),
            GovernorConnectorStatus(
                surface="salesforce",
                source_mode="sim",
                availability="healthy",
                write_capability="interactive",
                reason="Simulated Salesforce surface is interactive.",
                last_checked_at=checked_at,
            ),
        ]

    slack_token = os.environ.get("VEI_LIVE_SLACK_TOKEN", "").strip()
    slack_status = GovernorConnectorStatus(
        surface="slack",
        source_mode="live",
        availability="healthy" if slack_token else "degraded",
        write_capability="interactive" if slack_token else "unsupported",
        reason=(
            "Live Slack passthrough is available."
            if slack_token
            else "Set VEI_LIVE_SLACK_TOKEN to enable live Slack passthrough."
        ),
        last_checked_at=checked_at,
    )
    return [
        slack_status,
        GovernorConnectorStatus(
            surface="jira",
            source_mode="live",
            availability="healthy",
            write_capability="read_only",
            reason="Live Jira compatibility stays read-only in this milestone.",
            last_checked_at=checked_at,
        ),
        GovernorConnectorStatus(
            surface="graph",
            source_mode="live",
            availability="healthy",
            write_capability="read_only",
            reason="Live Graph compatibility stays read-only in this milestone.",
            last_checked_at=checked_at,
        ),
        GovernorConnectorStatus(
            surface="salesforce",
            source_mode="live",
            availability="healthy",
            write_capability="read_only",
            reason="Live Salesforce compatibility stays read-only in this milestone.",
            last_checked_at=checked_at,
        ),
    ]


def _normalize_surface(surface: str) -> str:
    normalized = str(surface or "").strip().lower()
    if normalized in {"jira", "tickets"}:
        return "tickets"
    if normalized in {"salesforce", "crm"}:
        return "crm"
    return normalized


def _surface_alias_set(surface: str) -> set[str]:
    raw = str(surface or "").strip().lower()
    normalized = _normalize_surface(raw)
    if raw == "graph":
        return {"graph", "mail", "calendar"}
    if normalized == "mail":
        return {"mail", "graph"}
    if normalized == "calendar":
        return {"calendar", "graph"}
    if normalized == "tickets":
        return {"tickets", "jira"}
    if normalized == "crm":
        return {"crm", "salesforce"}
    if normalized == "slack":
        return {"slack"}
    if normalized == "service_ops":
        return {"service_ops"}
    return {normalized}


def _env_bool(name: str) -> bool:
    value = os.environ.get(name)
    if value is None:
        return False
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _blocked_live_operations() -> set[str]:
    raw = os.environ.get("VEI_LIVE_BLOCK_OPS", "")
    return {item.strip() for item in raw.split(",") if item.strip()}
