from __future__ import annotations

from pydantic import BaseModel

from .models import GovernorAgentSpec, GovernorIngestEvent


class MirrorApprovalCommand(BaseModel):
    agent_id: str
    action: str = "approve"
    approval_id: str | None = None
    label: str | None = None


def default_service_ops_demo_agents() -> list[GovernorAgentSpec]:
    return [
        GovernorAgentSpec(
            agent_id="dispatch-bot",
            name="Dispatch Bot",
            mode="demo",
            role="dispatch_orchestrator",
            team="dispatch",
            allowed_surfaces=["slack", "service_ops"],
            policy_profile_id="operator",
            source="governor-demo",
        ),
        GovernorAgentSpec(
            agent_id="billing-bot",
            name="Billing Bot",
            mode="demo",
            role="billing_coordinator",
            team="finance",
            allowed_surfaces=["slack", "service_ops"],
            policy_profile_id="observer",
            source="governor-demo",
        ),
        GovernorAgentSpec(
            agent_id="control-lead",
            name="Control Lead",
            mode="demo",
            role="operations_approver",
            team="operations",
            allowed_surfaces=["slack", "service_ops", "jira", "salesforce"],
            policy_profile_id="approver",
            source="governor-demo",
        ),
    ]


def default_service_ops_demo_steps() -> (
    list[GovernorIngestEvent | MirrorApprovalCommand]
):
    return [
        GovernorIngestEvent(
            event_id="governor-demo-001",
            agent_id="dispatch-bot",
            external_tool="slack.chat.postMessage",
            resolved_tool="slack.send_message",
            focus_hint="slack",
            args={
                "channel": "#clearwater-dispatch",
                "text": (
                    "Dispatch Bot: rerouting Clearwater Medical to standby controls tech "
                    "while we keep the customer timeline intact."
                ),
            },
            label="Dispatch bot posts the reroute into Clearwater dispatch",
            source_mode="demo",
        ),
        GovernorIngestEvent(
            event_id="governor-demo-002",
            agent_id="dispatch-bot",
            external_tool="service_ops.assign_dispatch",
            resolved_tool="service_ops.assign_dispatch",
            focus_hint="service_ops",
            args={
                "work_order_id": "WO-CFS-100",
                "technician_id": "TECH-CFS-02",
                "appointment_id": "APT-CFS-100",
                "note": "Mirror demo reroute to preserve VIP same-day coverage.",
            },
            label="Dispatch bot assigns the backup controls technician",
            source_mode="demo",
        ),
        GovernorIngestEvent(
            event_id="governor-demo-003",
            agent_id="billing-bot",
            external_tool="service_ops.hold_billing",
            resolved_tool="service_ops.hold_billing",
            focus_hint="service_ops",
            args={
                "billing_case_id": "BILL-CFS-100",
                "reason": "Mirror demo: keep the VIP dispute contained during dispatch recovery.",
                "hold": True,
            },
            label="Billing bot pauses the disputed VIP billing case",
            source_mode="demo",
        ),
        GovernorIngestEvent(
            event_id="governor-demo-004",
            agent_id="dispatch-bot",
            external_tool="slack.chat.postMessage",
            resolved_tool="slack.send_message",
            focus_hint="slack",
            args={
                "channel": "#clearwater-dispatch",
                "text": "Dispatch Bot: field coverage is back online and finance containment is in place.",
            },
            label="Dispatch bot posts the executive ops update",
            source_mode="demo",
        ),
        GovernorIngestEvent(
            event_id="governor-demo-005",
            agent_id="billing-bot",
            external_tool="graph.messages.send",
            resolved_tool="mail.compose",
            focus_hint="mail",
            args={
                "to": "vip@clearwater.example.com",
                "subject": "Dispatch delay update",
                "body": "This should be blocked because Billing Bot is not cleared for mail.",
            },
            label="Billing bot tries to email the customer without mail access",
            source_mode="demo",
        ),
        GovernorIngestEvent(
            event_id="governor-demo-006",
            agent_id="dispatch-bot",
            external_tool="service_ops.update_policy",
            resolved_tool="service_ops.update_policy",
            focus_hint="service_ops",
            args={
                "billing_hold_on_dispute": False,
                "approval_threshold_usd": 2500,
                "reason": "Mirror demo: propose a riskier policy override while dispatch recovers.",
            },
            label="Dispatch bot proposes a risky local policy change",
            source_mode="demo",
        ),
        MirrorApprovalCommand(
            agent_id="control-lead",
            approval_id=None,
            label="Control lead approves the held policy change",
        ),
        GovernorIngestEvent(
            event_id="governor-demo-007",
            agent_id="dispatch-bot",
            external_tool="service_ops.clear_exception",
            resolved_tool="service_ops.clear_exception",
            focus_hint="service_ops",
            args={
                "exception_id": "EXC-CFS-100",
                "resolution_note": "Coverage restored and the dispatch path is stable again.",
            },
            label="Dispatch bot clears the dispatch exception after approval",
            source_mode="demo",
        ),
        GovernorIngestEvent(
            event_id="governor-demo-008",
            agent_id="control-lead",
            external_tool="jira.search",
            resolved_tool="tickets.list",
            focus_hint="jira",
            args={"limit": 3},
            label="Control lead checks Jira to confirm cross-surface visibility",
            source_mode="demo",
        ),
        GovernorIngestEvent(
            event_id="governor-demo-009",
            agent_id="control-lead",
            external_tool="salesforce.query.opportunity",
            resolved_tool="salesforce.opportunity.list",
            focus_hint="crm",
            args={"limit": 3},
            label="Control lead checks Salesforce opportunity visibility",
            source_mode="demo",
        ),
    ]
