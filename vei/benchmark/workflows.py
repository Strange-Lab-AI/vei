from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from pydantic import BaseModel

from vei.benchmark.models import (
    BenchmarkWorkflowParameter,
    BenchmarkWorkflowVariantManifest,
)
from vei.scenario_engine.models import WorkflowScenarioSpec


class SecurityContainmentWorkflowParams(BaseModel):
    app_id: str = "OAUTH-9001"
    alert_id: str = "ALT-9001"
    case_id: str = "CASE-0001"
    ticket_id: str = "SEC-417"
    brief_doc_id: str = "IR-RUNBOOK-1"
    notification_required: bool = True
    evidence_note: str = "Preserve app state before containment."
    suspension_reason: str = "Contain suspicious broad-scope app."
    case_note: str = "Targeted impact confirmed; customer notification required."
    ticket_note: str = (
        "Evidence preserved, app suspended, and notification decision recorded."
    )
    brief_update_note: str = (
        "Security comms should prepare a customer notification draft while the app remains suspended."
    )
    slack_channel: str = "#security-incident"
    slack_summary: str = (
        "OAuth app suspended after evidence preservation; incident record and notification decision updated."
    )


class EnterpriseOnboardingMigrationWorkflowParams(BaseModel):
    employee_id: str = "EMP-2201"
    user_id: str = "USR-ACQ-1"
    corporate_email: str = "jordan.sellers@example.com"
    crm_app_id: str = "APP-crm"
    doc_id: str = "GDRIVE-2201"
    tracking_ticket_id: str = "JRA-204"
    cutover_doc_id: str = "CUTOVER-2201"
    manager_email: str = "maya.rex@example.com"
    opportunity_id: str = "D-100"
    allowed_share_count: int = 1
    revoked_share_email: str = "channel-partner@example.net"
    deadline_max_ms: int = 86_400_000
    transfer_note: str = "Manager assumes ownership after acquisition cutover."
    onboarding_note: str = "Wave 1 migration completed successfully."
    ticket_update_note: str = (
        "Identity conflict resolved, playbook ownership transferred, and least-privilege access confirmed."
    )
    cutover_doc_note: str = (
        "Wave 1 cutover complete for Jordan Sellers; manager handoff approved and oversharing removed."
    )
    slack_channel: str = "#sales-cutover"
    slack_summary: str = (
        "Wave 1 seller cutover complete: identity resolved, CRM access granted, and playbook ownership transferred."
    )


class RevenueIncidentMitigationWorkflowParams(BaseModel):
    incident_id: str = "PD-9001"
    ticket_id: str = "INC-812"
    assignee: str = "commerce-ic"
    rollout_flag_key: str = "checkout_v2"
    rollout_pct: int = 15
    kill_switch_flag_key: str = "checkout_kill_switch"
    service_id: str = "svc-checkout"
    monitor_id: str = "mon-5001"
    workbook_id: str = "WB-CHK-1"
    sheet_id: str = "sheet-impact"
    impact_table_id: str = "tbl-impact"
    order_loss_cell: str = "B2"
    revenue_loss_cell: str = "B3"
    formula_cell: str = "B4"
    order_loss_per_hour: int = 430
    revenue_loss_usd: int = 128000
    comms_doc_id: str = "RUN-CHK-1"
    deal_id: str = "D-812"
    slack_channel: str = "#commerce-war-room"
    spreadsheet_note: str = "Impact updated while canary rollback is active."
    doc_update_note: str = (
        "Customer support should acknowledge intermittent checkout failures "
        "until mitigation is stable."
    )
    ticket_update_note: str = (
        "Feature-flag rollback active, impact workbook updated, and customer guidance drafted."
    )
    crm_activity_note: str = (
        "Estimated checkout revenue loss quantified and mitigation communicated to GTM."
    )
    slack_summary: str = (
        "Checkout mitigation active: rollout reduced, kill switch armed, and impact workbook updated."
    )
    recovering_note: str = "Traffic stabilizing after targeted rollback."
    resolution_note: str = "Traffic stabilized after targeted rollback and kill switch."
    deadline_max_ms: int = 180_000


class IdentityAccessGovernanceWorkflowParams(BaseModel):
    user_id: str = "USR-ACQ-1"
    employee_id: str = "EMP-2201"
    primary_app_id: str = "APP-crm"
    stale_app_id: str = "APP-analytics"
    doc_id: str = "GDRIVE-2201"
    request_id: str = "REQ-2201"
    ticket_id: str = "JRA-204"
    cutover_doc_id: str = "CUTOVER-2201"
    manager_email: str = "maya.rex@example.com"
    revoked_share_email: str = "channel-partner@example.net"
    allowed_share_count: int = 1
    deadline_max_ms: int = 86_400_000
    ticket_note: str = (
        "Imported identity governance review completed and tracker updated."
    )
    doc_update_note: str = (
        "Imported identity governance artifact updated with the final review decision."
    )
    slack_channel: str = "#identity-cutover"
    slack_summary: str = (
        "Imported identity governance workflow completed and shared state updated."
    )
    request_comment: str = "Approval advanced from imported policy workflow."
    onboarding_note: str = "Imported lifecycle handoff completed successfully."
    suspension_reason: str = "Temporary suspension during identity governance review."


WorkflowParams = (
    SecurityContainmentWorkflowParams
    | EnterpriseOnboardingMigrationWorkflowParams
    | RevenueIncidentMitigationWorkflowParams
    | IdentityAccessGovernanceWorkflowParams
)


@dataclass(frozen=True)
class _VariantDefinition:
    name: str
    title: str
    description: str
    scenario_name: str
    parameters: WorkflowParams


_PARAMETER_DESCRIPTIONS: Dict[str, Dict[str, str]] = {
    "security_containment": {
        "app_id": "Suspicious OAuth app under containment.",
        "alert_id": "SIEM alert linked to the containment case.",
        "case_id": "Investigation case updated during containment.",
        "ticket_id": "Tracking ticket that records containment follow-through.",
        "brief_doc_id": "Incident runbook or comms brief updated during containment.",
        "notification_required": "Whether the workflow records customer notification as required.",
        "evidence_note": "Forensics note attached when preserving Google evidence.",
        "suspension_reason": "Reason recorded for the targeted OAuth app suspension.",
        "case_note": "Containment note written back to the SIEM case.",
        "ticket_note": "Tracking ticket comment written after containment.",
        "brief_update_note": "Updated runbook/comms note describing the notification posture.",
        "slack_channel": "Slack channel used for security incident coordination.",
        "slack_summary": "Security summary posted after the containment decision is recorded.",
    },
    "enterprise_onboarding_migration": {
        "employee_id": "Employee record resolved and marked onboarded.",
        "user_id": "Provisioned identity user activated in Okta.",
        "corporate_email": "Resolved corporate email for the acquired seller.",
        "crm_app_id": "CRM application assignment granted after activation.",
        "doc_id": "Inherited shared document that must be restricted.",
        "tracking_ticket_id": "Jira-style ticket tracking the acquired-user cutover.",
        "cutover_doc_id": "Cutover checklist or manager handoff document updated during migration.",
        "manager_email": "Manager receiving ownership and final access review.",
        "opportunity_id": "Open opportunity transferred during the cutover.",
        "allowed_share_count": "Expected post-migration sharing count for the inherited document.",
        "revoked_share_email": "External share that must be removed during the cutover.",
        "deadline_max_ms": "Virtual-time deadline for the onboarding workflow to complete.",
        "transfer_note": "Note attached to the document ownership transfer.",
        "onboarding_note": "Final HRIS note recording onboarding completion.",
        "ticket_update_note": "Cutover status note written to the Jira tracker.",
        "cutover_doc_note": "Cutover checklist update recorded in the docs surface.",
        "slack_channel": "Slack channel used for migration coordinator updates.",
        "slack_summary": "Summary posted once the user cutover is safe to hand off.",
    },
    "revenue_incident_mitigation": {
        "incident_id": "PagerDuty incident mitigated by the workflow.",
        "ticket_id": "Incident ticket updated and resolved as mitigation progresses.",
        "assignee": "Incident assignee recorded during acknowledgement.",
        "rollout_flag_key": "Feature flag whose rollout is reduced during mitigation.",
        "rollout_pct": "Rollout percentage kept live as a controlled canary.",
        "kill_switch_flag_key": "Fallback kill switch enabled during mitigation.",
        "service_id": "Service marked as recovering after mitigation.",
        "monitor_id": "Monitor inspected while quantifying the checkout spike.",
        "workbook_id": "Spreadsheet workbook used as the revenue flight deck.",
        "sheet_id": "Workbook sheet used for impact quantification.",
        "impact_table_id": "Table updated with the quantified revenue impact row.",
        "order_loss_cell": "Cell capturing estimated orders lost per hour.",
        "revenue_loss_cell": "Cell capturing estimated revenue loss in USD.",
        "formula_cell": "Cell used for the spreadsheet formula backstop.",
        "order_loss_per_hour": "Estimated orders lost per hour during the incident.",
        "revenue_loss_usd": "Estimated revenue loss entered into the workbook.",
        "comms_doc_id": "Doc updated with support/customer communication guidance.",
        "deal_id": "CRM opportunity annotated with revenue impact.",
        "slack_channel": "Slack channel used for stakeholder updates.",
        "spreadsheet_note": "Narrative note attached to the spreadsheet impact row.",
        "doc_update_note": "Updated internal/customer communication guidance.",
        "ticket_update_note": "Ticket comment documenting the mitigation state.",
        "crm_activity_note": "CRM activity note recording customer/revenue impact.",
        "slack_summary": "Slack summary sent after mitigation is staged.",
        "recovering_note": "Recovery note attached to the monitoring service state.",
        "resolution_note": "Final incident note written when closing the page.",
        "deadline_max_ms": "Virtual-time deadline for the mixed-stack mitigation flow.",
    },
    "identity_access_governance": {
        "user_id": "Identity user under review.",
        "employee_id": "HRIS employee record connected to the identity workflow.",
        "primary_app_id": "Application that should remain after least-privilege review.",
        "stale_app_id": "Application that should be removed during entitlement cleanup.",
        "doc_id": "Imported Drive document or playbook under ACL review.",
        "request_id": "Service request whose approvals must be advanced.",
        "ticket_id": "Tracker issue updated during the governance workflow.",
        "cutover_doc_id": "Doc artifact updated with the governance outcome.",
        "manager_email": "Manager or active owner who should retain access.",
        "revoked_share_email": "External share that must be removed.",
        "allowed_share_count": "Expected post-remediation share count on the imported document.",
        "deadline_max_ms": "Virtual-time deadline for the governance workflow.",
        "ticket_note": "Tracker note written after the governance action completes.",
        "doc_update_note": "Document note written during imported policy follow-through.",
        "slack_summary": "Slack summary sent after the governance action is complete.",
        "request_comment": "Approval comment recorded during request advancement.",
        "onboarding_note": "Lifecycle note written when the user handoff completes.",
        "suspension_reason": "Reason recorded if the workflow suspends a user account.",
    },
}


_VARIANT_CATALOG: Dict[str, Dict[str, _VariantDefinition]] = {
    "security_containment": {
        "customer_notify": _VariantDefinition(
            name="customer_notify",
            title="Customer Notify",
            description=(
                "Contain the malicious OAuth app and explicitly record that "
                "customer notification is required."
            ),
            scenario_name="oauth_app_containment",
            parameters=SecurityContainmentWorkflowParams(),
        ),
        "internal_only_review": _VariantDefinition(
            name="internal_only_review",
            title="Internal Review",
            description=(
                "Contain the malicious OAuth app while recording that no "
                "customer notification is currently required."
            ),
            scenario_name="oauth_app_containment",
            parameters=SecurityContainmentWorkflowParams(
                notification_required=False,
                evidence_note="Preserve app state for internal review before containment.",
                case_note=(
                    "Targeted impact contained; no customer notification required "
                    "at this stage."
                ),
            ),
        ),
    },
    "enterprise_onboarding_migration": {
        "manager_cutover": _VariantDefinition(
            name="manager_cutover",
            title="Manager Cutover",
            description=(
                "Resolve the acquired seller, hand assets to the current manager, "
                "and complete the first-wave onboarding cutover."
            ),
            scenario_name="acquired_sales_onboarding",
            parameters=EnterpriseOnboardingMigrationWorkflowParams(),
        ),
        "alias_cutover": _VariantDefinition(
            name="alias_cutover",
            title="Alias Cutover",
            description=(
                "Resolve the acquired seller to an alias-based corporate identity "
                "while preserving least privilege and transferring ownership."
            ),
            scenario_name="acquired_sales_onboarding",
            parameters=EnterpriseOnboardingMigrationWorkflowParams(
                corporate_email="jordan.sellers+wave1@example.com",
                transfer_note="Manager assumes ownership after alias cutover.",
                onboarding_note="Alias-based cutover completed for wave 1.",
            ),
        ),
    },
    "revenue_incident_mitigation": {
        "revenue_ops_flightdeck": _VariantDefinition(
            name="revenue_ops_flightdeck",
            title="Revenue Ops Flight Deck",
            description=(
                "Quantify checkout impact in a spreadsheet, update GTM/customer "
                "artifacts, and contain the incident with safe rollout controls."
            ),
            scenario_name="checkout_spike_mitigation",
            parameters=RevenueIncidentMitigationWorkflowParams(
                assignee="commerce-ic",
                rollout_pct=10,
                recovering_note=(
                    "Traffic stabilizing after mixed-stack rollback and support guidance refresh."
                ),
                resolution_note=(
                    "Traffic stabilized after mixed-stack rollback, impact quantification, "
                    "and coordinated GTM updates."
                ),
            ),
        ),
        "kill_switch_backstop": _VariantDefinition(
            name="kill_switch_backstop",
            title="Kill Switch Backstop",
            description=(
                "Shrink checkout blast radius with a 15 percent canary and a "
                "kill-switch backstop."
            ),
            scenario_name="checkout_spike_mitigation",
            parameters=RevenueIncidentMitigationWorkflowParams(),
        ),
        "canary_floor": _VariantDefinition(
            name="canary_floor",
            title="Canary Floor",
            description=(
                "Drive checkout traffic down to a 5 percent canary before "
                "resolving the incident."
            ),
            scenario_name="checkout_spike_mitigation",
            parameters=RevenueIncidentMitigationWorkflowParams(
                assignee="release-controller",
                rollout_pct=5,
                recovering_note="Traffic stabilizing after 5 percent canary containment.",
                resolution_note=(
                    "Traffic stabilized after 5 percent canary containment and "
                    "kill switch backstop."
                ),
            ),
        ),
    },
    "identity_access_governance": {
        "oversharing_remediation": _VariantDefinition(
            name="oversharing_remediation",
            title="Oversharing Remediation",
            description="Remove imported external sharing and capture the artifact follow-through cleanly.",
            scenario_name="acquired_sales_onboarding",
            parameters=IdentityAccessGovernanceWorkflowParams(
                ticket_note="Imported oversharing remediated and tracker updated.",
                doc_update_note="Imported document share posture restored to internal visibility.",
                slack_summary="Imported oversharing remediated and Drive posture restored.",
            ),
        ),
        "approval_bottleneck": _VariantDefinition(
            name="approval_bottleneck",
            title="Approval Bottleneck",
            description="Advance a pending approval chain before the approved application can remain assigned.",
            scenario_name="acquired_sales_onboarding",
            parameters=IdentityAccessGovernanceWorkflowParams(
                ticket_note="Pending imported approval chain advanced and tracker updated.",
                doc_update_note="Approval bottleneck cleared from imported governance workflow.",
                slack_summary="Imported approval bottleneck cleared and request advanced.",
            ),
        ),
        "stale_entitlement_cleanup": _VariantDefinition(
            name="stale_entitlement_cleanup",
            title="Stale Entitlement Cleanup",
            description="Remove imported stale application access while preserving the approved app set.",
            scenario_name="acquired_sales_onboarding",
            parameters=IdentityAccessGovernanceWorkflowParams(
                ticket_note="Imported stale entitlement removed and tracker updated.",
                doc_update_note="Least-privilege cleanup completed for imported access review.",
                slack_summary="Imported stale entitlement removed successfully.",
            ),
        ),
        "break_glass_follow_up": _VariantDefinition(
            name="break_glass_follow_up",
            title="Break-Glass Follow-Up",
            description="Clean up imported emergency access and record the follow-up trail.",
            scenario_name="acquired_sales_onboarding",
            parameters=IdentityAccessGovernanceWorkflowParams(
                ticket_note="Imported break-glass access reviewed and follow-up captured.",
                doc_update_note="Break-glass follow-up recorded in imported governance artifact.",
                slack_summary="Imported break-glass follow-up captured and stale access removed.",
            ),
        ),
    },
}

_SCENARIO_TO_WORKFLOW = {
    "oauth_app_containment": "security_containment",
    "acquired_sales_onboarding": "enterprise_onboarding_migration",
    "checkout_spike_mitigation": "revenue_incident_mitigation",
}


def _build_security_containment_spec(
    params: SecurityContainmentWorkflowParams, *, variant_name: str
) -> WorkflowScenarioSpec:
    return WorkflowScenarioSpec.model_validate(
        {
            "name": "security_containment",
            "objective": {
                "statement": (
                    "Contain a malicious OAuth app with targeted action, preserve "
                    "evidence, and record the customer-notification decision."
                ),
                "success": [
                    "oauth app suspended",
                    "evidence preserved",
                    "notification decision recorded",
                    "incident artifacts updated",
                ],
            },
            "world": {"catalog": "oauth_app_containment"},
            "actors": [
                {"actor_id": "sec-lead", "role": "incident_commander"},
                {"actor_id": "siem", "role": "forensics_system"},
            ],
            "constraints": [
                {
                    "name": "targeted_containment",
                    "description": "Contain the suspicious app without broad user disruption.",
                },
                {
                    "name": "evidence",
                    "description": "Preserve evidence before or during containment.",
                },
            ],
            "steps": [
                {
                    "step_id": "inspect_app",
                    "description": "Inspect the suspicious OAuth app in Google Admin.",
                    "tool": "google_admin.get_oauth_app",
                    "args": {"app_id": params.app_id},
                    "expect": [
                        {
                            "kind": "result_equals",
                            "field": "risk_level",
                            "equals": "critical",
                        },
                        {
                            "kind": "result_equals",
                            "field": "status",
                            "equals": "ACTIVE",
                        },
                    ],
                },
                {
                    "step_id": "preserve_google_evidence",
                    "description": "Preserve Google-side evidence before disabling the app.",
                    "tool": "google_admin.preserve_oauth_evidence",
                    "args": {
                        "app_id": params.app_id,
                        "note": params.evidence_note,
                    },
                    "expect": [
                        {
                            "kind": "result_equals",
                            "field": "evidence_hold",
                            "equals": True,
                        }
                    ],
                    "on_failure": "jump:inspect_app",
                },
                {
                    "step_id": "preserve_siem_evidence",
                    "description": "Link the originating alert into the incident case.",
                    "tool": "siem.preserve_evidence",
                    "args": {
                        "alert_id": params.alert_id,
                        "case_id": params.case_id,
                        "note": "Preserved during OAuth containment.",
                    },
                    "expect": [
                        {
                            "kind": "result_equals",
                            "field": "evidence_preserved",
                            "equals": True,
                        }
                    ],
                },
                {
                    "step_id": "suspend_oauth_app",
                    "description": "Suspend only the malicious OAuth app.",
                    "tool": "google_admin.suspend_oauth_app",
                    "args": {
                        "app_id": params.app_id,
                        "reason": params.suspension_reason,
                    },
                    "expect": [
                        {
                            "kind": "result_equals",
                            "field": "status",
                            "equals": "SUSPENDED",
                        }
                    ],
                    "on_failure": "jump:preserve_google_evidence",
                },
                {
                    "step_id": "record_notification_decision",
                    "description": "Update the case with containment state and customer notification.",
                    "tool": "siem.update_case",
                    "args": {
                        "case_id": params.case_id,
                        "status": "CONTAINED",
                        "customer_notification_required": params.notification_required,
                        "note": params.case_note,
                    },
                    "expect": [
                        {
                            "kind": "result_equals",
                            "field": "status",
                            "equals": "CONTAINED",
                        },
                        {
                            "kind": "result_equals",
                            "field": "customer_notification_required",
                            "equals": params.notification_required,
                        },
                    ],
                },
                {
                    "step_id": "update_incident_brief",
                    "description": "Refresh the containment brief with the notification posture.",
                    "tool": "docs.update",
                    "args": {
                        "doc_id": params.brief_doc_id,
                        "body": (
                            "OAuth app containment summary.\n\n"
                            f"{params.case_note}\n\n"
                            f"{params.brief_update_note}"
                        ),
                    },
                    "expect": [
                        {
                            "kind": "result_equals",
                            "field": "doc_id",
                            "equals": params.brief_doc_id,
                        }
                    ],
                },
                {
                    "step_id": "comment_tracking_ticket",
                    "description": "Annotate the security tracking ticket with the containment outcome.",
                    "tool": "jira.add_comment",
                    "args": {
                        "issue_id": params.ticket_id,
                        "body": params.ticket_note,
                        "author": "sec-lead",
                    },
                    "expect": [
                        {
                            "kind": "result_equals",
                            "field": "issue_id",
                            "equals": params.ticket_id,
                        }
                    ],
                },
                {
                    "step_id": "post_security_summary",
                    "description": "Send a channel update after the containment decision is recorded.",
                    "tool": "slack.send_message",
                    "args": {
                        "channel": params.slack_channel,
                        "text": params.slack_summary,
                    },
                },
            ],
            "success_assertions": [
                {
                    "kind": "state_equals",
                    "field": f"components.google_admin.oauth_apps.{params.app_id}.status",
                    "equals": "SUSPENDED",
                },
                {
                    "kind": "state_equals",
                    "field": f"components.google_admin.oauth_apps.{params.app_id}.evidence_hold",
                    "equals": True,
                },
                {
                    "kind": "state_equals",
                    "field": f"components.siem.alerts.{params.alert_id}.evidence_preserved",
                    "equals": True,
                },
                {
                    "kind": "state_contains",
                    "field": f"components.siem.cases.{params.case_id}.evidence_refs",
                    "contains": params.alert_id,
                },
                {
                    "kind": "state_equals",
                    "field": (
                        f"components.siem.cases.{params.case_id}."
                        "customer_notification_required"
                    ),
                    "equals": params.notification_required,
                },
                {
                    "kind": "state_contains",
                    "field": f"components.docs.docs.{params.brief_doc_id}.body",
                    "contains": "notification",
                },
                {
                    "kind": "state_contains",
                    "field": f"components.tickets.metadata.{params.ticket_id}.comments",
                    "contains": "Evidence preserved",
                },
                {
                    "kind": "state_contains",
                    "field": f"components.slack.channels.{params.slack_channel}.messages",
                    "contains": "notification decision",
                },
            ],
            "failure_paths": [
                {
                    "name": "containment_requires_preserved_evidence",
                    "trigger_step": "suspend_oauth_app",
                    "recovery_steps": [
                        "preserve_google_evidence",
                        "preserve_siem_evidence",
                    ],
                    "notes": "Re-establish evidence preservation before retrying containment.",
                }
            ],
            "tags": ["benchmark-family", "security", "containment", variant_name],
            "metadata": {
                "benchmark_family": "security_containment",
                "workflow_variant": variant_name,
                "workflow_parameters": params.model_dump(mode="json"),
            },
        }
    )


def _build_enterprise_onboarding_spec(
    params: EnterpriseOnboardingMigrationWorkflowParams, *, variant_name: str
) -> WorkflowScenarioSpec:
    return WorkflowScenarioSpec.model_validate(
        {
            "name": "enterprise_onboarding_migration",
            "objective": {
                "statement": (
                    "Resolve onboarding conflicts, preserve least privilege, migrate "
                    "deal ownership, and prevent oversharing."
                ),
                "success": [
                    "identity conflict resolved",
                    "crm access assigned",
                    "document sharing restricted",
                    "deal ownership transferred",
                    "employee onboarded",
                    "cutover artifacts updated",
                ],
            },
            "world": {"catalog": "acquired_sales_onboarding"},
            "actors": [
                {"actor_id": "it-integration", "role": "migration_operator"},
                {"actor_id": "sales-manager", "role": "manager_reviewer"},
            ],
            "constraints": [
                {
                    "name": "least_privilege",
                    "description": "Grant only Slack and CRM to the migrated seller.",
                },
                {
                    "name": "oversharing",
                    "description": "Remove external-link sharing before ownership transfer.",
                },
            ],
            "steps": [
                {
                    "step_id": "resolve_identity",
                    "description": "Resolve the acquired employee into the corporate identity.",
                    "tool": "hris.resolve_identity",
                    "args": {
                        "employee_id": params.employee_id,
                        "corporate_email": params.corporate_email,
                        "note": "Merged acquired identity into the corporate tenant.",
                    },
                    "expect": [
                        {
                            "kind": "result_equals",
                            "field": "identity_conflict",
                            "equals": False,
                        }
                    ],
                },
                {
                    "step_id": "activate_user",
                    "description": "Activate the provisioned Okta user.",
                    "tool": "okta.activate_user",
                    "args": {"user_id": params.user_id},
                    "expect": [
                        {
                            "kind": "result_equals",
                            "field": "status",
                            "equals": "ACTIVE",
                        }
                    ],
                    "on_failure": "jump:resolve_identity",
                },
                {
                    "step_id": "assign_crm",
                    "description": "Grant CRM access after activation.",
                    "graph_domain": "identity_graph",
                    "graph_action": "assign_application",
                    "args": {"user_id": params.user_id, "app_id": params.crm_app_id},
                    "expect": [
                        {
                            "kind": "result_equals",
                            "field": "app_id",
                            "equals": params.crm_app_id,
                        }
                    ],
                },
                {
                    "step_id": "restrict_share",
                    "description": "Restrict inherited Drive sharing before migration.",
                    "graph_domain": "doc_graph",
                    "graph_action": "restrict_drive_share",
                    "args": {
                        "doc_id": params.doc_id,
                        "visibility": "internal",
                        "note": "Remove external-link sharing during migration.",
                    },
                    "expect": [
                        {
                            "kind": "result_equals",
                            "field": "visibility",
                            "equals": "internal",
                        },
                        {
                            "kind": "result_equals",
                            "field": "shared_with_count",
                            "equals": params.allowed_share_count,
                        },
                        {
                            "kind": "state_count_equals",
                            "field": (
                                f"components.google_admin.drive_shares.{params.doc_id}."
                                "shared_with"
                            ),
                            "equals": params.allowed_share_count,
                        },
                        {
                            "kind": "state_not_contains",
                            "field": (
                                f"components.google_admin.drive_shares.{params.doc_id}."
                                "shared_with"
                            ),
                            "contains": params.revoked_share_email,
                        },
                    ],
                    "on_failure": "continue",
                },
                {
                    "step_id": "transfer_playbook_owner",
                    "description": "Transfer the sales playbook to the current manager.",
                    "graph_domain": "doc_graph",
                    "graph_action": "transfer_drive_ownership",
                    "args": {
                        "doc_id": params.doc_id,
                        "owner": params.manager_email,
                        "note": params.transfer_note,
                    },
                    "expect": [
                        {
                            "kind": "result_equals",
                            "field": "owner",
                            "equals": params.manager_email,
                        }
                    ],
                },
                {
                    "step_id": "transfer_open_opportunity",
                    "description": "Move the inherited opportunity to the manager.",
                    "graph_domain": "revenue_graph",
                    "graph_action": "reassign_deal_owner",
                    "args": {
                        "id": params.opportunity_id,
                        "owner": params.manager_email,
                    },
                    "expect": [
                        {
                            "kind": "result_equals",
                            "field": "owner",
                            "equals": params.manager_email,
                        }
                    ],
                },
                {
                    "step_id": "mark_onboarded",
                    "description": "Mark the employee onboarded after cutover checks pass.",
                    "tool": "hris.mark_onboarded",
                    "args": {
                        "employee_id": params.employee_id,
                        "note": params.onboarding_note,
                    },
                    "expect": [
                        {
                            "kind": "result_equals",
                            "field": "onboarded",
                            "equals": True,
                        }
                    ],
                },
                {
                    "step_id": "update_cutover_doc",
                    "description": "Record the final cutover state in the checklist document.",
                    "tool": "docs.update",
                    "args": {
                        "doc_id": params.cutover_doc_id,
                        "body": (
                            "Wave 1 acquired-sales cutover.\n\n"
                            f"{params.cutover_doc_note}\n\n"
                            "Access is limited to Slack and CRM pending manager review."
                        ),
                    },
                    "expect": [
                        {
                            "kind": "result_equals",
                            "field": "doc_id",
                            "equals": params.cutover_doc_id,
                        }
                    ],
                },
                {
                    "step_id": "comment_cutover_ticket",
                    "description": "Annotate the Jira cutover tracker with the migration outcome.",
                    "tool": "jira.add_comment",
                    "args": {
                        "issue_id": params.tracking_ticket_id,
                        "body": params.ticket_update_note,
                        "author": "it-integration",
                    },
                    "expect": [
                        {
                            "kind": "result_equals",
                            "field": "issue_id",
                            "equals": params.tracking_ticket_id,
                        }
                    ],
                },
                {
                    "step_id": "post_cutover_summary",
                    "description": "Notify the migration channel once the user handoff is safe.",
                    "tool": "slack.send_message",
                    "args": {
                        "channel": params.slack_channel,
                        "text": params.slack_summary,
                    },
                },
            ],
            "success_assertions": [
                {
                    "kind": "state_equals",
                    "field": f"components.hris.employees.{params.employee_id}.identity_conflict",
                    "equals": False,
                },
                {
                    "kind": "state_equals",
                    "field": f"components.hris.employees.{params.employee_id}.onboarded",
                    "equals": True,
                },
                {
                    "kind": "state_equals",
                    "field": f"components.okta.users.{params.user_id}.status",
                    "equals": "ACTIVE",
                },
                {
                    "kind": "state_contains",
                    "field": f"components.okta.users.{params.user_id}.applications",
                    "contains": params.crm_app_id,
                },
                {
                    "kind": "state_equals",
                    "field": f"components.google_admin.drive_shares.{params.doc_id}.visibility",
                    "equals": "internal",
                },
                {
                    "kind": "state_equals",
                    "field": f"components.google_admin.drive_shares.{params.doc_id}.owner",
                    "equals": params.manager_email,
                },
                {
                    "kind": "state_equals",
                    "field": (
                        f"components.google_admin.drive_shares.{params.doc_id}."
                        "shared_with.0"
                    ),
                    "equals": params.manager_email,
                },
                {
                    "kind": "state_count_equals",
                    "field": f"components.google_admin.drive_shares.{params.doc_id}.shared_with",
                    "equals": params.allowed_share_count,
                },
                {
                    "kind": "state_not_contains",
                    "field": f"components.google_admin.drive_shares.{params.doc_id}.shared_with",
                    "contains": params.revoked_share_email,
                },
                {
                    "kind": "state_equals",
                    "field": f"components.crm.deals.{params.opportunity_id}.owner",
                    "equals": params.manager_email,
                },
                {
                    "kind": "state_contains",
                    "field": f"components.docs.docs.{params.cutover_doc_id}.body",
                    "contains": "Slack and CRM",
                },
                {
                    "kind": "state_contains",
                    "field": (
                        f"components.tickets.metadata.{params.tracking_ticket_id}.comments"
                    ),
                    "contains": "least-privilege",
                },
                {
                    "kind": "state_contains",
                    "field": f"components.slack.channels.{params.slack_channel}.messages",
                    "contains": "CRM access granted",
                },
                {
                    "kind": "time_max_ms",
                    "max_value": params.deadline_max_ms,
                    "description": "Complete onboarding before the virtual next-morning deadline.",
                },
            ],
            "failure_paths": [
                {
                    "name": "activation_depends_on_identity_resolution",
                    "trigger_step": "activate_user",
                    "recovery_steps": ["resolve_identity"],
                    "notes": "Retry activation only after HRIS identity data is clean.",
                }
            ],
            "tags": ["benchmark-family", "onboarding", "migration", variant_name],
            "metadata": {
                "benchmark_family": "enterprise_onboarding_migration",
                "workflow_variant": variant_name,
                "workflow_parameters": params.model_dump(mode="json"),
            },
        }
    )


def _build_revenue_incident_spec(
    params: RevenueIncidentMitigationWorkflowParams, *, variant_name: str
) -> WorkflowScenarioSpec:
    return WorkflowScenarioSpec.model_validate(
        {
            "name": "revenue_incident_mitigation",
            "objective": {
                "statement": (
                    "Contain a checkout incident with targeted rollback controls, "
                    "quantified revenue impact, and coordinated cross-surface recovery."
                ),
                "success": [
                    "incident acknowledged",
                    "rollout reduced",
                    "kill switch enabled",
                    "revenue impact quantified",
                    "communications updated",
                    "ticket and CRM follow-through completed",
                    "service marked recovering",
                    "incident resolved",
                ],
            },
            "world": {"catalog": "checkout_spike_mitigation"},
            "actors": [
                {"actor_id": "commerce-oncall", "role": "incident_commander"},
                {"actor_id": "release-controller", "role": "feature_flag_operator"},
            ],
            "constraints": [
                {
                    "name": "targeted_rollback",
                    "description": "Use control-plane actions before risky data writes.",
                },
                {
                    "name": "safe_recovery",
                    "description": "Resolve the incident only after mitigation is active.",
                },
                {
                    "name": "revenue_impact_recorded",
                    "description": "Quantify checkout impact before closing the page.",
                },
            ],
            "steps": [
                {
                    "step_id": "ack_incident",
                    "description": "Acknowledge the paging incident.",
                    "graph_domain": "obs_graph",
                    "graph_action": "ack_incident",
                    "args": {
                        "incident_id": params.incident_id,
                        "assignee": params.assignee,
                    },
                    "expect": [
                        {
                            "kind": "result_equals",
                            "field": "status",
                            "equals": "acknowledged",
                        }
                    ],
                },
                {
                    "step_id": "review_service",
                    "description": "Inspect the degraded checkout service before mitigation.",
                    "tool": "datadog.get_service",
                    "args": {"service_id": params.service_id},
                    "expect": [
                        {
                            "kind": "result_equals",
                            "field": "status",
                            "equals": "degraded",
                        }
                    ],
                },
                {
                    "step_id": "reduce_rollout",
                    "description": "Reduce rollout on checkout_v2 to shrink blast radius.",
                    "graph_domain": "ops_graph",
                    "graph_action": "update_rollout",
                    "args": {
                        "flag_key": params.rollout_flag_key,
                        "rollout_pct": params.rollout_pct,
                        "reason": "Contain checkout spike while assessing rollback.",
                    },
                    "expect": [
                        {
                            "kind": "result_equals",
                            "field": "rollout_pct",
                            "equals": params.rollout_pct,
                        }
                    ],
                    "on_failure": "jump:enable_kill_switch",
                },
                {
                    "step_id": "enable_kill_switch",
                    "description": "Enable the checkout kill switch as a safe fallback.",
                    "graph_domain": "ops_graph",
                    "graph_action": "set_flag",
                    "args": {
                        "flag_key": params.kill_switch_flag_key,
                        "enabled": True,
                        "reason": "Mitigate checkout failure spike.",
                    },
                    "expect": [
                        {
                            "kind": "result_equals",
                            "field": "enabled",
                            "equals": True,
                        }
                    ],
                },
                {
                    "step_id": "record_order_loss",
                    "description": "Write the estimated lost-order rate into the spreadsheet.",
                    "tool": "spreadsheet.update_cell",
                    "args": {
                        "workbook_id": params.workbook_id,
                        "sheet_id": params.sheet_id,
                        "cell": params.order_loss_cell,
                        "value": params.order_loss_per_hour,
                        "note": params.spreadsheet_note,
                    },
                    "expect": [
                        {
                            "kind": "result_equals",
                            "field": "cell",
                            "equals": params.order_loss_cell,
                        }
                    ],
                },
                {
                    "step_id": "record_revenue_loss",
                    "description": "Write the estimated revenue loss into the spreadsheet.",
                    "tool": "spreadsheet.update_cell",
                    "args": {
                        "workbook_id": params.workbook_id,
                        "sheet_id": params.sheet_id,
                        "cell": params.revenue_loss_cell,
                        "value": params.revenue_loss_usd,
                        "note": params.spreadsheet_note,
                    },
                    "expect": [
                        {
                            "kind": "result_equals",
                            "field": "cell",
                            "equals": params.revenue_loss_cell,
                        }
                    ],
                },
                {
                    "step_id": "record_impact_row",
                    "description": "Update the spreadsheet table with the quantified impact row.",
                    "tool": "spreadsheet.upsert_row",
                    "args": {
                        "workbook_id": params.workbook_id,
                        "sheet_id": params.sheet_id,
                        "match_field": "metric",
                        "match_value": "estimated_revenue_loss_usd",
                        "table_id": params.impact_table_id,
                        "row": {
                            "metric": "estimated_revenue_loss_usd",
                            "value": params.revenue_loss_usd,
                            "notes": params.spreadsheet_note,
                        },
                    },
                    "expect": [
                        {
                            "kind": "result_equals",
                            "field": "sheet_id",
                            "equals": params.sheet_id,
                        }
                    ],
                },
                {
                    "step_id": "set_impact_formula",
                    "description": "Set a formula backstop for the impact sheet.",
                    "tool": "spreadsheet.set_formula",
                    "args": {
                        "workbook_id": params.workbook_id,
                        "sheet_id": params.sheet_id,
                        "cell": params.formula_cell,
                        "formula": f"={params.order_loss_cell}*297",
                    },
                    "expect": [
                        {
                            "kind": "result_equals",
                            "field": "cell",
                            "equals": params.formula_cell,
                        }
                    ],
                },
                {
                    "step_id": "update_comms_doc",
                    "description": "Refresh the incident guidance doc with customer-facing language.",
                    "tool": "docs.update",
                    "args": {
                        "doc_id": params.comms_doc_id,
                        "body": (
                            "Checkout mitigation in progress.\n\n"
                            f"{params.doc_update_note}\n\n"
                            "Revenue impact has been quantified in the incident workbook."
                        ),
                    },
                    "expect": [
                        {
                            "kind": "result_equals",
                            "field": "doc_id",
                            "equals": params.comms_doc_id,
                        }
                    ],
                },
                {
                    "step_id": "comment_ticket",
                    "description": "Annotate the incident ticket with the mitigation state.",
                    "tool": "tickets.add_comment",
                    "args": {
                        "ticket_id": params.ticket_id,
                        "body": params.ticket_update_note,
                        "author": "commerce-ic",
                    },
                    "expect": [
                        {
                            "kind": "result_equals",
                            "field": "ticket_id",
                            "equals": params.ticket_id,
                        }
                    ],
                },
                {
                    "step_id": "log_revenue_followthrough",
                    "description": "Log the quantified impact against the active CRM deal.",
                    "tool": "crm.log_activity",
                    "args": {
                        "kind": "note",
                        "deal_id": params.deal_id,
                        "note": params.crm_activity_note,
                    },
                    "expect": [
                        {
                            "kind": "result_equals",
                            "field": "ok",
                            "equals": True,
                        }
                    ],
                },
                {
                    "step_id": "post_slack_summary",
                    "description": "Post a stakeholder summary once impact is quantified.",
                    "tool": "slack.send_message",
                    "args": {
                        "channel": params.slack_channel,
                        "text": params.slack_summary,
                    },
                },
                {
                    "step_id": "mark_service_recovering",
                    "description": "Mark the checkout service as recovering after mitigation.",
                    "tool": "datadog.update_service",
                    "args": {
                        "service_id": params.service_id,
                        "status": "recovering",
                        "note": params.recovering_note,
                    },
                    "expect": [
                        {
                            "kind": "result_equals",
                            "field": "status",
                            "equals": "recovering",
                        }
                    ],
                },
                {
                    "step_id": "resolve_ticket",
                    "description": "Resolve the ticket once rollback, docs, and CRM are updated.",
                    "tool": "tickets.transition",
                    "args": {"ticket_id": params.ticket_id, "status": "resolved"},
                    "expect": [
                        {
                            "kind": "result_equals",
                            "field": "status",
                            "equals": "resolved",
                        }
                    ],
                },
                {
                    "step_id": "resolve_incident",
                    "description": "Resolve the paging incident with the mitigation note.",
                    "tool": "pagerduty.resolve_incident",
                    "args": {
                        "incident_id": params.incident_id,
                        "note": params.resolution_note,
                    },
                    "expect": [
                        {
                            "kind": "result_equals",
                            "field": "status",
                            "equals": "resolved",
                        }
                    ],
                },
            ],
            "success_assertions": [
                {
                    "kind": "state_equals",
                    "field": f"components.pagerduty.incidents.{params.incident_id}.status",
                    "equals": "resolved",
                },
                {
                    "kind": "state_equals",
                    "field": (
                        f"components.feature_flags.flags.{params.rollout_flag_key}."
                        "rollout_pct"
                    ),
                    "equals": params.rollout_pct,
                },
                {
                    "kind": "state_equals",
                    "field": (
                        f"components.feature_flags.flags.{params.kill_switch_flag_key}."
                        "enabled"
                    ),
                    "equals": True,
                },
                {
                    "kind": "state_equals",
                    "field": f"components.datadog.services.{params.service_id}.status",
                    "equals": "recovering",
                },
                {
                    "kind": "state_contains",
                    "field": f"components.pagerduty.incidents.{params.incident_id}.notes",
                    "contains": "stabilized",
                },
                {
                    "kind": "state_equals",
                    "field": (
                        f"components.spreadsheet.workbooks.{params.workbook_id}.sheets."
                        f"{params.sheet_id}.cells.{params.order_loss_cell}"
                    ),
                    "equals": params.order_loss_per_hour,
                },
                {
                    "kind": "state_equals",
                    "field": (
                        f"components.spreadsheet.workbooks.{params.workbook_id}.sheets."
                        f"{params.sheet_id}.cells.{params.revenue_loss_cell}"
                    ),
                    "equals": params.revenue_loss_usd,
                },
                {
                    "kind": "state_equals",
                    "field": (
                        f"components.spreadsheet.workbooks.{params.workbook_id}.sheets."
                        f"{params.sheet_id}.formulas.{params.formula_cell}"
                    ),
                    "equals": f"={params.order_loss_cell}*297",
                },
                {
                    "kind": "state_contains",
                    "field": (
                        f"components.spreadsheet.workbooks.{params.workbook_id}.sheets."
                        f"{params.sheet_id}.rows"
                    ),
                    "contains": params.spreadsheet_note,
                },
                {
                    "kind": "state_contains",
                    "field": f"components.docs.docs.{params.comms_doc_id}.body",
                    "contains": "Revenue impact has been quantified",
                },
                {
                    "kind": "state_equals",
                    "field": f"components.tickets.tickets.{params.ticket_id}.status",
                    "equals": "resolved",
                },
                {
                    "kind": "state_contains",
                    "field": f"components.tickets.metadata.{params.ticket_id}.comments",
                    "contains": "impact",
                },
                {
                    "kind": "state_contains",
                    "field": "components.crm.activities",
                    "contains": "quantified",
                },
                {
                    "kind": "state_contains",
                    "field": f"components.slack.channels.{params.slack_channel}.messages",
                    "contains": "impact workbook updated",
                },
                {
                    "kind": "time_max_ms",
                    "max_value": params.deadline_max_ms,
                },
            ],
            "failure_paths": [
                {
                    "name": "rollout_reduction_falls_back_to_kill_switch",
                    "trigger_step": "reduce_rollout",
                    "recovery_steps": ["enable_kill_switch"],
                    "notes": "Use the kill switch if the rollout change does not stick.",
                },
                {
                    "name": "impact_must_be_recorded_before_resolution",
                    "trigger_step": "resolve_incident",
                    "recovery_steps": [
                        "record_order_loss",
                        "record_revenue_loss",
                        "record_impact_row",
                        "update_comms_doc",
                        "log_revenue_followthrough",
                    ],
                    "notes": "Do not close the incident before the revenue flight deck is updated.",
                },
            ],
            "tags": ["benchmark-family", "incident", "revenue", variant_name],
            "metadata": {
                "benchmark_family": "revenue_incident_mitigation",
                "workflow_variant": variant_name,
                "workflow_parameters": params.model_dump(mode="json"),
            },
        }
    )


def _build_identity_access_governance_spec(
    params: IdentityAccessGovernanceWorkflowParams, *, variant_name: str
) -> WorkflowScenarioSpec:
    base_steps: list[dict[str, Any]]
    success_assertions: list[dict[str, Any]]
    objective_success: list[str]

    if variant_name == "oversharing_remediation":
        objective_success = [
            "external sharing removed",
            "artifact trail updated",
            "stakeholder summary sent",
        ]
        base_steps = [
            {
                "step_id": "restrict_share",
                "description": "Reduce imported Drive sharing to an internal posture.",
                "graph_domain": "doc_graph",
                "graph_action": "restrict_drive_share",
                "args": {
                    "doc_id": params.doc_id,
                    "visibility": "internal",
                    "note": "Imported policy prohibits external share domains.",
                },
                "expect": [
                    {
                        "kind": "result_equals",
                        "field": "visibility",
                        "equals": "internal",
                    },
                    {
                        "kind": "result_equals",
                        "field": "shared_with_count",
                        "equals": params.allowed_share_count,
                    },
                ],
            },
            {
                "step_id": "update_governance_doc",
                "description": "Record the imported ACL remediation in the governance doc.",
                "graph_domain": "doc_graph",
                "graph_action": "update_document",
                "args": {
                    "doc_id": params.cutover_doc_id,
                    "body": params.doc_update_note,
                },
            },
            {
                "step_id": "comment_tracker",
                "description": "Annotate the tracker issue with the remediation result.",
                "graph_domain": "work_graph",
                "graph_action": "add_issue_comment",
                "args": {
                    "issue_id": params.ticket_id,
                    "body": params.ticket_note,
                    "author": "identity-admin",
                },
            },
            {
                "step_id": "post_summary",
                "description": "Notify the shared channel that imported oversharing is fixed.",
                "graph_domain": "comm_graph",
                "graph_action": "post_message",
                "args": {"channel": params.slack_channel, "text": params.slack_summary},
            },
        ]
        success_assertions = [
            {
                "kind": "state_equals",
                "field": f"components.google_admin.drive_shares.{params.doc_id}.visibility",
                "equals": "internal",
            },
            {
                "kind": "state_count_equals",
                "field": f"components.google_admin.drive_shares.{params.doc_id}.shared_with",
                "equals": params.allowed_share_count,
            },
            {
                "kind": "state_not_contains",
                "field": f"components.google_admin.drive_shares.{params.doc_id}.shared_with",
                "contains": params.revoked_share_email,
            },
            {
                "kind": "state_contains",
                "field": f"components.docs.docs.{params.cutover_doc_id}.body",
                "contains": "policy",
            },
            {
                "kind": "state_contains",
                "field": f"components.slack.channels.{params.slack_channel}.messages",
                "contains": "oversharing",
            },
        ]
    elif variant_name == "approval_bottleneck":
        objective_success = [
            "approval chain advanced",
            "approved app assigned",
            "artifact trail updated",
        ]
        base_steps = [
            {
                "step_id": "advance_approval",
                "description": "Advance the imported approval stage.",
                "graph_domain": "work_graph",
                "graph_action": "update_request_approval",
                "args": {
                    "request_id": params.request_id,
                    "approval_stage": "identity",
                    "approval_status": "APPROVED",
                    "status": "APPROVED",
                    "comment": params.request_comment,
                },
                "expect": [
                    {"kind": "result_equals", "field": "status", "equals": "APPROVED"}
                ],
            },
            {
                "step_id": "assign_primary_app",
                "description": "Grant the policy-approved application.",
                "graph_domain": "identity_graph",
                "graph_action": "assign_application",
                "args": {"user_id": params.user_id, "app_id": params.primary_app_id},
                "expect": [
                    {
                        "kind": "result_equals",
                        "field": "app_id",
                        "equals": params.primary_app_id,
                    }
                ],
            },
            {
                "step_id": "comment_tracker",
                "description": "Update the tracker with the approval outcome.",
                "graph_domain": "work_graph",
                "graph_action": "add_issue_comment",
                "args": {
                    "issue_id": params.ticket_id,
                    "body": params.ticket_note,
                    "author": "identity-admin",
                },
            },
            {
                "step_id": "post_summary",
                "description": "Notify the governance channel once the bottleneck is cleared.",
                "graph_domain": "comm_graph",
                "graph_action": "post_message",
                "args": {"channel": params.slack_channel, "text": params.slack_summary},
            },
        ]
        success_assertions = [
            {
                "kind": "state_contains",
                "field": f"components.okta.users.{params.user_id}.applications",
                "contains": params.primary_app_id,
            },
            {
                "kind": "state_contains",
                "field": f"components.servicedesk.requests.{params.request_id}.approvals",
                "contains": "APPROVED",
            },
            {
                "kind": "state_contains",
                "field": f"components.tickets.metadata.{params.ticket_id}.comments",
                "contains": "approval",
            },
            {
                "kind": "state_contains",
                "field": f"components.slack.channels.{params.slack_channel}.messages",
                "contains": "approval",
            },
        ]
    elif variant_name == "stale_entitlement_cleanup":
        objective_success = [
            "stale entitlement removed",
            "tracker updated",
            "stakeholder summary sent",
        ]
        base_steps = [
            {
                "step_id": "remove_stale_app",
                "description": "Remove imported stale application access.",
                "graph_domain": "identity_graph",
                "graph_action": "remove_application",
                "args": {"user_id": params.user_id, "app_id": params.stale_app_id},
                "expect": [
                    {
                        "kind": "result_equals",
                        "field": "app_id",
                        "equals": params.stale_app_id,
                    }
                ],
            },
            {
                "step_id": "comment_tracker",
                "description": "Record the entitlement cleanup in the tracker.",
                "graph_domain": "work_graph",
                "graph_action": "add_issue_comment",
                "args": {
                    "issue_id": params.ticket_id,
                    "body": params.ticket_note,
                    "author": "identity-admin",
                },
            },
            {
                "step_id": "post_summary",
                "description": "Notify the channel once stale access is removed.",
                "graph_domain": "comm_graph",
                "graph_action": "post_message",
                "args": {"channel": params.slack_channel, "text": params.slack_summary},
            },
        ]
        success_assertions = [
            {
                "kind": "state_not_contains",
                "field": f"components.okta.users.{params.user_id}.applications",
                "contains": params.stale_app_id,
            },
            {
                "kind": "state_contains",
                "field": f"components.tickets.metadata.{params.ticket_id}.comments",
                "contains": "stale",
            },
            {
                "kind": "state_contains",
                "field": f"components.slack.channels.{params.slack_channel}.messages",
                "contains": "stale entitlement",
            },
        ]
    else:
        objective_success = [
            "break-glass follow-up recorded",
            "temporary access removed",
            "artifacts updated",
        ]
        base_steps = [
            {
                "step_id": "remove_break_glass_app",
                "description": "Remove the imported temporary application access.",
                "graph_domain": "identity_graph",
                "graph_action": "remove_application",
                "args": {"user_id": params.user_id, "app_id": params.stale_app_id},
                "expect": [
                    {
                        "kind": "result_equals",
                        "field": "app_id",
                        "equals": params.stale_app_id,
                    }
                ],
            },
            {
                "step_id": "update_followup_doc",
                "description": "Write the break-glass follow-up into the governance doc.",
                "graph_domain": "doc_graph",
                "graph_action": "update_document",
                "args": {
                    "doc_id": params.cutover_doc_id,
                    "body": f"Break-glass follow-up.\n\n{params.doc_update_note}",
                },
            },
            {
                "step_id": "comment_tracker",
                "description": "Record the break-glass follow-up in the tracker.",
                "graph_domain": "work_graph",
                "graph_action": "add_issue_comment",
                "args": {
                    "issue_id": params.ticket_id,
                    "body": params.ticket_note,
                    "author": "security-review",
                },
            },
            {
                "step_id": "post_summary",
                "description": "Notify the channel that imported break-glass follow-up completed.",
                "graph_domain": "comm_graph",
                "graph_action": "post_message",
                "args": {"channel": params.slack_channel, "text": params.slack_summary},
            },
        ]
        success_assertions = [
            {
                "kind": "state_not_contains",
                "field": f"components.okta.users.{params.user_id}.applications",
                "contains": params.stale_app_id,
            },
            {
                "kind": "state_contains",
                "field": f"components.docs.docs.{params.cutover_doc_id}.body",
                "contains": "Break-glass",
            },
            {
                "kind": "state_contains",
                "field": f"components.tickets.metadata.{params.ticket_id}.comments",
                "contains": "break-glass",
            },
        ]

    success_assertions.append(
        {"kind": "time_max_ms", "max_value": params.deadline_max_ms}
    )
    return WorkflowScenarioSpec.model_validate(
        {
            "name": "identity_access_governance",
            "objective": {
                "statement": "Resolve imported identity governance drift using graph-native enterprise actions.",
                "success": objective_success,
            },
            "world": {"catalog": "acquired_sales_onboarding"},
            "actors": [
                {
                    "actor_id": "identity-admin",
                    "role": "Identity Admin",
                    "email": "identity-admin@example.com",
                },
                {
                    "actor_id": "sales-manager",
                    "role": "Sales Manager",
                    "email": params.manager_email,
                },
            ],
            "constraints": [
                {
                    "name": "least_privilege",
                    "description": "Keep imported access limited to the intended application set.",
                }
            ],
            "approvals": [
                {
                    "stage": "identity",
                    "approver": "identity-admin",
                    "required": variant_name == "approval_bottleneck",
                }
            ],
            "steps": base_steps,
            "success_assertions": success_assertions,
            "failure_paths": [
                {
                    "name": "artifact_followthrough_required",
                    "trigger_step": base_steps[-1]["step_id"],
                    "recovery_steps": [step["step_id"] for step in base_steps[:-1]],
                    "notes": "Artifact follow-through must be visible before the workflow is considered complete.",
                }
            ],
            "tags": ["benchmark-family", "identity", "governance", variant_name],
            "metadata": {
                "benchmark_family": "identity_access_governance",
                "workflow_variant": variant_name,
                "workflow_parameters": params.model_dump(mode="json"),
            },
        }
    )


_WORKFLOW_BUILDERS = {
    "security_containment": _build_security_containment_spec,
    "enterprise_onboarding_migration": _build_enterprise_onboarding_spec,
    "revenue_incident_mitigation": _build_revenue_incident_spec,
    "identity_access_governance": _build_identity_access_governance_spec,
}


def _parameter_value_type(value: str | int | float | bool) -> str:
    if isinstance(value, bool):
        return "bool"
    if isinstance(value, int):
        return "int"
    if isinstance(value, float):
        return "float"
    return "str"


def _variant_manifest(
    family_name: str, definition: _VariantDefinition
) -> BenchmarkWorkflowVariantManifest:
    descriptions = _PARAMETER_DESCRIPTIONS[family_name]
    parameters = [
        BenchmarkWorkflowParameter(
            name=name,
            value=value,
            value_type=_parameter_value_type(value),
            description=descriptions.get(name),
        )
        for name, value in definition.parameters.model_dump(mode="python").items()
    ]
    return BenchmarkWorkflowVariantManifest(
        family_name=family_name,
        workflow_name=family_name,
        variant_name=definition.name,
        title=definition.title,
        description=definition.description,
        scenario_name=definition.scenario_name,
        parameters=parameters,
    )


def _resolve_variant_name(family_name: str, variant_name: Optional[str]) -> str:
    catalog = _VARIANT_CATALOG[family_name]
    if variant_name is None:
        return next(iter(catalog))
    key = variant_name.strip().lower()
    if key not in catalog:
        raise KeyError(f"unknown workflow variant for {family_name}: {variant_name}")
    return key


def get_benchmark_family_workflow_spec(
    name: str,
    variant_name: Optional[str] = None,
    parameter_overrides: Optional[Dict[str, Any]] = None,
) -> WorkflowScenarioSpec:
    key = name.strip().lower()
    if key not in _VARIANT_CATALOG:
        raise KeyError(f"unknown benchmark family workflow: {name}")
    resolved_variant = _resolve_variant_name(key, variant_name)
    definition = _VARIANT_CATALOG[key][resolved_variant]
    builder = _WORKFLOW_BUILDERS[key]
    params = definition.parameters.model_copy(deep=True)
    if parameter_overrides:
        params = params.model_copy(update=dict(parameter_overrides))
    return builder(params, variant_name=resolved_variant)


def list_benchmark_family_workflow_specs() -> List[WorkflowScenarioSpec]:
    return [
        get_benchmark_family_workflow_spec(name) for name in sorted(_VARIANT_CATALOG)
    ]


def get_benchmark_family_workflow_variant(
    family_name: str, variant_name: str
) -> BenchmarkWorkflowVariantManifest:
    key = family_name.strip().lower()
    if key not in _VARIANT_CATALOG:
        raise KeyError(f"unknown benchmark family workflow: {family_name}")
    resolved_variant = _resolve_variant_name(key, variant_name)
    return _variant_manifest(key, _VARIANT_CATALOG[key][resolved_variant])


def list_benchmark_family_workflow_variants(
    family_name: Optional[str] = None,
) -> List[BenchmarkWorkflowVariantManifest]:
    family_names = (
        [family_name.strip().lower()]
        if family_name is not None
        else sorted(_VARIANT_CATALOG)
    )
    variants: List[BenchmarkWorkflowVariantManifest] = []
    for key in family_names:
        if key not in _VARIANT_CATALOG:
            raise KeyError(f"unknown benchmark family workflow: {family_name}")
        for variant_name in _VARIANT_CATALOG[key]:
            variants.append(_variant_manifest(key, _VARIANT_CATALOG[key][variant_name]))
    return variants


def resolve_benchmark_workflow_name(
    *,
    family_name: Optional[str] = None,
    scenario_name: Optional[str] = None,
) -> Optional[str]:
    if family_name:
        key = family_name.strip().lower()
        return key if key in _VARIANT_CATALOG else None
    if scenario_name:
        return _SCENARIO_TO_WORKFLOW.get(scenario_name.strip())
    return None


__all__ = [
    "BenchmarkWorkflowVariantManifest",
    "get_benchmark_family_workflow_spec",
    "get_benchmark_family_workflow_variant",
    "list_benchmark_family_workflow_specs",
    "list_benchmark_family_workflow_variants",
    "resolve_benchmark_workflow_name",
]
