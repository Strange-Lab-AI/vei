from __future__ import annotations

from dataclasses import dataclass
from typing import Dict

from vei.benchmark.workflow_models import (
    B2bSaasWorkflowParams,
    DigitalMarketingAgencyWorkflowParams,
    EnterpriseOnboardingMigrationWorkflowParams,
    IdentityAccessGovernanceWorkflowParams,
    RealEstateManagementWorkflowParams,
    RevenueIncidentMitigationWorkflowParams,
    SecurityContainmentWorkflowParams,
    ServiceOpsWorkflowParams,
    StorageSolutionsWorkflowParams,
    WorkflowParams,
)


@dataclass(frozen=True)
class _VariantDefinition:
    name: str
    title: str = ""
    description: str = ""
    scenario_name: str = ""
    parameters: WorkflowParams = None  # type: ignore[assignment]


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
    "real_estate_management": {
        "lease_id": "Lease that must be fully ready before tenant opening.",
        "work_order_id": "Maintenance work order blocking the opening.",
        "vendor_id": "Vendor assigned to clear the maintenance blocker.",
        "unit_id": "Unit reserved for the tenant opening.",
        "tenant_id": "Tenant whose opening readiness is under review.",
        "request_id": "Approval request needed to proceed with vendor execution.",
        "ticket_id": "Tracker ticket updated during the opening review.",
        "doc_id": "Opening checklist or tenant artifact updated during the workflow.",
        "slack_channel": "Slack channel used for property ops coordination.",
        "vendor_note": "Vendor assignment note recorded during the workflow.",
        "doc_update_note": "Opening checklist update written to the docs surface.",
        "ticket_note": "Tracker note written after the opening blockers are cleared.",
        "slack_summary": "Slack summary sent once opening readiness is restored.",
        "deadline_max_ms": "Virtual-time deadline for the opening-readiness workflow.",
    },
    "digital_marketing_agency": {
        "creative_id": "Creative asset that must be approved before launch.",
        "approval_id": "Approval record attached to the launch creative.",
        "request_id": "Service-request style approval record for the launch.",
        "campaign_id": "Campaign under launch guardrail review.",
        "report_id": "Client-facing or internal report artifact that must be refreshed.",
        "ticket_id": "Tracker issue updated during the launch workflow.",
        "doc_id": "Launch brief updated with the final readiness state.",
        "deal_id": "CRM object updated with the commercial launch note.",
        "slack_channel": "Slack channel used for launch coordination.",
        "pacing_pct": "Safe pacing level set before launch proceeds.",
        "report_note": "Note written when the launch report is refreshed.",
        "doc_update_note": "Launch brief update recorded in the docs surface.",
        "ticket_note": "Tracker note written after launch guardrails are cleared.",
        "crm_note": "Commercial note recorded against the client account or deal.",
        "slack_summary": "Slack summary sent after launch safety is restored.",
        "deadline_max_ms": "Virtual-time deadline for the launch guardrail workflow.",
    },
    "storage_solutions": {
        "request_id": "Approval request required before vendor dispatch.",
        "quote_id": "Strategic customer quote that must be made feasible.",
        "pool_id": "Capacity pool used to satisfy the customer request.",
        "units": "Units allocated to the strategic quote.",
        "site_id": "Site selected for the feasible storage commitment.",
        "order_id": "Downstream order that must receive vendor follow-through.",
        "vendor_id": "Vendor assigned to the fulfillment/dispatch action.",
        "ticket_id": "Tracker issue updated during the commitment workflow.",
        "doc_id": "Storage rollout or quote plan document updated during the workflow.",
        "deal_id": "CRM object updated with the commitment note.",
        "slack_channel": "Slack channel used for strategic commitment coordination.",
        "doc_update_note": "Document update recorded after feasible capacity is reserved.",
        "ticket_note": "Tracker note written after the quote becomes feasible.",
        "crm_note": "Commercial note recorded once the commitment is safe to send.",
        "slack_summary": "Slack summary posted after the commitment is aligned.",
        "deadline_max_ms": "Virtual-time deadline for the capacity-commitment workflow.",
    },
    "b2b_saas": {
        "request_id": "Support request escalated by the customer.",
        "ticket_id": "Engineering ticket blocking the customer fix.",
        "deal_id": "CRM deal representing the renewal.",
        "doc_id": "Renewal proposal or incident document updated during the workflow.",
        "contact_id": "CRM contact for the new decision-maker.",
        "slack_channel": "Slack channel used for renewal coordination.",
        "ticket_note": "Tracker note written after the integration is fixed.",
        "crm_note": "Commercial note recorded once the renewal is advanced.",
        "slack_summary": "Slack summary posted after the save plan is executed.",
        "discount_pct": "Discount percentage offered to the customer.",
        "deadline_max_ms": "Virtual-time deadline for the renewal workflow.",
    },
    "service_ops": {
        "request_id": "Approval request that advances the VIP service response.",
        "work_order_id": "Service work order that must be stabilized.",
        "appointment_id": "Field appointment that needs a valid dispatch assignment.",
        "technician_id": "Backup technician selected for the dispatch recovery.",
        "billing_case_id": "Billing case that should be held while the dispute is active.",
        "exception_id": "Primary service exception that should be explicitly resolved.",
        "ticket_id": "Tracker issue updated during the service-day recovery.",
        "doc_id": "Handoff or runbook document updated with the clean operating trail.",
        "slack_channel": "Slack channel used for dispatch and operations coordination.",
        "dispatch_note": "Dispatch assignment note recorded on the work order.",
        "billing_note": "Billing hold note recorded while the dispute remains active.",
        "doc_update_note": "Document update written after dispatch and billing are brought back into alignment.",
        "ticket_note": "Tracker note written after the service loop is stabilized.",
        "slack_summary": "Slack summary posted once the same-day operating path is safe again.",
        "deadline_max_ms": "Virtual-time deadline for the service-day recovery workflow.",
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
    "real_estate_management": {
        "tenant_opening_conflict": _VariantDefinition(
            name="tenant_opening_conflict",
            scenario_name="tenant_opening_conflict",
            parameters=RealEstateManagementWorkflowParams(),
        ),
        "vendor_no_show": _VariantDefinition(
            name="vendor_no_show",
            scenario_name="vendor_no_show",
            parameters=RealEstateManagementWorkflowParams(
                vendor_id="VEND-HPM-ELEC",
                vendor_note="Backup facilities vendor assigned after late HVAC no-show.",
                ticket_note="Backup vendor assigned, opening blockers re-sequenced, and tenant opening still viable.",
                slack_summary="Backup vendor locked in; Harbor Point still has a viable opening path.",
            ),
        ),
        "lease_revision_late": _VariantDefinition(
            name="lease_revision_late",
            scenario_name="lease_revision_late",
            parameters=RealEstateManagementWorkflowParams(
                deadline_max_ms=120000,
                doc_update_note="Late lease revision executed, vendor prep confirmed, and opening artifact refreshed under compressed time.",
                slack_summary="Late legal revision cleared just in time; Harbor Point opening path remains intact.",
            ),
        ),
        "double_booked_unit": _VariantDefinition(
            name="double_booked_unit",
            scenario_name="double_booked_unit",
            parameters=RealEstateManagementWorkflowParams(
                ticket_note="Reservation conflict cleared, unit reassigned correctly, and opening blockers resolved.",
                slack_summary="Unit conflict resolved and Harbor Point opening is aligned again.",
            ),
        ),
    },
    "digital_marketing_agency": {
        "campaign_launch_guardrail": _VariantDefinition(
            name="campaign_launch_guardrail",
            scenario_name="campaign_launch_guardrail",
            parameters=DigitalMarketingAgencyWorkflowParams(),
        ),
        "creative_not_approved": _VariantDefinition(
            name="creative_not_approved",
            scenario_name="creative_not_approved",
            parameters=DigitalMarketingAgencyWorkflowParams(
                ticket_note="Creative sign-off recovered before launch and client artifacts updated accordingly.",
                slack_summary="Creative approval cleared; launch can proceed with approved assets only.",
            ),
        ),
        "budget_runaway": _VariantDefinition(
            name="budget_runaway",
            scenario_name="budget_runaway",
            parameters=DigitalMarketingAgencyWorkflowParams(
                pacing_pct=70.0,
                report_note="Emergency pacing correction recorded after runaway spend review.",
                crm_note="Client budget protected after pacing rollback and refreshed launch reporting.",
            ),
        ),
        "client_reporting_mismatch": _VariantDefinition(
            name="client_reporting_mismatch",
            scenario_name="client_reporting_mismatch",
            parameters=DigitalMarketingAgencyWorkflowParams(
                doc_update_note="Launch brief reconciled with the refreshed client report and approval state.",
                slack_summary="Client-facing artifacts now agree on pacing, approval, and launch readiness.",
            ),
        ),
    },
    "storage_solutions": {
        "capacity_quote_commitment": _VariantDefinition(
            name="capacity_quote_commitment",
            scenario_name="capacity_quote_commitment",
            parameters=StorageSolutionsWorkflowParams(),
        ),
        "vendor_dispatch_gap": _VariantDefinition(
            name="vendor_dispatch_gap",
            scenario_name="vendor_dispatch_gap",
            parameters=StorageSolutionsWorkflowParams(
                vendor_id="VEND-ATS-TRUCK",
                ticket_note="Dispatch vendor gap closed after feasible allocation and revised commitment.",
                slack_summary="Vendor dispatch back on track; Zenith commitment is now feasible to send.",
            ),
        ),
        "fragmented_capacity": _VariantDefinition(
            name="fragmented_capacity",
            scenario_name="fragmented_capacity",
            parameters=StorageSolutionsWorkflowParams(
                pool_id="POOL-CHI-A",
                site_id="SITE-CHI-1",
                units=20,
                doc_update_note="Fragmented inventory resolved by shifting the commitment to the feasible Chicago block and updating rollout artifacts.",
            ),
        ),
        "overcommit_quote_risk": _VariantDefinition(
            name="overcommit_quote_risk",
            scenario_name="overcommit_quote_risk",
            parameters=StorageSolutionsWorkflowParams(
                units=60,
                crm_note="Unsafe overcommit removed after feasible storage plan and revised quote were confirmed.",
            ),
        ),
    },
    "b2b_saas": {
        "enterprise_renewal_risk": _VariantDefinition(
            name="enterprise_renewal_risk",
            scenario_name="enterprise_renewal_risk",
            parameters=B2bSaasWorkflowParams(),
        ),
        "support_escalation_spiral": _VariantDefinition(
            name="support_escalation_spiral",
            scenario_name="support_escalation_spiral",
            parameters=B2bSaasWorkflowParams(
                ticket_note="P1 ownership assigned, fix deployed, and customer confirmation received.",
                slack_summary="Apex P1 resolved and post-incident review scheduled.",
            ),
        ),
        "pricing_negotiation_deadlock": _VariantDefinition(
            name="pricing_negotiation_deadlock",
            scenario_name="pricing_negotiation_deadlock",
            parameters=B2bSaasWorkflowParams(
                discount_pct=40,
                crm_note="Renewal terms agreed after structured negotiation and value demonstration.",
            ),
        ),
    },
    "service_ops": {
        "service_day_collision": _VariantDefinition(
            name="service_day_collision",
            scenario_name="service_day_collision",
            parameters=ServiceOpsWorkflowParams(),
        ),
        "technician_no_show": _VariantDefinition(
            name="technician_no_show",
            scenario_name="technician_no_show",
            parameters=ServiceOpsWorkflowParams(
                billing_note="Billing remains stable while dispatch recovers the technician gap.",
                slack_summary="Backup dispatch locked in and the SLA stays intact for Clearwater Medical.",
            ),
        ),
        "billing_dispute_reopened": _VariantDefinition(
            name="billing_dispute_reopened",
            scenario_name="billing_dispute_reopened",
            parameters=ServiceOpsWorkflowParams(
                billing_note="Collections hold recorded immediately after the account dispute reopened.",
                ticket_note="Billing dispute reopened, hold recorded, and dispatch state kept consistent across teams.",
                slack_summary="Billing safely contained while the field response remains coherent for Clearwater Medical.",
            ),
        ),
    },
}

_SCENARIO_TO_WORKFLOW = {
    "oauth_app_containment": "security_containment",
    "acquired_sales_onboarding": "enterprise_onboarding_migration",
    "checkout_spike_mitigation": "revenue_incident_mitigation",
    "tenant_opening_conflict": "real_estate_management",
    "vendor_no_show": "real_estate_management",
    "lease_revision_late": "real_estate_management",
    "double_booked_unit": "real_estate_management",
    "campaign_launch_guardrail": "digital_marketing_agency",
    "creative_not_approved": "digital_marketing_agency",
    "budget_runaway": "digital_marketing_agency",
    "client_reporting_mismatch": "digital_marketing_agency",
    "capacity_quote_commitment": "storage_solutions",
    "vendor_dispatch_gap": "storage_solutions",
    "fragmented_capacity": "storage_solutions",
    "overcommit_quote_risk": "storage_solutions",
    "enterprise_renewal_risk": "b2b_saas",
    "support_escalation_spiral": "b2b_saas",
    "pricing_negotiation_deadlock": "b2b_saas",
    "service_day_collision": "service_ops",
    "technician_no_show": "service_ops",
    "billing_dispute_reopened": "service_ops",
}
