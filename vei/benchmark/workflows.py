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


class RealEstateManagementWorkflowParams(BaseModel):
    lease_id: str = "LEASE-HPM-14A"
    work_order_id: str = "WO-HPM-88"
    vendor_id: str = "VEND-HPM-HVAC"
    unit_id: str = "UNIT-HPM-14A"
    tenant_id: str = "TEN-HPM-ANCHOR"
    request_id: str = "REQ-HPM-1"
    ticket_id: str = "JRA-HPM-17"
    doc_id: str = "DOC-HPM-OPENING"
    slack_channel: str = "#harbor-point-ops"
    vendor_note: str = (
        "HVAC vendor assigned and prep window confirmed for Monday opening."
    )
    doc_update_note: str = (
        "lease amendment executed, vendor assigned, and unit reserved for Monday opening."
    )
    ticket_note: str = (
        "Opening blockers cleared: lease amendment executed, HVAC vendor assigned, and unit reserved."
    )
    slack_summary: str = (
        "Harbor Point opening is back on track: lease, vendor, and unit readiness are aligned."
    )
    deadline_max_ms: int = 180_000


class DigitalMarketingAgencyWorkflowParams(BaseModel):
    creative_id: str = "CRT-APEX-01"
    approval_id: str = "APR-APEX-01"
    request_id: str = "REQ-NSG-1"
    campaign_id: str = "CMP-APEX-01"
    report_id: str = "RPT-APEX-01"
    ticket_id: str = "JRA-NSG-33"
    doc_id: str = "DOC-NSG-LAUNCH"
    deal_id: str = "CRM-NSG-D1"
    slack_channel: str = "#northstar-launch"
    pacing_pct: float = 80.0
    report_note: str = (
        "Launch guardrail review completed; refreshed pacing and approval state captured."
    )
    doc_update_note: str = (
        "Creative approval complete, pacing normalized, and refreshed report linked for client launch."
    )
    ticket_note: str = (
        "Launch guardrails cleared: creative approved, pacing corrected, and report refreshed."
    )
    crm_note: str = (
        "Client launch risk reduced after approval, pacing, and artifact updates."
    )
    slack_summary: str = (
        "Apex Health launch is safe to proceed with approved creative and normalized pacing."
    )
    deadline_max_ms: int = 180_000


class StorageSolutionsWorkflowParams(BaseModel):
    request_id: str = "REQ-ATS-1"
    quote_id: str = "Q-ATS-900"
    pool_id: str = "POOL-MKE-B"
    units: int = 80
    site_id: str = "SITE-MKE-1"
    order_id: str = "ORD-ATS-900"
    vendor_id: str = "VEND-ATS-OPS"
    ticket_id: str = "JRA-ATS-51"
    doc_id: str = "DOC-ATS-QUOTE"
    deal_id: str = "CRM-ATS-D1"
    slack_channel: str = "#atlas-commitments"
    doc_update_note: str = (
        "Capacity reserved at Milwaukee West, quote revised to feasible commitment, and fulfillment vendor assigned."
    )
    ticket_note: str = (
        "Strategic quote is feasible: capacity allocated, quote revised, and vendor dispatch aligned."
    )
    crm_note: str = (
        "Quote risk reduced after feasible capacity reservation and ops alignment."
    )
    slack_summary: str = (
        "Zenith commitment is safe to send with feasible capacity, revised quote, and vendor follow-through."
    )
    deadline_max_ms: int = 180_000


class B2bSaasWorkflowParams(BaseModel):
    request_id: str = "SR-PIN-201"
    ticket_id: str = "JRA-PIN-101"
    deal_id: str = "DEAL-APEX-RENEWAL"
    doc_id: str = "DOC-PIN-PROPOSAL"
    contact_id: str = "CON-JORDAN"
    slack_channel: str = "#apex-renewal"
    ticket_note: str = (
        "Integration fix deployed, customer confirmed, and renewal proposal advanced."
    )
    crm_note: str = (
        "Apex renewal back on track after integration restore and stakeholder engagement."
    )
    slack_summary: str = (
        "Apex integration fixed, exec sponsor engaged, and renewal proposal ready to send."
    )
    discount_pct: int = 0
    deadline_max_ms: int = 180_000


WorkflowParams = (
    SecurityContainmentWorkflowParams
    | EnterpriseOnboardingMigrationWorkflowParams
    | RevenueIncidentMitigationWorkflowParams
    | IdentityAccessGovernanceWorkflowParams
    | RealEstateManagementWorkflowParams
    | DigitalMarketingAgencyWorkflowParams
    | StorageSolutionsWorkflowParams
    | B2bSaasWorkflowParams
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
            title="Tenant Opening Conflict",
            description=(
                "Restore tenant opening readiness by aligning lease, vendor, unit, and artifact state before Monday."
            ),
            scenario_name="tenant_opening_conflict",
            parameters=RealEstateManagementWorkflowParams(),
        ),
        "vendor_no_show": _VariantDefinition(
            name="vendor_no_show",
            title="Vendor No-Show",
            description="Recover opening readiness after a blocking prep vendor no-shows late in the cycle.",
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
            title="Lease Revision Late",
            description="Compress the readiness flow after late legal redlines change the opening timeline.",
            scenario_name="lease_revision_late",
            parameters=RealEstateManagementWorkflowParams(
                deadline_max_ms=120000,
                doc_update_note="Late lease revision executed, vendor prep confirmed, and opening artifact refreshed under compressed time.",
                slack_summary="Late legal revision cleared just in time; Harbor Point opening path remains intact.",
            ),
        ),
        "double_booked_unit": _VariantDefinition(
            name="double_booked_unit",
            title="Double-Booked Unit",
            description="Resolve a reservation conflict on the tenant unit before the opening fails downstream.",
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
            title="Campaign Launch Guardrail",
            description=(
                "Clear creative approval, normalize pacing, refresh reporting, and update client artifacts before launch."
            ),
            scenario_name="campaign_launch_guardrail",
            parameters=DigitalMarketingAgencyWorkflowParams(),
        ),
        "creative_not_approved": _VariantDefinition(
            name="creative_not_approved",
            title="Creative Not Approved",
            description="Recover a launch after client creative approval regresses back into rework.",
            scenario_name="creative_not_approved",
            parameters=DigitalMarketingAgencyWorkflowParams(
                ticket_note="Creative sign-off recovered before launch and client artifacts updated accordingly.",
                slack_summary="Creative approval cleared; launch can proceed with approved assets only.",
            ),
        ),
        "budget_runaway": _VariantDefinition(
            name="budget_runaway",
            title="Budget Runaway",
            description="Pull pacing back under control after the launch starts burning budget too quickly.",
            scenario_name="budget_runaway",
            parameters=DigitalMarketingAgencyWorkflowParams(
                pacing_pct=70.0,
                report_note="Emergency pacing correction recorded after runaway spend review.",
                crm_note="Client budget protected after pacing rollback and refreshed launch reporting.",
            ),
        ),
        "client_reporting_mismatch": _VariantDefinition(
            name="client_reporting_mismatch",
            title="Client Reporting Mismatch",
            description="Reconcile client-facing launch artifacts when the report and brief drift apart.",
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
            title="Capacity Quote Commitment",
            description=(
                "Make a strategic storage quote feasible by reserving capacity, revising the commitment, and aligning vendor follow-through."
            ),
            scenario_name="capacity_quote_commitment",
            parameters=StorageSolutionsWorkflowParams(),
        ),
        "vendor_dispatch_gap": _VariantDefinition(
            name="vendor_dispatch_gap",
            title="Vendor Dispatch Gap",
            description="Repair a feasible quote after downstream dispatch coverage breaks.",
            scenario_name="vendor_dispatch_gap",
            parameters=StorageSolutionsWorkflowParams(
                vendor_id="VEND-ATS-TRUCK",
                ticket_note="Dispatch vendor gap closed after feasible allocation and revised commitment.",
                slack_summary="Vendor dispatch back on track; Zenith commitment is now feasible to send.",
            ),
        ),
        "fragmented_capacity": _VariantDefinition(
            name="fragmented_capacity",
            title="Fragmented Capacity",
            description="Find a smaller feasible inventory block after overflow capacity fragments unexpectedly.",
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
            title="Overcommit Quote Risk",
            description="Unwind an already-overcommitted quote before the customer hears the wrong number.",
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
            title="Enterprise Renewal at Risk",
            description="Save a $480K renewal by fixing the integration, rebuilding trust, and neutralizing competitive pressure.",
            scenario_name="enterprise_renewal_risk",
            parameters=B2bSaasWorkflowParams(),
        ),
        "support_escalation_spiral": _VariantDefinition(
            name="support_escalation_spiral",
            title="Support Escalation Spiral",
            description="Resolve a P1 bouncing between teams before the customer loses patience.",
            scenario_name="support_escalation_spiral",
            parameters=B2bSaasWorkflowParams(
                ticket_note="P1 ownership assigned, fix deployed, and customer confirmation received.",
                slack_summary="Apex P1 resolved and post-incident review scheduled.",
            ),
        ),
        "pricing_negotiation_deadlock": _VariantDefinition(
            name="pricing_negotiation_deadlock",
            title="Pricing Negotiation Deadlock",
            description="Break a pricing stalemate without giving away all the margin.",
            scenario_name="pricing_negotiation_deadlock",
            parameters=B2bSaasWorkflowParams(
                discount_pct=40,
                crm_note="Renewal terms agreed after structured negotiation and value demonstration.",
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


def _build_real_estate_management_spec(
    params: RealEstateManagementWorkflowParams, *, variant_name: str
) -> WorkflowScenarioSpec:
    return WorkflowScenarioSpec.model_validate(
        {
            "name": "real_estate_management",
            "objective": {
                "statement": "Restore tenant opening readiness without losing lease, vendor, and artifact consistency.",
                "success": [
                    "lease ready",
                    "vendor assigned",
                    "unit reserved",
                    "artifacts updated",
                ],
            },
            "world": {"catalog": "tenant_opening_conflict"},
            "actors": [
                {"actor_id": "property-ops", "role": "Property Ops Lead"},
                {"actor_id": "leasing", "role": "Leasing Manager"},
            ],
            "constraints": [
                {
                    "name": "tenant_readiness",
                    "description": "Do not allow an invalid or unprepared tenant opening.",
                }
            ],
            "approvals": [
                {"stage": "vendor", "approver": "property-ops", "required": True}
            ],
            "steps": [
                {
                    "step_id": "approve_vendor_request",
                    "description": "Advance the vendor approval request for tenant prep.",
                    "graph_domain": "work_graph",
                    "graph_action": "update_request_approval",
                    "args": {
                        "request_id": params.request_id,
                        "approval_stage": "vendor",
                        "approval_status": "APPROVED",
                        "comment": "Vendor prep approved for anchor tenant opening.",
                    },
                },
                {
                    "step_id": "execute_lease",
                    "description": "Execute the pending lease amendment.",
                    "graph_domain": "property_graph",
                    "graph_action": "update_lease_milestone",
                    "args": {
                        "lease_id": params.lease_id,
                        "milestone": "executed",
                        "status": "ready",
                    },
                },
                {
                    "step_id": "assign_hvac_vendor",
                    "description": "Assign the HVAC vendor to the blocking work order.",
                    "graph_domain": "property_graph",
                    "graph_action": "assign_vendor",
                    "args": {
                        "work_order_id": params.work_order_id,
                        "vendor_id": params.vendor_id,
                        "note": params.vendor_note,
                    },
                },
                {
                    "step_id": "reserve_opening_unit",
                    "description": "Reserve the opening unit for the anchor tenant.",
                    "graph_domain": "property_graph",
                    "graph_action": "reserve_unit",
                    "args": {
                        "unit_id": params.unit_id,
                        "tenant_id": params.tenant_id,
                        "status": "reserved",
                    },
                },
                {
                    "step_id": "update_opening_checklist",
                    "description": "Refresh the opening checklist artifact.",
                    "graph_domain": "doc_graph",
                    "graph_action": "update_document",
                    "args": {"doc_id": params.doc_id, "body": params.doc_update_note},
                },
                {
                    "step_id": "comment_tracker",
                    "description": "Write the property tracker follow-through note.",
                    "graph_domain": "work_graph",
                    "graph_action": "add_issue_comment",
                    "args": {"issue_id": params.ticket_id, "body": params.ticket_note},
                },
                {
                    "step_id": "post_summary",
                    "description": "Post the opening-ready summary to Slack.",
                    "graph_domain": "comm_graph",
                    "graph_action": "post_message",
                    "args": {
                        "channel": params.slack_channel,
                        "text": params.slack_summary,
                    },
                },
            ],
            "success_assertions": [
                {
                    "kind": "state_equals",
                    "field": f"components.property_ops.leases.{params.lease_id}.milestone",
                    "equals": "executed",
                },
                {
                    "kind": "state_equals",
                    "field": f"components.property_ops.leases.{params.lease_id}.status",
                    "equals": "ready",
                },
                {
                    "kind": "state_equals",
                    "field": f"components.property_ops.work_orders.{params.work_order_id}.vendor_id",
                    "equals": params.vendor_id,
                },
                {
                    "kind": "state_equals",
                    "field": f"components.property_ops.units.{params.unit_id}.reserved_for",
                    "equals": params.tenant_id,
                },
                {
                    "kind": "state_contains",
                    "field": f"components.docs.docs.{params.doc_id}.body",
                    "contains": params.doc_update_note,
                },
                {
                    "kind": "state_contains",
                    "field": f"components.tickets.metadata.{params.ticket_id}.comments",
                    "contains": params.ticket_note,
                },
                {
                    "kind": "state_contains",
                    "field": f"components.slack.channels.{params.slack_channel}.messages",
                    "contains": params.slack_summary,
                },
                {"kind": "time_max_ms", "max_value": params.deadline_max_ms},
            ],
            "failure_paths": [
                {
                    "name": "opening_ready_branch",
                    "trigger_step": "assign_hvac_vendor",
                    "recovery_steps": [
                        "execute_lease",
                        "reserve_opening_unit",
                        "update_opening_checklist",
                    ],
                    "notes": "Branch if vendor assignment lands but lease or unit state still blocks the opening.",
                }
            ],
            "tags": ["benchmark-family", "vertical", "real-estate", variant_name],
            "metadata": {
                "benchmark_family": "real_estate_management",
                "workflow_variant": variant_name,
                "workflow_parameters": params.model_dump(mode="json"),
            },
        }
    )


def _build_digital_marketing_agency_spec(
    params: DigitalMarketingAgencyWorkflowParams, *, variant_name: str
) -> WorkflowScenarioSpec:
    return WorkflowScenarioSpec.model_validate(
        {
            "name": "digital_marketing_agency",
            "objective": {
                "statement": "Make the client launch safe by clearing approvals, pacing, and reporting drift before spend burns.",
                "success": [
                    "creative approved",
                    "pacing normalized",
                    "report refreshed",
                    "client artifacts updated",
                ],
            },
            "world": {"catalog": "campaign_launch_guardrail"},
            "actors": [
                {"actor_id": "account-lead", "role": "Account Lead"},
                {"actor_id": "creative-director", "role": "Creative Director"},
            ],
            "constraints": [
                {
                    "name": "launch_safety",
                    "description": "Do not launch unapproved or overspending campaign state.",
                }
            ],
            "approvals": [
                {"stage": "creative", "approver": "creative-director", "required": True}
            ],
            "steps": [
                {
                    "step_id": "advance_launch_request",
                    "description": "Advance the launch request approval.",
                    "graph_domain": "work_graph",
                    "graph_action": "update_request_approval",
                    "args": {
                        "request_id": params.request_id,
                        "approval_stage": "creative",
                        "approval_status": "APPROVED",
                        "comment": "Creative sign-off captured for launch guardrail.",
                    },
                },
                {
                    "step_id": "approve_creative",
                    "description": "Approve the pending launch creative.",
                    "graph_domain": "campaign_graph",
                    "graph_action": "approve_creative",
                    "args": {
                        "creative_id": params.creative_id,
                        "approval_id": params.approval_id,
                    },
                },
                {
                    "step_id": "normalize_pacing",
                    "description": "Reduce pacing to a safe launch level.",
                    "graph_domain": "campaign_graph",
                    "graph_action": "adjust_budget_pacing",
                    "args": {
                        "campaign_id": params.campaign_id,
                        "pacing_pct": params.pacing_pct,
                    },
                },
                {
                    "step_id": "refresh_report",
                    "description": "Refresh the stale launch report artifact.",
                    "graph_domain": "campaign_graph",
                    "graph_action": "publish_report_note",
                    "args": {"report_id": params.report_id, "note": params.report_note},
                },
                {
                    "step_id": "update_launch_brief",
                    "description": "Update the launch brief for the client team.",
                    "graph_domain": "doc_graph",
                    "graph_action": "update_document",
                    "args": {"doc_id": params.doc_id, "body": params.doc_update_note},
                },
                {
                    "step_id": "annotate_tracker",
                    "description": "Update the tracker issue.",
                    "graph_domain": "work_graph",
                    "graph_action": "add_issue_comment",
                    "args": {"issue_id": params.ticket_id, "body": params.ticket_note},
                },
                {
                    "step_id": "log_client_note",
                    "description": "Record the client/commercial note.",
                    "graph_domain": "revenue_graph",
                    "graph_action": "log_activity",
                    "args": {
                        "kind": "note",
                        "deal_id": params.deal_id,
                        "note": params.crm_note,
                    },
                },
                {
                    "step_id": "post_launch_summary",
                    "description": "Post the launch-safe summary to Slack.",
                    "graph_domain": "comm_graph",
                    "graph_action": "post_message",
                    "args": {
                        "channel": params.slack_channel,
                        "text": params.slack_summary,
                    },
                },
            ],
            "success_assertions": [
                {
                    "kind": "state_equals",
                    "field": f"components.campaign_ops.creatives.{params.creative_id}.status",
                    "equals": "approved",
                },
                {
                    "kind": "state_equals",
                    "field": f"components.campaign_ops.approvals.{params.approval_id}.status",
                    "equals": "approved",
                },
                {
                    "kind": "state_equals",
                    "field": f"components.campaign_ops.campaigns.{params.campaign_id}.pacing_pct",
                    "equals": params.pacing_pct,
                },
                {
                    "kind": "state_equals",
                    "field": f"components.campaign_ops.reports.{params.report_id}.stale",
                    "equals": False,
                },
                {
                    "kind": "state_contains",
                    "field": f"components.docs.docs.{params.doc_id}.body",
                    "contains": params.doc_update_note,
                },
                {
                    "kind": "state_contains",
                    "field": f"components.tickets.metadata.{params.ticket_id}.comments",
                    "contains": params.ticket_note,
                },
                {
                    "kind": "state_contains",
                    "field": "components.crm.activities",
                    "contains": params.crm_note,
                },
                {
                    "kind": "state_contains",
                    "field": f"components.slack.channels.{params.slack_channel}.messages",
                    "contains": params.slack_summary,
                },
                {"kind": "time_max_ms", "max_value": params.deadline_max_ms},
            ],
            "failure_paths": [
                {
                    "name": "pause_launch_branch",
                    "trigger_step": "normalize_pacing",
                    "recovery_steps": [
                        "refresh_report",
                        "update_launch_brief",
                        "post_launch_summary",
                    ],
                    "notes": "Branch if pacing is corrected but approval/artifact follow-through still drifts.",
                }
            ],
            "tags": ["benchmark-family", "vertical", "marketing", variant_name],
            "metadata": {
                "benchmark_family": "digital_marketing_agency",
                "workflow_variant": variant_name,
                "workflow_parameters": params.model_dump(mode="json"),
            },
        }
    )


def _build_storage_solutions_spec(
    params: StorageSolutionsWorkflowParams, *, variant_name: str
) -> WorkflowScenarioSpec:
    return WorkflowScenarioSpec.model_validate(
        {
            "name": "storage_solutions",
            "objective": {
                "statement": "Turn a risky storage quote into a feasible commitment before the customer hears a bad number.",
                "success": [
                    "capacity feasible",
                    "quote revised",
                    "vendor action assigned",
                    "artifacts updated",
                ],
            },
            "world": {"catalog": "capacity_quote_commitment"},
            "actors": [
                {"actor_id": "solutions-engineer", "role": "Solutions Engineer"},
                {"actor_id": "ops-lead", "role": "Operations Lead"},
            ],
            "constraints": [
                {
                    "name": "capacity_feasibility",
                    "description": "Do not commit more capacity than the network can actually support.",
                }
            ],
            "approvals": [
                {"stage": "dispatch", "approver": "ops-lead", "required": True}
            ],
            "steps": [
                {
                    "step_id": "advance_dispatch_request",
                    "description": "Advance the dispatch approval request.",
                    "graph_domain": "work_graph",
                    "graph_action": "update_request_approval",
                    "args": {
                        "request_id": params.request_id,
                        "approval_stage": "dispatch",
                        "approval_status": "APPROVED",
                        "comment": "Dispatch approval captured for strategic storage commitment.",
                    },
                },
                {
                    "step_id": "allocate_capacity",
                    "description": "Reserve feasible capacity for the strategic quote.",
                    "graph_domain": "inventory_graph",
                    "graph_action": "allocate_capacity",
                    "args": {
                        "quote_id": params.quote_id,
                        "pool_id": params.pool_id,
                        "units": params.units,
                    },
                },
                {
                    "step_id": "revise_quote",
                    "description": "Revise the quote to the feasible site and commitment.",
                    "graph_domain": "inventory_graph",
                    "graph_action": "revise_quote",
                    "args": {
                        "quote_id": params.quote_id,
                        "site_id": params.site_id,
                        "committed_units": params.units,
                    },
                },
                {
                    "step_id": "assign_vendor_action",
                    "description": "Assign downstream vendor execution.",
                    "graph_domain": "inventory_graph",
                    "graph_action": "assign_vendor_action",
                    "args": {
                        "order_id": params.order_id,
                        "vendor_id": params.vendor_id,
                        "status": "scheduled",
                    },
                },
                {
                    "step_id": "update_rollout_plan",
                    "description": "Update the rollout plan artifact.",
                    "graph_domain": "doc_graph",
                    "graph_action": "update_document",
                    "args": {"doc_id": params.doc_id, "body": params.doc_update_note},
                },
                {
                    "step_id": "comment_tracker",
                    "description": "Update the tracker with feasible commitment details.",
                    "graph_domain": "work_graph",
                    "graph_action": "add_issue_comment",
                    "args": {"issue_id": params.ticket_id, "body": params.ticket_note},
                },
                {
                    "step_id": "log_account_note",
                    "description": "Record the commercial commitment note.",
                    "graph_domain": "revenue_graph",
                    "graph_action": "log_activity",
                    "args": {
                        "kind": "note",
                        "deal_id": params.deal_id,
                        "note": params.crm_note,
                    },
                },
                {
                    "step_id": "post_commitment_summary",
                    "description": "Post the safe commitment summary in Slack.",
                    "graph_domain": "comm_graph",
                    "graph_action": "post_message",
                    "args": {
                        "channel": params.slack_channel,
                        "text": params.slack_summary,
                    },
                },
            ],
            "success_assertions": [
                {
                    "kind": "state_equals",
                    "field": f"components.inventory_ops.quotes.{params.quote_id}.site_id",
                    "equals": params.site_id,
                },
                {
                    "kind": "state_equals",
                    "field": f"components.inventory_ops.quotes.{params.quote_id}.committed_units",
                    "equals": params.units,
                },
                {
                    "kind": "state_equals",
                    "field": f"components.inventory_ops.orders.{params.order_id}.vendor_id",
                    "equals": params.vendor_id,
                },
                {
                    "kind": "state_contains",
                    "field": "components.inventory_ops.allocations",
                    "contains": params.quote_id,
                },
                {
                    "kind": "state_contains",
                    "field": f"components.docs.docs.{params.doc_id}.body",
                    "contains": params.doc_update_note,
                },
                {
                    "kind": "state_contains",
                    "field": f"components.tickets.metadata.{params.ticket_id}.comments",
                    "contains": params.ticket_note,
                },
                {
                    "kind": "state_contains",
                    "field": "components.crm.activities",
                    "contains": params.crm_note,
                },
                {
                    "kind": "state_contains",
                    "field": f"components.slack.channels.{params.slack_channel}.messages",
                    "contains": params.slack_summary,
                },
                {"kind": "time_max_ms", "max_value": params.deadline_max_ms},
            ],
            "failure_paths": [
                {
                    "name": "overcommit_branch",
                    "trigger_step": "allocate_capacity",
                    "recovery_steps": [
                        "revise_quote",
                        "assign_vendor_action",
                        "post_commitment_summary",
                    ],
                    "notes": "Branch if capacity is partially reserved but quote or vendor execution is still inconsistent.",
                }
            ],
            "tags": ["benchmark-family", "vertical", "storage", variant_name],
            "metadata": {
                "benchmark_family": "storage_solutions",
                "workflow_variant": variant_name,
                "workflow_parameters": params.model_dump(mode="json"),
            },
        }
    )


def _build_b2b_saas_spec(
    params: B2bSaasWorkflowParams, *, variant_name: str
) -> WorkflowScenarioSpec:
    return WorkflowScenarioSpec.model_validate(
        {
            "name": "b2b_saas",
            "objective": {
                "statement": "Save a $480K enterprise renewal by fixing the product, rebuilding trust, and closing the deal.",
                "success": [
                    "integration fixed",
                    "stakeholder engaged",
                    "competitive threat neutralized",
                    "renewal advanced",
                ],
            },
            "world": {"catalog": "enterprise_renewal_risk"},
            "actors": [
                {"actor_id": "cs-lead", "role": "Customer Success Lead"},
                {"actor_id": "sales-rep", "role": "Account Executive"},
            ],
            "constraints": [
                {
                    "name": "customer_trust",
                    "description": "Do not advance the commercial conversation before the product issue is resolved.",
                }
            ],
            "approvals": [
                {
                    "stage": "engineering",
                    "approver": "product-lead",
                    "required": True,
                }
            ],
            "steps": [
                {
                    "step_id": "advance_fix_approval",
                    "description": "Approve the hotfix release.",
                    "graph_domain": "work_graph",
                    "graph_action": "update_request_approval",
                    "args": {
                        "request_id": params.request_id,
                        "approval_stage": "engineering",
                        "approval_status": "APPROVED",
                        "comment": "Hotfix approved for customer-facing release.",
                    },
                },
                {
                    "step_id": "resolve_p1",
                    "description": "Close the P1 ticket after the fix is deployed.",
                    "graph_domain": "work_graph",
                    "graph_action": "add_issue_comment",
                    "args": {
                        "issue_id": params.ticket_id,
                        "body": params.ticket_note,
                    },
                },
                {
                    "step_id": "update_renewal_doc",
                    "description": "Update the renewal proposal with the fix evidence.",
                    "graph_domain": "doc_graph",
                    "graph_action": "update_document",
                    "args": {
                        "doc_id": params.doc_id,
                        "body": "Renewal proposal updated with integration fix confirmation and success metrics.",
                    },
                },
                {
                    "step_id": "log_renewal_activity",
                    "description": "Record the renewal progress in CRM.",
                    "graph_domain": "revenue_graph",
                    "graph_action": "log_activity",
                    "args": {
                        "kind": "note",
                        "deal_id": params.deal_id,
                        "note": params.crm_note,
                    },
                },
                {
                    "step_id": "post_save_summary",
                    "description": "Post the renewal save plan summary to Slack.",
                    "graph_domain": "comm_graph",
                    "graph_action": "post_message",
                    "args": {
                        "channel": params.slack_channel,
                        "text": params.slack_summary,
                    },
                },
            ],
            "success_assertions": [
                {
                    "kind": "state_contains",
                    "field": f"components.tickets.metadata.{params.ticket_id}.comments",
                    "contains": params.ticket_note,
                },
                {
                    "kind": "state_contains",
                    "field": "components.crm.activities",
                    "contains": params.crm_note,
                },
                {
                    "kind": "state_contains",
                    "field": f"components.slack.channels.{params.slack_channel}.messages",
                    "contains": params.slack_summary,
                },
                {"kind": "time_max_ms", "max_value": params.deadline_max_ms},
            ],
            "failure_paths": [
                {
                    "name": "discount_without_fix",
                    "trigger_step": "log_renewal_activity",
                    "recovery_steps": [
                        "resolve_p1",
                        "update_renewal_doc",
                        "post_save_summary",
                    ],
                    "notes": "Branch if the commercial move happens before the product fix lands.",
                }
            ],
            "tags": ["benchmark-family", "vertical", "saas", variant_name],
            "metadata": {
                "benchmark_family": "b2b_saas",
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
    "real_estate_management": _build_real_estate_management_spec,
    "digital_marketing_agency": _build_digital_marketing_agency_spec,
    "storage_solutions": _build_storage_solutions_spec,
    "b2b_saas": _build_b2b_saas_spec,
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
