from __future__ import annotations

from pydantic import BaseModel


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


class ServiceOpsWorkflowParams(BaseModel):
    request_id: str = "SR-CFS-100"
    work_order_id: str = "WO-CFS-100"
    appointment_id: str = "APT-CFS-100"
    technician_id: str = "TECH-CFS-02"
    billing_case_id: str = "BILL-CFS-100"
    exception_id: str = "EXC-CFS-101"
    ticket_id: str = "JRA-CFS-10"
    doc_id: str = "DOC-CFS-HANDOFF"
    slack_channel: str = "#clearwater-dispatch"
    dispatch_note: str = (
        "Backup controls technician assigned and VIP appointment stabilized."
    )
    billing_note: str = (
        "Disputed invoice remains on hold while the VIP service response is coordinated."
    )
    doc_update_note: str = (
        "Backup dispatch assigned, billing hold linked to the dispute, and field-to-billing handoff aligned."
    )
    ticket_note: str = (
        "VIP service loop stabilized: dispatch reassigned, billing held safely, and exceptions explicitly resolved."
    )
    slack_summary: str = (
        "Clearwater Medical is stabilized: backup tech dispatched, billing held, and one coherent customer path restored."
    )
    deadline_max_ms: int = 180_000


class KnowledgeAuthoringWorkflowParams(BaseModel):
    subject_object_ref: str = "crm_deal:CRM-NSG-D1"
    template_id: str = "proposal_v1"
    target: str = "proposal"
    prompt: str = (
        "Draft a grounded proposal that explains the phased launch path, delivery health, pricing, and approval guardrails."
    )
    expected_artifact_id: str = "ART-0001"
    required_source_asset_ids: list[str] = [
        "KA-NSG-001",
        "KA-NSG-003",
        "KA-NSG-004",
    ]
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
    | ServiceOpsWorkflowParams
    | KnowledgeAuthoringWorkflowParams
)
