from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field

from .faults import FaultOverlaySpec


class VerticalScenarioVariantSpec(BaseModel):
    vertical_name: str
    name: str
    title: str
    description: str
    scenario_name: str
    workflow_variant: str
    workflow_parameter_overrides: dict[str, Any] = Field(default_factory=dict)
    fault_overlays: list[FaultOverlaySpec] = Field(default_factory=list)
    branch_labels: list[str] = Field(default_factory=list)
    rationale: str
    change_summary: list[str] = Field(default_factory=list)
    metadata: dict[str, Any] = Field(default_factory=dict)


def list_vertical_scenario_variants(
    vertical_name: str,
) -> list[VerticalScenarioVariantSpec]:
    key = vertical_name.strip().lower()
    if key not in _VERTICAL_SCENARIO_VARIANTS:
        raise KeyError(f"unknown vertical scenario variants: {vertical_name}")
    return list(_VERTICAL_SCENARIO_VARIANTS[key].values())


def get_vertical_scenario_variant(
    vertical_name: str, variant_name: str
) -> VerticalScenarioVariantSpec:
    key = vertical_name.strip().lower()
    variant_key = variant_name.strip().lower()
    variants = _VERTICAL_SCENARIO_VARIANTS.get(key)
    if variants is None or variant_key not in variants:
        raise KeyError(f"unknown scenario variant: {vertical_name}/{variant_name}")
    return variants[variant_key]


def default_vertical_scenario_variant(
    vertical_name: str,
) -> VerticalScenarioVariantSpec:
    key = vertical_name.strip().lower()
    return list_vertical_scenario_variants(key)[0]


_VERTICAL_SCENARIO_VARIANTS: dict[str, dict[str, VerticalScenarioVariantSpec]] = {
    "real_estate_management": {
        "tenant_opening_conflict": VerticalScenarioVariantSpec(
            vertical_name="real_estate_management",
            name="tenant_opening_conflict",
            title="Tenant Opening Conflict",
            description=(
                "Restore tenant opening readiness by aligning the lease amendment, vendor prep, "
                "unit reservation, and opening artifacts before Monday morning."
            ),
            scenario_name="tenant_opening_conflict",
            workflow_variant="tenant_opening_conflict",
            branch_labels=[
                "Fast-track the vendor and protect the opening date",
                "Escalate the amendment delay and push the opening",
            ],
            rationale="This is the flagship Harbor Point conflict and the clearest entry point for the property world.",
            change_summary=[
                "Anchor tenant opening remains blocked on lease execution and HVAC prep.",
                "Property checklist and tracker still show unresolved blockers.",
            ],
        ),
        "vendor_no_show": VerticalScenarioVariantSpec(
            vertical_name="real_estate_management",
            name="vendor_no_show",
            title="Vendor No-Show",
            description=(
                "A key prep vendor drops out late, forcing property ops to switch vendors without losing the opening window."
            ),
            scenario_name="vendor_no_show",
            workflow_variant="vendor_no_show",
            workflow_parameter_overrides={
                "vendor_id": "VEND-HPM-ELEC",
                "vendor_note": "Backup facilities vendor assigned after HVAC no-show.",
                "ticket_note": "Backup vendor assigned and opening blockers re-sequenced without slipping the tenant opening.",
                "slack_summary": "Backup vendor locked in; Harbor Point still has a viable opening path.",
            },
            fault_overlays=[
                FaultOverlaySpec(
                    name="vendor_no_show_status",
                    path="capability_graphs.property_graph.work_orders[work_order_id=WO-HPM-88].status",
                    operation="set",
                    value="vendor_no_show",
                    label="Blocking HVAC work order is now in vendor no-show state.",
                    rationale="The original vendor dropped out the night before readiness review.",
                    visibility="visible",
                ),
                FaultOverlaySpec(
                    name="vendor_no_show_note",
                    path="capability_graphs.doc_graph.documents[doc_id=DOC-HPM-OPENING].body",
                    operation="set",
                    value="Opening checklist draft.\n\nOriginal HVAC vendor no-show confirmed.\nBackup assignment still pending.",
                    label="Opening checklist now records the vendor no-show.",
                    rationale="The shared artifact needs to explain why a backup vendor is being routed in.",
                    visibility="visible",
                ),
            ],
            branch_labels=[
                "Swap to the backup vendor and keep the opening date",
                "Wait for the original vendor and risk missing Monday",
            ],
            rationale="Tests whether the same world can survive a last-minute vendor shock without rebuilding the company.",
            change_summary=[
                "Original HVAC vendor is gone; backup assignment becomes the critical decision.",
                "Opening checklist now reflects the no-show and handoff risk.",
            ],
        ),
        "lease_revision_late": VerticalScenarioVariantSpec(
            vertical_name="real_estate_management",
            name="lease_revision_late",
            title="Lease Revision Late",
            description=(
                "Legal redlines arrive late and compress the time available to finish opening prep safely."
            ),
            scenario_name="lease_revision_late",
            workflow_variant="lease_revision_late",
            workflow_parameter_overrides={
                "deadline_max_ms": 120000,
                "doc_update_note": "Late lease revision executed, vendor prep confirmed, and opening artifact refreshed under compressed time.",
                "slack_summary": "Late legal revision cleared just in time; Harbor Point opening path remains intact.",
            },
            fault_overlays=[
                FaultOverlaySpec(
                    name="late_revision_milestone",
                    path="capability_graphs.property_graph.leases[lease_id=LEASE-HPM-14A].milestone",
                    operation="set",
                    value="late_redlines",
                    label="Lease milestone regressed to late redlines.",
                    rationale="Counsel returned revisions late, compressing the readiness window.",
                    visibility="hidden",
                ),
                FaultOverlaySpec(
                    name="late_revision_deadline",
                    path="capability_graphs.property_graph.tenants[tenant_id=TEN-HPM-ANCHOR].opening_deadline_ms",
                    operation="increment",
                    value=-21600000,
                    label="Opening deadline moved six hours earlier.",
                    rationale="The tenant insists on a shorter handoff window after legal delay.",
                    visibility="hidden",
                ),
            ],
            branch_labels=[
                "Execute the revision now and compress prep",
                "Delay execution and trade safety for schedule certainty",
            ],
            rationale="Shows deadline pressure without changing the underlying company structure.",
            change_summary=[
                "Lease milestone regresses to late redlines.",
                "Tenant deadline pressure increases by six hours.",
            ],
        ),
        "double_booked_unit": VerticalScenarioVariantSpec(
            vertical_name="real_estate_management",
            name="double_booked_unit",
            title="Double-Booked Unit",
            description=(
                "The opening unit was tentatively promised elsewhere, so property ops must resolve the reservation conflict before move-in."
            ),
            scenario_name="double_booked_unit",
            workflow_variant="double_booked_unit",
            workflow_parameter_overrides={
                "ticket_note": "Reservation conflict cleared, unit reassigned correctly, and opening blockers resolved.",
                "slack_summary": "Unit conflict resolved and Harbor Point opening is now aligned again.",
            },
            fault_overlays=[
                FaultOverlaySpec(
                    name="double_booked_unit_status",
                    path="capability_graphs.property_graph.units[unit_id=UNIT-HPM-14A].status",
                    operation="set",
                    value="reserved",
                    label="Unit 14A is already marked reserved.",
                    rationale="The unit was tentatively held for another tenant during a portfolio shuffle.",
                    visibility="visible",
                ),
                FaultOverlaySpec(
                    name="double_booked_unit_tenant",
                    path="capability_graphs.property_graph.units[unit_id=UNIT-HPM-14A].reserved_for",
                    operation="set",
                    value="TEN-HPM-CONFLICT",
                    label="Reservation points to the wrong tenant.",
                    rationale="Ops needs to resolve the conflicting reservation before opening day.",
                    visibility="visible",
                ),
            ],
            branch_labels=[
                "Reclaim the unit for the anchor tenant immediately",
                "Leave the conflict unresolved and trigger a move-in failure",
            ],
            rationale="Demonstrates that one world can express spatial conflicts, not just paperwork drift.",
            change_summary=[
                "Unit 14A is reserved for the wrong tenant.",
                "Opening success now depends on reservation integrity as well as lease/vendor state.",
            ],
        ),
    },
    "digital_marketing_agency": {
        "campaign_launch_guardrail": VerticalScenarioVariantSpec(
            vertical_name="digital_marketing_agency",
            name="campaign_launch_guardrail",
            title="Campaign Launch Guardrail",
            description=(
                "Clear creative approval, normalize pacing, refresh reporting, and update launch artifacts before spend burns."
            ),
            scenario_name="campaign_launch_guardrail",
            workflow_variant="campaign_launch_guardrail",
            branch_labels=[
                "Pause the launch and protect spend",
                "Push through stale approval state and hope nothing breaks",
            ],
            rationale="Flagship launch-safety demo for the agency world.",
            change_summary=[
                "Creative approval, spend pacing, and reporting freshness are all off at once.",
            ],
        ),
        "creative_not_approved": VerticalScenarioVariantSpec(
            vertical_name="digital_marketing_agency",
            name="creative_not_approved",
            title="Creative Not Approved",
            description=(
                "Client creative sign-off stalls while the launch clock keeps ticking."
            ),
            scenario_name="creative_not_approved",
            workflow_variant="creative_not_approved",
            workflow_parameter_overrides={
                "ticket_note": "Creative sign-off recovered before launch and client artifacts updated accordingly.",
                "slack_summary": "Creative approval cleared; launch can proceed with approved assets only.",
            },
            fault_overlays=[
                FaultOverlaySpec(
                    name="creative_rejected",
                    path="capability_graphs.campaign_graph.creatives[creative_id=CRT-APEX-01].status",
                    operation="set",
                    value="rejected_pending_rework",
                    label="Hero creative is currently rejected.",
                    rationale="Client feedback landed late and invalidated the launch-ready asset.",
                    visibility="visible",
                ),
                FaultOverlaySpec(
                    name="approval_reset",
                    path="capability_graphs.campaign_graph.approvals[approval_id=APR-APEX-01].status",
                    operation="set",
                    value="rework_required",
                    label="Client creative approval moved back to rework required.",
                    rationale="Approval has to be re-earned before spend is released.",
                    visibility="visible",
                ),
            ],
            branch_labels=[
                "Hold launch until rework is approved",
                "Ship the unapproved creative and risk client trust",
            ],
            rationale="Shows approval drift without changing the base client, campaign, or operator stack.",
            change_summary=[
                "Creative asset is rejected and approval has regressed to rework required.",
            ],
        ),
        "budget_runaway": VerticalScenarioVariantSpec(
            vertical_name="digital_marketing_agency",
            name="budget_runaway",
            title="Budget Runaway",
            description=(
                "Spend accelerates too quickly and forces the team to rebalance pacing before the client burns budget."
            ),
            scenario_name="budget_runaway",
            workflow_variant="budget_runaway",
            workflow_parameter_overrides={
                "pacing_pct": 70.0,
                "report_note": "Emergency pacing correction recorded after runaway spend review.",
                "crm_note": "Client budget protected after pacing rollback and refreshed launch reporting.",
            },
            fault_overlays=[
                FaultOverlaySpec(
                    name="runaway_pacing",
                    path="capability_graphs.campaign_graph.campaigns[campaign_id=CMP-APEX-01].pacing_pct",
                    operation="set",
                    value=171.0,
                    label="Campaign pacing is now in runaway territory.",
                    rationale="Spend accelerated after a targeting change landed without review.",
                    visibility="visible",
                ),
                FaultOverlaySpec(
                    name="runaway_spend",
                    path="capability_graphs.campaign_graph.campaigns[campaign_id=CMP-APEX-01].spend_usd",
                    operation="set",
                    value=122500,
                    label="Spend has already exceeded the nominal launch budget.",
                    rationale="The client is now at real risk of overspend if the launch is not corrected.",
                    visibility="visible",
                ),
            ],
            branch_labels=[
                "Throttle spend and protect budget",
                "Keep spend live to preserve reach and accept client risk",
            ],
            rationale="Turns the same launch world into a budget-control problem instead of only an approval problem.",
            change_summary=[
                "Pacing spikes to 171% and spend exceeds plan.",
                "The key tradeoff becomes budget protection versus launch momentum.",
            ],
        ),
        "client_reporting_mismatch": VerticalScenarioVariantSpec(
            vertical_name="digital_marketing_agency",
            name="client_reporting_mismatch",
            title="Client Reporting Mismatch",
            description=(
                "The launch report and client brief disagree, creating a trust and communication risk before go-live."
            ),
            scenario_name="client_reporting_mismatch",
            workflow_variant="client_reporting_mismatch",
            workflow_parameter_overrides={
                "doc_update_note": "Launch brief reconciled with the refreshed client report and approval state.",
                "slack_summary": "Client-facing artifacts now agree on pacing, approval, and launch readiness.",
            },
            fault_overlays=[
                FaultOverlaySpec(
                    name="stale_launch_brief",
                    path="capability_graphs.doc_graph.documents[doc_id=DOC-NSG-LAUNCH].body",
                    operation="set",
                    value="Launch brief.\n\nClient brief still says approval complete.\nBudget pacing note is stale.",
                    label="Client launch brief is now inconsistent with the actual system state.",
                    rationale="The internal artifact misstates both approval and pacing posture.",
                    visibility="visible",
                ),
                FaultOverlaySpec(
                    name="report_status_mismatch",
                    path="capability_graphs.campaign_graph.reports[report_id=RPT-APEX-01].status",
                    operation="set",
                    value="mismatch_detected",
                    label="Report status is explicitly marked as mismatched.",
                    rationale="The report needs reconciliation before anyone can trust it.",
                    visibility="visible",
                ),
            ],
            branch_labels=[
                "Reconcile the report and relaunch confidence",
                "Ignore the mismatch and let client trust erode",
            ],
            rationale="Useful for showing that artifact integrity is as important as raw launch mechanics.",
            change_summary=[
                "Launch brief and report now disagree about approval and pacing.",
            ],
        ),
    },
    "storage_solutions": {
        "capacity_quote_commitment": VerticalScenarioVariantSpec(
            vertical_name="storage_solutions",
            name="capacity_quote_commitment",
            title="Capacity Quote Commitment",
            description=(
                "Make a strategic storage quote feasible by reserving capacity, revising the commitment, and aligning vendor follow-through."
            ),
            scenario_name="capacity_quote_commitment",
            workflow_variant="capacity_quote_commitment",
            branch_labels=[
                "Reserve capacity and keep the customer timeline",
                "Send the quote before ops alignment and create a downstream failure",
            ],
            rationale="Flagship capacity-feasibility demo for the storage world.",
            change_summary=[
                "Capacity is fragmented and the quote is not yet operationally safe.",
            ],
        ),
        "vendor_dispatch_gap": VerticalScenarioVariantSpec(
            vertical_name="storage_solutions",
            name="vendor_dispatch_gap",
            title="Vendor Dispatch Gap",
            description=(
                "Capacity exists, but downstream vendor dispatch is blocked and the customer commitment is at risk."
            ),
            scenario_name="vendor_dispatch_gap",
            workflow_variant="vendor_dispatch_gap",
            workflow_parameter_overrides={
                "vendor_id": "VEND-ATS-TRUCK",
                "ticket_note": "Dispatch vendor gap closed after feasible allocation and revised commitment.",
                "slack_summary": "Vendor dispatch back on track; Zenith commitment is now feasible to send.",
            },
            fault_overlays=[
                FaultOverlaySpec(
                    name="dispatch_gap",
                    path="capability_graphs.inventory_graph.orders[order_id=ORD-ATS-900].status",
                    operation="set",
                    value="vendor_blocked",
                    label="Order is blocked on missing dispatch coverage.",
                    rationale="The preferred fulfillment vendor cannot meet the requested customer timeline.",
                    visibility="visible",
                ),
                FaultOverlaySpec(
                    name="dispatch_plan_stale",
                    path="capability_graphs.doc_graph.documents[doc_id=DOC-ATS-QUOTE].body",
                    operation="set",
                    value="Rollout plan draft.\n\nCapacity feasible.\nDispatch vendor still unavailable.",
                    label="Rollout plan now calls out the dispatch gap explicitly.",
                    rationale="Ops cannot commit without a new vendor assignment path.",
                    visibility="visible",
                ),
            ],
            branch_labels=[
                "Re-route dispatch to an alternate vendor",
                "Keep the quote live without fulfillment coverage",
            ],
            rationale="Shows that feasibility is not only about capacity; fulfillment adapters matter too.",
            change_summary=[
                "Order is blocked on missing dispatch coverage.",
                "Rollout plan now highlights the vendor gap instead of only capacity fragmentation.",
            ],
        ),
        "fragmented_capacity": VerticalScenarioVariantSpec(
            vertical_name="storage_solutions",
            name="fragmented_capacity",
            title="Fragmented Capacity",
            description=(
                "Enough total capacity exists across the network, but not in one clean block, forcing a more deliberate allocation choice."
            ),
            scenario_name="fragmented_capacity",
            workflow_variant="fragmented_capacity",
            workflow_parameter_overrides={
                "pool_id": "POOL-CHI-A",
                "units": 20,
                "site_id": "SITE-CHI-1",
                "doc_update_note": "Fragmented inventory resolved by shifting the commitment to the feasible Chicago block and updating rollout artifacts.",
            },
            fault_overlays=[
                FaultOverlaySpec(
                    name="mke_pool_reduced",
                    path="capability_graphs.inventory_graph.capacity_pools[pool_id=POOL-MKE-B].reserved_units",
                    operation="increment",
                    value=100,
                    label="Milwaukee overflow pool is now heavily reserved.",
                    rationale="A separate customer hold consumed much of the easy overflow capacity.",
                    visibility="visible",
                ),
                FaultOverlaySpec(
                    name="quote_fragmented_note",
                    path="capability_graphs.inventory_graph.quotes[quote_id=Q-ATS-900].status",
                    operation="set",
                    value="fragmented_review",
                    label="Quote is in fragmented review state.",
                    rationale="The team has to choose a smaller feasible block or risk overcommit.",
                    visibility="visible",
                ),
            ],
            branch_labels=[
                "Shrink the commitment and keep the customer",
                "Pretend the fragmented capacity is good enough",
            ],
            rationale="Demonstrates a different storage tradeoff without changing the company or the customer.",
            change_summary=[
                "Overflow capacity is no longer cleanly available.",
                "The quote must be revised around a smaller feasible block.",
            ],
        ),
        "overcommit_quote_risk": VerticalScenarioVariantSpec(
            vertical_name="storage_solutions",
            name="overcommit_quote_risk",
            title="Overcommit Quote Risk",
            description=(
                "The quote already reflects an unsafe commitment, so the team must unwind it before the customer hears the wrong number."
            ),
            scenario_name="overcommit_quote_risk",
            workflow_variant="overcommit_quote_risk",
            workflow_parameter_overrides={
                "committed_units": 60,
                "crm_note": "Unsafe overcommit removed after feasible storage plan and revised quote were confirmed.",
            },
            fault_overlays=[
                FaultOverlaySpec(
                    name="quote_overcommitted",
                    path="capability_graphs.inventory_graph.quotes[quote_id=Q-ATS-900].committed_units",
                    operation="set",
                    value=140,
                    label="Quote already promises more capacity than the network can safely deliver.",
                    rationale="Commercial pressure caused a commitment to be drafted before feasibility review.",
                    visibility="visible",
                ),
                FaultOverlaySpec(
                    name="quote_risk_status",
                    path="capability_graphs.inventory_graph.quotes[quote_id=Q-ATS-900].status",
                    operation="set",
                    value="overcommitted",
                    label="Quote status is explicitly overcommitted.",
                    rationale="This is the classic storage-world failure we want the demo to catch.",
                    visibility="visible",
                ),
            ],
            branch_labels=[
                "Revise the quote downward and preserve trust",
                "Send the overcommitted quote and defer the failure downstream",
            ],
            rationale="Makes the revenue-versus-feasibility tradeoff explicit in the storage domain.",
            change_summary=[
                "Quote already promises 140 units before feasibility review.",
                "The run has to unwind a bad commitment instead of merely completing a good one.",
            ],
        ),
    },
    "b2b_saas": {
        "enterprise_renewal_risk": VerticalScenarioVariantSpec(
            vertical_name="b2b_saas",
            name="enterprise_renewal_risk",
            title="Enterprise Renewal at Risk",
            description=(
                "Save a $480K enterprise renewal by fixing a broken integration, "
                "rebuilding stakeholder trust, and neutralizing a competitive threat."
            ),
            scenario_name="enterprise_renewal_risk",
            workflow_variant="enterprise_renewal_risk",
            branch_labels=[
                "Fix the integration first and rebuild trust before the renewal conversation",
                "Lead with the discount and hope the product issues don't kill the deal",
            ],
            rationale="Flagship B2B SaaS crisis: product failure, champion loss, and competitive pressure converge on one renewal.",
            change_summary=[
                "Product champion departed; new decision-maker is evaluating a competitor.",
                "Breaking API change has left the customer's pipeline down for 9 days.",
            ],
        ),
        "support_escalation_spiral": VerticalScenarioVariantSpec(
            vertical_name="b2b_saas",
            name="support_escalation_spiral",
            title="Support Escalation Spiral",
            description=(
                "A P1 support ticket is bouncing between engineering and CS while "
                "the customer's patience runs out."
            ),
            scenario_name="support_escalation_spiral",
            workflow_variant="support_escalation_spiral",
            workflow_parameter_overrides={
                "ticket_note": "P1 ownership assigned, fix deployed, and customer confirmation received.",
                "slack_summary": "Apex P1 resolved and post-incident review scheduled.",
            },
            fault_overlays=[
                FaultOverlaySpec(
                    name="p1_ownership_gap",
                    path="capability_graphs.work_graph.tickets[ticket_id=JRA-PIN-101].assignee",
                    operation="set",
                    value="unassigned",
                    label="P1 ticket has no clear owner.",
                    rationale="The ticket has been reassigned three times in 9 days.",
                    visibility="visible",
                ),
                FaultOverlaySpec(
                    name="escalation_deadline",
                    path="capability_graphs.work_graph.service_requests[request_id=SR-PIN-201].status",
                    operation="set",
                    value="critical_overdue",
                    label="Support escalation is now critically overdue.",
                    rationale="SLA breach is imminent and the customer has mentioned it in writing.",
                    visibility="visible",
                ),
            ],
            branch_labels=[
                "Assign a single owner and fast-track the fix",
                "Keep the ticket in triage and risk SLA breach",
            ],
            rationale="Shows how the same company world can surface an ops failure instead of a commercial one.",
            change_summary=[
                "P1 ticket ownership is unassigned after multiple handoffs.",
                "Support escalation has crossed the SLA threshold.",
            ],
        ),
        "pricing_negotiation_deadlock": VerticalScenarioVariantSpec(
            vertical_name="b2b_saas",
            name="pricing_negotiation_deadlock",
            title="Pricing Negotiation Deadlock",
            description=(
                "The customer is pushing for a 40% discount and sales cannot get "
                "finance alignment on the counteroffer."
            ),
            scenario_name="pricing_negotiation_deadlock",
            workflow_variant="pricing_negotiation_deadlock",
            workflow_parameter_overrides={
                "discount_pct": 40,
                "crm_note": "Renewal terms agreed after structured negotiation and value demonstration.",
            },
            fault_overlays=[
                FaultOverlaySpec(
                    name="deal_stage_regression",
                    path="capability_graphs.revenue_graph.deals[id=DEAL-APEX-RENEWAL].stage",
                    operation="set",
                    value="negotiation_stalled",
                    label="Deal stage has regressed to negotiation stalled.",
                    rationale="Customer's discount demand exceeds the pre-approved authority.",
                    visibility="visible",
                ),
                FaultOverlaySpec(
                    name="proposal_blocked",
                    path="capability_graphs.doc_graph.documents[doc_id=DOC-PIN-PROPOSAL].body",
                    operation="set",
                    value=(
                        "Renewal proposal draft.\n\n"
                        "BLOCKED: Customer requesting 40% discount. Finance has not approved.\n"
                        "Counter-proposal needs CRO sign-off before it can be sent."
                    ),
                    label="Renewal proposal is blocked on pricing approval.",
                    rationale="Sales cannot send the proposal without finance alignment.",
                    visibility="visible",
                ),
            ],
            branch_labels=[
                "Offer value-based counter and hold the line on price",
                "Accept the discount to save the deal and eat the margin",
            ],
            rationale="Demonstrates commercial pressure without changing the product or support situation.",
            change_summary=[
                "Deal stage regressed to negotiation stalled.",
                "Renewal proposal is blocked on discount authorization.",
            ],
        ),
    },
    "service_ops": {
        "service_day_collision": VerticalScenarioVariantSpec(
            vertical_name="service_ops",
            name="service_day_collision",
            title="Service Day Collision",
            description=(
                "A VIP outage, technician no-show, and billing dispute collide on the same service account before the morning stabilizes."
            ),
            scenario_name="service_day_collision",
            workflow_variant="service_day_collision",
            branch_labels=[
                "Reassign dispatch, hold billing, and keep the customer whole",
                "Let the morning fragment into dispatch drift and billing mistakes",
            ],
            rationale="This is the flagship Clearwater field-services story and the clearest proof of a service-ops pack.",
            change_summary=[
                "One VIP account now carries dispatch risk, billing risk, and SLA risk at the same time.",
            ],
        ),
        "technician_no_show": VerticalScenarioVariantSpec(
            vertical_name="service_ops",
            name="technician_no_show",
            title="Technician No-Show",
            description=(
                "Billing is stable, but the assigned technician drops out and the dispatch bench has to recover the day quickly."
            ),
            scenario_name="technician_no_show",
            workflow_variant="technician_no_show",
            workflow_parameter_overrides={
                "billing_case_id": "BILL-CFS-100",
                "billing_note": "Billing remains stable while dispatch recovers the technician gap.",
                "slack_summary": "Backup dispatch locked in; Clearwater keeps the SLA without reopening billing risk.",
            },
            fault_overlays=[
                FaultOverlaySpec(
                    name="billing_clear",
                    path="capability_graphs.ops_graph.billing_cases[billing_case_id=BILL-CFS-100].dispute_status",
                    operation="set",
                    value="clear",
                    label="The disputed invoice has already been cleared.",
                    rationale="This variant isolates dispatch recovery instead of billing risk.",
                    visibility="visible",
                ),
                FaultOverlaySpec(
                    name="billing_exception_resolved",
                    path="capability_graphs.ops_graph.exceptions[exception_id=EXC-CFS-101].status",
                    operation="set",
                    value="resolved",
                    label="Billing exception is no longer open.",
                    rationale="The morning pressure is now concentrated on technician recovery and SLA protection.",
                    visibility="visible",
                ),
            ],
            branch_labels=[
                "Recover the route with the right backup technician",
                "Keep the work unassigned and let the SLA slip",
            ],
            rationale="Shows that the same service company can pivot into a dispatch-first mission without rebuilding the world.",
            change_summary=[
                "Billing risk is cleared out of the way.",
                "Dispatch recovery becomes the dominant operating problem.",
            ],
        ),
        "billing_dispute_reopened": VerticalScenarioVariantSpec(
            vertical_name="service_ops",
            name="billing_dispute_reopened",
            title="Billing Dispute Reopened",
            description=(
                "Dispatch has a plausible recovery path, but finance reopens the account dispute and the customer trust problem gets worse."
            ),
            scenario_name="billing_dispute_reopened",
            workflow_variant="billing_dispute_reopened",
            workflow_parameter_overrides={
                "billing_note": "Collections hold recorded and dispute escalation linked to the active field response.",
                "slack_summary": "Billing safely contained while the field response stays on track for Clearwater Medical.",
            },
            fault_overlays=[
                FaultOverlaySpec(
                    name="backup_tech_preassigned",
                    path="capability_graphs.ops_graph.appointments[appointment_id=APT-CFS-100].technician_id",
                    operation="set",
                    value="TECH-CFS-02",
                    label="Backup technician is already in the slot.",
                    rationale="This variant shifts focus away from dispatch uncertainty and toward account safety.",
                    visibility="visible",
                ),
                FaultOverlaySpec(
                    name="dispatch_stable",
                    path="capability_graphs.ops_graph.appointments[appointment_id=APT-CFS-100].dispatch_status",
                    operation="set",
                    value="assigned",
                    label="Dispatch no longer looks at risk.",
                    rationale="The field plan is workable; the real pressure is now billing trust.",
                    visibility="visible",
                ),
                FaultOverlaySpec(
                    name="reopened_dispute",
                    path="capability_graphs.ops_graph.billing_cases[billing_case_id=BILL-CFS-100].dispute_status",
                    operation="set",
                    value="reopened",
                    label="The customer dispute is reopened mid-morning.",
                    rationale="Finance risk gets worse right as the field team tries to recover the service day.",
                    visibility="visible",
                ),
            ],
            branch_labels=[
                "Contain finance risk before the customer relationship degrades",
                "Focus only on field completion and let billing damage keep spreading",
            ],
            rationale="Shows the same company world bending toward trust and revenue preservation instead of dispatch only.",
            change_summary=[
                "Dispatch is mostly stabilized.",
                "Billing dispute risk becomes the primary operating problem.",
            ],
        ),
    },
}


__all__ = [
    "VerticalScenarioVariantSpec",
    "default_vertical_scenario_variant",
    "get_vertical_scenario_variant",
    "list_vertical_scenario_variants",
]
