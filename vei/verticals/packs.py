from __future__ import annotations

from typing import Dict, List

from pydantic import BaseModel, Field

from vei.blueprint.models import (
    BlueprintAsset,
    BlueprintCampaignApprovalAsset,
    BlueprintCampaignAsset,
    BlueprintCampaignGraphAsset,
    BlueprintCampaignReportAsset,
    BlueprintCapabilityGraphsAsset,
    BlueprintClientAsset,
    BlueprintCommGraphAsset,
    BlueprintCreativeAsset,
    BlueprintDocumentAsset,
    BlueprintDocGraphAsset,
    BlueprintLeaseAsset,
    BlueprintPropertyAsset,
    BlueprintPropertyGraphAsset,
    BlueprintRevenueGraphAsset,
    BlueprintSlackChannelAsset,
    BlueprintSlackMessageAsset,
    BlueprintTicketAsset,
    BlueprintUnitAsset,
    BlueprintVendorAsset,
    BlueprintWorkGraphAsset,
    BlueprintWorkOrderAsset,
    BlueprintServiceRequestAsset,
    BlueprintApprovalAsset,
    BlueprintBuildingAsset,
    BlueprintTenantAsset,
    BlueprintCrmCompanyAsset,
    BlueprintCrmContactAsset,
    BlueprintCrmDealAsset,
    BlueprintInventoryGraphAsset,
    BlueprintSiteAsset,
    BlueprintCapacityPoolAsset,
    BlueprintStorageUnitAsset,
    BlueprintQuoteAsset,
    BlueprintOrderAsset,
)


class VerticalPackManifest(BaseModel):
    name: str
    title: str
    description: str
    company_name: str
    company_briefing: str
    failure_impact: str
    objective_focus: str
    scenario_name: str
    workflow_name: str
    workflow_variant: str
    key_surfaces: List[str] = Field(default_factory=list)
    proves: List[str] = Field(default_factory=list)
    what_if_branches: List[str] = Field(default_factory=list)


def list_vertical_pack_manifests() -> List[VerticalPackManifest]:
    return sorted(_VERTICAL_PACKS.values(), key=lambda item: item.name)


def get_vertical_pack_manifest(name: str) -> VerticalPackManifest:
    key = name.strip().lower()
    if key not in _VERTICAL_PACKS:
        raise KeyError(f"unknown vertical pack: {name}")
    return _VERTICAL_PACKS[key]


def build_vertical_blueprint_asset(name: str) -> BlueprintAsset:
    key = name.strip().lower()
    builder = _VERTICAL_BUILDERS.get(key)
    if builder is None:
        raise KeyError(f"unknown vertical pack: {name}")
    return builder()


def _channel(channel: str, text: str) -> BlueprintSlackChannelAsset:
    return BlueprintSlackChannelAsset(
        channel=channel,
        unread=4,
        messages=[
            BlueprintSlackMessageAsset(
                ts="1710000000.000100", user="ops-bot", text=text
            )
        ],
    )


def _real_estate_asset() -> BlueprintAsset:
    return BlueprintAsset(
        name="real_estate_management.blueprint",
        title="Harbor Point Management",
        description=(
            "Major tenant opening readiness with lease, maintenance, vendor, and "
            "artifact coordination pressure."
        ),
        scenario_name="tenant_opening_conflict",
        family_name="real_estate_management",
        workflow_name="real_estate_management",
        workflow_variant="tenant_opening_conflict",
        requested_facades=["slack", "docs", "jira", "servicedesk", "property_ops"],
        capability_graphs=BlueprintCapabilityGraphsAsset(
            organization_name="Harbor Point Management",
            organization_domain="harborpoint.example.com",
            timezone="America/Los_Angeles",
            scenario_brief=(
                "Anchor tenant opening is scheduled for Monday morning, but lease "
                "execution, vendor assignment, and maintenance readiness are drifting."
            ),
            comm_graph=BlueprintCommGraphAsset(
                slack_initial_message="Monday opening readiness review starts now.",
                slack_channels=[
                    _channel(
                        "#harbor-point-ops",
                        "Anchor tenant opening still blocked by lease amendment and HVAC work order.",
                    )
                ],
            ),
            doc_graph=BlueprintDocGraphAsset(
                documents=[
                    BlueprintDocumentAsset(
                        doc_id="DOC-HPM-OPENING",
                        title="Harbor Point Opening Checklist",
                        body="Opening checklist draft.\n\nLease amendment pending.\nVendor still unassigned.",
                        tags=["opening", "tenant"],
                    )
                ]
            ),
            work_graph=BlueprintWorkGraphAsset(
                tickets=[
                    BlueprintTicketAsset(
                        ticket_id="JRA-HPM-17",
                        title="Tenant opening blocker review",
                        status="open",
                        assignee="ops-manager",
                        description="Resolve lease/vendor blockers before Monday opening.",
                    )
                ],
                service_requests=[
                    BlueprintServiceRequestAsset(
                        request_id="REQ-HPM-1",
                        title="Vendor approval for unit 14A prep",
                        status="pending_approval",
                        requester="leasing-manager",
                        approvals=[
                            BlueprintApprovalAsset(stage="vendor", status="PENDING")
                        ],
                    )
                ],
            ),
            property_graph=BlueprintPropertyGraphAsset(
                properties=[
                    BlueprintPropertyAsset(
                        property_id="PROP-HPM-1",
                        name="Harbor Point Plaza",
                        city="Oakland",
                        state="CA",
                        portfolio="bay-area-retail",
                    )
                ],
                buildings=[
                    BlueprintBuildingAsset(
                        building_id="BLDG-HPM-1",
                        property_id="PROP-HPM-1",
                        name="Building A",
                    )
                ],
                units=[
                    BlueprintUnitAsset(
                        unit_id="UNIT-HPM-14A",
                        building_id="BLDG-HPM-1",
                        label="14A",
                        status="vacant",
                    )
                ],
                tenants=[
                    BlueprintTenantAsset(
                        tenant_id="TEN-HPM-ANCHOR",
                        name="BlueBottle Fitness",
                        segment="anchor",
                        opening_deadline_ms=1710432000000,
                    )
                ],
                leases=[
                    BlueprintLeaseAsset(
                        lease_id="LEASE-HPM-14A",
                        tenant_id="TEN-HPM-ANCHOR",
                        unit_id="UNIT-HPM-14A",
                        status="pending",
                        milestone="amendment_pending",
                        amendment_pending=True,
                    )
                ],
                vendors=[
                    BlueprintVendorAsset(
                        vendor_id="VEND-HPM-HVAC",
                        name="Westshore HVAC",
                        specialty="hvac",
                    ),
                    BlueprintVendorAsset(
                        vendor_id="VEND-HPM-ELEC",
                        name="Brightline Electric",
                        specialty="electrical",
                    ),
                ],
                work_orders=[
                    BlueprintWorkOrderAsset(
                        work_order_id="WO-HPM-88",
                        property_id="PROP-HPM-1",
                        title="HVAC commissioning for unit 14A",
                        status="pending_vendor",
                    )
                ],
            ),
            metadata={
                "vertical": "real_estate_management",
                "what_if_branches": [
                    "Delay vendor assignment and miss opening",
                    "Execute amendment but leave unit unreserved",
                ],
            },
        ),
        metadata={"vertical": "real_estate_management"},
    )


def _marketing_asset() -> BlueprintAsset:
    return BlueprintAsset(
        name="digital_marketing_agency.blueprint",
        title="Northstar Growth",
        description=(
            "Client launch guardrail with creative approval, budget pacing, and reporting freshness pressure."
        ),
        scenario_name="campaign_launch_guardrail",
        family_name="digital_marketing_agency",
        workflow_name="digital_marketing_agency",
        workflow_variant="campaign_launch_guardrail",
        requested_facades=[
            "slack",
            "docs",
            "jira",
            "servicedesk",
            "campaign_ops",
            "crm",
        ],
        capability_graphs=BlueprintCapabilityGraphsAsset(
            organization_name="Northstar Growth",
            organization_domain="northstar.example.com",
            timezone="America/New_York",
            scenario_brief=(
                "A major paid-media launch is about to go live with stale reporting, incomplete approval, and unsafe pacing."
            ),
            comm_graph=BlueprintCommGraphAsset(
                slack_initial_message="Launch control room is live for Apex Health.",
                slack_channels=[
                    _channel(
                        "#northstar-launch",
                        "Apex Health launch is pacing hot and creative approval is still pending.",
                    )
                ],
            ),
            doc_graph=BlueprintDocGraphAsset(
                documents=[
                    BlueprintDocumentAsset(
                        doc_id="DOC-NSG-LAUNCH",
                        title="Apex Health Launch Brief",
                        body="Launch brief.\n\nCreative approval outstanding.\nReporting artifact stale.",
                        tags=["launch", "client"],
                    )
                ]
            ),
            work_graph=BlueprintWorkGraphAsset(
                tickets=[
                    BlueprintTicketAsset(
                        ticket_id="JRA-NSG-33",
                        title="Apex Health launch guardrail",
                        status="open",
                        assignee="account-lead",
                        description="Clear approval, pacing, and reporting blockers before launch.",
                    )
                ],
                service_requests=[
                    BlueprintServiceRequestAsset(
                        request_id="REQ-NSG-1",
                        title="Creative sign-off",
                        status="pending_approval",
                        requester="creative-director",
                        approvals=[
                            BlueprintApprovalAsset(stage="creative", status="PENDING")
                        ],
                    )
                ],
            ),
            revenue_graph=BlueprintRevenueGraphAsset(
                companies=[
                    BlueprintCrmCompanyAsset(
                        id="CRM-NSG-C1",
                        name="Apex Health",
                        domain="apexhealth.example.com",
                    )
                ],
                contacts=[
                    BlueprintCrmContactAsset(
                        id="CRM-NSG-P1",
                        email="melissa@apexhealth.example.com",
                        first_name="Melissa",
                        last_name="Grant",
                        company_id="CRM-NSG-C1",
                    )
                ],
                deals=[
                    BlueprintCrmDealAsset(
                        id="CRM-NSG-D1",
                        name="Apex Q2 Retainer",
                        amount=180000,
                        stage="launch_risk",
                        owner="casey.growth@example.com",
                        company_id="CRM-NSG-C1",
                        contact_id="CRM-NSG-P1",
                    )
                ],
            ),
            campaign_graph=BlueprintCampaignGraphAsset(
                clients=[
                    BlueprintClientAsset(
                        client_id="CLIENT-APEX", name="Apex Health", tier="enterprise"
                    )
                ],
                campaigns=[
                    BlueprintCampaignAsset(
                        campaign_id="CMP-APEX-01",
                        client_id="CLIENT-APEX",
                        name="Apex Spring Launch",
                        channel="paid_social",
                        status="scheduled",
                        budget_usd=95000,
                        spend_usd=98000,
                        pacing_pct=128.0,
                    )
                ],
                creatives=[
                    BlueprintCreativeAsset(
                        creative_id="CRT-APEX-01",
                        campaign_id="CMP-APEX-01",
                        title="Hero Video",
                        status="pending_review",
                        approval_required=True,
                    )
                ],
                approvals=[
                    BlueprintCampaignApprovalAsset(
                        approval_id="APR-APEX-01",
                        campaign_id="CMP-APEX-01",
                        stage="client_creative",
                        status="pending",
                    )
                ],
                reports=[
                    BlueprintCampaignReportAsset(
                        report_id="RPT-APEX-01",
                        campaign_id="CMP-APEX-01",
                        title="Launch Readiness Snapshot",
                        status="stale",
                        stale=True,
                    )
                ],
                metadata={"primary_client": "CLIENT-APEX"},
            ),
            metadata={
                "vertical": "digital_marketing_agency",
                "what_if_branches": [
                    "Launch without creative approval",
                    "Pause launch but fail to update client artifacts",
                ],
            },
        ),
        metadata={"vertical": "digital_marketing_agency"},
    )


def _storage_asset() -> BlueprintAsset:
    return BlueprintAsset(
        name="storage_solutions.blueprint",
        title="Atlas Storage Systems",
        description=(
            "Strategic capacity quote commitment with fragmented inventory, vendor coordination, and customer artifact pressure."
        ),
        scenario_name="capacity_quote_commitment",
        family_name="storage_solutions",
        workflow_name="storage_solutions",
        workflow_variant="capacity_quote_commitment",
        requested_facades=[
            "slack",
            "docs",
            "jira",
            "servicedesk",
            "inventory_ops",
            "crm",
        ],
        capability_graphs=BlueprintCapabilityGraphsAsset(
            organization_name="Atlas Storage Systems",
            organization_domain="atlasstorage.example.com",
            timezone="America/Chicago",
            scenario_brief=(
                "A strategic customer wants urgent capacity, but inventory is fragmented and the quote may overcommit before ops planning is aligned."
            ),
            comm_graph=BlueprintCommGraphAsset(
                slack_initial_message="Strategic quote review is now in the war room.",
                slack_channels=[
                    _channel(
                        "#atlas-commitments",
                        "The Zenith quote is at risk of overcommit unless capacity and vendor planning are aligned.",
                    )
                ],
            ),
            doc_graph=BlueprintDocGraphAsset(
                documents=[
                    BlueprintDocumentAsset(
                        doc_id="DOC-ATS-QUOTE",
                        title="Zenith Storage Rollout Plan",
                        body="Rollout plan draft.\n\nCapacity fragmented.\nVendor assignment pending.",
                        tags=["quote", "ops"],
                    )
                ]
            ),
            work_graph=BlueprintWorkGraphAsset(
                tickets=[
                    BlueprintTicketAsset(
                        ticket_id="JRA-ATS-51",
                        title="Zenith capacity commitment review",
                        status="open",
                        assignee="solutions-engineer",
                        description="Confirm feasible capacity before customer commitment is sent.",
                    )
                ],
                service_requests=[
                    BlueprintServiceRequestAsset(
                        request_id="REQ-ATS-1",
                        title="Vendor dispatch approval",
                        status="pending_approval",
                        requester="ops-lead",
                        approvals=[
                            BlueprintApprovalAsset(stage="dispatch", status="PENDING")
                        ],
                    )
                ],
            ),
            revenue_graph=BlueprintRevenueGraphAsset(
                companies=[
                    BlueprintCrmCompanyAsset(
                        id="CRM-ATS-C1",
                        name="Zenith Biologics",
                        domain="zenithbio.example.com",
                    )
                ],
                contacts=[
                    BlueprintCrmContactAsset(
                        id="CRM-ATS-P1",
                        email="darcy@zenithbio.example.com",
                        first_name="Darcy",
                        last_name="Ng",
                        company_id="CRM-ATS-C1",
                    )
                ],
                deals=[
                    BlueprintCrmDealAsset(
                        id="CRM-ATS-D1",
                        name="Zenith Expansion",
                        amount=420000,
                        stage="quote_at_risk",
                        owner="morgan.storage@example.com",
                        company_id="CRM-ATS-C1",
                        contact_id="CRM-ATS-P1",
                    )
                ],
            ),
            inventory_graph=BlueprintInventoryGraphAsset(
                sites=[
                    BlueprintSiteAsset(
                        site_id="SITE-CHI-1",
                        name="Chicago North",
                        city="Chicago",
                        region="midwest",
                    ),
                    BlueprintSiteAsset(
                        site_id="SITE-MKE-1",
                        name="Milwaukee West",
                        city="Milwaukee",
                        region="midwest",
                    ),
                ],
                capacity_pools=[
                    BlueprintCapacityPoolAsset(
                        pool_id="POOL-CHI-A",
                        site_id="SITE-CHI-1",
                        name="Climate A",
                        total_units=140,
                        reserved_units=120,
                    ),
                    BlueprintCapacityPoolAsset(
                        pool_id="POOL-MKE-B",
                        site_id="SITE-MKE-1",
                        name="Overflow B",
                        total_units=180,
                        reserved_units=30,
                    ),
                ],
                storage_units=[
                    BlueprintStorageUnitAsset(
                        unit_id="UNIT-CHI-A1", pool_id="POOL-CHI-A", label="A1"
                    ),
                    BlueprintStorageUnitAsset(
                        unit_id="UNIT-MKE-B1", pool_id="POOL-MKE-B", label="B1"
                    ),
                ],
                quotes=[
                    BlueprintQuoteAsset(
                        quote_id="Q-ATS-900",
                        customer_name="Zenith Biologics",
                        requested_units=80,
                        status="draft",
                        site_id="SITE-CHI-1",
                        committed_units=0,
                    )
                ],
                orders=[
                    BlueprintOrderAsset(
                        order_id="ORD-ATS-900",
                        quote_id="Q-ATS-900",
                        status="pending_vendor",
                        site_id="SITE-CHI-1",
                    )
                ],
                allocations=[],
                vendors=[
                    BlueprintVendorAsset(
                        vendor_id="VEND-ATS-TRUCK",
                        name="Rapid Freight",
                        specialty="transport",
                    ),
                    BlueprintVendorAsset(
                        vendor_id="VEND-ATS-OPS",
                        name="ColdVault Ops",
                        specialty="fulfillment",
                    ),
                ],
                metadata={"strategic_customer": "Zenith Biologics"},
            ),
            metadata={
                "vertical": "storage_solutions",
                "what_if_branches": [
                    "Commit quote before capacity is feasible",
                    "Reserve capacity but forget vendor/ops follow-through",
                ],
            },
        ),
        metadata={"vertical": "storage_solutions"},
    )


_VERTICAL_PACKS: Dict[str, VerticalPackManifest] = {
    "real_estate_management": VerticalPackManifest(
        name="real_estate_management",
        title="Real Estate Management",
        description="Lease, vendor, and property-readiness conflict for a high-stakes tenant opening.",
        company_name="Harbor Point Management",
        company_briefing=(
            "Harbor Point Management operates retail and mixed-use properties, coordinating "
            "leasing, property operations, vendors, tenant readiness, and customer-facing artifacts."
        ),
        failure_impact=(
            "If this scenario goes badly, Harbor Point misses a flagship tenant opening, loses tenant trust, "
            "and creates an expensive operational scramble across leasing, facilities, and vendors."
        ),
        objective_focus=(
            "Keep the opening valid and business-real: lease state, unit readiness, vendor work, and tenant-facing "
            "artifacts all need to line up before Monday morning."
        ),
        scenario_name="tenant_opening_conflict",
        workflow_name="real_estate_management",
        workflow_variant="tenant_opening_conflict",
        key_surfaces=["property_graph", "docs", "slack", "jira", "servicedesk"],
        proves=[
            "branchable opening readiness",
            "vendor/lease coordination",
            "artifact follow-through",
        ],
        what_if_branches=[
            "Delay vendor assignment and miss opening",
            "Execute amendment but leave the unit unreserved",
        ],
    ),
    "digital_marketing_agency": VerticalPackManifest(
        name="digital_marketing_agency",
        title="Digital Marketing Agency",
        description="Launch guardrail workflow for a campaign with approval, pacing, and reporting risk.",
        company_name="Northstar Growth",
        company_briefing=(
            "Northstar Growth runs client campaigns across channels, creative approvals, reporting, budgets, "
            "and account communication, with launch integrity depending on multiple teams staying aligned."
        ),
        failure_impact=(
            "If this scenario breaks, the agency can launch unapproved creative, overspend budget, and erode client trust "
            "with stale reporting and confused communication."
        ),
        objective_focus=(
            "Protect launch integrity: approvals, pacing, reporting, and client-facing artifacts should all be trustworthy "
            "before spend is allowed to keep flowing."
        ),
        scenario_name="campaign_launch_guardrail",
        workflow_name="digital_marketing_agency",
        workflow_variant="campaign_launch_guardrail",
        key_surfaces=["campaign_graph", "docs", "slack", "jira", "crm"],
        proves=["launch safety", "budget control", "client artifact hygiene"],
        what_if_branches=[
            "Pause the launch and protect spend",
            "Push through with stale reporting and approval drift",
        ],
    ),
    "storage_solutions": VerticalPackManifest(
        name="storage_solutions",
        title="Storage Solutions",
        description="Strategic customer quote with fragmented capacity and fulfillment coordination pressure.",
        company_name="Atlas Storage Systems",
        company_briefing=(
            "Atlas Storage Systems designs and fulfills large-scale storage rollouts, coordinating quotes, capacity, "
            "site allocation, vendors, fulfillment planning, and customer commitments."
        ),
        failure_impact=(
            "If this scenario fails, Atlas can overcommit capacity, send an impossible quote, and create downstream "
            "fulfillment failures for a strategic customer rollout."
        ),
        objective_focus=(
            "Keep the commercial promise feasible: capacity allocation, ops planning, vendor follow-through, and "
            "customer-facing artifacts must remain internally consistent."
        ),
        scenario_name="capacity_quote_commitment",
        workflow_name="storage_solutions",
        workflow_variant="capacity_quote_commitment",
        key_surfaces=["inventory_graph", "docs", "slack", "jira", "crm"],
        proves=["capacity feasibility", "quote accuracy", "ops follow-through"],
        what_if_branches=[
            "Reserve fragmented capacity and keep the customer timeline",
            "Overcommit the quote and create a downstream fulfillment failure",
        ],
    ),
}


_VERTICAL_BUILDERS = {
    "real_estate_management": _real_estate_asset,
    "digital_marketing_agency": _marketing_asset,
    "storage_solutions": _storage_asset,
}
