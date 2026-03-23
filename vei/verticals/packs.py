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
    BlueprintMailMessageAsset,
    BlueprintMailThreadAsset,
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
    BlueprintAllocationAsset,
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


def _slack_message(
    ts: str,
    user: str,
    text: str,
    *,
    thread_ts: str | None = None,
) -> BlueprintSlackMessageAsset:
    return BlueprintSlackMessageAsset(
        ts=ts,
        user=user,
        text=text,
        thread_ts=thread_ts,
    )


def _channel(
    channel: str,
    *,
    unread: int = 0,
    messages: List[BlueprintSlackMessageAsset],
) -> BlueprintSlackChannelAsset:
    return BlueprintSlackChannelAsset(
        channel=channel,
        unread=unread,
        messages=messages,
    )


def _mail_message(
    from_address: str,
    to_address: str,
    subject: str,
    body_text: str,
    *,
    unread: bool = True,
    time_ms: int | None = None,
) -> BlueprintMailMessageAsset:
    return BlueprintMailMessageAsset(
        from_address=from_address,
        to_address=to_address,
        subject=subject,
        body_text=body_text,
        unread=unread,
        time_ms=time_ms,
    )


def _mail_thread(
    thread_id: str,
    *,
    title: str,
    category: str,
    messages: List[BlueprintMailMessageAsset],
) -> BlueprintMailThreadAsset:
    return BlueprintMailThreadAsset(
        thread_id=thread_id,
        title=title,
        category=category,
        messages=messages,
    )


def _real_estate_asset() -> BlueprintAsset:
    slack_channels = [
        _channel(
            "#harbor-point-ops",
            unread=4,
            messages=[
                _slack_message(
                    "1710000000.000100",
                    "harbor.ops",
                    "Anchor tenant opening still blocked by lease amendment and HVAC work order.",
                ),
                _slack_message(
                    "1710000060.000200",
                    "nina.leasing",
                    "Legal says the amendment packet is in final review, but the tenant wants a clean Monday answer today.",
                ),
                _slack_message(
                    "1710000120.000300",
                    "marcus.facilities",
                    "We can hold the prep window, but only if Westshore confirms the Saturday slot before 3pm.",
                    thread_ts="1710000000.000100",
                ),
                _slack_message(
                    "1710000180.000400",
                    "sophia.gm",
                    "If the unit is not reserved by tonight, storefront teams need a fallback plan.",
                    thread_ts="1710000000.000100",
                ),
            ],
        ),
        _channel(
            "#leasing-huddle",
            unread=2,
            messages=[
                _slack_message(
                    "1710000240.000500",
                    "nina.leasing",
                    "Meridian wants confirmation that signage, keys, and opening-day access will all be synchronized.",
                ),
                _slack_message(
                    "1710000300.000600",
                    "harper.legal",
                    "The amendment is ready to execute once the revised occupancy language is acknowledged.",
                ),
                _slack_message(
                    "1710000360.000700",
                    "sam.tenants",
                    "I can send the tenant-facing readiness note as soon as the checklist is updated.",
                    thread_ts="1710000240.000500",
                ),
            ],
        ),
        _channel(
            "#vendor-desk",
            unread=3,
            messages=[
                _slack_message(
                    "1710000420.000800",
                    "marcus.facilities",
                    "Westshore HVAC has not accepted the commissioning hold yet.",
                ),
                _slack_message(
                    "1710000480.000900",
                    "marcus.facilities",
                    "Brightline Electric confirmed backup availability for Monday morning only.",
                ),
                _slack_message(
                    "1710000540.001000",
                    "sophia.gm",
                    "Keep a second vendor warm in case the HVAC window slips again.",
                ),
            ],
        ),
        _channel(
            "#tenant-success",
            unread=1,
            messages=[
                _slack_message(
                    "1710000600.001100",
                    "sam.tenants",
                    "Tenant launch packet still says amendment pending. I need the updated opening language before noon.",
                ),
                _slack_message(
                    "1710000660.001200",
                    "nina.leasing",
                    "Hold customer-facing sends until lease and unit status line up.",
                    thread_ts="1710000600.001100",
                ),
            ],
        ),
        _channel(
            "#exec-brief",
            unread=1,
            messages=[
                _slack_message(
                    "1710000720.001300",
                    "darren.ops",
                    "Board walkthrough is tomorrow. Harbor Point needs a credible opening status, not optimism.",
                ),
            ],
        ),
        _channel(
            "#weekend-opening",
            unread=2,
            messages=[
                _slack_message(
                    "1710000780.001400",
                    "weekend.concierge",
                    "Security needs the final vendor roster and after-hours access list by 6pm Friday.",
                ),
                _slack_message(
                    "1710000840.001500",
                    "sam.tenants",
                    "Meridian is staging trainers onsite Sunday night if we stay green.",
                ),
            ],
        ),
    ]
    mail_threads = [
        _mail_thread(
            "MAIL-HPM-ANCHOR",
            title="Meridian opening coordination",
            category="customer",
            messages=[
                _mail_message(
                    "melissa@meridianfitness.example.com",
                    "nina.leasing@harborpoint.example.com",
                    "Need Monday opening confirmation",
                    "We are locking staffing and signage now. Can you confirm the unit will be ready and reserved for us by end of day?",
                    time_ms=1710001000000,
                ),
                _mail_message(
                    "nina.leasing@harborpoint.example.com",
                    "melissa@meridianfitness.example.com",
                    "Re: Need Monday opening confirmation",
                    "We are coordinating lease execution, vendor prep, and opening-day access now. I will send the final readiness packet today.",
                    unread=False,
                    time_ms=1710001300000,
                ),
            ],
        ),
        _mail_thread(
            "MAIL-HPM-HVAC",
            title="Westshore HVAC commissioning window",
            category="vendor",
            messages=[
                _mail_message(
                    "dispatch@westshorehvac.example.com",
                    "marcus.facilities@harborpoint.example.com",
                    "Saturday slot still unconfirmed",
                    "We can hold the 9am Saturday window for Harbor Point if approval is finalized before 3pm today.",
                    time_ms=1710001600000,
                ),
                _mail_message(
                    "marcus.facilities@harborpoint.example.com",
                    "dispatch@westshorehvac.example.com",
                    "Re: Saturday slot still unconfirmed",
                    "Approval is moving now. Please keep the crew on soft hold for unit 14A.",
                    unread=False,
                    time_ms=1710001900000,
                ),
            ],
        ),
        _mail_thread(
            "MAIL-HPM-LEGAL",
            title="Lease amendment final language",
            category="internal_follow_through",
            messages=[
                _mail_message(
                    "harper.legal@harborpoint.example.com",
                    "nina.leasing@harborpoint.example.com",
                    "Redlines for LEASE-HPM-14A",
                    "Updated amendment language is attached. Need leasing signoff on occupancy wording before execution.",
                    time_ms=1710002200000,
                ),
            ],
        ),
        _mail_thread(
            "MAIL-HPM-ACCESS",
            title="Weekend access and security roster",
            category="internal_follow_through",
            messages=[
                _mail_message(
                    "security@harborpoint.example.com",
                    "sophia.gm@harborpoint.example.com",
                    "Need final access roster for Harbor Point weekend work",
                    "Please send approved vendor names, unit list, and after-hours access windows for the opening prep teams.",
                    time_ms=1710002500000,
                ),
            ],
        ),
    ]
    documents = [
        BlueprintDocumentAsset(
            doc_id="DOC-HPM-OPENING",
            title="Harbor Point Opening Checklist",
            body=(
                "Opening checklist draft.\n\nLease amendment pending.\nVendor still unassigned.\n"
                "Tenant packet cannot go out until the unit is reserved and after-hours access is confirmed."
            ),
            tags=["opening", "tenant"],
        ),
        BlueprintDocumentAsset(
            doc_id="DOC-HPM-REDLINES",
            title="Meridian 14A Lease Amendment Redlines",
            body=(
                "Occupancy clause revised for Monday launch.\n"
                "Counsel notes that execution must happen before storefront reservation is finalized."
            ),
            tags=["lease", "legal"],
        ),
        BlueprintDocumentAsset(
            doc_id="DOC-HPM-VENDORS",
            title="Harbor Point Vendor Coverage Matrix",
            body=(
                "Westshore HVAC on soft hold.\nBrightline Electric available Monday fallback.\n"
                "Weekend access roster still missing final badge approvals."
            ),
            tags=["vendors", "ops"],
        ),
        BlueprintDocumentAsset(
            doc_id="DOC-HPM-ACCESS",
            title="Weekend Opening Access Plan",
            body=(
                "Security, loading dock, and concierge staffing plan for Harbor Point Plaza.\n"
                "Requires final vendor roster and unit reservation state."
            ),
            tags=["security", "opening"],
        ),
        BlueprintDocumentAsset(
            doc_id="DOC-HPM-TENANT",
            title="Meridian Tenant Launch Packet",
            body=(
                "Tenant-facing packet with move-in timing, storefront access, and opening-day support contacts.\n"
                "Currently marked hold until lease and prep milestones are complete."
            ),
            tags=["tenant", "comms"],
        ),
    ]
    tickets = [
        BlueprintTicketAsset(
            ticket_id="JRA-HPM-17",
            title="Tenant opening blocker review",
            status="open",
            assignee="ops-manager",
            description="Resolve lease/vendor blockers before Monday opening.",
        ),
        BlueprintTicketAsset(
            ticket_id="JRA-HPM-21",
            title="Storefront signage install timing",
            status="in_progress",
            assignee="sam.tenants",
            description="Coordinate signage install with unit handoff and after-hours access.",
        ),
        BlueprintTicketAsset(
            ticket_id="JRA-HPM-25",
            title="Weekend security staffing confirmation",
            status="open",
            assignee="security-lead",
            description="Confirm badge desk and loading dock support for Saturday prep crews.",
        ),
        BlueprintTicketAsset(
            ticket_id="JRA-HPM-29",
            title="Harbor Point opening packet refresh",
            status="review",
            assignee="leasing-coordinator",
            description="Update tenant-facing packet after lease and reservation status change.",
        ),
        BlueprintTicketAsset(
            ticket_id="JRA-HPM-34",
            title="North annex lobby maintenance follow-up",
            status="closed",
            assignee="facilities-coordinator",
            description="Secondary maintenance item for a different tenant suite at Harbor Point.",
        ),
    ]
    service_requests = [
        BlueprintServiceRequestAsset(
            request_id="REQ-HPM-1",
            title="Vendor approval for unit 14A prep",
            status="pending_approval",
            requester="leasing-manager",
            description="Approve HVAC vendor access and prep window for the anchor tenant space.",
            approvals=[BlueprintApprovalAsset(stage="vendor", status="PENDING")],
        ),
        BlueprintServiceRequestAsset(
            request_id="REQ-HPM-2",
            title="Weekend access roster approval",
            status="pending_approval",
            requester="security-lead",
            description="Approve the final after-hours roster for vendors and concierge coverage.",
            approvals=[BlueprintApprovalAsset(stage="security", status="PENDING")],
        ),
        BlueprintServiceRequestAsset(
            request_id="REQ-HPM-3",
            title="Tenant welcome packet release",
            status="approved",
            requester="tenant-success",
            description="Release the customer-facing launch packet once the opening state is green.",
            approvals=[BlueprintApprovalAsset(stage="tenant_comms", status="APPROVED")],
        ),
        BlueprintServiceRequestAsset(
            request_id="REQ-HPM-4",
            title="Loading dock Sunday staging window",
            status="in_progress",
            requester="facilities-dispatch",
            description="Reserve Sunday evening dock access for equipment staging and storefront setup.",
            approvals=[BlueprintApprovalAsset(stage="operations", status="APPROVED")],
        ),
    ]
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
        requested_facades=[
            "slack",
            "mail",
            "docs",
            "jira",
            "servicedesk",
            "property_ops",
        ],
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
                slack_channels=slack_channels,
                mail_threads=mail_threads,
            ),
            doc_graph=BlueprintDocGraphAsset(documents=documents),
            work_graph=BlueprintWorkGraphAsset(
                tickets=tickets,
                service_requests=service_requests,
            ),
            property_graph=BlueprintPropertyGraphAsset(
                properties=[
                    BlueprintPropertyAsset(
                        property_id="PROP-HPM-1",
                        name="Harbor Point Plaza",
                        city="Oakland",
                        state="CA",
                        portfolio="bay-area-retail",
                    ),
                    BlueprintPropertyAsset(
                        property_id="PROP-HPM-2",
                        name="Jack London Lofts",
                        city="Oakland",
                        state="CA",
                        portfolio="mixed-use",
                    ),
                ],
                buildings=[
                    BlueprintBuildingAsset(
                        building_id="BLDG-HPM-1",
                        property_id="PROP-HPM-1",
                        name="Building A",
                    ),
                    BlueprintBuildingAsset(
                        building_id="BLDG-HPM-2",
                        property_id="PROP-HPM-1",
                        name="South Arcade",
                    ),
                    BlueprintBuildingAsset(
                        building_id="BLDG-HPM-3",
                        property_id="PROP-HPM-2",
                        name="Loft Tower",
                    ),
                ],
                units=[
                    BlueprintUnitAsset(
                        unit_id="UNIT-HPM-14A",
                        building_id="BLDG-HPM-1",
                        label="14A",
                        status="vacant",
                    ),
                    BlueprintUnitAsset(
                        unit_id="UNIT-HPM-12C",
                        building_id="BLDG-HPM-1",
                        label="12C",
                        status="occupied",
                        reserved_for="TEN-HPM-STUDIO",
                    ),
                    BlueprintUnitAsset(
                        unit_id="UNIT-HPM-7B",
                        building_id="BLDG-HPM-2",
                        label="7B",
                        status="reserved",
                        reserved_for="TEN-HPM-SALON",
                    ),
                    BlueprintUnitAsset(
                        unit_id="UNIT-HPM-L3",
                        building_id="BLDG-HPM-3",
                        label="L3",
                        status="occupied",
                        reserved_for="TEN-HPM-LOFT",
                    ),
                ],
                tenants=[
                    BlueprintTenantAsset(
                        tenant_id="TEN-HPM-ANCHOR",
                        name="Meridian Fitness",
                        segment="anchor",
                        opening_deadline_ms=1710432000000,
                    ),
                    BlueprintTenantAsset(
                        tenant_id="TEN-HPM-STUDIO",
                        name="Harbor Yoga Lab",
                        segment="lifestyle",
                        opening_deadline_ms=1713013200000,
                    ),
                    BlueprintTenantAsset(
                        tenant_id="TEN-HPM-SALON",
                        name="Canal Salon",
                        segment="boutique",
                        opening_deadline_ms=1712073600000,
                    ),
                    BlueprintTenantAsset(
                        tenant_id="TEN-HPM-LOFT",
                        name="Signal Works",
                        segment="office",
                        opening_deadline_ms=1715625600000,
                    ),
                ],
                leases=[
                    BlueprintLeaseAsset(
                        lease_id="LEASE-HPM-14A",
                        tenant_id="TEN-HPM-ANCHOR",
                        unit_id="UNIT-HPM-14A",
                        status="pending",
                        milestone="amendment_pending",
                        amendment_pending=True,
                    ),
                    BlueprintLeaseAsset(
                        lease_id="LEASE-HPM-12C",
                        tenant_id="TEN-HPM-STUDIO",
                        unit_id="UNIT-HPM-12C",
                        status="active",
                        milestone="open",
                    ),
                    BlueprintLeaseAsset(
                        lease_id="LEASE-HPM-7B",
                        tenant_id="TEN-HPM-SALON",
                        unit_id="UNIT-HPM-7B",
                        status="ready",
                        milestone="keys_released",
                    ),
                    BlueprintLeaseAsset(
                        lease_id="LEASE-HPM-L3",
                        tenant_id="TEN-HPM-LOFT",
                        unit_id="UNIT-HPM-L3",
                        status="active",
                        milestone="renewal_watch",
                    ),
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
                    BlueprintVendorAsset(
                        vendor_id="VEND-HPM-SEC",
                        name="Sentinel Access",
                        specialty="security",
                    ),
                    BlueprintVendorAsset(
                        vendor_id="VEND-HPM-SIGN",
                        name="Eastbay Signworks",
                        specialty="signage",
                    ),
                ],
                work_orders=[
                    BlueprintWorkOrderAsset(
                        work_order_id="WO-HPM-88",
                        property_id="PROP-HPM-1",
                        title="HVAC commissioning for unit 14A",
                        status="pending_vendor",
                    ),
                    BlueprintWorkOrderAsset(
                        work_order_id="WO-HPM-77",
                        property_id="PROP-HPM-1",
                        title="Storefront signage install for unit 14A",
                        status="scheduled",
                        vendor_id="VEND-HPM-SIGN",
                    ),
                    BlueprintWorkOrderAsset(
                        work_order_id="WO-HPM-55",
                        property_id="PROP-HPM-1",
                        title="Weekend access desk staffing",
                        status="in_progress",
                        vendor_id="VEND-HPM-SEC",
                    ),
                    BlueprintWorkOrderAsset(
                        work_order_id="WO-HPM-42",
                        property_id="PROP-HPM-2",
                        title="Lobby lighting tune-up",
                        status="scheduled",
                        vendor_id="VEND-HPM-ELEC",
                    ),
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
    slack_channels = [
        _channel(
            "#northstar-launch",
            unread=5,
            messages=[
                _slack_message(
                    "1710100000.000100",
                    "casey.growth",
                    "Apex Health launch is pacing hot and creative approval is still pending.",
                ),
                _slack_message(
                    "1710100060.000200",
                    "mila.media",
                    "Meta spend crossed the guardrail overnight. If we do nothing we burn the weekly cap by lunch.",
                ),
                _slack_message(
                    "1710100120.000300",
                    "jon.creative",
                    "Client wants the final disclaimer slate visible before we ship the hero cut.",
                    thread_ts="1710100000.000100",
                ),
                _slack_message(
                    "1710100180.000400",
                    "riley.analytics",
                    "Readiness snapshot is stale. Last report still shows Friday pacing.",
                    thread_ts="1710100000.000100",
                ),
            ],
        ),
        _channel(
            "#client-apex",
            unread=2,
            messages=[
                _slack_message(
                    "1710100240.000500",
                    "casey.growth",
                    "Holding client-facing update until creative sign-off and pacing plan line up.",
                ),
                _slack_message(
                    "1710100300.000600",
                    "sophie.accounts",
                    "Melissa is asking whether launch is still green for this afternoon.",
                ),
            ],
        ),
        _channel(
            "#creative-review",
            unread=3,
            messages=[
                _slack_message(
                    "1710100360.000700",
                    "jon.creative",
                    "Hero Video v5 is uploaded. Need legal slate call and client approval.",
                ),
                _slack_message(
                    "1710100420.000800",
                    "mila.media",
                    "If the hero cut slips, we should pause paid social and keep search live.",
                ),
                _slack_message(
                    "1710100480.000900",
                    "jon.creative",
                    "Static fallback assets are ready if we need a safer launch path.",
                    thread_ts="1710100360.000700",
                ),
            ],
        ),
        _channel(
            "#budget-watch",
            unread=2,
            messages=[
                _slack_message(
                    "1710100540.001000",
                    "riley.analytics",
                    "Search pacing is healthy. Paid social is the outlier.",
                ),
                _slack_message(
                    "1710100600.001100",
                    "alina.finance",
                    "Apex launch is 28% over planned pacing if the current multiplier holds.",
                ),
            ],
        ),
        _channel(
            "#exec-brief",
            unread=1,
            messages=[
                _slack_message(
                    "1710100660.001200",
                    "noah.partner",
                    "Protect client trust first. Late launch is recoverable, broken launch is not.",
                ),
            ],
        ),
        _channel(
            "#reporting-cab",
            unread=1,
            messages=[
                _slack_message(
                    "1710100720.001300",
                    "riley.analytics",
                    "Dashboard datasource lagged during the weekend sync. Ready to republish once pacing is corrected.",
                ),
            ],
        ),
    ]
    mail_threads = [
        _mail_thread(
            "MAIL-NSG-CLIENT",
            title="Apex Health launch timing",
            category="customer",
            messages=[
                _mail_message(
                    "melissa@apexhealth.example.com",
                    "alex.account@northstar.example.com",
                    "Still on for this afternoon?",
                    "Please confirm whether the hero creative and reporting packet will be finalized before launch. Our leadership team is asking now.",
                    time_ms=1710101000000,
                ),
                _mail_message(
                    "alex.account@northstar.example.com",
                    "melissa@apexhealth.example.com",
                    "Re: Still on for this afternoon?",
                    "We are verifying creative approval and pacing safety. I will send the readiness note shortly.",
                    unread=False,
                    time_ms=1710101300000,
                ),
            ],
        ),
        _mail_thread(
            "MAIL-NSG-CREATIVE",
            title="Hero video approval chain",
            category="internal_follow_through",
            messages=[
                _mail_message(
                    "jon.creative@northstar.example.com",
                    "alex.account@northstar.example.com",
                    "Hero cut ready for final signoff",
                    "Version 5 is uploaded with the medical disclaimer slate. Need the client signoff recorded before trafficking.",
                    time_ms=1710101600000,
                ),
            ],
        ),
        _mail_thread(
            "MAIL-NSG-REPORT",
            title="Readiness report refresh",
            category="internal_follow_through",
            messages=[
                _mail_message(
                    "riley.analytics@northstar.example.com",
                    "alex.account@northstar.example.com",
                    "Launch report still stale",
                    "The PDF packet is still pointing at Friday data. I can republish as soon as pacing corrections are agreed.",
                    time_ms=1710101900000,
                ),
            ],
        ),
        _mail_thread(
            "MAIL-NSG-VENDOR",
            title="Paid social pacing escalation",
            category="vendor",
            messages=[
                _mail_message(
                    "support@metapartners.example.com",
                    "alex.account@northstar.example.com",
                    "Bid multipliers changed during weekend learning phase",
                    "We observed elevated bid pressure on the Apex launch set. Recommend pausing auto-expansion until caps are reset.",
                    time_ms=1710102200000,
                ),
            ],
        ),
    ]
    documents = [
        BlueprintDocumentAsset(
            doc_id="DOC-NSG-LAUNCH",
            title="Apex Health Launch Brief",
            body=(
                "Launch brief.\n\nCreative approval outstanding.\nReporting artifact stale.\n"
                "Paid social pacing exceeds the approved launch guardrail."
            ),
            tags=["launch", "client"],
        ),
        BlueprintDocumentAsset(
            doc_id="DOC-NSG-CREATIVE",
            title="Apex Creative Review Notes",
            body=(
                "Hero video requires final disclaimer confirmation.\n"
                "Static fallback assets are approved and can substitute if the client signoff stalls."
            ),
            tags=["creative", "review"],
        ),
        BlueprintDocumentAsset(
            doc_id="DOC-NSG-BUDGET",
            title="Apex Paid Media Budget Tracker",
            body=(
                "Weekly spend plan versus current pacing.\n"
                "Meta social is over target; branded search remains inside guardrails."
            ),
            tags=["budget", "media"],
        ),
        BlueprintDocumentAsset(
            doc_id="DOC-NSG-REPORT",
            title="Launch Readiness Snapshot Draft",
            body=(
                "Reporting packet for client review.\n"
                "Needs refreshed pacing metrics and final launch recommendation."
            ),
            tags=["reporting", "client"],
        ),
        BlueprintDocumentAsset(
            doc_id="DOC-NSG-COMMS",
            title="Apex Client Update Draft",
            body=(
                "Client-facing status note that explains launch readiness, pacing risk, and the safe fallback plan."
            ),
            tags=["client", "comms"],
        ),
    ]
    tickets = [
        BlueprintTicketAsset(
            ticket_id="JRA-NSG-33",
            title="Apex Health launch guardrail",
            status="open",
            assignee="account-lead",
            description="Clear approval, pacing, and reporting blockers before launch.",
        ),
        BlueprintTicketAsset(
            ticket_id="JRA-NSG-37",
            title="Refresh launch reporting packet",
            status="in_progress",
            assignee="analytics-lead",
            description="Republish the client packet with current pacing and channel status.",
        ),
        BlueprintTicketAsset(
            ticket_id="JRA-NSG-41",
            title="Confirm disclaimer slate in hero cut",
            status="review",
            assignee="creative-director",
            description="Make sure the final creative approved by the client matches trafficking assets.",
        ),
        BlueprintTicketAsset(
            ticket_id="JRA-NSG-45",
            title="North Valley retargeting QA",
            status="open",
            assignee="campaign-manager",
            description="Secondary client task that keeps the workspace feeling live without driving the Apex mission.",
        ),
        BlueprintTicketAsset(
            ticket_id="JRA-NSG-49",
            title="Weekly account recap prep",
            status="closed",
            assignee="account-coordinator",
            description="Wrap last week’s internal recap for Monday portfolio review.",
        ),
    ]
    service_requests = [
        BlueprintServiceRequestAsset(
            request_id="REQ-NSG-1",
            title="Creative sign-off",
            status="pending_approval",
            requester="creative-director",
            description="Record final client approval before the hero creative is trafficked.",
            approvals=[BlueprintApprovalAsset(stage="creative", status="PENDING")],
        ),
        BlueprintServiceRequestAsset(
            request_id="REQ-NSG-2",
            title="Budget cap exception review",
            status="pending_approval",
            requester="media-manager",
            description="Request temporary cap increase if the launch remains live without a pacing reset.",
            approvals=[BlueprintApprovalAsset(stage="finance", status="PENDING")],
        ),
        BlueprintServiceRequestAsset(
            request_id="REQ-NSG-3",
            title="Client reporting release",
            status="approved",
            requester="analytics-lead",
            description="Approve the refreshed readiness packet for client distribution.",
            approvals=[BlueprintApprovalAsset(stage="account", status="APPROVED")],
        ),
        BlueprintServiceRequestAsset(
            request_id="REQ-NSG-4",
            title="Fallback static creative package",
            status="in_progress",
            requester="creative-ops",
            description="Keep the fallback asset bundle ready if the hero cut remains blocked.",
            approvals=[BlueprintApprovalAsset(stage="creative_ops", status="APPROVED")],
        ),
    ]
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
            "mail",
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
                slack_channels=slack_channels,
                mail_threads=mail_threads,
            ),
            doc_graph=BlueprintDocGraphAsset(documents=documents),
            work_graph=BlueprintWorkGraphAsset(
                tickets=tickets,
                service_requests=service_requests,
            ),
            revenue_graph=BlueprintRevenueGraphAsset(
                companies=[
                    BlueprintCrmCompanyAsset(
                        id="CRM-NSG-C1",
                        name="Apex Health",
                        domain="apexhealth.example.com",
                    ),
                    BlueprintCrmCompanyAsset(
                        id="CRM-NSG-C2",
                        name="North Valley Dental",
                        domain="northvalleydental.example.com",
                    ),
                ],
                contacts=[
                    BlueprintCrmContactAsset(
                        id="CRM-NSG-P1",
                        email="melissa@apexhealth.example.com",
                        first_name="Melissa",
                        last_name="Grant",
                        company_id="CRM-NSG-C1",
                    ),
                    BlueprintCrmContactAsset(
                        id="CRM-NSG-P2",
                        email="owen@northvalleydental.example.com",
                        first_name="Owen",
                        last_name="Price",
                        company_id="CRM-NSG-C2",
                    ),
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
                    ),
                    BlueprintCrmDealAsset(
                        id="CRM-NSG-D2",
                        name="North Valley Summer Sprint",
                        amount=88000,
                        stage="qa_review",
                        owner="sophie.accounts@example.com",
                        company_id="CRM-NSG-C2",
                        contact_id="CRM-NSG-P2",
                    ),
                ],
            ),
            campaign_graph=BlueprintCampaignGraphAsset(
                clients=[
                    BlueprintClientAsset(
                        client_id="CLIENT-APEX", name="Apex Health", tier="enterprise"
                    ),
                    BlueprintClientAsset(
                        client_id="CLIENT-NVD",
                        name="North Valley Dental",
                        tier="growth",
                    ),
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
                    ),
                    BlueprintCampaignAsset(
                        campaign_id="CMP-APEX-02",
                        client_id="CLIENT-APEX",
                        name="Apex Search Defense",
                        channel="paid_search",
                        status="active",
                        budget_usd=28000,
                        spend_usd=21400,
                        pacing_pct=94.0,
                    ),
                    BlueprintCampaignAsset(
                        campaign_id="CMP-NVD-01",
                        client_id="CLIENT-NVD",
                        name="North Valley Retargeting",
                        channel="display",
                        status="active",
                        budget_usd=18000,
                        spend_usd=11000,
                        pacing_pct=88.0,
                    ),
                ],
                creatives=[
                    BlueprintCreativeAsset(
                        creative_id="CRT-APEX-01",
                        campaign_id="CMP-APEX-01",
                        title="Hero Video",
                        status="pending_review",
                        approval_required=True,
                    ),
                    BlueprintCreativeAsset(
                        creative_id="CRT-APEX-02",
                        campaign_id="CMP-APEX-01",
                        title="Static Fallback Carousel",
                        status="approved",
                        approval_required=True,
                    ),
                    BlueprintCreativeAsset(
                        creative_id="CRT-APEX-03",
                        campaign_id="CMP-APEX-02",
                        title="Search Ad Set April",
                        status="approved",
                        approval_required=False,
                    ),
                    BlueprintCreativeAsset(
                        creative_id="CRT-NVD-01",
                        campaign_id="CMP-NVD-01",
                        title="Retargeting Banner Kit",
                        status="approved",
                        approval_required=True,
                    ),
                ],
                approvals=[
                    BlueprintCampaignApprovalAsset(
                        approval_id="APR-APEX-01",
                        campaign_id="CMP-APEX-01",
                        stage="client_creative",
                        status="pending",
                    ),
                    BlueprintCampaignApprovalAsset(
                        approval_id="APR-APEX-02",
                        campaign_id="CMP-APEX-01",
                        stage="budget_guardrail",
                        status="pending",
                    ),
                    BlueprintCampaignApprovalAsset(
                        approval_id="APR-NVD-01",
                        campaign_id="CMP-NVD-01",
                        stage="client_creative",
                        status="approved",
                    ),
                ],
                reports=[
                    BlueprintCampaignReportAsset(
                        report_id="RPT-APEX-01",
                        campaign_id="CMP-APEX-01",
                        title="Launch Readiness Snapshot",
                        status="stale",
                        stale=True,
                    ),
                    BlueprintCampaignReportAsset(
                        report_id="RPT-APEX-02",
                        campaign_id="CMP-APEX-02",
                        title="Search Pacing Summary",
                        status="fresh",
                        stale=False,
                    ),
                    BlueprintCampaignReportAsset(
                        report_id="RPT-NVD-01",
                        campaign_id="CMP-NVD-01",
                        title="Retargeting Weekly Readout",
                        status="fresh",
                        stale=False,
                    ),
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
    slack_channels = [
        _channel(
            "#atlas-commitments",
            unread=4,
            messages=[
                _slack_message(
                    "1710200000.000100",
                    "morgan.storage",
                    "The Zenith quote is at risk of overcommit unless capacity and vendor planning are aligned.",
                ),
                _slack_message(
                    "1710200060.000200",
                    "haruto.ops",
                    "Chicago North can carry the first wave, but only if cold-chain overflow is reassigned out of Pool A.",
                ),
                _slack_message(
                    "1710200120.000300",
                    "jules.fulfillment",
                    "Rapid Freight has not accepted the dispatch window yet.",
                    thread_ts="1710200000.000100",
                ),
                _slack_message(
                    "1710200180.000400",
                    "priya.revops",
                    "Customer-facing quote still reads as fully committed capacity. That is ahead of ops reality.",
                    thread_ts="1710200000.000100",
                ),
            ],
        ),
        _channel(
            "#capacity-watch",
            unread=2,
            messages=[
                _slack_message(
                    "1710200240.000500",
                    "reese.capacity",
                    "Pool CHI-A is at 86% reservation. Overflow pool MKE-B remains mostly open.",
                ),
                _slack_message(
                    "1710200300.000600",
                    "haruto.ops",
                    "Milwaukee can take some of the temperature-stable units, but not the cold-chain tranche.",
                ),
            ],
        ),
        _channel(
            "#dispatch-desk",
            unread=2,
            messages=[
                _slack_message(
                    "1710200360.000700",
                    "jules.fulfillment",
                    "Need vendor confirmation plus loading sequence before I mark ORD-ATS-900 ready.",
                ),
                _slack_message(
                    "1710200420.000800",
                    "leo.dispatch",
                    "Rapid Freight requested updated pallet counts and site split.",
                ),
            ],
        ),
        _channel(
            "#customer-zenith",
            unread=1,
            messages=[
                _slack_message(
                    "1710200480.000900",
                    "morgan.storage",
                    "Holding the customer note until the quote and allocation story match.",
                ),
                _slack_message(
                    "1710200510.000950",
                    "darcy.zenith",
                    "Our lab ops team only needs one truthful answer: what capacity is truly committed today?",
                ),
            ],
        ),
        _channel(
            "#ops-forecast",
            unread=1,
            messages=[
                _slack_message(
                    "1710200540.001000",
                    "priya.revops",
                    "Atlas Medline quote is also drawing on Chicago capacity next week. Keep that in the picture.",
                ),
            ],
        ),
        _channel(
            "#exec-brief",
            unread=1,
            messages=[
                _slack_message(
                    "1710200600.001100",
                    "audrey.gm",
                    "We can lose a day. We cannot lose credibility with a strategic rollout promise.",
                ),
            ],
        ),
    ]
    mail_threads = [
        _mail_thread(
            "MAIL-ATS-CUSTOMER",
            title="Zenith rollout commitment",
            category="customer",
            messages=[
                _mail_message(
                    "darcy@zenithbio.example.com",
                    "priya.revops@atlasstorage.example.com",
                    "Can you still honor the April rollout window?",
                    "Our program team needs a written commitment on unit count, site split, and dispatch timing by this afternoon.",
                    time_ms=1710201000000,
                ),
                _mail_message(
                    "priya.revops@atlasstorage.example.com",
                    "darcy@zenithbio.example.com",
                    "Re: Can you still honor the April rollout window?",
                    "We are finalizing the feasible site split and dispatch plan now. I will send the updated rollout note shortly.",
                    unread=False,
                    time_ms=1710201300000,
                ),
            ],
        ),
        _mail_thread(
            "MAIL-ATS-DISPATCH",
            title="Rapid Freight dispatch hold",
            category="vendor",
            messages=[
                _mail_message(
                    "ops@rapidfreight.example.com",
                    "haruto.ops@atlasstorage.example.com",
                    "Need confirmed unit counts before dispatch lock",
                    "We can protect the Friday pickup window if the final pallet map and cold-chain count arrive before 2pm.",
                    time_ms=1710201600000,
                ),
            ],
        ),
        _mail_thread(
            "MAIL-ATS-OPS",
            title="Chicago overflow planning",
            category="internal_follow_through",
            messages=[
                _mail_message(
                    "haruto.ops@atlasstorage.example.com",
                    "priya.revops@atlasstorage.example.com",
                    "Feasible split for Zenith if Milwaukee takes ambient inventory",
                    "Attaching the draft split. We still need quote language updated so sales does not overpromise Chicago capacity.",
                    time_ms=1710201900000,
                ),
            ],
        ),
        _mail_thread(
            "MAIL-ATS-COMMERCIAL",
            title="Quote wording for strategic customer",
            category="internal_follow_through",
            messages=[
                _mail_message(
                    "priya.revops@atlasstorage.example.com",
                    "haruto.ops@atlasstorage.example.com",
                    "Quote still shows full commitment",
                    "Please revise the quote note so it matches the allocation plan and the vendor dispatch status before it goes out.",
                    time_ms=1710202200000,
                ),
            ],
        ),
    ]
    documents = [
        BlueprintDocumentAsset(
            doc_id="DOC-ATS-QUOTE",
            title="Zenith Storage Rollout Plan",
            body=(
                "Rollout plan draft.\n\nCapacity fragmented.\nVendor assignment pending.\n"
                "Customer note should not promise a fully committed rollout until allocation is feasible."
            ),
            tags=["quote", "ops"],
        ),
        BlueprintDocumentAsset(
            doc_id="DOC-ATS-CAPACITY",
            title="Midwest Capacity Split Worksheet",
            body=(
                "Chicago cold-chain demand versus Milwaukee ambient overflow.\n"
                "Shows the site split needed to keep the Zenith program feasible."
            ),
            tags=["capacity", "planning"],
        ),
        BlueprintDocumentAsset(
            doc_id="DOC-ATS-DISPATCH",
            title="Dispatch Window Checklist",
            body=(
                "Vendor pickup windows, pallet counts, and site sequencing for strategic storage rollouts."
            ),
            tags=["dispatch", "vendors"],
        ),
        BlueprintDocumentAsset(
            doc_id="DOC-ATS-CUSTOMER",
            title="Zenith Customer Commitment Draft",
            body=(
                "Customer-facing note with capacity promise, ramp timing, and operating assumptions.\n"
                "Needs revision before it matches current allocation reality."
            ),
            tags=["customer", "comms"],
        ),
        BlueprintDocumentAsset(
            doc_id="DOC-ATS-ALTAS",
            title="Atlas Medline Overflow Notes",
            body=(
                "Secondary customer planning note that explains why Chicago capacity is tighter than the quote currently implies."
            ),
            tags=["portfolio", "capacity"],
        ),
    ]
    tickets = [
        BlueprintTicketAsset(
            ticket_id="JRA-ATS-51",
            title="Zenith capacity commitment review",
            status="open",
            assignee="solutions-engineer",
            description="Confirm feasible capacity before customer commitment is sent.",
        ),
        BlueprintTicketAsset(
            ticket_id="JRA-ATS-58",
            title="Update site split in quote language",
            status="in_progress",
            assignee="revops-manager",
            description="Make the commercial promise match the current feasible allocation plan.",
        ),
        BlueprintTicketAsset(
            ticket_id="JRA-ATS-61",
            title="Confirm cold-chain overflow routing",
            status="review",
            assignee="ops-planner",
            description="Validate Chicago and Milwaukee split for the Zenith rollout.",
        ),
        BlueprintTicketAsset(
            ticket_id="JRA-ATS-66",
            title="Atlas Medline dispatch follow-up",
            status="open",
            assignee="dispatch-coordinator",
            description="Secondary customer task that keeps the operations world populated.",
        ),
        BlueprintTicketAsset(
            ticket_id="JRA-ATS-70",
            title="Quarterly warehouse labeling audit",
            status="closed",
            assignee="site-ops",
            description="Closed housekeeping task from last week’s warehouse review.",
        ),
    ]
    service_requests = [
        BlueprintServiceRequestAsset(
            request_id="REQ-ATS-1",
            title="Vendor dispatch approval",
            status="pending_approval",
            requester="ops-lead",
            description="Approve dispatch and loading sequence once the capacity plan is feasible.",
            approvals=[BlueprintApprovalAsset(stage="dispatch", status="PENDING")],
        ),
        BlueprintServiceRequestAsset(
            request_id="REQ-ATS-2",
            title="Overflow pool reservation release",
            status="pending_approval",
            requester="capacity-manager",
            description="Release Milwaukee overflow reserve if Zenith takes the ambient split.",
            approvals=[BlueprintApprovalAsset(stage="capacity", status="PENDING")],
        ),
        BlueprintServiceRequestAsset(
            request_id="REQ-ATS-3",
            title="Customer commitment packet",
            status="approved",
            requester="account-exec",
            description="Approve the customer-ready rollout packet once quote language is accurate.",
            approvals=[BlueprintApprovalAsset(stage="commercial", status="APPROVED")],
        ),
        BlueprintServiceRequestAsset(
            request_id="REQ-ATS-4",
            title="Cold-chain loading dock reservation",
            status="in_progress",
            requester="warehouse-ops",
            description="Reserve the dock window needed for the first Chicago wave.",
            approvals=[BlueprintApprovalAsset(stage="site_ops", status="APPROVED")],
        ),
    ]
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
            "mail",
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
                slack_channels=slack_channels,
                mail_threads=mail_threads,
            ),
            doc_graph=BlueprintDocGraphAsset(documents=documents),
            work_graph=BlueprintWorkGraphAsset(
                tickets=tickets,
                service_requests=service_requests,
            ),
            revenue_graph=BlueprintRevenueGraphAsset(
                companies=[
                    BlueprintCrmCompanyAsset(
                        id="CRM-ATS-C1",
                        name="Zenith Biologics",
                        domain="zenithbio.example.com",
                    ),
                    BlueprintCrmCompanyAsset(
                        id="CRM-ATS-C2",
                        name="Atlas Medline",
                        domain="atlasmedline.example.com",
                    ),
                ],
                contacts=[
                    BlueprintCrmContactAsset(
                        id="CRM-ATS-P1",
                        email="darcy@zenithbio.example.com",
                        first_name="Darcy",
                        last_name="Ng",
                        company_id="CRM-ATS-C1",
                    ),
                    BlueprintCrmContactAsset(
                        id="CRM-ATS-P2",
                        email="nora@atlasmedline.example.com",
                        first_name="Nora",
                        last_name="Bishop",
                        company_id="CRM-ATS-C2",
                    ),
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
                    ),
                    BlueprintCrmDealAsset(
                        id="CRM-ATS-D2",
                        name="Atlas Medline Overflow Program",
                        amount=145000,
                        stage="ops_review",
                        owner="priya.revops@example.com",
                        company_id="CRM-ATS-C2",
                        contact_id="CRM-ATS-P2",
                    ),
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
                    BlueprintSiteAsset(
                        site_id="SITE-IND-1",
                        name="Indianapolis East",
                        city="Indianapolis",
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
                    BlueprintCapacityPoolAsset(
                        pool_id="POOL-CHI-C",
                        site_id="SITE-CHI-1",
                        name="Cold Chain C",
                        total_units=60,
                        reserved_units=54,
                    ),
                    BlueprintCapacityPoolAsset(
                        pool_id="POOL-IND-A",
                        site_id="SITE-IND-1",
                        name="Ambient A",
                        total_units=220,
                        reserved_units=140,
                    ),
                ],
                storage_units=[
                    BlueprintStorageUnitAsset(
                        unit_id="UNIT-CHI-A1", pool_id="POOL-CHI-A", label="A1"
                    ),
                    BlueprintStorageUnitAsset(
                        unit_id="UNIT-MKE-B1", pool_id="POOL-MKE-B", label="B1"
                    ),
                    BlueprintStorageUnitAsset(
                        unit_id="UNIT-CHI-C7",
                        pool_id="POOL-CHI-C",
                        label="C7",
                        status="reserved",
                    ),
                    BlueprintStorageUnitAsset(
                        unit_id="UNIT-CHI-C8",
                        pool_id="POOL-CHI-C",
                        label="C8",
                        status="reserved",
                    ),
                    BlueprintStorageUnitAsset(
                        unit_id="UNIT-IND-A4",
                        pool_id="POOL-IND-A",
                        label="A4",
                        status="available",
                    ),
                    BlueprintStorageUnitAsset(
                        unit_id="UNIT-IND-A5",
                        pool_id="POOL-IND-A",
                        label="A5",
                        status="available",
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
                    ),
                    BlueprintQuoteAsset(
                        quote_id="Q-ATS-901",
                        customer_name="Atlas Medline",
                        requested_units=40,
                        status="review",
                        site_id="SITE-IND-1",
                        committed_units=20,
                    ),
                ],
                orders=[
                    BlueprintOrderAsset(
                        order_id="ORD-ATS-900",
                        quote_id="Q-ATS-900",
                        status="pending_vendor",
                        site_id="SITE-CHI-1",
                    ),
                    BlueprintOrderAsset(
                        order_id="ORD-ATS-901",
                        quote_id="Q-ATS-901",
                        status="scheduled",
                        site_id="SITE-IND-1",
                    ),
                ],
                allocations=[
                    BlueprintAllocationAsset(
                        allocation_id="ALLOC-ATS-1",
                        quote_id="Q-ATS-901",
                        pool_id="POOL-IND-A",
                        units=20,
                        status="reserved",
                    )
                ],
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
                    BlueprintVendorAsset(
                        vendor_id="VEND-ATS-REEFER",
                        name="Polar Chain",
                        specialty="cold_chain",
                    ),
                    BlueprintVendorAsset(
                        vendor_id="VEND-ATS-LABOR",
                        name="Lakefront Labor",
                        specialty="warehouse_staffing",
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
        key_surfaces=["property_graph", "docs", "slack", "mail", "jira", "servicedesk"],
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
        key_surfaces=["campaign_graph", "docs", "slack", "mail", "jira", "crm"],
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
        key_surfaces=["inventory_graph", "docs", "slack", "mail", "jira", "crm"],
        proves=["capacity feasibility", "quote accuracy", "ops follow-through"],
        what_if_branches=[
            "Reserve fragmented capacity and keep the customer timeline",
            "Overcommit the quote and create a downstream fulfillment failure",
        ],
    ),
}


def _saas_asset() -> BlueprintAsset:
    slack_channels = [
        _channel(
            "#apex-renewal",
            unread=4,
            messages=[
                _slack_message(
                    "1710200000.000100",
                    "priya.cs",
                    "Apex renewal is in 6 weeks and Jordan still hasn't agreed to an exec sponsor meeting.",
                ),
                _slack_message(
                    "1710200060.000200",
                    "derek.sales",
                    "Their new VP of Data is evaluating DataVault. We need the integration fix live before she makes a recommendation.",
                ),
                _slack_message(
                    "1710200120.000300",
                    "lin.product",
                    "The v4.2 breaking change hit Apex's ETL pipeline. Engineering has a patch but it needs QA and a customer-facing release note.",
                    thread_ts="1710200060.000200",
                ),
                _slack_message(
                    "1710200180.000400",
                    "priya.cs",
                    "Taylor Chen was our champion and he left last month. Jordan Blake is the new decision-maker and she hasn't seen a single success metric from us.",
                    thread_ts="1710200000.000100",
                ),
            ],
        ),
        _channel(
            "#product-incidents",
            unread=3,
            messages=[
                _slack_message(
                    "1710200240.000500",
                    "lin.product",
                    "v4.2 broke backward compat on the batch export endpoint. Three enterprise customers affected, Apex is the loudest.",
                ),
                _slack_message(
                    "1710200300.000600",
                    "omar.eng",
                    "Patch is ready in staging. Need product sign-off before we cut a hotfix release.",
                ),
                _slack_message(
                    "1710200360.000700",
                    "lin.product",
                    "Sign-off given. Release note needs to go out same day as the fix. Priya, can CS draft the customer email?",
                    thread_ts="1710200240.000500",
                ),
            ],
        ),
        _channel(
            "#customer-success",
            unread=2,
            messages=[
                _slack_message(
                    "1710200420.000800",
                    "priya.cs",
                    "Apex health score dropped from 82 to 54 in the last 30 days. Integration failure plus champion departure.",
                ),
                _slack_message(
                    "1710200480.000900",
                    "nadia.csm",
                    "Their support ticket has been open for 9 days with no resolution. That alone is enough to lose the renewal.",
                ),
            ],
        ),
        _channel(
            "#exec-review",
            unread=1,
            messages=[
                _slack_message(
                    "1710200540.001000",
                    "sam.cro",
                    "Apex is $480K ARR. If we lose this renewal it wipes out the Q3 expansion target. Get me a save plan by EOD.",
                ),
            ],
        ),
        _channel(
            "#support-escalation",
            unread=2,
            messages=[
                _slack_message(
                    "1710200600.001100",
                    "kai.support",
                    "Apex P1 is still bouncing between eng and CS. Someone needs to own it.",
                ),
                _slack_message(
                    "1710200660.001200",
                    "nadia.csm",
                    "I'm taking point. Engineering confirms the fix is in staging. I need a timeline I can send to Jordan.",
                    thread_ts="1710200600.001100",
                ),
            ],
        ),
    ]
    mail_threads = [
        _mail_thread(
            "MAIL-PIN-001",
            title="Integration broken since v4.2 update",
            category="customer_escalation",
            messages=[
                _mail_message(
                    "jordan.blake@apexfinancial.example.com",
                    "support@pinnacle.example.com",
                    "Integration broken since v4.2 update",
                    "Our ETL pipeline has been failing since your v4.2 release last week. "
                    "This is blocking our quarterly reporting cycle. We need a fix or a rollback path immediately.",
                ),
                _mail_message(
                    "support@pinnacle.example.com",
                    "jordan.blake@apexfinancial.example.com",
                    "Re: Integration broken since v4.2 update",
                    "We've identified the issue and a patch is in staging. "
                    "I'll follow up with a confirmed timeline within 24 hours.",
                    unread=False,
                ),
            ],
        ),
        _mail_thread(
            "MAIL-PIN-002",
            title="DataVault migration assessment",
            category="competitive_intel",
            messages=[
                _mail_message(
                    "derek.sales@pinnacle.example.com",
                    "priya.cs@pinnacle.example.com",
                    "FW: DataVault migration assessment",
                    "Forwarding this. A DataVault rep sent Jordan a migration ROI calculator last week. "
                    "She mentioned it in passing during the QBR reschedule call. We need to get ahead of this.",
                ),
            ],
        ),
        _mail_thread(
            "MAIL-PIN-003",
            title="Renewal proposal and success review",
            category="renewal",
            messages=[
                _mail_message(
                    "derek.sales@pinnacle.example.com",
                    "jordan.blake@apexfinancial.example.com",
                    "Renewal proposal and success review",
                    "Jordan, I'd like to schedule time to walk through the renewal terms "
                    "and share a success metrics review. We have some product updates "
                    "that directly address the reporting gaps your team flagged.",
                ),
            ],
        ),
        _mail_thread(
            "MAIL-PIN-004",
            title="Executive sponsorship for Apex partnership",
            category="exec_engagement",
            messages=[
                _mail_message(
                    "sam.cro@pinnacle.example.com",
                    "jordan.blake@apexfinancial.example.com",
                    "Executive sponsorship for Apex partnership",
                    "Jordan, I wanted to introduce myself as your executive sponsor at Pinnacle. "
                    "I understand the recent integration issue caused real disruption and I want to "
                    "make sure we're fully aligned on getting you back to full confidence.",
                ),
            ],
        ),
    ]
    documents = [
        BlueprintDocumentAsset(
            doc_id="DOC-PIN-SOW",
            title="Apex Financial Services - Statement of Work",
            body=(
                "Master SOW for Pinnacle Analytics platform.\n\n"
                "Contract value: $480,000 ARR. Renewal date: 6 weeks.\n"
                "Modules: Core Analytics, Batch Export API, Dashboard Suite.\n"
                "SLA: 99.9% uptime, P1 response within 4 hours."
            ),
            tags=["contract", "renewal"],
        ),
        BlueprintDocumentAsset(
            doc_id="DOC-PIN-PROPOSAL",
            title="Apex Renewal Proposal (Draft)",
            body=(
                "Renewal proposal draft.\n\n"
                "Proposed terms: 2-year renewal at $480K ARR with expansion to Data Warehouse module.\n"
                "Status: NOT SENT. Blocked on integration fix and champion replacement.\n"
                "Risk: Customer is evaluating DataVault. Need success metrics and exec engagement first."
            ),
            tags=["renewal", "sales"],
        ),
        BlueprintDocumentAsset(
            doc_id="DOC-PIN-POSTMORTEM",
            title="v4.2 Batch Export API - Incident Postmortem",
            body=(
                "Incident postmortem.\n\n"
                "Root cause: Backward-incompatible schema change in batch export endpoint.\n"
                "Impact: 3 enterprise customers, Apex Financial most affected (ETL pipeline failure).\n"
                "Fix: Patch in staging, pending QA. Customer-facing release note required.\n"
                "Prevention: Add API contract tests to CI pipeline."
            ),
            tags=["engineering", "incident"],
        ),
        BlueprintDocumentAsset(
            doc_id="DOC-PIN-COMPETITIVE",
            title="Competitive Analysis: DataVault vs Pinnacle",
            body=(
                "Competitive brief.\n\n"
                "DataVault strengths: Lower price point, native warehouse connectors.\n"
                "Pinnacle strengths: Deeper analytics, better dashboarding, enterprise SLA.\n"
                "Key risk: DataVault is offering free migration assessment to Apex.\n"
                "Counter: Demonstrate ROI with Apex's own data; highlight switching cost."
            ),
            tags=["competitive", "sales"],
        ),
        BlueprintDocumentAsset(
            doc_id="DOC-PIN-HEALTHPLAN",
            title="Apex Customer Success Plan",
            body=(
                "Success plan (OUTDATED).\n\n"
                "Champion: Taylor Chen (DEPARTED).\n"
                "Health score: 54 (was 82). Key risk factors: integration failure, champion loss.\n"
                "Next QBR: Rescheduled, no confirmed date.\n"
                "Action items: Fix integration, map new stakeholders, schedule exec meeting."
            ),
            tags=["customer_success", "account"],
        ),
    ]
    tickets = [
        BlueprintTicketAsset(
            ticket_id="JRA-PIN-101",
            title="P1: Batch export API breaking change regression",
            status="in_progress",
            priority="critical",
            assignee="omar.eng",
            description="v4.2 broke backward compat on batch export. Patch in staging.",
            tags=["engineering", "p1", "apex"],
        ),
        BlueprintTicketAsset(
            ticket_id="JRA-PIN-102",
            title="Apex integration restore and customer notification",
            status="open",
            priority="high",
            assignee="priya.cs",
            description="Deploy fix, send release note, confirm pipeline recovery with Apex.",
            tags=["support", "apex"],
        ),
        BlueprintTicketAsset(
            ticket_id="JRA-PIN-103",
            title="Renewal readiness review for Apex Financial",
            status="open",
            priority="high",
            assignee="derek.sales",
            description="Prepare renewal proposal, competitive defense, and exec sponsor plan.",
            tags=["sales", "renewal"],
        ),
        BlueprintTicketAsset(
            ticket_id="JRA-PIN-104",
            title="Map new Apex stakeholders and schedule exec meeting",
            status="open",
            priority="medium",
            assignee="priya.cs",
            description="Taylor Chen departed. Map Jordan Blake's priorities and schedule CRO intro.",
            tags=["customer_success", "stakeholder"],
        ),
        BlueprintTicketAsset(
            ticket_id="JRA-PIN-105",
            title="DataVault competitive displacement defense",
            status="open",
            priority="high",
            assignee="derek.sales",
            description="Apex is evaluating DataVault. Prepare ROI comparison and switching cost analysis.",
            tags=["sales", "competitive"],
        ),
    ]
    service_requests = [
        BlueprintServiceRequestAsset(
            request_id="SR-PIN-201",
            title="Apex P1 support escalation",
            status="escalated",
            priority="critical",
            requester="jordan.blake@apexfinancial.example.com",
            description="ETL pipeline broken for 9 days. Customer is losing patience.",
            tags=["support", "escalation"],
            approvals=[BlueprintApprovalAsset(stage="engineering", status="PENDING")],
        ),
        BlueprintServiceRequestAsset(
            request_id="SR-PIN-202",
            title="Apex QBR reschedule request",
            status="pending",
            priority="medium",
            requester="jordan.blake@apexfinancial.example.com",
            description="New VP of Data wants to reschedule QBR to include product roadmap review.",
            tags=["customer_success", "qbr"],
            approvals=[
                BlueprintApprovalAsset(stage="customer_success", status="PENDING")
            ],
        ),
        BlueprintServiceRequestAsset(
            request_id="SR-PIN-203",
            title="Renewal discount authorization",
            status="pending_approval",
            priority="high",
            requester="derek.sales@pinnacle.example.com",
            description="Authorize up to 15% discount on Apex renewal to counter competitive pressure.",
            tags=["sales", "renewal"],
            approvals=[BlueprintApprovalAsset(stage="finance", status="PENDING")],
        ),
        BlueprintServiceRequestAsset(
            request_id="SR-PIN-204",
            title="Hotfix release approval for v4.2.1",
            status="approved",
            priority="high",
            requester="lin.product@pinnacle.example.com",
            description="Sign off on the batch export API patch before customer-facing release.",
            tags=["engineering", "release"],
            approvals=[BlueprintApprovalAsset(stage="product", status="APPROVED")],
        ),
    ]
    return BlueprintAsset(
        name="b2b_saas.blueprint",
        title="Pinnacle Analytics",
        description=(
            "Enterprise SaaS renewal at risk with integration failure, "
            "champion departure, and competitive displacement pressure."
        ),
        scenario_name="enterprise_renewal_risk",
        family_name="b2b_saas",
        workflow_name="b2b_saas",
        workflow_variant="enterprise_renewal_risk",
        requested_facades=[
            "slack",
            "mail",
            "docs",
            "jira",
            "servicedesk",
            "crm",
        ],
        capability_graphs=BlueprintCapabilityGraphsAsset(
            organization_name="Pinnacle Analytics",
            organization_domain="pinnacle.example.com",
            timezone="America/New_York",
            scenario_brief=(
                "A $480K enterprise renewal is at risk. The product champion left, "
                "a breaking API change hit the customer's pipeline, and the new "
                "decision-maker is evaluating a competitor."
            ),
            comm_graph=BlueprintCommGraphAsset(
                slack_initial_message="Apex renewal war room is live.",
                slack_channels=slack_channels,
                mail_threads=mail_threads,
            ),
            doc_graph=BlueprintDocGraphAsset(documents=documents),
            work_graph=BlueprintWorkGraphAsset(
                tickets=tickets,
                service_requests=service_requests,
            ),
            revenue_graph=BlueprintRevenueGraphAsset(
                companies=[
                    BlueprintCrmCompanyAsset(
                        id="CRM-APEX",
                        name="Apex Financial Services",
                        domain="apexfinancial.example.com",
                    ),
                    BlueprintCrmCompanyAsset(
                        id="CRM-MERIDIAN",
                        name="Meridian Health Group",
                        domain="meridianhg.example.com",
                    ),
                ],
                contacts=[
                    BlueprintCrmContactAsset(
                        id="CON-JORDAN",
                        email="jordan.blake@apexfinancial.example.com",
                        first_name="Jordan",
                        last_name="Blake",
                        company_id="CRM-APEX",
                    ),
                    BlueprintCrmContactAsset(
                        id="CON-TAYLOR",
                        email="taylor.chen@apexfinancial.example.com",
                        first_name="Taylor",
                        last_name="Chen",
                        do_not_contact=True,
                        company_id="CRM-APEX",
                    ),
                ],
                deals=[
                    BlueprintCrmDealAsset(
                        id="DEAL-APEX-RENEWAL",
                        name="Apex Financial Renewal FY26",
                        amount=480000.0,
                        stage="at_risk",
                        owner="derek.sales",
                        contact_id="CON-JORDAN",
                        company_id="CRM-APEX",
                    ),
                    BlueprintCrmDealAsset(
                        id="DEAL-MERIDIAN-EXPAND",
                        name="Meridian Health Expansion",
                        amount=120000.0,
                        stage="negotiation",
                        owner="derek.sales",
                        contact_id=None,
                        company_id="CRM-MERIDIAN",
                    ),
                ],
            ),
            metadata={
                "vertical": "b2b_saas",
                "what_if_branches": [
                    "Fix the integration and rebuild trust before the renewal conversation",
                    "Lead with the discount and hope the product issues don't kill the deal",
                ],
            },
        ),
        metadata={"vertical": "b2b_saas"},
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
        key_surfaces=["property_graph", "docs", "slack", "mail", "jira", "servicedesk"],
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
        key_surfaces=["campaign_graph", "docs", "slack", "mail", "jira", "crm"],
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
        key_surfaces=["inventory_graph", "docs", "slack", "mail", "jira", "crm"],
        proves=["capacity feasibility", "quote accuracy", "ops follow-through"],
        what_if_branches=[
            "Reserve fragmented capacity and keep the customer timeline",
            "Overcommit the quote and create a downstream fulfillment failure",
        ],
    ),
    "b2b_saas": VerticalPackManifest(
        name="b2b_saas",
        title="B2B SaaS",
        description="Enterprise renewal at risk with integration failure, champion departure, and competitive pressure.",
        company_name="Pinnacle Analytics",
        company_briefing=(
            "Pinnacle Analytics is a B2B analytics SaaS company. Their largest customer, "
            "Apex Financial ($480K ARR), is 6 weeks from renewal with a broken integration, "
            "a departed champion, and a competitor circling."
        ),
        failure_impact=(
            "If this scenario fails, Pinnacle loses a $480K renewal, the Q3 expansion target collapses, "
            "and the competitor gains a reference customer in financial services."
        ),
        objective_focus=(
            "Save the renewal: fix the integration, rebuild stakeholder trust, neutralize the competitive threat, "
            "and get the proposal in front of the new decision-maker before she signs with DataVault."
        ),
        scenario_name="enterprise_renewal_risk",
        workflow_name="b2b_saas",
        workflow_variant="enterprise_renewal_risk",
        key_surfaces=["revenue_graph", "docs", "slack", "mail", "jira", "servicedesk"],
        proves=[
            "cross-functional renewal coordination",
            "competitive displacement defense",
            "product-to-CS handoff under pressure",
        ],
        what_if_branches=[
            "Fix the integration first and rebuild trust before the renewal conversation",
            "Lead with the discount and hope the product issues don't kill the deal",
        ],
    ),
}


_VERTICAL_BUILDERS = {
    "real_estate_management": _real_estate_asset,
    "digital_marketing_agency": _marketing_asset,
    "storage_solutions": _storage_asset,
    "b2b_saas": _saas_asset,
}
