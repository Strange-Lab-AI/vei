from __future__ import annotations

from vei.blueprint.api import (
    BlueprintApprovalAsset,
    BlueprintAsset,
    BlueprintBuildingAsset,
    BlueprintCapabilityGraphsAsset,
    BlueprintCommGraphAsset,
    BlueprintDocGraphAsset,
    BlueprintDocumentAsset,
    BlueprintLeaseAsset,
    BlueprintPropertyAsset,
    BlueprintPropertyGraphAsset,
    BlueprintServiceRequestAsset,
    BlueprintTenantAsset,
    BlueprintTicketAsset,
    BlueprintUnitAsset,
    BlueprintVendorAsset,
    BlueprintWorkGraphAsset,
    BlueprintWorkOrderAsset,
)

from .packs_helpers import _channel, _mail_message, _mail_thread, _slack_message


def build() -> BlueprintAsset:
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
