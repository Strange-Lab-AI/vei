from __future__ import annotations

from vei.blueprint.api import (
    BlueprintAllocationAsset,
    BlueprintApprovalAsset,
    BlueprintAsset,
    BlueprintCapacityPoolAsset,
    BlueprintCapabilityGraphsAsset,
    BlueprintCommGraphAsset,
    BlueprintCrmCompanyAsset,
    BlueprintCrmContactAsset,
    BlueprintCrmDealAsset,
    BlueprintDocGraphAsset,
    BlueprintDocumentAsset,
    BlueprintInventoryGraphAsset,
    BlueprintOrderAsset,
    BlueprintQuoteAsset,
    BlueprintRevenueGraphAsset,
    BlueprintServiceRequestAsset,
    BlueprintSiteAsset,
    BlueprintStorageUnitAsset,
    BlueprintTicketAsset,
    BlueprintVendorAsset,
    BlueprintWorkGraphAsset,
)

from .packs_helpers import _channel, _mail_message, _mail_thread, _slack_message


def build() -> BlueprintAsset:
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
