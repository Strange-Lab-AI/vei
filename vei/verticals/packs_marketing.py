from __future__ import annotations

from vei.blueprint.models import (
    BlueprintApprovalAsset,
    BlueprintAsset,
    BlueprintCampaignApprovalAsset,
    BlueprintCampaignAsset,
    BlueprintCampaignGraphAsset,
    BlueprintCampaignReportAsset,
    BlueprintCapabilityGraphsAsset,
    BlueprintClientAsset,
    BlueprintCommGraphAsset,
    BlueprintCreativeAsset,
    BlueprintCrmCompanyAsset,
    BlueprintCrmContactAsset,
    BlueprintCrmDealAsset,
    BlueprintDocGraphAsset,
    BlueprintDocumentAsset,
    BlueprintKnowledgeAsset,
    BlueprintKnowledgeEdgeAsset,
    BlueprintKnowledgeGraphAsset,
    BlueprintKnowledgeProvenanceAsset,
    BlueprintRevenueGraphAsset,
    BlueprintServiceRequestAsset,
    BlueprintTicketAsset,
    BlueprintWorkGraphAsset,
)

from .packs_helpers import _channel, _mail_message, _mail_thread, _slack_message


def build() -> BlueprintAsset:
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
    knowledge_assets = [
        BlueprintKnowledgeAsset(
            asset_id="KA-NSG-001",
            kind="transcript",
            title="Apex launch kickoff transcript",
            body=(
                "Melissa confirmed the client wants a safer launch path if pacing stays above plan. "
                "The proposal should show a phased launch, weekly reporting, and a clear approval gate."
            ),
            summary="Client wants a phased launch with explicit approval gates and weekly reporting.",
            tags=["proposal", "client", "launch"],
            provenance=BlueprintKnowledgeProvenanceAsset(
                source="granola",
                source_id="granola-apex-kickoff",
                captured_at="2026-04-10T15:00:00Z",
                shelf_life_ms=30 * 86_400_000,
                metadata={"captured_at_ms": 1775833200000},
            ),
            linked_object_refs=["crm_deal:CRM-NSG-D1", "campaign:CMP-APEX-01"],
            metadata={"captured_at_ms": 1775833200000},
        ),
        BlueprintKnowledgeAsset(
            asset_id="KA-NSG-002",
            kind="deliverable",
            title="Apex Q2 strategy outline",
            body=(
                "Deliverables: phased paid social relaunch, refreshed launch packet, and weekly executive readout. "
                "The account team should package the safer launch path as a proposal update."
            ),
            summary="Defines the phased relaunch deliverables and weekly executive readout for Apex.",
            tags=["proposal", "deliverable", "strategy"],
            provenance=BlueprintKnowledgeProvenanceAsset(
                source="notion",
                source_id="notion-apex-q2-strategy",
                captured_at="2026-04-08T16:30:00Z",
                shelf_life_ms=21 * 86_400_000,
                metadata={"captured_at_ms": 1775665800000},
            ),
            linked_object_refs=["crm_deal:CRM-NSG-D1", "client:CLIENT-APEX"],
            metadata={"captured_at_ms": 1775665800000},
        ),
        BlueprintKnowledgeAsset(
            asset_id="KA-NSG-003",
            kind="pricing",
            title="Apex retainer pricing guide",
            body=(
                "Base retainer remains $180000 for the quarter. "
                "A phased relaunch add-on stays inside the approved envelope when media pacing is reset."
            ),
            summary="Quarterly retainer is $180000 with a phased relaunch add-on inside the approved envelope.",
            tags=["proposal", "pricing", "finance"],
            provenance=BlueprintKnowledgeProvenanceAsset(
                source="notion",
                source_id="notion-apex-pricing",
                captured_at="2026-04-09T18:00:00Z",
                shelf_life_ms=30 * 86_400_000,
                metadata={"captured_at_ms": 1775757600000},
            ),
            linked_object_refs=["crm_deal:CRM-NSG-D1", "client:CLIENT-APEX"],
            metrics={"retainer_amount": 180000},
            metadata={"captured_at_ms": 1775757600000},
        ),
        BlueprintKnowledgeAsset(
            asset_id="KA-NSG-004",
            kind="metric_snapshot",
            title="Apex delivery health snapshot",
            body=(
                "Latest Linear cycle shows 11 of 12 launch tasks complete and the reporting repair on track for same-day delivery."
            ),
            summary="11 of 12 launch tasks are complete and the reporting repair remains on track.",
            tags=["proposal", "metrics", "timeline"],
            provenance=BlueprintKnowledgeProvenanceAsset(
                source="linear",
                source_id="linear-apex-launch-cycle",
                captured_at="2026-04-11T12:00:00Z",
                shelf_life_ms=14 * 86_400_000,
                metadata={"captured_at_ms": 1775908800000},
            ),
            linked_object_refs=["crm_deal:CRM-NSG-D1", "campaign:CMP-APEX-01"],
            metrics={"scope_completed": 11, "scope_planned": 12},
            metadata={"captured_at_ms": 1775908800000},
        ),
        BlueprintKnowledgeAsset(
            asset_id="KA-NSG-005",
            kind="sop",
            title="Northstar proposal guardrails",
            body=(
                "Every external proposal must cite its pricing source, current delivery snapshot, and the last client conversation before signoff."
            ),
            summary="Proposals must cite pricing, delivery status, and the last client conversation.",
            tags=["proposal", "sop", "review"],
            provenance=BlueprintKnowledgeProvenanceAsset(
                source="notion",
                source_id="notion-proposal-guardrails",
                captured_at="2026-04-01T14:00:00Z",
                shelf_life_ms=90 * 86_400_000,
                metadata={"captured_at_ms": 1775052000000},
            ),
            linked_object_refs=["crm_deal:CRM-NSG-D1"],
            metadata={"captured_at_ms": 1775052000000},
        ),
    ]
    knowledge_edges = [
        BlueprintKnowledgeEdgeAsset(
            edge_id="knowledge-edge-0001",
            kind="applies_to",
            from_asset_id="KA-NSG-001",
            to_ref="crm_deal:CRM-NSG-D1",
        ),
        BlueprintKnowledgeEdgeAsset(
            edge_id="knowledge-edge-0002",
            kind="applies_to",
            from_asset_id="KA-NSG-002",
            to_ref="crm_deal:CRM-NSG-D1",
        ),
        BlueprintKnowledgeEdgeAsset(
            edge_id="knowledge-edge-0003",
            kind="applies_to",
            from_asset_id="KA-NSG-003",
            to_ref="crm_deal:CRM-NSG-D1",
        ),
        BlueprintKnowledgeEdgeAsset(
            edge_id="knowledge-edge-0004",
            kind="applies_to",
            from_asset_id="KA-NSG-004",
            to_ref="crm_deal:CRM-NSG-D1",
        ),
        BlueprintKnowledgeEdgeAsset(
            edge_id="knowledge-edge-0005",
            kind="applies_to",
            from_asset_id="KA-NSG-005",
            to_ref="crm_deal:CRM-NSG-D1",
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
            knowledge_graph=BlueprintKnowledgeGraphAsset(
                assets=knowledge_assets,
                edges=knowledge_edges,
                metadata={
                    "primary_subject": "crm_deal:CRM-NSG-D1",
                    "reference_now_ms": 1775908800000,
                },
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
