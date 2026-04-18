from __future__ import annotations

from vei.blueprint.api import (
    BlueprintApprovalAsset,
    BlueprintAsset,
    BlueprintCapabilityGraphsAsset,
    BlueprintCommGraphAsset,
    BlueprintCrmCompanyAsset,
    BlueprintCrmContactAsset,
    BlueprintCrmDealAsset,
    BlueprintDocGraphAsset,
    BlueprintDocumentAsset,
    BlueprintRevenueGraphAsset,
    BlueprintServiceRequestAsset,
    BlueprintTicketAsset,
    BlueprintWorkGraphAsset,
)

from .packs_helpers import _channel, _mail_message, _mail_thread, _slack_message


def build() -> BlueprintAsset:
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
