from __future__ import annotations


from vei.world.scenario import (
    Scenario,
    Document,
    Ticket,
    Participant,
    IdentityApplicationSeed,
    IdentityGroupSeed,
    IdentityUserSeed,
    ServiceDeskIncident,
    ServiceDeskRequest,
)


def scenario_macrocompute_default() -> Scenario:
    return Scenario(
        budget_cap_usd=3500,
        derail_prob=0.1,
        slack_initial_message="Reminder: citations required for any request over $2k.",
        vendor_reply_variants=[
            "Thanks — Price: $3199, ETA: 5-7 business days.",
            "> On Mon, we received your request\nPRICE: USD 3,199\nEta: within 5-7 business days\n--\nBest, MacroCompute",
            "quote attached (inline): total: $3,199.00, ETA: 5 business days. Regards, Sales",
            "PRICE - $3199; eta: approx. 1 week\n\n\nJohn Doe\nSales Representative\nMacroCompute",
        ],
        browser_nodes=None,
    )


def scenario_extended_store() -> Scenario:
    nodes = {
        "home": {
            "url": "https://vweb.local/home",
            "title": "MacroCompute — Home",
            "excerpt": "Welcome. Browse categories to find laptops and accessories.",
            "affordances": [
                {"tool": "browser.click", "args": {"node_id": "CLICK:open_category#0"}},
            ],
            "next": {"CLICK:open_category#0": "category"},
        },
        "category": {
            "url": "https://vweb.local/cat/laptops",
            "title": "Laptops — Category",
            "excerpt": "Showing 2 results",
            "affordances": [
                {"tool": "browser.click", "args": {"node_id": "CLICK:open_pdp1#0"}},
                {"tool": "browser.click", "args": {"node_id": "CLICK:open_pdp2#0"}},
                {"tool": "browser.back", "args": {}},
            ],
            "next": {
                "CLICK:open_pdp1#0": "pdp1",
                "CLICK:open_pdp2#0": "pdp2",
                "BACK": "home",
            },
        },
        "pdp1": {
            "url": "https://vweb.local/pdp/macrobook-pro-16",
            "title": "MacroBook Pro 16 — Product",
            "excerpt": "Powerful 16-inch laptop. Price $3199. See specifications.",
            "affordances": [
                {"tool": "browser.click", "args": {"node_id": "CLICK:open_specs1#0"}},
                {"tool": "browser.back", "args": {}},
            ],
            "next": {"CLICK:open_specs1#0": "specs1", "BACK": "category"},
        },
        "specs1": {
            "url": "https://vweb.local/pdp/macrobook-pro-16/specs",
            "title": "MacroBook Pro 16 — Specifications",
            "excerpt": "16-core CPU, 32GB RAM, 1TB SSD",
            "affordances": [{"tool": "browser.back", "args": {}}],
            "next": {"BACK": "pdp1"},
        },
        "pdp2": {
            "url": "https://vweb.local/pdp/macrobook-air-13",
            "title": "MacroBook Air 13 — Product",
            "excerpt": "Lightweight 13-inch laptop. Price $1299.",
            "affordances": [
                {"tool": "browser.back", "args": {}},
            ],
            "next": {"BACK": "category"},
        },
    }
    return Scenario(
        budget_cap_usd=3500,
        derail_prob=0.05,
        slack_initial_message="Reminder: include budget and citations.",
        vendor_reply_variants=None,
        browser_nodes=nodes,
    )


def scenario_multi_channel() -> Scenario:
    docs = {
        "policy": Document(
            doc_id="POLICY-1",
            title="Expense Policy",
            body="All purchases over $2000 require manager approval.",
            tags=["policy"],
        )
    }
    tickets = {
        "TCK-42": Ticket(
            ticket_id="TCK-42",
            title="Procurement Request",
            status="open",
            description="Acquire MacroBook Pro 16",
            history=[{"status": "open"}],
        )
    }
    database_tables = {
        "procurement_orders": [
            {
                "id": "PO-1001",
                "vendor": "MacroCompute",
                "amount_usd": 3199,
                "status": "PENDING_APPROVAL",
            }
        ],
        "approval_audit": [
            {
                "id": "APR-1001",
                "entity_type": "purchase_order",
                "entity_id": "PO-1001",
                "status": "PENDING",
                "approver": "finance@macrocompute.example",
            }
        ],
    }
    events = [
        {
            "dt_ms": 5000,
            "target": "mail",
            "payload": {
                "from": "vendor@example.com",
                "body_text": "Quote $3199, ETA 5 days",
                "subj": "Quote",
            },
        }
    ]
    identity_users = {
        "USR-1001": IdentityUserSeed(
            user_id="USR-1001",
            email="alice@macrocompute.example",
            login="alice",
            first_name="Alice",
            last_name="Nguyen",
            title="IT Support Lead",
            department="IT",
            status="ACTIVE",
            groups=["GRP-procurement", "GRP-it"],
            applications=["APP-erp", "APP-slack"],
            factors=["totp"],
        ),
        "USR-2001": IdentityUserSeed(
            user_id="USR-2001",
            email="brian@macrocompute.example",
            login="brian",
            first_name="Brian",
            last_name="Park",
            title="Finance Analyst",
            department="Finance",
            status="PROVISIONED",
            groups=["GRP-finance"],
            applications=["APP-erp"],
        ),
        "USR-3001": IdentityUserSeed(
            user_id="USR-3001",
            email="sara@macrocompute.example",
            login="sara",
            first_name="Sara",
            last_name="Kent",
            title="Vendor Ops",
            department="Operations",
            status="DEPROVISIONED",
            groups=["GRP-operations"],
            applications=["APP-erp"],
        ),
    }
    identity_groups = {
        "GRP-procurement": IdentityGroupSeed(
            group_id="GRP-procurement",
            name="Procurement Admins",
            description="Manage procurement approvals",
            members=["USR-1001"],
        ),
        "GRP-finance": IdentityGroupSeed(
            group_id="GRP-finance",
            name="Finance Analysts",
            description="Review spend and approvals",
            members=["USR-2001"],
        ),
        "GRP-it": IdentityGroupSeed(
            group_id="GRP-it",
            name="IT Support",
            description="Manage SSO and device controls",
            members=["USR-1001"],
        ),
        "GRP-operations": IdentityGroupSeed(
            group_id="GRP-operations",
            name="Operations",
            members=["USR-3001"],
        ),
    }
    identity_apps = {
        "APP-erp": IdentityApplicationSeed(
            app_id="APP-erp",
            label="Macro ERP",
            description="Finance and procurement ERP",
            sign_on_mode="SAML_2_0",
            assignments=["USR-1001", "USR-2001"],
        ),
        "APP-slack": IdentityApplicationSeed(
            app_id="APP-slack",
            label="Slack",
            description="Team messaging",
            sign_on_mode="OIDC",
            assignments=["USR-1001"],
        ),
    }
    service_incidents = {
        "INC-5001": ServiceDeskIncident(
            incident_id="INC-5001",
            title="Supplier portal MFA failures",
            status="IN_PROGRESS",
            priority="P2",
            assignee="maya.ops",
            description="Multiple procurement approvers cannot MFA into the supplier portal.",
            history=[
                {"status": "NEW"},
                {"status": "IN_PROGRESS", "assignee": "maya.ops"},
            ],
        )
    }
    service_requests = {
        "REQ-8801": ServiceDeskRequest(
            request_id="REQ-8801",
            title="Access: Procurement Admin",
            status="PENDING_APPROVAL",
            requester="amy@macrocompute.example",
            description="Need elevated rights to review MacroBook vendor contract.",
            approvals=[
                {"stage": "manager", "status": "APPROVED"},
                {"stage": "security", "status": "PENDING"},
            ],
            history=[{"status": "PENDING_APPROVAL"}],
        )
    }
    return Scenario(
        budget_cap_usd=3200,
        derail_prob=0.05,
        slack_initial_message="Please reference ticket TCK-42 and attach documentation.",
        vendor_reply_variants=["Quote: $3199, ETA 5 days"],
        documents=docs,
        tickets=tickets,
        database_tables=database_tables,
        derail_events=events,
        identity_users=identity_users,
        identity_groups=identity_groups,
        identity_applications=identity_apps,
        service_incidents=service_incidents,
        service_requests=service_requests,
    )


def scenario_multi_channel_compliance() -> Scenario:
    docs = {
        "policy": Document(
            doc_id="POLICY-1",
            title="Expense Policy",
            body="All purchases over $2000 require manager approval and attached vendor quote.",
            tags=["policy", "compliance"],
        ),
        "checklist": Document(
            doc_id="PROC-7",
            title="Procurement Checklist",
            body=(
                "1. Capture vendor quote in Docs\n"
                "2. Link quote to ticket TCK-42\n"
                "3. Log CRM note with approved amount\n"
                "4. Confirm delivery ticket is opened"
            ),
            tags=["checklist", "procurement"],
        ),
        "risk-register": Document(
            doc_id="RISK-9",
            title="Risk Register Excerpt",
            body="Record ETA and supplier commitments for audits. Missing ETA triggers compliance follow-up.",
            tags=["audit", "risk"],
        ),
    }
    tickets = {
        "TCK-42": Ticket(
            ticket_id="TCK-42",
            title="Procurement Request",
            status="open",
            description="Acquire MacroBook Pro 16 with accessories",
            history=[{"status": "open"}, {"status": "triaged"}],
        ),
        "TCK-88": Ticket(
            ticket_id="TCK-88",
            title="Delivery Coordination",
            status="pending",
            description="Arrange delivery once quote is approved",
            history=[{"status": "pending"}],
        ),
    }
    participants = [
        Participant(
            participant_id="mgr-amy",
            name="Amy Santiago",
            role="Procurement Manager",
            email="amy@macrocompute.example",
            slack="@amy",
        ),
        Participant(
            participant_id="auditor-li",
            name="Li Zhang",
            role="Compliance Auditor",
            email="li@macrocompute.example",
            slack="@li",
        ),
    ]
    events = [
        {
            "dt_ms": 5000,
            "target": "mail",
            "payload": {
                "from": "sales@macrocompute.example",
                "subj": "Formal Quote",
                "body_text": "Quote $3199, ETA 5 business days. Attach this to Docs and confirm delivery ticket.",
            },
        },
        {
            "dt_ms": 9000,
            "target": "slack",
            "payload": {
                "channel": "#procurement",
                "text": "@amy: Please ensure the quote doc is stored under PROC-7 and ticket TCK-88 is updated with ETA.",
            },
        },
        {
            "dt_ms": 12000,
            "target": "mail",
            "payload": {
                "from": "li@macrocompute.example",
                "subj": "Audit Reminder",
                "body_text": "Compliance check: log the CRM contact, include ETA in note, and link risk register entry RISK-9.",
            },
        },
    ]
    return Scenario(
        budget_cap_usd=3200,
        derail_prob=0.08,
        slack_initial_message=(
            "Remember: TCK-42 must reference the stored quote doc, and delivery tracking lives in TCK-88."
        ),
        vendor_reply_variants=[
            "Formal quote: Total $3199, ETA 5 business days.",
            "MacroCompute Sales — Quote: 3,199 USD. Delivery promise: 5 days. Attach to PROC-7.",
        ],
        documents=docs,
        tickets=tickets,
        participants=participants,
        derail_events=events,
        metadata={
            "requires_documents": ["PROC-7", "RISK-9"],
            "linked_tickets": ["TCK-42", "TCK-88"],
        },
    )
