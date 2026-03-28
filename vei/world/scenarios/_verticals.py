from __future__ import annotations


from vei.world.scenario import (
    Scenario,
    Document,
    Ticket,
    SpreadsheetSheet,
    SpreadsheetWorkbook,
    IdentityApplicationSeed,
    IdentityGroupSeed,
    IdentityUserSeed,
)


def scenario_acquired_sales_onboarding() -> Scenario:
    return Scenario(
        slack_initial_message=(
            "By 9 AM virtual time tomorrow, onboard 147 acquired sales users while "
            "fixing identity conflicts, preserving least privilege, and preventing oversharing."
        ),
        slack_channels={
            "#sales-cutover": {
                "messages": [
                    {
                        "ts": "1",
                        "user": "it-integration",
                        "text": (
                            "Wave 1 acquired-sales cutover starts now. Resolve identity conflicts, "
                            "remove oversharing, and hand off ownership before tomorrow morning."
                        ),
                        "thread_ts": None,
                    }
                ],
                "unread": 0,
            }
        },
        documents={
            "POL-ACCESS-9": Document(
                doc_id="POL-ACCESS-9",
                title="Acquisition Access Policy",
                body=(
                    "Grant least privilege first. Sales users receive CRM + Slack. "
                    "No external Drive sharing until manager review is complete."
                ),
                tags=["policy", "identity", "acquisition"],
            ),
            "CUTOVER-2201": Document(
                doc_id="CUTOVER-2201",
                title="Wave 1 Sales Cutover Checklist",
                body=(
                    "Cutover owner: it-integration.\n\n"
                    "Required before handoff:\n"
                    "- resolve identity conflicts\n"
                    "- remove external document sharing\n"
                    "- transfer open opportunities\n"
                    "- notify manager once least-privilege access is confirmed"
                ),
                tags=["cutover", "sales", "acquisition"],
            ),
        },
        tickets={
            "JRA-204": Ticket(
                ticket_id="JRA-204",
                title="Acquisition Wave 1 onboarding tracker",
                status="open",
                assignee="it-integration",
                description="Resolve identity conflicts and confirm document ownership migration.",
                history=[{"status": "open"}],
            )
        },
        identity_users={
            "USR-ACQ-1": IdentityUserSeed(
                user_id="USR-ACQ-1",
                email="jordan.sellers@oldco.example.com",
                login="jordan.sellers",
                first_name="Jordan",
                last_name="Sellers",
                title="Account Executive",
                department="Sales",
                status="PROVISIONED",
                groups=["GRP-acquired-sales"],
                applications=["APP-slack"],
            ),
            "USR-ACQ-2": IdentityUserSeed(
                user_id="USR-ACQ-2",
                email="maya.rex@example.com",
                login="maya.rex",
                first_name="Maya",
                last_name="Rex",
                title="Sales Manager",
                department="Sales",
                status="ACTIVE",
                groups=["GRP-sales-managers"],
                applications=["APP-slack", "APP-crm"],
            ),
        },
        identity_groups={
            "GRP-acquired-sales": IdentityGroupSeed(
                group_id="GRP-acquired-sales",
                name="Acquired Sales",
                members=["USR-ACQ-1"],
            ),
            "GRP-sales-managers": IdentityGroupSeed(
                group_id="GRP-sales-managers",
                name="Sales Managers",
                members=["USR-ACQ-2"],
            ),
        },
        identity_applications={
            "APP-crm": IdentityApplicationSeed(
                app_id="APP-crm",
                label="Salesforce",
                assignments=["USR-ACQ-2"],
            ),
            "APP-slack": IdentityApplicationSeed(
                app_id="APP-slack",
                label="Slack",
                assignments=["USR-ACQ-1", "USR-ACQ-2"],
            ),
        },
        google_admin={
            "oauth_apps": {},
            "drive_shares": {
                "GDRIVE-2201": {
                    "doc_id": "GDRIVE-2201",
                    "title": "Enterprise Accounts Playbook",
                    "owner": "departed.manager@oldco.example.com",
                    "visibility": "external_link",
                    "classification": "internal",
                    "shared_with": [
                        "channel-partner@example.net",
                        "maya.rex@example.com",
                    ],
                    "history": [],
                }
            },
        },
        hris={
            "employees": {
                "EMP-2201": {
                    "employee_id": "EMP-2201",
                    "email": "jordan.sellers@oldco.example.com",
                    "display_name": "Jordan Sellers",
                    "department": "Sales",
                    "manager": "maya.rex@example.com",
                    "status": "pre_start",
                    "cohort": "acquired-sales-wave-1",
                    "identity_conflict": True,
                    "onboarded": False,
                    "notes": [],
                },
                "EMP-2202": {
                    "employee_id": "EMP-2202",
                    "email": "erin.falcon@oldco.example.com",
                    "display_name": "Erin Falcon",
                    "department": "Sales",
                    "manager": "maya.rex@example.com",
                    "status": "pre_start",
                    "cohort": "acquired-sales-wave-1",
                    "identity_conflict": False,
                    "onboarded": False,
                    "notes": [],
                },
            }
        },
        crm={
            "companies": [
                {
                    "id": "CO-100",
                    "name": "Northwind Retail",
                    "domain": "northwind.example.com",
                    "created_ms": 1700000000000,
                }
            ],
            "contacts": [
                {
                    "id": "C-100",
                    "email": "buyer@northwind.example.com",
                    "first_name": "Nina",
                    "last_name": "Buyer",
                    "do_not_contact": False,
                    "company_id": "CO-100",
                    "created_ms": 1700000000000,
                }
            ],
            "deals": [
                {
                    "id": "D-100",
                    "name": "Northwind Expansion",
                    "amount": 240000,
                    "stage": "Negotiation",
                    "contact_id": "C-100",
                    "company_id": "CO-100",
                    "owner": "departed.manager@oldco.example.com",
                    "created_ms": 1700000000000,
                }
            ],
        },
        metadata={
            "benchmark_family": "enterprise_onboarding_migration",
            "scenario_type": "acceptance",
            "difficulty": "hard",
            "expected_steps": [10, 20],
            "allowed_application_ids": ["APP-slack", "APP-crm"],
            "tags": ["onboarding", "identity", "salesforce", "least-privilege"],
        },
    )


def scenario_checkout_spike_mitigation() -> Scenario:
    return Scenario(
        slack_initial_message=(
            "Checkout conversion is dropping. Mitigate the spike, keep customer comms accurate, "
            "and avoid data corruption while revenue risk is high."
        ),
        slack_channels={
            "#commerce-war-room": {
                "messages": [
                    {
                        "ts": "1",
                        "user": "commerce-oncall",
                        "text": (
                            "Checkout conversion is dropping fast. Quantify revenue impact, "
                            "shrink rollout safely, and keep support guidance accurate."
                        ),
                        "thread_ts": None,
                    }
                ],
                "unread": 0,
            }
        },
        tickets={
            "INC-812": Ticket(
                ticket_id="INC-812",
                title="Checkout failure spike",
                status="in_progress",
                assignee="commerce-oncall",
                description="Mitigate incident without corrupting order state.",
                history=[{"status": "open"}, {"status": "in_progress"}],
            )
        },
        documents={
            "RUN-CHK-1": Document(
                doc_id="RUN-CHK-1",
                title="Checkout Incident Comms And Checklist",
                body=(
                    "If rollback is needed, first disable risky rollout and verify no duplicate charge writes.\n\n"
                    "Customer-facing guidance must stay accurate while mitigation is active."
                ),
                tags=["incident", "commerce"],
            )
        },
        spreadsheets={
            "WB-CHK-1": SpreadsheetWorkbook(
                workbook_id="WB-CHK-1",
                title="Checkout Revenue Flight Deck",
                owner="commerce-ops@example.com",
                shared_with=["sales-ops@example.com", "support-lead@example.com"],
                permissions={
                    "commerce-ops@example.com": "owner",
                    "sales-ops@example.com": "editor",
                    "support-lead@example.com": "viewer",
                },
                sheets=[
                    SpreadsheetSheet(
                        sheet_id="sheet-impact",
                        title="Impact",
                        columns=["metric", "value", "notes"],
                        rows=[
                            {
                                "metric": "baseline_conversion_pct",
                                "value": 2.8,
                                "notes": "Pre-incident trailing 4h average",
                            }
                        ],
                        cells={"A1": "Metric", "B1": "Value", "C1": "Notes"},
                        formulas={},
                        tables=[
                            {
                                "table_id": "tbl-impact",
                                "name": "ImpactSummary",
                                "columns": ["metric", "value", "notes"],
                            }
                        ],
                        filters=[],
                        sorts=[],
                    )
                ],
            )
        },
        datadog={
            "services": {
                "svc-checkout": {
                    "service_id": "svc-checkout",
                    "name": "checkout-api",
                    "status": "degraded",
                    "error_rate_pct": 18.4,
                    "latency_p95_ms": 2240,
                    "revenue_tier": "critical",
                    "notes": [],
                }
            },
            "monitors": {
                "mon-5001": {
                    "monitor_id": "mon-5001",
                    "title": "Checkout 5xx spike",
                    "service_id": "svc-checkout",
                    "status": "alert",
                    "severity": "critical",
                    "threshold": "5xx > 3%",
                    "current_value": "18.4%",
                    "muted": False,
                    "history": [],
                }
            },
        },
        pagerduty={
            "incidents": {
                "PD-9001": {
                    "incident_id": "PD-9001",
                    "title": "Checkout latency and error spike",
                    "status": "triggered",
                    "urgency": "high",
                    "service_id": "svc-checkout",
                    "assignee": "oncall-commerce",
                    "notes": [],
                }
            }
        },
        feature_flags={
            "flags": {
                "checkout_v2": {
                    "flag_key": "checkout_v2",
                    "service": "checkout-api",
                    "env": "prod",
                    "enabled": True,
                    "rollout_pct": 100,
                    "history": [],
                },
                "checkout_kill_switch": {
                    "flag_key": "checkout_kill_switch",
                    "service": "checkout-api",
                    "env": "prod",
                    "enabled": False,
                    "rollout_pct": 0,
                    "history": [],
                },
            }
        },
        crm={
            "companies": [
                {
                    "id": "CO-812",
                    "name": "Evergreen Retail",
                    "domain": "evergreen.example.com",
                    "created_ms": 1700000000000,
                }
            ],
            "contacts": [
                {
                    "id": "C-812",
                    "email": "ops-buyer@evergreen.example.com",
                    "first_name": "Kira",
                    "last_name": "Tennant",
                    "do_not_contact": False,
                    "company_id": "CO-812",
                    "created_ms": 1700000000000,
                }
            ],
            "deals": [
                {
                    "id": "D-812",
                    "name": "Evergreen Checkout Expansion",
                    "amount": 420000,
                    "stage": "Negotiation",
                    "contact_id": "C-812",
                    "company_id": "CO-812",
                    "owner": "commerce-revops@example.com",
                    "created_ms": 1700000000000,
                    "updated_ms": 1700000000000,
                }
            ],
            "activities": [],
        },
        metadata={
            "benchmark_family": "revenue_incident_mitigation",
            "scenario_type": "acceptance",
            "difficulty": "hard",
            "expected_steps": [12, 24],
            "tags": [
                "incident",
                "checkout",
                "feature-flags",
                "reliability",
                "spreadsheet",
                "crm",
            ],
        },
    )


def scenario_tenant_opening_conflict() -> Scenario:
    return Scenario(
        slack_initial_message="Harbor Point opening readiness review starts now.",
        metadata={
            "benchmark_family": "real_estate_management",
            "scenario_type": "vertical_demo",
            "difficulty": "hard",
            "expected_steps": [8, 18],
            "tags": [
                "real-estate",
                "property",
                "lease",
                "vendor",
                "opening",
            ],
        },
    )


def scenario_campaign_launch_guardrail() -> Scenario:
    return Scenario(
        slack_initial_message="Northstar launch guardrail review is live.",
        metadata={
            "benchmark_family": "digital_marketing_agency",
            "scenario_type": "vertical_demo",
            "difficulty": "hard",
            "expected_steps": [8, 18],
            "tags": [
                "marketing",
                "campaign",
                "budget",
                "approval",
                "launch",
            ],
        },
    )


def scenario_capacity_quote_commitment() -> Scenario:
    return Scenario(
        slack_initial_message="Atlas strategic quote readiness review is live.",
        metadata={
            "benchmark_family": "storage_solutions",
            "scenario_type": "vertical_demo",
            "difficulty": "hard",
            "expected_steps": [8, 18],
            "tags": [
                "storage",
                "inventory",
                "capacity",
                "quote",
                "ops",
            ],
        },
    )


def _b2b_saas_scenario(name: str, slack_msg: str) -> Scenario:
    return Scenario(
        slack_initial_message=slack_msg,
        metadata={
            "benchmark_family": "b2b_saas",
            "scenario_type": "vertical_demo",
            "difficulty": "hard",
            "expected_steps": [8, 18],
            "tags": ["saas", "renewal", "crm", "support", "enterprise"],
        },
    )
