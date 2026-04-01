from __future__ import annotations

import os
import random
from typing import Dict, List

from .models import (
    CorpusBundle,
    EnterpriseProfile,
    GeneratedEnvironment,
    GeneratedWorkflowSpec,
)

ORG_STEMS = [
    "MacroCompute",
    "Northwind",
    "Acme Dynamics",
    "Blue Harbor",
    "SummitWorks",
    "Atlas Forge",
    "QuantaBridge",
]

DEPARTMENTS = [
    "Finance",
    "Procurement",
    "Security",
    "Operations",
    "PeopleOps",
    "Legal",
]

VENDOR_NAMES = [
    "MacroCompute",
    "Dell Business",
    "HP Enterprise",
    "Lenovo Pro",
    "Acer Commercial",
]

WORKFLOW_FAMILIES = [
    "procurement_quote",
    "db_audit",
    "sales_pipeline",
    "calendar_review",
    "risk_escalation",
    "identity_access_review",
    "procure_to_pay",
]


def generate_corpus(
    *,
    seed: int = 42042,
    environment_count: int = 10,
    scenarios_per_environment: int = 10,
) -> CorpusBundle:
    rng = random.Random(seed)
    environments: List[GeneratedEnvironment] = []
    workflows: List[GeneratedWorkflowSpec] = []

    for env_idx in range(max(1, environment_count)):
        env_seed = rng.randint(1, 10_000_000)
        environment = _generate_environment(seed=env_seed, index=env_idx)
        environments.append(environment)
        for scenario_idx in range(max(1, scenarios_per_environment)):
            workflow_seed = rng.randint(1, 10_000_000)
            workflow = _generate_workflow_spec(
                environment=environment,
                seed=workflow_seed,
                index=scenario_idx,
            )
            workflows.append(workflow)

    return CorpusBundle(
        seed=seed,
        environments=environments,
        workflows=workflows,
        metadata={
            "environment_count": len(environments),
            "workflow_count": len(workflows),
        },
    )


def _generate_environment(*, seed: int, index: int) -> GeneratedEnvironment:
    rng = random.Random(seed)
    org_stem = ORG_STEMS[index % len(ORG_STEMS)]
    org_name = f"{org_stem} {rng.choice(['Inc', 'Group', 'Systems', 'Holdings'])}"
    domain_token = org_stem.lower().replace(" ", "")
    primary_domain = f"{domain_token}.example"
    budget_cap = rng.randint(1800, 5500)
    vendors = _sample_vendors(rng)
    po_id = f"PO-{index+1:04d}"
    approval_id = f"APR-{index+1:04d}"
    world_template = {
        "budget_cap_usd": budget_cap,
        "derail_prob": round(rng.uniform(0.0, 0.1), 3),
        "slack_initial_message": (
            f"Procurement run for {org_name}. Include budget and citation in approvals."
        ),
        "vendors": vendors,
        "browser_nodes": _browser_nodes(vendors),
        "database_tables": {
            "procurement_orders": [
                {
                    "id": po_id,
                    "vendor": vendors[0]["name"],
                    "amount_usd": int(vendors[0]["price"][1]),
                    "status": "PENDING_APPROVAL",
                    "cost_center": "IT-OPS",
                }
            ],
            "approval_audit": [
                {
                    "id": approval_id,
                    "entity_type": "purchase_order",
                    "entity_id": po_id,
                    "status": "PENDING",
                    "approver": f"finance@{primary_domain}",
                }
            ],
        },
        "derail_events": [
            {
                "dt_ms": 5000,
                "target": "mail",
                "payload": {
                    "from": f"sales@{primary_domain}",
                    "subj": "Requested Quote",
                    "body_text": f"{org_name} pricing package attached. Please confirm ETA and approver.",
                },
            }
        ],
    }
    return GeneratedEnvironment(
        env_id=f"ENV-{index+1:04d}",
        seed=seed,
        profile=EnterpriseProfile(
            org_id=f"ORG-{index+1:04d}",
            org_name=org_name,
            primary_domain=primary_domain,
            departments=_sample_departments(rng),
            budget_cap_usd=budget_cap,
        ),
        world_template=world_template,
    )


def _generate_workflow_spec(
    *,
    environment: GeneratedEnvironment,
    seed: int,
    index: int,
) -> GeneratedWorkflowSpec:
    rng = random.Random(seed)
    approver = f"approver{index+1}@{environment.profile.primary_domain}"
    quote_to = f"vendor{index+1}@{environment.profile.primary_domain}"
    scenario_id = f"{environment.env_id}-SCN-{index+1:04d}"
    family = WORKFLOW_FAMILIES[index % len(WORKFLOW_FAMILIES)]
    budget = _choose_budget(rng, environment.profile.budget_cap_usd)
    po_id = f"PO-{environment.env_id.split('-', 1)[1]}-{index+1:03d}"
    crm_deal_create_tool = _crm_tool_name("deal_create")
    crm_activity_tool = _crm_tool_name("activity_log")

    objective = _objective_for_family(family)
    success = _success_for_family(family)
    steps = _steps_for_family(
        family=family,
        scenario_id=scenario_id,
        org_name=environment.profile.org_name,
        quote_to=quote_to,
        approver=approver,
        budget=budget,
        po_id=po_id,
        crm_deal_create_tool=crm_deal_create_tool,
        crm_activity_tool=crm_activity_tool,
    )
    failure_paths = _failure_paths_for_family(family)

    workflow_spec: Dict[str, object] = {
        "name": scenario_id,
        "objective": {"statement": objective, "success": success},
        "world": environment.world_template,
        "actors": [
            {
                "actor_id": "agent",
                "role": "procurement_operator",
                "email": f"agent@{environment.profile.primary_domain}",
            },
            {
                "actor_id": "approver",
                "role": "finance_manager",
                "email": approver,
            },
        ],
        "constraints": [
            {
                "name": "budget_cap",
                "description": f"Approval amount must be <= {environment.profile.budget_cap_usd}",
                "required": True,
            },
            {
                "name": "citation_required",
                "description": "At least one browser/doc read action before approval",
                "required": True,
            },
        ],
        "approvals": [
            {
                "stage": "finance",
                "approver": approver,
                "required": True,
                "evidence": "slack thread + ticket or db audit row",
            }
        ],
        "steps": steps,
        "success_assertions": [
            {
                "kind": "pending_max",
                "field": "total",
                "max_value": 20,
            }
        ],
        "failure_paths": failure_paths,
        "tags": [
            "generated",
            "enterprise",
            family,
            rng.choice(["procurement", "finance", "ops"]),
        ],
        "metadata": {
            "environment_id": environment.env_id,
            "scenario_seed": seed,
            "workflow_family": family,
            "crm_deal_create_tool": crm_deal_create_tool,
            "crm_activity_tool": crm_activity_tool,
        },
    }

    return GeneratedWorkflowSpec(
        scenario_id=scenario_id,
        env_id=environment.env_id,
        seed=seed,
        spec=workflow_spec,
    )


def _sample_departments(rng: random.Random) -> List[str]:
    count = rng.randint(3, 5)
    return sorted(rng.sample(DEPARTMENTS, k=count))


def _sample_vendors(rng: random.Random) -> List[Dict[str, object]]:
    vendors = []
    for name in rng.sample(VENDOR_NAMES, k=3):
        base_price = rng.randint(1200, 4200)
        eta = rng.randint(3, 10)
        vendors.append(
            {
                "name": name,
                "price": [base_price - 200, base_price + 200],
                "eta_days": [max(1, eta - 1), eta + 1],
            }
        )
    return vendors


def _browser_nodes(vendors: List[Dict[str, object]]) -> Dict[str, object]:
    home_affordances = []
    home_next = {}
    nodes: Dict[str, Dict[str, object]] = {
        "home": {
            "url": "https://vweb.local/home",
            "title": "Enterprise Procurement Catalog",
            "excerpt": "Choose a vendor and review offer details.",
            "affordances": home_affordances,
            "next": home_next,
        }
    }
    for idx, vendor in enumerate(vendors, start=1):
        slug = f"vendor_{idx}"
        node_id = f"CLICK:open_{slug}#0"
        home_affordances.append({"tool": "browser.click", "args": {"node_id": node_id}})
        home_next[node_id] = slug
        nodes[slug] = {
            "url": f"https://vweb.local/vendor/{idx}",
            "title": str(vendor["name"]),
            "excerpt": (
                f"Price range {vendor['price'][0]}-{vendor['price'][1]} USD, "
                f"ETA {vendor['eta_days'][0]}-{vendor['eta_days'][1]} days."
            ),
            "affordances": [{"tool": "browser.back", "args": {}}],
            "next": {"BACK": "home"},
        }
    return nodes


def _choose_budget(rng: random.Random, cap: int) -> int:
    return max(500, cap - rng.randint(50, 300))


def _crm_tool_name(operation: str) -> str:
    packs_env = os.environ.get("VEI_CRM_ALIAS_PACKS", "hubspot,salesforce")
    packs = [pack.strip().lower() for pack in packs_env.split(",") if pack.strip()]
    if "salesforce" in packs:
        if operation == "deal_create":
            return "salesforce.opportunity.create"
        if operation == "activity_log":
            return "salesforce.activity.log"
    if "hubspot" in packs:
        if operation == "deal_create":
            return "hubspot.deals.create"
        if operation == "activity_log":
            return "hubspot.activities.log"
    if operation == "deal_create":
        return "crm.create_deal"
    return "crm.log_activity"


def _objective_for_family(family: str) -> str:
    if family == "db_audit":
        return (
            "Validate procurement records in DB and route finance approval artifacts."
        )
    if family == "sales_pipeline":
        return "Open a sales pipeline artifact tied to procurement execution evidence."
    if family == "calendar_review":
        return "Schedule review operations and sync approvals across calendar/mail/db."
    if family == "risk_escalation":
        return "Escalate procurement risk with CRM logging and cross-channel notifications."
    if family == "identity_access_review":
        return "Process an enterprise access request through identity and service-desk controls."
    if family == "procure_to_pay":
        return "Execute procure-to-pay lifecycle with ERP and approval audit updates."
    return "Collect vendor evidence, email quote request, and route approval execution."


def _success_for_family(family: str) -> List[str]:
    if family == "db_audit":
        return [
            "Approval audit table inspected",
            "Finance escalation email sent",
            "Approval audit row upserted",
        ]
    if family == "sales_pipeline":
        return [
            "CRM pipeline opportunity created",
            "Quote summary captured in docs",
            "Approval context announced in Slack",
        ]
    if family == "calendar_review":
        return [
            "Review meeting scheduled",
            "Procurement order status updated",
            "Action ticket opened",
        ]
    if family == "risk_escalation":
        return [
            "Risk signal captured in CRM activity",
            "Escalation email sent",
            "Escalation posted in Slack",
        ]
    if family == "identity_access_review":
        return [
            "Pending request reviewed in ServiceDesk",
            "Identity group assignment updated in Okta",
            "Approval status posted in Slack",
        ]
    if family == "procure_to_pay":
        return [
            "Purchase order created in ERP",
            "Invoice matched and payment posted",
            "Audit log row persisted in database",
        ]
    return [
        "Vendor quote requested via mail",
        "Approval request posted in Slack with budget",
        "Execution ticket created",
    ]


def _steps_for_family(
    *,
    family: str,
    scenario_id: str,
    org_name: str,
    quote_to: str,
    approver: str,
    budget: int,
    po_id: str,
    crm_deal_create_tool: str,
    crm_activity_tool: str,
) -> List[Dict[str, object]]:
    if family == "db_audit":
        return [
            {
                "step_id": "query_audit",
                "description": "Read approval audit rows from the DB.",
                "tool": "db.query",
                "args": {"table": "approval_audit", "limit": 10},
                "expect": [
                    {
                        "kind": "result_contains",
                        "field": "table",
                        "contains": "approval_audit",
                    }
                ],
            },
            {
                "step_id": "escalate_finance",
                "description": "Email finance for approval confirmation.",
                "tool": "mail.compose",
                "args": {
                    "to": approver,
                    "subj": f"{scenario_id} approval confirmation",
                    "body_text": (
                        f"Please confirm approval for {scenario_id} budget ${budget}."
                    ),
                },
                "expect": [{"kind": "result_contains", "field": "id", "contains": "m"}],
            },
            {
                "step_id": "post_approval",
                "description": "Post approval request in procurement Slack channel.",
                "tool": "slack.send_message",
                "args": {
                    "channel": "#procurement",
                    "text": (
                        f"Approval needed for {scenario_id}. Budget ${budget}. "
                        "DB audit row checked."
                    ),
                },
                "expect": [{"kind": "result_contains", "field": "ts", "contains": ""}],
            },
            {
                "step_id": "write_audit",
                "description": "Write approval workflow state into audit DB.",
                "tool": "db.upsert",
                "args": {
                    "table": "approval_audit",
                    "row": {
                        "id": f"APR-{scenario_id}",
                        "entity_type": "purchase_order",
                        "entity_id": po_id,
                        "status": "REQUESTED",
                        "approver": approver,
                    },
                },
                "expect": [
                    {"kind": "result_contains", "field": "id", "contains": "APR-"}
                ],
            },
            {
                "step_id": "create_ticket",
                "description": "Open ticket for approval follow-up.",
                "tool": "tickets.create",
                "args": {
                    "title": f"{scenario_id} approval follow-up",
                    "description": "Track finance approval progress and audit linkage.",
                    "assignee": "agent",
                },
                "expect": [
                    {
                        "kind": "result_contains",
                        "field": "ticket_id",
                        "contains": "TCK-",
                    }
                ],
            },
        ]

    if family == "sales_pipeline":
        return [
            {
                "step_id": "create_opportunity",
                "description": "Create pipeline opportunity for this procurement plan.",
                "tool": crm_deal_create_tool,
                "args": {
                    "name": f"{org_name} {scenario_id} renewal",
                    "amount": budget,
                    "stage": "Qualification",
                },
                "expect": [
                    {"kind": "result_contains", "field": "id", "contains": "D-"}
                ],
            },
            {
                "step_id": "capture_quote_doc",
                "description": "Write quote summary into docs for reviewer context.",
                "tool": "docs.create",
                "args": {
                    "title": f"{scenario_id} quote summary",
                    "body": (
                        f"Scenario {scenario_id}: budget ${budget}, approver {approver}."
                    ),
                    "tags": ["quote", "approval", "generated"],
                },
                "expect": [
                    {"kind": "result_contains", "field": "doc_id", "contains": "DOC-"}
                ],
            },
            {
                "step_id": "request_vendor_quote",
                "description": "Send quote request to vendor contact.",
                "tool": "mail.compose",
                "args": {
                    "to": quote_to,
                    "subj": f"{org_name} quote request ({scenario_id})",
                    "body_text": (
                        "Please confirm total amount, ETA, and contract validity window."
                    ),
                },
                "expect": [{"kind": "result_contains", "field": "id", "contains": "m"}],
            },
            {
                "step_id": "post_approval",
                "description": "Post finance approval context in Slack.",
                "tool": "slack.send_message",
                "args": {
                    "channel": "#procurement",
                    "text": (
                        f"Approval request {scenario_id}: budget ${budget}, "
                        "CRM opportunity opened, docs summary captured."
                    ),
                },
                "expect": [{"kind": "result_contains", "field": "ts", "contains": ""}],
            },
            {
                "step_id": "log_activity",
                "description": "Log final approval context in CRM activity stream.",
                "tool": crm_activity_tool,
                "args": {
                    "kind": "note",
                    "note": (
                        f"Scenario {scenario_id} submitted for finance approval at budget ${budget}."
                    ),
                },
                "expect": [
                    {"kind": "result_contains", "field": "ok", "contains": "True"}
                ],
            },
        ]

    if family == "calendar_review":
        return [
            {
                "step_id": "schedule_review",
                "description": "Schedule a finance review call.",
                "tool": "calendar.create_event",
                "args": {
                    "title": f"{scenario_id} finance approval review",
                    "start_ms": 3600_000,
                    "end_ms": 4200_000,
                    "attendees": [approver],
                    "location": "Virtual",
                },
                "expect": [
                    {
                        "kind": "result_contains",
                        "field": "event_id",
                        "contains": "EVT-",
                    }
                ],
            },
            {
                "step_id": "mail_review_context",
                "description": "Email review context and expected decision.",
                "tool": "mail.compose",
                "args": {
                    "to": approver,
                    "subj": f"{scenario_id} review agenda",
                    "body_text": (
                        f"Agenda: approve procurement plan {scenario_id} for ${budget}."
                    ),
                },
                "expect": [{"kind": "result_contains", "field": "id", "contains": "m"}],
            },
            {
                "step_id": "mark_order",
                "description": "Update procurement order state in DB.",
                "tool": "db.upsert",
                "args": {
                    "table": "procurement_orders",
                    "row": {
                        "id": po_id,
                        "vendor": org_name,
                        "amount_usd": budget,
                        "status": "REVIEW_SCHEDULED",
                        "cost_center": "FIN-OPS",
                    },
                },
                "expect": [
                    {"kind": "result_contains", "field": "id", "contains": "PO-"}
                ],
            },
            {
                "step_id": "announce_channel",
                "description": "Post approval workflow status to Slack.",
                "tool": "slack.send_message",
                "args": {
                    "channel": "#procurement",
                    "text": (
                        f"Scheduled finance review for {scenario_id}. "
                        f"Order {po_id} marked REVIEW_SCHEDULED."
                    ),
                },
                "expect": [{"kind": "result_contains", "field": "ts", "contains": ""}],
            },
            {
                "step_id": "create_ticket",
                "description": "Create an execution ticket for operational follow-up.",
                "tool": "tickets.create",
                "args": {
                    "title": f"{scenario_id} operations follow-up",
                    "description": "Coordinate finance review outcome and next actions.",
                    "assignee": "agent",
                },
                "expect": [
                    {
                        "kind": "result_contains",
                        "field": "ticket_id",
                        "contains": "TCK-",
                    }
                ],
            },
        ]

    if family == "risk_escalation":
        return [
            {
                "step_id": "inspect_catalog",
                "description": "Review procurement browser context for anomalies.",
                "tool": "browser.read",
                "args": {},
                "expect": [
                    {"kind": "result_contains", "field": "title", "contains": ""}
                ],
            },
            {
                "step_id": "query_orders",
                "description": "Read current procurement order states from DB.",
                "tool": "db.query",
                "args": {"table": "procurement_orders", "limit": 10},
                "expect": [
                    {
                        "kind": "result_contains",
                        "field": "table",
                        "contains": "procurement_orders",
                    }
                ],
            },
            {
                "step_id": "log_crm_risk",
                "description": "Record risk context in CRM activity log.",
                "tool": crm_activity_tool,
                "args": {
                    "kind": "note",
                    "note": (
                        f"Potential delivery risk for {scenario_id}; escalate pending approval."
                    ),
                },
                "expect": [
                    {"kind": "result_contains", "field": "ok", "contains": "True"}
                ],
            },
            {
                "step_id": "mail_escalation",
                "description": "Escalate approval request by email.",
                "tool": "mail.compose",
                "args": {
                    "to": approver,
                    "subj": f"{scenario_id} risk escalation",
                    "body_text": (
                        "Delivery risk identified. Please approve mitigation budget and timeline."
                    ),
                },
                "expect": [{"kind": "result_contains", "field": "id", "contains": "m"}],
            },
            {
                "step_id": "post_approval",
                "description": "Post approval escalation context in Slack.",
                "tool": "slack.send_message",
                "args": {
                    "channel": "#procurement",
                    "text": (
                        f"Escalation: {scenario_id} needs finance approval for risk mitigation."
                    ),
                },
                "expect": [{"kind": "result_contains", "field": "ts", "contains": ""}],
            },
        ]

    if family == "identity_access_review":
        return [
            {
                "step_id": "list_pending_requests",
                "description": "Review pending access requests in ServiceDesk.",
                "tool": "servicedesk.list_requests",
                "args": {"status": "PENDING_APPROVAL", "limit": 10},
                "expect": [
                    {"kind": "result_contains", "field": "requests", "contains": "REQ-"}
                ],
            },
            {
                "step_id": "inspect_identity",
                "description": "Inspect user state in Okta before assignment.",
                "tool": "okta.get_user",
                "args": {"user_id": "USR-9001"},
                "expect": [
                    {
                        "kind": "result_contains",
                        "field": "email",
                        "contains": "example.com",
                    }
                ],
            },
            {
                "step_id": "assign_group",
                "description": "Assign user to IT support group for temporary access.",
                "tool": "okta.assign_group",
                "args": {"user_id": "USR-9001", "group_id": "GRP-it"},
                "expect": [
                    {"kind": "result_contains", "field": "group_id", "contains": "GRP-"}
                ],
            },
            {
                "step_id": "approve_request",
                "description": "Update service request approval stage.",
                "tool": "servicedesk.update_request",
                "args": {
                    "request_id": "REQ-8801",
                    "status": "APPROVED",
                    "approval_stage": "security",
                    "approval_status": "APPROVED",
                    "comment": "Okta group assignment completed and validated.",
                },
                "expect": [
                    {
                        "kind": "result_contains",
                        "field": "status",
                        "contains": "APPROVED",
                    }
                ],
            },
            {
                "step_id": "announce_access",
                "description": "Announce access completion in Slack.",
                "tool": "slack.send_message",
                "args": {
                    "channel": "#procurement",
                    "text": (
                        f"Access request {scenario_id} approved; identity assignment applied for review."
                    ),
                },
                "expect": [{"kind": "result_contains", "field": "ts", "contains": ""}],
            },
        ]

    if family == "procure_to_pay":
        return [
            {
                "step_id": "create_po",
                "description": "Create ERP purchase order for procurement plan.",
                "tool": "erp.create_po",
                "args": {
                    "vendor": "MacroCompute",
                    "currency": "USD",
                    "lines": [
                        {
                            "item_id": "LAPTOP-15",
                            "desc": "Laptop fleet refresh",
                            "qty": 5,
                            "unit_price": budget / 5,
                        }
                    ],
                },
                "expect": [
                    {"kind": "result_contains", "field": "id", "contains": "PO-"}
                ],
            },
            {
                "step_id": "receive_goods",
                "description": "Receive goods against the ERP purchase order.",
                "tool": "erp.receive_goods",
                "args": {
                    "po_id": "PO-1",
                    "lines": [{"item_id": "LAPTOP-15", "qty": 5}],
                },
                "expect": [
                    {"kind": "result_contains", "field": "id", "contains": "RCPT-"}
                ],
            },
            {
                "step_id": "submit_invoice",
                "description": "Submit invoice for the received order.",
                "tool": "erp.submit_invoice",
                "args": {
                    "vendor": "MacroCompute",
                    "po_id": "PO-1",
                    "lines": [
                        {"item_id": "LAPTOP-15", "qty": 5, "unit_price": budget / 5}
                    ],
                },
                "expect": [
                    {"kind": "result_contains", "field": "id", "contains": "INV-"}
                ],
            },
            {
                "step_id": "match_three_way",
                "description": "Run ERP three-way match.",
                "tool": "erp.match_three_way",
                "args": {
                    "po_id": "PO-1",
                    "invoice_id": "INV-1",
                    "receipt_id": "RCPT-1",
                },
                "expect": [
                    {"kind": "result_contains", "field": "status", "contains": "MATCH"}
                ],
            },
            {
                "step_id": "post_payment",
                "description": "Post invoice payment after successful match.",
                "tool": "erp.post_payment",
                "args": {"invoice_id": "INV-1", "amount": float(budget)},
                "expect": [
                    {"kind": "result_contains", "field": "status", "contains": "PAID"}
                ],
            },
            {
                "step_id": "write_audit",
                "description": "Write procure-to-pay completion row to audit DB.",
                "tool": "db.upsert",
                "args": {
                    "table": "approval_audit",
                    "row": {
                        "id": f"APR-{scenario_id}",
                        "entity_type": "purchase_order",
                        "entity_id": "PO-1",
                        "status": "PAID",
                        "approver": approver,
                    },
                },
                "expect": [
                    {"kind": "result_contains", "field": "id", "contains": "APR-"}
                ],
            },
        ]

    return [
        {
            "step_id": "read_browser",
            "description": "Open procurement catalog context.",
            "tool": "browser.read",
            "args": {},
            "expect": [{"kind": "result_contains", "field": "title", "contains": ""}],
        },
        {
            "step_id": "search_docs",
            "description": "Search policy docs for procurement guidance.",
            "tool": "docs.search",
            "args": {"query": "policy"},
            "expect": [],
        },
        {
            "step_id": "request_quote",
            "description": "Send quote request email to the assigned vendor contact.",
            "tool": "mail.compose",
            "args": {
                "to": quote_to,
                "subj": f"{org_name} procurement quote request",
                "body_text": (
                    f"Please share quote and ETA for laptop batch ({scenario_id}). "
                    "Include total amount and delivery timeline."
                ),
            },
            "expect": [{"kind": "result_contains", "field": "id", "contains": "m"}],
        },
        {
            "step_id": "post_approval",
            "description": "Post approval request in procurement Slack channel.",
            "tool": "slack.send_message",
            "args": {
                "channel": "#procurement",
                "text": (
                    f"Request approval for {scenario_id}. Budget ${budget}. "
                    "Evidence reviewed in browser/docs."
                ),
            },
            "expect": [{"kind": "result_contains", "field": "ts", "contains": ""}],
        },
        {
            "step_id": "create_ticket",
            "description": "Create ticket with workflow completion note.",
            "tool": "tickets.create",
            "args": {
                "title": f"{scenario_id} execution summary",
                "description": (
                    f"{scenario_id} executed: quote requested and approval posted."
                ),
                "assignee": "agent",
            },
            "expect": [
                {"kind": "result_contains", "field": "ticket_id", "contains": "TCK-"}
            ],
        },
    ]


def _failure_paths_for_family(family: str) -> List[Dict[str, object]]:
    if family == "db_audit":
        return [
            {
                "name": "audit_write_retry",
                "trigger_step": "write_audit",
                "recovery_steps": ["post_approval"],
                "notes": "If DB write fails, keep approval thread updated.",
            }
        ]
    if family == "sales_pipeline":
        return [
            {
                "name": "crm_activity_retry",
                "trigger_step": "log_activity",
                "recovery_steps": ["post_approval"],
                "notes": "If CRM logging fails, continue with approval channel artifacts.",
            }
        ]
    if family == "calendar_review":
        return [
            {
                "name": "calendar_recover",
                "trigger_step": "schedule_review",
                "recovery_steps": ["mail_review_context", "announce_channel"],
                "notes": "If event creation fails, preserve approval context over mail/slack.",
            }
        ]
    if family == "risk_escalation":
        return [
            {
                "name": "escalation_continue",
                "trigger_step": "log_crm_risk",
                "recovery_steps": ["mail_escalation", "post_approval"],
                "notes": "Escalate even if CRM activity logging is unavailable.",
            }
        ]
    if family == "identity_access_review":
        return [
            {
                "name": "identity_assign_retry",
                "trigger_step": "assign_group",
                "recovery_steps": ["approve_request", "announce_access"],
                "notes": "If identity assignment fails, continue request progression with explicit comment.",
            }
        ]
    if family == "procure_to_pay":
        return [
            {
                "name": "three_way_mismatch_recovery",
                "trigger_step": "match_three_way",
                "recovery_steps": ["write_audit"],
                "notes": "Persist mismatch details to audit table for AP investigation.",
            }
        ]
    return [
        {
            "name": "ticket_recover",
            "trigger_step": "create_ticket",
            "recovery_steps": ["post_approval"],
            "notes": "Proceed if ticket service is unavailable.",
        }
    ]
