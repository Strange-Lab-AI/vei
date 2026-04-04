from __future__ import annotations

import os
from typing import Any

from vei.blueprint import FacadePlugin

from .alias_packs import CRM_ALIAS_PACKS, ERP_ALIAS_PACKS
from .tool_registry import ToolSpec


def build_alias_map() -> dict[str, str]:
    alias_map: dict[str, str] = {}
    erp_packs_env = os.environ.get("VEI_ALIAS_PACKS", "xero").strip()
    erp_packs = [pack.strip() for pack in erp_packs_env.split(",") if pack.strip()]
    for pack in erp_packs:
        for alias, base in ERP_ALIAS_PACKS.get(pack, []):
            alias_map[alias] = base

    crm_packs_env = os.environ.get("VEI_CRM_ALIAS_PACKS", "hubspot,salesforce").strip()
    crm_packs = [pack.strip() for pack in crm_packs_env.split(",") if pack.strip()]
    for pack in crm_packs:
        for alias, base in CRM_ALIAS_PACKS.get(pack, []):
            alias_map[alias] = base
    return alias_map


def build_builtin_tool_specs() -> list[ToolSpec]:
    specs: list[ToolSpec] = []
    specs.extend(_core_specs())
    specs.extend(_docs_specs())
    specs.extend(_calendar_specs())
    specs.extend(_ticket_specs())
    specs.extend(_database_specs())
    specs.extend(_erp_specs())
    specs.extend(_crm_specs())
    return specs


def build_help_payload(router: Any) -> dict[str, Any]:
    focuses = [
        "browser",
        "slack",
        "mail",
        "docs",
        "calendar",
        "tickets",
        "db",
        "erp",
        "crm",
        "okta",
        "servicedesk",
    ]
    for entry in router.facade_plugins.values():
        plugin: FacadePlugin = entry["plugin"]
        for focus in plugin.focuses:
            if focus not in focuses:
                focuses.append(focus)

    focus_menus: dict[str, list[dict[str, Any]]] = {}
    for focus in focuses:
        menu = router._action_menu(focus)
        if menu:
            focus_menus[focus] = menu

    tools = [
        {
            "tool": spec.name,
            "description": spec.description,
            "permissions": list(spec.permissions),
            "side_effects": list(spec.side_effects),
        }
        for spec in sorted(router.registry.list(), key=lambda item: item.name)
    ]

    software = [
        "slack",
        "mail",
        "browser",
        "docs",
        "calendar",
        "tickets",
        "db",
        "erp",
        "crm",
        "okta",
        "servicedesk",
    ]
    for entry in router.facade_plugins.values():
        plugin: FacadePlugin = entry["plugin"]
        if plugin.manifest.name not in software:
            software.append(plugin.manifest.name)

    return {
        "instructions": (
            "Use MCP tools against the virtual enterprise. "
            "Typical loop: observe -> call one tool -> observe again."
        ),
        "software": software,
        "tools": tools,
        "focus_action_menus": focus_menus,
        "examples": [
            {"tool": "vei.observe", "args": {"focus": "browser"}},
            {
                "tool": "mail.compose",
                "args": {
                    "to": "sales@macrocompute.example",
                    "subj": "Quote request",
                    "body_text": "Please send quote and ETA.",
                },
            },
            {
                "tool": "slack.send_message",
                "args": {
                    "channel": "#procurement",
                    "text": "Approval request with budget and evidence.",
                },
            },
            {"tool": "db.query", "args": {"table": "approval_audit", "limit": 5}},
            {
                "tool": "servicedesk.list_requests",
                "args": {"status": "PENDING_APPROVAL", "limit": 5},
            },
            {"tool": "okta.list_users", "args": {"status": "ACTIVE", "limit": 5}},
        ],
    }


def _core_specs() -> list[ToolSpec]:
    return [
        ToolSpec(
            name="vei.observe",
            description="Obtain the current observation (advances time).",
            side_effects=("time_advance",),
            default_latency_ms=1000,
        ),
        ToolSpec(
            name="vei.tick",
            description="Advance logical time and deliver due events.",
            side_effects=("time_advance", "event_delivery"),
        ),
        ToolSpec(
            name="vei.act_and_observe",
            description="Execute a tool then fetch the next observation.",
            side_effects=("time_advance",),
        ),
        ToolSpec(
            name="vei.tools.search",
            description="Search the MCP tool catalog for relevant entries.",
        ),
        ToolSpec(
            name="vei.state",
            description="Inspect state head, receipts, and recent tool calls.",
            side_effects=(),
        ),
        ToolSpec(
            name="vei.inject",
            description="Inject an external event (e.g. human message) into the simulation.",
            side_effects=("event_schedule",),
        ),
        ToolSpec(
            name="slack.send_message",
            description="Post a message into a Slack channel thread.",
            side_effects=("slack_outbound",),
            permissions=("slack:write",),
            default_latency_ms=500,
            latency_jitter_ms=200,
            fault_probability=0.01,
        ),
        ToolSpec(
            name="slack.open_channel",
            description="Open a Slack channel view.",
            side_effects=(),
            permissions=("slack:read",),
        ),
        ToolSpec(
            name="slack.fetch_thread",
            description="Fetch a Slack thread for review.",
            side_effects=(),
            permissions=("slack:read",),
        ),
        ToolSpec(
            name="slack.list_channels",
            description="List available Slack channels.",
            permissions=("slack:read",),
        ),
        ToolSpec(
            name="slack.react",
            description="Add a reaction to a Slack message.",
            side_effects=("slack_outbound",),
            permissions=("slack:write",),
        ),
        ToolSpec(
            name="mail.compose",
            description="Send an email to a recipient.",
            side_effects=("mail_outbound", "event_schedule"),
            permissions=("mail:write",),
            default_latency_ms=800,
            latency_jitter_ms=300,
            fault_probability=0.02,
        ),
        ToolSpec(
            name="mail.list",
            description="List newest messages in the inbox.",
            permissions=("mail:read",),
        ),
        ToolSpec(
            name="mail.open",
            description="Open a specific email body.",
            permissions=("mail:read",),
        ),
        ToolSpec(
            name="mail.reply",
            description="Reply to an existing email thread.",
            side_effects=("mail_outbound", "event_schedule"),
            permissions=("mail:write",),
            default_latency_ms=800,
            latency_jitter_ms=300,
            fault_probability=0.02,
        ),
        ToolSpec(
            name="browser.read",
            description="Read current browser node.",
            permissions=("browser:read",),
        ),
        ToolSpec(
            name="browser.click",
            description="Click a UI element and navigate.",
            side_effects=("browser_navigation",),
            permissions=("browser:write",),
        ),
        ToolSpec(
            name="browser.find",
            description="Search current document for affordances.",
            permissions=("browser:read",),
        ),
        ToolSpec(
            name="browser.open",
            description="Open a URL inside the virtual browser.",
            side_effects=("browser_navigation",),
            permissions=("browser:write",),
        ),
        ToolSpec(
            name="browser.back",
            description="Navigate back to the previous page.",
            side_effects=("browser_navigation",),
            permissions=("browser:write",),
        ),
        ToolSpec(
            name="browser.type",
            description="Type text into a field.",
            side_effects=("browser_input",),
            permissions=("browser:write",),
        ),
        ToolSpec(
            name="browser.submit",
            description="Submit a form.",
            side_effects=("browser_navigation",),
            permissions=("browser:write",),
        ),
    ]


def _docs_specs() -> list[ToolSpec]:
    return [
        ToolSpec(
            name="docs.list",
            description="List documents in the knowledge base with optional filtering/pagination.",
            permissions=("docs:read",),
        ),
        ToolSpec(
            name="docs.read",
            description="Read a document by id.",
            permissions=("docs:read",),
        ),
        ToolSpec(
            name="docs.search",
            description="Search documents for a query.",
            permissions=("docs:read",),
        ),
        ToolSpec(
            name="docs.create",
            description="Create a new document entry.",
            permissions=("docs:write",),
            side_effects=("docs_mutation",),
            default_latency_ms=400,
            latency_jitter_ms=150,
        ),
        ToolSpec(
            name="docs.update",
            description="Update an existing document.",
            permissions=("docs:write",),
            side_effects=("docs_mutation",),
            default_latency_ms=350,
            latency_jitter_ms=120,
        ),
    ]


def _calendar_specs() -> list[ToolSpec]:
    return [
        ToolSpec(
            name="calendar.list_events",
            description="List calendar events with optional filtering/pagination.",
            permissions=("calendar:read",),
        ),
        ToolSpec(
            name="calendar.create_event",
            description="Create a new calendar event.",
            permissions=("calendar:write",),
            side_effects=("calendar_mutation",),
            default_latency_ms=600,
            latency_jitter_ms=200,
        ),
        ToolSpec(
            name="calendar.accept",
            description="Accept a calendar invite.",
            permissions=("calendar:write",),
            side_effects=("calendar_response",),
            default_latency_ms=300,
            latency_jitter_ms=150,
        ),
        ToolSpec(
            name="calendar.decline",
            description="Decline a calendar invite.",
            permissions=("calendar:write",),
            side_effects=("calendar_response",),
            default_latency_ms=300,
            latency_jitter_ms=150,
        ),
        ToolSpec(
            name="calendar.update_event",
            description="Update event fields (time, attendees, description, status).",
            permissions=("calendar:write",),
            side_effects=("calendar_mutation",),
            default_latency_ms=500,
            latency_jitter_ms=180,
        ),
        ToolSpec(
            name="calendar.cancel_event",
            description="Cancel a calendar event with optional reason.",
            permissions=("calendar:write",),
            side_effects=("calendar_mutation",),
            default_latency_ms=420,
            latency_jitter_ms=140,
        ),
    ]


def _ticket_specs() -> list[ToolSpec]:
    return [
        ToolSpec(
            name="tickets.list",
            description="List tickets in the queue with optional filtering/pagination.",
            permissions=("tickets:read",),
        ),
        ToolSpec(
            name="tickets.get",
            description="Fetch ticket details.",
            permissions=("tickets:read",),
        ),
        ToolSpec(
            name="tickets.create",
            description="Create a new ticket.",
            permissions=("tickets:write",),
            side_effects=("tickets_mutation",),
            default_latency_ms=500,
            latency_jitter_ms=200,
            fault_probability=0.03,
        ),
        ToolSpec(
            name="tickets.update",
            description="Update ticket fields.",
            permissions=("tickets:write",),
            side_effects=("tickets_mutation",),
            default_latency_ms=400,
            latency_jitter_ms=150,
        ),
        ToolSpec(
            name="tickets.transition",
            description="Transition a ticket to a new status.",
            permissions=("tickets:write",),
            side_effects=("tickets_mutation",),
            default_latency_ms=420,
            latency_jitter_ms=160,
            fault_probability=0.02,
        ),
        ToolSpec(
            name="tickets.add_comment",
            description="Add a comment to a ticket.",
            permissions=("tickets:write",),
            side_effects=("tickets_mutation",),
            default_latency_ms=350,
            latency_jitter_ms=130,
        ),
    ]


def _database_specs() -> list[ToolSpec]:
    return [
        ToolSpec(
            name="db.list_tables",
            description="List available enterprise database tables.",
            permissions=("db:read",),
        ),
        ToolSpec(
            name="db.describe_table",
            description="Describe columns and row counts for a database table.",
            permissions=("db:read",),
        ),
        ToolSpec(
            name="db.query",
            description="Run a structured query over a database table.",
            permissions=("db:read",),
        ),
        ToolSpec(
            name="db.upsert",
            description="Insert or update a row in a database table.",
            permissions=("db:write",),
            side_effects=("db_mutation",),
            default_latency_ms=450,
            latency_jitter_ms=150,
            fault_probability=0.01,
        ),
    ]


def _erp_specs() -> list[ToolSpec]:
    return [
        ToolSpec(
            name="erp.create_po",
            description="Create a purchase order.",
            permissions=("erp:write",),
        ),
        ToolSpec(
            name="erp.get_po",
            description="Retrieve a purchase order.",
            permissions=("erp:read",),
        ),
        ToolSpec(
            name="erp.list_pos",
            description="List purchase orders.",
            permissions=("erp:read",),
        ),
        ToolSpec(
            name="erp.receive_goods",
            description="Record goods receipt.",
            permissions=("erp:write",),
        ),
        ToolSpec(
            name="erp.submit_invoice",
            description="Submit a vendor invoice.",
            permissions=("erp:write",),
        ),
        ToolSpec(
            name="erp.get_invoice",
            description="Retrieve invoice detail.",
            permissions=("erp:read",),
        ),
        ToolSpec(
            name="erp.list_invoices",
            description="List invoices.",
            permissions=("erp:read",),
        ),
        ToolSpec(
            name="erp.match_three_way",
            description="Run three-way match.",
            permissions=("erp:write",),
        ),
        ToolSpec(
            name="erp.post_payment",
            description="Post a payment.",
            permissions=("erp:write",),
        ),
    ]


def _crm_specs() -> list[ToolSpec]:
    return [
        ToolSpec(
            name="crm.create_contact",
            description="Create a CRM contact.",
            permissions=("crm:write",),
        ),
        ToolSpec(
            name="crm.get_contact",
            description="Fetch CRM contact details.",
            permissions=("crm:read",),
        ),
        ToolSpec(
            name="crm.list_contacts",
            description="List contacts.",
            permissions=("crm:read",),
        ),
        ToolSpec(
            name="crm.create_company",
            description="Create a company record.",
            permissions=("crm:write",),
        ),
        ToolSpec(
            name="crm.get_company",
            description="Fetch company details.",
            permissions=("crm:read",),
        ),
        ToolSpec(
            name="crm.list_companies",
            description="List company records.",
            permissions=("crm:read",),
        ),
        ToolSpec(
            name="crm.associate_contact_company",
            description="Link contact to company.",
            permissions=("crm:write",),
        ),
        ToolSpec(
            name="crm.create_deal",
            description="Create a deal/opportunity.",
            permissions=("crm:write",),
        ),
        ToolSpec(
            name="crm.get_deal",
            description="Fetch deal details.",
            permissions=("crm:read",),
        ),
        ToolSpec(
            name="crm.list_deals",
            description="List deals.",
            permissions=("crm:read",),
        ),
        ToolSpec(
            name="crm.update_deal_stage",
            description="Update deal stage.",
            permissions=("crm:write",),
        ),
        ToolSpec(
            name="crm.reassign_deal_owner",
            description="Transfer deal ownership.",
            permissions=("crm:write",),
        ),
        ToolSpec(
            name="crm.log_activity",
            description="Log an activity.",
            permissions=("crm:write",),
        ),
    ]
