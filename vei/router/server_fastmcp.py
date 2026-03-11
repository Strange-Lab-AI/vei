import os
from typing import Any

from mcp.server.fastmcp import server as fserver
from pydantic import BaseModel, Field

from vei.blueprint.plugins import list_runtime_facade_plugins

from .core import Router, MCPError
from .tool_registry import ToolSpec
from .alias_packs import ERP_ALIAS_PACKS, CRM_ALIAS_PACKS


class SlackOpenArgs(BaseModel):
    channel: str


class SlackSendArgs(BaseModel):
    channel: str
    text: str
    thread_ts: str | None = None


class SlackReactArgs(BaseModel):
    channel: str
    ts: str
    emoji: str


class SlackFetchThreadArgs(BaseModel):
    channel: str
    thread_ts: str


class MailListArgs(BaseModel):
    folder: str = "INBOX"


class MailOpenArgs(BaseModel):
    id: str


class MailComposeArgs(BaseModel):
    to: str
    subj: str
    body_text: str


class MailReplyArgs(BaseModel):
    id: str
    body_text: str


class BrowserOpenArgs(BaseModel):
    url: str


class BrowserFindArgs(BaseModel):
    query: str
    top_k: int = 10


class BrowserClickArgs(BaseModel):
    node_id: str


class BrowserTypeArgs(BaseModel):
    node_id: str
    text: str


class BrowserSubmitArgs(BaseModel):
    form_id: str


class ObserveArgs(BaseModel):
    focus: str | None = None


class ResetArgs(BaseModel):
    seed: int | None = None


class ActAndObserveArgs(BaseModel):
    tool: str
    # Avoid shared mutable defaults across requests
    args: dict[str, Any] = Field(default_factory=dict)


class TickArgs(BaseModel):
    dt_ms: int = 1000


class _RouterHolder:
    def __init__(self, router: Router):
        self.router = router


def create_mcp_server(
    router: Router,
    host: str | None = None,
    port: int | None = None,
    mount_path: str = "/",
) -> fserver.FastMCP:
    # Read host/port from args or env (defaults)
    if host is None:
        host = os.environ.get("VEI_HOST", "127.0.0.1")
    if port is None:
        try:
            port = int(os.environ.get("VEI_PORT", "3001"))
        except ValueError:
            port = 3001

    # Honor logging and debug via env so diagnostics show up
    log_level = os.environ.get("FASTMCP_LOG_LEVEL", "INFO").upper()
    debug_flag = os.environ.get("FASTMCP_DEBUG", "0") in {
        "1",
        "true",
        "TRUE",
        "yes",
        "on",
    }

    # Relax transport security for local dev if explicitly requested
    ts = None
    if os.environ.get("FASTMCP_DISABLE_SECURITY") in {"1", "true", "TRUE", "yes", "on"}:
        try:
            from mcp.server.fastmcp.server import TransportSecuritySettings  # type: ignore

            ts = TransportSecuritySettings(enable_dns_rebinding_protection=False)
        except Exception:
            ts = None

    srv = fserver.FastMCP(
        name="VEI Router",
        instructions="Virtual Enterprise Internet — synthetic MCP world",
        host=host,
        port=port,
        mount_path=mount_path,
        log_level=log_level,  # ensure FastMCP logging reflects env
        debug=debug_flag,
        transport_security=ts,
    )
    holder = _RouterHolder(router)

    # Utility to access current router
    def R() -> Router:
        return holder.router

    def _safe_call(tool: str, args: dict[str, Any]) -> dict[str, Any]:
        try:
            return R().call_and_step(tool, args)
        except MCPError as e:
            return {"error": {"code": e.code, "message": e.message}}

    @srv.tool(name="slack.list_channels", description="List Slack channels")
    def slack_list_channels() -> list[str]:
        return R().call_and_step("slack.list_channels", {})  # type: ignore[return-value]

    @srv.tool(name="slack.open_channel", description="Open a Slack channel")
    def slack_open_channel(channel: str) -> dict[str, Any]:
        try:
            return R().call_and_step("slack.open_channel", {"channel": channel})
        except MCPError as e:
            return {"error": {"code": e.code, "message": e.message}}

    @srv.tool(name="slack.send_message", description="Send a Slack message")
    def slack_send_message(
        channel: str, text: str, thread_ts: str = None
    ) -> dict[str, Any]:
        try:
            return R().call_and_step(
                "slack.send_message",
                {"channel": channel, "text": text, "thread_ts": thread_ts},
            )
        except MCPError as e:
            return {"error": {"code": e.code, "message": e.message}}

    @srv.tool(name="slack.react", description="React to a message")
    def slack_react(channel: str, ts: str, emoji: str) -> dict[str, Any]:
        try:
            return R().call_and_step(
                "slack.react", {"channel": channel, "ts": ts, "emoji": emoji}
            )
        except MCPError as e:
            return {"error": {"code": e.code, "message": e.message}}

    @srv.tool(name="slack.fetch_thread", description="Fetch a thread")
    def slack_fetch_thread(channel: str, thread_ts: str) -> dict[str, Any]:
        try:
            return R().call_and_step(
                "slack.fetch_thread", {"channel": channel, "thread_ts": thread_ts}
            )
        except MCPError as e:
            return {"error": {"code": e.code, "message": e.message}}

    @srv.tool(name="mail.list", description="List mail folder")
    def mail_list(folder: str = "INBOX") -> list[dict[str, Any]]:
        return R().call_and_step("mail.list", {"folder": folder})  # type: ignore[return-value]

    @srv.tool(name="mail.open", description="Open a message")
    def mail_open(id: str) -> dict[str, Any]:
        try:
            return R().call_and_step("mail.open", {"id": id})
        except MCPError as e:
            return {"error": {"code": e.code, "message": e.message}}

    @srv.tool(name="mail.compose", description="Compose a message")
    def mail_compose(to: str, subj: str, body_text: str) -> dict[str, Any]:
        return R().call_and_step(
            "mail.compose", {"to": to, "subj": subj, "body_text": body_text}
        )

    @srv.tool(name="mail.reply", description="Reply to a message")
    def mail_reply(id: str, body_text: str) -> dict[str, Any]:
        try:
            return R().call_and_step("mail.reply", {"id": id, "body_text": body_text})
        except MCPError as e:
            return {"error": {"code": e.code, "message": e.message}}

    @srv.tool(name="browser.open", description="Open a URL")
    def browser_open(url: str) -> dict[str, Any]:
        return R().call_and_step("browser.open", {"url": url})

    @srv.tool(name="browser.find", description="Find visible affordances")
    def browser_find(query: str, top_k: int = 10) -> dict[str, Any]:
        return R().call_and_step("browser.find", {"query": query, "top_k": top_k})

    @srv.tool(name="browser.click", description="Click an affordance")
    def browser_click(node_id: str) -> dict[str, Any]:
        try:
            return R().call_and_step("browser.click", {"node_id": node_id})
        except MCPError as e:
            return {"error": {"code": e.code, "message": e.message}}

    @srv.tool(name="browser.type", description="Type into a field")
    def browser_type(node_id: str, text: str) -> dict[str, Any]:
        return R().call_and_step("browser.type", {"node_id": node_id, "text": text})

    @srv.tool(name="browser.submit", description="Submit a form")
    def browser_submit(form_id: str) -> dict[str, Any]:
        return R().call_and_step("browser.submit", {"form_id": form_id})

    @srv.tool(name="docs.list", description="List knowledge base documents")
    def docs_list(
        query: str = None,
        tag: str = None,
        status: str = None,
        owner: str = None,
        limit: int = None,
        cursor: str = None,
        sort_by: str = "updated_ms",
        sort_dir: str = "desc",
    ) -> dict[str, Any] | list[dict[str, Any]]:
        return R().call_and_step(  # type: ignore[return-value]
            "docs.list",
            {
                "query": query,
                "tag": tag,
                "status": status,
                "owner": owner,
                "limit": limit,
                "cursor": cursor,
                "sort_by": sort_by,
                "sort_dir": sort_dir,
            },
        )

    @srv.tool(name="docs.read", description="Read a document")
    def docs_read(doc_id: str) -> dict[str, Any]:
        try:
            return R().call_and_step("docs.read", {"doc_id": doc_id})
        except MCPError as e:
            return {"error": {"code": e.code, "message": e.message}}

    @srv.tool(name="docs.search", description="Search documents")
    def docs_search(
        query: str, limit: int = 20, cursor: str = None
    ) -> dict[str, Any] | list[dict[str, Any]]:
        return R().call_and_step(  # type: ignore[return-value]
            "docs.search",
            {"query": query, "limit": limit, "cursor": cursor},
        )

    @srv.tool(name="docs.create", description="Create a document")
    def docs_create(
        title: str,
        body: str,
        tags: list = None,
        owner: str = None,
        status: str = "DRAFT",
    ) -> dict[str, Any]:
        return R().call_and_step(
            "docs.create",
            {
                "title": title,
                "body": body,
                "tags": tags,
                "owner": owner,
                "status": status,
            },
        )

    @srv.tool(name="docs.update", description="Update a document")
    def docs_update(
        doc_id: str,
        title: str = None,
        body: str = None,
        tags: list = None,
        status: str = None,
    ) -> dict[str, Any]:
        try:
            return R().call_and_step(
                "docs.update",
                {
                    "doc_id": doc_id,
                    "title": title,
                    "body": body,
                    "tags": tags,
                    "status": status,
                },
            )
        except MCPError as e:
            return {"error": {"code": e.code, "message": e.message}}

    @srv.tool(name="calendar.list_events", description="List calendar events")
    def calendar_list_events(
        attendee: str = None,
        status: str = None,
        starts_after_ms: int = None,
        ends_before_ms: int = None,
        limit: int = None,
        cursor: str = None,
        sort_dir: str = "asc",
    ) -> dict[str, Any] | list[dict[str, Any]]:
        return R().call_and_step(  # type: ignore[return-value]
            "calendar.list_events",
            {
                "attendee": attendee,
                "status": status,
                "starts_after_ms": starts_after_ms,
                "ends_before_ms": ends_before_ms,
                "limit": limit,
                "cursor": cursor,
                "sort_dir": sort_dir,
            },
        )

    @srv.tool(name="calendar.create_event", description="Create a calendar event")
    def calendar_create_event(
        title: str,
        start_ms: int,
        end_ms: int,
        attendees: list = None,
        location: str = None,
        description: str = None,
        organizer: str = None,
        status: str = "CONFIRMED",
    ) -> dict[str, Any]:
        return R().call_and_step(
            "calendar.create_event",
            {
                "title": title,
                "start_ms": start_ms,
                "end_ms": end_ms,
                "attendees": attendees,
                "location": location,
                "description": description,
                "organizer": organizer,
                "status": status,
            },
        )

    @srv.tool(name="calendar.accept", description="Accept a calendar invite")
    def calendar_accept(event_id: str, attendee: str) -> dict[str, Any]:
        try:
            return R().call_and_step(
                "calendar.accept", {"event_id": event_id, "attendee": attendee}
            )
        except MCPError as e:
            return {"error": {"code": e.code, "message": e.message}}

    @srv.tool(name="calendar.decline", description="Decline a calendar invite")
    def calendar_decline(event_id: str, attendee: str) -> dict[str, Any]:
        try:
            return R().call_and_step(
                "calendar.decline", {"event_id": event_id, "attendee": attendee}
            )
        except MCPError as e:
            return {"error": {"code": e.code, "message": e.message}}

    @srv.tool(name="calendar.update_event", description="Update a calendar event")
    def calendar_update_event(
        event_id: str,
        title: str = None,
        start_ms: int = None,
        end_ms: int = None,
        attendees: list = None,
        location: str = None,
        description: str = None,
        status: str = None,
    ) -> dict[str, Any]:
        try:
            return R().call_and_step(
                "calendar.update_event",
                {
                    "event_id": event_id,
                    "title": title,
                    "start_ms": start_ms,
                    "end_ms": end_ms,
                    "attendees": attendees,
                    "location": location,
                    "description": description,
                    "status": status,
                },
            )
        except MCPError as e:
            return {"error": {"code": e.code, "message": e.message}}

    @srv.tool(name="calendar.cancel_event", description="Cancel a calendar event")
    def calendar_cancel_event(event_id: str, reason: str = None) -> dict[str, Any]:
        try:
            return R().call_and_step(
                "calendar.cancel_event",
                {"event_id": event_id, "reason": reason},
            )
        except MCPError as e:
            return {"error": {"code": e.code, "message": e.message}}

    @srv.tool(name="tickets.list", description="List tickets")
    def tickets_list(
        status: str = None,
        assignee: str = None,
        priority: str = None,
        query: str = None,
        limit: int = None,
        cursor: str = None,
        sort_by: str = "updated_ms",
        sort_dir: str = "desc",
    ) -> dict[str, Any] | list[dict[str, Any]]:
        return R().call_and_step(  # type: ignore[return-value]
            "tickets.list",
            {
                "status": status,
                "assignee": assignee,
                "priority": priority,
                "query": query,
                "limit": limit,
                "cursor": cursor,
                "sort_by": sort_by,
                "sort_dir": sort_dir,
            },
        )

    @srv.tool(name="tickets.get", description="Get ticket detail")
    def tickets_get(ticket_id: str) -> dict[str, Any]:
        try:
            return R().call_and_step("tickets.get", {"ticket_id": ticket_id})
        except MCPError as e:
            return {"error": {"code": e.code, "message": e.message}}

    @srv.tool(name="tickets.create", description="Create a ticket")
    def tickets_create(
        title: str,
        description: str = None,
        assignee: str = None,
        priority: str = "P3",
        severity: str = "medium",
        labels: list = None,
    ) -> dict[str, Any]:
        return R().call_and_step(
            "tickets.create",
            {
                "title": title,
                "description": description,
                "assignee": assignee,
                "priority": priority,
                "severity": severity,
                "labels": labels,
            },
        )

    @srv.tool(name="tickets.update", description="Update a ticket")
    def tickets_update(
        ticket_id: str,
        description: str = None,
        assignee: str = None,
        priority: str = None,
        severity: str = None,
        labels: list = None,
    ) -> dict[str, Any]:
        try:
            return R().call_and_step(
                "tickets.update",
                {
                    "ticket_id": ticket_id,
                    "description": description,
                    "assignee": assignee,
                    "priority": priority,
                    "severity": severity,
                    "labels": labels,
                },
            )
        except MCPError as e:
            return {"error": {"code": e.code, "message": e.message}}

    @srv.tool(name="tickets.transition", description="Transition ticket status")
    def tickets_transition(ticket_id: str, status: str) -> dict[str, Any]:
        try:
            return R().call_and_step(
                "tickets.transition", {"ticket_id": ticket_id, "status": status}
            )
        except MCPError as e:
            return {"error": {"code": e.code, "message": e.message}}

    @srv.tool(name="tickets.add_comment", description="Add a comment to a ticket")
    def tickets_add_comment(
        ticket_id: str, body: str, author: str = "agent"
    ) -> dict[str, Any]:
        try:
            return R().call_and_step(
                "tickets.add_comment",
                {"ticket_id": ticket_id, "body": body, "author": author},
            )
        except MCPError as e:
            return {"error": {"code": e.code, "message": e.message}}

    @srv.tool(name="browser.read", description="Read current page")
    def browser_read() -> dict[str, Any]:
        return R().call_and_step("browser.read", {})

    @srv.tool(name="browser.back", description="Navigate back")
    def browser_back() -> dict[str, Any]:
        return R().call_and_step("browser.back", {})

    # --- ERP twin tools ---
    @srv.tool(name="erp.create_po", description="Create a purchase order (PO)")
    def erp_create_po(
        vendor: str, currency: str, lines: list[dict[str, Any]]
    ) -> dict[str, Any]:
        return _safe_call(
            "erp.create_po", {"vendor": vendor, "currency": currency, "lines": lines}
        )

    @srv.tool(name="erp.get_po", description="Get a PO by id")
    def erp_get_po(id: str) -> dict[str, Any]:
        return _safe_call("erp.get_po", {"id": id})

    @srv.tool(name="erp.list_pos", description="List all POs")
    def erp_list_pos(
        vendor: str = None,
        status: str = None,
        currency: str = None,
        limit: int = None,
        cursor: str = None,
        sort_by: str = "created_ms",
        sort_dir: str = "desc",
    ) -> dict[str, Any] | list[dict[str, Any]]:
        return R().call_and_step(  # type: ignore[return-value]
            "erp.list_pos",
            {
                "vendor": vendor,
                "status": status,
                "currency": currency,
                "limit": limit,
                "cursor": cursor,
                "sort_by": sort_by,
                "sort_dir": sort_dir,
            },
        )

    @srv.tool(name="erp.receive_goods", description="Receive goods against a PO")
    def erp_receive_goods(po_id: str, lines: list[dict[str, Any]]) -> dict[str, Any]:
        return _safe_call("erp.receive_goods", {"po_id": po_id, "lines": lines})

    @srv.tool(name="erp.submit_invoice", description="Submit an invoice for a PO")
    def erp_submit_invoice(
        vendor: str, po_id: str, lines: list[dict[str, Any]]
    ) -> dict[str, Any]:
        return _safe_call(
            "erp.submit_invoice", {"vendor": vendor, "po_id": po_id, "lines": lines}
        )

    @srv.tool(name="erp.get_invoice", description="Get invoice by id")
    def erp_get_invoice(id: str) -> dict[str, Any]:
        return _safe_call("erp.get_invoice", {"id": id})

    @srv.tool(name="erp.list_invoices", description="List invoices")
    def erp_list_invoices(
        status: str = None,
        vendor: str = None,
        po_id: str = None,
        limit: int = None,
        cursor: str = None,
        sort_by: str = "updated_ms",
        sort_dir: str = "desc",
    ) -> dict[str, Any] | list[dict[str, Any]]:
        return R().call_and_step(  # type: ignore[return-value]
            "erp.list_invoices",
            {
                "status": status,
                "vendor": vendor,
                "po_id": po_id,
                "limit": limit,
                "cursor": cursor,
                "sort_by": sort_by,
                "sort_dir": sort_dir,
            },
        )

    @srv.tool(
        name="erp.match_three_way",
        description="Three-way match PO vs receipt vs invoice",
    )
    def erp_match_three_way(
        po_id: str, invoice_id: str, receipt_id: str = None
    ) -> dict[str, Any]:
        return _safe_call(
            "erp.match_three_way",
            {"po_id": po_id, "invoice_id": invoice_id, "receipt_id": receipt_id},
        )

    @srv.tool(name="erp.post_payment", description="Post a payment against an invoice")
    def erp_post_payment(invoice_id: str, amount: float) -> dict[str, Any]:
        return _safe_call(
            "erp.post_payment", {"invoice_id": invoice_id, "amount": amount}
        )

    # --- CRM twin tools ---
    @srv.tool(name="crm.create_contact", description="Create a CRM contact")
    def crm_create_contact(
        email: str,
        first_name: str = None,
        last_name: str = None,
        do_not_contact: bool = False,
    ) -> dict[str, Any]:
        return _safe_call(
            "crm.create_contact",
            {
                "email": email,
                "first_name": first_name,
                "last_name": last_name,
                "do_not_contact": do_not_contact,
            },
        )

    @srv.tool(name="crm.get_contact", description="Get contact by id")
    def crm_get_contact(id: str) -> dict[str, Any]:
        return _safe_call("crm.get_contact", {"id": id})

    @srv.tool(name="crm.list_contacts", description="List contacts")
    def crm_list_contacts(
        query: str = None,
        company_id: str = None,
        do_not_contact: bool = None,
        limit: int = None,
        cursor: str = None,
        sort_by: str = "created_ms",
        sort_dir: str = "asc",
    ) -> dict[str, Any] | list[dict[str, Any]]:
        return R().call_and_step(  # type: ignore[return-value]
            "crm.list_contacts",
            {
                "query": query,
                "company_id": company_id,
                "do_not_contact": do_not_contact,
                "limit": limit,
                "cursor": cursor,
                "sort_by": sort_by,
                "sort_dir": sort_dir,
            },
        )

    @srv.tool(name="crm.create_company", description="Create a company")
    def crm_create_company(name: str, domain: str = None) -> dict[str, Any]:
        return _safe_call("crm.create_company", {"name": name, "domain": domain})

    @srv.tool(name="crm.get_company", description="Get company by id")
    def crm_get_company(id: str) -> dict[str, Any]:
        return _safe_call("crm.get_company", {"id": id})

    @srv.tool(name="crm.list_companies", description="List companies")
    def crm_list_companies(
        query: str = None,
        domain: str = None,
        limit: int = None,
        cursor: str = None,
        sort_by: str = "name",
        sort_dir: str = "asc",
    ) -> dict[str, Any] | list[dict[str, Any]]:
        return R().call_and_step(  # type: ignore[return-value]
            "crm.list_companies",
            {
                "query": query,
                "domain": domain,
                "limit": limit,
                "cursor": cursor,
                "sort_by": sort_by,
                "sort_dir": sort_dir,
            },
        )

    @srv.tool(
        name="crm.associate_contact_company",
        description="Associate contact and company",
    )
    def crm_associate_contact_company(
        contact_id: str, company_id: str
    ) -> dict[str, Any]:
        return _safe_call(
            "crm.associate_contact_company",
            {"contact_id": contact_id, "company_id": company_id},
        )

    @srv.tool(name="crm.create_deal", description="Create a deal")
    def crm_create_deal(
        name: str,
        amount: float,
        stage: str = "New",
        contact_id: str = None,
        company_id: str = None,
        close_date: str = None,
    ) -> dict[str, Any]:
        return _safe_call(
            "crm.create_deal",
            {
                "name": name,
                "amount": amount,
                "stage": stage,
                "contact_id": contact_id,
                "company_id": company_id,
                "close_date": close_date,
            },
        )

    @srv.tool(name="crm.get_deal", description="Get deal by id")
    def crm_get_deal(id: str) -> dict[str, Any]:
        return _safe_call("crm.get_deal", {"id": id})

    @srv.tool(name="crm.list_deals", description="List deals")
    def crm_list_deals(
        stage: str = None,
        company_id: str = None,
        min_amount: float = None,
        max_amount: float = None,
        limit: int = None,
        cursor: str = None,
        sort_by: str = "updated_ms",
        sort_dir: str = "desc",
    ) -> dict[str, Any] | list[dict[str, Any]]:
        return R().call_and_step(  # type: ignore[return-value]
            "crm.list_deals",
            {
                "stage": stage,
                "company_id": company_id,
                "min_amount": min_amount,
                "max_amount": max_amount,
                "limit": limit,
                "cursor": cursor,
                "sort_by": sort_by,
                "sort_dir": sort_dir,
            },
        )

    @srv.tool(name="crm.update_deal_stage", description="Update deal stage")
    def crm_update_deal_stage(id: str, stage: str) -> dict[str, Any]:
        return _safe_call("crm.update_deal_stage", {"id": id, "stage": stage})

    @srv.tool(name="crm.log_activity", description="Log activity (note/email_outreach)")
    def crm_log_activity(
        kind: str, contact_id: str = None, deal_id: str = None, note: str = None
    ) -> dict[str, Any]:
        return _safe_call(
            "crm.log_activity",
            {"kind": kind, "contact_id": contact_id, "deal_id": deal_id, "note": note},
        )

    @srv.tool(name="db.list_tables", description="List enterprise database tables")
    def db_list_tables(
        query: str = None,
        limit: int = None,
        cursor: str = None,
        sort_by: str = "table",
        sort_dir: str = "asc",
    ) -> dict[str, Any] | list[dict[str, Any]]:
        return R().call_and_step(  # type: ignore[return-value]
            "db.list_tables",
            {
                "query": query,
                "limit": limit,
                "cursor": cursor,
                "sort_by": sort_by,
                "sort_dir": sort_dir,
            },
        )

    @srv.tool(
        name="db.describe_table",
        description="Describe columns and row count for a table",
    )
    def db_describe_table(table: str) -> dict[str, Any]:
        return _safe_call("db.describe_table", {"table": table})

    @srv.tool(name="db.query", description="Query rows from an enterprise table")
    def db_query(
        table: str,
        filters: dict[str, Any] = None,
        columns: list[str] = None,
        limit: int = 20,
        offset: int = 0,
        cursor: str = None,
        sort_by: str = None,
        descending: bool = False,
    ) -> dict[str, Any]:
        return _safe_call(
            "db.query",
            {
                "table": table,
                "filters": filters,
                "columns": columns,
                "limit": limit,
                "offset": offset,
                "cursor": cursor,
                "sort_by": sort_by,
                "descending": descending,
            },
        )

    @srv.tool(name="db.upsert", description="Insert or update a table row")
    def db_upsert(table: str, row: dict[str, Any], key: str = "id") -> dict[str, Any]:
        return _safe_call("db.upsert", {"table": table, "row": row, "key": key})

    # --- ServiceDesk twin tools ---
    @srv.tool(name="servicedesk.list_incidents", description="List service incidents")
    def servicedesk_list_incidents(
        status: str = None,
        priority: str = None,
        query: str = None,
        assignee: str = None,
        limit: int = None,
        cursor: str = None,
        sort_by: str = "id",
        sort_dir: str = "asc",
    ) -> dict[str, Any]:
        return _safe_call(
            "servicedesk.list_incidents",
            {
                "status": status,
                "priority": priority,
                "query": query,
                "assignee": assignee,
                "limit": limit,
                "cursor": cursor,
                "sort_by": sort_by,
                "sort_dir": sort_dir,
            },
        )

    @srv.tool(name="servicedesk.get_incident", description="Get incident details")
    def servicedesk_get_incident(incident_id: str) -> dict[str, Any]:
        return _safe_call("servicedesk.get_incident", {"incident_id": incident_id})

    @srv.tool(name="servicedesk.update_incident", description="Update incident fields")
    def servicedesk_update_incident(
        incident_id: str,
        status: str = None,
        assignee: str = None,
        comment: str = None,
    ) -> dict[str, Any]:
        return _safe_call(
            "servicedesk.update_incident",
            {
                "incident_id": incident_id,
                "status": status,
                "assignee": assignee,
                "comment": comment,
            },
        )

    @srv.tool(name="servicedesk.list_requests", description="List service requests")
    def servicedesk_list_requests(
        status: str = None,
        requester: str = None,
        query: str = None,
        limit: int = None,
        cursor: str = None,
        sort_by: str = "id",
        sort_dir: str = "asc",
    ) -> dict[str, Any]:
        return _safe_call(
            "servicedesk.list_requests",
            {
                "status": status,
                "requester": requester,
                "query": query,
                "limit": limit,
                "cursor": cursor,
                "sort_by": sort_by,
                "sort_dir": sort_dir,
            },
        )

    @srv.tool(name="servicedesk.get_request", description="Get request details")
    def servicedesk_get_request(request_id: str) -> dict[str, Any]:
        return _safe_call("servicedesk.get_request", {"request_id": request_id})

    @srv.tool(name="servicedesk.update_request", description="Update request fields")
    def servicedesk_update_request(
        request_id: str,
        status: str = None,
        approval_stage: str = None,
        approval_status: str = None,
        comment: str = None,
    ) -> dict[str, Any]:
        return _safe_call(
            "servicedesk.update_request",
            {
                "request_id": request_id,
                "status": status,
                "approval_stage": approval_stage,
                "approval_status": approval_status,
                "comment": comment,
            },
        )

    # --- Okta twin tools ---
    @srv.tool(name="okta.list_users", description="List directory users")
    def okta_list_users(
        status: str = None,
        query: str = None,
        include_groups: bool = False,
        limit: int = None,
        cursor: str = None,
        sort_by: str = "email",
        sort_dir: str = "asc",
    ) -> dict[str, Any]:
        return _safe_call(
            "okta.list_users",
            {
                "status": status,
                "query": query,
                "include_groups": include_groups,
                "limit": limit,
                "cursor": cursor,
                "sort_by": sort_by,
                "sort_dir": sort_dir,
            },
        )

    @srv.tool(name="okta.get_user", description="Get a directory user")
    def okta_get_user(user_id: str) -> dict[str, Any]:
        return _safe_call("okta.get_user", {"user_id": user_id})

    @srv.tool(name="okta.activate_user", description="Activate user account")
    def okta_activate_user(user_id: str) -> dict[str, Any]:
        return _safe_call("okta.activate_user", {"user_id": user_id})

    @srv.tool(name="okta.deactivate_user", description="Deactivate user account")
    def okta_deactivate_user(user_id: str, reason: str = None) -> dict[str, Any]:
        return _safe_call(
            "okta.deactivate_user", {"user_id": user_id, "reason": reason}
        )

    @srv.tool(name="okta.suspend_user", description="Suspend user account")
    def okta_suspend_user(user_id: str, reason: str = None) -> dict[str, Any]:
        return _safe_call("okta.suspend_user", {"user_id": user_id, "reason": reason})

    @srv.tool(name="okta.unsuspend_user", description="Unsuspend user account")
    def okta_unsuspend_user(user_id: str) -> dict[str, Any]:
        return _safe_call("okta.unsuspend_user", {"user_id": user_id})

    @srv.tool(name="okta.reset_password", description="Generate reset token")
    def okta_reset_password(user_id: str) -> dict[str, Any]:
        return _safe_call("okta.reset_password", {"user_id": user_id})

    @srv.tool(name="okta.list_groups", description="List identity groups")
    def okta_list_groups(
        query: str = None,
        include_members: bool = False,
        limit: int = None,
        cursor: str = None,
        sort_by: str = "name",
        sort_dir: str = "asc",
    ) -> dict[str, Any]:
        return _safe_call(
            "okta.list_groups",
            {
                "query": query,
                "include_members": include_members,
                "limit": limit,
                "cursor": cursor,
                "sort_by": sort_by,
                "sort_dir": sort_dir,
            },
        )

    @srv.tool(name="okta.assign_group", description="Assign user to group")
    def okta_assign_group(user_id: str, group_id: str) -> dict[str, Any]:
        return _safe_call(
            "okta.assign_group", {"user_id": user_id, "group_id": group_id}
        )

    @srv.tool(name="okta.unassign_group", description="Unassign user from group")
    def okta_unassign_group(user_id: str, group_id: str) -> dict[str, Any]:
        return _safe_call(
            "okta.unassign_group", {"user_id": user_id, "group_id": group_id}
        )

    @srv.tool(name="okta.list_applications", description="List SSO applications")
    def okta_list_applications(
        query: str = None,
        limit: int = None,
        cursor: str = None,
        sort_by: str = "label",
        sort_dir: str = "asc",
    ) -> dict[str, Any]:
        return _safe_call(
            "okta.list_applications",
            {
                "query": query,
                "limit": limit,
                "cursor": cursor,
                "sort_by": sort_by,
                "sort_dir": sort_dir,
            },
        )

    @srv.tool(name="okta.assign_application", description="Assign app to user")
    def okta_assign_application(user_id: str, app_id: str) -> dict[str, Any]:
        return _safe_call(
            "okta.assign_application", {"user_id": user_id, "app_id": app_id}
        )

    @srv.tool(name="okta.unassign_application", description="Unassign app from user")
    def okta_unassign_application(user_id: str, app_id: str) -> dict[str, Any]:
        return _safe_call(
            "okta.unassign_application", {"user_id": user_id, "app_id": app_id}
        )

    # --- Configurable alias packs (ERP) ---
    packs_env = os.environ.get("VEI_ALIAS_PACKS", "xero").strip()
    packs = [p.strip() for p in packs_env.split(",") if p.strip()]

    def _register_alias(alias_name: str, base_tool: str) -> None:
        # Register a thin passthrough tool dynamically
        @srv.tool(name=alias_name, description=f"Alias → {base_tool}")
        def _alias_passthrough(**kwargs: Any) -> dict[str, Any]:  # type: ignore[no-redef]
            return _safe_call(base_tool, dict(kwargs))

        base_spec = R().registry.get(base_tool)
        if base_spec:
            alias_spec = ToolSpec(
                name=alias_name,
                description=f"Alias → {base_tool}. {base_spec.description}",
                side_effects=base_spec.side_effects,
                permissions=base_spec.permissions,
                default_latency_ms=base_spec.default_latency_ms,
                latency_jitter_ms=base_spec.latency_jitter_ms,
                nominal_cost=base_spec.nominal_cost,
                returns=base_spec.returns,
                fault_probability=base_spec.fault_probability,
            )
        else:
            alias_spec = ToolSpec(name=alias_name, description=f"Alias → {base_tool}")
        try:
            R().registry.register(alias_spec)
        except ValueError:
            pass

    for pack in packs:
        for alias, base in ERP_ALIAS_PACKS.get(pack, []):
            _register_alias(alias, base)

    # CRM alias packs
    crm_packs_env = os.environ.get("VEI_CRM_ALIAS_PACKS", "hubspot,salesforce").strip()
    crm_packs = [p.strip() for p in crm_packs_env.split(",") if p.strip()]
    for pack in crm_packs:
        for alias, base in CRM_ALIAS_PACKS.get(pack, []):
            _register_alias(alias, base)

    def _register_passthrough(tool_name: str, description: str) -> None:
        @srv.tool(name=tool_name, description=description)
        def _provider_passthrough(**kwargs: Any) -> dict[str, Any]:  # type: ignore[no-redef]
            return _safe_call(tool_name, dict(kwargs))

    dynamic_prefixes = (
        "google_admin.",
        "siem.",
        "datadog.",
        "pagerduty.",
        "feature_flags.",
        "hris.",
        "jira.",
    )
    plugin_prefixes: list[str] = list(dynamic_prefixes)
    for plugin in list_runtime_facade_plugins():
        if plugin.provider_factory is None:
            continue
        for prefix in plugin.tool_prefixes:
            if prefix not in plugin_prefixes:
                plugin_prefixes.append(prefix)
    for spec in sorted(R().registry.list(), key=lambda item: item.name):
        if spec.name.startswith(tuple(plugin_prefixes)):
            _register_passthrough(spec.name, spec.description)

    @srv.tool(
        name="vei.observe", description="Get current observation summary + action menu"
    )
    def vei_observe(focus: str = None) -> dict[str, Any]:
        return R().observe(focus_hint=focus).model_dump()

    @srv.tool(name="vei.ping", description="Health check and current logical time")
    def vei_ping() -> dict[str, Any]:
        return {"ok": True, "time_ms": R().bus.clock_ms}

    @srv.tool(
        name="vei.reset",
        description="Reset the simulation deterministically (optionally with a new seed)",
    )
    def vei_reset(seed: int = None) -> dict[str, Any]:
        old = R()
        new_seed = (
            int(seed) if seed is not None else int(os.environ.get("VEI_SEED", "42042"))
        )
        # Preserve scenario and artifacts configuration so the environment stays consistent for the session
        new_router = Router(
            seed=new_seed, artifacts_dir=old.trace.out_dir, scenario=old.scenario
        )
        holder.router = new_router
        return {"ok": True, "seed": new_seed, "time_ms": new_router.bus.clock_ms}

    @srv.tool(
        name="vei.act_and_observe",
        description="Execute a tool and return its result and a post-action observation",
    )
    def vei_act_and_observe(
        tool: str, args: dict[str, Any] = Field(default_factory=dict)
    ) -> dict[str, Any]:
        data = R().act_and_observe(tool, args)
        return data

    @srv.tool(
        name="vei.call", description="Call any tool name with args via the VEI router"
    )
    def vei_call(
        tool: str, args: dict[str, Any] = Field(default_factory=dict)
    ) -> dict[str, Any]:
        try:
            return R().call_and_step(tool, args)
        except MCPError as e:
            return {"error": {"code": e.code, "message": e.message}}

    @srv.tool(
        name="vei.tools.search",
        description="Search the tool catalog for relevant entries",
    )
    def vei_tools_search(query: str, top_k: int = 10) -> dict[str, Any]:
        limit = top_k if isinstance(top_k, int) else 10
        if limit < 0:
            limit = 0
        return R().search_tools(query, top_k=limit)

    @srv.tool(
        name="vei.tick",
        description="Advance logical time by dt_ms and deliver due events",
    )
    def vei_tick(dt_ms: int = 1000) -> dict[str, Any]:
        return R().tick(dt_ms)

    @srv.tool(
        name="vei.pending",
        description="Return pending event counts without advancing time",
    )
    def vei_pending() -> dict[str, int]:
        return R().pending()

    @srv.tool(
        name="vei.state",
        description="Inspect state head, receipts, and recent tool calls",
    )
    def vei_state(
        include_state: bool = False, tool_tail: int = 20, include_receipts: bool = True
    ) -> dict[str, Any]:
        return R().state_snapshot(
            include_state=include_state,
            tool_tail=tool_tail,
            include_receipts=include_receipts,
        )

    @srv.tool(
        name="vei.help",
        description="Usage help: how to interact via MCP and example actions",
    )
    def vei_help() -> dict[str, Any]:
        return R().help_payload()

    return srv
