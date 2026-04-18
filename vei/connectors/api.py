from __future__ import annotations

from dataclasses import dataclass
from typing import Mapping, Protocol

from .models import (
    AdapterMode,
    ConnectorRequest,
    ConnectorResult,
    OperationClass,
    PolicyDecision,
    ServiceName,
    ConnectorReceipt,
)

_BOUNDARY_EXPORTS = (ConnectorReceipt,)


class ConnectorAdapter(Protocol):
    """Typed connector contract implemented by sim/replay/live adapters."""

    def execute(self, request: ConnectorRequest) -> ConnectorResult: ...


class PolicyGate(Protocol):
    """Typed policy gate used before any adapter invocation."""

    def evaluate(
        self, request: ConnectorRequest, mode: AdapterMode
    ) -> PolicyDecision: ...


@dataclass(frozen=True)
class ToolRoute:
    service: ServiceName
    operation: str
    operation_class: OperationClass


@dataclass
class AdapterTriplet:
    sim: ConnectorAdapter
    replay: ConnectorAdapter
    live: ConnectorAdapter

    def for_mode(self, mode: AdapterMode) -> ConnectorAdapter:
        if mode == AdapterMode.REPLAY:
            return self.replay
        if mode == AdapterMode.LIVE:
            return self.live
        return self.sim


TOOL_ROUTES: Mapping[str, ToolRoute] = {
    "slack.list_channels": ToolRoute(
        service=ServiceName.SLACK,
        operation="list_channels",
        operation_class=OperationClass.READ,
    ),
    "slack.open_channel": ToolRoute(
        service=ServiceName.SLACK,
        operation="open_channel",
        operation_class=OperationClass.READ,
    ),
    "slack.fetch_thread": ToolRoute(
        service=ServiceName.SLACK,
        operation="fetch_thread",
        operation_class=OperationClass.READ,
    ),
    "slack.send_message": ToolRoute(
        service=ServiceName.SLACK,
        operation="send_message",
        operation_class=OperationClass.WRITE_SAFE,
    ),
    "slack.react": ToolRoute(
        service=ServiceName.SLACK,
        operation="react",
        operation_class=OperationClass.WRITE_SAFE,
    ),
    "mail.list": ToolRoute(
        service=ServiceName.MAIL,
        operation="list",
        operation_class=OperationClass.READ,
    ),
    "mail.open": ToolRoute(
        service=ServiceName.MAIL,
        operation="open",
        operation_class=OperationClass.READ,
    ),
    "mail.compose": ToolRoute(
        service=ServiceName.MAIL,
        operation="compose",
        operation_class=OperationClass.WRITE_SAFE,
    ),
    "mail.reply": ToolRoute(
        service=ServiceName.MAIL,
        operation="reply",
        operation_class=OperationClass.WRITE_SAFE,
    ),
    "docs.list": ToolRoute(
        service=ServiceName.DOCS,
        operation="list",
        operation_class=OperationClass.READ,
    ),
    "docs.read": ToolRoute(
        service=ServiceName.DOCS,
        operation="read",
        operation_class=OperationClass.READ,
    ),
    "docs.search": ToolRoute(
        service=ServiceName.DOCS,
        operation="search",
        operation_class=OperationClass.READ,
    ),
    "docs.create": ToolRoute(
        service=ServiceName.DOCS,
        operation="create",
        operation_class=OperationClass.WRITE_SAFE,
    ),
    "docs.update": ToolRoute(
        service=ServiceName.DOCS,
        operation="update",
        operation_class=OperationClass.WRITE_SAFE,
    ),
    "calendar.list_events": ToolRoute(
        service=ServiceName.CALENDAR,
        operation="list_events",
        operation_class=OperationClass.READ,
    ),
    "calendar.create_event": ToolRoute(
        service=ServiceName.CALENDAR,
        operation="create_event",
        operation_class=OperationClass.WRITE_SAFE,
    ),
    "calendar.accept": ToolRoute(
        service=ServiceName.CALENDAR,
        operation="accept",
        operation_class=OperationClass.WRITE_SAFE,
    ),
    "calendar.decline": ToolRoute(
        service=ServiceName.CALENDAR,
        operation="decline",
        operation_class=OperationClass.WRITE_SAFE,
    ),
    "calendar.update_event": ToolRoute(
        service=ServiceName.CALENDAR,
        operation="update_event",
        operation_class=OperationClass.WRITE_SAFE,
    ),
    "calendar.cancel_event": ToolRoute(
        service=ServiceName.CALENDAR,
        operation="cancel_event",
        operation_class=OperationClass.WRITE_RISKY,
    ),
    "tickets.list": ToolRoute(
        service=ServiceName.TICKETS,
        operation="list",
        operation_class=OperationClass.READ,
    ),
    "tickets.get": ToolRoute(
        service=ServiceName.TICKETS,
        operation="get",
        operation_class=OperationClass.READ,
    ),
    "tickets.create": ToolRoute(
        service=ServiceName.TICKETS,
        operation="create",
        operation_class=OperationClass.WRITE_SAFE,
    ),
    "tickets.update": ToolRoute(
        service=ServiceName.TICKETS,
        operation="update",
        operation_class=OperationClass.WRITE_SAFE,
    ),
    "tickets.transition": ToolRoute(
        service=ServiceName.TICKETS,
        operation="transition",
        operation_class=OperationClass.WRITE_RISKY,
    ),
    "tickets.add_comment": ToolRoute(
        service=ServiceName.TICKETS,
        operation="add_comment",
        operation_class=OperationClass.WRITE_SAFE,
    ),
    "db.list_tables": ToolRoute(
        service=ServiceName.DB,
        operation="list_tables",
        operation_class=OperationClass.READ,
    ),
    "db.describe_table": ToolRoute(
        service=ServiceName.DB,
        operation="describe_table",
        operation_class=OperationClass.READ,
    ),
    "db.query": ToolRoute(
        service=ServiceName.DB,
        operation="query",
        operation_class=OperationClass.READ,
    ),
    "db.upsert": ToolRoute(
        service=ServiceName.DB,
        operation="upsert",
        operation_class=OperationClass.WRITE_SAFE,
    ),
    "erp.create_po": ToolRoute(
        service=ServiceName.ERP,
        operation="create_po",
        operation_class=OperationClass.WRITE_SAFE,
    ),
    "erp.get_po": ToolRoute(
        service=ServiceName.ERP,
        operation="get_po",
        operation_class=OperationClass.READ,
    ),
    "erp.list_pos": ToolRoute(
        service=ServiceName.ERP,
        operation="list_pos",
        operation_class=OperationClass.READ,
    ),
    "erp.receive_goods": ToolRoute(
        service=ServiceName.ERP,
        operation="receive_goods",
        operation_class=OperationClass.WRITE_SAFE,
    ),
    "erp.submit_invoice": ToolRoute(
        service=ServiceName.ERP,
        operation="submit_invoice",
        operation_class=OperationClass.WRITE_SAFE,
    ),
    "erp.get_invoice": ToolRoute(
        service=ServiceName.ERP,
        operation="get_invoice",
        operation_class=OperationClass.READ,
    ),
    "erp.list_invoices": ToolRoute(
        service=ServiceName.ERP,
        operation="list_invoices",
        operation_class=OperationClass.READ,
    ),
    "erp.match_three_way": ToolRoute(
        service=ServiceName.ERP,
        operation="match_three_way",
        operation_class=OperationClass.WRITE_SAFE,
    ),
    "erp.post_payment": ToolRoute(
        service=ServiceName.ERP,
        operation="post_payment",
        operation_class=OperationClass.WRITE_RISKY,
    ),
    "crm.create_contact": ToolRoute(
        service=ServiceName.CRM,
        operation="create_contact",
        operation_class=OperationClass.WRITE_SAFE,
    ),
    "crm.get_contact": ToolRoute(
        service=ServiceName.CRM,
        operation="get_contact",
        operation_class=OperationClass.READ,
    ),
    "crm.list_contacts": ToolRoute(
        service=ServiceName.CRM,
        operation="list_contacts",
        operation_class=OperationClass.READ,
    ),
    "crm.create_company": ToolRoute(
        service=ServiceName.CRM,
        operation="create_company",
        operation_class=OperationClass.WRITE_SAFE,
    ),
    "crm.get_company": ToolRoute(
        service=ServiceName.CRM,
        operation="get_company",
        operation_class=OperationClass.READ,
    ),
    "crm.list_companies": ToolRoute(
        service=ServiceName.CRM,
        operation="list_companies",
        operation_class=OperationClass.READ,
    ),
    "crm.associate_contact_company": ToolRoute(
        service=ServiceName.CRM,
        operation="associate_contact_company",
        operation_class=OperationClass.WRITE_SAFE,
    ),
    "crm.create_deal": ToolRoute(
        service=ServiceName.CRM,
        operation="create_deal",
        operation_class=OperationClass.WRITE_SAFE,
    ),
    "crm.get_deal": ToolRoute(
        service=ServiceName.CRM,
        operation="get_deal",
        operation_class=OperationClass.READ,
    ),
    "crm.list_deals": ToolRoute(
        service=ServiceName.CRM,
        operation="list_deals",
        operation_class=OperationClass.READ,
    ),
    "crm.update_deal_stage": ToolRoute(
        service=ServiceName.CRM,
        operation="update_deal_stage",
        operation_class=OperationClass.WRITE_SAFE,
    ),
    "crm.log_activity": ToolRoute(
        service=ServiceName.CRM,
        operation="log_activity",
        operation_class=OperationClass.WRITE_SAFE,
    ),
    "okta.list_users": ToolRoute(
        service=ServiceName.OKTA,
        operation="list_users",
        operation_class=OperationClass.READ,
    ),
    "okta.get_user": ToolRoute(
        service=ServiceName.OKTA,
        operation="get_user",
        operation_class=OperationClass.READ,
    ),
    "okta.activate_user": ToolRoute(
        service=ServiceName.OKTA,
        operation="activate_user",
        operation_class=OperationClass.WRITE_SAFE,
    ),
    "okta.deactivate_user": ToolRoute(
        service=ServiceName.OKTA,
        operation="deactivate_user",
        operation_class=OperationClass.WRITE_RISKY,
    ),
    "okta.suspend_user": ToolRoute(
        service=ServiceName.OKTA,
        operation="suspend_user",
        operation_class=OperationClass.WRITE_SAFE,
    ),
    "okta.unsuspend_user": ToolRoute(
        service=ServiceName.OKTA,
        operation="unsuspend_user",
        operation_class=OperationClass.WRITE_SAFE,
    ),
    "okta.reset_password": ToolRoute(
        service=ServiceName.OKTA,
        operation="reset_password",
        operation_class=OperationClass.WRITE_SAFE,
    ),
    "okta.list_groups": ToolRoute(
        service=ServiceName.OKTA,
        operation="list_groups",
        operation_class=OperationClass.READ,
    ),
    "okta.assign_group": ToolRoute(
        service=ServiceName.OKTA,
        operation="assign_group",
        operation_class=OperationClass.WRITE_SAFE,
    ),
    "okta.unassign_group": ToolRoute(
        service=ServiceName.OKTA,
        operation="unassign_group",
        operation_class=OperationClass.WRITE_SAFE,
    ),
    "okta.list_applications": ToolRoute(
        service=ServiceName.OKTA,
        operation="list_applications",
        operation_class=OperationClass.READ,
    ),
    "okta.assign_application": ToolRoute(
        service=ServiceName.OKTA,
        operation="assign_application",
        operation_class=OperationClass.WRITE_SAFE,
    ),
    "okta.unassign_application": ToolRoute(
        service=ServiceName.OKTA,
        operation="unassign_application",
        operation_class=OperationClass.WRITE_SAFE,
    ),
    "servicedesk.list_incidents": ToolRoute(
        service=ServiceName.SERVICEDESK,
        operation="list_incidents",
        operation_class=OperationClass.READ,
    ),
    "servicedesk.get_incident": ToolRoute(
        service=ServiceName.SERVICEDESK,
        operation="get_incident",
        operation_class=OperationClass.READ,
    ),
    "servicedesk.update_incident": ToolRoute(
        service=ServiceName.SERVICEDESK,
        operation="update_incident",
        operation_class=OperationClass.WRITE_SAFE,
    ),
    "servicedesk.list_requests": ToolRoute(
        service=ServiceName.SERVICEDESK,
        operation="list_requests",
        operation_class=OperationClass.READ,
    ),
    "servicedesk.get_request": ToolRoute(
        service=ServiceName.SERVICEDESK,
        operation="get_request",
        operation_class=OperationClass.READ,
    ),
    "servicedesk.update_request": ToolRoute(
        service=ServiceName.SERVICEDESK,
        operation="update_request",
        operation_class=OperationClass.WRITE_SAFE,
    ),
}


def parse_adapter_mode(raw: str | None) -> AdapterMode:
    value = (raw or AdapterMode.SIM.value).strip().lower()
    for mode in AdapterMode:
        if mode.value == value:
            return mode
    return AdapterMode.SIM


def managed_tool(tool: str) -> bool:
    return tool in TOOL_ROUTES


# Public runtime/factory API.
from .runtime import ConnectorRuntime, create_default_runtime  # noqa: E402

__all__ = [
    "AdapterTriplet",
    "ConnectorAdapter",
    "ConnectorRuntime",
    "PolicyGate",
    "TOOL_ROUTES",
    "ToolRoute",
    "create_default_runtime",
    "managed_tool",
    "parse_adapter_mode",
]
