from __future__ import annotations

from typing import Any, Dict, Optional

from vei.world.models import WorldState

from .models import (
    CommGraphChannelView,
    CommGraphView,
    DocGraphView,
    DocumentView,
    DriveShareView,
    HrisEmployeeView,
    IdentityApplicationView,
    IdentityGraphView,
    IdentityGroupView,
    IdentityPolicyView,
    IdentityUserView,
    RevenueCompanyView,
    RevenueContactView,
    RevenueDealView,
    RevenueGraphView,
    RuntimeCapabilityGraphs,
    ServiceRequestView,
    WorkGraphView,
    WorkItemView,
)


def build_runtime_capability_graphs(state: WorldState) -> RuntimeCapabilityGraphs:
    graph_seed = _builder_capability_graphs(state)
    comm_graph = _build_comm_graph(state.components)
    doc_graph = _build_doc_graph(state.components)
    work_graph = _build_work_graph(state.components)
    identity_graph = _build_identity_graph(state.components, graph_seed)
    revenue_graph = _build_revenue_graph(state.components)

    available_domains = [
        domain
        for domain, graph in (
            ("comm_graph", comm_graph),
            ("doc_graph", doc_graph),
            ("work_graph", work_graph),
            ("identity_graph", identity_graph),
            ("revenue_graph", revenue_graph),
        )
        if graph is not None
    ]
    return RuntimeCapabilityGraphs(
        branch=state.branch,
        clock_ms=state.clock_ms,
        available_domains=available_domains,
        comm_graph=comm_graph,
        doc_graph=doc_graph,
        work_graph=work_graph,
        identity_graph=identity_graph,
        revenue_graph=revenue_graph,
        metadata={
            "scenario_name": state.scenario.get("name"),
            "builder_mode": _scenario_metadata(state).get("builder_mode"),
            "scenario_template_name": _scenario_metadata(state).get(
                "scenario_template_name"
            ),
        },
    )


def get_runtime_capability_graph(
    state: WorldState, domain: str
) -> Optional[
    CommGraphView | DocGraphView | WorkGraphView | IdentityGraphView | RevenueGraphView
]:
    graphs = build_runtime_capability_graphs(state)
    normalized = domain.strip().lower()
    if normalized == "comm_graph":
        return graphs.comm_graph
    if normalized == "doc_graph":
        return graphs.doc_graph
    if normalized == "work_graph":
        return graphs.work_graph
    if normalized == "identity_graph":
        return graphs.identity_graph
    if normalized == "revenue_graph":
        return graphs.revenue_graph
    raise KeyError(f"unknown capability graph domain: {domain}")


def _build_comm_graph(components: Dict[str, Dict[str, Any]]) -> Optional[CommGraphView]:
    slack = components.get("slack", {})
    mail = components.get("mail", {})
    channels = []
    for channel_name, payload in sorted((slack.get("channels") or {}).items()):
        messages = payload.get("messages") or []
        latest_text = messages[-1].get("text") if messages else None
        channels.append(
            CommGraphChannelView(
                channel=str(channel_name),
                unread=int(payload.get("unread", 0) or 0),
                message_count=len(messages),
                latest_text=latest_text,
            )
        )
    inbox = list(mail.get("inbox") or [])
    if not channels and not inbox:
        return None
    return CommGraphView(channels=channels, inbox_count=len(inbox))


def _build_doc_graph(components: Dict[str, Dict[str, Any]]) -> Optional[DocGraphView]:
    docs_component = components.get("docs", {})
    admin = components.get("google_admin", {})
    documents = [
        DocumentView(
            doc_id=str(doc_id),
            title=str(payload.get("title", doc_id)),
            tags=[str(tag) for tag in (payload.get("tags") or [])],
        )
        for doc_id, payload in sorted((docs_component.get("docs") or {}).items())
    ]
    drive_shares = [
        DriveShareView(
            doc_id=str(doc_id),
            title=str(payload.get("title", doc_id)),
            owner=str(payload.get("owner", "")),
            visibility=str(payload.get("visibility", "internal")),
            classification=str(payload.get("classification", "internal")),
            shared_with=[str(item) for item in (payload.get("shared_with") or [])],
        )
        for doc_id, payload in sorted((admin.get("drive_shares") or {}).items())
    ]
    if not documents and not drive_shares:
        return None
    return DocGraphView(documents=documents, drive_shares=drive_shares)


def _build_work_graph(components: Dict[str, Dict[str, Any]]) -> Optional[WorkGraphView]:
    tickets_component = components.get("tickets", {})
    servicedesk = components.get("servicedesk", {})
    tickets = [
        WorkItemView(
            item_id=str(ticket_id),
            title=str(payload.get("title", ticket_id)),
            status=str(payload.get("status", "unknown")),
            assignee=_optional_str(payload.get("assignee")),
            kind="ticket",
        )
        for ticket_id, payload in sorted(
            (tickets_component.get("tickets") or {}).items()
        )
    ]
    service_requests = [
        ServiceRequestView(
            request_id=str(request_id),
            title=str(payload.get("title", request_id)),
            status=str(payload.get("status", "unknown")),
            requester=_optional_str(payload.get("requester")),
            approval_stages=[
                str(item.get("stage"))
                for item in (payload.get("approvals") or [])
                if item.get("stage") is not None
            ],
        )
        for request_id, payload in sorted((servicedesk.get("requests") or {}).items())
    ]
    incidents = [
        WorkItemView(
            item_id=str(incident_id),
            title=str(payload.get("title", incident_id)),
            status=str(payload.get("status", "unknown")),
            assignee=_optional_str(payload.get("assignee")),
            kind="incident",
        )
        for incident_id, payload in sorted((servicedesk.get("incidents") or {}).items())
    ]
    if not tickets and not service_requests and not incidents:
        return None
    return WorkGraphView(
        tickets=tickets, service_requests=service_requests, incidents=incidents
    )


def _build_identity_graph(
    components: Dict[str, Dict[str, Any]], graph_seed: Dict[str, Any]
) -> Optional[IdentityGraphView]:
    okta = components.get("okta", {})
    hris = components.get("hris", {})
    identity_seed = graph_seed.get("identity_graph") or {}
    policies = [
        IdentityPolicyView(
            policy_id=str(policy.get("policy_id", "")),
            title=str(policy.get("title", "")),
            allowed_application_ids=[
                str(item) for item in (policy.get("allowed_application_ids") or [])
            ],
            forbidden_share_domains=[
                str(item) for item in (policy.get("forbidden_share_domains") or [])
            ],
            required_approval_stages=[
                str(item) for item in (policy.get("required_approval_stages") or [])
            ],
            deadline_max_ms=(
                int(policy["deadline_max_ms"])
                if policy.get("deadline_max_ms") is not None
                else None
            ),
        )
        for policy in identity_seed.get("policies", [])
    ]
    users = [
        IdentityUserView(
            user_id=str(user_id),
            email=str(payload.get("email", "")),
            display_name=_optional_str(payload.get("display_name"))
            or _full_name(payload),
            status=str(payload.get("status", "UNKNOWN")),
            groups=[str(item) for item in (payload.get("groups") or [])],
            applications=[str(item) for item in (payload.get("applications") or [])],
        )
        for user_id, payload in sorted((okta.get("users") or {}).items())
    ]
    groups = [
        IdentityGroupView(
            group_id=str(group_id),
            name=str(payload.get("name", group_id)),
            members=[str(item) for item in (payload.get("members") or [])],
        )
        for group_id, payload in sorted((okta.get("groups") or {}).items())
    ]
    applications = [
        IdentityApplicationView(
            app_id=str(app_id),
            label=str(payload.get("label", app_id)),
            status=str(payload.get("status", "UNKNOWN")),
            assignments=[str(item) for item in (payload.get("assignments") or [])],
        )
        for app_id, payload in sorted((okta.get("apps") or {}).items())
    ]
    employees = [
        HrisEmployeeView(
            employee_id=str(employee_id),
            email=str(payload.get("email", "")),
            display_name=str(payload.get("display_name", employee_id)),
            department=str(payload.get("department", "")),
            manager=str(payload.get("manager", "")),
            status=str(payload.get("status", "unknown")),
            identity_conflict=bool(payload.get("identity_conflict", False)),
            onboarded=bool(payload.get("onboarded", False)),
        )
        for employee_id, payload in sorted((hris.get("employees") or {}).items())
    ]
    if not users and not groups and not applications and not employees and not policies:
        return None
    return IdentityGraphView(
        users=users,
        groups=groups,
        applications=applications,
        hris_employees=employees,
        policies=policies,
    )


def _build_revenue_graph(
    components: Dict[str, Dict[str, Any]]
) -> Optional[RevenueGraphView]:
    crm = components.get("crm", {})
    companies = [
        RevenueCompanyView(
            company_id=str(company_id),
            name=str(payload.get("name", company_id)),
            domain=str(payload.get("domain", "")),
        )
        for company_id, payload in sorted((crm.get("companies") or {}).items())
    ]
    contacts = [
        RevenueContactView(
            contact_id=str(contact_id),
            email=str(payload.get("email", "")),
            full_name=_full_name(payload) or str(payload.get("email", contact_id)),
            company_id=_optional_str(payload.get("company_id")),
        )
        for contact_id, payload in sorted((crm.get("contacts") or {}).items())
    ]
    deals = [
        RevenueDealView(
            deal_id=str(deal_id),
            name=str(payload.get("name", deal_id)),
            amount=float(payload.get("amount", 0.0) or 0.0),
            stage=str(payload.get("stage", "unknown")),
            owner=str(payload.get("owner", "")),
            company_id=_optional_str(payload.get("company_id")),
            contact_id=_optional_str(payload.get("contact_id")),
        )
        for deal_id, payload in sorted((crm.get("deals") or {}).items())
    ]
    if not companies and not contacts and not deals:
        return None
    return RevenueGraphView(companies=companies, contacts=contacts, deals=deals)


def _scenario_metadata(state: WorldState) -> Dict[str, Any]:
    scenario = state.scenario or {}
    metadata = scenario.get("metadata")
    return metadata if isinstance(metadata, dict) else {}


def _builder_capability_graphs(state: WorldState) -> Dict[str, Any]:
    metadata = _scenario_metadata(state)
    raw = metadata.get("builder_capability_graphs")
    return raw if isinstance(raw, dict) else {}


def _optional_str(value: Any) -> Optional[str]:
    if value is None:
        return None
    text = str(value)
    return text if text else None


def _full_name(payload: Dict[str, Any]) -> Optional[str]:
    first = _optional_str(payload.get("first_name"))
    last = _optional_str(payload.get("last_name"))
    if first and last:
        return f"{first} {last}"
    return first or last


__all__ = [
    "build_runtime_capability_graphs",
    "get_runtime_capability_graph",
]
