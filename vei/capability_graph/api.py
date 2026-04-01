from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Iterable, Optional

from vei.blueprint.models import CapabilityDomain
from vei.world.models import WorldState

from .models import (
    AllocationView,
    CampaignApprovalView,
    CampaignGraphView,
    CampaignReportView,
    CampaignView,
    CapacityPoolView,
    ClientView,
    CapabilityGraphActionInput,
    CapabilityGraphActionSchema,
    CapabilityGraphPlan,
    CapabilityGraphPlanStep,
    CommGraphChannelView,
    CommGraphView,
    DataGraphView,
    DataWorkbookView,
    DocGraphView,
    DocumentView,
    DriveShareView,
    HrisEmployeeView,
    IdentityApplicationView,
    IdentityGraphView,
    IdentityGroupView,
    IdentityPolicyView,
    IdentityUserView,
    InventoryGraphView,
    ObsGraphView,
    ObsIncidentView,
    ObsMonitorView,
    ObsServiceView,
    OpsAppointmentView,
    OpsBillingCaseView,
    OpsCustomerView,
    OpsExceptionView,
    OpsFlagView,
    OpsGraphView,
    OpsPolicyView,
    OpsTechnicianView,
    OpsWorkOrderView,
    OrderView,
    LeaseView,
    PropertyGraphView,
    PropertyView,
    QuoteView,
    RevenueCompanyView,
    RevenueContactView,
    RevenueDealView,
    RevenueGraphView,
    RuntimeCapabilityGraphs,
    ServiceRequestView,
    SiteView,
    UnitView,
    VendorView,
    WorkGraphView,
    WorkOrderGraphView,
    WorkItemView,
    CreativeView,
)


@dataclass(frozen=True)
class _ResolvedGraphAction:
    domain: CapabilityDomain
    action: str
    tool: str
    args: Dict[str, Any]
    title: str
    step_id: Optional[str] = None


def build_runtime_capability_graphs(state: WorldState) -> RuntimeCapabilityGraphs:
    graph_seed = _builder_capability_graphs(state)
    allowed_domains = _allowed_domains(state)
    comm_graph = _build_comm_graph(state.components)
    doc_graph = _build_doc_graph(state.components)
    work_graph = _build_work_graph(state.components)
    identity_graph = _build_identity_graph(state.components, graph_seed)
    revenue_graph = _build_revenue_graph(state.components)
    data_graph = _build_data_graph(state.components)
    obs_graph = _build_obs_graph(state.components)
    ops_graph = _build_ops_graph(state.components)
    property_graph = _build_property_graph(state.components)
    campaign_graph = _build_campaign_graph(state.components)
    inventory_graph = _build_inventory_graph(state.components)

    if allowed_domains is not None:
        if "comm_graph" not in allowed_domains:
            comm_graph = None
        if "doc_graph" not in allowed_domains:
            doc_graph = None
        if "work_graph" not in allowed_domains:
            work_graph = None
        if "identity_graph" not in allowed_domains:
            identity_graph = None
        if "revenue_graph" not in allowed_domains:
            revenue_graph = None
        if "data_graph" not in allowed_domains:
            data_graph = None
        if "obs_graph" not in allowed_domains:
            obs_graph = None
        if "ops_graph" not in allowed_domains:
            ops_graph = None
        if "property_graph" not in allowed_domains:
            property_graph = None
        if "campaign_graph" not in allowed_domains:
            campaign_graph = None
        if "inventory_graph" not in allowed_domains:
            inventory_graph = None

    available_domains_list = [
        domain
        for domain, graph in (
            ("comm_graph", comm_graph),
            ("doc_graph", doc_graph),
            ("work_graph", work_graph),
            ("identity_graph", identity_graph),
            ("revenue_graph", revenue_graph),
            ("data_graph", data_graph),
            ("obs_graph", obs_graph),
            ("ops_graph", ops_graph),
            ("property_graph", property_graph),
            ("campaign_graph", campaign_graph),
            ("inventory_graph", inventory_graph),
        )
        if graph is not None
    ]
    return RuntimeCapabilityGraphs(
        branch=state.branch,
        clock_ms=state.clock_ms,
        available_domains=available_domains_list,
        comm_graph=comm_graph,
        doc_graph=doc_graph,
        work_graph=work_graph,
        identity_graph=identity_graph,
        revenue_graph=revenue_graph,
        data_graph=data_graph,
        obs_graph=obs_graph,
        ops_graph=ops_graph,
        property_graph=property_graph,
        campaign_graph=campaign_graph,
        inventory_graph=inventory_graph,
        metadata={
            "scenario_name": state.scenario.get("name"),
            "builder_mode": _scenario_metadata(state).get("builder_mode"),
            "scenario_template_name": _scenario_metadata(state).get(
                "scenario_template_name"
            ),
            "organization_domain": _optional_str(graph_seed.get("organization_domain")),
        },
    )


def get_runtime_capability_graph(state: WorldState, domain: str) -> Optional[Any]:
    graphs = build_runtime_capability_graphs(state)
    normalized = _normalize_domain(domain)
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
    if normalized == "data_graph":
        return graphs.data_graph
    if normalized == "obs_graph":
        return graphs.obs_graph
    if normalized == "ops_graph":
        return graphs.ops_graph
    if normalized == "property_graph":
        return graphs.property_graph
    if normalized == "campaign_graph":
        return graphs.campaign_graph
    if normalized == "inventory_graph":
        return graphs.inventory_graph
    raise KeyError(f"unknown capability graph domain: {domain}")


def build_graph_action_plan(
    state: WorldState,
    *,
    domain: Optional[str] = None,
    limit: int = 12,
) -> CapabilityGraphPlan:
    graphs = build_runtime_capability_graphs(state)
    selected_domain = _normalize_domain(domain) if domain else None
    if selected_domain and selected_domain not in graphs.available_domains:
        raise KeyError(f"unknown capability graph domain: {domain}")
    allowed_domains = set(graphs.available_domains)
    available_actions = [
        schema
        for schema in _ACTION_SCHEMAS
        if schema.domain in allowed_domains
        and (selected_domain is None or schema.domain == selected_domain)
    ]
    steps = _suggest_graph_steps(state, graphs)
    if selected_domain is not None:
        steps = [step for step in steps if step.domain == selected_domain]
    max_steps = max(1, int(limit))
    trimmed_steps = steps[:max_steps]
    next_focuses = _unique(
        _focus_for_plan_step(step)
        for step in trimmed_steps
        if _focus_for_plan_step(step)
    )
    return CapabilityGraphPlan(
        branch=state.branch,
        clock_ms=state.clock_ms,
        scenario_name=_scenario_name(state),
        available_domains=list(graphs.available_domains),
        available_actions=available_actions,
        suggested_steps=trimmed_steps,
        next_focuses=next_focuses,
        metadata={
            "organization_domain": graphs.metadata.get("organization_domain"),
            "builder_mode": graphs.metadata.get("builder_mode"),
        },
    )


def resolve_graph_action(
    state: WorldState,
    request: CapabilityGraphActionInput,
) -> _ResolvedGraphAction:
    if request.step_id:
        plan = build_graph_action_plan(state, limit=32)
        for step in plan.suggested_steps:
            if step.step_id != request.step_id:
                continue
            merged_args = dict(step.args)
            merged_args.update(request.args or {})
            if request.domain and request.domain != step.domain:
                raise ValueError(
                    f"step_id {request.step_id} resolves to {step.domain}, "
                    f"not {request.domain}"
                )
            if request.action and request.action != step.action:
                raise ValueError(
                    f"step_id {request.step_id} resolves to {step.action}, "
                    f"not {request.action}"
                )
            return _ResolvedGraphAction(
                domain=step.domain,
                action=step.action,
                tool=step.tool,
                args=merged_args,
                title=step.title,
                step_id=step.step_id,
            )
        raise ValueError(f"unknown graph plan step: {request.step_id}")

    if request.domain is None or request.action is None:
        raise ValueError("graph action requires either step_id or domain + action")
    schema = _ACTION_SCHEMA_INDEX.get((request.domain, request.action))
    if schema is None:
        raise ValueError(f"unsupported graph action: {request.domain}.{request.action}")
    for field in schema.required_args:
        if field not in request.args or request.args.get(field) in {None, ""}:
            raise ValueError(
                f"{request.domain}.{request.action} requires argument: {field}"
            )
    return _ResolvedGraphAction(
        domain=request.domain,
        action=request.action,
        tool=schema.tool,
        args=dict(request.args),
        title=schema.title,
    )


def get_graph_action_schema(
    domain: CapabilityDomain, action: str
) -> Optional[CapabilityGraphActionSchema]:
    return _ACTION_SCHEMA_INDEX.get((domain, action))


def list_graph_action_schemas() -> list[CapabilityGraphActionSchema]:
    return list(_ACTION_SCHEMAS)


def validate_graph_action_input(
    request: CapabilityGraphActionInput,
) -> CapabilityGraphActionSchema:
    if request.domain is None or request.action is None:
        raise ValueError("graph action requires both domain and action")
    schema = _ACTION_SCHEMA_INDEX.get((request.domain, request.action))
    if schema is None:
        raise ValueError(f"unsupported graph action: {request.domain}.{request.action}")
    for field in schema.required_args:
        if field not in request.args or request.args.get(field) in {None, ""}:
            raise ValueError(
                f"{request.domain}.{request.action} requires argument: {field}"
            )
    return schema


def infer_graph_action_object_refs(
    *,
    domain: CapabilityDomain,
    action: str,
    args: Dict[str, Any] | None = None,
    result: Dict[str, Any] | None = None,
) -> list[str]:
    refs: set[str] = set()
    payload = {**dict(args or {}), **dict(result or {})}

    def _add(prefix: str, key: str) -> None:
        value = payload.get(key)
        if value not in (None, ""):
            refs.add(f"{prefix}:{value}")

    normalized_domain = _normalize_domain(domain)
    normalized_action = action.strip().lower()

    if normalized_domain == "identity_graph":
        _add("identity_user", "user_id")
        _add("identity_group", "group_id")
        _add("identity_application", "app_id")
    elif normalized_domain == "doc_graph":
        if "drive_share" in normalized_action or "ownership" in normalized_action:
            _add("drive_share", "doc_id")
        _add("document", "doc_id")
    elif normalized_domain == "work_graph":
        _add("ticket", "issue_id")
        _add("ticket", "ticket_id")
        _add("service_request", "request_id")
        _add("incident", "incident_id")
    elif normalized_domain == "comm_graph":
        _add("comm_channel", "channel")
    elif normalized_domain == "revenue_graph":
        _add("crm_deal", "deal_id")
        _add("crm_deal", "id")
        _add("crm_company", "company_id")
        _add("crm_contact", "contact_id")
    elif normalized_domain == "data_graph":
        _add("workbook", "workbook_id")
        _add("worksheet", "sheet_id")
        _add("table", "table_id")
        _add("cell", "cell")
    elif normalized_domain == "obs_graph":
        _add("service", "service_id")
        _add("monitor", "monitor_id")
        _add("incident", "incident_id")
    elif normalized_domain == "ops_graph":
        _add("feature_flag", "flag_key")
        _add("service", "service_id")
        _add("service_customer", "customer_id")
        _add("service_work_order", "work_order_id")
        _add("service_technician", "technician_id")
        _add("service_appointment", "appointment_id")
        _add("billing_case", "billing_case_id")
        _add("service_exception", "exception_id")
    elif normalized_domain == "property_graph":
        _add("property", "property_id")
        _add("work_order", "work_order_id")
        _add("lease", "lease_id")
        _add("unit", "unit_id")
        _add("vendor", "vendor_id")
    elif normalized_domain == "campaign_graph":
        _add("campaign", "campaign_id")
        _add("creative", "creative_id")
        _add("approval", "approval_id")
        _add("report", "report_id")
    elif normalized_domain == "inventory_graph":
        _add("site", "site_id")
        _add("capacity_pool", "pool_id")
        _add("quote", "quote_id")
        _add("order", "order_id")
        _add("allocation", "allocation_id")

    return sorted(refs)


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
    components: Dict[str, Dict[str, Any]],
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


def _build_data_graph(components: Dict[str, Dict[str, Any]]) -> Optional[DataGraphView]:
    spreadsheet = components.get("spreadsheet", {})
    workbooks = [
        DataWorkbookView(
            workbook_id=str(workbook_id),
            title=str(payload.get("title", workbook_id)),
            owner=_optional_str(payload.get("owner")),
            sheet_count=len(payload.get("sheets", {})),
            shared_with=[str(item) for item in (payload.get("shared_with") or [])],
        )
        for workbook_id, payload in sorted((spreadsheet.get("workbooks") or {}).items())
    ]
    if not workbooks:
        return None
    return DataGraphView(workbooks=workbooks)


def _build_obs_graph(components: Dict[str, Dict[str, Any]]) -> Optional[ObsGraphView]:
    datadog = components.get("datadog", {})
    pagerduty = components.get("pagerduty", {})
    services = [
        ObsServiceView(
            service_id=str(service_id),
            name=str(payload.get("name", service_id)),
            status=str(payload.get("status", "unknown")),
            error_rate_pct=(
                float(payload.get("error_rate_pct"))
                if payload.get("error_rate_pct") is not None
                else None
            ),
            latency_p95_ms=(
                int(payload.get("latency_p95_ms"))
                if payload.get("latency_p95_ms") is not None
                else None
            ),
            revenue_tier=_optional_str(payload.get("revenue_tier")),
        )
        for service_id, payload in sorted((datadog.get("services") or {}).items())
    ]
    monitors = [
        ObsMonitorView(
            monitor_id=str(monitor_id),
            title=str(payload.get("title", monitor_id)),
            service_id=_optional_str(payload.get("service_id")),
            status=str(payload.get("status", "unknown")),
            severity=_optional_str(payload.get("severity")),
            muted=bool(payload.get("muted", False)),
        )
        for monitor_id, payload in sorted((datadog.get("monitors") or {}).items())
    ]
    incidents = [
        ObsIncidentView(
            incident_id=str(incident_id),
            title=str(payload.get("title", incident_id)),
            status=str(payload.get("status", "unknown")),
            urgency=_optional_str(payload.get("urgency")),
            service_id=_optional_str(payload.get("service_id")),
            assignee=_optional_str(payload.get("assignee")),
        )
        for incident_id, payload in sorted((pagerduty.get("incidents") or {}).items())
    ]
    if not services and not monitors and not incidents:
        return None
    return ObsGraphView(services=services, monitors=monitors, incidents=incidents)


def _build_ops_graph(components: Dict[str, Dict[str, Any]]) -> Optional[OpsGraphView]:
    feature_flags = components.get("feature_flags", {})
    service_ops = components.get("service_ops", {})
    flags = [
        OpsFlagView(
            flag_key=str(flag_key),
            service=_optional_str(payload.get("service")),
            env=_optional_str(payload.get("env")),
            enabled=bool(payload.get("enabled", False)),
            rollout_pct=int(payload.get("rollout_pct", 0) or 0),
        )
        for flag_key, payload in sorted((feature_flags.get("flags") or {}).items())
    ]
    customers = [
        OpsCustomerView(
            customer_id=str(item_id),
            name=str(payload.get("name", item_id)),
            tier=str(payload.get("tier", "standard")),
            account_status=str(payload.get("account_status", "active")),
            vip=bool(payload.get("vip", False)),
            dispute_open=bool(payload.get("dispute_open", False)),
        )
        for item_id, payload in sorted((service_ops.get("customers") or {}).items())
    ]
    work_orders = [
        OpsWorkOrderView(
            work_order_id=str(item_id),
            service_request_id=str(payload.get("service_request_id", "")),
            customer_id=str(payload.get("customer_id", "")),
            title=str(payload.get("title", item_id)),
            status=str(payload.get("status", "unknown")),
            required_skill=_optional_str(payload.get("required_skill")),
            technician_id=_optional_str(payload.get("technician_id")),
            appointment_id=_optional_str(payload.get("appointment_id")),
            estimated_amount_usd=float(payload.get("estimated_amount_usd", 0.0) or 0.0),
        )
        for item_id, payload in sorted((service_ops.get("work_orders") or {}).items())
    ]
    technicians = [
        OpsTechnicianView(
            technician_id=str(item_id),
            name=str(payload.get("name", item_id)),
            status=str(payload.get("status", "available")),
            skills=[str(skill) for skill in (payload.get("skills") or [])],
            current_appointment_id=_optional_str(payload.get("current_appointment_id")),
            home_zone=_optional_str(payload.get("home_zone")),
        )
        for item_id, payload in sorted((service_ops.get("technicians") or {}).items())
    ]
    appointments = [
        OpsAppointmentView(
            appointment_id=str(item_id),
            service_request_id=str(payload.get("service_request_id", "")),
            customer_id=str(payload.get("customer_id", "")),
            work_order_id=str(payload.get("work_order_id", "")),
            status=str(payload.get("status", "unknown")),
            technician_id=_optional_str(payload.get("technician_id")),
            scheduled_for_ms=(
                int(payload.get("scheduled_for_ms"))
                if payload.get("scheduled_for_ms") is not None
                else None
            ),
            dispatch_status=str(payload.get("dispatch_status", "pending")),
            reschedule_count=int(payload.get("reschedule_count", 0) or 0),
        )
        for item_id, payload in sorted((service_ops.get("appointments") or {}).items())
    ]
    billing_cases = [
        OpsBillingCaseView(
            billing_case_id=str(item_id),
            customer_id=str(payload.get("customer_id", "")),
            invoice_id=_optional_str(payload.get("invoice_id")),
            dispute_status=str(payload.get("dispute_status", "clear")),
            hold=bool(payload.get("hold", False)),
            amount_usd=float(payload.get("amount_usd", 0.0) or 0.0),
        )
        for item_id, payload in sorted((service_ops.get("billing_cases") or {}).items())
    ]
    exceptions = [
        OpsExceptionView(
            exception_id=str(item_id),
            type=str(payload.get("type", item_id)),
            severity=str(payload.get("severity", "medium")),
            status=str(payload.get("status", "open")),
            customer_id=_optional_str(payload.get("customer_id")),
            service_request_id=_optional_str(payload.get("service_request_id")),
            work_order_id=_optional_str(payload.get("work_order_id")),
        )
        for item_id, payload in sorted((service_ops.get("exceptions") or {}).items())
    ]
    policy_payload = service_ops.get("policy") or {}
    policy = (
        OpsPolicyView(
            approval_threshold_usd=float(
                policy_payload.get("approval_threshold_usd", 1000.0) or 1000.0
            ),
            vip_priority_override=bool(
                policy_payload.get("vip_priority_override", True)
            ),
            billing_hold_on_dispute=bool(
                policy_payload.get("billing_hold_on_dispute", True)
            ),
            max_auto_reschedules=int(
                policy_payload.get("max_auto_reschedules", 2) or 2
            ),
        )
        if policy_payload
        else None
    )
    if (
        not flags
        and not customers
        and not work_orders
        and not technicians
        and not appointments
        and not billing_cases
        and not exceptions
    ):
        return None
    return OpsGraphView(
        flags=flags,
        customers=customers,
        work_orders=work_orders,
        technicians=technicians,
        appointments=appointments,
        billing_cases=billing_cases,
        exceptions=exceptions,
        policy=policy,
    )


def _build_property_graph(
    components: Dict[str, Dict[str, Any]],
) -> Optional[PropertyGraphView]:
    property_ops = components.get("property_ops", {})
    properties = [
        PropertyView(
            property_id=str(item_id),
            name=str(payload.get("name", item_id)),
            city=str(payload.get("city", "")),
            state=str(payload.get("state", "")),
            status=str(payload.get("status", "active")),
        )
        for item_id, payload in sorted((property_ops.get("properties") or {}).items())
    ]
    units = [
        UnitView(
            unit_id=str(item_id),
            building_id=str(payload.get("building_id", "")),
            label=str(payload.get("label", item_id)),
            status=str(payload.get("status", "vacant")),
            reserved_for=_optional_str(payload.get("reserved_for")),
        )
        for item_id, payload in sorted((property_ops.get("units") or {}).items())
    ]
    leases = [
        LeaseView(
            lease_id=str(item_id),
            tenant_id=str(payload.get("tenant_id", "")),
            unit_id=str(payload.get("unit_id", "")),
            status=str(payload.get("status", "unknown")),
            milestone=str(payload.get("milestone", "draft")),
            amendment_pending=bool(payload.get("amendment_pending", False)),
        )
        for item_id, payload in sorted((property_ops.get("leases") or {}).items())
    ]
    vendors = [
        VendorView(
            vendor_id=str(item_id),
            name=str(payload.get("name", item_id)),
            specialty=str(payload.get("specialty", "")),
            status=str(payload.get("status", "active")),
        )
        for item_id, payload in sorted((property_ops.get("vendors") or {}).items())
    ]
    work_orders = [
        WorkOrderGraphView(
            work_order_id=str(item_id),
            property_id=str(payload.get("property_id", "")),
            title=str(payload.get("title", item_id)),
            status=str(payload.get("status", "unknown")),
            vendor_id=_optional_str(payload.get("vendor_id")),
            scheduled_for_ms=(
                int(payload.get("scheduled_for_ms"))
                if payload.get("scheduled_for_ms") is not None
                else None
            ),
        )
        for item_id, payload in sorted((property_ops.get("work_orders") or {}).items())
    ]
    if not properties and not units and not leases and not vendors and not work_orders:
        return None
    return PropertyGraphView(
        properties=properties,
        units=units,
        leases=leases,
        vendors=vendors,
        work_orders=work_orders,
    )


def _build_campaign_graph(
    components: Dict[str, Dict[str, Any]],
) -> Optional[CampaignGraphView]:
    campaign_ops = components.get("campaign_ops", {})
    clients = [
        ClientView(
            client_id=str(item_id),
            name=str(payload.get("name", item_id)),
            tier=str(payload.get("tier", "standard")),
        )
        for item_id, payload in sorted((campaign_ops.get("clients") or {}).items())
    ]
    campaigns = [
        CampaignView(
            campaign_id=str(item_id),
            client_id=str(payload.get("client_id", "")),
            name=str(payload.get("name", item_id)),
            channel=str(payload.get("channel", "")),
            status=str(payload.get("status", "unknown")),
            budget_usd=float(payload.get("budget_usd", 0.0) or 0.0),
            spend_usd=float(payload.get("spend_usd", 0.0) or 0.0),
            pacing_pct=float(payload.get("pacing_pct", 100.0) or 100.0),
        )
        for item_id, payload in sorted((campaign_ops.get("campaigns") or {}).items())
    ]
    creatives = [
        CreativeView(
            creative_id=str(item_id),
            campaign_id=str(payload.get("campaign_id", "")),
            title=str(payload.get("title", item_id)),
            status=str(payload.get("status", "unknown")),
            approval_required=bool(payload.get("approval_required", True)),
        )
        for item_id, payload in sorted((campaign_ops.get("creatives") or {}).items())
    ]
    approvals = [
        CampaignApprovalView(
            approval_id=str(item_id),
            campaign_id=str(payload.get("campaign_id", "")),
            stage=str(payload.get("stage", "")),
            status=str(payload.get("status", "unknown")),
        )
        for item_id, payload in sorted((campaign_ops.get("approvals") or {}).items())
    ]
    reports = [
        CampaignReportView(
            report_id=str(item_id),
            campaign_id=str(payload.get("campaign_id", "")),
            title=str(payload.get("title", item_id)),
            status=str(payload.get("status", "unknown")),
            stale=bool(payload.get("stale", False)),
        )
        for item_id, payload in sorted((campaign_ops.get("reports") or {}).items())
    ]
    if (
        not clients
        and not campaigns
        and not creatives
        and not approvals
        and not reports
    ):
        return None
    return CampaignGraphView(
        clients=clients,
        campaigns=campaigns,
        creatives=creatives,
        approvals=approvals,
        reports=reports,
    )


def _build_inventory_graph(
    components: Dict[str, Dict[str, Any]],
) -> Optional[InventoryGraphView]:
    inventory_ops = components.get("inventory_ops", {})
    sites = [
        SiteView(
            site_id=str(item_id),
            name=str(payload.get("name", item_id)),
            city=str(payload.get("city", "")),
            region=str(payload.get("region", "")),
            status=str(payload.get("status", "active")),
        )
        for item_id, payload in sorted((inventory_ops.get("sites") or {}).items())
    ]
    pools = [
        CapacityPoolView(
            pool_id=str(item_id),
            site_id=str(payload.get("site_id", "")),
            name=str(payload.get("name", item_id)),
            total_units=int(payload.get("total_units", 0) or 0),
            reserved_units=int(payload.get("reserved_units", 0) or 0),
        )
        for item_id, payload in sorted(
            (inventory_ops.get("capacity_pools") or {}).items()
        )
    ]
    quotes = [
        QuoteView(
            quote_id=str(item_id),
            customer_name=str(payload.get("customer_name", "")),
            requested_units=int(payload.get("requested_units", 0) or 0),
            status=str(payload.get("status", "unknown")),
            site_id=_optional_str(payload.get("site_id")),
            committed_units=int(payload.get("committed_units", 0) or 0),
        )
        for item_id, payload in sorted((inventory_ops.get("quotes") or {}).items())
    ]
    orders = [
        OrderView(
            order_id=str(item_id),
            quote_id=str(payload.get("quote_id", "")),
            status=str(payload.get("status", "unknown")),
            site_id=_optional_str(payload.get("site_id")),
        )
        for item_id, payload in sorted((inventory_ops.get("orders") or {}).items())
    ]
    allocations = [
        AllocationView(
            allocation_id=str(item_id),
            quote_id=str(payload.get("quote_id", "")),
            pool_id=str(payload.get("pool_id", "")),
            units=int(payload.get("units", 0) or 0),
            status=str(payload.get("status", "unknown")),
        )
        for item_id, payload in sorted((inventory_ops.get("allocations") or {}).items())
    ]
    vendors = [
        VendorView(
            vendor_id=str(item_id),
            name=str(payload.get("name", item_id)),
            specialty=str(payload.get("specialty", "")),
            status=str(payload.get("status", "active")),
        )
        for item_id, payload in sorted((inventory_ops.get("vendors") or {}).items())
    ]
    if not sites and not pools and not quotes and not orders and not allocations:
        return None
    return InventoryGraphView(
        sites=sites,
        capacity_pools=pools,
        quotes=quotes,
        orders=orders,
        allocations=allocations,
        vendors=vendors,
    )


def _suggest_graph_steps(
    state: WorldState, graphs: RuntimeCapabilityGraphs
) -> list[CapabilityGraphPlanStep]:
    graph_seed = _builder_capability_graphs(state)
    organization_domain = _optional_str(graph_seed.get("organization_domain"))
    steps: list[CapabilityGraphPlanStep] = []
    steps.extend(_identity_steps(state, graphs.identity_graph))
    steps.extend(
        _doc_steps(state, graphs.doc_graph, graphs.identity_graph, organization_domain)
    )
    steps.extend(_obs_steps(state, graphs.obs_graph))
    steps.extend(_ops_steps(state, graphs.ops_graph))
    steps.extend(_data_steps(state, graphs.data_graph))
    steps.extend(_work_steps(state, graphs.work_graph))
    steps.extend(_revenue_steps(state, graphs.revenue_graph, graphs.identity_graph))
    steps.extend(_comm_steps(graphs.comm_graph))
    steps.extend(_property_steps(graphs.property_graph))
    steps.extend(_campaign_steps(graphs.campaign_graph))
    steps.extend(_inventory_steps(graphs.inventory_graph))
    return sorted(steps, key=_plan_sort_key)


def _identity_steps(
    state: WorldState, graph: Optional[IdentityGraphView]
) -> list[CapabilityGraphPlanStep]:
    if graph is None:
        return []
    active_employee_emails = {
        employee.email
        for employee in graph.hris_employees
        if employee.status.upper() in {"ACTIVE", "PENDING"}
    }
    allowed_apps = _unique(
        app_id
        for policy in graph.policies
        for app_id in policy.allowed_application_ids
        if app_id
    )
    steps: list[CapabilityGraphPlanStep] = []
    for user in graph.users:
        if user.status.upper() in {"SUSPENDED", "DEPROVISIONED"}:
            continue
        if user.email not in active_employee_emails and active_employee_emails:
            steps.append(
                _plan_step(
                    domain="identity_graph",
                    action="suspend_user",
                    title=f"Suspend stale user {user.email}",
                    rationale="This user is not present in the active employee set.",
                    priority="high",
                    tool="okta.suspend_user",
                    args={
                        "user_id": user.user_id,
                        "reason": "Capability graph plan: stale identity not present in active HRIS records.",
                    },
                    target_id=user.user_id,
                    target_kind="user",
                    tags=["identity", "least_privilege"],
                )
            )
        missing_apps = [
            app_id for app_id in allowed_apps if app_id not in set(user.applications)
        ]
        if missing_apps and _user_has_identity_conflict(user, graph):
            steps.append(
                _plan_step(
                    domain="identity_graph",
                    action="assign_application",
                    title=f"Grant {missing_apps[0]} to {user.email}",
                    rationale="The active identity policy allows this application, and the user is still missing it.",
                    priority="high",
                    tool="okta.assign_application",
                    args={"user_id": user.user_id, "app_id": missing_apps[0]},
                    target_id=user.user_id,
                    target_kind="application_assignment",
                    tags=["identity", "least_privilege", "onboarding"],
                )
            )
    return steps


def _doc_steps(
    state: WorldState,
    graph: Optional[DocGraphView],
    identity_graph: Optional[IdentityGraphView],
    organization_domain: Optional[str],
) -> list[CapabilityGraphPlanStep]:
    if graph is None:
        return []
    forbidden_domains = {
        domain
        for policy in (identity_graph.policies if identity_graph else [])
        for domain in policy.forbidden_share_domains
        if domain
    }
    owner_candidate = _preferred_owner_email(identity_graph)
    steps: list[CapabilityGraphPlanStep] = []
    for share in graph.drive_shares:
        external_principals = [
            principal
            for principal in share.shared_with
            if _is_external_principal(principal, organization_domain, forbidden_domains)
        ]
        if share.visibility == "external_link" or external_principals:
            steps.append(
                _plan_step(
                    domain="doc_graph",
                    action="restrict_drive_share",
                    title=f"Restrict external sharing on {share.title}",
                    rationale="The drive share is externally exposed or includes forbidden external principals.",
                    priority="high",
                    tool="google_admin.restrict_drive_share",
                    args={
                        "doc_id": share.doc_id,
                        "visibility": "internal",
                        "note": "Capability graph plan: remove external sharing and return the doc to internal visibility.",
                    },
                    target_id=share.doc_id,
                    target_kind="drive_share",
                    tags=["docs", "oversharing", "policy"],
                )
            )
        if owner_candidate and _looks_departed_owner(share.owner):
            steps.append(
                _plan_step(
                    domain="doc_graph",
                    action="transfer_drive_ownership",
                    title=f"Transfer {share.title} to {owner_candidate}",
                    rationale="The current drive owner looks stale for this organization state.",
                    priority="high",
                    tool="google_admin.transfer_drive_ownership",
                    args={
                        "doc_id": share.doc_id,
                        "owner": owner_candidate,
                        "note": "Capability graph plan: move ownership to an active internal owner.",
                    },
                    target_id=share.doc_id,
                    target_kind="drive_share",
                    tags=["docs", "ownership"],
                )
            )
    documents = state.components.get("docs", {}).get("docs", {})
    for document in graph.documents:
        metadata = (
            state.components.get("docs", {})
            .get("metadata", {})
            .get(document.doc_id, {})
        )
        if "checklist" in document.title.lower() or "comms" in document.title.lower():
            steps.append(
                _plan_step(
                    domain="doc_graph",
                    action="update_document",
                    title=f"Update {document.title} with a fresh status note",
                    rationale="This document looks like an active runbook or comms artifact and should reflect current state.",
                    priority="medium",
                    tool="docs.update",
                    args={
                        "doc_id": document.doc_id,
                        "body": str(documents.get(document.doc_id, {}).get("body", ""))
                        + "\n\nUpdated from capability graph plan.",
                        "status": metadata.get("status", "ACTIVE"),
                    },
                    target_id=document.doc_id,
                    target_kind="document",
                    tags=["docs", "artifact_follow_through"],
                )
            )
            break
    return steps


def _obs_steps(
    state: WorldState, graph: Optional[ObsGraphView]
) -> list[CapabilityGraphPlanStep]:
    if graph is None:
        return []
    steps: list[CapabilityGraphPlanStep] = []
    for incident in graph.incidents:
        if incident.status.lower() == "triggered":
            steps.append(
                _plan_step(
                    domain="obs_graph",
                    action="ack_incident",
                    title=f"Acknowledge incident {incident.incident_id}",
                    rationale="Triggered incidents should be acknowledged early to establish ownership.",
                    priority="high",
                    tool="pagerduty.ack_incident",
                    args={
                        "incident_id": incident.incident_id,
                        "assignee": incident.assignee,
                    },
                    target_id=incident.incident_id,
                    target_kind="incident",
                    tags=["incident_response", "ownership"],
                )
            )
    for service in graph.services:
        if service.status.lower() in {"degraded", "alert", "investigating"}:
            steps.append(
                _plan_step(
                    domain="obs_graph",
                    action="annotate_service",
                    title=f"Annotate {service.name} with investigation status",
                    rationale="Degraded services should carry a visible operator note while mitigation is underway.",
                    priority="medium",
                    tool="datadog.update_service",
                    args={
                        "service_id": service.service_id,
                        "status": service.status,
                        "note": "Capability graph plan: investigating and mitigating.",
                    },
                    target_id=service.service_id,
                    target_kind="service",
                    tags=["observability", "annotation"],
                )
            )
            break
    return steps


def _ops_steps(
    state: WorldState, graph: Optional[OpsGraphView]
) -> list[CapabilityGraphPlanStep]:
    if graph is None:
        return []
    steps: list[CapabilityGraphPlanStep] = []
    planned_dispatch_work_orders: set[str] = set()
    for flag in graph.flags:
        if flag.enabled and flag.rollout_pct > 50 and "kill" not in flag.flag_key:
            target_rollout = (
                0 if flag.rollout_pct >= 50 else max(0, flag.rollout_pct // 2)
            )
            steps.append(
                _plan_step(
                    domain="ops_graph",
                    action="update_rollout",
                    title=f"Reduce rollout for {flag.flag_key}",
                    rationale="This flag is broadly enabled and should be reduced while the system is unhealthy.",
                    priority="high",
                    tool="feature_flags.update_rollout",
                    args={
                        "flag_key": flag.flag_key,
                        "rollout_pct": target_rollout,
                        "env": flag.env,
                        "reason": "Capability graph plan: reduce blast radius during mitigation.",
                    },
                    target_id=flag.flag_key,
                    target_kind="feature_flag",
                    tags=["rollout", "blast_radius"],
                )
            )
    available_technicians = [
        tech
        for tech in graph.technicians
        if tech.status.lower() in {"available", "standby"}
    ]
    for billing_case in graph.billing_cases:
        if billing_case.dispute_status.lower() not in {"open", "reopened", "disputed"}:
            continue
        if billing_case.hold:
            continue
        steps.append(
            _plan_step(
                domain="ops_graph",
                action="hold_billing",
                title=f"Hold billing for {billing_case.billing_case_id}",
                rationale="Disputed invoices should be held before any new billing action continues.",
                priority="high",
                tool="service_ops.hold_billing",
                args={
                    "billing_case_id": billing_case.billing_case_id,
                    "reason": "Capability graph plan: hold disputed billing before customer trust erodes.",
                },
                target_id=billing_case.billing_case_id,
                target_kind="billing_case",
                tags=["billing", "customer_trust"],
            )
        )
    work_order_map = {item.work_order_id: item for item in graph.work_orders}
    for issue in graph.exceptions:
        if issue.status.lower() in {"resolved", "mitigated"}:
            continue
        work_order = work_order_map.get(issue.work_order_id or "")
        if (
            issue.type.lower() in {"technician_unavailable", "sla_risk", "dispatch_gap"}
            and work_order is not None
        ):
            if work_order.work_order_id in planned_dispatch_work_orders:
                continue
            required_skill = (work_order.required_skill or "").strip().lower()
            matching_technician = next(
                (
                    tech
                    for tech in available_technicians
                    if not required_skill
                    or required_skill
                    in {skill.strip().lower() for skill in tech.skills}
                ),
                None,
            )
            if matching_technician is not None:
                steps.append(
                    _plan_step(
                        domain="ops_graph",
                        action="assign_dispatch",
                        title=f"Dispatch backup tech for {work_order.work_order_id}",
                        rationale="An open dispatch exception exists and a compatible technician is available.",
                        priority="high",
                        tool="service_ops.assign_dispatch",
                        args={
                            "work_order_id": work_order.work_order_id,
                            "technician_id": matching_technician.technician_id,
                            "appointment_id": work_order.appointment_id,
                            "note": "Capability graph plan: dispatch backup technician to stabilize service day.",
                        },
                        target_id=work_order.work_order_id,
                        target_kind="service_work_order",
                        tags=["dispatch", "sla"],
                    )
                )
                planned_dispatch_work_orders.add(work_order.work_order_id)
                continue
        steps.append(
            _plan_step(
                domain="ops_graph",
                action="clear_exception",
                title=f"Resolve {issue.exception_id}",
                rationale="The open exception still needs an explicit resolution trail.",
                priority="medium",
                tool="service_ops.clear_exception",
                args={
                    "exception_id": issue.exception_id,
                    "resolution_note": "Capability graph plan: explicitly resolve lingering service exception.",
                },
                target_id=issue.exception_id,
                target_kind="service_exception",
                tags=["exception", "follow_through"],
            )
        )
    return steps


def _data_steps(
    state: WorldState, graph: Optional[DataGraphView]
) -> list[CapabilityGraphPlanStep]:
    if graph is None:
        return []
    spreadsheet = state.components.get("spreadsheet", {})
    steps: list[CapabilityGraphPlanStep] = []
    for workbook in graph.workbooks:
        sheets = (
            spreadsheet.get("workbooks", {})
            .get(workbook.workbook_id, {})
            .get("sheets", {})
        )
        if not sheets:
            continue
        first_sheet_id, first_sheet = next(iter(sheets.items()))
        first_table = (first_sheet.get("tables") or [{}])[0]
        title = workbook.title.lower()
        if "revenue" in title or "flight deck" in title or "impact" in title:
            steps.append(
                _plan_step(
                    domain="data_graph",
                    action="upsert_row",
                    title=f"Capture impact row in {workbook.title}",
                    rationale="This workbook looks like the active operating workbook for the incident and should record impact explicitly.",
                    priority="medium",
                    tool="spreadsheet.upsert_row",
                    args={
                        "workbook_id": workbook.workbook_id,
                        "sheet_id": str(first_sheet_id),
                        "match_field": "metric",
                        "match_value": "estimated_revenue_impact",
                        "row": {
                            "metric": "estimated_revenue_impact",
                            "value": "pending_assessment",
                            "notes": "Added from capability graph plan.",
                        },
                        "table_id": first_table.get("table_id"),
                    },
                    target_id=workbook.workbook_id,
                    target_kind="workbook",
                    tags=["spreadsheet", "impact_assessment"],
                )
            )
            break
    return steps


def _work_steps(
    state: WorldState, graph: Optional[WorkGraphView]
) -> list[CapabilityGraphPlanStep]:
    if graph is None:
        return []
    steps: list[CapabilityGraphPlanStep] = []
    for ticket in graph.tickets:
        if ticket.item_id.startswith("JRA-") and ticket.status.lower() == "open":
            steps.append(
                _plan_step(
                    domain="work_graph",
                    action="transition_issue",
                    title=f"Move issue {ticket.item_id} into progress",
                    rationale="Open tracker issues should move into active execution once work begins.",
                    priority="medium",
                    tool="jira.transition_issue",
                    args={"issue_id": ticket.item_id, "status": "in_progress"},
                    target_id=ticket.item_id,
                    target_kind="issue",
                    tags=["tracker", "execution"],
                )
            )
        elif ticket.status.lower() == "open":
            steps.append(
                _plan_step(
                    domain="work_graph",
                    action="transition_ticket",
                    title=f"Move ticket {ticket.item_id} into progress",
                    rationale="Open tickets should reflect that work is underway.",
                    priority="medium",
                    tool="tickets.transition",
                    args={"ticket_id": ticket.item_id, "status": "in_progress"},
                    target_id=ticket.item_id,
                    target_kind="ticket",
                    tags=["ticketing", "execution"],
                )
            )
    service_request_state = state.components.get("servicedesk", {}).get("requests", {})
    for request in graph.service_requests:
        raw = service_request_state.get(request.request_id, {})
        pending_stage = next(
            (
                item.get("stage")
                for item in (raw.get("approvals") or [])
                if str(item.get("status", "")).upper() == "PENDING"
            ),
            None,
        )
        if pending_stage:
            steps.append(
                _plan_step(
                    domain="work_graph",
                    action="update_request_approval",
                    title=f"Advance approval on {request.request_id}",
                    rationale="This service request is blocked on an explicit approval stage.",
                    priority="high",
                    tool="servicedesk.update_request",
                    args={
                        "request_id": request.request_id,
                        "approval_stage": str(pending_stage),
                        "approval_status": "APPROVED",
                        "comment": "Capability graph plan: approval advanced.",
                    },
                    target_id=request.request_id,
                    target_kind="service_request",
                    tags=["approval", "workflow"],
                )
            )
    for incident in graph.incidents:
        if incident.status.lower() in {"new", "in_progress"}:
            steps.append(
                _plan_step(
                    domain="work_graph",
                    action="update_incident",
                    title=f"Add operator note to incident {incident.item_id}",
                    rationale="The incident record should reflect current mitigation work.",
                    priority="medium",
                    tool="servicedesk.update_incident",
                    args={
                        "incident_id": incident.item_id,
                        "status": incident.status,
                        "comment": "Capability graph plan: mitigation in progress.",
                    },
                    target_id=incident.item_id,
                    target_kind="incident",
                    tags=["incident_record", "artifact_follow_through"],
                )
            )
            break
    return steps


def _revenue_steps(
    state: WorldState,
    graph: Optional[RevenueGraphView],
    identity_graph: Optional[IdentityGraphView],
) -> list[CapabilityGraphPlanStep]:
    if graph is None:
        return []
    preferred_owner = _preferred_owner_email(identity_graph)
    steps: list[CapabilityGraphPlanStep] = []
    for deal in graph.deals:
        if preferred_owner and _looks_departed_owner(deal.owner):
            steps.append(
                _plan_step(
                    domain="revenue_graph",
                    action="reassign_deal_owner",
                    title=f"Reassign deal {deal.deal_id} to {preferred_owner}",
                    rationale="The current revenue owner looks stale for this organization state.",
                    priority="high",
                    tool="crm.reassign_deal_owner",
                    args={"id": deal.deal_id, "owner": preferred_owner},
                    target_id=deal.deal_id,
                    target_kind="deal",
                    tags=["revenue", "ownership"],
                )
            )
        steps.append(
            _plan_step(
                domain="revenue_graph",
                action="log_activity",
                title=f"Log a revenue-impact note on {deal.deal_id}",
                rationale="Important deals should carry an explicit activity trail during material incidents.",
                priority="medium",
                tool="crm.log_activity",
                args={
                    "kind": "note",
                    "deal_id": deal.deal_id,
                    "contact_id": deal.contact_id,
                    "note": "Capability graph plan: incident mitigation may affect conversion and revenue timing.",
                },
                target_id=deal.deal_id,
                target_kind="deal",
                tags=["revenue", "artifact_follow_through"],
            )
        )
        break
    return steps


def _comm_steps(graph: Optional[CommGraphView]) -> list[CapabilityGraphPlanStep]:
    if graph is None or not graph.channels:
        return []
    channel = graph.channels[0]
    return [
        _plan_step(
            domain="comm_graph",
            action="post_message",
            title=f"Post an update in {channel.channel}",
            rationale="Visible coordination channels should carry a concise progress update.",
            priority="low",
            tool="slack.send_message",
            args={
                "channel": channel.channel,
                "text": "Capability graph plan: investigating now and updating related systems.",
            },
            target_id=channel.channel,
            target_kind="channel",
            tags=["communication"],
        )
    ]


def _property_steps(
    graph: Optional[PropertyGraphView],
) -> list[CapabilityGraphPlanStep]:
    if graph is None:
        return []
    steps: list[CapabilityGraphPlanStep] = []
    for lease in graph.leases:
        if lease.amendment_pending or lease.milestone.lower() in {"draft", "pending"}:
            steps.append(
                _plan_step(
                    domain="property_graph",
                    action="update_lease_milestone",
                    title=f"Advance lease {lease.lease_id}",
                    rationale="This tenant opening still depends on a lease milestone completing.",
                    priority="high",
                    tool="property.update_lease_milestone",
                    args={
                        "lease_id": lease.lease_id,
                        "milestone": "executed",
                        "status": "ready",
                    },
                    target_id=lease.lease_id,
                    target_kind="lease",
                    tags=["real_estate", "lease", "opening_readiness"],
                )
            )
            break
    for work_order in graph.work_orders:
        if work_order.status.lower() in {"new", "blocked", "pending_vendor"}:
            vendor_id = graph.vendors[0].vendor_id if graph.vendors else None
            if vendor_id:
                steps.append(
                    _plan_step(
                        domain="property_graph",
                        action="assign_vendor",
                        title=f"Assign vendor for {work_order.work_order_id}",
                        rationale="A pending work order needs a committed vendor before tenant opening.",
                        priority="high",
                        tool="property.assign_vendor",
                        args={
                            "work_order_id": work_order.work_order_id,
                            "vendor_id": vendor_id,
                        },
                        target_id=work_order.work_order_id,
                        target_kind="work_order",
                        tags=["real_estate", "vendor", "maintenance"],
                    )
                )
                break
    for unit in graph.units:
        if unit.status.lower() == "vacant":
            lease = next(
                (item for item in graph.leases if item.unit_id == unit.unit_id), None
            )
            if lease is not None:
                steps.append(
                    _plan_step(
                        domain="property_graph",
                        action="reserve_unit",
                        title=f"Reserve unit {unit.label}",
                        rationale="The tenant-facing unit should be explicitly reserved before opening.",
                        priority="medium",
                        tool="property.reserve_unit",
                        args={"unit_id": unit.unit_id, "tenant_id": lease.tenant_id},
                        target_id=unit.unit_id,
                        target_kind="unit",
                        tags=["real_estate", "occupancy"],
                    )
                )
                break
    return steps


def _campaign_steps(
    graph: Optional[CampaignGraphView],
) -> list[CapabilityGraphPlanStep]:
    if graph is None:
        return []
    steps: list[CapabilityGraphPlanStep] = []
    for creative in graph.creatives:
        if creative.approval_required and creative.status.lower() != "approved":
            approval = next(
                (
                    item
                    for item in graph.approvals
                    if item.campaign_id == creative.campaign_id
                ),
                None,
            )
            if approval is not None:
                steps.append(
                    _plan_step(
                        domain="campaign_graph",
                        action="approve_creative",
                        title=f"Approve creative {creative.title}",
                        rationale="The launch should not proceed with unapproved creative.",
                        priority="high",
                        tool="campaign.approve_creative",
                        args={
                            "creative_id": creative.creative_id,
                            "approval_id": approval.approval_id,
                        },
                        target_id=creative.creative_id,
                        target_kind="creative",
                        tags=["marketing", "approval", "launch_safety"],
                    )
                )
                break
    for campaign in graph.campaigns:
        if campaign.pacing_pct > 110 or campaign.spend_usd > campaign.budget_usd:
            steps.append(
                _plan_step(
                    domain="campaign_graph",
                    action="adjust_budget_pacing",
                    title=f"Reduce pacing for {campaign.name}",
                    rationale="Budget pacing is unsafe for this pending launch.",
                    priority="high",
                    tool="campaign.adjust_budget_pacing",
                    args={"campaign_id": campaign.campaign_id, "pacing_pct": 80.0},
                    target_id=campaign.campaign_id,
                    target_kind="campaign",
                    tags=["marketing", "budget", "guardrail"],
                )
            )
            break
    for report in graph.reports:
        if report.stale:
            steps.append(
                _plan_step(
                    domain="campaign_graph",
                    action="publish_report_note",
                    title=f"Refresh report {report.title}",
                    rationale="Launch comms should reference a current report artifact.",
                    priority="medium",
                    tool="campaign.publish_report_note",
                    args={
                        "report_id": report.report_id,
                        "note": "Launch guardrail review completed.",
                    },
                    target_id=report.report_id,
                    target_kind="report",
                    tags=["marketing", "artifact_follow_through"],
                )
            )
            break
    return steps


def _inventory_steps(
    graph: Optional[InventoryGraphView],
) -> list[CapabilityGraphPlanStep]:
    if graph is None:
        return []
    steps: list[CapabilityGraphPlanStep] = []
    for quote in graph.quotes:
        if quote.committed_units < quote.requested_units:
            pool = next(
                (
                    item
                    for item in graph.capacity_pools
                    if (item.total_units - item.reserved_units)
                    >= (quote.requested_units - quote.committed_units)
                ),
                None,
            )
            if pool is not None:
                steps.append(
                    _plan_step(
                        domain="inventory_graph",
                        action="allocate_capacity",
                        title=f"Allocate capacity for {quote.quote_id}",
                        rationale="The customer quote should only be committed once feasible capacity is reserved.",
                        priority="high",
                        tool="inventory.allocate_capacity",
                        args={
                            "quote_id": quote.quote_id,
                            "pool_id": pool.pool_id,
                            "units": quote.requested_units - quote.committed_units,
                        },
                        target_id=quote.quote_id,
                        target_kind="quote",
                        tags=["storage", "capacity", "feasibility"],
                    )
                )
                steps.append(
                    _plan_step(
                        domain="inventory_graph",
                        action="revise_quote",
                        title=f"Revise quote {quote.quote_id}",
                        rationale="The quote should reflect the feasible site and committed capacity.",
                        priority="high",
                        tool="inventory.revise_quote",
                        args={
                            "quote_id": quote.quote_id,
                            "site_id": pool.site_id,
                            "committed_units": quote.requested_units,
                        },
                        target_id=quote.quote_id,
                        target_kind="quote",
                        tags=["storage", "quote_accuracy"],
                    )
                )
                break
    for order in graph.orders:
        if order.status.lower() in {"new", "pending_vendor"} and graph.vendors:
            steps.append(
                _plan_step(
                    domain="inventory_graph",
                    action="assign_vendor_action",
                    title=f"Assign vendor for order {order.order_id}",
                    rationale="The downstream storage rollout needs a vendor/fulfillment plan before commitment.",
                    priority="medium",
                    tool="inventory.assign_vendor_action",
                    args={
                        "order_id": order.order_id,
                        "vendor_id": graph.vendors[0].vendor_id,
                    },
                    target_id=order.order_id,
                    target_kind="order",
                    tags=["storage", "ops_follow_through"],
                )
            )
            break
    return steps


def _plan_step(
    *,
    domain: CapabilityDomain,
    action: str,
    title: str,
    rationale: str,
    priority: str,
    tool: str,
    args: Dict[str, Any],
    target_id: Optional[str],
    target_kind: Optional[str],
    tags: Iterable[str],
) -> CapabilityGraphPlanStep:
    identifier = target_id or title.lower().replace(" ", "-")
    return CapabilityGraphPlanStep(
        step_id=f"{domain}:{action}:{identifier}",
        domain=domain,
        action=action,
        title=title,
        rationale=rationale,
        priority=priority,
        tool=tool,
        args=args,
        target_id=target_id,
        target_kind=target_kind,
        tags=[str(tag) for tag in tags],
    )


def _plan_sort_key(step: CapabilityGraphPlanStep) -> tuple[int, str, str]:
    priority_rank = {"high": 0, "medium": 1, "low": 2}
    return (priority_rank.get(step.priority, 99), step.domain, step.step_id)


def _scenario_metadata(state: WorldState) -> Dict[str, Any]:
    scenario = state.scenario or {}
    metadata = scenario.get("metadata")
    return metadata if isinstance(metadata, dict) else {}


def _allowed_domains(state: WorldState) -> Optional[set[str]]:
    metadata = _scenario_metadata(state)
    hint_map = metadata.get("builder_blueprint_orientation")
    if not isinstance(hint_map, dict):
        return None
    raw = hint_map.get("capability_domains")
    if not isinstance(raw, list):
        return None
    normalized = {str(item).strip().lower() for item in raw if str(item).strip()}
    return normalized or None


def _builder_capability_graphs(state: WorldState) -> Dict[str, Any]:
    metadata = _scenario_metadata(state)
    raw = metadata.get("builder_capability_graphs")
    return raw if isinstance(raw, dict) else {}


def _scenario_name(state: WorldState) -> Optional[str]:
    scenario = state.scenario or {}
    metadata = _scenario_metadata(state)
    return (
        _optional_str(scenario.get("name"))
        or _optional_str(metadata.get("builder_runtime_scenario_name"))
        or _optional_str(metadata.get("scenario_name"))
        or _optional_str(metadata.get("scenario_template_name"))
    )


def _normalize_domain(domain: str) -> CapabilityDomain:
    normalized = domain.strip().lower()
    if normalized not in _DOMAIN_SET:
        raise KeyError(f"unknown capability graph domain: {domain}")
    return normalized  # type: ignore[return-value]


def _unique(items: Iterable[Optional[str]]) -> list[str]:
    seen: set[str] = set()
    ordered: list[str] = []
    for item in items:
        if item is None or item in seen:
            continue
        seen.add(item)
        ordered.append(item)
    return ordered


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


def _email_domain(principal: str) -> Optional[str]:
    if "@" not in principal:
        return None
    return principal.rsplit("@", 1)[1].lower()


def _is_external_principal(
    principal: str,
    organization_domain: Optional[str],
    forbidden_domains: set[str],
) -> bool:
    domain = _email_domain(principal)
    if domain is None:
        return True
    if domain in forbidden_domains:
        return True
    if domain.endswith("example.com"):
        return False
    if organization_domain and domain == organization_domain.lower():
        return False
    return True


def _preferred_owner_email(
    identity_graph: Optional[IdentityGraphView],
) -> Optional[str]:
    if identity_graph is None:
        return None
    for employee in identity_graph.hris_employees:
        if employee.status.upper() == "ACTIVE" and not employee.identity_conflict:
            return employee.email
    for user in identity_graph.users:
        if user.status.upper() == "ACTIVE":
            return user.email
    return None


def _looks_departed_owner(owner: Optional[str]) -> bool:
    if owner is None:
        return False
    lowered = owner.lower()
    return any(marker in lowered for marker in ("departed", "former", "oldco"))


def _user_has_identity_conflict(
    user: IdentityUserView, graph: IdentityGraphView
) -> bool:
    for employee in graph.hris_employees:
        if employee.email == user.email and employee.identity_conflict:
            return True
    return False


def _focus_for_domain(domain: CapabilityDomain) -> Optional[str]:
    return _DOMAIN_FOCUS.get(domain)


def _focus_for_plan_step(step: CapabilityGraphPlanStep) -> Optional[str]:
    tool_focus = _focus_for_tool(step.tool)
    if tool_focus is not None:
        return tool_focus
    return _focus_for_domain(step.domain)


def _focus_for_tool(tool: Optional[str]) -> Optional[str]:
    if not tool:
        return None
    tool_prefix = tool.strip().lower().split(".", 1)[0]
    return _TOOL_FOCUS.get(tool_prefix)


def _schema(
    *,
    domain: CapabilityDomain,
    action: str,
    title: str,
    description: str,
    tool: str,
    required_args: Iterable[str],
    optional_args: Iterable[str] = (),
    tags: Iterable[str] = (),
) -> CapabilityGraphActionSchema:
    return CapabilityGraphActionSchema(
        domain=domain,
        action=action,
        title=title,
        description=description,
        tool=tool,
        required_args=[str(item) for item in required_args],
        optional_args=[str(item) for item in optional_args],
        tags=[str(item) for item in tags],
    )


_ACTION_SCHEMAS = [
    _schema(
        domain="comm_graph",
        action="post_message",
        title="Post message",
        description="Post a coordination update into a visible communication channel.",
        tool="slack.send_message",
        required_args=("channel", "text"),
        optional_args=("thread_ts",),
        tags=("communication",),
    ),
    _schema(
        domain="doc_graph",
        action="update_document",
        title="Update document",
        description="Update a document artifact such as a runbook or comms note.",
        tool="docs.update",
        required_args=("doc_id",),
        optional_args=("title", "body", "tags", "status"),
        tags=("docs",),
    ),
    _schema(
        domain="doc_graph",
        action="restrict_drive_share",
        title="Restrict drive share",
        description="Reduce a drive share back to a safer visibility setting.",
        tool="google_admin.restrict_drive_share",
        required_args=("doc_id",),
        optional_args=("visibility", "note"),
        tags=("docs", "policy"),
    ),
    _schema(
        domain="doc_graph",
        action="transfer_drive_ownership",
        title="Transfer drive ownership",
        description="Move a drive document to a new active owner.",
        tool="google_admin.transfer_drive_ownership",
        required_args=("doc_id", "owner"),
        optional_args=("note",),
        tags=("docs", "ownership"),
    ),
    _schema(
        domain="work_graph",
        action="transition_ticket",
        title="Transition ticket",
        description="Move a ticket to a new workflow state.",
        tool="tickets.transition",
        required_args=("ticket_id", "status"),
        tags=("ticketing",),
    ),
    _schema(
        domain="work_graph",
        action="add_ticket_comment",
        title="Comment on ticket",
        description="Add a comment to a ticket artifact.",
        tool="tickets.add_comment",
        required_args=("ticket_id", "body"),
        optional_args=("author",),
        tags=("ticketing", "artifact_follow_through"),
    ),
    _schema(
        domain="work_graph",
        action="transition_issue",
        title="Transition issue",
        description="Move a Jira-style issue to a new workflow state.",
        tool="jira.transition_issue",
        required_args=("issue_id", "status"),
        tags=("tracker",),
    ),
    _schema(
        domain="work_graph",
        action="add_issue_comment",
        title="Comment on issue",
        description="Add a note to a Jira-style issue.",
        tool="jira.add_comment",
        required_args=("issue_id", "body"),
        optional_args=("author",),
        tags=("tracker", "artifact_follow_through"),
    ),
    _schema(
        domain="work_graph",
        action="update_request_approval",
        title="Update request approval",
        description="Advance or revise a service request approval stage.",
        tool="servicedesk.update_request",
        required_args=("request_id",),
        optional_args=("status", "approval_stage", "approval_status", "comment"),
        tags=("approval",),
    ),
    _schema(
        domain="work_graph",
        action="update_incident",
        title="Update incident",
        description="Update the status or note trail on a service incident.",
        tool="servicedesk.update_incident",
        required_args=("incident_id",),
        optional_args=("status", "assignee", "comment"),
        tags=("incident_record",),
    ),
    _schema(
        domain="identity_graph",
        action="assign_group",
        title="Assign group",
        description="Add a user to an identity group.",
        tool="okta.assign_group",
        required_args=("user_id", "group_id"),
        tags=("identity",),
    ),
    _schema(
        domain="identity_graph",
        action="remove_group",
        title="Remove group",
        description="Remove a user from an identity group.",
        tool="okta.unassign_group",
        required_args=("user_id", "group_id"),
        tags=("identity", "least_privilege"),
    ),
    _schema(
        domain="identity_graph",
        action="assign_application",
        title="Assign application",
        description="Grant an application assignment to a user.",
        tool="okta.assign_application",
        required_args=("user_id", "app_id"),
        tags=("identity", "access"),
    ),
    _schema(
        domain="identity_graph",
        action="remove_application",
        title="Remove application",
        description="Remove an application assignment from a user.",
        tool="okta.unassign_application",
        required_args=("user_id", "app_id"),
        tags=("identity", "least_privilege"),
    ),
    _schema(
        domain="identity_graph",
        action="suspend_user",
        title="Suspend user",
        description="Suspend a user account during containment or offboarding.",
        tool="okta.suspend_user",
        required_args=("user_id",),
        optional_args=("reason",),
        tags=("identity", "containment"),
    ),
    _schema(
        domain="identity_graph",
        action="unsuspend_user",
        title="Unsuspend user",
        description="Restore access to a suspended user account.",
        tool="okta.unsuspend_user",
        required_args=("user_id",),
        tags=("identity", "recovery"),
    ),
    _schema(
        domain="revenue_graph",
        action="update_deal_stage",
        title="Update deal stage",
        description="Move a revenue deal to a different stage.",
        tool="crm.update_deal_stage",
        required_args=("id", "stage"),
        tags=("revenue",),
    ),
    _schema(
        domain="revenue_graph",
        action="reassign_deal_owner",
        title="Reassign deal owner",
        description="Move revenue ownership to a new seller or manager.",
        tool="crm.reassign_deal_owner",
        required_args=("id", "owner"),
        tags=("revenue", "ownership"),
    ),
    _schema(
        domain="revenue_graph",
        action="log_activity",
        title="Log CRM activity",
        description="Attach a note or outreach artifact to a deal or contact.",
        tool="crm.log_activity",
        required_args=("kind",),
        optional_args=("deal_id", "contact_id", "note"),
        tags=("revenue", "artifact_follow_through"),
    ),
    _schema(
        domain="data_graph",
        action="update_cell",
        title="Update cell",
        description="Edit a single spreadsheet cell.",
        tool="spreadsheet.update_cell",
        required_args=("workbook_id", "sheet_id", "cell", "value"),
        optional_args=("note",),
        tags=("spreadsheet",),
    ),
    _schema(
        domain="data_graph",
        action="upsert_row",
        title="Upsert row",
        description="Insert or update a spreadsheet row in a table-like sheet.",
        tool="spreadsheet.upsert_row",
        required_args=("workbook_id", "sheet_id", "match_field", "match_value", "row"),
        optional_args=("table_id",),
        tags=("spreadsheet", "analysis"),
    ),
    _schema(
        domain="data_graph",
        action="set_formula",
        title="Set formula",
        description="Write a spreadsheet formula into a cell.",
        tool="spreadsheet.set_formula",
        required_args=("workbook_id", "sheet_id", "cell", "formula"),
        tags=("spreadsheet", "analysis"),
    ),
    _schema(
        domain="data_graph",
        action="share_workbook",
        title="Share workbook",
        description="Adjust workbook sharing to another principal.",
        tool="spreadsheet.share_workbook",
        required_args=("workbook_id", "principal", "role"),
        tags=("spreadsheet", "sharing"),
    ),
    _schema(
        domain="obs_graph",
        action="ack_incident",
        title="Acknowledge incident",
        description="Take ownership of a live incident.",
        tool="pagerduty.ack_incident",
        required_args=("incident_id",),
        optional_args=("assignee",),
        tags=("incident_response",),
    ),
    _schema(
        domain="obs_graph",
        action="resolve_incident",
        title="Resolve incident",
        description="Mark a PagerDuty incident as resolved.",
        tool="pagerduty.resolve_incident",
        required_args=("incident_id",),
        optional_args=("note",),
        tags=("incident_response", "closure"),
    ),
    _schema(
        domain="obs_graph",
        action="escalate_incident",
        title="Escalate incident",
        description="Reassign a live incident to another responder.",
        tool="pagerduty.escalate_incident",
        required_args=("incident_id", "assignee"),
        tags=("incident_response", "escalation"),
    ),
    _schema(
        domain="obs_graph",
        action="annotate_service",
        title="Annotate service",
        description="Write a status/note update onto a monitored service.",
        tool="datadog.update_service",
        required_args=("service_id",),
        optional_args=("status", "note"),
        tags=("observability",),
    ),
    _schema(
        domain="ops_graph",
        action="set_flag",
        title="Set feature flag",
        description="Enable or disable a feature flag.",
        tool="feature_flags.set_flag",
        required_args=("flag_key", "enabled"),
        optional_args=("env", "reason"),
        tags=("rollout",),
    ),
    _schema(
        domain="ops_graph",
        action="update_rollout",
        title="Update rollout",
        description="Change the rollout percentage for a feature flag.",
        tool="feature_flags.update_rollout",
        required_args=("flag_key", "rollout_pct"),
        optional_args=("env", "reason"),
        tags=("rollout", "blast_radius"),
    ),
    _schema(
        domain="ops_graph",
        action="assign_dispatch",
        title="Assign dispatch",
        description="Assign a compatible technician to the service work order.",
        tool="service_ops.assign_dispatch",
        required_args=("work_order_id", "technician_id"),
        optional_args=("appointment_id", "scheduled_for_ms", "note"),
        tags=("dispatch", "service"),
    ),
    _schema(
        domain="ops_graph",
        action="reschedule_dispatch",
        title="Reschedule dispatch",
        description="Reschedule a field appointment onto a different technician slot.",
        tool="service_ops.reschedule_dispatch",
        required_args=("appointment_id", "technician_id", "scheduled_for_ms"),
        optional_args=("note",),
        tags=("dispatch", "service"),
    ),
    _schema(
        domain="ops_graph",
        action="hold_billing",
        title="Hold billing",
        description="Pause billing activity for a disputed customer case.",
        tool="service_ops.hold_billing",
        required_args=("billing_case_id",),
        optional_args=("reason", "hold"),
        tags=("billing", "customer_trust"),
    ),
    _schema(
        domain="ops_graph",
        action="clear_exception",
        title="Clear exception",
        description="Resolve or mitigate a service operations exception.",
        tool="service_ops.clear_exception",
        required_args=("exception_id",),
        optional_args=("resolution_note", "status"),
        tags=("exception", "service"),
    ),
    _schema(
        domain="ops_graph",
        action="update_policy",
        title="Update service policy",
        description="Adjust service-ops policy knobs and rerun from the same snapshot.",
        tool="service_ops.update_policy",
        required_args=(),
        optional_args=(
            "approval_threshold_usd",
            "vip_priority_override",
            "billing_hold_on_dispute",
            "max_auto_reschedules",
            "reason",
        ),
        tags=("policy", "service"),
    ),
    _schema(
        domain="property_graph",
        action="assign_vendor",
        title="Assign vendor",
        description="Assign a vendor to a property work order.",
        tool="property.assign_vendor",
        required_args=("work_order_id", "vendor_id"),
        optional_args=("note",),
        tags=("real_estate", "maintenance"),
    ),
    _schema(
        domain="property_graph",
        action="reschedule_work_order",
        title="Reschedule work order",
        description="Move a work order to a safer or more feasible time slot.",
        tool="property.reschedule_work_order",
        required_args=("work_order_id", "scheduled_for_ms"),
        optional_args=("note",),
        tags=("real_estate", "maintenance"),
    ),
    _schema(
        domain="property_graph",
        action="update_lease_milestone",
        title="Update lease milestone",
        description="Advance a lease/amendment milestone before tenant opening.",
        tool="property.update_lease_milestone",
        required_args=("lease_id", "milestone"),
        optional_args=("status",),
        tags=("real_estate", "lease"),
    ),
    _schema(
        domain="property_graph",
        action="reserve_unit",
        title="Reserve unit",
        description="Reserve a unit for a tenant commitment or opening.",
        tool="property.reserve_unit",
        required_args=("unit_id", "tenant_id"),
        optional_args=("status",),
        tags=("real_estate", "occupancy"),
    ),
    _schema(
        domain="campaign_graph",
        action="approve_creative",
        title="Approve creative",
        description="Approve a pending creative asset and its approval record.",
        tool="campaign.approve_creative",
        required_args=("creative_id", "approval_id"),
        tags=("marketing", "approval"),
    ),
    _schema(
        domain="campaign_graph",
        action="adjust_budget_pacing",
        title="Adjust budget pacing",
        description="Change campaign pacing before launch or overspend.",
        tool="campaign.adjust_budget_pacing",
        required_args=("campaign_id", "pacing_pct"),
        optional_args=("note",),
        tags=("marketing", "budget"),
    ),
    _schema(
        domain="campaign_graph",
        action="pause_channel_launch",
        title="Pause launch",
        description="Pause a campaign launch on a specific channel.",
        tool="campaign.pause_channel_launch",
        required_args=("campaign_id",),
        optional_args=("reason",),
        tags=("marketing", "launch_safety"),
    ),
    _schema(
        domain="campaign_graph",
        action="publish_report_note",
        title="Publish report note",
        description="Refresh a campaign report or annotate it for launch readiness.",
        tool="campaign.publish_report_note",
        required_args=("report_id", "note"),
        tags=("marketing", "artifact_follow_through"),
    ),
    _schema(
        domain="inventory_graph",
        action="allocate_capacity",
        title="Allocate capacity",
        description="Reserve feasible capacity for a strategic customer quote.",
        tool="inventory.allocate_capacity",
        required_args=("quote_id", "pool_id", "units"),
        tags=("storage", "capacity"),
    ),
    _schema(
        domain="inventory_graph",
        action="reserve_inventory_block",
        title="Reserve inventory block",
        description="Reserve capacity before final quote commitment.",
        tool="inventory.reserve_inventory_block",
        required_args=("pool_id", "units"),
        optional_args=("note",),
        tags=("storage", "capacity"),
    ),
    _schema(
        domain="inventory_graph",
        action="revise_quote",
        title="Revise quote",
        description="Update a quote to reflect feasible storage allocation.",
        tool="inventory.revise_quote",
        required_args=("quote_id", "site_id", "committed_units"),
        tags=("storage", "quote"),
    ),
    _schema(
        domain="inventory_graph",
        action="assign_vendor_action",
        title="Assign vendor action",
        description="Assign a downstream fulfillment/vendor task for a storage order.",
        tool="inventory.assign_vendor_action",
        required_args=("order_id", "vendor_id"),
        optional_args=("status",),
        tags=("storage", "fulfillment"),
    ),
]

_ACTION_SCHEMA_INDEX = {
    (schema.domain, schema.action): schema for schema in _ACTION_SCHEMAS
}

_DOMAIN_SET = {
    "comm_graph",
    "doc_graph",
    "work_graph",
    "identity_graph",
    "revenue_graph",
    "data_graph",
    "obs_graph",
    "ops_graph",
    "property_graph",
    "campaign_graph",
    "inventory_graph",
}

_DOMAIN_FOCUS = {
    "comm_graph": "slack",
    "doc_graph": "docs",
    "work_graph": "tickets",
    "identity_graph": "identity",
    "revenue_graph": "crm",
    "data_graph": "spreadsheet",
    "obs_graph": "pagerduty",
    "property_graph": "property",
    "campaign_graph": "campaign",
    "inventory_graph": "inventory",
}

_TOOL_FOCUS = {
    "feature_flags": "feature_flags",
    "service_ops": "service_ops",
}


__all__ = [
    "build_graph_action_plan",
    "build_runtime_capability_graphs",
    "get_graph_action_schema",
    "get_runtime_capability_graph",
    "list_graph_action_schemas",
    "resolve_graph_action",
    "validate_graph_action_input",
]
