from __future__ import annotations

from typing import Any, Dict

from .models import LivingSurfaceItem, LivingSurfacePanel, SurfacePanelStatus
from ._surface_panels_shared import build_panel, compact_badges, dict_records, truncate


def build_revenue_panel(
    components: Dict[str, Dict[str, Any]],
    context: Dict[str, Any],
) -> LivingSurfacePanel | None:
    if _runtime_family(context) != "revenue":
        return None
    crm = components.get("crm", {})
    if not isinstance(crm, dict) or not any(
        crm.get(key) for key in ("companies", "contacts", "deals")
    ):
        return None

    companies = dict_records(crm, "companies")
    contacts = dict_records(crm, "contacts")
    deals = dict_records(crm, "deals")
    items: list[LivingSurfaceItem] = []

    for deal_id, deal in list(deals.items())[:3]:
        company = companies.get(str(deal.get("company_id")))
        contact = contacts.get(str(deal.get("contact_id")))
        stage = str(deal.get("stage", "unknown"))
        amount = float(deal.get("amount", 0) or 0)
        owner = str(deal.get("owner", "unassigned"))
        contact_name = ""
        if isinstance(contact, dict):
            first = str(contact.get("first_name", "")).strip()
            last = str(contact.get("last_name", "")).strip()
            contact_name = " ".join(part for part in (first, last) if part)
        items.append(
            LivingSurfaceItem(
                item_id=f"revenue_deal:{deal_id}",
                title=str(deal.get("name", deal_id)),
                subtitle=(
                    str(company.get("name"))
                    if isinstance(company, dict)
                    else str(deal.get("company_id", "account"))
                ),
                body=truncate(
                    f"Stage {stage} · ${amount:,.0f} · owner {owner}"
                    + (f" · contact {contact_name}" if contact_name else ""),
                    180,
                ),
                status=revenue_stage_status(stage),
                badges=compact_badges([stage, f"${amount:,.0f}"]),
                highlight_ref=f"revenue_deal:{deal_id}",
            )
        )

    for contact_id, contact in list(contacts.items())[:2]:
        company = companies.get(str(contact.get("company_id")))
        full_name = (
            " ".join(
                part
                for part in (
                    str(contact.get("first_name", "")).strip(),
                    str(contact.get("last_name", "")).strip(),
                )
                if part
            )
            or contact_id
        )
        do_not_contact = bool(contact.get("do_not_contact"))
        items.append(
            LivingSurfaceItem(
                item_id=f"revenue_contact:{contact_id}",
                title=full_name,
                subtitle=(
                    str(company.get("name"))
                    if isinstance(company, dict)
                    else str(contact.get("company_id", "account"))
                ),
                body=truncate(
                    f"Email {contact.get('email', 'unknown')} · "
                    f"{'do not contact' if do_not_contact else 'active stakeholder'}",
                    180,
                ),
                status=("warning" if do_not_contact else "ok"),
                badges=compact_badges(
                    ["do_not_contact" if do_not_contact else "active"]
                ),
                highlight_ref=f"revenue_contact:{contact_id}",
            )
        )

    return build_panel(
        surface="vertical_heartbeat",
        kind="vertical_heartbeat",
        title="Revenue Pulse",
        accent="#d26a1b",
        headline=(
            f"{len(deals)} deals · {len(companies)} accounts · "
            f"{len(contacts)} contacts"
        ),
        items=items[:6],
    )


def build_service_ops_panel(
    components: Dict[str, Dict[str, Any]],
    context: Dict[str, Any],
) -> LivingSurfacePanel | None:
    if _runtime_family(context) != "service_ops":
        return None
    service_ops = components.get("service_ops", {})
    if not isinstance(service_ops, dict) or not any(
        service_ops.get(key)
        for key in ("work_orders", "appointments", "billing_cases", "exceptions")
    ):
        return None

    items: list[LivingSurfaceItem] = []
    work_orders = dict_records(service_ops, "work_orders")
    appointments = dict_records(service_ops, "appointments")
    technicians = dict_records(service_ops, "technicians")
    billing_cases = dict_records(service_ops, "billing_cases")
    exceptions = dict_records(service_ops, "exceptions")

    for work_order_id, work_order in list(work_orders.items())[:2]:
        appointment = appointments.get(str(work_order.get("appointment_id")))
        technician = technicians.get(str(work_order.get("technician_id")))
        subtitle = (
            str(technician.get("name"))
            if isinstance(technician, dict)
            else "technician unassigned"
        )
        dispatch_status = (
            str(appointment.get("dispatch_status", "pending"))
            if isinstance(appointment, dict)
            else "pending"
        )
        items.append(
            LivingSurfaceItem(
                item_id=f"service_work_order:{work_order_id}",
                title=str(work_order.get("title", work_order_id)),
                subtitle=subtitle,
                body=truncate(
                    f"Dispatch {dispatch_status} · skill {work_order.get('required_skill', 'general')}",
                    160,
                ),
                status=str(work_order.get("status", "")),
                badges=compact_badges(
                    [str(work_order.get("status", "")), dispatch_status]
                ),
                highlight_ref=f"service_work_order:{work_order_id}",
            )
        )

    for billing_case_id, billing_case in list(billing_cases.items())[:2]:
        hold_state = "hold" if billing_case.get("hold") else "live"
        items.append(
            LivingSurfaceItem(
                item_id=f"billing_case:{billing_case_id}",
                title=f"Billing {billing_case_id}",
                subtitle=str(billing_case.get("invoice_id", "invoice")),
                body=truncate(
                    f"Dispute {billing_case.get('dispute_status', 'clear')} · {hold_state}",
                    150,
                ),
                status=("warning" if billing_case.get("hold") else "critical"),
                badges=compact_badges(
                    [str(billing_case.get("dispute_status", "")), hold_state]
                ),
                highlight_ref=f"billing_case:{billing_case_id}",
            )
        )

    for exception_id, issue in list(exceptions.items())[:2]:
        items.append(
            LivingSurfaceItem(
                item_id=f"service_exception:{exception_id}",
                title=str(issue.get("type", exception_id)).replace("_", " ").title(),
                subtitle=str(issue.get("work_order_id", "")),
                body=truncate(
                    f"Severity {issue.get('severity', 'medium')} · status {issue.get('status', 'open')}",
                    150,
                ),
                status=str(issue.get("status", "")),
                badges=compact_badges(
                    [str(issue.get("severity", "")), str(issue.get("status", ""))]
                ),
                highlight_ref=f"service_exception:{exception_id}",
            )
        )

    return build_panel(
        surface="vertical_heartbeat",
        kind="vertical_heartbeat",
        title="Service Loop",
        accent="#1e6cf2",
        headline=(
            f"{len(work_orders)} work orders · {len(appointments)} appointments · "
            f"{len(billing_cases)} billing cases"
        ),
        items=items[:6],
        policy=dict(service_ops.get("policy") or {}),
    )


def build_property_panel(
    components: Dict[str, Dict[str, Any]],
    context: Dict[str, Any],
) -> LivingSurfacePanel | None:
    if _runtime_family(context) != "property":
        return None
    property_ops = components.get("property_ops", {})
    if not isinstance(property_ops, dict) or not any(
        property_ops.get(key) for key in ("leases", "units", "work_orders")
    ):
        return None

    items: list[LivingSurfaceItem] = []
    leases = dict_records(property_ops, "leases")
    units = dict_records(property_ops, "units")
    work_orders = dict_records(property_ops, "work_orders")
    vendors = dict_records(property_ops, "vendors")

    for lease_id, lease in list(leases.items())[:2]:
        unit = units.get(str(lease.get("unit_id")))
        items.append(
            LivingSurfaceItem(
                item_id=f"lease:{lease_id}",
                title=f"Lease {lease_id}",
                subtitle=(
                    str(unit.get("label", lease.get("unit_id", "")))
                    if isinstance(unit, dict)
                    else str(lease.get("unit_id", ""))
                ),
                body=truncate(
                    f"Milestone {lease.get('milestone', 'unknown')} · amendment {'pending' if lease.get('amendment_pending') else 'cleared'}",
                    160,
                ),
                status=str(lease.get("status", "")),
                badges=compact_badges(
                    [str(lease.get("status", "")), str(lease.get("milestone", ""))]
                ),
                highlight_ref=f"lease:{lease_id}",
            )
        )

    for work_order_id, work_order in list(work_orders.items())[:2]:
        vendor = vendors.get(str(work_order.get("vendor_id")))
        items.append(
            LivingSurfaceItem(
                item_id=f"work_order:{work_order_id}",
                title=str(work_order.get("title", work_order_id)),
                subtitle=(
                    str(vendor.get("name"))
                    if isinstance(vendor, dict)
                    else "vendor unassigned"
                ),
                body=truncate(f"Status {work_order.get('status', 'unknown')}", 160),
                status=str(work_order.get("status", "")),
                badges=compact_badges(
                    [str(work_order.get("status", "")), str(work_order_id)]
                ),
                highlight_ref=f"work_order:{work_order_id}",
            )
        )

    for unit_id, unit in list(units.items())[:2]:
        items.append(
            LivingSurfaceItem(
                item_id=f"unit:{unit_id}",
                title=f"Unit {unit.get('label', unit_id)}",
                subtitle=str(unit.get("reserved_for", "open")),
                body=truncate(f"Status {unit.get('status', 'unknown')}", 140),
                status=str(unit.get("status", "")),
                badges=compact_badges([str(unit.get("status", ""))]),
                highlight_ref=f"unit:{unit_id}",
            )
        )

    return build_panel(
        surface="vertical_heartbeat",
        kind="vertical_heartbeat",
        title="Opening Readiness",
        accent="#1e6cf2",
        headline=f"{len(leases)} leases · {len(work_orders)} work orders · {len(units)} units",
        items=items[:6],
    )


def build_campaign_panel(
    components: Dict[str, Dict[str, Any]],
    context: Dict[str, Any],
) -> LivingSurfacePanel | None:
    if _runtime_family(context) != "campaign":
        return None
    campaign_ops = components.get("campaign_ops", {})
    if not isinstance(campaign_ops, dict) or not any(
        campaign_ops.get(key)
        for key in ("campaigns", "creatives", "approvals", "reports")
    ):
        return None

    items: list[LivingSurfaceItem] = []
    campaigns = dict_records(campaign_ops, "campaigns")
    creatives = dict_records(campaign_ops, "creatives")
    approvals = dict_records(campaign_ops, "approvals")
    reports = dict_records(campaign_ops, "reports")

    for campaign_id, campaign in list(campaigns.items())[:2]:
        items.append(
            LivingSurfaceItem(
                item_id=f"campaign:{campaign_id}",
                title=str(campaign.get("name", campaign_id)),
                subtitle=str(campaign.get("channel", "campaign")),
                body=truncate(
                    f"Pacing {campaign.get('pacing_pct', 0)}% · spend ${campaign.get('spend_usd', 0):,} / ${campaign.get('budget_usd', 0):,}",
                    160,
                ),
                status=str(campaign.get("status", "")),
                badges=compact_badges([str(campaign.get("status", ""))]),
                highlight_ref=f"campaign:{campaign_id}",
            )
        )

    for creative_id, creative in list(creatives.items())[:2]:
        items.append(
            LivingSurfaceItem(
                item_id=f"creative:{creative_id}",
                title=str(creative.get("title", creative_id)),
                subtitle=str(creative.get("campaign_id", "")),
                body=truncate(
                    f"Status {creative.get('status', 'unknown')} · approval {'required' if creative.get('approval_required') else 'not required'}",
                    160,
                ),
                status=str(creative.get("status", "")),
                badges=compact_badges([str(creative.get("status", ""))]),
                highlight_ref=f"creative:{creative_id}",
            )
        )

    for approval_id, approval in list(approvals.items())[:1]:
        items.append(
            LivingSurfaceItem(
                item_id=f"approval:{approval_id}",
                title=f"Approval {approval.get('stage', approval_id)}",
                subtitle=str(approval.get("campaign_id", "")),
                body=truncate(f"Status {approval.get('status', 'unknown')}", 140),
                status=str(approval.get("status", "")),
                badges=compact_badges([str(approval.get("status", ""))]),
                highlight_ref=f"approval:{approval_id}",
            )
        )

    for report_id, report in list(reports.items())[:1]:
        items.append(
            LivingSurfaceItem(
                item_id=f"report:{report_id}",
                title=str(report.get("title", report_id)),
                subtitle=str(report.get("campaign_id", "")),
                body=truncate(
                    f"Report is {'stale' if report.get('stale') else 'fresh'}", 140
                ),
                status=str(report.get("status", "")),
                badges=compact_badges([str(report.get("status", ""))]),
                highlight_ref=f"report:{report_id}",
            )
        )

    return build_panel(
        surface="vertical_heartbeat",
        kind="vertical_heartbeat",
        title="Launch Readiness",
        accent="#1e6cf2",
        headline=f"{len(campaigns)} campaigns · {len(creatives)} creatives · {len(approvals)} approvals",
        items=items[:6],
    )


def build_inventory_panel(
    components: Dict[str, Dict[str, Any]],
    context: Dict[str, Any],
) -> LivingSurfacePanel | None:
    if _runtime_family(context) != "inventory":
        return None
    inventory_ops = components.get("inventory_ops", {})
    if not isinstance(inventory_ops, dict) or not any(
        inventory_ops.get(key) for key in ("quotes", "capacity_pools", "orders")
    ):
        return None

    items: list[LivingSurfaceItem] = []
    quotes = dict_records(inventory_ops, "quotes")
    pools = dict_records(inventory_ops, "capacity_pools")
    orders = dict_records(inventory_ops, "orders")
    allocations = dict_records(inventory_ops, "allocations")

    for quote_id, quote in list(quotes.items())[:2]:
        items.append(
            LivingSurfaceItem(
                item_id=f"quote:{quote_id}",
                title=str(quote.get("customer_name", quote_id)),
                subtitle=str(quote_id),
                body=truncate(
                    f"Requested {quote.get('requested_units', 0)} · committed {quote.get('committed_units', 0)}",
                    160,
                ),
                status=str(quote.get("status", "")),
                badges=compact_badges([str(quote.get("status", ""))]),
                highlight_ref=f"quote:{quote_id}",
            )
        )

    for pool_id, pool in list(pools.items())[:2]:
        total = int(pool.get("total_units", 0) or 0)
        reserved = int(pool.get("reserved_units", 0) or 0)
        headroom = max(total - reserved, 0)
        items.append(
            LivingSurfaceItem(
                item_id=f"capacity_pool:{pool_id}",
                title=str(pool.get("name", pool_id)),
                subtitle=str(pool.get("site_id", "")),
                body=truncate(f"Headroom {headroom} of {total} units", 140),
                status=(
                    "critical"
                    if headroom <= 10
                    else "warning" if headroom <= 30 else "ok"
                ),
                badges=compact_badges([f"{reserved}/{total} reserved"]),
                highlight_ref=f"capacity_pool:{pool_id}",
            )
        )

    for order_id, order in list(orders.items())[:1]:
        items.append(
            LivingSurfaceItem(
                item_id=f"order:{order_id}",
                title=f"Order {order_id}",
                subtitle=str(order.get("site_id", "")),
                body=truncate(f"Status {order.get('status', 'unknown')}", 140),
                status=str(order.get("status", "")),
                badges=compact_badges([str(order.get("status", ""))]),
                highlight_ref=f"order:{order_id}",
            )
        )

    for allocation_id, allocation in list(allocations.items())[:1]:
        items.append(
            LivingSurfaceItem(
                item_id=f"allocation:{allocation_id}",
                title=f"Allocation {allocation_id}",
                subtitle=str(allocation.get("pool_id", "")),
                body=truncate(f"{allocation.get('units', 0)} units reserved", 140),
                status=str(allocation.get("status", "")),
                badges=compact_badges([str(allocation.get("status", ""))]),
                highlight_ref=f"allocation:{allocation_id}",
            )
        )

    return build_panel(
        surface="vertical_heartbeat",
        kind="vertical_heartbeat",
        title="Commitment Readiness",
        accent="#1e6cf2",
        headline=f"{len(quotes)} quotes · {len(pools)} pools · {len(orders)} orders",
        items=items[:6],
    )


def revenue_stage_status(stage: str) -> SurfacePanelStatus:
    normalized = stage.strip().lower()
    if normalized in {"at_risk", "blocked", "closed_lost"}:
        return "critical"
    if normalized in {"negotiation", "stalled", "evaluation"}:
        return "warning"
    return "ok"


def _runtime_family(context: Dict[str, Any]) -> str:
    return str(context.get("vertical_runtime_family", "")).strip().lower()
