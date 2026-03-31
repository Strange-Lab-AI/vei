from __future__ import annotations

from pathlib import Path
from typing import Any, Dict

from vei.orientation.api import build_world_orientation
from vei.world.models import WorldState
from vei.workspace.api import load_workspace

from .models import (
    LivingSurfaceItem,
    LivingSurfacePanel,
    LivingSurfaceState,
    RunManifest,
    RunSnapshotRef,
    SurfacePanelKind,
    SurfacePanelStatus,
)


def build_surface_state(
    *,
    workspace_root: Path,
    run_id: str,
    state: WorldState,
    run_manifest: RunManifest,
    snapshots: list[RunSnapshotRef],
) -> LivingSurfaceState:
    workspace = load_workspace(workspace_root)
    orientation = build_world_orientation(state)
    metadata = _scenario_metadata(state)
    vertical_name = _vertical_name(metadata, workspace)
    company_name = orientation.organization_name or workspace.title or workspace.name
    current_tension = _current_tension(metadata, state, orientation)

    panels = [
        _build_chat_panel(state.components.get("slack", {})),
        _build_mail_panel(state.components.get("mail", {})),
        _build_ticket_panel(state.components.get("tickets", {})),
        _build_docs_panel(
            state.components.get("docs", {}),
            state.components.get("google_admin", {}),
        ),
        _build_approval_panel(state.components.get("servicedesk", {})),
        _build_vertical_panel(vertical_name, state.components),
    ]

    return LivingSurfaceState(
        company_name=company_name,
        vertical_name=vertical_name,
        run_id=run_id,
        branch=run_manifest.branch or state.branch,
        snapshot_id=(snapshots[-1].snapshot_id if snapshots else 0),
        current_tension=current_tension,
        panels=[p for p in panels if p is not None],
    )


# ---------------------------------------------------------------------------
# Metadata helpers
# ---------------------------------------------------------------------------


def _scenario_metadata(state: WorldState) -> Dict[str, Any]:
    metadata = state.scenario.get("metadata")
    return metadata if isinstance(metadata, dict) else {}


def _builder_env(metadata: Dict[str, Any]) -> Dict[str, Any]:
    env = metadata.get("builder_environment")
    return env if isinstance(env, dict) else {}


def _builder_graphs_meta(metadata: Dict[str, Any]) -> Dict[str, Any]:
    graphs = metadata.get("builder_capability_graphs")
    if isinstance(graphs, dict):
        inner = graphs.get("metadata")
        return inner if isinstance(inner, dict) else graphs
    return {}


def _vertical_name(metadata: Dict[str, Any], workspace: Any) -> str:
    for source in (_builder_env(metadata), _builder_graphs_meta(metadata)):
        vertical = source.get("vertical")
        if isinstance(vertical, str) and vertical:
            return vertical
    if isinstance(getattr(workspace, "source_ref", None), str) and workspace.source_ref:
        return workspace.source_ref
    return "workspace"


def _current_tension(
    metadata: Dict[str, Any], state: WorldState, orientation: Any
) -> str:
    for source in (_builder_env(metadata), _builder_graphs_meta(metadata)):
        brief = source.get("scenario_brief")
        if isinstance(brief, str) and brief:
            return brief
    description = state.scenario.get("description")
    if isinstance(description, str) and description:
        return description
    return orientation.summary


# ---------------------------------------------------------------------------
# Panel builders
# ---------------------------------------------------------------------------


def _build_chat_panel(slack: Dict[str, Any]) -> LivingSurfacePanel | None:
    channels = slack.get("channels")
    if not isinstance(channels, dict) or not channels:
        return None

    rows: list[tuple[float, str, Dict[str, Any], int]] = []
    for channel_name, payload in channels.items():
        if not isinstance(payload, dict):
            continue
        unread = int(payload.get("unread", 0) or 0)
        for message in payload.get("messages", []):
            if isinstance(message, dict):
                rows.append(
                    (_slack_ts(message.get("ts")), channel_name, message, unread)
                )

    if not rows:
        return None

    rows.sort(key=lambda item: item[0], reverse=True)
    items = [
        LivingSurfaceItem(
            item_id=f"chat:{channel_name}:{message.get('ts', index)}",
            title=str(message.get("user", "team member")),
            subtitle=channel_name,
            body=_truncate(str(message.get("text", "")), 160),
            status=("attention" if unread else "ok"),
            badges=_compact_badges(
                [
                    channel_name,
                    "thread reply" if message.get("thread_ts") else "",
                    f"{unread} unread" if unread else "",
                ]
            ),
            highlight_ref=f"slack:{channel_name}:{message.get('ts', index)}",
        )
        for index, (_, channel_name, message, unread) in enumerate(rows[:8], start=1)
    ]
    unread_total = sum(
        int(payload.get("unread", 0) or 0)
        for payload in channels.values()
        if isinstance(payload, dict)
    )
    return _panel(
        surface="slack",
        kind="chat",
        title="Team Chat",
        accent="#36c5f0",
        headline=f"{len(channels)} channels · {len(rows)} messages",
        items=items,
        fallback_status=("attention" if unread_total else "ok"),
    )


def _build_mail_panel(mail: Dict[str, Any]) -> LivingSurfacePanel | None:
    messages = mail.get("messages")
    if not isinstance(messages, dict) or not messages:
        return None

    threads: Dict[str, list[Dict[str, Any]]] = {}
    for message in messages.values():
        if not isinstance(message, dict):
            continue
        thread_id = str(message.get("thread_id") or message.get("subj") or "mail")
        threads.setdefault(thread_id, []).append(message)

    if not threads:
        return None

    rows: list[tuple[int, str, list[Dict[str, Any]]]] = []
    for thread_id, thread_messages in threads.items():
        latest_time = max(int(m.get("time_ms", 0) or 0) for m in thread_messages)
        rows.append((latest_time, thread_id, thread_messages))
    rows.sort(key=lambda item: item[0], reverse=True)

    items = []
    unread_total = 0
    for _, thread_id, thread_messages in rows[:6]:
        ordered = sorted(
            thread_messages,
            key=lambda m: int(m.get("time_ms", 0) or 0),
            reverse=True,
        )
        latest = ordered[0]
        unread_count = sum(1 for m in thread_messages if m.get("unread"))
        unread_total += unread_count
        items.append(
            LivingSurfaceItem(
                item_id=f"mail:{thread_id}",
                title=str(latest.get("subj", thread_id)),
                subtitle=str(latest.get("from", "inbox")),
                body=_truncate(str(latest.get("body_text", "")), 160),
                status=("attention" if unread_count else "ok"),
                badges=_compact_badges(
                    [
                        str(latest.get("category", "")),
                        f"{len(thread_messages)} messages",
                        f"{unread_count} unread" if unread_count else "",
                    ]
                ),
                highlight_ref=f"mail:{thread_id}",
            )
        )

    return _panel(
        surface="mail",
        kind="mail",
        title="Email",
        accent="#ffb454",
        headline=f"{len(threads)} threads · {unread_total} unread",
        items=items,
        fallback_status=("attention" if unread_total else "ok"),
    )


def _build_ticket_panel(tickets: Dict[str, Any]) -> LivingSurfacePanel | None:
    payload = tickets.get("tickets")
    if not isinstance(payload, dict) or not payload:
        return None

    ordered = sorted(
        (item for item in payload.values() if isinstance(item, dict)),
        key=lambda item: (
            _ticket_sort_rank(str(item.get("status", ""))),
            str(item.get("ticket_id", "")),
        ),
    )
    items = [
        LivingSurfaceItem(
            item_id=f"ticket:{item.get('ticket_id', index)}",
            title=str(item.get("title", item.get("ticket_id", "ticket"))),
            subtitle=str(item.get("assignee", "unassigned")),
            body=_truncate(str(item.get("description", "")), 140),
            status=str(item.get("status", "")),
            badges=_compact_badges(
                [str(item.get("status", "")), str(item.get("ticket_id", ""))]
            ),
            highlight_ref=f"ticket:{item.get('ticket_id', index)}",
        )
        for index, item in enumerate(ordered[:8], start=1)
    ]
    return _panel(
        surface="tickets",
        kind="queue",
        title="Work Tracker",
        accent="#ff6d5e",
        headline=f"{len(payload)} active tickets",
        items=items,
    )


def _build_docs_panel(
    docs: Dict[str, Any],
    google_admin: Dict[str, Any],
) -> LivingSurfacePanel | None:
    payload = _dict_records(docs, "docs")
    if not payload:
        return None

    metadata = _dict_records(docs, "metadata")
    shares = _dict_records(google_admin, "drive_shares")
    ordered = sorted(
        payload.values(),
        key=lambda item: int(
            metadata.get(str(item.get("doc_id")), {}).get("updated_ms", 0) or 0
        ),
        reverse=True,
    )

    items = []
    for index, item in enumerate(ordered[:6], start=1):
        doc_id = str(item.get("doc_id", index))
        share = shares.get(doc_id)
        tags = (
            [str(tag) for tag in item.get("tags", [])]
            if isinstance(item.get("tags"), list)
            else []
        )
        items.append(
            LivingSurfaceItem(
                item_id=f"doc:{doc_id}",
                title=str(item.get("title", doc_id)),
                subtitle=doc_id,
                body=_truncate(str(item.get("body", "")), 160),
                status="ok",
                badges=_compact_badges(
                    tags[:2]
                    + (
                        [str(share.get("visibility", ""))]
                        if isinstance(share, dict)
                        else []
                    )
                ),
                highlight_ref=f"doc:{doc_id}",
            )
        )

    return _panel(
        surface="docs",
        kind="document",
        title="Documents",
        accent="#1aa88d",
        headline=f"{len(payload)} artifacts in circulation",
        items=items,
    )


def _build_approval_panel(
    servicedesk: Dict[str, Any],
) -> LivingSurfacePanel | None:
    requests = _dict_records(servicedesk, "requests")
    if not requests:
        return None

    ordered = sorted(
        requests.values(),
        key=lambda item: (
            _approval_sort_rank(str(item.get("status", ""))),
            str(item.get("request_id", "")),
        ),
    )
    items = []
    for index, item in enumerate(ordered[:8], start=1):
        approval_list = _dict_list(item, "approvals")
        pending_count = sum(
            1 for a in approval_list if str(a.get("status", "")).upper() == "PENDING"
        )
        items.append(
            LivingSurfaceItem(
                item_id=f"request:{item.get('request_id', index)}",
                title=str(item.get("title", item.get("request_id", "request"))),
                subtitle=str(item.get("requester", "requester")),
                body=_truncate(str(item.get("description", "")), 140),
                status=str(item.get("status", "")),
                badges=_compact_badges(
                    [
                        str(item.get("status", "")),
                        f"{pending_count} pending" if pending_count else "",
                    ]
                ),
                highlight_ref=f"request:{item.get('request_id', index)}",
            )
        )

    pending_total = sum(
        1
        for item in ordered
        if str(item.get("status", "")).lower()
        in {"pending_approval", "pending", "review"}
    )
    return _panel(
        surface="approvals",
        kind="approval",
        title="Approvals",
        accent="#9b7bff",
        headline=f"{len(requests)} routed requests",
        items=items,
        fallback_status=("warning" if pending_total else "ok"),
    )


# ---------------------------------------------------------------------------
# Vertical-specific panels
# ---------------------------------------------------------------------------


def _build_vertical_panel(
    vertical_name: str,
    components: Dict[str, Dict[str, Any]],
) -> LivingSurfacePanel | None:
    if vertical_name == "service_ops":
        return _build_service_ops_panel(components.get("service_ops", {}))
    if vertical_name == "real_estate_management":
        return _build_property_panel(components.get("property_ops", {}))
    if vertical_name == "digital_marketing_agency":
        return _build_campaign_panel(components.get("campaign_ops", {}))
    if vertical_name == "storage_solutions":
        return _build_inventory_panel(components.get("inventory_ops", {}))
    return None


def _build_service_ops_panel(
    service_ops: Dict[str, Any],
) -> LivingSurfacePanel | None:
    if not isinstance(service_ops, dict) or not any(
        service_ops.get(key)
        for key in ("work_orders", "appointments", "billing_cases", "exceptions")
    ):
        return None

    items: list[LivingSurfaceItem] = []
    work_orders = _dict_records(service_ops, "work_orders")
    appointments = _dict_records(service_ops, "appointments")
    technicians = _dict_records(service_ops, "technicians")
    billing_cases = _dict_records(service_ops, "billing_cases")
    exceptions = _dict_records(service_ops, "exceptions")

    for work_order_id, work_order in list(work_orders.items())[:2]:
        if not isinstance(work_order, dict):
            continue
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
                body=_truncate(
                    f"Dispatch {dispatch_status} · skill {work_order.get('required_skill', 'general')}",
                    160,
                ),
                status=str(work_order.get("status", "")),
                badges=_compact_badges(
                    [str(work_order.get("status", "")), dispatch_status]
                ),
                highlight_ref=f"service_work_order:{work_order_id}",
            )
        )

    for billing_case_id, billing_case in list(billing_cases.items())[:2]:
        if not isinstance(billing_case, dict):
            continue
        hold_state = "hold" if billing_case.get("hold") else "live"
        items.append(
            LivingSurfaceItem(
                item_id=f"billing_case:{billing_case_id}",
                title=f"Billing {billing_case_id}",
                subtitle=str(billing_case.get("invoice_id", "invoice")),
                body=_truncate(
                    f"Dispute {billing_case.get('dispute_status', 'clear')} · {hold_state}",
                    150,
                ),
                status=("warning" if billing_case.get("hold") else "critical"),
                badges=_compact_badges(
                    [str(billing_case.get("dispute_status", "")), hold_state]
                ),
                highlight_ref=f"billing_case:{billing_case_id}",
            )
        )

    for exception_id, issue in list(exceptions.items())[:2]:
        if not isinstance(issue, dict):
            continue
        items.append(
            LivingSurfaceItem(
                item_id=f"service_exception:{exception_id}",
                title=str(issue.get("type", exception_id)).replace("_", " ").title(),
                subtitle=str(issue.get("work_order_id", "")),
                body=_truncate(
                    f"Severity {issue.get('severity', 'medium')} · status {issue.get('status', 'open')}",
                    150,
                ),
                status=str(issue.get("status", "")),
                badges=_compact_badges(
                    [str(issue.get("severity", "")), str(issue.get("status", ""))]
                ),
                highlight_ref=f"service_exception:{exception_id}",
            )
        )

    return _panel(
        surface="vertical_heartbeat",
        kind="vertical_heartbeat",
        title="Service Loop",
        accent="#1e6cf2",
        headline=(
            f"{len(work_orders)} work orders · {len(appointments)} appointments · "
            f"{len(billing_cases)} billing cases"
        ),
        items=items[:6],
    )


def _build_property_panel(
    property_ops: Dict[str, Any],
) -> LivingSurfacePanel | None:
    if not isinstance(property_ops, dict) or not any(
        property_ops.get(k) for k in ("leases", "units", "work_orders")
    ):
        return None

    items: list[LivingSurfaceItem] = []
    leases = _dict_records(property_ops, "leases")
    units = _dict_records(property_ops, "units")
    work_orders = _dict_records(property_ops, "work_orders")
    vendors = _dict_records(property_ops, "vendors")

    for lease_id, lease in list(leases.items())[:2]:
        if not isinstance(lease, dict):
            continue
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
                body=_truncate(
                    f"Milestone {lease.get('milestone', 'unknown')} · amendment {'pending' if lease.get('amendment_pending') else 'cleared'}",
                    160,
                ),
                status=str(lease.get("status", "")),
                badges=_compact_badges(
                    [str(lease.get("status", "")), str(lease.get("milestone", ""))]
                ),
                highlight_ref=f"lease:{lease_id}",
            )
        )

    for wo_id, wo in list(work_orders.items())[:2]:
        if not isinstance(wo, dict):
            continue
        vendor = vendors.get(str(wo.get("vendor_id")))
        items.append(
            LivingSurfaceItem(
                item_id=f"work_order:{wo_id}",
                title=str(wo.get("title", wo_id)),
                subtitle=(
                    str(vendor.get("name"))
                    if isinstance(vendor, dict)
                    else "vendor unassigned"
                ),
                body=_truncate(f"Status {wo.get('status', 'unknown')}", 160),
                status=str(wo.get("status", "")),
                badges=_compact_badges([str(wo.get("status", "")), str(wo_id)]),
                highlight_ref=f"work_order:{wo_id}",
            )
        )

    for unit_id, unit in list(units.items())[:2]:
        if not isinstance(unit, dict):
            continue
        items.append(
            LivingSurfaceItem(
                item_id=f"unit:{unit_id}",
                title=f"Unit {unit.get('label', unit_id)}",
                subtitle=str(unit.get("reserved_for", "open")),
                body=_truncate(f"Status {unit.get('status', 'unknown')}", 140),
                status=str(unit.get("status", "")),
                badges=_compact_badges([str(unit.get("status", ""))]),
                highlight_ref=f"unit:{unit_id}",
            )
        )

    return _panel(
        surface="vertical_heartbeat",
        kind="vertical_heartbeat",
        title="Opening Readiness",
        accent="#1e6cf2",
        headline=f"{len(leases)} leases · {len(work_orders)} work orders · {len(units)} units",
        items=items[:6],
    )


def _build_campaign_panel(
    campaign_ops: Dict[str, Any],
) -> LivingSurfacePanel | None:
    if not isinstance(campaign_ops, dict) or not any(
        campaign_ops.get(k) for k in ("campaigns", "creatives", "approvals", "reports")
    ):
        return None

    items: list[LivingSurfaceItem] = []
    campaigns = _dict_records(campaign_ops, "campaigns")
    creatives = _dict_records(campaign_ops, "creatives")
    approvals = _dict_records(campaign_ops, "approvals")
    reports = _dict_records(campaign_ops, "reports")

    for cid, c in list(campaigns.items())[:2]:
        if not isinstance(c, dict):
            continue
        items.append(
            LivingSurfaceItem(
                item_id=f"campaign:{cid}",
                title=str(c.get("name", cid)),
                subtitle=str(c.get("channel", "campaign")),
                body=_truncate(
                    f"Pacing {c.get('pacing_pct', 0)}% · spend ${c.get('spend_usd', 0):,} / ${c.get('budget_usd', 0):,}",
                    160,
                ),
                status=str(c.get("status", "")),
                badges=_compact_badges([str(c.get("status", ""))]),
                highlight_ref=f"campaign:{cid}",
            )
        )

    for crid, cr in list(creatives.items())[:2]:
        if not isinstance(cr, dict):
            continue
        items.append(
            LivingSurfaceItem(
                item_id=f"creative:{crid}",
                title=str(cr.get("title", crid)),
                subtitle=str(cr.get("campaign_id", "")),
                body=_truncate(
                    f"Status {cr.get('status', 'unknown')} · approval {'required' if cr.get('approval_required') else 'not required'}",
                    160,
                ),
                status=str(cr.get("status", "")),
                badges=_compact_badges([str(cr.get("status", ""))]),
                highlight_ref=f"creative:{crid}",
            )
        )

    for aid, a in list(approvals.items())[:1]:
        if not isinstance(a, dict):
            continue
        items.append(
            LivingSurfaceItem(
                item_id=f"approval:{aid}",
                title=f"Approval {a.get('stage', aid)}",
                subtitle=str(a.get("campaign_id", "")),
                body=_truncate(f"Status {a.get('status', 'unknown')}", 140),
                status=str(a.get("status", "")),
                badges=_compact_badges([str(a.get("status", ""))]),
                highlight_ref=f"approval:{aid}",
            )
        )

    for rid, r in list(reports.items())[:1]:
        if not isinstance(r, dict):
            continue
        items.append(
            LivingSurfaceItem(
                item_id=f"report:{rid}",
                title=str(r.get("title", rid)),
                subtitle=str(r.get("campaign_id", "")),
                body=_truncate(
                    f"Report is {'stale' if r.get('stale') else 'fresh'}", 140
                ),
                status=str(r.get("status", "")),
                badges=_compact_badges([str(r.get("status", ""))]),
                highlight_ref=f"report:{rid}",
            )
        )

    return _panel(
        surface="vertical_heartbeat",
        kind="vertical_heartbeat",
        title="Launch Readiness",
        accent="#1e6cf2",
        headline=f"{len(campaigns)} campaigns · {len(creatives)} creatives · {len(approvals)} approvals",
        items=items[:6],
    )


def _build_inventory_panel(
    inventory_ops: Dict[str, Any],
) -> LivingSurfacePanel | None:
    if not isinstance(inventory_ops, dict) or not any(
        inventory_ops.get(k) for k in ("quotes", "capacity_pools", "orders")
    ):
        return None

    items: list[LivingSurfaceItem] = []
    quotes = _dict_records(inventory_ops, "quotes")
    pools = _dict_records(inventory_ops, "capacity_pools")
    orders = _dict_records(inventory_ops, "orders")
    allocations = _dict_records(inventory_ops, "allocations")

    for qid, q in list(quotes.items())[:2]:
        if not isinstance(q, dict):
            continue
        items.append(
            LivingSurfaceItem(
                item_id=f"quote:{qid}",
                title=str(q.get("customer_name", qid)),
                subtitle=str(qid),
                body=_truncate(
                    f"Requested {q.get('requested_units', 0)} · committed {q.get('committed_units', 0)}",
                    160,
                ),
                status=str(q.get("status", "")),
                badges=_compact_badges([str(q.get("status", ""))]),
                highlight_ref=f"quote:{qid}",
            )
        )

    for pid, p in list(pools.items())[:2]:
        if not isinstance(p, dict):
            continue
        total = int(p.get("total_units", 0) or 0)
        reserved = int(p.get("reserved_units", 0) or 0)
        headroom = max(total - reserved, 0)
        items.append(
            LivingSurfaceItem(
                item_id=f"capacity_pool:{pid}",
                title=str(p.get("name", pid)),
                subtitle=str(p.get("site_id", "")),
                body=_truncate(f"Headroom {headroom} of {total} units", 140),
                status=(
                    "critical"
                    if headroom <= 10
                    else "warning" if headroom <= 30 else "ok"
                ),
                badges=_compact_badges([f"{reserved}/{total} reserved"]),
                highlight_ref=f"capacity_pool:{pid}",
            )
        )

    for oid, o in list(orders.items())[:1]:
        if not isinstance(o, dict):
            continue
        items.append(
            LivingSurfaceItem(
                item_id=f"order:{oid}",
                title=f"Order {oid}",
                subtitle=str(o.get("site_id", "")),
                body=_truncate(f"Status {o.get('status', 'unknown')}", 140),
                status=str(o.get("status", "")),
                badges=_compact_badges([str(o.get("status", ""))]),
                highlight_ref=f"order:{oid}",
            )
        )

    for alloc_id, alloc in list(allocations.items())[:1]:
        if not isinstance(alloc, dict):
            continue
        items.append(
            LivingSurfaceItem(
                item_id=f"allocation:{alloc_id}",
                title=f"Allocation {alloc_id}",
                subtitle=str(alloc.get("pool_id", "")),
                body=_truncate(f"{alloc.get('units', 0)} units reserved", 140),
                status=str(alloc.get("status", "")),
                badges=_compact_badges([str(alloc.get("status", ""))]),
                highlight_ref=f"allocation:{alloc_id}",
            )
        )

    return _panel(
        surface="vertical_heartbeat",
        kind="vertical_heartbeat",
        title="Commitment Readiness",
        accent="#1e6cf2",
        headline=f"{len(quotes)} quotes · {len(pools)} pools · {len(orders)} orders",
        items=items[:6],
    )


# ---------------------------------------------------------------------------
# Shared utilities
# ---------------------------------------------------------------------------


def _panel(
    *,
    surface: str,
    kind: SurfacePanelKind,
    title: str,
    accent: str,
    headline: str,
    items: list[LivingSurfaceItem],
    fallback_status: SurfacePanelStatus | str | None = None,
) -> LivingSurfacePanel | None:
    if not items:
        return None
    return LivingSurfacePanel(
        surface=surface,
        kind=kind,
        title=title,
        accent=accent,
        status=_aggregate_status(items, fallback=fallback_status or "ok"),
        headline=headline,
        items=items,
        highlight_refs=[
            item.highlight_ref for item in items if item.highlight_ref is not None
        ],
    )


def _aggregate_status(
    items: list[LivingSurfaceItem],
    *,
    fallback: SurfacePanelStatus | str,
) -> SurfacePanelStatus:
    levels = [str(item.status or "").lower() for item in items]
    if any(
        level in {"critical", "pending_vendor", "stale", "launch_risk"}
        for level in levels
    ):
        return "critical"
    if any(
        level in {"warning", "pending", "pending_approval", "review"}
        for level in levels
    ):
        return "warning"
    if any(
        level in {"attention", "open", "in_progress", "scheduled", "draft"}
        for level in levels
    ):
        return "attention"
    if fallback in ("ok", "attention", "warning", "critical"):
        return fallback  # type: ignore[return-value]
    return "ok"


def _ticket_sort_rank(status: str) -> int:
    normalized = status.lower()
    if normalized in {"open", "pending"}:
        return 0
    if normalized in {"in_progress", "review"}:
        return 1
    if normalized in {"scheduled", "ready"}:
        return 2
    return 3


def _approval_sort_rank(status: str) -> int:
    normalized = status.lower()
    if normalized in {"pending_approval", "pending"}:
        return 0
    if normalized in {"in_progress", "review"}:
        return 1
    if normalized in {"approved", "complete"}:
        return 2
    return 3


def _dict_records(payload: Dict[str, Any], key: str) -> Dict[str, Dict[str, Any]]:
    value = payload.get(key)
    if not isinstance(value, dict):
        return {}
    return {str(k): item for k, item in value.items() if isinstance(item, dict)}


def _dict_list(payload: Dict[str, Any], key: str) -> list[Dict[str, Any]]:
    value = payload.get(key)
    if not isinstance(value, list):
        return []
    return [item for item in value if isinstance(item, dict)]


def _compact_badges(values: list[str]) -> list[str]:
    return [v for v in values if v]


def _truncate(value: str, limit: int) -> str:
    if len(value) <= limit:
        return value
    return f"{value[: limit - 1].rstrip()}\u2026"


def _slack_ts(value: Any) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0
