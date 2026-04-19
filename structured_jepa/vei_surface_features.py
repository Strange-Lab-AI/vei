from __future__ import annotations

from collections.abc import Iterable
from typing import Any

from .utils import normalize_token


def summarize_snapshot_surface_features(
    state_payload: object,
) -> tuple[dict[str, float], dict[str, object]]:
    numeric_summary: dict[str, float] = {
        "surface_panel_count__all": 0.0,
        "surface_item_count__all": 0.0,
    }
    categorical_summary: dict[str, object] = {
        "surface_primary_panel": "__none__",
        "surface_primary_status": "__none__",
    }
    if not isinstance(state_payload, dict):
        return numeric_summary, categorical_summary

    components = state_payload.get("components", {})
    if not isinstance(components, dict):
        return numeric_summary, categorical_summary

    panels: list[tuple[str, str, int]] = []
    _summarize_slack_panel(components.get("slack"), numeric_summary, panels)
    _summarize_mail_panel(components.get("mail"), numeric_summary, panels)
    _summarize_ticket_panel(components.get("tickets"), numeric_summary, panels)
    _summarize_docs_panel(
        components.get("docs"),
        components.get("google_admin"),
        numeric_summary,
        panels,
    )
    _summarize_approval_panel(components.get("servicedesk"), numeric_summary, panels)
    _summarize_vertical_panel(components, numeric_summary, panels)

    if not panels:
        return numeric_summary, categorical_summary

    primary_panel = max(panels, key=_panel_rank)
    categorical_summary["surface_primary_panel"] = primary_panel[0]
    categorical_summary["surface_primary_status"] = primary_panel[1]
    return numeric_summary, categorical_summary


def _summarize_slack_panel(
    payload: object,
    summary: dict[str, float],
    panels: list[tuple[str, str, int]],
) -> None:
    if not isinstance(payload, dict):
        return
    channels = payload.get("channels", {})
    if not isinstance(channels, dict) or not channels:
        return

    channel_count = len(channels)
    message_count = 0
    unread_total = 0
    for channel_payload in channels.values():
        if not isinstance(channel_payload, dict):
            continue
        unread_total += _int_value(channel_payload.get("unread", 0))
        messages = channel_payload.get("messages", [])
        if isinstance(messages, list):
            message_count += sum(1 for item in messages if isinstance(item, dict))

    if message_count <= 0:
        return

    summary["surface_item_count__slack_channels"] = float(channel_count)
    summary["surface_item_count__slack_messages"] = float(message_count)
    summary["surface_signal__slack_unread"] = float(unread_total)
    status = "attention" if unread_total > 0 else "ok"
    _record_panel(summary, panels, panel_name="slack", status=status, item_count=message_count)


def _summarize_mail_panel(
    payload: object,
    summary: dict[str, float],
    panels: list[tuple[str, str, int]],
) -> None:
    if not isinstance(payload, dict):
        return
    messages = payload.get("messages", {})
    if not isinstance(messages, dict) or not messages:
        return

    thread_ids = {
        str(message.get("thread_id") or message.get("subj") or "mail")
        for message in messages.values()
        if isinstance(message, dict)
    }
    thread_count = len(thread_ids)
    message_count = sum(1 for message in messages.values() if isinstance(message, dict))
    unread_total = sum(
        1 for message in messages.values() if isinstance(message, dict) and message.get("unread")
    )
    if thread_count <= 0:
        return

    summary["surface_item_count__mail_threads"] = float(thread_count)
    summary["surface_item_count__mail_messages"] = float(message_count)
    summary["surface_signal__mail_unread"] = float(unread_total)
    status = "attention" if unread_total > 0 else "ok"
    _record_panel(summary, panels, panel_name="mail", status=status, item_count=thread_count)


def _summarize_ticket_panel(
    payload: object,
    summary: dict[str, float],
    panels: list[tuple[str, str, int]],
) -> None:
    if not isinstance(payload, dict):
        return
    tickets = _dict_records(payload, "tickets")
    if not tickets:
        return

    status_counts = _status_counts(tickets.values())
    unresolved_count = sum(
        count for status, count in status_counts.items() if status not in _RESOLVED_STATUSES
    )
    summary["surface_item_count__tickets"] = float(len(tickets))
    summary["surface_signal__tickets_unresolved"] = float(unresolved_count)
    for status, count in status_counts.items():
        summary[f"surface_item_count__tickets_status__{status}"] = float(count)

    status = "attention" if unresolved_count > 0 else "ok"
    _record_panel(summary, panels, panel_name="tickets", status=status, item_count=len(tickets))


def _summarize_docs_panel(
    docs_payload: object,
    google_admin_payload: object,
    summary: dict[str, float],
    panels: list[tuple[str, str, int]],
) -> None:
    if not isinstance(docs_payload, dict):
        return
    docs = _dict_records(docs_payload, "docs")
    if not docs:
        return

    shares = (
        _dict_records(google_admin_payload, "drive_shares")
        if isinstance(google_admin_payload, dict)
        else {}
    )
    summary["surface_item_count__docs"] = float(len(docs))
    summary["surface_signal__docs_shared"] = float(len(shares))
    _record_panel(summary, panels, panel_name="docs", status="ok", item_count=len(docs))


def _summarize_approval_panel(
    payload: object,
    summary: dict[str, float],
    panels: list[tuple[str, str, int]],
) -> None:
    if not isinstance(payload, dict):
        return
    requests = _dict_records(payload, "requests")
    if not requests:
        return

    pending_count = 0
    for request in requests.values():
        if not isinstance(request, dict):
            continue
        request_status = normalize_token(request.get("status"), default="").lower()
        if request_status in {"pending_approval", "pending", "review"}:
            pending_count += 1
        approvals = request.get("approvals", [])
        if isinstance(approvals, list):
            pending_count += sum(
                1
                for approval in approvals
                if isinstance(approval, dict)
                and normalize_token(approval.get("status"), default="").lower() == "pending"
            )

    summary["surface_item_count__approvals_requests"] = float(len(requests))
    summary["surface_signal__approvals_pending"] = float(pending_count)
    status = "warning" if pending_count > 0 else "ok"
    _record_panel(summary, panels, panel_name="approvals", status=status, item_count=len(requests))


def _summarize_vertical_panel(
    components: dict[str, Any],
    summary: dict[str, float],
    panels: list[tuple[str, str, int]],
) -> None:
    if _summarize_service_ops_panel(components.get("service_ops"), summary, panels):
        return
    if _summarize_property_panel(components.get("property_ops"), summary, panels):
        return
    if _summarize_campaign_panel(components.get("campaign_ops"), summary, panels):
        return
    _summarize_inventory_panel(components.get("inventory_ops"), summary, panels)


def _summarize_service_ops_panel(
    payload: object,
    summary: dict[str, float],
    panels: list[tuple[str, str, int]],
) -> bool:
    if not isinstance(payload, dict):
        return False
    work_orders = _dict_records(payload, "work_orders")
    appointments = _dict_records(payload, "appointments")
    billing_cases = _dict_records(payload, "billing_cases")
    exceptions = _dict_records(payload, "exceptions")
    if not work_orders and not appointments and not billing_cases and not exceptions:
        return False

    assigned_appointments = 0
    for appointment in appointments.values():
        if not isinstance(appointment, dict):
            continue
        dispatch_status = normalize_token(
            appointment.get("dispatch_status"),
            default="pending",
        ).lower()
        if dispatch_status == "assigned":
            assigned_appointments += 1

    holds_or_disputes = 0
    for billing_case in billing_cases.values():
        if not isinstance(billing_case, dict):
            continue
        if bool(billing_case.get("hold")):
            holds_or_disputes += 1
            continue
        dispute_status = normalize_token(
            billing_case.get("dispute_status"),
            default="clear",
        ).lower()
        if dispute_status not in {"clear", "resolved", "closed"}:
            holds_or_disputes += 1

    open_exceptions = 0
    critical_exceptions = 0
    for issue in exceptions.values():
        if not isinstance(issue, dict):
            continue
        status = normalize_token(issue.get("status"), default="open").lower()
        if status not in _RESOLVED_STATUSES:
            open_exceptions += 1
        severity = normalize_token(issue.get("severity"), default="medium").lower()
        if severity in {"critical", "high"}:
            critical_exceptions += 1

    summary["surface_item_count__service_ops_work_orders"] = float(len(work_orders))
    summary["surface_item_count__service_ops_appointments"] = float(len(appointments))
    summary["surface_item_count__service_ops_billing_cases"] = float(len(billing_cases))
    summary["surface_item_count__service_ops_exceptions"] = float(len(exceptions))
    summary["surface_signal__service_ops_assigned_appointments"] = float(assigned_appointments)
    summary["surface_signal__service_ops_holds_or_disputes"] = float(holds_or_disputes)
    summary["surface_signal__service_ops_open_exceptions"] = float(open_exceptions)
    summary["surface_signal__service_ops_critical_exceptions"] = float(critical_exceptions)

    item_count = len(work_orders) + len(appointments) + len(billing_cases) + len(exceptions)
    if critical_exceptions > 0 or holds_or_disputes > 0:
        status = "critical"
    elif open_exceptions > 0:
        status = "warning"
    elif len(appointments) > 0 and assigned_appointments < len(appointments):
        status = "warning"
    else:
        status = "ok"
    _record_panel(
        summary,
        panels,
        panel_name="vertical_service_ops",
        status=status,
        item_count=item_count,
    )
    return True


def _summarize_property_panel(
    payload: object,
    summary: dict[str, float],
    panels: list[tuple[str, str, int]],
) -> bool:
    if not isinstance(payload, dict):
        return False
    leases = _dict_records(payload, "leases")
    units = _dict_records(payload, "units")
    work_orders = _dict_records(payload, "work_orders")
    if not leases and not units and not work_orders:
        return False

    open_work_orders = sum(
        1
        for work_order in work_orders.values()
        if isinstance(work_order, dict)
        and normalize_token(work_order.get("status"), default="").lower() not in _RESOLVED_STATUSES
    )
    summary["surface_item_count__property_leases"] = float(len(leases))
    summary["surface_item_count__property_units"] = float(len(units))
    summary["surface_item_count__property_work_orders"] = float(len(work_orders))
    summary["surface_signal__property_open_work_orders"] = float(open_work_orders)

    item_count = len(leases) + len(units) + len(work_orders)
    status = "warning" if open_work_orders > 0 else "ok"
    _record_panel(
        summary,
        panels,
        panel_name="vertical_property",
        status=status,
        item_count=item_count,
    )
    return True


def _summarize_campaign_panel(
    payload: object,
    summary: dict[str, float],
    panels: list[tuple[str, str, int]],
) -> bool:
    if not isinstance(payload, dict):
        return False
    campaigns = _dict_records(payload, "campaigns")
    creatives = _dict_records(payload, "creatives")
    approvals = _dict_records(payload, "approvals")
    reports = _dict_records(payload, "reports")
    if not campaigns and not creatives and not approvals and not reports:
        return False

    pending_approvals = sum(
        1
        for approval in approvals.values()
        if isinstance(approval, dict)
        and normalize_token(approval.get("status"), default="").lower()
        in {"pending", "pending_approval", "review"}
    )
    stale_reports = sum(
        1 for report in reports.values() if isinstance(report, dict) and bool(report.get("stale"))
    )
    summary["surface_item_count__campaigns"] = float(len(campaigns))
    summary["surface_item_count__campaign_creatives"] = float(len(creatives))
    summary["surface_item_count__campaign_approvals"] = float(len(approvals))
    summary["surface_item_count__campaign_reports"] = float(len(reports))
    summary["surface_signal__campaign_pending_approvals"] = float(pending_approvals)
    summary["surface_signal__campaign_stale_reports"] = float(stale_reports)

    item_count = len(campaigns) + len(creatives) + len(approvals) + len(reports)
    if stale_reports > 0:
        status = "critical"
    elif pending_approvals > 0:
        status = "warning"
    else:
        status = "ok"
    _record_panel(
        summary,
        panels,
        panel_name="vertical_campaign",
        status=status,
        item_count=item_count,
    )
    return True


def _summarize_inventory_panel(
    payload: object,
    summary: dict[str, float],
    panels: list[tuple[str, str, int]],
) -> bool:
    if not isinstance(payload, dict):
        return False
    quotes = _dict_records(payload, "quotes")
    pools = _dict_records(payload, "capacity_pools")
    orders = _dict_records(payload, "orders")
    allocations = _dict_records(payload, "allocations")
    if not quotes and not pools and not orders and not allocations:
        return False

    low_headroom_critical = 0
    low_headroom_warning = 0
    for pool in pools.values():
        if not isinstance(pool, dict):
            continue
        total_units = _int_value(pool.get("total_units", 0))
        reserved_units = _int_value(pool.get("reserved_units", 0))
        headroom = max(total_units - reserved_units, 0)
        if headroom <= 10:
            low_headroom_critical += 1
        elif headroom <= 30:
            low_headroom_warning += 1

    summary["surface_item_count__inventory_quotes"] = float(len(quotes))
    summary["surface_item_count__inventory_pools"] = float(len(pools))
    summary["surface_item_count__inventory_orders"] = float(len(orders))
    summary["surface_item_count__inventory_allocations"] = float(len(allocations))
    summary["surface_signal__inventory_low_headroom_critical"] = float(low_headroom_critical)
    summary["surface_signal__inventory_low_headroom_warning"] = float(low_headroom_warning)

    item_count = len(quotes) + len(pools) + len(orders) + len(allocations)
    if low_headroom_critical > 0:
        status = "critical"
    elif low_headroom_warning > 0:
        status = "warning"
    else:
        status = "ok"
    _record_panel(
        summary,
        panels,
        panel_name="vertical_inventory",
        status=status,
        item_count=item_count,
    )
    return True


def _record_panel(
    summary: dict[str, float],
    panels: list[tuple[str, str, int]],
    *,
    panel_name: str,
    status: str,
    item_count: int,
) -> None:
    if item_count <= 0:
        return

    normalized_status = normalize_token(status, default="ok").lower()
    _increment(summary, "surface_panel_count__all", 1.0)
    _increment(summary, f"surface_panel_count__{panel_name}", 1.0)
    _increment(summary, "surface_item_count__all", float(item_count))
    _increment(summary, f"surface_item_count__{panel_name}", float(item_count))
    _increment(summary, f"surface_status__{normalized_status}", 1.0)
    panels.append((panel_name, normalized_status, item_count))


def _panel_rank(panel: tuple[str, str, int]) -> tuple[int, int]:
    _, status, item_count = panel
    return (_STATUS_RANK.get(status, 0), item_count)


def _increment(summary: dict[str, float], key: str, value: float) -> None:
    summary[key] = summary.get(key, 0.0) + value


def _status_counts(records: Iterable[object]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for record in records:
        if not isinstance(record, dict):
            continue
        status = normalize_token(record.get("status"), default="unknown").lower()
        counts[status] = counts.get(status, 0) + 1
    return counts


def _dict_records(payload: object, key: str) -> dict[str, Any]:
    if not isinstance(payload, dict):
        return {}
    value = payload.get(key, {})
    return value if isinstance(value, dict) else {}


def _int_value(value: object) -> int:
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    if isinstance(value, str):
        try:
            return int(value)
        except ValueError:
            return 0
    return 0


_RESOLVED_STATUSES = {"closed", "resolved", "done", "complete", "completed", "approved"}
_STATUS_RANK = {
    "critical": 4,
    "warning": 3,
    "attention": 2,
    "ok": 1,
    "__none__": 0,
}
