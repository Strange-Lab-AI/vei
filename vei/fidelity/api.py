from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Iterable

from vei.blueprint.api import create_world_session_from_blueprint
from vei.capability_graph.models import CapabilityGraphActionInput
from vei.workspace.api import (
    build_workspace_scenario_asset,
    load_workspace,
    load_workspace_blueprint_asset,
    resolve_workspace_scenario,
    temporary_env,
)

from .models import (
    FidelityStatus,
    TwinFidelityCase,
    TwinFidelityCheck,
    TwinFidelityReport,
)

if TYPE_CHECKING:
    from vei.world.api import WorldSessionAPI


REPORT_PATH = Path("fidelity_report.json")


def build_workspace_fidelity_report(root: str | Path) -> TwinFidelityReport:
    workspace_root = Path(root).expanduser().resolve()
    manifest = load_workspace(workspace_root)
    scenario = resolve_workspace_scenario(workspace_root, manifest)
    asset = build_workspace_scenario_asset(
        load_workspace_blueprint_asset(workspace_root),
        scenario,
    )
    artifacts_dir = workspace_root / ".artifacts" / "fidelity"
    state_dir = workspace_root / ".artifacts" / "fidelity_state"
    artifacts_dir.mkdir(parents=True, exist_ok=True)
    state_dir.mkdir(parents=True, exist_ok=True)
    with temporary_env("VEI_STATE_DIR", str(state_dir)):
        session = create_world_session_from_blueprint(
            asset,
            seed=42042,
            artifacts_dir=str(artifacts_dir),
            branch=f"{manifest.name}.fidelity",
        )
    session.snapshot("fidelity.start")
    cases = [
        _check_slack_surface(session),
        _check_docs_surface(session),
        _check_tickets_surface(session),
        _check_identity_surface(session),
        _check_vertical_surface(session, str(manifest.source_ref or "")),
    ]
    status = _combine_status(item.status for item in cases)
    report = TwinFidelityReport(
        generated_at=_iso_now(),
        workspace_root=str(workspace_root),
        company_name=manifest.title,
        status=status,
        summary=(
            "The playable worlds use boundary-faithful twins for the surfaces that make "
            "the missions believable. These checks verify that the twin responds like "
            "a real work surface, changes visible state, and fails cleanly when asked "
            "to do something invalid."
        ),
        cases=cases,
        metadata={
            "scenario_name": scenario.name,
            "vertical": manifest.source_ref,
            "active_scenario_variant": scenario.metadata.get(
                "vertical_scenario_variant"
            ),
        },
    )
    write_workspace_fidelity_report(workspace_root, report)
    return report


def load_workspace_fidelity_report(root: str | Path) -> TwinFidelityReport | None:
    workspace_root = Path(root).expanduser().resolve()
    path = workspace_root / REPORT_PATH
    if not path.exists():
        return None
    return TwinFidelityReport.model_validate_json(path.read_text(encoding="utf-8"))


def get_or_build_workspace_fidelity_report(root: str | Path) -> TwinFidelityReport:
    cached = load_workspace_fidelity_report(root)
    if cached is not None:
        return cached
    return build_workspace_fidelity_report(root)


def write_workspace_fidelity_report(
    root: str | Path,
    report: TwinFidelityReport,
) -> Path:
    workspace_root = Path(root).expanduser().resolve()
    path = workspace_root / REPORT_PATH
    path.write_text(report.model_dump_json(indent=2), encoding="utf-8")
    return path


def _check_slack_surface(session: WorldSessionAPI) -> TwinFidelityCase:
    channels_result = session.call_tool("slack.list_channels", {})
    if isinstance(channels_result, list):
        channels_list = channels_result
    else:
        channels_list = channels_result.get("channels", [])
    first = channels_list[0] if channels_list else "#general"
    channel = first if isinstance(first, str) else first.get("name", "#general")
    message = "Fidelity probe: update recorded."
    result = session.call_tool(
        "slack.send_message",
        {"channel": channel, "text": message},
    )
    channel_view = session.call_tool("slack.open_channel", {"channel": channel})
    thread_view = session.call_tool(
        "slack.fetch_thread",
        {"channel": channel, "thread_ts": str(result.get("ts", ""))},
    )
    messages = list(channel_view.get("messages", []))
    thread_messages = list(thread_view.get("messages", []))
    message_visible = any(item.get("text") == message for item in messages)
    thread_visible = any(item.get("ts") == result.get("ts") for item in thread_messages)
    checks = [
        TwinFidelityCheck(
            name="request_shape",
            status="ok" if str(result.get("ts", "")).strip() else "error",
            summary="Slack twin accepts a normal post-message request shape.",
            payload={"response_keys": sorted(result.keys())},
        ),
        TwinFidelityCheck(
            name="observable_side_effect",
            status="ok" if message_visible else "error",
            summary="Posted message becomes visible in the live channel state.",
            payload={"message_count": len(messages)},
        ),
        TwinFidelityCheck(
            name="status_semantics",
            status="ok" if thread_visible else "warning",
            summary="Slack write returns a thread/message reference that can be resolved immediately.",
            payload={"ts": result.get("ts")},
        ),
    ]
    return TwinFidelityCase(
        surface="slack",
        title="Slack-like comms boundary",
        boundary_contract="Message write requests return explicit success and appear in observable channel history.",
        why_it_matters="Players need comms surfaces that feel trustworthy because many missions end with visible coordination artifacts.",
        resolved_tool="slack.send_message",
        status=_combine_status(item.status for item in checks),
        checks=checks,
    )


def _check_docs_surface(session: WorldSessionAPI) -> TwinFidelityCase:
    docs_list = session.call_tool("docs.list", {})
    all_docs = (
        docs_list if isinstance(docs_list, list) else docs_list.get("documents", [])
    )
    if all_docs and isinstance(all_docs[0], dict):
        doc_id = all_docs[0].get("doc_id", "DOC-FIDELITY-1")
    elif all_docs and isinstance(all_docs[0], str):
        doc_id = all_docs[0]
    else:
        doc_id = "DOC-FIDELITY-1"
    body = "Boundary fidelity note recorded."
    result = session.call_tool(
        "docs.update",
        {"doc_id": doc_id, "body": body},
    )
    document = session.call_tool("docs.read", {"doc_id": doc_id})
    checks = [
        TwinFidelityCheck(
            name="request_shape",
            status="ok" if result.get("doc_id") == doc_id else "error",
            summary="Docs twin accepts a body update using a normal document identifier.",
            payload={"doc_id": doc_id},
        ),
        TwinFidelityCheck(
            name="observable_side_effect",
            status="ok" if document.get("body") == body else "error",
            summary="Document body changes are visible in the observable document surface.",
            payload={"title": document.get("title")},
        ),
        TwinFidelityCheck(
            name="error_semantics",
            status=(
                "ok"
                if "unknown document"
                in _safe_tool_error(
                    session,
                    "docs.update",
                    {"doc_id": "DOC-DOES-NOT-EXIST", "body": body},
                )
                else "warning"
            ),
            summary="Invalid document writes fail explicitly.",
        ),
    ]
    return TwinFidelityCase(
        surface="docs",
        title="Docs boundary",
        boundary_contract="Document updates should preserve normal write semantics and surface the new body immediately.",
        why_it_matters="Mission outcomes are often proven by updated artifacts, so docs cannot feel like fake placeholders.",
        resolved_tool="docs.update",
        status=_combine_status(item.status for item in checks),
        checks=checks,
    )


def _check_tickets_surface(session: WorldSessionAPI) -> TwinFidelityCase:
    tickets_list = session.call_tool("tickets.list", {})
    all_tickets = (
        tickets_list
        if isinstance(tickets_list, list)
        else tickets_list.get("issues", [])
    )
    if all_tickets and isinstance(all_tickets[0], dict):
        ticket_id = all_tickets[0].get("key") or all_tickets[0].get(
            "ticket_id", "JRA-FIDELITY-1"
        )
    elif all_tickets and isinstance(all_tickets[0], str):
        ticket_id = all_tickets[0]
    else:
        ticket_id = "JRA-FIDELITY-1"
    comment = "Fidelity probe: blocker reviewed."
    result = session.call_tool(
        "tickets.add_comment",
        {"ticket_id": ticket_id, "body": comment},
    )
    ticket = session.call_tool("tickets.get", {"ticket_id": ticket_id})
    history = list(ticket.get("history", []))
    cid = result.get("comment_id")
    comment_visible = any(
        item.get("comment") == cid or item.get("comment_id") == cid for item in history
    )
    checks = [
        TwinFidelityCheck(
            name="request_shape",
            status="ok" if result.get("comment_id") else "error",
            summary="Ticket twin accepts a comment write on an open issue.",
            payload={"comment_id": result.get("comment_id")},
        ),
        TwinFidelityCheck(
            name="side_effect",
            status="ok" if comment_visible else "error",
            summary="The new comment leaves observable ticket history behind.",
            payload={"history_count": len(history)},
        ),
        TwinFidelityCheck(
            name="history_semantics",
            status="ok" if ticket.get("status") else "warning",
            summary="Ticket state remains readable after the write and keeps explicit status semantics.",
            payload={"status": ticket.get("status")},
        ),
    ]
    return TwinFidelityCase(
        surface="tickets",
        title="Ticket / service-desk boundary",
        boundary_contract="Work-item updates should change the shared tracker state with explicit, observable history.",
        why_it_matters="Mission play needs a believable shared work tracker because many moves are coordination moves, not raw mutations.",
        resolved_tool="tickets.add_comment",
        status=_combine_status(item.status for item in checks),
        checks=checks,
    )


def _check_identity_surface(session: WorldSessionAPI) -> TwinFidelityCase:
    requests_result = session.call_tool("servicedesk.list_requests", {})
    rows = (
        requests_result.get("requests", [])
        if isinstance(requests_result, dict)
        else requests_result if isinstance(requests_result, list) else []
    )
    request_id = "REQ-FIDELITY-1"
    approval_stage = "vendor"
    for row in rows:
        if not isinstance(row, dict):
            continue
        rid = row.get("id") or row.get("request_id")
        if not rid:
            continue
        detail = session.call_tool("servicedesk.get_request", {"request_id": rid})
        if not isinstance(detail, dict):
            continue
        request_id = rid
        for appr in detail.get("approvals", []):
            if isinstance(appr, dict) and appr.get("stage"):
                approval_stage = appr["stage"]
                break
        break
    action = CapabilityGraphActionInput(
        domain="work_graph",
        action="update_request_approval",
        args={
            "request_id": request_id,
            "approval_stage": approval_stage,
            "approval_status": "APPROVED",
            "comment": "Boundary probe approval.",
        },
    )
    result = session.graph_action(action)
    request_state = session.call_tool(
        "servicedesk.get_request",
        {"request_id": request_id},
    )
    request_view = next(
        (
            item
            for item in result.graph.get("service_requests", [])
            if item.get("request_id") == request_id
        ),
        {},
    )
    checks = [
        TwinFidelityCheck(
            name="approval_write",
            status="ok" if result.ok else "error",
            summary="Approval writes succeed through the shared work/control-plane surface.",
            payload={"resolved_tool": result.tool},
        ),
        TwinFidelityCheck(
            name="approval_status",
            status=(
                "ok"
                if request_view.get("status", "").lower()
                in {"approved", "pending_approval"}
                else "warning"
            ),
            summary="Approval state remains explicit and readable after the update.",
            payload={
                "status": request_view.get("status"),
                "approval_count": len(request_state.get("approvals", [])),
            },
        ),
        TwinFidelityCheck(
            name="error_semantics",
            status=(
                "ok"
                if "unknown request"
                in _safe_tool_error(
                    session,
                    "servicedesk.get_request",
                    {"request_id": "REQ-DOES-NOT-EXIST"},
                ).lower()
                else "warning"
            ),
            summary="Invalid approval requests fail with a concrete validation message.",
        ),
    ]
    return TwinFidelityCase(
        surface="identity",
        title="Approval / control-plane boundary",
        boundary_contract="Approval-like surfaces should enforce required fields and leave readable state transitions behind.",
        why_it_matters="The missions depend on policy and approval gates feeling strict rather than theatrical.",
        resolved_tool=result.tool,
        status=_combine_status(item.status for item in checks),
        checks=checks,
    )


def _check_property_surface(session: WorldSessionAPI) -> TwinFidelityCase:
    action = CapabilityGraphActionInput(
        domain="property_graph",
        action="assign_vendor",
        args={
            "work_order_id": "WO-HPM-88",
            "vendor_id": "VEND-HPM-HVAC",
            "note": "Boundary fidelity assignment.",
        },
    )
    result = session.graph_action(action)
    work_order = next(
        (
            item
            for item in result.graph.get("work_orders", [])
            if item.get("work_order_id") == "WO-HPM-88"
        ),
        {},
    )
    checks = [
        TwinFidelityCheck(
            name="resolved_tool",
            status="ok" if result.tool.endswith("assign_vendor") else "warning",
            summary="Property graph action resolves to a concrete adapter tool, not a fake no-op.",
            payload={"tool": result.tool},
        ),
        TwinFidelityCheck(
            name="side_effect",
            status="ok" if work_order.get("vendor_id") == "VEND-HPM-HVAC" else "error",
            summary="Vendor assignment mutates the property work-order state visibly.",
            payload={"status": work_order.get("status")},
        ),
        TwinFidelityCheck(
            name="object_refs",
            status=(
                "ok"
                if "work_order:WO-HPM-88"
                in result.metadata.get("affected_object_refs", [])
                else "warning"
            ),
            summary="Vertical adapter actions record affected business objects for replay and exports.",
            payload={"refs": result.metadata.get("affected_object_refs", [])},
        ),
    ]
    return TwinFidelityCase(
        surface="property",
        title="Hero vertical boundary",
        boundary_contract="The hero-world property adapter must mutate real business state and record the affected objects cleanly.",
        why_it_matters="This is the vertical surface that makes Harbor Point feel like a company, not a generic task board.",
        resolved_tool=result.tool,
        status=_combine_status(item.status for item in checks),
        checks=checks,
    )


def _check_campaign_surface(session: WorldSessionAPI) -> TwinFidelityCase:
    action = CapabilityGraphActionInput(
        domain="campaign_graph",
        action="approve_creative",
        args={
            "creative_id": "CRT-APEX-01",
            "approval_id": "APR-APEX-01",
        },
    )
    result = session.graph_action(action)
    creative = next(
        (
            item
            for item in result.graph.get("creatives", [])
            if item.get("creative_id") == "CRT-APEX-01"
        ),
        {},
    )
    approval = next(
        (
            item
            for item in result.graph.get("approvals", [])
            if item.get("approval_id") == "APR-APEX-01"
        ),
        {},
    )
    checks = [
        TwinFidelityCheck(
            name="resolved_tool",
            status="ok" if result.tool.endswith("approve_creative") else "warning",
            summary="Campaign graph actions resolve to the concrete creative-approval twin.",
            payload={"tool": result.tool},
        ),
        TwinFidelityCheck(
            name="creative_state",
            status="ok" if creative.get("status") == "approved" else "error",
            summary="Creative status changes are visible in the campaign graph.",
            payload={"status": creative.get("status")},
        ),
        TwinFidelityCheck(
            name="approval_state",
            status="ok" if approval.get("status") == "approved" else "error",
            summary="The attached approval record also moves to an explicit approved state.",
            payload={"status": approval.get("status")},
        ),
    ]
    return TwinFidelityCase(
        surface="campaign",
        title="Campaign launch boundary",
        boundary_contract="Campaign approval actions should change both creative state and the approval record with explicit, observable effects.",
        why_it_matters="The marketing missions only feel credible if approval and launch twins behave like real control points.",
        resolved_tool=result.tool,
        status=_combine_status(item.status for item in checks),
        checks=checks,
    )


def _check_inventory_surface(session: WorldSessionAPI) -> TwinFidelityCase:
    action = CapabilityGraphActionInput(
        domain="inventory_graph",
        action="allocate_capacity",
        args={
            "quote_id": "Q-ATS-900",
            "pool_id": "POOL-MKE-B",
            "units": 70,
        },
    )
    result = session.graph_action(action)
    allocation = next(
        (
            item
            for item in result.graph.get("allocations", [])
            if item.get("quote_id") == "Q-ATS-900"
        ),
        {},
    )
    pool = next(
        (
            item
            for item in result.graph.get("capacity_pools", [])
            if item.get("pool_id") == "POOL-MKE-B"
        ),
        {},
    )
    checks = [
        TwinFidelityCheck(
            name="resolved_tool",
            status="ok" if result.tool.endswith("allocate_capacity") else "warning",
            summary="Inventory actions resolve to the real capacity-allocation twin.",
            payload={"tool": result.tool},
        ),
        TwinFidelityCheck(
            name="allocation_written",
            status="ok" if allocation else "error",
            summary="Capacity allocation becomes a visible business object in the world state.",
            payload={"allocation": allocation},
        ),
        TwinFidelityCheck(
            name="pool_balance",
            status="ok" if int(pool.get("reserved_units", 0)) >= 70 else "error",
            summary="The selected pool records the reservation pressure created by the action.",
            payload={"reserved_units": pool.get("reserved_units")},
        ),
    ]
    return TwinFidelityCase(
        surface="inventory",
        title="Inventory / fulfillment boundary",
        boundary_contract="Capacity actions should reserve real pool state and leave a concrete allocation object behind.",
        why_it_matters="The storage missions depend on capacity promises having real operational consequences, not just optimistic labels.",
        resolved_tool=result.tool,
        status=_combine_status(item.status for item in checks),
        checks=checks,
    )


def _check_service_ops_surface(session: WorldSessionAPI) -> TwinFidelityCase:
    action = CapabilityGraphActionInput(
        domain="ops_graph",
        action="hold_billing",
        args={
            "billing_case_id": "BILL-CFS-100",
            "reason": "Boundary fidelity hold.",
        },
    )
    result = session.graph_action(action)
    billing_case = next(
        (
            item
            for item in result.graph.get("billing_cases", [])
            if item.get("billing_case_id") == "BILL-CFS-100"
        ),
        {},
    )
    checks = [
        TwinFidelityCheck(
            name="resolved_tool",
            status="ok" if result.tool.endswith("hold_billing") else "warning",
            summary="Service-ops graph actions resolve to the concrete dispatch-and-billing twin.",
            payload={"tool": result.tool},
        ),
        TwinFidelityCheck(
            name="billing_hold_written",
            status="ok" if billing_case.get("hold") is True else "error",
            summary="Billing hold decisions become visible state in the shared service loop.",
            payload={"billing_case": billing_case},
        ),
        TwinFidelityCheck(
            name="object_refs",
            status=(
                "ok"
                if "billing_case:BILL-CFS-100"
                in result.metadata.get("affected_object_refs", [])
                else "warning"
            ),
            summary="Service-ops actions record the affected customer/billing objects for replay and comparison.",
            payload={"refs": result.metadata.get("affected_object_refs", [])},
        ),
    ]
    return TwinFidelityCase(
        surface="service_ops",
        title="Service operations boundary",
        boundary_contract="Service-ops actions should mutate shared dispatch or billing state in a way that is observable and replayable.",
        why_it_matters="The Clearwater demo only feels real if dispatch and billing decisions leave a tangible business trail.",
        resolved_tool=result.tool,
        status=_combine_status(item.status for item in checks),
        checks=checks,
    )


def _safe_tool_error(
    session: WorldSessionAPI, tool: str, args: dict[str, object]
) -> str:
    try:
        session.call_tool(tool, args)
    except Exception as exc:  # pragma: no cover - defensive
        return str(exc)
    return ""


def _combine_status(statuses: Iterable[FidelityStatus]) -> FidelityStatus:
    items = list(statuses)
    if any(item == "error" for item in items):
        return "error"
    if any(item == "warning" for item in items):
        return "warning"
    return "ok"


def _iso_now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat()


def _check_vertical_surface(
    session: WorldSessionAPI, vertical_name: str
) -> TwinFidelityCase:
    normalized = vertical_name.strip().lower()
    if normalized == "service_ops":
        return _check_service_ops_surface(session)
    if normalized == "digital_marketing_agency":
        return _check_campaign_surface(session)
    if normalized == "storage_solutions":
        return _check_inventory_surface(session)
    if normalized == "b2b_saas":
        return _check_revenue_surface(session)
    if normalized == "real_estate_management":
        return _check_property_surface(session)
    return TwinFidelityCase(
        surface="property",
        title="Vertical surface (skipped)",
        boundary_contract="No vertical-specific fidelity check available for this workspace type.",
        why_it_matters="Vertical fidelity checks are only defined for known vertical types.",
        status="ok",
        checks=[],
    )


def _check_revenue_surface(session: WorldSessionAPI) -> TwinFidelityCase:
    action = CapabilityGraphActionInput(
        domain="revenue_graph",
        action="log_activity",
        args={
            "kind": "note",
            "deal_id": "DEAL-APEX-RENEWAL",
            "note": "Fidelity probe: renewal check recorded.",
        },
    )
    result = session.graph_action(action)
    checks = [
        TwinFidelityCheck(
            name="revenue_graph_action",
            status="ok" if result.ok else "warning",
            summary="Revenue graph accepts a deal activity note.",
            payload={"resolved_tool": result.tool},
        ),
    ]
    return TwinFidelityCase(
        surface="revenue_graph",
        title="Revenue / CRM boundary",
        boundary_contract="CRM actions should record deal activity with explicit, observable state.",
        why_it_matters="Renewal missions depend on CRM state changes being visible and consistent.",
        resolved_tool=result.tool,
        status=_combine_status(item.status for item in checks),
        checks=checks,
    )
