from __future__ import annotations

from pathlib import Path

from vei.run.api import (
    get_workspace_run_dir,
    load_run_contract_evaluation,
    load_run_manifest,
    get_workspace_run_manifest_path,
)
from vei.run import load_run_events
from vei.run.models import RunTimelineEvent

from .models import Runbook, RunbookStep


def build_runbook(workspace_root: Path, run_id: str) -> Runbook:
    manifest = load_run_manifest(
        get_workspace_run_manifest_path(workspace_root, run_id)
    )
    contract_eval = load_run_contract_evaluation(workspace_root, run_id)

    run_dir = get_workspace_run_dir(workspace_root, run_id)
    events_path = run_dir / "events.jsonl"
    events: list[RunTimelineEvent] = []
    if events_path.exists():
        events = load_run_events(events_path)

    tool_events = [e for e in events if e.kind in ("trace_call", "workflow_step")]

    contract_name = ""
    success_predicates: list[str] = []
    forbidden_predicates: list[str] = []
    if contract_eval:
        contract_name = contract_eval.get("contract_name", "")
        for pred in contract_eval.get("metadata", {}).get("success_predicates", []):
            if isinstance(pred, dict):
                success_predicates.append(str(pred.get("name", "")))
            elif isinstance(pred, str):
                success_predicates.append(pred)
        for pred in contract_eval.get("metadata", {}).get("forbidden_predicates", []):
            if isinstance(pred, dict):
                forbidden_predicates.append(str(pred.get("name", "")))
            elif isinstance(pred, str):
                forbidden_predicates.append(pred)

    steps: list[RunbookStep] = []
    seen_tools: set[str] = set()
    for i, event in enumerate(tool_events):
        tool = event.tool or event.resolved_tool or ""
        domain = event.graph_domain or _infer_domain(tool)
        action = event.graph_action or event.label or tool

        is_decision_point = tool in seen_tools
        seen_tools.add(tool)

        payload = dict(event.payload) if event.payload else {}
        args = {
            k: v
            for k, v in payload.items()
            if k not in ("result", "observation", "emitted")
        }

        postcondition = ""
        if event.status:
            postcondition = f"status={event.status}"

        related_predicates = [
            p for p in success_predicates if _predicate_relates(p, tool, domain)
        ]

        steps.append(
            RunbookStep(
                index=i + 1,
                domain=domain,
                action=action,
                tool=tool,
                args_template=args,
                precondition=f"step {i}" if i > 0 else "initial state",
                postcondition=postcondition,
                contract_predicates=related_predicates,
                decision_point=is_decision_point,
            )
        )

    scenario_name = manifest.scenario_name or ""
    decision_count = sum(1 for s in steps if s.decision_point)
    success_rate = None
    if contract_eval and contract_eval.get("ok") is not None:
        success_rate = 1.0 if contract_eval["ok"] else 0.0

    return Runbook(
        title=f"Runbook: {scenario_name}",
        scenario_name=scenario_name,
        contract_name=contract_name,
        steps=steps,
        decision_points=decision_count,
        total_steps=len(steps),
        success_rate=success_rate,
    )


def _infer_domain(tool: str) -> str:
    prefix = tool.split(".")[0].lower() if "." in tool else tool.lower()
    domain_map = {
        "slack": "comm_graph",
        "mail": "comm_graph",
        "jira": "work_graph",
        "tickets": "work_graph",
        "servicedesk": "work_graph",
        "docs": "doc_graph",
        "browser": "doc_graph",
        "okta": "identity_graph",
        "google_admin": "identity_graph",
        "crm": "revenue_graph",
        "erp": "revenue_graph",
    }
    return domain_map.get(prefix, "ops_graph")


def _predicate_relates(predicate: str, tool: str, domain: str) -> bool:
    pred_lower = predicate.lower()
    return tool.split(".")[0].lower() in pred_lower or domain in pred_lower
