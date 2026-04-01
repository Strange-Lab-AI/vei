from __future__ import annotations

from pathlib import Path
from typing import List, Optional

from vei.contract.api import build_contract_from_workflow, evaluate_contract
from vei.router import Router
from vei.scenario_engine.api import CompiledWorkflow
from vei.world import WorldSession

from .models import (
    ScenarioRunResult,
    StepExecution,
    ValidationIssue,
    ValidationReport,
    WorkflowOutcomeValidation,
)
from .validator import (
    evaluate_assertions,
    static_validate_workflow,
)


def run_compiled_workflow(
    workflow: CompiledWorkflow,
    *,
    seed: int = 42042,
    artifacts_dir: Optional[str] = None,
    connector_mode: str = "sim",
    branch: str = "main",
) -> ScenarioRunResult:
    artifacts = Path(artifacts_dir) if artifacts_dir else None
    if artifacts:
        artifacts.mkdir(parents=True, exist_ok=True)

    router = Router(
        seed=seed,
        artifacts_dir=str(artifacts) if artifacts else None,
        scenario=workflow.scenario,
        connector_mode=connector_mode,
        branch=branch,
    )
    world = WorldSession.attach_router(router)
    initial_snapshot = world.snapshot("workflow.start")
    available_tools = _workflow_available_tools(
        workflow, [spec.name for spec in router.registry.list()]
    )
    static_report = static_validate_workflow(workflow, available_tools=available_tools)

    if not static_report.ok:
        final_snapshot = world.snapshot("workflow.static_invalid")
        return ScenarioRunResult(
            ok=False,
            workflow_name=workflow.spec.name,
            static_validation=static_report,
            dynamic_validation=ValidationReport(ok=False, issues=[]),
            steps=[],
            artifacts_dir=str(artifacts) if artifacts else None,
            branch=final_snapshot.branch,
            initial_snapshot_id=initial_snapshot.snapshot_id,
            final_snapshot_id=final_snapshot.snapshot_id,
            initial_snapshot_label=initial_snapshot.label,
            final_snapshot_label=final_snapshot.label,
            final_state=final_snapshot.data.model_dump(mode="json"),
            metadata={
                "reason": "static validation failed",
                "initial_snapshot_id": initial_snapshot.snapshot_id,
                "final_snapshot_id": final_snapshot.snapshot_id,
            },
        )

    step_results: List[StepExecution] = []
    dynamic_issues: List[ValidationIssue] = []
    index = 0
    guard = 0
    max_guard = max(1, len(workflow.steps) * 3)

    while index < len(workflow.steps):
        guard += 1
        if guard > max_guard:
            dynamic_issues.append(
                ValidationIssue(
                    code="runner.loop_guard",
                    message="Workflow execution exceeded loop guard budget",
                )
            )
            break

        step = workflow.steps[index]
        try:
            resolved_tool = step.tool
            graph_action_ref = None
            graph_domain = step.graph_domain
            graph_action = step.graph_action
            graph_intent = None
            object_refs: list[str] = []
            result_payload: object
            if step.graph_domain and step.graph_action:
                graph_action_ref = f"{step.graph_domain}.{step.graph_action}"
                graph_result = world.graph_action(dict(step.args))
                result_payload = graph_result.result
                resolved_tool = graph_result.tool
                graph_intent = str(
                    graph_result.metadata.get("graph_intent") or graph_action_ref
                )
                object_refs = [
                    str(item)
                    for item in (
                        graph_result.metadata.get("affected_object_refs") or []
                    )
                ]
            else:
                result_payload = router.call_and_step(step.tool, dict(step.args))
            observation = router.snapshot_observation(
                _focus_for_step(step)
            ).model_dump()
            pending = router.pending()
            current_state = world.current_state().model_dump(mode="json")
            assertion_failures = evaluate_assertions(
                step=step,
                result=result_payload,
                observation=observation,
                pending=pending,
                state=current_state,
                time_ms=router.bus.clock_ms,
            )
            ok = not assertion_failures
            step_results.append(
                StepExecution(
                    step_id=step.step_id,
                    tool=step.tool,
                    resolved_tool=resolved_tool,
                    graph_action_ref=graph_action_ref,
                    graph_domain=graph_domain,
                    graph_action=graph_action,
                    graph_intent=graph_intent,
                    ok=ok,
                    result=result_payload,
                    observation=observation,
                    assertion_failures=assertion_failures,
                    object_refs=object_refs,
                    time_ms=router.bus.clock_ms,
                )
            )
            if assertion_failures:
                dynamic_issues.append(
                    ValidationIssue(
                        code="assertion.failed",
                        message="; ".join(assertion_failures),
                        step_id=step.step_id,
                    )
                )
                next_index = _resolve_failure_target(workflow, step.on_failure, index)
                if next_index is None:
                    break
                index = next_index
                continue
        except Exception as exc:  # noqa: BLE001
            step_results.append(
                StepExecution(
                    step_id=step.step_id,
                    tool=step.tool,
                    resolved_tool=resolved_tool,
                    graph_action_ref=graph_action_ref,
                    graph_domain=graph_domain,
                    graph_action=graph_action,
                    graph_intent=graph_intent,
                    ok=False,
                    result={"error": str(exc)},
                    observation={},
                    assertion_failures=[str(exc)],
                    object_refs=object_refs,
                    time_ms=router.bus.clock_ms,
                )
            )
            dynamic_issues.append(
                ValidationIssue(
                    code="step.exception",
                    message=str(exc),
                    step_id=step.step_id,
                )
            )
            next_index = _resolve_failure_target(workflow, step.on_failure, index)
            if next_index is None:
                break
            index = next_index
            continue

        index += 1

    # Evaluate top-level success assertions against final snapshot.
    final_observation = router.snapshot_observation("browser").model_dump()
    final_pending = router.pending()
    final_snapshot = world.snapshot("workflow.final")
    final_state = final_snapshot.data
    contract_validation = _evaluate_workflow_contract(
        workflow=workflow,
        oracle_state=final_state.model_dump(mode="json"),
        visible_observation=final_observation,
        result=step_results[-1].result if step_results else {},
        pending=final_pending,
        time_ms=router.bus.clock_ms,
        available_tools=available_tools,
        validation_mode="workflow",
    )
    for issue in contract_validation.dynamic_validation.issues:
        dynamic_issues.append(
            ValidationIssue(
                code=issue.code.replace("predicate", "assertion"),
                message=issue.message,
                metadata={
                    **issue.metadata,
                    "predicate_name": issue.predicate_name,
                    "source": issue.source,
                    "contract_name": contract_validation.contract_name,
                },
            )
        )

    dynamic_report = ValidationReport(
        ok=not any(issue.severity == "error" for issue in dynamic_issues),
        issues=dynamic_issues,
    )
    return ScenarioRunResult(
        ok=static_report.ok and dynamic_report.ok,
        workflow_name=workflow.spec.name,
        static_validation=static_report,
        dynamic_validation=dynamic_report,
        steps=step_results,
        artifacts_dir=str(artifacts) if artifacts else None,
        branch=final_snapshot.branch,
        initial_snapshot_id=initial_snapshot.snapshot_id,
        final_snapshot_id=final_snapshot.snapshot_id,
        initial_snapshot_label=initial_snapshot.label,
        final_snapshot_label=final_snapshot.label,
        final_state=final_state.model_dump(mode="json"),
        contract_validation=contract_validation,
        metadata={
            "connector_mode": connector_mode,
            "state_head": router.state_store.head,
            "time_ms": router.bus.clock_ms,
            "connector_last_receipt": router.connector_runtime.last_receipt(),
            "initial_snapshot_id": initial_snapshot.snapshot_id,
            "final_snapshot_id": final_snapshot.snapshot_id,
            "contract_name": contract_validation.contract_name,
        },
    )


def validate_compiled_workflow_outcome(
    workflow: CompiledWorkflow,
    *,
    oracle_state: dict,
    time_ms: int = 0,
    available_tools: List[str] | None = None,
    result: object | None = None,
    visible_observation: dict | None = None,
    pending: dict[str, int] | None = None,
) -> WorkflowOutcomeValidation:
    contract_validation = _evaluate_workflow_contract(
        workflow=workflow,
        oracle_state=oracle_state,
        visible_observation=visible_observation or {},
        result=result or {},
        pending=pending or _pending_summary_from_state(oracle_state),
        time_ms=time_ms,
        available_tools=_workflow_available_tools(workflow, available_tools),
        validation_mode="state",
    )
    return WorkflowOutcomeValidation(
        ok=contract_validation.ok,
        workflow_name=workflow.spec.name,
        contract_name=contract_validation.contract_name,
        static_validation=_to_validation_report(contract_validation.static_validation),
        dynamic_validation=_to_validation_report(
            contract_validation.dynamic_validation
        ),
        step_count=len(workflow.steps),
        success_assertion_count=(
            contract_validation.success_predicate_count
            + contract_validation.forbidden_predicate_count
        ),
        success_assertions_passed=(
            contract_validation.success_predicates_passed
            + max(
                0,
                contract_validation.forbidden_predicate_count
                - contract_validation.forbidden_predicates_failed,
            )
        ),
        success_assertions_failed=(
            contract_validation.success_predicates_failed
            + contract_validation.forbidden_predicates_failed
        ),
        forbidden_predicate_count=contract_validation.forbidden_predicate_count,
        forbidden_predicates_failed=contract_validation.forbidden_predicates_failed,
        policy_invariant_count=contract_validation.policy_invariant_count,
        policy_invariants_failed=contract_validation.policy_invariants_failed,
        metadata={
            **contract_validation.metadata,
            "time_ms": time_ms,
        },
    )


def _evaluate_workflow_contract(
    *,
    workflow: CompiledWorkflow,
    oracle_state: dict,
    visible_observation: dict[str, object],
    result: object,
    pending: dict[str, int],
    time_ms: int,
    available_tools: List[str] | None,
    validation_mode: str,
):
    contract = build_contract_from_workflow(workflow)
    return evaluate_contract(
        contract,
        oracle_state=oracle_state,
        visible_observation=visible_observation,
        result=result,
        pending=pending,
        time_ms=time_ms,
        available_tools=available_tools,
        validation_mode=validation_mode,
    )


def _to_validation_report(report) -> ValidationReport:
    return ValidationReport(
        ok=report.ok,
        issues=[
            ValidationIssue(
                code=issue.code.replace("predicate", "assertion"),
                message=issue.message,
                severity=issue.severity,
                metadata={
                    **issue.metadata,
                    "predicate_name": issue.predicate_name,
                    "source": issue.source,
                },
            )
            for issue in report.issues
        ],
    )


def _resolve_failure_target(
    workflow: CompiledWorkflow, on_failure: str, current_index: int
) -> Optional[int]:
    behavior = (on_failure or "fail").strip().lower()
    if behavior in {"continue", "skip"}:
        return current_index + 1
    if behavior.startswith("jump:"):
        step_id = behavior.split(":", 1)[1]
        target = workflow.step_lookup.get(step_id)
        if target:
            return max(0, target.index - 1)
    return None


def _workflow_available_tools(
    workflow: CompiledWorkflow, available_tools: List[str] | None
) -> List[str] | None:
    if available_tools is None:
        return None
    resolved = set(available_tools)
    if any(step.graph_domain and step.graph_action for step in workflow.steps):
        resolved.update({"vei.graph_action", "vei.graph_plan"})
    return sorted(resolved)


def _focus_for_step(step) -> str:
    if getattr(step, "graph_domain", None):
        return _focus_for_graph_domain(str(step.graph_domain))
    return _focus_for_tool(step.tool)


def _focus_for_graph_domain(domain: str) -> str:
    return {
        "comm_graph": "slack",
        "doc_graph": "docs",
        "work_graph": "tickets",
        "identity_graph": "identity",
        "revenue_graph": "crm",
        "data_graph": "spreadsheet",
        "obs_graph": "pagerduty",
        "ops_graph": "feature_flags",
    }.get(domain, "browser")


def _focus_for_tool(tool: str) -> str:
    for prefix in (
        "slack",
        "mail",
        "docs",
        "calendar",
        "tickets",
        "erp",
        "crm",
        "db",
        "browser",
        "okta",
        "servicedesk",
        "google_admin",
        "siem",
        "datadog",
        "pagerduty",
        "feature_flags",
        "hris",
        "jira",
    ):
        if tool.startswith(f"{prefix}."):
            return prefix
    if tool.startswith("salesforce.") or tool.startswith("hubspot."):
        return "crm"
    if (
        tool.startswith("xero.")
        or tool.startswith("netsuite.")
        or tool.startswith("dynamics.")
        or tool.startswith("quickbooks.")
    ):
        return "erp"
    return "browser"


def _pending_summary_from_state(state: dict) -> dict[str, int]:
    summary: dict[str, int] = {"total": 0}
    pending_events = state.get("pending_events", [])
    if not isinstance(pending_events, list):
        return summary
    for event in pending_events:
        if not isinstance(event, dict):
            continue
        target = str(event.get("target", "unknown"))
        summary[target] = summary.get(target, 0) + 1
        summary["total"] += 1
    return summary
