from __future__ import annotations

from pathlib import Path
from typing import List, Optional

from vei.router.core import Router
from vei.scenario_engine.compiler import CompiledWorkflow
from vei.world.session import WorldSession

from .models import (
    ScenarioRunResult,
    StepExecution,
    ValidationIssue,
    ValidationReport,
    WorkflowOutcomeValidation,
)
from .validator import (
    evaluate_assertion_specs,
    evaluate_assertions,
    static_validate_workflow,
)


def run_compiled_workflow(
    workflow: CompiledWorkflow,
    *,
    seed: int = 42042,
    artifacts_dir: Optional[str] = None,
    connector_mode: str = "sim",
) -> ScenarioRunResult:
    artifacts = Path(artifacts_dir) if artifacts_dir else None
    if artifacts:
        artifacts.mkdir(parents=True, exist_ok=True)

    router = Router(
        seed=seed,
        artifacts_dir=str(artifacts) if artifacts else None,
        scenario=workflow.scenario,
        connector_mode=connector_mode,
    )
    world = WorldSession.attach_router(router)
    initial_snapshot = world.snapshot("workflow.start")
    available_tools = [spec.name for spec in router.registry.list()]
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
            result = router.call_and_step(step.tool, dict(step.args))
            observation = router.snapshot_observation(
                _focus_for_tool(step.tool)
            ).model_dump()
            pending = router.pending()
            current_state = world.current_state().model_dump(mode="json")
            assertion_failures = evaluate_assertions(
                step=step,
                result=result,
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
                    ok=ok,
                    result=result,
                    observation=observation,
                    assertion_failures=assertion_failures,
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
                    ok=False,
                    result={"error": str(exc)},
                    observation={},
                    assertion_failures=[str(exc)],
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
    if workflow.spec.success_assertions:
        last_result = step_results[-1].result if step_results else {}
        for failure in evaluate_assertion_specs(
            assertions=workflow.spec.success_assertions,
            result=last_result,
            observation=final_observation,
            pending=final_pending,
            state=final_state.model_dump(mode="json"),
            time_ms=router.bus.clock_ms,
        ):
            dynamic_issues.append(
                ValidationIssue(
                    code="success_assertion.failed",
                    message=failure,
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
        metadata={
            "connector_mode": connector_mode,
            "state_head": router.state_store.head,
            "time_ms": router.bus.clock_ms,
            "connector_last_receipt": router.connector_runtime.last_receipt(),
            "initial_snapshot_id": initial_snapshot.snapshot_id,
            "final_snapshot_id": final_snapshot.snapshot_id,
        },
    )


def validate_compiled_workflow_outcome(
    workflow: CompiledWorkflow,
    *,
    state: dict,
    time_ms: int = 0,
    available_tools: List[str] | None = None,
    result: object | None = None,
    observation: dict | None = None,
    pending: dict[str, int] | None = None,
) -> WorkflowOutcomeValidation:
    static_report = static_validate_workflow(workflow, available_tools=available_tools)
    if not static_report.ok:
        return WorkflowOutcomeValidation(
            ok=False,
            workflow_name=workflow.spec.name,
            static_validation=static_report,
            dynamic_validation=ValidationReport(ok=False, issues=[]),
            step_count=len(workflow.steps),
            success_assertion_count=len(workflow.spec.success_assertions),
            success_assertions_passed=0,
            success_assertions_failed=len(workflow.spec.success_assertions),
            metadata={
                "validation_mode": "state",
                "time_ms": time_ms,
            },
        )

    failures = evaluate_assertion_specs(
        assertions=workflow.spec.success_assertions,
        result=result or {},
        observation=observation or {},
        pending=pending or _pending_summary_from_state(state),
        state=state,
        time_ms=time_ms,
    )
    dynamic_issues = [
        ValidationIssue(code="success_assertion.failed", message=failure)
        for failure in failures
    ]
    dynamic_report = ValidationReport(
        ok=not any(issue.severity == "error" for issue in dynamic_issues),
        issues=dynamic_issues,
    )
    assertion_count = len(workflow.spec.success_assertions)
    failed_count = len(failures)
    return WorkflowOutcomeValidation(
        ok=static_report.ok and dynamic_report.ok,
        workflow_name=workflow.spec.name,
        static_validation=static_report,
        dynamic_validation=dynamic_report,
        step_count=len(workflow.steps),
        success_assertion_count=assertion_count,
        success_assertions_passed=max(0, assertion_count - failed_count),
        success_assertions_failed=failed_count,
        metadata={
            "validation_mode": "state",
            "time_ms": time_ms,
        },
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
