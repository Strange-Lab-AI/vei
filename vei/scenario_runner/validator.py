from __future__ import annotations

from typing import Any, Iterable, List, Optional

from vei.capability_graph.models import CapabilityGraphActionInput
from vei.capability_graph.api import validate_graph_action_input
from vei.contract import evaluate_assertion_specs as evaluate_contract_assertions
from vei.scenario_engine.api import CompiledStep, CompiledWorkflow
from vei.scenario_engine.models import AssertionSpec

from .models import ValidationIssue, ValidationReport


def static_validate_workflow(
    workflow: CompiledWorkflow,
    *,
    available_tools: Optional[Iterable[str]] = None,
) -> ValidationReport:
    issues: List[ValidationIssue] = []
    tool_set = set(available_tools or [])

    for step in workflow.steps:
        if step.graph_domain and step.graph_action:
            try:
                validate_graph_action_input(
                    CapabilityGraphActionInput(
                        domain=step.graph_domain,
                        action=step.graph_action,
                        args=step.args.get("args", {}),
                    )
                )
            except ValueError as exc:
                issues.append(
                    ValidationIssue(
                        code="graph_action.invalid",
                        message=(
                            f"Step {step.step_id} uses invalid graph action: {exc}"
                        ),
                        step_id=step.step_id,
                    )
                )
            continue
        if tool_set and step.tool not in tool_set:
            issues.append(
                ValidationIssue(
                    code="tool.unavailable",
                    message=f"Step {step.step_id} uses unavailable tool: {step.tool}",
                    step_id=step.step_id,
                )
            )

    step_ids = {step.step_id for step in workflow.steps}
    for path in workflow.spec.failure_paths:
        if path.trigger_step not in step_ids:
            issues.append(
                ValidationIssue(
                    code="failure_path.trigger_missing",
                    message=(
                        f"Failure path '{path.name}' references unknown trigger step "
                        f"{path.trigger_step}"
                    ),
                    step_id=path.trigger_step,
                )
            )
        for recovery in path.recovery_steps:
            if recovery not in step_ids:
                issues.append(
                    ValidationIssue(
                        code="failure_path.recovery_missing",
                        message=(
                            f"Failure path '{path.name}' references unknown recovery step "
                            f"{recovery}"
                        ),
                        step_id=recovery,
                    )
                )

    if workflow.spec.approvals and not any(
        "approve" in step.description.lower()
        or "approve" in step.tool
        or "approve" in (step.graph_action or "")
        for step in workflow.steps
    ):
        issues.append(
            ValidationIssue(
                code="approval.unmapped",
                message="Workflow declares approvals but no approval-like step exists",
                severity="warning",
            )
        )

    return ValidationReport(
        ok=not any(i.severity == "error" for i in issues), issues=issues
    )


def evaluate_assertions(
    *,
    step: CompiledStep,
    result: Any,
    observation: dict[str, Any],
    pending: dict[str, int],
    state: dict[str, Any],
    time_ms: int,
) -> List[str]:
    return evaluate_assertion_specs(
        assertions=step.expect,
        result=result,
        observation=observation,
        pending=pending,
        state=state,
        time_ms=time_ms,
    )


def evaluate_assertion_specs(
    *,
    assertions: Iterable[AssertionSpec],
    result: Any,
    observation: dict[str, Any],
    pending: dict[str, int],
    state: dict[str, Any],
    time_ms: int,
) -> List[str]:
    return evaluate_contract_assertions(
        assertions=assertions,
        result=result,
        observation=observation,
        pending=pending,
        oracle_state=state,
        time_ms=time_ms,
    )
