from __future__ import annotations

from typing import Any, Iterable, List, Optional

from vei.scenario_engine.compiler import CompiledStep, CompiledWorkflow
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
        "approve" in step.description.lower() or "approve" in step.tool
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
    failures: List[str] = []
    for assertion in assertions:
        msg = _assertion_failure(
            assertion=assertion,
            result=result,
            observation=observation,
            pending=pending,
            state=state,
            time_ms=time_ms,
        )
        if msg:
            failures.append(msg)
    return failures


def _assertion_failure(
    *,
    assertion: AssertionSpec,
    result: Any,
    observation: dict[str, Any],
    pending: dict[str, int],
    state: dict[str, Any],
    time_ms: int,
) -> Optional[str]:
    if assertion.kind == "result_contains":
        value = _resolve_field(result, assertion.field)
        needle = assertion.contains or ""
        if needle not in str(value):
            return f"expected result field '{assertion.field}' to contain '{needle}'"
        return None

    if assertion.kind == "result_not_contains":
        value = _resolve_field(result, assertion.field)
        needle = assertion.contains or ""
        if needle in str(value):
            return (
                f"expected result field '{assertion.field}' to not contain '{needle}'"
            )
        return None

    if assertion.kind == "result_equals":
        value = _resolve_field(result, assertion.field)
        expected = assertion.equals
        if value != expected:
            return (
                f"expected result field '{assertion.field}' == {expected!r}, "
                f"got {value!r}"
            )
        return None

    if assertion.kind == "observation_contains":
        focus = assertion.focus or "summary"
        value = _resolve_field(observation, focus)
        needle = assertion.contains or ""
        if needle not in str(value):
            return f"expected observation '{focus}' to contain '{needle}'"
        return None

    if assertion.kind == "observation_not_contains":
        focus = assertion.focus or "summary"
        value = _resolve_field(observation, focus)
        needle = assertion.contains or ""
        if needle in str(value):
            return f"expected observation '{focus}' to not contain '{needle}'"
        return None

    if assertion.kind == "pending_max":
        field = assertion.field or "total"
        value = _resolve_field(pending, field)
        max_value = assertion.max_value if assertion.max_value is not None else 0
        try:
            numeric = int(value)
        except Exception:  # noqa: BLE001
            return f"pending field '{field}' is not numeric: {value}"
        if numeric > max_value:
            return f"expected pending '{field}' <= {max_value}, got {numeric}"
        return None

    if assertion.kind == "state_contains":
        value = _resolve_field(state, assertion.field)
        needle = assertion.contains or ""
        if needle not in str(value):
            return f"expected state field '{assertion.field}' to contain '{needle}'"
        return None

    if assertion.kind == "state_not_contains":
        value = _resolve_field(state, assertion.field)
        needle = assertion.contains or ""
        if needle in str(value):
            return f"expected state field '{assertion.field}' to not contain '{needle}'"
        return None

    if assertion.kind == "state_equals":
        value = _resolve_field(state, assertion.field)
        expected = assertion.equals
        if value != expected:
            return (
                f"expected state field '{assertion.field}' == {expected!r}, "
                f"got {value!r}"
            )
        return None

    if assertion.kind == "state_exists":
        value = _resolve_field(state, assertion.field)
        if value is None:
            return f"expected state field '{assertion.field}' to exist"
        return None

    if assertion.kind == "state_count_equals":
        value = _resolve_field(state, assertion.field)
        count = _resolve_count(value)
        expected = assertion.equals
        if count is None:
            return f"state field '{assertion.field}' is not countable: {value!r}"
        if count != expected:
            return (
                f"expected state field '{assertion.field}' count == {expected!r}, "
                f"got {count!r}"
            )
        return None

    if assertion.kind == "state_count_max":
        value = _resolve_field(state, assertion.field)
        count = _resolve_count(value)
        max_value = assertion.max_value if assertion.max_value is not None else 0
        if count is None:
            return f"state field '{assertion.field}' is not countable: {value!r}"
        if count > max_value:
            return (
                f"expected state field '{assertion.field}' count <= {max_value}, "
                f"got {count}"
            )
        return None

    if assertion.kind == "time_max_ms":
        max_value = assertion.max_value if assertion.max_value is not None else 0
        if time_ms > max_value:
            return f"expected workflow time <= {max_value} ms, got {time_ms} ms"
        return None

    return f"unknown assertion kind: {assertion.kind}"


def _resolve_field(payload: Any, field: str | None) -> Any:
    if field is None or field == "":
        return payload
    current = payload
    for key in field.split("."):
        if isinstance(current, dict):
            current = current.get(key)
            continue
        if isinstance(current, list):
            try:
                current = current[int(key)]
            except Exception:  # noqa: BLE001
                return None
            continue
        return None
    return current


def _resolve_count(value: Any) -> int | None:
    if value is None:
        return None
    if isinstance(value, (str, list, tuple, set, dict)):
        return len(value)
    try:
        return len(value)  # type: ignore[arg-type]
    except Exception:  # noqa: BLE001
        return None
