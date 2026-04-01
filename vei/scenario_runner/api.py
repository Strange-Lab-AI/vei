from __future__ import annotations

from typing import Iterable, Protocol

from vei.scenario_engine.api import CompiledWorkflow

from .models import ScenarioRunResult, ValidationReport, WorkflowOutcomeValidation
from .runner import run_compiled_workflow, validate_compiled_workflow_outcome
from .validator import static_validate_workflow


class WorkflowRunnerAPI(Protocol):
    def __call__(
        self,
        workflow: CompiledWorkflow,
        *,
        seed: int = 42042,
        artifacts_dir: str | None = None,
        connector_mode: str = "sim",
        branch: str = "main",
    ) -> ScenarioRunResult: ...


def run_workflow(
    workflow: CompiledWorkflow,
    *,
    seed: int = 42042,
    artifacts_dir: str | None = None,
    connector_mode: str = "sim",
    branch: str = "main",
) -> ScenarioRunResult:
    return run_compiled_workflow(
        workflow,
        seed=seed,
        artifacts_dir=artifacts_dir,
        connector_mode=connector_mode,
        branch=branch,
    )


def validate_workflow(
    workflow: CompiledWorkflow, *, available_tools: Iterable[str] | None = None
) -> ValidationReport:
    return static_validate_workflow(workflow, available_tools=available_tools)


def validate_workflow_outcome(
    workflow: CompiledWorkflow,
    *,
    oracle_state: dict | None = None,
    time_ms: int = 0,
    available_tools: Iterable[str] | None = None,
    result: object | None = None,
    visible_observation: dict | None = None,
    pending: dict[str, int] | None = None,
    state: dict | None = None,
    observation: dict | None = None,
) -> WorkflowOutcomeValidation:
    resolved_state = oracle_state if oracle_state is not None else state
    if resolved_state is None:
        raise ValueError("validate_workflow_outcome requires oracle_state")
    resolved_observation = (
        visible_observation if visible_observation is not None else observation
    )
    return validate_compiled_workflow_outcome(
        workflow,
        oracle_state=resolved_state,
        time_ms=time_ms,
        available_tools=list(available_tools) if available_tools is not None else None,
        result=result,
        visible_observation=resolved_observation,
        pending=pending,
    )
