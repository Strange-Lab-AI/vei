from __future__ import annotations

from typing import Iterable, Protocol

from vei.scenario_engine.compiler import CompiledWorkflow

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
    ) -> ScenarioRunResult: ...


def run_workflow(
    workflow: CompiledWorkflow,
    *,
    seed: int = 42042,
    artifacts_dir: str | None = None,
    connector_mode: str = "sim",
) -> ScenarioRunResult:
    return run_compiled_workflow(
        workflow,
        seed=seed,
        artifacts_dir=artifacts_dir,
        connector_mode=connector_mode,
    )


def validate_workflow(
    workflow: CompiledWorkflow, *, available_tools: Iterable[str] | None = None
) -> ValidationReport:
    return static_validate_workflow(workflow, available_tools=available_tools)


def validate_workflow_outcome(
    workflow: CompiledWorkflow,
    *,
    state: dict,
    time_ms: int = 0,
    available_tools: Iterable[str] | None = None,
    result: object | None = None,
    observation: dict | None = None,
    pending: dict[str, int] | None = None,
) -> WorkflowOutcomeValidation:
    return validate_compiled_workflow_outcome(
        workflow,
        state=state,
        time_ms=time_ms,
        available_tools=list(available_tools) if available_tools is not None else None,
        result=result,
        observation=observation,
        pending=pending,
    )
