from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional

from vei.world import Scenario, compile_scene, generate_scenario, get_scenario

from .models import WorkflowScenarioSpec, WorkflowStepSpec


@dataclass(frozen=True)
class CompiledStep:
    index: int
    step_id: str
    description: str
    tool: str
    args: Dict[str, Any]
    graph_domain: Optional[str]
    graph_action: Optional[str]
    expect: list
    on_failure: str


@dataclass(frozen=True)
class CompiledWorkflow:
    spec: WorkflowScenarioSpec
    scenario: Scenario
    steps: List[CompiledStep]
    step_lookup: Dict[str, CompiledStep]


def load_workflow_spec(payload: Any) -> WorkflowScenarioSpec:
    if isinstance(payload, WorkflowScenarioSpec):
        return payload
    if isinstance(payload, dict):
        return WorkflowScenarioSpec.model_validate(payload)
    if isinstance(payload, (str, Path)):
        path = Path(payload)
        if path.exists():
            data = json.loads(path.read_text(encoding="utf-8"))
            return WorkflowScenarioSpec.model_validate(data)
        data = json.loads(str(payload))
        return WorkflowScenarioSpec.model_validate(data)
    raise TypeError(f"unsupported workflow spec payload: {type(payload)!r}")


def compile_workflow_spec(spec: Any, seed: int = 42042) -> CompiledWorkflow:
    workflow = load_workflow_spec(spec)
    world = dict(workflow.world or {})
    scenario = _compile_world(world, seed=seed)

    merged_metadata = dict(scenario.metadata or {})
    merged_metadata.update(
        {
            "workflow_name": workflow.name,
            "workflow_objective": workflow.objective.statement,
            "workflow_success": list(workflow.objective.success),
            "workflow_actors": [actor.model_dump() for actor in workflow.actors],
            "workflow_constraints": [
                constraint.model_dump() for constraint in workflow.constraints
            ],
            "workflow_approvals": [
                approval.model_dump() for approval in workflow.approvals
            ],
            "workflow_tags": list(workflow.tags),
            "workflow_metadata": dict(workflow.metadata),
        }
    )
    if workflow.metadata:
        for key in ("benchmark_family", "workflow_variant", "workflow_parameters"):
            if key in workflow.metadata:
                merged_metadata[key] = workflow.metadata[key]
    scenario.metadata = merged_metadata

    steps: List[CompiledStep] = []
    for idx, step in enumerate(workflow.steps, start=1):
        steps.append(_compile_step(idx, step))
    step_lookup = {step.step_id: step for step in steps}
    return CompiledWorkflow(
        spec=workflow,
        scenario=scenario,
        steps=steps,
        step_lookup=step_lookup,
    )


def _compile_world(world: Dict[str, Any], seed: int) -> Scenario:
    if not world:
        return Scenario()
    if "catalog" in world:
        return get_scenario(str(world["catalog"]))
    if "meta" in world or "budget" in world or "slack" in world:
        return compile_scene(world, seed=seed)
    return generate_scenario(world, seed=seed)


def _compile_step(idx: int, step: WorkflowStepSpec) -> CompiledStep:
    graph_domain = step.graph_domain
    graph_action = step.graph_action
    compiled_tool = step.tool or "vei.graph_action"
    compiled_args = dict(step.args)
    if graph_domain and graph_action:
        compiled_args = {
            "domain": graph_domain,
            "action": graph_action,
            "args": dict(step.args),
        }
    return CompiledStep(
        index=idx,
        step_id=step.step_id,
        description=step.description,
        tool=compiled_tool,
        args=compiled_args,
        graph_domain=graph_domain,
        graph_action=graph_action,
        expect=list(step.expect),
        on_failure=step.on_failure,
    )
