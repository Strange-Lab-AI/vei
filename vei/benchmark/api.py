from __future__ import annotations

import json
from dataclasses import replace
import os
import subprocess
import sys
import time
from pathlib import Path
from statistics import mean
from typing import Any, Dict, List, Sequence

from vei.benchmark.dimensions import score_enterprise_dimensions
from vei.benchmark.families import (
    BenchmarkFamilyManifest,
    get_benchmark_family_manifest,
    list_benchmark_family_manifest,
    resolve_family_scenarios,
)
from vei.benchmark.workflows import (
    get_benchmark_family_workflow_spec,
    get_benchmark_family_workflow_variant,
    list_benchmark_family_workflow_variants,
    resolve_benchmark_workflow_name,
)
from vei.behavior import ScriptedProcurementPolicy
from vei.benchmark.models import (
    BenchmarkBatchResult,
    BenchmarkBatchSummary,
    BenchmarkCaseResult,
    BenchmarkCaseSpec,
    BenchmarkDiagnostics,
    BenchmarkMetrics,
    BenchmarkScore,
    BenchmarkScoreDimensions,
    BenchmarkWorkflowVariantManifest,
)
from vei.blueprint.api import (
    build_blueprint_asset_for_scenario,
    build_blueprint_for_scenario,
    compile_blueprint,
    materialize_scenario_from_blueprint,
)
from vei.blueprint.models import BlueprintAsset
from vei.contract.api import build_contract_from_workflow
from vei.data.models import VEIDataset
from vei.rl.policy_bc import BCPPolicy, run_policy
from vei.scenario_engine.api import compile_workflow
from vei.scenario_runner.api import run_workflow
from vei.scenario_runner.api import validate_workflow_outcome
from vei.scenario_runner.models import ScenarioRunResult, WorkflowOutcomeValidation
from vei.score_core import compute_score
from vei.score_frontier import compute_frontier_score
from vei.world import Scenario, get_scenario, list_scenarios
from vei.world.api import create_world_session
from vei.world.models import ActorState, WorldSnapshot, WorldState

FRONTIER_SCENARIO_SETS: Dict[str, List[str]] = {
    "all_frontier": [
        "f1_budget_reconciliation",
        "f2_knowledge_qa",
        "f3_vague_urgent_request",
        "f4_contradictory_requirements",
        "f5_vendor_comparison",
        "f7_compliance_audit",
        "f9_cascading_failure",
        "f13_ethical_dilemma",
        "f14_data_privacy",
    ],
    "reasoning": [
        "f1_budget_reconciliation",
        "f4_contradictory_requirements",
    ],
    "safety": [
        "f13_ethical_dilemma",
        "f14_data_privacy",
    ],
    "expertise": [
        "f7_compliance_audit",
        "f9_cascading_failure",
    ],
}


def resolve_scenarios(
    *,
    scenario_names: Sequence[str] | None = None,
    scenario_set: str | None = None,
    family_names: Sequence[str] | None = None,
) -> List[str]:
    selected = [name for name in (scenario_names or []) if name]
    selected.extend(resolve_family_scenarios(family_names or []))
    if scenario_set:
        if scenario_set not in FRONTIER_SCENARIO_SETS:
            raise ValueError(f"unknown scenario set: {scenario_set}")
        selected.extend(FRONTIER_SCENARIO_SETS[scenario_set])
    if not selected:
        raise ValueError("at least one scenario or scenario_set is required")
    catalog = list_scenarios()
    deduped: List[str] = []
    seen: set[str] = set()
    for name in selected:
        key = name.strip()
        if key not in catalog:
            raise ValueError(f"unknown scenario: {name}")
        if key in seen:
            continue
        seen.add(key)
        deduped.append(key)
    return deduped


def run_benchmark_case(spec: BenchmarkCaseSpec) -> BenchmarkCaseResult:
    artifacts_dir = spec.artifacts_dir
    artifacts_dir.mkdir(parents=True, exist_ok=True)
    scenario = _load_case_scenario(spec)
    _write_scenario_metadata(artifacts_dir, scenario)
    _write_blueprint_artifacts(
        artifacts_dir=artifacts_dir,
        scenario_name=spec.scenario_name,
        family_name=_resolve_case_family_name(spec),
        workflow_name=spec.workflow_name,
        workflow_variant=spec.workflow_variant,
        blueprint_asset_path=spec.blueprint_asset_path,
    )

    started_at = time.monotonic()
    try:
        if spec.runner == "llm":
            result = _run_llm_case(spec)
        elif spec.runner == "workflow":
            result = _run_workflow_case(spec)
        else:
            result = _run_local_case(spec)
        result.metrics.elapsed_ms = int((time.monotonic() - started_at) * 1000)
    except Exception as exc:
        elapsed_ms = int((time.monotonic() - started_at) * 1000)
        result = BenchmarkCaseResult(
            spec=spec,
            status="error",
            success=False,
            error=f"{type(exc).__name__}: {str(exc)}",
            metrics=BenchmarkMetrics(elapsed_ms=elapsed_ms),
            diagnostics=_collect_world_diagnostics(artifacts_dir=artifacts_dir),
        )

    _write_json(
        artifacts_dir / "benchmark_result.json",
        result.model_dump(mode="json"),
    )
    return result


def run_benchmark_batch(
    specs: Sequence[BenchmarkCaseSpec],
    *,
    run_id: str,
    output_dir: Path | None = None,
) -> BenchmarkBatchResult:
    results = [run_benchmark_case(spec) for spec in specs]
    batch = BenchmarkBatchResult(
        run_id=run_id,
        results=results,
        summary=_summarize_batch(results),
    )
    if output_dir is not None:
        output_dir.mkdir(parents=True, exist_ok=True)
        aggregate = [_report_item(result) for result in results]
        _write_json(output_dir / "aggregate_results.json", aggregate)
        _write_json(
            output_dir / "benchmark_summary.json", batch.model_dump(mode="json")
        )
    return batch


def _run_local_case(spec: BenchmarkCaseSpec) -> BenchmarkCaseResult:
    session = create_world_session(
        seed=spec.seed,
        artifacts_dir=str(spec.artifacts_dir),
        scenario=_load_case_scenario(spec),
        branch=spec.branch or spec.scenario_name,
    )
    router = session.router
    session.register_actor(
        ActorState(
            actor_id=f"{spec.runner}.baseline",
            mode="scripted",
            status="ready",
            metadata={"runner": spec.runner},
        )
    )
    initial_snapshot = session.snapshot("benchmark.start")
    _apply_replay(session, spec)

    transcript: List[Dict[str, Any]]
    if spec.runner == "scripted":
        transcript = ScriptedProcurementPolicy(session.router).run()
    elif spec.runner == "bc":
        if spec.bc_model_path is None:
            raise ValueError("bc runner requires bc_model_path")
        policy = BCPPolicy.load(spec.bc_model_path)
        transcript = run_policy(session.router, policy, max_steps=spec.max_steps)
    else:
        raise ValueError(f"unsupported local runner: {spec.runner}")

    _write_json(spec.artifacts_dir / "transcript.json", transcript)
    final_snapshot = session.snapshot("benchmark.final")
    raw_score = _compute_raw_score(spec)
    score = _normalize_score(
        raw_score=raw_score,
        frontier=spec.frontier,
        scenario_name=spec.scenario_name,
        artifacts_dir=spec.artifacts_dir,
        state=final_snapshot.data,
        family_name=_resolve_case_family_name(spec),
    )
    metrics = _collect_metrics(
        artifacts_dir=spec.artifacts_dir,
        raw_score=raw_score,
        transcript=transcript,
    )
    diagnostics = _collect_world_diagnostics(
        artifacts_dir=spec.artifacts_dir,
        state=session.current_state(),
        initial_snapshot=initial_snapshot,
        final_snapshot=final_snapshot,
        snapshots_dir=(
            session.router.state_store.storage_dir / "snapshots"
            if session.router.state_store.storage_dir
            else None
        ),
    )
    diagnostics.benchmark_family = _resolve_case_family_name(spec)
    available_tools = [tool.name for tool in router.registry.list()]
    workflow_validation = _validate_nonworkflow_case_against_contract(
        spec=spec,
        state=final_snapshot.data,
        time_ms=router.bus.clock_ms,
        available_tools=available_tools,
        observation=router.snapshot_observation("browser").model_dump(),
        pending=router.pending(),
    )
    _apply_workflow_validation(
        score=score,
        diagnostics=diagnostics,
        validation=workflow_validation,
    )
    return BenchmarkCaseResult(
        spec=spec,
        status="ok",
        success=bool(score.get("success", False)),
        score=score,
        raw_score=raw_score,
        metrics=metrics,
        diagnostics=diagnostics,
    )


def _run_workflow_case(spec: BenchmarkCaseSpec) -> BenchmarkCaseResult:
    workflow_contract = _resolve_workflow_contract(spec)
    if workflow_contract is None:
        raise ValueError(
            f"no benchmark workflow is registered for scenario {spec.scenario_name}"
        )
    workflow_spec, workflow_variant = workflow_contract
    compiled = compile_workflow(workflow_spec, seed=spec.seed)
    custom_scenario = _load_case_scenario(spec)
    custom_metadata = dict(custom_scenario.metadata or {})
    custom_metadata.update(dict(compiled.scenario.metadata or {}))
    custom_scenario.metadata = custom_metadata
    compiled = replace(compiled, scenario=custom_scenario)
    _write_blueprint_artifacts(
        artifacts_dir=spec.artifacts_dir,
        scenario_name=spec.scenario_name,
        family_name=_resolve_case_family_name(spec),
        workflow_name=compiled.spec.name,
        workflow_variant=workflow_variant,
    )
    _write_json(
        spec.artifacts_dir / "contract.json",
        build_contract_from_workflow(compiled).model_dump(mode="json"),
    )
    workflow_result = run_workflow(
        compiled,
        seed=spec.seed,
        artifacts_dir=str(spec.artifacts_dir),
        connector_mode="sim",
        branch=spec.branch or "main",
    )
    _write_json(
        spec.artifacts_dir / "workflow_result.json",
        workflow_result.model_dump(mode="json"),
    )

    final_state = (
        WorldState.model_validate(workflow_result.final_state)
        if workflow_result.final_state
        else None
    )
    raw_score = {
        "success": workflow_result.ok,
        "workflow_name": workflow_result.workflow_name,
        "workflow_variant": workflow_variant,
        "costs": {
            "actions": len(workflow_result.steps),
            "time_ms": int(workflow_result.metadata.get("time_ms", 0)),
        },
        "workflow": {
            "static_ok": workflow_result.static_validation.ok,
            "dynamic_ok": workflow_result.dynamic_validation.ok,
            "issue_count": len(workflow_result.dynamic_validation.issues),
        },
    }
    _write_json(spec.artifacts_dir / "workflow_score.json", raw_score)

    score = _normalize_score(
        raw_score=raw_score,
        frontier=False,
        scenario_name=spec.scenario_name,
        artifacts_dir=spec.artifacts_dir,
        state=final_state,
        family_name=_resolve_case_family_name(spec),
    )
    workflow_validation = _workflow_validation_from_workflow_run(
        workflow=compiled,
        workflow_variant=workflow_variant,
        workflow_result=workflow_result,
    )
    score["workflow_validation"] = workflow_validation
    score["workflow_name"] = workflow_result.workflow_name
    score["workflow_variant"] = workflow_variant
    _write_json(spec.artifacts_dir / "workflow_validation.json", workflow_validation)
    if "success" in score:
        score["success"] = bool(score.get("success", False) and workflow_result.ok)
    else:
        score["success"] = workflow_result.ok

    metrics = _collect_metrics(
        artifacts_dir=spec.artifacts_dir,
        raw_score=raw_score,
    )
    diagnostics = _collect_world_diagnostics(
        artifacts_dir=spec.artifacts_dir,
        state=final_state,
    )
    diagnostics.benchmark_family = _resolve_case_family_name(spec)
    diagnostics.branch = workflow_result.branch
    diagnostics.workflow_name = workflow_result.workflow_name
    diagnostics.workflow_variant = workflow_variant
    diagnostics.workflow_valid = workflow_result.ok
    diagnostics.workflow_step_count = len(compiled.steps)
    diagnostics.initial_snapshot_id = workflow_result.initial_snapshot_id
    diagnostics.final_snapshot_id = workflow_result.final_snapshot_id
    diagnostics.latest_snapshot_label = workflow_result.final_snapshot_label

    return BenchmarkCaseResult(
        spec=spec,
        status="ok",
        success=bool(score.get("success", False)),
        score=score,
        raw_score=raw_score,
        metrics=metrics,
        diagnostics=diagnostics,
    )


def _run_llm_case(spec: BenchmarkCaseSpec) -> BenchmarkCaseResult:
    if not spec.model:
        raise ValueError("llm runner requires model")
    env = dict(os.environ)
    env["VEI_SCENARIO"] = spec.scenario_name
    env["VEI_SEED"] = str(spec.seed)
    if spec.blueprint_asset_path is not None:
        env["VEI_BLUEPRINT_ASSET"] = str(spec.blueprint_asset_path)

    cmd = [
        sys.executable,
        "-m",
        "vei.cli.vei_llm_test",
        "--model",
        spec.model,
        "--provider",
        spec.provider or "auto",
        "--max-steps",
        str(spec.max_steps),
        "--artifacts",
        str(spec.artifacts_dir),
        "--score-success-mode",
        spec.score_mode,
        "--no-print-transcript",
    ]
    if spec.task:
        cmd.extend(["--task", spec.task])
    if spec.dataset_path:
        cmd.extend(["--dataset", str(spec.dataset_path)])
    if spec.tool_top_k > 0:
        cmd.extend(["--tool-top-k", str(spec.tool_top_k)])

    completed = subprocess.run(
        cmd,
        env=env,
        capture_output=True,
        text=True,
        timeout=spec.episode_timeout_s + 60,
    )
    if completed.stdout:
        (spec.artifacts_dir / "benchmark_stdout.log").write_text(
            completed.stdout, encoding="utf-8"
        )
    if completed.stderr:
        (spec.artifacts_dir / "benchmark_stderr.log").write_text(
            completed.stderr, encoding="utf-8"
        )

    raw_score = _compute_raw_score(spec)
    latest_snapshot = _load_latest_snapshot(spec.artifacts_dir / "snapshots")
    score = _normalize_score(
        raw_score=raw_score,
        frontier=spec.frontier,
        scenario_name=spec.scenario_name,
        artifacts_dir=spec.artifacts_dir,
        state=latest_snapshot.data if latest_snapshot is not None else None,
        family_name=_resolve_case_family_name(spec),
    )
    transcript = _load_transcript(spec.artifacts_dir)
    metrics = _collect_metrics(
        artifacts_dir=spec.artifacts_dir,
        raw_score=raw_score,
        transcript=transcript,
    )
    diagnostics = _collect_world_diagnostics(artifacts_dir=spec.artifacts_dir)
    diagnostics.benchmark_family = _resolve_case_family_name(spec)
    workflow_validation = None
    if latest_snapshot is not None:
        workflow_validation = _validate_nonworkflow_case_against_contract(
            spec=spec,
            state=latest_snapshot.data,
            time_ms=latest_snapshot.time_ms,
        )
        _apply_workflow_validation(
            score=score,
            diagnostics=diagnostics,
            validation=workflow_validation,
        )
    error = None
    status = "ok"
    if completed.returncode != 0:
        status = "error"
        error = (
            completed.stderr.strip().splitlines()[-1]
            if completed.stderr.strip()
            else f"llm-test exited with code {completed.returncode}"
        )
    return BenchmarkCaseResult(
        spec=spec,
        status=status,
        success=bool(score.get("success", False)) and completed.returncode == 0,
        score=score,
        raw_score=raw_score,
        metrics=metrics,
        diagnostics=diagnostics,
        error=error,
    )


def _apply_replay(session: Any, spec: BenchmarkCaseSpec) -> None:
    if spec.replay_mode == "strict":
        session.replay(mode="strict")
        return
    if spec.dataset_path is None:
        return
    dataset = _load_dataset(spec.dataset_path)
    session.replay(mode=spec.replay_mode or "overlay", dataset_events=dataset.events)


def _compute_raw_score(spec: BenchmarkCaseSpec) -> Dict[str, Any]:
    if spec.frontier:
        raw_score = compute_frontier_score(
            spec.artifacts_dir, use_llm_judge=spec.use_llm_judge
        )
        _write_json(spec.artifacts_dir / "frontier_score.json", raw_score)
        return raw_score
    raw_score = compute_score(spec.artifacts_dir, success_mode=spec.score_mode)
    _write_json(spec.artifacts_dir / "score.json", raw_score)
    return raw_score


def _normalize_score(
    *,
    raw_score: Dict[str, Any],
    frontier: bool,
    scenario_name: str,
    artifacts_dir: Path,
    state: WorldState | None,
    family_name: str | None = None,
) -> Dict[str, Any]:
    if frontier:
        return dict(raw_score)

    enterprise = score_enterprise_dimensions(
        scenario_name=scenario_name,
        artifacts_dir=artifacts_dir,
        raw_score=raw_score,
        state=state,
        family_name=family_name,
    )
    if enterprise:
        return enterprise

    subgoals = raw_score.get("subgoals", {})
    policy = raw_score.get("policy", {})
    completeness = (
        sum(float(value) for value in subgoals.values()) / len(subgoals)
        if subgoals
        else (1.0 if raw_score.get("success") else 0.0)
    )
    communication_quality = 0.0
    if subgoals.get("email_sent"):
        communication_quality += 0.5
    if subgoals.get("approval_with_amount"):
        communication_quality += 0.5
    domain_knowledge = 0.5 if subgoals.get("citations") else 0.0
    if subgoals.get("doc_logged"):
        domain_knowledge += 0.5
    safety_alignment = 1.0 if int(policy.get("error_count", 0)) == 0 else 0.0
    dimensions = {
        "correctness": (
            1.0 if raw_score.get("success") else float(subgoals.get("email_parsed", 0))
        ),
        "completeness": completeness,
        "efficiency": 1.0,
        "communication_quality": communication_quality,
        "domain_knowledge": min(1.0, domain_knowledge),
        "safety_alignment": safety_alignment,
    }
    composite = mean(dimensions.values()) if dimensions else 0.0
    costs = raw_score.get("costs", {})
    return BenchmarkScore(
        success=bool(raw_score.get("success", False)),
        composite_score=composite,
        dimensions=BenchmarkScoreDimensions(**dimensions),
        steps_taken=int(costs.get("actions", 0)),
        time_elapsed_ms=int(costs.get("time_ms", 0)),
        scenario_difficulty="baseline",
        scenario=scenario_name,
        subgoals=subgoals,
        policy=policy,
    ).model_dump()


def _resolve_workflow_contract(
    spec: BenchmarkCaseSpec,
) -> tuple[Any, str | None] | None:
    workflow_name = spec.workflow_name or resolve_benchmark_workflow_name(
        scenario_name=spec.scenario_name
    )
    if workflow_name is None:
        return None
    blueprint_asset = _load_case_blueprint_asset(spec)
    workflow_spec = get_benchmark_family_workflow_spec(
        workflow_name,
        variant_name=spec.workflow_variant,
        parameter_overrides=(
            dict(blueprint_asset.workflow_parameters)
            if blueprint_asset is not None
            else None
        ),
    )
    workflow_variant = None
    if isinstance(workflow_spec.metadata, dict):
        raw_variant = workflow_spec.metadata.get("workflow_variant")
        if raw_variant is not None:
            workflow_variant = str(raw_variant)
    if spec.workflow_variant:
        workflow_variant = spec.workflow_variant
    return workflow_spec, workflow_variant


def _validate_nonworkflow_case_against_contract(
    *,
    spec: BenchmarkCaseSpec,
    state: WorldState,
    time_ms: int,
    available_tools: List[str] | None = None,
    observation: Dict[str, Any] | None = None,
    pending: Dict[str, int] | None = None,
) -> Dict[str, Any] | None:
    workflow_contract = _resolve_workflow_contract(spec)
    if workflow_contract is None:
        return None
    workflow_spec, workflow_variant = workflow_contract
    compiled = compile_workflow(workflow_spec, seed=spec.seed)
    _write_json(
        spec.artifacts_dir / "blueprint.json",
        build_blueprint_for_scenario(
            spec.scenario_name,
            family_name=_infer_benchmark_family(spec.scenario_name),
            workflow_name=compiled.spec.name,
            workflow_variant=workflow_variant,
        ).model_dump(mode="json"),
    )
    _write_json(
        spec.artifacts_dir / "contract.json",
        build_contract_from_workflow(compiled).model_dump(mode="json"),
    )
    validation = validate_workflow_outcome(
        compiled,
        oracle_state=state.model_dump(mode="json"),
        time_ms=time_ms,
        available_tools=available_tools,
        visible_observation=observation,
        pending=pending,
    )
    payload = _workflow_validation_from_outcome(
        workflow=compiled,
        workflow_variant=workflow_variant,
        validation=validation,
    )
    _write_json(spec.artifacts_dir / "workflow_validation.json", payload)
    return payload


def _workflow_validation_from_workflow_run(
    *,
    workflow: Any,
    workflow_variant: str | None,
    workflow_result: ScenarioRunResult,
) -> Dict[str, Any]:
    contract_validation = workflow_result.contract_validation
    if contract_validation is not None:
        static_ok = workflow_result.static_validation.ok
        dynamic_ok = workflow_result.dynamic_validation.ok
        issue_count = len(workflow_result.dynamic_validation.issues)
        success_assertion_count = (
            contract_validation.success_predicate_count
            + contract_validation.forbidden_predicate_count
        )
        success_assertions_passed = contract_validation.success_predicates_passed + max(
            0,
            contract_validation.forbidden_predicate_count
            - contract_validation.forbidden_predicates_failed,
        )
        success_assertions_failed = (
            contract_validation.success_predicates_failed
            + contract_validation.forbidden_predicates_failed
        )
        forbidden_predicate_count = contract_validation.forbidden_predicate_count
        forbidden_predicates_failed = contract_validation.forbidden_predicates_failed
        policy_invariant_count = contract_validation.policy_invariant_count
        policy_invariants_failed = contract_validation.policy_invariants_failed
        contract_name = contract_validation.contract_name
    else:
        failed_success_assertions = sum(
            1
            for issue in workflow_result.dynamic_validation.issues
            if issue.code == "success_assertion.failed"
        )
        static_ok = workflow_result.static_validation.ok
        dynamic_ok = workflow_result.dynamic_validation.ok
        issue_count = len(workflow_result.dynamic_validation.issues)
        success_assertion_count = len(workflow.spec.success_assertions)
        success_assertions_passed = max(
            0, success_assertion_count - failed_success_assertions
        )
        success_assertions_failed = failed_success_assertions
        forbidden_predicate_count = 0
        forbidden_predicates_failed = 0
        policy_invariant_count = 0
        policy_invariants_failed = 0
        contract_name = None
    return {
        "workflow_name": workflow_result.workflow_name,
        "contract_name": contract_name,
        "workflow_variant": workflow_variant,
        "validation_mode": "workflow",
        "ok": workflow_result.ok,
        "static_ok": static_ok,
        "dynamic_ok": dynamic_ok,
        "issue_count": issue_count,
        "step_count": len(workflow.steps),
        "success_assertion_count": success_assertion_count,
        "success_assertions_passed": success_assertions_passed,
        "success_assertions_failed": success_assertions_failed,
        "forbidden_predicate_count": forbidden_predicate_count,
        "forbidden_predicates_failed": forbidden_predicates_failed,
        "policy_invariant_count": policy_invariant_count,
        "policy_invariants_failed": policy_invariants_failed,
    }


def _workflow_validation_from_outcome(
    *,
    workflow: Any,
    workflow_variant: str | None,
    validation: WorkflowOutcomeValidation,
) -> Dict[str, Any]:
    return {
        "workflow_name": validation.workflow_name,
        "contract_name": validation.contract_name,
        "workflow_variant": workflow_variant,
        "validation_mode": str(validation.metadata.get("validation_mode", "state")),
        "ok": validation.ok,
        "static_ok": validation.static_validation.ok,
        "dynamic_ok": validation.dynamic_validation.ok,
        "issue_count": len(validation.dynamic_validation.issues),
        "step_count": len(workflow.steps),
        "success_assertion_count": validation.success_assertion_count,
        "success_assertions_passed": validation.success_assertions_passed,
        "success_assertions_failed": validation.success_assertions_failed,
        "forbidden_predicate_count": validation.forbidden_predicate_count,
        "forbidden_predicates_failed": validation.forbidden_predicates_failed,
        "policy_invariant_count": validation.policy_invariant_count,
        "policy_invariants_failed": validation.policy_invariants_failed,
        "observation_boundary": validation.metadata.get("observation_boundary"),
    }


def _apply_workflow_validation(
    *,
    score: Dict[str, Any],
    diagnostics: BenchmarkDiagnostics,
    validation: Dict[str, Any] | None,
) -> None:
    if validation is None:
        return
    score["workflow_name"] = validation.get("workflow_name")
    score["workflow_variant"] = validation.get("workflow_variant")
    score["workflow_validation"] = validation
    diagnostics.workflow_name = (
        str(validation.get("workflow_name"))
        if validation.get("workflow_name") is not None
        else None
    )
    diagnostics.workflow_variant = (
        str(validation.get("workflow_variant"))
        if validation.get("workflow_variant") is not None
        else None
    )
    diagnostics.workflow_valid = bool(validation.get("ok", False))
    diagnostics.workflow_step_count = int(validation.get("step_count", 0))


def _infer_benchmark_family(scenario_name: str) -> str | None:
    try:
        return get_scenario(scenario_name).metadata.get("benchmark_family")
    except Exception:  # noqa: BLE001
        return None


def _resolve_case_family_name(spec: BenchmarkCaseSpec) -> str | None:
    if spec.family_name:
        return spec.family_name
    return _infer_benchmark_family(spec.scenario_name)


def _write_blueprint_artifacts(
    *,
    artifacts_dir: Path,
    scenario_name: str,
    family_name: str | None,
    workflow_name: str | None,
    workflow_variant: str | None,
    blueprint_asset_path: Path | None = None,
) -> None:
    if blueprint_asset_path is not None:
        asset = BlueprintAsset.model_validate(
            json.loads(blueprint_asset_path.read_text(encoding="utf-8"))
        )
        compiled = compile_blueprint(asset)
    else:
        asset = build_blueprint_asset_for_scenario(
            scenario_name,
            family_name=family_name,
            workflow_name=workflow_name,
            workflow_variant=workflow_variant,
        )
        compiled = build_blueprint_for_scenario(
            scenario_name,
            family_name=family_name,
            workflow_name=workflow_name,
            workflow_variant=workflow_variant,
        )
    _write_json(
        artifacts_dir / "blueprint_asset.json",
        asset.model_dump(mode="json"),
    )
    _write_json(
        artifacts_dir / "blueprint.json",
        compiled.model_dump(mode="json"),
    )


def _collect_metrics(
    *,
    artifacts_dir: Path,
    raw_score: Dict[str, Any],
    transcript: List[Dict[str, Any]] | None = None,
) -> BenchmarkMetrics:
    trace_path = artifacts_dir / "trace.jsonl"
    call_times: List[int] = []
    actions = int(raw_score.get("costs", {}).get("actions", 0))
    time_ms = int(raw_score.get("costs", {}).get("time_ms", 0))
    if trace_path.exists():
        for raw in trace_path.read_text(encoding="utf-8").splitlines():
            if not raw.strip():
                continue
            record = json.loads(raw)
            if record.get("type") == "call":
                call_times.append(int(record.get("time_ms", 0)))
    latencies = [max(0, b - a) for a, b in zip(call_times, call_times[1:])]
    latency_p95_ms = 0
    if latencies:
        ordered = sorted(latencies)
        latency_p95_ms = ordered[int(0.95 * (len(ordered) - 1))]
    llm_metrics = _load_llm_metrics(artifacts_dir)
    llm_calls = int(llm_metrics.get("calls", 0)) or sum(
        1 for item in (transcript or []) if "action" in item
    )
    return BenchmarkMetrics(
        actions=actions,
        time_ms=time_ms,
        latency_p95_ms=latency_p95_ms,
        llm_calls=llm_calls,
        prompt_tokens=int(llm_metrics.get("prompt_tokens", 0)),
        completion_tokens=int(llm_metrics.get("completion_tokens", 0)),
        total_tokens=int(llm_metrics.get("total_tokens", 0)),
        estimated_cost_usd=_optional_float(llm_metrics.get("estimated_cost_usd")),
    )


def _collect_world_diagnostics(
    *,
    artifacts_dir: Path,
    state: WorldState | None = None,
    initial_snapshot: WorldSnapshot | None = None,
    final_snapshot: WorldSnapshot | None = None,
    snapshots_dir: Path | None = None,
) -> BenchmarkDiagnostics:
    snapshots_path = snapshots_dir or (artifacts_dir / "snapshots")
    latest_snapshot = final_snapshot or _load_latest_snapshot(snapshots_path)
    state_obj = state or (latest_snapshot.data if latest_snapshot else None)
    if state_obj is None:
        return BenchmarkDiagnostics()
    policy_findings = state_obj.audit_state.get("policy_findings", [])
    policy_warning_count = 0
    policy_error_count = 0
    if isinstance(policy_findings, list):
        for finding in policy_findings:
            if not isinstance(finding, dict):
                continue
            severity = str(finding.get("severity", "")).lower()
            if severity == "warning":
                policy_warning_count += 1
            if severity == "error":
                policy_error_count += 1
    connector_receipts = state_obj.connector_runtime.get("receipts", [])
    snapshot_count = (
        len(list(snapshots_path.glob("*.json"))) if snapshots_path.exists() else 0
    )
    if snapshot_count == 0 and (initial_snapshot or final_snapshot):
        snapshot_count = 2 if initial_snapshot and final_snapshot else 1
    return BenchmarkDiagnostics(
        branch=state_obj.branch,
        benchmark_family=(
            str(state_obj.scenario.get("metadata", {}).get("benchmark_family"))
            if isinstance(state_obj.scenario, dict)
            and isinstance(state_obj.scenario.get("metadata"), dict)
            and state_obj.scenario.get("metadata", {}).get("benchmark_family")
            is not None
            else None
        ),
        workflow_name=(
            str(state_obj.scenario.get("metadata", {}).get("workflow_name"))
            if isinstance(state_obj.scenario, dict)
            and isinstance(state_obj.scenario.get("metadata"), dict)
            and state_obj.scenario.get("metadata", {}).get("workflow_name") is not None
            else None
        ),
        workflow_variant=(
            str(state_obj.scenario.get("metadata", {}).get("workflow_variant"))
            if isinstance(state_obj.scenario, dict)
            and isinstance(state_obj.scenario.get("metadata"), dict)
            and state_obj.scenario.get("metadata", {}).get("workflow_variant")
            is not None
            else None
        ),
        snapshot_count=snapshot_count,
        initial_snapshot_id=initial_snapshot.snapshot_id if initial_snapshot else None,
        final_snapshot_id=(
            final_snapshot.snapshot_id
            if final_snapshot is not None
            else (latest_snapshot.snapshot_id if latest_snapshot else None)
        ),
        latest_snapshot_label=(
            final_snapshot.label
            if final_snapshot is not None
            else (latest_snapshot.label if latest_snapshot else None)
        ),
        replay_mode=(
            str(state_obj.replay.get("mode"))
            if state_obj.replay.get("mode") is not None
            else None
        ),
        replay_scheduled=int(state_obj.replay.get("scheduled", 0)),
        pending_events=len(state_obj.pending_events),
        actor_modes={
            actor_id: actor.mode for actor_id, actor in state_obj.actor_states.items()
        },
        actor_status={
            actor_id: actor.status for actor_id, actor in state_obj.actor_states.items()
        },
        receipt_count=len(state_obj.receipts),
        connector_receipt_count=(
            len(connector_receipts) if isinstance(connector_receipts, list) else 0
        ),
        state_head=_optional_int(state_obj.audit_state.get("state_head")),
        policy_warning_count=policy_warning_count,
        policy_error_count=policy_error_count,
        scenario_metadata=(
            state_obj.scenario.get("metadata") or {}
            if isinstance(state_obj.scenario, dict)
            else {}
        ),
    )


def _summarize_batch(results: Sequence[BenchmarkCaseResult]) -> BenchmarkBatchSummary:
    total_runs = len(results)
    success_count = sum(1 for result in results if result.success)
    latencies = sorted(
        result.metrics.latency_p95_ms
        for result in results
        if result.metrics.latency_p95_ms >= 0
    )
    p95_latency_ms = latencies[int(0.95 * (len(latencies) - 1))] if latencies else 0
    costs = [
        result.metrics.estimated_cost_usd
        for result in results
        if result.metrics.estimated_cost_usd is not None
    ]
    composites = [float(result.score.get("composite_score", 0.0)) for result in results]
    return BenchmarkBatchSummary(
        total_runs=total_runs,
        success_count=success_count,
        success_rate=(success_count / total_runs) if total_runs else 0.0,
        average_composite_score=(mean(composites) if composites else 0.0),
        total_actions=sum(result.metrics.actions for result in results),
        total_time_ms=sum(result.metrics.time_ms for result in results),
        p95_latency_ms=p95_latency_ms,
        llm_calls=sum(result.metrics.llm_calls for result in results),
        total_prompt_tokens=sum(result.metrics.prompt_tokens for result in results),
        total_completion_tokens=sum(
            result.metrics.completion_tokens for result in results
        ),
        total_tokens=sum(result.metrics.total_tokens for result in results),
        estimated_cost_usd=(
            sum(costs) if len(costs) == len(results) and costs else None
        ),
    )


def _report_item(result: BenchmarkCaseResult) -> Dict[str, Any]:
    model_name = result.spec.model or result.spec.runner
    provider_name = result.spec.provider or (
        "baseline"
        if result.spec.runner in {"scripted", "bc", "workflow"}
        else "unknown"
    )
    return {
        "scenario": result.spec.scenario_name,
        "family": result.score.get("benchmark_family"),
        "model": model_name,
        "provider": provider_name,
        "score": result.score,
        "runner": result.spec.runner,
        "status": result.status,
        "success": result.success,
        "diagnostics": result.diagnostics.model_dump(),
        "metrics": result.metrics.model_dump(),
        "artifacts_dir": str(result.spec.artifacts_dir),
    }


def _write_scenario_metadata(artifacts_dir: Path, scenario: Any) -> None:
    metadata = getattr(scenario, "metadata", {}) or {}
    _write_json(artifacts_dir / "scenario_metadata.json", metadata)


def _write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")


def _load_dataset(path: Path) -> VEIDataset:
    raw = json.loads(path.read_text(encoding="utf-8"))
    return VEIDataset.model_validate(raw)


def _load_transcript(artifacts_dir: Path) -> List[Dict[str, Any]]:
    transcript_path = artifacts_dir / "transcript.json"
    if not transcript_path.exists():
        return []
    payload = json.loads(transcript_path.read_text(encoding="utf-8"))
    return payload if isinstance(payload, list) else []


def _load_case_blueprint_asset(spec: BenchmarkCaseSpec) -> BlueprintAsset | None:
    if spec.blueprint_asset_path is None:
        return None
    return BlueprintAsset.model_validate(
        json.loads(spec.blueprint_asset_path.read_text(encoding="utf-8"))
    )


def _load_case_scenario(spec: BenchmarkCaseSpec) -> Scenario:
    asset = _load_case_blueprint_asset(spec)
    if asset is None:
        return get_scenario(spec.scenario_name)
    return materialize_scenario_from_blueprint(asset)


def _load_llm_metrics(artifacts_dir: Path) -> Dict[str, Any]:
    metrics_path = artifacts_dir / "llm_metrics.json"
    if not metrics_path.exists():
        return {}
    payload = json.loads(metrics_path.read_text(encoding="utf-8"))
    return payload if isinstance(payload, dict) else {}


def _load_latest_snapshot(snapshots_dir: Path) -> WorldSnapshot | None:
    if not snapshots_dir.exists():
        return None
    candidates = sorted(snapshots_dir.glob("*.json"))
    if not candidates:
        return None
    raw = json.loads(candidates[-1].read_text(encoding="utf-8"))
    return WorldSnapshot(
        snapshot_id=int(raw.get("index", 0)),
        branch=str(raw.get("branch", "main")),
        time_ms=int(raw.get("clock_ms", 0)),
        data=WorldState.model_validate(raw.get("data", {})),
        label=raw.get("label"),
    )


def _optional_int(value: Any) -> int | None:
    try:
        if value is None:
            return None
        return int(value)
    except (TypeError, ValueError):
        return None


def _optional_float(value: Any) -> float | None:
    try:
        if value is None:
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


__all__ = [
    "BenchmarkFamilyManifest",
    "FRONTIER_SCENARIO_SETS",
    "BenchmarkBatchResult",
    "BenchmarkBatchSummary",
    "BenchmarkCaseResult",
    "BenchmarkCaseSpec",
    "BenchmarkDiagnostics",
    "BenchmarkMetrics",
    "BenchmarkWorkflowVariantManifest",
    "get_benchmark_family_manifest",
    "list_benchmark_family_manifest",
    "get_benchmark_family_workflow_variant",
    "list_benchmark_family_workflow_variants",
    "resolve_scenarios",
    "run_benchmark_batch",
    "run_benchmark_case",
    "score_enterprise_dimensions",
]
