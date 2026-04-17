from __future__ import annotations

from dataclasses import dataclass
import json
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Dict, Iterator, Optional
from uuid import uuid4

from vei.benchmark.api import run_benchmark_case
from vei.benchmark.models import BenchmarkCaseSpec, BenchmarkRunner
from vei.capability_graph.api import build_runtime_capability_graphs
from vei.contract.models import ContractEvaluationResult
from vei.orientation.api import build_world_orientation
from vei.visualization.api import (
    flow_channel_from_focus,
    flow_channel_from_tool,
    load_trace,
)
from vei.world import StateStore
from vei.world.models import WorldState
from vei.workspace.api import (
    compile_workspace,
    evaluate_workspace_contract_against_state,
    list_workspace_runs,
    load_workspace,
    resolve_workspace_scenario,
    upsert_workspace_run,
)
from vei.workspace.models import WorkspaceRunEntry
from vei.workspace.models import WorkspaceManifest, WorkspaceScenarioSpec

from .events import (
    append_run_event,
    append_run_events,
    load_run_events,
)
from .models import (
    LivingSurfaceState,
    RunArtifactIndex,
    RunContractSummary,
    RunManifest,
    RunSnapshotRef,
    RunTimelineEvent,
)
from .reproducibility import (
    build_reproducibility_record,
    merge_reproducibility_metadata,
)


@dataclass(frozen=True)
class _PreparedRunLaunch:
    workspace_root: Path
    workspace_manifest: WorkspaceManifest
    scenario: WorkspaceScenarioSpec
    runner: BenchmarkRunner
    seed: int
    run_id: str
    branch: str
    model: str | None
    provider: str | None
    bc_model_path: Path | None
    task: str | None
    max_steps: int
    run_dir: Path
    artifacts_dir: Path
    state_dir: Path
    events_path: Path
    blueprint_asset_path: Path
    contract_path: str
    contract_source_path: Path
    manifest_stub: RunManifest
    benchmark_spec: BenchmarkCaseSpec


def launch_workspace_run(
    root: str | Path,
    *,
    runner: str,
    scenario_name: Optional[str] = None,
    run_id: Optional[str] = None,
    seed: int = 42042,
    branch: Optional[str] = None,
    model: Optional[str] = None,
    provider: Optional[str] = None,
    bc_model_path: str | Path | None = None,
    task: Optional[str] = None,
    max_steps: int = 12,
) -> RunManifest:
    prepared = _prepare_run_launch(
        root=root,
        runner=runner,
        scenario_name=scenario_name,
        run_id=run_id,
        seed=seed,
        branch=branch,
        model=model,
        provider=provider,
        bc_model_path=bc_model_path,
        task=task,
        max_steps=max_steps,
    )
    _record_run_start(prepared)

    try:
        with _temporary_env("VEI_STATE_DIR", str(prepared.state_dir)):
            result = run_benchmark_case(prepared.benchmark_spec)
        return _finalize_successful_run(prepared, result)
    except Exception as exc:
        _finalize_failed_run(prepared, exc)
        raise


def _prepare_run_launch(
    *,
    root: str | Path,
    runner: str,
    scenario_name: str | None,
    run_id: str | None,
    seed: int,
    branch: str | None,
    model: str | None,
    provider: str | None,
    bc_model_path: str | Path | None,
    task: str | None,
    max_steps: int,
) -> _PreparedRunLaunch:
    workspace_root = Path(root).expanduser().resolve()
    normalized_runner = normalize_runner(runner)
    resolved_bc_model_path = _resolve_bc_model_path(
        normalized_runner,
        bc_model_path,
    )
    summary = compile_workspace(workspace_root)
    workspace_manifest = summary.manifest
    scenario = resolve_workspace_scenario(
        workspace_root,
        workspace_manifest,
        scenario_name,
    )

    resolved_run_id = run_id or generate_run_id()
    resolved_branch = branch or f"{workspace_manifest.name}.{resolved_run_id}"
    run_dir = workspace_root / workspace_manifest.runs_dir / resolved_run_id
    if run_dir.exists():
        raise ValueError(f"run_id already exists: {resolved_run_id}")

    artifacts_dir = run_dir / "artifacts"
    state_dir = run_dir / "state"
    events_path = run_dir / "events.jsonl"
    artifacts_dir.mkdir(parents=True, exist_ok=True)
    state_dir.mkdir(parents=True, exist_ok=True)

    scenario_root = workspace_root / workspace_manifest.compiled_root / scenario.name
    blueprint_asset_path = scenario_root / "blueprint_asset.json"
    contract_path = (
        scenario.contract_path
        or f"{workspace_manifest.contracts_dir}/{scenario.name}.contract.json"
    )
    contract_source_path = workspace_root / contract_path

    manifest_stub = _build_running_manifest(
        workspace_root=workspace_root,
        workspace_manifest=workspace_manifest,
        scenario=scenario,
        runner=normalized_runner,
        seed=seed,
        run_id=resolved_run_id,
        branch=resolved_branch,
        model=model,
        provider=provider,
        bc_model_path=resolved_bc_model_path,
        run_dir=run_dir,
        artifacts_dir=artifacts_dir,
        state_dir=state_dir,
        events_path=events_path,
        blueprint_asset_path=blueprint_asset_path,
        contract_path=contract_path,
        contract_source_path=contract_source_path,
    )
    benchmark_spec = _build_benchmark_case_spec(
        workspace_manifest=workspace_manifest,
        scenario=scenario,
        runner=normalized_runner,
        seed=seed,
        branch=resolved_branch,
        model=model,
        provider=provider,
        bc_model_path=resolved_bc_model_path,
        task=task,
        max_steps=max_steps,
        blueprint_asset_path=blueprint_asset_path,
        artifacts_dir=artifacts_dir,
    )
    return _PreparedRunLaunch(
        workspace_root=workspace_root,
        workspace_manifest=workspace_manifest,
        scenario=scenario,
        runner=normalized_runner,
        seed=seed,
        run_id=resolved_run_id,
        branch=resolved_branch,
        model=model,
        provider=provider,
        bc_model_path=resolved_bc_model_path,
        task=task,
        max_steps=max_steps,
        run_dir=run_dir,
        artifacts_dir=artifacts_dir,
        state_dir=state_dir,
        events_path=events_path,
        blueprint_asset_path=blueprint_asset_path,
        contract_path=contract_path,
        contract_source_path=contract_source_path,
        manifest_stub=manifest_stub,
        benchmark_spec=benchmark_spec,
    )


def _resolve_bc_model_path(
    runner: BenchmarkRunner,
    bc_model_path: str | Path | None,
) -> Path | None:
    if bc_model_path is None:
        if runner == "bc":
            raise ValueError("bc runner requires bc_model_path")
        return None
    return Path(bc_model_path).expanduser().resolve()


def _build_running_manifest(
    *,
    workspace_root: Path,
    workspace_manifest: WorkspaceManifest,
    scenario: WorkspaceScenarioSpec,
    runner: BenchmarkRunner,
    seed: int,
    run_id: str,
    branch: str,
    model: str | None,
    provider: str | None,
    bc_model_path: Path | None,
    run_dir: Path,
    artifacts_dir: Path,
    state_dir: Path,
    events_path: Path,
    blueprint_asset_path: Path,
    contract_path: str,
    contract_source_path: Path,
) -> RunManifest:
    return RunManifest(
        run_id=run_id,
        workspace_name=workspace_manifest.name,
        scenario_name=scenario.name,
        runner=runner,
        status="running",
        started_at=_iso_now(),
        seed=seed,
        branch=branch,
        model=model,
        provider=provider,
        bc_model_path=str(bc_model_path) if bc_model_path is not None else None,
        workflow_name=scenario.workflow_name,
        workflow_variant=scenario.workflow_variant,
        artifacts=RunArtifactIndex(
            run_dir=str(run_dir.relative_to(workspace_root)),
            artifacts_dir=str(artifacts_dir.relative_to(workspace_root)),
            state_dir=str(state_dir.relative_to(workspace_root)),
            events_path=str(events_path.relative_to(workspace_root)),
            blueprint_asset_path=str(blueprint_asset_path.relative_to(workspace_root)),
            contract_path=contract_path,
        ),
        metadata=merge_reproducibility_metadata(
            {
                "workspace_scenario": scenario.name,
                "bc_model_path": str(bc_model_path) if bc_model_path else None,
            },
            seed=seed,
            blueprint_asset_path=blueprint_asset_path,
            contract_path=contract_source_path,
        ),
    )


def _build_benchmark_case_spec(
    *,
    workspace_manifest: WorkspaceManifest,
    scenario: WorkspaceScenarioSpec,
    runner: BenchmarkRunner,
    seed: int,
    branch: str,
    model: str | None,
    provider: str | None,
    bc_model_path: Path | None,
    task: str | None,
    max_steps: int,
    blueprint_asset_path: Path,
    artifacts_dir: Path,
) -> BenchmarkCaseSpec:
    return BenchmarkCaseSpec(
        runner=runner,
        scenario_name=scenario.scenario_name or scenario.name,
        family_name=scenario.workflow_name,
        workflow_name=scenario.workflow_name,
        workflow_variant=scenario.workflow_variant,
        blueprint_asset_path=blueprint_asset_path,
        seed=seed,
        artifacts_dir=artifacts_dir,
        branch=branch,
        model=model,
        provider=provider,
        bc_model_path=bc_model_path,
        task=task,
        max_steps=max_steps,
        metadata={
            "workspace_name": workspace_manifest.name,
            "workspace_scenario": scenario.name,
        },
    )


def _record_run_start(prepared: _PreparedRunLaunch) -> None:
    write_run_manifest(prepared.workspace_root, prepared.manifest_stub)
    append_run_event(
        prepared.events_path,
        RunTimelineEvent(
            index=0,
            kind="run_started",
            label=f"{prepared.runner} run started",
            channel="World",
            time_ms=0,
            runner=prepared.runner,
            status="running",
            branch=prepared.branch,
            payload={
                "workspace_name": prepared.workspace_manifest.name,
                "scenario_name": prepared.scenario.name,
                "seed": prepared.seed,
                "model": prepared.model,
                "provider": prepared.provider,
            },
        ),
    )
    upsert_workspace_run(
        prepared.workspace_root,
        WorkspaceRunEntry(
            run_id=prepared.run_id,
            scenario_name=prepared.scenario.name,
            runner=prepared.runner,
            status="running",
            manifest_path=str(
                (prepared.run_dir / "run_manifest.json").relative_to(
                    prepared.workspace_root
                )
            ),
            started_at=prepared.manifest_stub.started_at,
            branch=prepared.branch,
        ),
    )


def _finalize_successful_run(
    prepared: _PreparedRunLaunch,
    result: Any,
) -> RunManifest:
    contract_eval = evaluate_run_workspace_contract(
        prepared.workspace_root,
        run_id=prepared.run_id,
        scenario_name=prepared.scenario.name,
    )
    _append_artifact_events(
        prepared.workspace_root,
        prepared.run_id,
        runner=prepared.runner,
    )
    append_run_event(
        prepared.events_path,
        RunTimelineEvent(
            index=0,
            kind="run_completed",
            label=f"{prepared.runner} run completed",
            channel="World",
            time_ms=int(result.metrics.time_ms or result.metrics.elapsed_ms or 0),
            runner=prepared.runner,
            status="ok" if result.status == "ok" else "error",
            branch=result.diagnostics.branch or prepared.branch,
            payload={
                "success": result.success,
                "error": result.error,
                "metrics": _event_metrics_payload(result.metrics),
                "diagnostics": result.diagnostics.model_dump(mode="json"),
            },
        ),
    )
    timeline = build_run_timeline(prepared.workspace_root, prepared.run_id)
    write_run_timeline(prepared.workspace_root, prepared.run_id, timeline)
    snapshots = list_run_snapshots(prepared.workspace_root, prepared.run_id)

    final_manifest = RunManifest(
        run_id=prepared.run_id,
        workspace_name=prepared.workspace_manifest.name,
        scenario_name=prepared.scenario.name,
        runner=prepared.runner,
        status="ok" if result.status == "ok" else "error",
        started_at=prepared.manifest_stub.started_at,
        completed_at=_iso_now(),
        seed=prepared.seed,
        branch=result.diagnostics.branch or prepared.branch,
        model=prepared.model,
        provider=prepared.provider,
        bc_model_path=(
            str(prepared.bc_model_path) if prepared.bc_model_path is not None else None
        ),
        workflow_name=result.diagnostics.workflow_name
        or prepared.scenario.workflow_name,
        workflow_variant=(
            result.diagnostics.workflow_variant or prepared.scenario.workflow_variant
        ),
        success=result.success,
        metrics=result.metrics,
        diagnostics=result.diagnostics,
        contract=_contract_summary(contract_eval, prepared.run_dir),
        artifacts=_build_final_artifact_index(prepared),
        snapshots=snapshots,
        error=result.error,
        metadata=merge_reproducibility_metadata(
            {
                "workspace_scenario": prepared.scenario.name,
                "contract_ok": contract_eval.ok if contract_eval is not None else None,
                "bc_model_path": (
                    str(prepared.bc_model_path) if prepared.bc_model_path else None
                ),
            },
            seed=prepared.seed,
            blueprint_asset_path=prepared.blueprint_asset_path,
            contract_path=prepared.contract_source_path,
        ),
    )
    write_run_manifest(prepared.workspace_root, final_manifest)
    upsert_workspace_run(
        prepared.workspace_root,
        WorkspaceRunEntry(
            run_id=prepared.run_id,
            scenario_name=prepared.scenario.name,
            runner=prepared.runner,
            status=final_manifest.status,
            manifest_path=str(
                (prepared.run_dir / "run_manifest.json").relative_to(
                    prepared.workspace_root
                )
            ),
            started_at=prepared.manifest_stub.started_at,
            completed_at=final_manifest.completed_at,
            success=final_manifest.success,
            branch=final_manifest.branch,
        ),
    )
    return final_manifest


def _build_final_artifact_index(prepared: _PreparedRunLaunch) -> RunArtifactIndex:
    return RunArtifactIndex(
        run_dir=str(prepared.run_dir.relative_to(prepared.workspace_root)),
        artifacts_dir=str(prepared.artifacts_dir.relative_to(prepared.workspace_root)),
        state_dir=str(prepared.state_dir.relative_to(prepared.workspace_root)),
        events_path=str(prepared.events_path.relative_to(prepared.workspace_root)),
        blueprint_asset_path=str(
            prepared.blueprint_asset_path.relative_to(prepared.workspace_root)
        ),
        blueprint_path=_relative_if_exists(
            prepared.workspace_root,
            prepared.artifacts_dir / "blueprint.json",
        ),
        contract_path=prepared.contract_path,
        timeline_path=str(
            (prepared.run_dir / "timeline.json").relative_to(prepared.workspace_root)
        ),
        benchmark_result_path=_relative_if_exists(
            prepared.workspace_root,
            prepared.artifacts_dir / "benchmark_result.json",
        ),
        score_path=_relative_if_exists(
            prepared.workspace_root,
            prepared.artifacts_dir / "score.json",
        ),
        workflow_result_path=_relative_if_exists(
            prepared.workspace_root,
            prepared.artifacts_dir / "workflow_result.json",
        ),
        transcript_path=_relative_if_exists(
            prepared.workspace_root,
            prepared.artifacts_dir / "transcript.json",
        ),
        trace_path=_relative_if_exists(
            prepared.workspace_root,
            prepared.artifacts_dir / "trace.jsonl",
        ),
    )


def _finalize_failed_run(
    prepared: _PreparedRunLaunch,
    exc: Exception,
) -> None:
    _append_artifact_events(
        prepared.workspace_root,
        prepared.run_id,
        runner=prepared.runner,
    )
    append_run_event(
        prepared.events_path,
        RunTimelineEvent(
            index=0,
            kind="run_failed",
            label=f"{prepared.runner} run failed",
            channel="World",
            time_ms=0,
            runner=prepared.runner,
            status="error",
            branch=prepared.branch,
            payload={"error": str(exc)},
        ),
    )
    timeline = build_run_timeline(prepared.workspace_root, prepared.run_id)
    if timeline:
        write_run_timeline(prepared.workspace_root, prepared.run_id, timeline)

    error_manifest = prepared.manifest_stub.model_copy(
        update={
            "status": "error",
            "completed_at": _iso_now(),
            "error": str(exc),
            "metadata": merge_reproducibility_metadata(
                {
                    **dict(prepared.manifest_stub.metadata),
                    "error": str(exc),
                },
                seed=prepared.seed,
                blueprint_asset_path=prepared.blueprint_asset_path,
                contract_path=prepared.contract_source_path,
            ),
            "artifacts": prepared.manifest_stub.artifacts.model_copy(
                update={
                    "timeline_path": _relative_if_exists(
                        prepared.workspace_root,
                        prepared.run_dir / "timeline.json",
                    )
                }
            ),
        }
    )
    write_run_manifest(prepared.workspace_root, error_manifest)
    upsert_workspace_run(
        prepared.workspace_root,
        WorkspaceRunEntry(
            run_id=prepared.run_id,
            scenario_name=prepared.scenario.name,
            runner=prepared.runner,
            status="error",
            manifest_path=str(
                (prepared.run_dir / "run_manifest.json").relative_to(
                    prepared.workspace_root
                )
            ),
            started_at=prepared.manifest_stub.started_at,
            completed_at=error_manifest.completed_at,
            success=False,
            branch=prepared.branch,
            metadata={"error": str(exc)},
        ),
    )


def generate_run_id(*, prefix: str = "run") -> str:
    stamp = datetime.now(UTC).strftime("%Y%m%d_%H%M%S_%f")
    return f"{prefix}_{stamp}_{uuid4().hex[:6]}"


def _event_metrics_payload(metrics: Any) -> dict[str, Any]:
    payload = metrics.model_dump(mode="json")
    payload["elapsed_ms"] = int(payload.get("time_ms", 0) or 0)
    return payload


def list_run_manifests(root: str | Path) -> list[RunManifest]:
    workspace_root = Path(root).expanduser().resolve()
    manifests: list[RunManifest] = []
    for run_entry in list_workspace_runs(workspace_root):
        manifest_path = workspace_root / run_entry.manifest_path
        if manifest_path.exists():
            manifests.append(load_run_manifest(manifest_path))
    return manifests


def load_run_manifest(path: str | Path) -> RunManifest:
    payload = json.loads(Path(path).read_text(encoding="utf-8"))
    return RunManifest.model_validate(payload)


def write_run_manifest(root: str | Path, manifest: RunManifest) -> RunManifest:
    workspace_root = Path(root).expanduser().resolve()
    run_dir = workspace_root / manifest.artifacts.run_dir
    run_dir.mkdir(parents=True, exist_ok=True)
    manifest_path = run_dir / "run_manifest.json"
    temp_path = run_dir / f".run_manifest.{uuid4().hex}.tmp"
    temp_path.write_text(manifest.model_dump_json(indent=2), encoding="utf-8")
    temp_path.replace(manifest_path)
    return manifest


def get_workspace_runs_dir(root: str | Path) -> Path:
    workspace_root = Path(root).expanduser().resolve()
    manifest = load_workspace(workspace_root)
    return workspace_root / manifest.runs_dir


def get_workspace_run_dir(root: str | Path, run_id: str) -> Path:
    return get_workspace_runs_dir(root) / run_id


def get_workspace_run_manifest_path(root: str | Path, run_id: str) -> Path:
    return get_workspace_run_dir(root, run_id) / "run_manifest.json"


def load_run_timeline(path: str | Path) -> list[RunTimelineEvent]:
    payload = json.loads(Path(path).read_text(encoding="utf-8"))
    return [RunTimelineEvent.model_validate(item) for item in payload]


def write_run_timeline(
    root: str | Path, run_id: str, events: list[RunTimelineEvent]
) -> Path:
    workspace_root = Path(root).expanduser().resolve()
    manifest = load_run_manifest(
        get_workspace_run_manifest_path(workspace_root, run_id)
    )
    path = workspace_root / manifest.artifacts.run_dir / "timeline.json"
    path.write_text(
        json.dumps([event.model_dump(mode="json") for event in events], indent=2),
        encoding="utf-8",
    )
    return path


def build_run_timeline(root: str | Path, run_id: str) -> list[RunTimelineEvent]:
    workspace_root = Path(root).expanduser().resolve()
    events_path = get_workspace_run_dir(workspace_root, run_id) / "events.jsonl"
    if events_path.exists():
        events = load_run_events(events_path)
        for idx, item in enumerate(events, start=1):
            item.index = idx
        return events
    return _build_run_timeline_from_artifacts(workspace_root, run_id)


def _build_run_timeline_from_artifacts(
    workspace_root: Path, run_id: str
) -> list[RunTimelineEvent]:
    run_dir = get_workspace_run_dir(workspace_root, run_id)
    artifacts_dir = run_dir / "artifacts"
    events: list[RunTimelineEvent] = []
    index = 1

    trace_path = artifacts_dir / "trace.jsonl"
    if trace_path.exists():
        for record in load_trace(trace_path):
            record_type = str(record.get("type", "")).lower()
            time_ms = int(record.get("time_ms", 0))
            if record_type == "call":
                tool = str(record.get("tool", ""))
                events.append(
                    RunTimelineEvent(
                        index=index,
                        kind="trace_call",
                        label=f"{tool}",
                        channel=flow_channel_from_tool(tool),
                        time_ms=time_ms,
                        tool=tool,
                        object_refs=_infer_object_refs(tool, record.get("args", {})),
                        payload={
                            "args": record.get("args", {}),
                            "response": record.get("response", {}),
                        },
                    )
                )
                index += 1
            elif record_type == "event":
                target = str(record.get("target", "world"))
                events.append(
                    RunTimelineEvent(
                        index=index,
                        kind="trace_event",
                        label=f"{target} event",
                        channel=flow_channel_from_focus(target),
                        time_ms=time_ms,
                        object_refs=_infer_object_refs(
                            target, record.get("payload", {})
                        ),
                        payload={
                            "target": target,
                            "payload": record.get("payload", {}),
                            "emitted": record.get("emitted", {}),
                        },
                    )
                )
                index += 1

    workflow_result_path = artifacts_dir / "workflow_result.json"
    if workflow_result_path.exists():
        workflow_payload = json.loads(workflow_result_path.read_text(encoding="utf-8"))
        for step in workflow_payload.get("steps", []):
            tool = str(step.get("tool", "vei.graph_action"))
            resolved_tool = (
                str(step.get("resolved_tool")) if step.get("resolved_tool") else None
            )
            graph_domain = (
                str(step.get("graph_domain")) if step.get("graph_domain") else None
            )
            graph_action = (
                str(step.get("graph_action")) if step.get("graph_action") else None
            )
            graph_intent = (
                str(step.get("graph_intent"))
                if step.get("graph_intent")
                else (
                    f"{graph_domain}.{graph_action}"
                    if graph_domain and graph_action
                    else None
                )
            )
            channel = flow_channel_from_tool(resolved_tool or tool)
            object_refs = [
                str(item)
                for item in (
                    step.get("object_refs")
                    or _infer_object_refs(
                        resolved_tool or tool,
                        {
                            **dict(step.get("result", {}) or {}),
                            **dict(step.get("args", {}) or {}),
                        },
                    )
                )
            ]
            events.append(
                RunTimelineEvent(
                    index=index,
                    kind="workflow_step",
                    label=str(step.get("step_id", "workflow-step")),
                    channel=channel,
                    time_ms=int(step.get("time_ms", 0) or 0),
                    tool=tool,
                    resolved_tool=resolved_tool,
                    graph_action_ref=(
                        str(step.get("graph_action_ref"))
                        if step.get("graph_action_ref")
                        else None
                    ),
                    graph_domain=graph_domain,
                    graph_action=graph_action,
                    graph_intent=graph_intent,
                    object_refs=object_refs,
                    payload={
                        "args": step.get("args", {}),
                        "result": step.get("result", {}),
                        "observation": step.get("observation", {}),
                        "assertion_failures": step.get("assertion_failures", []),
                    },
                )
            )
            index += 1

    receipts_path = artifacts_dir / "connector_receipts.jsonl"
    if receipts_path.exists():
        for raw in receipts_path.read_text(encoding="utf-8").splitlines():
            if not raw.strip():
                continue
            receipt = json.loads(raw)
            events.append(
                RunTimelineEvent(
                    index=index,
                    kind="receipt",
                    label=f"{receipt.get('service', 'connector')}:{receipt.get('operation', 'receipt')}",
                    channel="World",
                    time_ms=int(receipt.get("time_ms", 0) or 0),
                    object_refs=_infer_object_refs(
                        f"{receipt.get('service', 'world')}.{receipt.get('operation', 'receipt')}",
                        receipt,
                    ),
                    payload=receipt,
                )
            )
            index += 1

    for snapshot in list_run_snapshots(workspace_root, run_id):
        events.append(
            RunTimelineEvent(
                index=index,
                kind="snapshot",
                label=snapshot.label or f"snapshot {snapshot.snapshot_id}",
                channel="World",
                time_ms=snapshot.time_ms,
                snapshot_id=snapshot.snapshot_id,
                branch=snapshot.branch,
                payload={"path": snapshot.path},
            )
        )
        index += 1

    contract_eval_path = run_dir / "workspace_contract_evaluation.json"
    if contract_eval_path.exists():
        contract_eval = json.loads(contract_eval_path.read_text(encoding="utf-8"))
        events.append(
            RunTimelineEvent(
                index=index,
                kind="contract",
                label=f"contract {'passed' if contract_eval.get('ok') else 'failed'}",
                channel="World",
                time_ms=int(contract_eval.get("metadata", {}).get("time_ms", 0) or 0),
                payload=contract_eval,
            )
        )

    events.sort(key=lambda item: (item.time_ms, item.index))
    for idx, item in enumerate(events, start=1):
        item.index = idx
    return events


def load_run_events_for_run(root: str | Path, run_id: str) -> list[RunTimelineEvent]:
    workspace_root = Path(root).expanduser().resolve()
    path = get_workspace_run_dir(workspace_root, run_id) / "events.jsonl"
    if not path.exists():
        return []
    return load_run_events(path)


def list_run_snapshots(root: str | Path, run_id: str) -> list[RunSnapshotRef]:
    workspace_root = Path(root).expanduser().resolve()
    run_dir = get_workspace_run_dir(workspace_root, run_id)
    manifest_path = get_workspace_run_manifest_path(workspace_root, run_id)
    branch = None
    if manifest_path.exists():
        branch = load_run_manifest(manifest_path).branch
    if branch is None:
        return []
    state_dir = run_dir / "state"
    store = StateStore(base_dir=state_dir, branch=branch)
    refs: list[RunSnapshotRef] = []
    for snapshot_path in store.list_snapshot_paths():
        payload = json.loads(snapshot_path.read_text(encoding="utf-8"))
        refs.append(
            RunSnapshotRef(
                snapshot_id=int(payload.get("index", 0)),
                branch=str(payload.get("branch", branch)),
                label=(str(payload.get("label")) if payload.get("label") else None),
                time_ms=int(payload.get("clock_ms", 0) or 0),
                path=str(snapshot_path.relative_to(workspace_root)),
            )
        )
    return refs


def load_run_snapshot_payload(
    root: str | Path, run_id: str, snapshot_id: int
) -> Dict[str, Any]:
    workspace_root = Path(root).expanduser().resolve()
    for snapshot in list_run_snapshots(workspace_root, run_id):
        if snapshot.snapshot_id == snapshot_id:
            return json.loads(
                (workspace_root / snapshot.path).read_text(encoding="utf-8")
            )
    raise ValueError(f"snapshot not found: {snapshot_id}")


def build_facade_panel(
    facade_name: str,
    components: Dict[str, Dict[str, Any]],
    context: Dict[str, Any],
) -> Any:
    from ._surfaces import build_facade_panel as _build_facade_panel

    return _build_facade_panel(facade_name, components, context)


def build_slack_surface_panel(
    components: Dict[str, Dict[str, Any]],
    context: Dict[str, Any],
) -> Any:
    from ._surface_panels_business import build_slack_panel

    return build_slack_panel(components, context)


def build_mail_surface_panel(
    components: Dict[str, Dict[str, Any]],
    context: Dict[str, Any],
) -> Any:
    from ._surface_panels_business import build_mail_panel

    return build_mail_panel(components, context)


def build_ticket_surface_panel(
    components: Dict[str, Dict[str, Any]],
    context: Dict[str, Any],
) -> Any:
    from ._surface_panels_business import build_ticket_panel

    return build_ticket_panel(components, context)


def build_docs_surface_panel(
    components: Dict[str, Dict[str, Any]],
    context: Dict[str, Any],
) -> Any:
    from ._surface_panels_business import build_docs_panel

    return build_docs_panel(components, context)


def build_approval_surface_panel(
    components: Dict[str, Dict[str, Any]],
    context: Dict[str, Any],
) -> Any:
    from ._surface_panels_business import build_approval_panel

    return build_approval_panel(components, context)


def build_notes_surface_panel(
    components: Dict[str, Dict[str, Any]],
    context: Dict[str, Any],
) -> Any:
    from ._surface_panels_business import build_notes_panel

    return build_notes_panel(components, context)


def build_knowledge_surface_panel(
    components: Dict[str, Dict[str, Any]],
    context: Dict[str, Any],
) -> Any:
    from ._surface_panels_business import build_knowledge_panel

    return build_knowledge_panel(components, context)


def build_revenue_surface_panel(
    components: Dict[str, Dict[str, Any]],
    context: Dict[str, Any],
) -> Any:
    from ._surface_panels_vertical import build_revenue_panel

    return build_revenue_panel(components, context)


def build_property_surface_panel(
    components: Dict[str, Dict[str, Any]],
    context: Dict[str, Any],
) -> Any:
    from ._surface_panels_vertical import build_property_panel

    return build_property_panel(components, context)


def build_campaign_surface_panel(
    components: Dict[str, Dict[str, Any]],
    context: Dict[str, Any],
) -> Any:
    from ._surface_panels_vertical import build_campaign_panel

    return build_campaign_panel(components, context)


def build_inventory_surface_panel(
    components: Dict[str, Dict[str, Any]],
    context: Dict[str, Any],
) -> Any:
    from ._surface_panels_vertical import build_inventory_panel

    return build_inventory_panel(components, context)


def build_service_ops_surface_panel(
    components: Dict[str, Dict[str, Any]],
    context: Dict[str, Any],
) -> Any:
    from ._surface_panels_vertical import build_service_ops_panel

    return build_service_ops_panel(components, context)


def verify_run_replay(root: str | Path, run_id: str) -> Dict[str, Any]:
    workspace_root = Path(root).expanduser().resolve()
    manifest = load_run_manifest(
        get_workspace_run_manifest_path(workspace_root, run_id)
    )
    events_path = (
        workspace_root / manifest.artifacts.events_path
        if manifest.artifacts.events_path
        else get_workspace_run_dir(workspace_root, run_id) / "events.jsonl"
    )
    if not events_path.exists():
        raise ValueError(f"run is missing events.jsonl: {run_id}")

    try:
        run_events = load_run_events(events_path)
    except Exception as exc:
        raise ValueError(f"run events are invalid: {exc}") from exc

    snapshot_events = [
        event
        for event in run_events
        if event.kind == "snapshot" and event.snapshot_id is not None
    ]
    if not snapshot_events:
        raise ValueError(f"run has no snapshot events: {run_id}")

    snapshots = list_run_snapshots(workspace_root, run_id)
    if not snapshots:
        raise ValueError(f"run has no snapshots: {run_id}")
    blueprint_rel = manifest.artifacts.blueprint_asset_path
    if not blueprint_rel:
        raise ValueError(f"run manifest is missing blueprint asset path: {run_id}")

    from vei.blueprint import BlueprintAsset, create_world_session_from_blueprint
    from vei.world.api import restore_router_state, serialize_router_state
    from vei.world.models import WorldState

    latest = snapshots[-1]
    latest_event_snapshot_id = int(snapshot_events[-1].snapshot_id or 0)
    latest_event_snapshot = next(
        (item for item in snapshots if item.snapshot_id == latest_event_snapshot_id),
        None,
    )
    event_snapshot_match = latest_event_snapshot is not None
    latest_snapshot_match = latest.snapshot_id == latest_event_snapshot_id

    blueprint_path = workspace_root / blueprint_rel
    mismatch_keys: list[str] = []
    ok = False
    if latest_event_snapshot is not None:
        snapshot_payload = load_run_snapshot_payload(
            workspace_root,
            run_id,
            latest_event_snapshot.snapshot_id,
        )
        expected_state = WorldState.model_validate(snapshot_payload.get("data", {}))
        asset = BlueprintAsset.model_validate_json(
            blueprint_path.read_text(encoding="utf-8")
        )
        session = create_world_session_from_blueprint(
            asset,
            seed=manifest.seed,
            branch=manifest.branch or expected_state.branch,
        )
        restore_router_state(session.router, expected_state)
        actual_state = serialize_router_state(session.router)
        expected_json = expected_state.model_dump(mode="json")
        actual_json = actual_state.model_dump(mode="json")
        ok = actual_json == expected_json
        mismatch_keys = sorted(
            key
            for key in set(expected_json) | set(actual_json)
            if expected_json.get(key) != actual_json.get(key)
        )

    reproducibility = build_reproducibility_record(
        seed=manifest.seed,
        blueprint_asset_path=blueprint_path,
        contract_path=(
            workspace_root / manifest.artifacts.contract_path
            if manifest.artifacts.contract_path
            else None
        ),
    )
    recorded = manifest.metadata.get("reproducibility")
    reproducibility_match = (
        recorded == reproducibility if isinstance(recorded, dict) else None
    )
    return {
        "ok": (
            ok
            and (reproducibility_match in {True, None})
            and event_snapshot_match
            and latest_snapshot_match
        ),
        "run_id": run_id,
        "snapshot_id": latest_event_snapshot_id,
        "state_match": ok,
        "event_log_path": str(events_path.relative_to(workspace_root)),
        "event_count": len(run_events),
        "event_snapshot_match": event_snapshot_match,
        "latest_event_snapshot_id": latest_event_snapshot_id,
        "latest_disk_snapshot_id": latest.snapshot_id,
        "latest_snapshot_match": latest_snapshot_match,
        "reproducibility_match": reproducibility_match,
        "mismatch_keys": mismatch_keys,
        "recorded_reproducibility": recorded,
        "verified_reproducibility": reproducibility,
    }


def diff_run_snapshots(
    root: str | Path, run_id: str, snapshot_from: int, snapshot_to: int
) -> Dict[str, Any]:
    before = load_run_snapshot_payload(root, run_id, snapshot_from)
    after = load_run_snapshot_payload(root, run_id, snapshot_to)
    flat_before: Dict[str, Any] = {}
    flat_after: Dict[str, Any] = {}
    _flatten_json("", before.get("data", {}), flat_before)
    _flatten_json("", after.get("data", {}), flat_after)
    keys = sorted(set(flat_before) | set(flat_after))
    return {
        "from": snapshot_from,
        "to": snapshot_to,
        "added": {key: flat_after[key] for key in keys if key not in flat_before},
        "removed": {key: flat_before[key] for key in keys if key not in flat_after},
        "changed": {
            key: {"from": flat_before[key], "to": flat_after[key]}
            for key in keys
            if key in flat_before
            and key in flat_after
            and flat_before[key] != flat_after[key]
        },
    }


def diff_cross_run_snapshots(
    root: str | Path,
    run_id_a: str,
    snapshot_a: int,
    run_id_b: str,
    snapshot_b: int,
) -> Dict[str, Any]:
    before = load_run_snapshot_payload(root, run_id_a, snapshot_a)
    after = load_run_snapshot_payload(root, run_id_b, snapshot_b)
    flat_before: Dict[str, Any] = {}
    flat_after: Dict[str, Any] = {}
    _flatten_json("", before.get("data", {}), flat_before)
    _flatten_json("", after.get("data", {}), flat_after)
    _remove_cross_run_metadata(flat_before)
    _remove_cross_run_metadata(flat_after)
    keys = sorted(set(flat_before) | set(flat_after))
    return {
        "run_a": run_id_a,
        "snapshot_a": snapshot_a,
        "run_b": run_id_b,
        "snapshot_b": snapshot_b,
        "added": {key: flat_after[key] for key in keys if key not in flat_before},
        "removed": {key: flat_before[key] for key in keys if key not in flat_after},
        "changed": {
            key: {"from": flat_before[key], "to": flat_after[key]}
            for key in keys
            if key in flat_before
            and key in flat_after
            and flat_before[key] != flat_after[key]
        },
    }


def get_run_orientation(root: str | Path, run_id: str) -> Dict[str, Any]:
    state = _latest_run_state(root, run_id)
    return build_world_orientation(state).model_dump(mode="json")


def get_run_capability_graphs(root: str | Path, run_id: str) -> Dict[str, Any]:
    state = _latest_run_state(root, run_id)
    return build_runtime_capability_graphs(state).model_dump(mode="json")


def get_run_surface_state(root: str | Path, run_id: str) -> LivingSurfaceState:
    from ._surfaces import build_surface_state

    workspace_root = Path(root).expanduser().resolve()
    state = _latest_run_state(workspace_root, run_id)
    if state is None:
        raise ValueError(f"run has no snapshots: {run_id}")

    run_manifest = load_run_manifest(
        get_workspace_run_manifest_path(workspace_root, run_id)
    )
    snapshots = list_run_snapshots(workspace_root, run_id)
    return build_surface_state(
        workspace_root=workspace_root,
        run_id=run_id,
        state=state,
        run_manifest=run_manifest,
        snapshots=snapshots,
    )


def get_run_knowledge_state(root: str | Path, run_id: str) -> Dict[str, Any]:
    state = _latest_run_state(root, run_id)
    if state is None:
        return {}
    return dict(state.components.get("knowledge") or {})


def load_run_contract_evaluation(
    root: str | Path, run_id: str
) -> Dict[str, Any] | None:
    workspace_root = Path(root).expanduser().resolve()
    path = (
        get_workspace_run_dir(workspace_root, run_id)
        / "workspace_contract_evaluation.json"
    )
    if not path.exists():
        return None
    return json.loads(path.read_text(encoding="utf-8"))


def evaluate_run_workspace_contract(
    root: str | Path,
    *,
    run_id: str,
    scenario_name: Optional[str] = None,
) -> ContractEvaluationResult | None:
    workspace_root = Path(root).expanduser().resolve()
    run_dir = get_workspace_run_dir(workspace_root, run_id)
    if not run_dir.exists():
        return None
    evaluation_inputs = _extract_run_evaluation_inputs(workspace_root, run_id)
    if evaluation_inputs is None:
        return None
    result = evaluate_workspace_contract_against_state(
        root=workspace_root,
        scenario_name=scenario_name,
        **evaluation_inputs,
    )
    (run_dir / "workspace_contract_evaluation.json").write_text(
        result.model_dump_json(indent=2), encoding="utf-8"
    )
    return result


def _extract_run_evaluation_inputs(
    workspace_root: Path, run_id: str
) -> Dict[str, Any] | None:
    run_dir = get_workspace_run_dir(workspace_root, run_id)
    artifacts_dir = run_dir / "artifacts"
    state = _latest_run_state(workspace_root, run_id)
    if state is None:
        return None

    workflow_result_path = artifacts_dir / "workflow_result.json"
    if workflow_result_path.exists():
        payload = json.loads(workflow_result_path.read_text(encoding="utf-8"))
        steps = payload.get("steps", [])
        final_step = steps[-1] if steps else {}
        return {
            "oracle_state": state.model_dump(mode="json"),
            "visible_observation": final_step.get("observation", {}),
            "result": final_step.get("result", {}),
            "pending": {},
            "time_ms": int(payload.get("metadata", {}).get("time_ms", 0) or 0),
            "available_tools": None,
        }

    transcript_path = artifacts_dir / "transcript.json"
    last_observation: Dict[str, Any] = {}
    last_result: Any = {}
    if transcript_path.exists():
        transcript = json.loads(transcript_path.read_text(encoding="utf-8"))
        if isinstance(transcript, list):
            for item in transcript:
                if isinstance(item, dict) and isinstance(item.get("observation"), dict):
                    last_observation = item["observation"]
                if isinstance(item, dict) and isinstance(item.get("result"), dict):
                    last_result = item["result"]
    return {
        "oracle_state": state.model_dump(mode="json"),
        "visible_observation": last_observation,
        "result": last_result,
        "pending": {},
        "time_ms": int(state.clock_ms),
        "available_tools": None,
    }


def _latest_run_state(root: str | Path, run_id: str) -> WorldState | None:
    workspace_root = Path(root).expanduser().resolve()
    snapshots = list_run_snapshots(workspace_root, run_id)
    if not snapshots:
        return None
    latest = snapshots[-1]
    payload = json.loads((workspace_root / latest.path).read_text(encoding="utf-8"))
    return WorldState.model_validate(payload.get("data", {}))


def _append_artifact_events(root: Path, run_id: str, *, runner: str) -> None:
    run_dir = get_workspace_run_dir(root, run_id)
    events_path = run_dir / "events.jsonl"
    existing_keys: set[tuple[str, str, int, str | None]] = set()
    if events_path.exists():
        for event in load_run_events(events_path):
            existing_keys.add(
                (
                    event.kind,
                    event.label,
                    int(event.time_ms),
                    event.graph_action_ref or event.tool,
                )
            )
    pending_events = _build_run_timeline_from_artifacts(root, run_id)
    appendable: list[RunTimelineEvent] = []
    for event in pending_events:
        key = (
            event.kind,
            event.label,
            int(event.time_ms),
            event.graph_action_ref or event.tool,
        )
        if key in existing_keys:
            continue
        event.runner = runner
        appendable.append(event)
        existing_keys.add(key)
    if appendable:
        append_run_events(events_path, appendable)


def _infer_object_refs(tool: str, payload: Dict[str, Any]) -> list[str]:
    refs: set[str] = set()
    normalized_tool = tool.strip().lower()

    def _add(prefix: str, key: str) -> None:
        value = payload.get(key)
        if value not in (None, ""):
            refs.add(f"{prefix}:{value}")

    if "okta" in normalized_tool or "identity" in normalized_tool:
        _add("identity_user", "user_id")
        _add("identity_group", "group_id")
        _add("identity_application", "app_id")
    if "google_admin" in normalized_tool or "doc" in normalized_tool:
        if (
            "restrict_drive_share" in normalized_tool
            or "transfer_drive_ownership" in normalized_tool
        ):
            _add("drive_share", "doc_id")
        else:
            _add("document", "doc_id")
    if "jira" in normalized_tool or "ticket" in normalized_tool:
        _add("ticket", "issue_id")
        _add("ticket", "ticket_id")
        _add("service_request", "request_id")
        _add("incident", "incident_id")
    if "slack" in normalized_tool or "comm_graph" in normalized_tool:
        _add("comm_channel", "channel")
    if "crm" in normalized_tool:
        _add("crm_deal", "id")
        _add("crm_deal", "deal_id")
    if "hris" in normalized_tool:
        _add("hris_employee", "employee_id")
    return sorted(refs)


def _flatten_json(prefix: str, value: Any, out: Dict[str, Any]) -> None:
    if isinstance(value, dict):
        for key, item in value.items():
            next_prefix = f"{prefix}.{key}" if prefix else str(key)
            _flatten_json(next_prefix, item, out)
        return
    out[prefix] = value


def _remove_cross_run_metadata(payload: Dict[str, Any]) -> None:
    for key in list(payload):
        if key in {
            "branch",
            "clock_ms",
            "rng_state",
            "receipts",
            "trace_entries",
            "audit_state.state_head",
            "audit_state.state.tool_calls",
        }:
            payload.pop(key, None)
            continue
        if key.endswith(".clock_ms"):
            payload.pop(key, None)
            continue
        if key.startswith("audit_state.state.meta.") and key.endswith(
            (
                "branch",
                "snapshot_id",
                "trace_id",
                "timeline_cursor",
                "run_id",
            )
        ):
            payload.pop(key, None)
            continue
        if key.startswith("actors.") and key.endswith((".cursor", ".mode")):
            payload.pop(key, None)
            continue
        if key.startswith("connector_runtime."):
            payload.pop(key, None)
            continue
        if key.startswith("components.mirror."):
            payload.pop(key, None)


def _contract_summary(
    result: ContractEvaluationResult | None, run_dir: Path
) -> RunContractSummary:
    if result is None:
        return RunContractSummary()
    issue_count = len(result.static_validation.issues) + len(
        result.dynamic_validation.issues
    )
    return RunContractSummary(
        contract_name=result.contract_name,
        ok=result.ok,
        success_assertion_count=(
            result.success_predicate_count + result.forbidden_predicate_count
        ),
        success_assertions_passed=(
            result.success_predicates_passed
            + max(
                0, result.forbidden_predicate_count - result.forbidden_predicates_failed
            )
        ),
        success_assertions_failed=(
            result.success_predicates_failed + result.forbidden_predicates_failed
        ),
        issue_count=issue_count,
        evaluation_path=str((run_dir / "workspace_contract_evaluation.json").name),
    )


def _relative_if_exists(root: Path, path: Path) -> str | None:
    if not path.exists():
        return None
    return str(path.relative_to(root))


@contextmanager
def _temporary_env(name: str, value: str | None) -> Iterator[None]:
    import os

    previous = os.environ.get(name)
    if value is None:
        os.environ.pop(name, None)
    else:
        os.environ[name] = value
    try:
        yield
    finally:
        if previous is None:
            os.environ.pop(name, None)
        else:
            os.environ[name] = previous


def _iso_now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat()


def normalize_runner(runner: str) -> BenchmarkRunner:
    normalized = runner.strip().lower()
    if normalized not in {"workflow", "scripted", "bc", "llm"}:
        raise ValueError("runner must be workflow, scripted, bc, or llm")
    return normalized  # type: ignore[return-value]
