from __future__ import annotations

import json
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Dict, Optional
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
from vei.world.models import WorldState
from vei.world.state import StateStore
from vei.workspace.api import (
    compile_workspace,
    evaluate_workspace_contract_against_state,
    list_workspace_runs,
    resolve_workspace_scenario,
    upsert_workspace_run,
)
from vei.workspace.models import WorkspaceRunEntry

from .models import (
    RunArtifactIndex,
    RunContractSummary,
    RunManifest,
    RunSnapshotRef,
    RunTimelineEvent,
)


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
    task: Optional[str] = None,
    max_steps: int = 12,
) -> RunManifest:
    workspace_root = Path(root).expanduser().resolve()
    normalized_runner = normalize_runner(runner)
    summary = compile_workspace(workspace_root)
    manifest = summary.manifest
    scenario = resolve_workspace_scenario(workspace_root, manifest, scenario_name)

    resolved_run_id = run_id or generate_run_id()
    resolved_branch = branch or f"{manifest.name}.{resolved_run_id}"
    run_dir = workspace_root / manifest.runs_dir / resolved_run_id
    if run_dir.exists():
        raise ValueError(f"run_id already exists: {resolved_run_id}")
    artifacts_dir = run_dir / "artifacts"
    state_dir = run_dir / "state"
    artifacts_dir.mkdir(parents=True, exist_ok=True)
    state_dir.mkdir(parents=True, exist_ok=True)

    scenario_root = workspace_root / manifest.compiled_root / scenario.name
    blueprint_asset_path = scenario_root / "blueprint_asset.json"
    contract_path = (
        scenario.contract_path
        or f"{manifest.contracts_dir}/{scenario.name}.contract.json"
    )
    manifest_stub = RunManifest(
        run_id=resolved_run_id,
        workspace_name=manifest.name,
        scenario_name=scenario.name,
        runner=normalized_runner,
        status="running",
        started_at=_iso_now(),
        seed=seed,
        branch=resolved_branch,
        model=model,
        provider=provider,
        workflow_name=scenario.workflow_name,
        workflow_variant=scenario.workflow_variant,
        artifacts=RunArtifactIndex(
            run_dir=str(run_dir.relative_to(workspace_root)),
            artifacts_dir=str(artifacts_dir.relative_to(workspace_root)),
            state_dir=str(state_dir.relative_to(workspace_root)),
            blueprint_asset_path=str(blueprint_asset_path.relative_to(workspace_root)),
            contract_path=contract_path,
        ),
        metadata={"workspace_scenario": scenario.name},
    )
    write_run_manifest(workspace_root, manifest_stub)
    upsert_workspace_run(
        workspace_root,
        WorkspaceRunEntry(
            run_id=resolved_run_id,
            scenario_name=scenario.name,
            runner=normalized_runner,
            status="running",
            manifest_path=str(
                (run_dir / "run_manifest.json").relative_to(workspace_root)
            ),
            started_at=manifest_stub.started_at,
            branch=resolved_branch,
        ),
    )

    spec = BenchmarkCaseSpec(
        runner=normalized_runner,
        scenario_name=scenario.scenario_name or scenario.name,
        family_name=scenario.workflow_name,
        workflow_name=scenario.workflow_name,
        workflow_variant=scenario.workflow_variant,
        blueprint_asset_path=blueprint_asset_path,
        seed=seed,
        artifacts_dir=artifacts_dir,
        branch=resolved_branch,
        model=model,
        provider=provider,
        task=task,
        max_steps=max_steps,
        metadata={"workspace_name": manifest.name, "workspace_scenario": scenario.name},
    )

    try:
        with _temporary_env("VEI_STATE_DIR", str(state_dir)):
            result = run_benchmark_case(spec)

        contract_eval = evaluate_run_workspace_contract(
            workspace_root,
            run_id=resolved_run_id,
            scenario_name=scenario.name,
        )
        timeline = build_run_timeline(workspace_root, resolved_run_id)
        write_run_timeline(workspace_root, resolved_run_id, timeline)

        snapshots = list_run_snapshots(workspace_root, resolved_run_id)
        final_manifest = RunManifest(
            run_id=resolved_run_id,
            workspace_name=manifest.name,
            scenario_name=scenario.name,
            runner=normalized_runner,
            status=("ok" if result.status == "ok" else "error"),
            started_at=manifest_stub.started_at,
            completed_at=_iso_now(),
            seed=seed,
            branch=result.diagnostics.branch or resolved_branch,
            model=model,
            provider=provider,
            workflow_name=result.diagnostics.workflow_name or scenario.workflow_name,
            workflow_variant=result.diagnostics.workflow_variant
            or scenario.workflow_variant,
            success=result.success,
            metrics=result.metrics,
            diagnostics=result.diagnostics,
            contract=_contract_summary(contract_eval, run_dir),
            artifacts=RunArtifactIndex(
                run_dir=str(run_dir.relative_to(workspace_root)),
                artifacts_dir=str(artifacts_dir.relative_to(workspace_root)),
                state_dir=str(state_dir.relative_to(workspace_root)),
                blueprint_asset_path=str(
                    blueprint_asset_path.relative_to(workspace_root)
                ),
                blueprint_path=_relative_if_exists(
                    workspace_root, artifacts_dir / "blueprint.json"
                ),
                contract_path=contract_path,
                timeline_path=str(
                    (run_dir / "timeline.json").relative_to(workspace_root)
                ),
                benchmark_result_path=_relative_if_exists(
                    workspace_root, artifacts_dir / "benchmark_result.json"
                ),
                score_path=_relative_if_exists(
                    workspace_root, artifacts_dir / "score.json"
                ),
                workflow_result_path=_relative_if_exists(
                    workspace_root, artifacts_dir / "workflow_result.json"
                ),
                transcript_path=_relative_if_exists(
                    workspace_root, artifacts_dir / "transcript.json"
                ),
                trace_path=_relative_if_exists(
                    workspace_root, artifacts_dir / "trace.jsonl"
                ),
            ),
            snapshots=snapshots,
            error=result.error,
            metadata={
                "workspace_scenario": scenario.name,
                "contract_ok": contract_eval.ok if contract_eval is not None else None,
            },
        )
        write_run_manifest(workspace_root, final_manifest)
        upsert_workspace_run(
            workspace_root,
            WorkspaceRunEntry(
                run_id=resolved_run_id,
                scenario_name=scenario.name,
                runner=normalized_runner,
                status=final_manifest.status,
                manifest_path=str(
                    (run_dir / "run_manifest.json").relative_to(workspace_root)
                ),
                started_at=manifest_stub.started_at,
                completed_at=final_manifest.completed_at,
                success=final_manifest.success,
                branch=final_manifest.branch,
            ),
        )
        return final_manifest
    except Exception as exc:
        timeline = build_run_timeline(workspace_root, resolved_run_id)
        if timeline:
            write_run_timeline(workspace_root, resolved_run_id, timeline)
        error_manifest = manifest_stub.model_copy(
            update={
                "status": "error",
                "completed_at": _iso_now(),
                "error": str(exc),
                "artifacts": manifest_stub.artifacts.model_copy(
                    update={
                        "timeline_path": _relative_if_exists(
                            workspace_root, run_dir / "timeline.json"
                        )
                    }
                ),
            }
        )
        write_run_manifest(workspace_root, error_manifest)
        upsert_workspace_run(
            workspace_root,
            WorkspaceRunEntry(
                run_id=resolved_run_id,
                scenario_name=scenario.name,
                runner=normalized_runner,
                status="error",
                manifest_path=str(
                    (run_dir / "run_manifest.json").relative_to(workspace_root)
                ),
                started_at=manifest_stub.started_at,
                completed_at=error_manifest.completed_at,
                success=False,
                branch=resolved_branch,
                metadata={"error": str(exc)},
            ),
        )
        raise


def generate_run_id(*, prefix: str = "run") -> str:
    stamp = datetime.now(UTC).strftime("%Y%m%d_%H%M%S_%f")
    return f"{prefix}_{stamp}_{uuid4().hex[:6]}"


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
    (run_dir / "run_manifest.json").write_text(
        manifest.model_dump_json(indent=2), encoding="utf-8"
    )
    return manifest


def load_run_timeline(path: str | Path) -> list[RunTimelineEvent]:
    payload = json.loads(Path(path).read_text(encoding="utf-8"))
    return [RunTimelineEvent.model_validate(item) for item in payload]


def write_run_timeline(
    root: str | Path, run_id: str, events: list[RunTimelineEvent]
) -> Path:
    workspace_root = Path(root).expanduser().resolve()
    manifest = load_run_manifest(workspace_root / "runs" / run_id / "run_manifest.json")
    path = workspace_root / manifest.artifacts.run_dir / "timeline.json"
    path.write_text(
        json.dumps([event.model_dump(mode="json") for event in events], indent=2),
        encoding="utf-8",
    )
    return path


def build_run_timeline(root: str | Path, run_id: str) -> list[RunTimelineEvent]:
    workspace_root = Path(root).expanduser().resolve()
    run_dir = workspace_root / "runs" / run_id
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
            channel = flow_channel_from_tool(resolved_tool or tool)
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
                    object_refs=_infer_object_refs(
                        resolved_tool or tool,
                        {
                            **dict(step.get("result", {}) or {}),
                            **dict(step.get("args", {}) or {}),
                        },
                    ),
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


def list_run_snapshots(root: str | Path, run_id: str) -> list[RunSnapshotRef]:
    workspace_root = Path(root).expanduser().resolve()
    run_dir = workspace_root / "runs" / run_id
    manifest_path = run_dir / "run_manifest.json"
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


def get_run_orientation(root: str | Path, run_id: str) -> Dict[str, Any]:
    state = _latest_run_state(root, run_id)
    return build_world_orientation(state).model_dump(mode="json")


def get_run_capability_graphs(root: str | Path, run_id: str) -> Dict[str, Any]:
    state = _latest_run_state(root, run_id)
    return build_runtime_capability_graphs(state).model_dump(mode="json")


def load_run_contract_evaluation(
    root: str | Path, run_id: str
) -> Dict[str, Any] | None:
    workspace_root = Path(root).expanduser().resolve()
    path = workspace_root / "runs" / run_id / "workspace_contract_evaluation.json"
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
    run_dir = workspace_root / "runs" / run_id
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
    run_dir = workspace_root / "runs" / run_id
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
def _temporary_env(name: str, value: str | None):
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
