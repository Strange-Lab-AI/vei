from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from vei.scenario_engine.models import WorkflowStepSpec

from vei.benchmark import get_benchmark_family_workflow_spec
from vei.blueprint.api import create_world_session_from_blueprint
from vei.capability_graph.models import CapabilityGraphActionInput
from vei.contract.models import ContractEvaluationResult
from vei.fidelity import (
    build_workspace_fidelity_report,
    get_or_build_workspace_fidelity_report,
)
from vei.run.api import (
    build_run_timeline,
    generate_run_id,
    get_workspace_run_dir,
    get_workspace_run_manifest_path,
    list_run_manifests,
    list_run_snapshots,
    load_run_contract_evaluation,
    load_run_manifest,
    load_run_snapshot_payload,
    write_run_manifest,
)
from vei.run import append_run_event
from vei.run.models import (
    RunArtifactIndex,
    RunContractSummary,
    RunManifest,
    RunTimelineEvent,
)
from vei.verticals import (
    load_workspace_story_manifest,
    prepare_vertical_story,
    VerticalDemoSpec,
)
from vei.workspace.api import (
    activate_workspace_contract_variant,
    activate_workspace_scenario_variant,
    build_workspace_scenario_asset,
    evaluate_workspace_contract_against_state,
    load_workspace,
    load_workspace_blueprint_asset,
    preview_workspace_scenario,
    resolve_workspace_scenario,
    temporary_env,
    upsert_workspace_run,
)
from vei.workspace.models import WorkspaceRunEntry

from .models import (
    MissionCatalog,
    MissionExportBundle,
    MissionMoveState,
    MissionScorecard,
    MissionSessionState,
    PlayableShowcaseResult,
    PlayableShowcaseWorldResult,
    PlayerMoveResult,
    PlayableMissionMoveSpec,
    PlayableMissionSpec,
)


MISSION_STATE_FILE = "mission_state.json"
MISSION_EXPORT_FILE = "mission_exports.json"
PLAYABLE_BUNDLE_FILE = "playable_manifest.json"
PLAYABLE_OVERVIEW_FILE = "playable_overview.md"


def build_playable_mission_catalog() -> MissionCatalog:
    missions = _mission_specs()
    included_worlds: list[str] = []
    for mission in missions:
        if mission.vertical_name not in included_worlds:
            included_worlds.append(mission.vertical_name)
    hero_world = next(
        (mission.vertical_name for mission in missions if mission.hero),
        included_worlds[0] if included_worlds else "",
    )
    return MissionCatalog(
        hero_world=hero_world,
        included_worlds=included_worlds,
        missions=missions,
    )


def list_playable_missions(
    vertical_name: str | None = None,
) -> list[PlayableMissionSpec]:
    missions = build_playable_mission_catalog().missions
    if vertical_name is None:
        return missions
    key = vertical_name.strip().lower()
    return [item for item in missions if item.vertical_name == key]


def get_playable_mission(
    vertical_name: str,
    mission_name: str,
) -> PlayableMissionSpec:
    key = vertical_name.strip().lower()
    mission_key = mission_name.strip().lower()
    for mission in list_playable_missions(key):
        if mission.mission_name == mission_key:
            return mission
    raise KeyError(f"unknown playable mission: {vertical_name}/{mission_name}")


def list_workspace_playable_missions(root: str | Path) -> list[PlayableMissionSpec]:
    manifest = load_workspace(root)
    vertical_name = str(manifest.source_ref or "").strip().lower()
    if manifest.source_kind != "vertical" or not vertical_name:
        return []
    return list_playable_missions(vertical_name)


def activate_workspace_playable_mission(
    root: str | Path,
    mission_name: str,
    *,
    objective_variant: str | None = None,
) -> dict[str, Any]:
    workspace_root = Path(root).expanduser().resolve()
    manifest = load_workspace(workspace_root)
    if manifest.source_kind != "vertical" or not manifest.source_ref:
        raise ValueError("playable missions require a vertical workspace")
    mission = get_playable_mission(manifest.source_ref, mission_name)
    activate_workspace_scenario_variant(
        workspace_root,
        mission.scenario_variant,
        bootstrap_contract=True,
    )
    resolved_objective = objective_variant or mission.default_objective
    activate_workspace_contract_variant(workspace_root, resolved_objective)
    preview = preview_workspace_scenario(workspace_root)
    fidelity = build_workspace_fidelity_report(workspace_root)
    _write_playable_bundle_preview(
        workspace_root,
        world_name=manifest.title,
        mission=mission,
        objective_variant=resolved_objective,
        fidelity_status=fidelity.status,
    )
    return {
        "mission": mission.model_dump(mode="json"),
        "objective_variant": resolved_objective,
        "preview": preview,
    }


def prepare_playable_workspace(
    root: str | Path,
    *,
    world: str,
    mission: str | None = None,
    objective: str | None = None,
    compare_runner: str = "scripted",
    overwrite: bool = True,
    seed: int = 42042,
    max_steps: int = 18,
) -> MissionSessionState:
    workspace_root = Path(root).expanduser().resolve()
    available_missions = list_playable_missions(world)
    if not available_missions:
        raise ValueError(f"unknown playable world: {world}")
    selected_mission = mission or available_missions[0].mission_name
    selected_spec = get_playable_mission(world, selected_mission)
    resolved_objective = objective or selected_spec.default_objective
    story = prepare_vertical_story(
        VerticalDemoSpec(
            vertical_name=world,
            workspace_root=workspace_root,
            scenario_variant=selected_spec.scenario_variant,
            contract_variant=resolved_objective,
            compare_runner=compare_runner,  # type: ignore[arg-type]
            overwrite=overwrite,
            seed=seed,
            max_steps=max_steps,
        )
    )
    activate_workspace_playable_mission(
        workspace_root,
        selected_mission,
        objective_variant=resolved_objective,
    )
    state = start_workspace_mission_run(
        workspace_root,
        mission_name=selected_mission,
        objective_variant=resolved_objective,
        seed=seed,
        baseline_run_id=story.workflow_run_id,
        comparison_run_id=story.comparison_run_id,
    )
    fidelity = get_or_build_workspace_fidelity_report(workspace_root)
    state.metadata["fidelity_status"] = fidelity.status
    _write_playable_bundle(workspace_root, state)
    return state


def run_playable_showcase(
    root: str | Path,
    *,
    world_names: list[str] | None = None,
    mission_name: str | None = None,
    objective_variant: str | None = None,
    compare_runner: str = "scripted",
    run_id: str = "playable_showcase",
    overwrite: bool = True,
    seed: int = 42042,
    max_steps: int = 18,
) -> PlayableShowcaseResult:
    bundle_root = Path(root).expanduser().resolve() / run_id
    bundle_root.mkdir(parents=True, exist_ok=True)
    catalog = build_playable_mission_catalog()
    selected_worlds = world_names or list(catalog.included_worlds)
    worlds: list[PlayableShowcaseWorldResult] = []
    for world in selected_worlds:
        workspace_root = bundle_root / world
        state = prepare_playable_workspace(
            workspace_root,
            world=world,
            mission=mission_name if len(selected_worlds) == 1 else None,
            objective=objective_variant if len(selected_worlds) == 1 else None,
            compare_runner=compare_runner,
            overwrite=overwrite,
            seed=seed,
            max_steps=max_steps,
        )
        fidelity = get_or_build_workspace_fidelity_report(workspace_root)
        worlds.append(
            PlayableShowcaseWorldResult(
                vertical_name=world,
                world_name=state.world_name,
                workspace_root=workspace_root,
                mission_name=state.mission.mission_name,
                objective_variant=state.objective_variant,
                human_run_id=state.run_id,
                baseline_run_id=state.baseline_run_id,
                comparison_run_id=state.comparison_run_id,
                fidelity_status=fidelity.status,
                ui_command=(
                    "python -m vei.cli.vei ui serve "
                    f"--root {workspace_root} --host 127.0.0.1 --port 3011"
                ),
            )
        )
    result = PlayableShowcaseResult(
        run_id=run_id,
        root=bundle_root,
        hero_world=catalog.hero_world,
        worlds=worlds,
        result_path=bundle_root / "playable_showcase_result.json",
        overview_path=bundle_root / "playable_showcase_overview.md",
    )
    result.result_path.write_text(
        result.model_dump_json(indent=2),
        encoding="utf-8",
    )
    result.overview_path.write_text(
        _render_playable_showcase_overview(result),
        encoding="utf-8",
    )
    return result


def load_workspace_playable_bundle(root: str | Path) -> dict[str, Any] | None:
    workspace_root = Path(root).expanduser().resolve()
    path = workspace_root / PLAYABLE_BUNDLE_FILE
    if not path.exists():
        return None
    return json.loads(path.read_text(encoding="utf-8"))


def start_workspace_mission_run(
    root: str | Path,
    *,
    mission_name: str,
    objective_variant: str | None = None,
    run_id: str | None = None,
    seed: int = 42042,
    baseline_run_id: str | None = None,
    comparison_run_id: str | None = None,
) -> MissionSessionState:
    workspace_root = Path(root).expanduser().resolve()
    manifest = load_workspace(workspace_root)
    if manifest.source_kind != "vertical" or not manifest.source_ref:
        raise ValueError("playable missions require a vertical workspace")
    mission = get_playable_mission(manifest.source_ref, mission_name)
    objective_name = objective_variant or mission.default_objective
    activate_workspace_playable_mission(
        workspace_root,
        mission_name,
        objective_variant=objective_name,
    )
    run_id = run_id or generate_run_id(prefix="human_play")
    run_dir = get_workspace_run_dir(workspace_root, run_id)
    if run_dir.exists():
        raise ValueError(f"run_id already exists: {run_id}")
    artifacts_dir = run_dir / "artifacts"
    state_dir = run_dir / "state"
    artifacts_dir.mkdir(parents=True, exist_ok=True)
    state_dir.mkdir(parents=True, exist_ok=True)

    branch_name = f"{manifest.name}.{run_id}"
    session = _build_workspace_session(
        workspace_root,
        state_dir=state_dir,
        artifacts_dir=artifacts_dir,
        branch_name=branch_name,
        seed=seed,
    )
    snapshot = session.snapshot("mission.start")
    contract_eval = _evaluate_play_session(
        workspace_root, snapshot.data.model_dump(), {}
    )
    write_contract_evaluation(
        workspace_root,
        run_id,
        contract_eval,
    )
    state = MissionSessionState(
        run_id=run_id,
        workspace_root=workspace_root,
        world_name=manifest.title,
        mission=mission,
        objective_variant=objective_name,
        status="running",
        branch_name=branch_name,
        action_budget_remaining=mission.action_budget,
        last_snapshot_id=snapshot.snapshot_id,
        selected_run_id=run_id,
        baseline_run_id=baseline_run_id,
        comparison_run_id=comparison_run_id,
        scorecard=_build_scorecard(
            mission=mission,
            contract_eval=contract_eval,
            move_count=0,
            action_budget_remaining=mission.action_budget,
        ),
        metadata={"seed": seed},
    )
    state.available_moves = _build_move_states(
        workspace_root,
        mission=mission,
        executed_move_ids=[],
        turn_index=0,
        action_budget_remaining=mission.action_budget,
    )
    state.exports = build_mission_run_exports(workspace_root, state)
    _write_mission_state(workspace_root, run_id, state)
    _write_mission_exports(workspace_root, run_id, state.exports)
    _write_human_run_manifest(
        workspace_root,
        state,
        started_at=_iso_now(),
        status="running",
        success=None,
        contract_eval=contract_eval,
        error=None,
    )
    _upsert_human_run_index(workspace_root, state, started_at=_iso_now())
    append_run_event(
        run_dir / "events.jsonl",
        RunTimelineEvent(
            index=0,
            kind="run_started",
            label="human mission run started",
            channel="Plan",
            time_ms=snapshot.time_ms,
            runner="human",
            status="running",
            branch=branch_name,
            payload={
                "mission_name": mission.mission_name,
                "objective_variant": objective_name,
                "world_name": manifest.title,
            },
        ),
    )
    append_run_event(
        run_dir / "events.jsonl",
        RunTimelineEvent(
            index=0,
            kind="snapshot",
            label="mission.start",
            channel="World",
            time_ms=snapshot.time_ms,
            runner="human",
            branch=branch_name,
            snapshot_id=snapshot.snapshot_id,
            payload={
                "path": _snapshot_path(
                    workspace_root,
                    run_id,
                    branch_name,
                    snapshot.snapshot_id,
                )
            },
        ),
    )
    append_run_event(
        run_dir / "events.jsonl",
        RunTimelineEvent(
            index=0,
            kind="contract",
            label="mission score initialized",
            channel="World",
            time_ms=snapshot.time_ms,
            runner="human",
            status="running",
            branch=branch_name,
            payload=contract_eval.model_dump(mode="json"),
        ),
    )
    return state


def load_workspace_mission_state(
    root: str | Path,
    run_id: str | None = None,
) -> MissionSessionState | None:
    workspace_root = Path(root).expanduser().resolve()
    resolved_run_id = run_id or _latest_human_run_id(workspace_root)
    if resolved_run_id is None:
        return None
    path = get_workspace_run_dir(workspace_root, resolved_run_id) / MISSION_STATE_FILE
    if not path.exists():
        return None
    return MissionSessionState.model_validate_json(path.read_text(encoding="utf-8"))


def apply_workspace_mission_move(
    root: str | Path,
    *,
    run_id: str,
    move_id: str,
) -> MissionSessionState:
    workspace_root = Path(root).expanduser().resolve()
    state = load_workspace_mission_state(workspace_root, run_id)
    if state is None:
        raise ValueError(f"mission run not found: {run_id}")
    if state.status != "running":
        raise ValueError("mission run is not active")
    move_state = next(
        (item for item in state.available_moves if item.move_id == move_id), None
    )
    if move_state is None:
        raise ValueError(f"mission move not found: {move_id}")
    if move_state.availability == "blocked":
        raise ValueError(move_state.blocked_reason or "mission move is blocked")

    session = _restore_workspace_session(workspace_root, state)
    result = session.graph_action(move_state.graph_action)
    observation = session.observe(_focus_hint_for_domain(result.domain))
    snapshot = session.snapshot(f"move:{move_id}")
    contract_eval = _evaluate_play_session(
        workspace_root,
        snapshot.data.model_dump(),
        {
            "observation": observation,
            "result": result.result,
            "time_ms": snapshot.time_ms,
        },
    )
    write_contract_evaluation(workspace_root, run_id, contract_eval)

    executed = PlayerMoveResult(
        move_id=move_state.move_id,
        title=move_state.title,
        branch_label=_branch_label_for_move(state, move_state),
        summary=move_state.consequence_preview,
        graph_intent=str(result.metadata.get("graph_intent")),
        resolved_tool=result.tool,
        object_refs=list(result.metadata.get("affected_object_refs") or []),
        time_ms=snapshot.time_ms,
        payload={
            "result": result.result,
            "observation": observation,
            "availability": move_state.availability,
        },
    )
    state.executed_moves.append(executed)
    state.turn_index += 1
    state.action_budget_remaining = max(0, state.action_budget_remaining - 1)
    state.last_snapshot_id = snapshot.snapshot_id
    state.scorecard = _build_scorecard(
        mission=state.mission,
        contract_eval=contract_eval,
        move_count=len(state.executed_moves),
        action_budget_remaining=state.action_budget_remaining,
    )
    state.available_moves = _build_move_states(
        workspace_root,
        mission=state.mission,
        executed_move_ids=[item.move_id for item in state.executed_moves],
        turn_index=state.turn_index,
        action_budget_remaining=state.action_budget_remaining,
    )
    state.exports = build_mission_run_exports(workspace_root, state)
    _write_mission_state(workspace_root, run_id, state)
    _write_mission_exports(workspace_root, run_id, state.exports)
    _write_human_step_events(
        workspace_root, state, executed, snapshot.snapshot_id, contract_eval
    )

    if contract_eval.ok or state.action_budget_remaining == 0:
        return finish_workspace_mission_run(workspace_root, run_id=run_id)
    return state


def branch_workspace_mission_run(
    root: str | Path,
    *,
    run_id: str,
    branch_name: str | None = None,
) -> MissionSessionState:
    workspace_root = Path(root).expanduser().resolve()
    state = load_workspace_mission_state(workspace_root, run_id)
    if state is None:
        raise ValueError(f"mission run not found: {run_id}")
    if state.last_snapshot_id is None:
        raise ValueError("mission run has no snapshot to branch from")

    new_run_id = generate_run_id(prefix="human_branch")
    new_branch_name = branch_name or f"{state.branch_name}.branch"
    snapshot_payload = _load_snapshot_payload(
        workspace_root, run_id, state.last_snapshot_id
    )
    _seed_branch_snapshot(
        workspace_root,
        new_run_id=new_run_id,
        branch_name=new_branch_name,
        payload=snapshot_payload,
    )
    branched = MissionSessionState.model_validate(
        {
            **state.model_dump(mode="python"),
            "run_id": new_run_id,
            "branch_name": new_branch_name,
            "selected_run_id": new_run_id,
            "status": "running",
        }
    )
    _write_mission_state(workspace_root, new_run_id, branched)
    _write_mission_exports(workspace_root, new_run_id, branched.exports)
    contract_eval = _load_contract_eval_for_run(workspace_root, run_id)
    _write_human_run_manifest(
        workspace_root,
        branched,
        started_at=_iso_now(),
        status="running",
        success=None,
        contract_eval=contract_eval,
        error=None,
    )
    _upsert_human_run_index(workspace_root, branched, started_at=_iso_now())
    append_run_event(
        get_workspace_run_dir(workspace_root, new_run_id) / "events.jsonl",
        RunTimelineEvent(
            index=0,
            kind="run_started",
            label="human mission branch started",
            channel="Plan",
            time_ms=int(snapshot_payload.get("clock_ms", 0)),
            runner="human",
            status="running",
            branch=new_branch_name,
            payload={"source_run_id": run_id, "branch_name": new_branch_name},
        ),
    )
    return branched


def finish_workspace_mission_run(
    root: str | Path,
    *,
    run_id: str,
) -> MissionSessionState:
    workspace_root = Path(root).expanduser().resolve()
    state = load_workspace_mission_state(workspace_root, run_id)
    if state is None:
        raise ValueError(f"mission run not found: {run_id}")
    contract_eval = _load_contract_eval_for_run(workspace_root, run_id)
    state.status = "completed"
    state.exports = build_mission_run_exports(workspace_root, state)
    _write_mission_state(workspace_root, run_id, state)
    _write_mission_exports(workspace_root, run_id, state.exports)
    completed_at = _iso_now()
    _write_human_run_manifest(
        workspace_root,
        state,
        started_at=load_run_manifest(
            get_workspace_run_manifest_path(workspace_root, run_id)
        ).started_at,
        status="ok" if contract_eval.ok else "error",
        success=contract_eval.ok,
        contract_eval=contract_eval,
        error=None if contract_eval.ok else "mission contract failed",
        completed_at=completed_at,
    )
    _upsert_human_run_index(
        workspace_root,
        state,
        started_at=load_run_manifest(
            get_workspace_run_manifest_path(workspace_root, run_id)
        ).started_at,
        completed_at=completed_at,
        success=contract_eval.ok,
        status="ok" if contract_eval.ok else "error",
    )
    append_run_event(
        get_workspace_run_dir(workspace_root, run_id) / "events.jsonl",
        RunTimelineEvent(
            index=0,
            kind="run_completed",
            label="human mission run completed",
            channel="World",
            time_ms=int(contract_eval.metadata.get("time_ms", 0) or 0),
            runner="human",
            status="ok" if contract_eval.ok else "error",
            branch=state.branch_name,
            payload={"contract_ok": contract_eval.ok},
        ),
    )
    return state


def build_mission_run_exports(
    root: str | Path,
    state: MissionSessionState,
) -> list[MissionExportBundle]:
    workspace_root = Path(root).expanduser().resolve()
    contract_eval = _load_contract_eval_for_run(workspace_root, state.run_id)
    timeline = build_run_timeline(workspace_root, state.run_id)
    story = load_workspace_story_manifest(workspace_root)
    return [
        MissionExportBundle(
            name="rl",
            title="RL Episode Export",
            summary="This run already has state transitions, actions, rewards, and branch boundaries that can become trainable episodes later.",
            payload={
                "transition_count": max(1, len(state.executed_moves)),
                "reward_signal": state.scorecard.overall_score,
                "branch_boundaries": len(state.mission.branch_labels),
                "contract_ok": contract_eval.ok,
            },
        ),
        MissionExportBundle(
            name="eval",
            title="Continuous Eval Export",
            summary="The same mission can be compared against the workflow baseline and the freer agent path using the shared contract model.",
            payload={
                "mission": state.mission.mission_name,
                "objective": state.objective_variant,
                "baseline_run_id": (
                    state.baseline_run_id or story.workflow_run_id if story else None
                ),
                "comparison_run_id": (
                    state.comparison_run_id or story.comparison_run_id
                    if story
                    else None
                ),
                "human_contract_ok": contract_eval.ok,
            },
        ),
        MissionExportBundle(
            name="agent_ops",
            title="Agent Ops Export",
            summary="This bundle already contains the event spine, resolved tools, object references, and branch context needed for observability and review.",
            payload={
                "event_count": len(timeline),
                "resolved_tools": sorted(
                    {item.resolved_tool for item in timeline if item.resolved_tool}
                ),
                "object_refs": sorted(
                    {ref for move in state.executed_moves for ref in move.object_refs}
                ),
            },
        ),
    ]


def export_mission_run(
    root: str | Path,
    *,
    run_id: str,
    export_format: str,
) -> dict[str, Any]:
    state = load_workspace_mission_state(root, run_id)
    if state is None:
        raise ValueError(f"mission run not found: {run_id}")
    normalized = export_format.strip().lower().replace("-", "_")
    alias_map = {"agentops": "agent_ops", "agent_ops": "agent_ops"}
    normalized = alias_map.get(normalized, normalized)
    for item in build_mission_run_exports(root, state):
        if item.name == normalized:
            return item.model_dump(mode="json")
    raise ValueError("export format must be rl, eval, or agent-ops")


def write_contract_evaluation(
    root: str | Path,
    run_id: str,
    contract_eval: ContractEvaluationResult,
) -> Path:
    workspace_root = Path(root).expanduser().resolve()
    path = (
        get_workspace_run_dir(workspace_root, run_id)
        / "workspace_contract_evaluation.json"
    )
    path.write_text(contract_eval.model_dump_json(indent=2), encoding="utf-8")
    return path


def render_playable_overview(state: MissionSessionState) -> str:
    lines = [
        f"# VEI Playable World · {state.world_name}",
        "",
        f"- Mission: `{state.mission.title}`",
        f"- Objective: `{state.objective_variant}`",
        f"- Human run: `{state.run_id}`",
        f"- Workflow baseline: `{state.baseline_run_id or 'n/a'}`",
        f"- Comparison run: `{state.comparison_run_id or 'n/a'}`",
        f"- Score: `{state.scorecard.overall_score}`",
        f"- Mission success: `{state.scorecard.mission_success}`",
        "",
        state.mission.briefing,
        "",
        "## Why this works as a playable world",
        "",
        f"- {state.mission.why_it_matters}",
        "- The player uses the same graph-native action ladder as the automated runs.",
        "- Every move writes to the same run/event/snapshot model used elsewhere in VEI.",
        "- The result already looks like future RL, eval, and agent-ops data.",
        "",
        "## Branch labels",
        "",
    ]
    lines.extend(f"- {item}" for item in state.mission.branch_labels)
    lines.extend(
        [
            "",
            "## Next command",
            "",
            f"- `python -m vei.cli.vei ui serve --root {state.workspace_root} --host 127.0.0.1 --port 3011`",
        ]
    )
    return "\n".join(lines).rstrip() + "\n"


def _render_playable_preview_overview(
    *,
    world_name: str,
    mission: PlayableMissionSpec,
    objective_variant: str,
    root: Path,
) -> str:
    return (
        f"# VEI Playable World · {world_name}\n\n"
        f"- Mission: `{mission.title}`\n"
        f"- Objective: `{objective_variant}`\n\n"
        f"{mission.briefing}\n\n"
        "## Why this works as a playable world\n\n"
        f"- {mission.why_it_matters}\n"
        "- The player uses the same graph-native action ladder as the automated runs.\n"
        "- Every move writes to the same run/event/snapshot model used elsewhere in VEI.\n"
        "- The result already looks like future RL, eval, and agent-ops data.\n\n"
        "## Next command\n\n"
        f"- `python -m vei.cli.vei ui serve --root {root} --host 127.0.0.1 --port 3011`\n"
    )


def _build_playable_bundle_payload(
    *,
    root: Path,
    world_name: str,
    mission: PlayableMissionSpec,
    run_id: str | None,
    fidelity_status: str | None,
    objective_variant: str | None = None,
) -> dict[str, Any]:
    return {
        "world_name": world_name,
        "hero": mission.hero,
        "mission": mission.model_dump(mode="json"),
        "objective_variant": objective_variant,
        "run_id": run_id,
        "fidelity_status": fidelity_status,
        "ui_command": (
            "python -m vei.cli.vei ui serve "
            f"--root {root} --host 127.0.0.1 --port 3011"
        ),
    }


def _write_playable_bundle_preview(
    root: Path,
    *,
    world_name: str,
    mission: PlayableMissionSpec,
    objective_variant: str,
    fidelity_status: str | None,
) -> None:
    bundle = _build_playable_bundle_payload(
        root=root,
        world_name=world_name,
        mission=mission,
        run_id=None,
        fidelity_status=fidelity_status,
        objective_variant=objective_variant,
    )
    (root / PLAYABLE_BUNDLE_FILE).write_text(
        json.dumps(bundle, indent=2),
        encoding="utf-8",
    )
    (root / PLAYABLE_OVERVIEW_FILE).write_text(
        _render_playable_preview_overview(
            world_name=world_name,
            mission=mission,
            objective_variant=objective_variant,
            root=root,
        ),
        encoding="utf-8",
    )


def _write_playable_bundle(root: Path, state: MissionSessionState) -> None:
    bundle = _build_playable_bundle_payload(
        root=root,
        world_name=state.world_name,
        mission=state.mission,
        run_id=state.run_id,
        fidelity_status=state.metadata.get("fidelity_status"),
        objective_variant=state.objective_variant,
    )
    (root / PLAYABLE_BUNDLE_FILE).write_text(
        json.dumps(bundle, indent=2),
        encoding="utf-8",
    )
    (root / PLAYABLE_OVERVIEW_FILE).write_text(
        render_playable_overview(state),
        encoding="utf-8",
    )


def _build_workspace_session(
    workspace_root: Path,
    *,
    state_dir: Path,
    artifacts_dir: Path,
    branch_name: str,
    seed: int,
):
    scenario = resolve_workspace_scenario(workspace_root)
    asset = build_workspace_scenario_asset(
        load_workspace_blueprint_asset(workspace_root),
        scenario,
    )
    with temporary_env("VEI_STATE_DIR", str(state_dir)):
        return create_world_session_from_blueprint(
            asset,
            seed=seed,
            artifacts_dir=str(artifacts_dir),
            branch=branch_name,
        )


def _restore_workspace_session(
    workspace_root: Path,
    state: MissionSessionState,
):
    run_dir = get_workspace_run_dir(workspace_root, state.run_id)
    manifest = load_run_manifest(run_dir / "run_manifest.json")
    session = _build_workspace_session(
        workspace_root,
        state_dir=run_dir / "state",
        artifacts_dir=run_dir / "artifacts",
        branch_name=state.branch_name,
        seed=manifest.seed,
    )
    if state.last_snapshot_id is not None:
        session.restore(state.last_snapshot_id)
    return session


def _evaluate_play_session(
    workspace_root: Path,
    oracle_state: dict[str, Any],
    payload: dict[str, Any],
) -> ContractEvaluationResult:
    return evaluate_workspace_contract_against_state(
        root=workspace_root,
        oracle_state=oracle_state,
        visible_observation=dict(payload.get("observation") or {}),
        result=payload.get("result") or {},
        pending={},
        time_ms=int(payload.get("time_ms", 0) or 0),
        available_tools=None,
    )


def _build_scorecard(
    *,
    mission: PlayableMissionSpec,
    contract_eval: ContractEvaluationResult,
    move_count: int,
    action_budget_remaining: int,
) -> MissionScorecard:
    total_assertions = (
        contract_eval.success_predicate_count + contract_eval.forbidden_predicate_count
    )
    passed_assertions = contract_eval.success_predicates_passed + max(
        0,
        contract_eval.forbidden_predicate_count
        - contract_eval.forbidden_predicates_failed,
    )

    meta = contract_eval.metadata or {}
    category_weights: dict[str, float] = meta.get("category_weights") or {}
    predicate_categories: dict[str, str] = meta.get("predicate_categories") or {}
    failed_names: set[str] = set(meta.get("failed_predicate_names") or [])

    if category_weights and predicate_categories:
        weighted_earned = 0.0
        weighted_total = 0.0
        for pred_name, category in predicate_categories.items():
            w = category_weights.get(category, 1.0)
            weighted_total += w
            if pred_name not in failed_names:
                weighted_earned += w
        completion_ratio = weighted_earned / max(weighted_total, 1.0)
    else:
        completion_ratio = passed_assertions / max(total_assertions, 1)

    score = int(round(completion_ratio * 70))
    if contract_eval.ok:
        score += 20
    if contract_eval.policy_invariants_failed == 0:
        score += 10
    score = max(0, min(100, score))

    budget_ratio = action_budget_remaining / max(mission.action_budget, 1)
    if budget_ratio >= 0.5:
        deadline_pressure = "stable"
    elif budget_ratio >= 0.25:
        deadline_pressure = "compressed"
    else:
        deadline_pressure = "critical"

    issue_count = len(contract_eval.static_validation.issues) + len(
        contract_eval.dynamic_validation.issues
    )
    if issue_count == 0:
        business_risk = "low"
    elif issue_count <= 2:
        business_risk = "moderate"
    else:
        business_risk = "high"

    artifact_hygiene = "strong"
    if move_count == 0:
        artifact_hygiene = "untouched"
    elif completion_ratio < 0.7:
        artifact_hygiene = "partial"

    policy_correctness = (
        "sound" if contract_eval.policy_invariants_failed == 0 else "drifting"
    )
    summary = (
        "Mission solved cleanly."
        if contract_eval.ok
        else "Mission still has business risk or policy drift to resolve."
    )
    return MissionScorecard(
        overall_score=score,
        mission_success=contract_eval.ok,
        deadline_pressure=deadline_pressure,
        business_risk=business_risk,
        artifact_hygiene=artifact_hygiene,
        policy_correctness=policy_correctness,
        move_count=move_count,
        action_budget_remaining=action_budget_remaining,
        summary=summary,
        contract_issue_count=issue_count,
        success_assertions_passed=passed_assertions,
        success_assertions_total=total_assertions,
    )


def _build_move_states(
    workspace_root: Path,
    *,
    mission: PlayableMissionSpec,
    executed_move_ids: list[str],
    turn_index: int,
    action_budget_remaining: int,
) -> list[MissionMoveState]:
    workflow_moves = _workflow_move_specs(workspace_root, mission)
    all_moves = workflow_moves + list(mission.manual_moves)
    move_states: list[MissionMoveState] = []
    for move in all_moves:
        if move.move_id in executed_move_ids:
            move_states.append(
                MissionMoveState(
                    move_id=move.move_id,
                    title=move.title,
                    summary=move.summary,
                    availability="blocked",
                    consequence_preview=move.consequence_preview,
                    graph_action=move.graph_action,
                    blocked_reason="already used on this branch",
                    executed=True,
                    metadata=dict(move.metadata),
                )
            )
            continue
        if action_budget_remaining <= 0:
            move_states.append(
                MissionMoveState(
                    move_id=move.move_id,
                    title=move.title,
                    summary=move.summary,
                    availability="blocked",
                    consequence_preview=move.consequence_preview,
                    graph_action=move.graph_action,
                    blocked_reason="action budget exhausted",
                    metadata=dict(move.metadata),
                )
            )
            continue
        availability = "available"
        if move.tier == "risky":
            availability = "risky"
        elif move.step_index is not None and move.step_index <= turn_index + 1:
            availability = "recommended"
        move_states.append(
            MissionMoveState(
                move_id=move.move_id,
                title=move.title,
                summary=move.summary,
                availability=availability,  # type: ignore[arg-type]
                consequence_preview=move.consequence_preview,
                graph_action=move.graph_action,
                metadata=dict(move.metadata),
            )
        )
    return move_states


def _workflow_consequence(step: "WorkflowStepSpec") -> str:
    domain = _format_domain_title(step.graph_domain) if step.graph_domain else None
    action = (step.graph_action or "").replace("_", " ")
    target = step.args.get("name") or step.args.get("channel") or step.args.get("id")
    parts: list[str] = []
    if domain and action:
        parts.append(f"Fires {domain} -> {action}")
    elif step.tool:
        parts.append(f"Calls {step.tool}")
    if target:
        parts.append(f"on '{target}'")
    if not parts:
        return f"Advances the mission through {step.step_id}."
    return ". ".join([" ".join(parts), f"Advances the mission through {step.step_id}"])


def _format_domain_title(domain: str | None) -> str:
    titles = {
        "comm_graph": "Communications",
        "doc_graph": "Documents",
        "work_graph": "Workflows",
        "identity_graph": "Identity",
        "revenue_graph": "Revenue",
        "data_graph": "Data",
        "obs_graph": "Observability",
        "ops_graph": "Operations",
        "property_graph": "Property",
        "campaign_graph": "Campaign",
        "inventory_graph": "Inventory",
    }
    if not domain:
        return ""
    return titles.get(domain, domain.replace("_", " ").title())


def _workflow_move_specs(
    workspace_root: Path,
    mission: PlayableMissionSpec,
) -> list[PlayableMissionMoveSpec]:
    scenario = resolve_workspace_scenario(workspace_root)
    workflow = get_benchmark_family_workflow_spec(
        scenario.workflow_name or mission.vertical_name,
        variant_name=scenario.workflow_variant,
        parameter_overrides=scenario.workflow_parameters,
    )
    moves: list[PlayableMissionMoveSpec] = []
    for index, step in enumerate(workflow.steps):
        moves.append(
            PlayableMissionMoveSpec(
                move_id=f"step:{step.step_id}",
                title=step.description,
                summary=step.description,
                tier="recommended",
                graph_action=CapabilityGraphActionInput(
                    domain=step.graph_domain,
                    action=step.graph_action,
                    args=dict(step.args),
                ),
                consequence_preview=_workflow_consequence(step),
                step_index=index,
                step_id=step.step_id,
            )
        )
    return moves


def _write_mission_state(
    workspace_root: Path,
    run_id: str,
    state: MissionSessionState,
) -> Path:
    path = get_workspace_run_dir(workspace_root, run_id) / MISSION_STATE_FILE
    path.write_text(state.model_dump_json(indent=2), encoding="utf-8")
    return path


def _write_mission_exports(
    workspace_root: Path,
    run_id: str,
    exports: list[MissionExportBundle],
) -> Path:
    path = get_workspace_run_dir(workspace_root, run_id) / MISSION_EXPORT_FILE
    path.write_text(
        json.dumps([item.model_dump(mode="json") for item in exports], indent=2),
        encoding="utf-8",
    )
    return path


def _write_human_run_manifest(
    workspace_root: Path,
    state: MissionSessionState,
    *,
    started_at: str,
    status: str,
    success: bool | None,
    contract_eval: ContractEvaluationResult,
    error: str | None,
    completed_at: str | None = None,
) -> RunManifest:
    run_dir = get_workspace_run_dir(workspace_root, state.run_id)
    manifest = RunManifest(
        run_id=state.run_id,
        workspace_name=load_workspace(workspace_root).name,
        scenario_name=state.mission.mission_name,
        runner="human",
        status=status,  # type: ignore[arg-type]
        started_at=started_at,
        completed_at=completed_at,
        seed=int(state.metadata.get("seed", 42042)),
        branch=state.branch_name,
        success=success,
        contract=_contract_summary(contract_eval),
        artifacts=RunArtifactIndex(
            run_dir=str(run_dir.relative_to(workspace_root)),
            artifacts_dir=str((run_dir / "artifacts").relative_to(workspace_root)),
            state_dir=str((run_dir / "state").relative_to(workspace_root)),
            events_path=str((run_dir / "events.jsonl").relative_to(workspace_root)),
            contract_path=str(
                (run_dir / "workspace_contract_evaluation.json").relative_to(
                    workspace_root
                )
            ),
        ),
        snapshots=list_run_snapshots(workspace_root, state.run_id),
        error=error,
        metadata={
            "play_mode": "human",
            "mission_name": state.mission.mission_name,
            "objective_variant": state.objective_variant,
            "baseline_run_id": state.baseline_run_id,
            "comparison_run_id": state.comparison_run_id,
        },
    )
    return write_run_manifest(workspace_root, manifest)


def _upsert_human_run_index(
    workspace_root: Path,
    state: MissionSessionState,
    *,
    started_at: str,
    completed_at: str | None = None,
    success: bool | None = None,
    status: str = "running",
) -> None:
    upsert_workspace_run(
        workspace_root,
        WorkspaceRunEntry(
            run_id=state.run_id,
            scenario_name=state.mission.mission_name,
            runner="human",
            status=status,  # type: ignore[arg-type]
            manifest_path=str(
                (
                    get_workspace_run_manifest_path(workspace_root, state.run_id)
                ).relative_to(workspace_root)
            ),
            started_at=started_at,
            completed_at=completed_at,
            success=success,
            branch=state.branch_name,
            metadata={"play_mode": "human"},
        ),
    )


def _write_human_step_events(
    workspace_root: Path,
    state: MissionSessionState,
    move: PlayerMoveResult,
    snapshot_id: int,
    contract_eval: ContractEvaluationResult,
) -> None:
    run_dir = get_workspace_run_dir(workspace_root, state.run_id)
    append_run_event(
        run_dir / "events.jsonl",
        RunTimelineEvent(
            index=0,
            kind="workflow_step",
            label=move.title,
            channel=_channel_for_graph_intent(move.graph_intent),
            time_ms=move.time_ms,
            runner="human",
            resolved_tool=move.resolved_tool,
            graph_intent=move.graph_intent,
            graph_domain=move.graph_intent.split(".", 1)[0],
            graph_action=(
                move.graph_intent.split(".", 1)[1]
                if "." in move.graph_intent
                else move.graph_intent
            ),
            object_refs=move.object_refs,
            branch=state.branch_name,
            payload=move.payload,
        ),
    )
    append_run_event(
        run_dir / "events.jsonl",
        RunTimelineEvent(
            index=0,
            kind="snapshot",
            label=f"move:{move.move_id}",
            channel="World",
            time_ms=move.time_ms,
            runner="human",
            branch=state.branch_name,
            snapshot_id=snapshot_id,
            payload={
                "path": _snapshot_path(
                    workspace_root,
                    state.run_id,
                    state.branch_name,
                    snapshot_id,
                )
            },
        ),
    )
    append_run_event(
        run_dir / "events.jsonl",
        RunTimelineEvent(
            index=0,
            kind="contract",
            label="mission score updated",
            channel="World",
            time_ms=move.time_ms,
            runner="human",
            branch=state.branch_name,
            payload={
                "ok": contract_eval.ok,
                "issue_count": len(contract_eval.dynamic_validation.issues)
                + len(contract_eval.static_validation.issues),
                "score": state.scorecard.overall_score,
            },
        ),
    )


def _seed_branch_snapshot(
    workspace_root: Path,
    *,
    new_run_id: str,
    branch_name: str,
    payload: dict[str, Any],
) -> None:
    run_dir = get_workspace_run_dir(workspace_root, new_run_id)
    branch_dir = run_dir / "state" / branch_name
    state_dir = branch_dir / "snapshots"
    artifacts_dir = run_dir / "artifacts"
    state_dir.mkdir(parents=True, exist_ok=True)
    artifacts_dir.mkdir(parents=True, exist_ok=True)
    (branch_dir / "events.jsonl").touch()
    (branch_dir / "receipts.jsonl").touch()
    (branch_dir / "connector_receipts.jsonl").touch()
    updated = dict(payload)
    updated["branch"] = branch_name
    updated["data"] = {
        **dict(updated.get("data") or {}),
        "branch": branch_name,
    }
    snapshot_id = int(updated.get("index", 1) or 1)
    (state_dir / f"{snapshot_id:09d}.json").write_text(
        json.dumps(updated, indent=2),
        encoding="utf-8",
    )


def _load_snapshot_payload(
    workspace_root: Path,
    run_id: str,
    snapshot_id: int,
) -> dict[str, Any]:
    return load_run_snapshot_payload(workspace_root, run_id, snapshot_id)


def _load_contract_eval_for_run(
    workspace_root: Path,
    run_id: str,
) -> ContractEvaluationResult:
    payload = load_run_contract_evaluation(workspace_root, run_id)
    if payload is None:
        raise ValueError(f"contract evaluation missing for run: {run_id}")
    return ContractEvaluationResult.model_validate(payload)


def _latest_human_run_id(workspace_root: Path) -> str | None:
    for manifest in list_run_manifests(workspace_root):
        if manifest.runner == "human":
            return manifest.run_id
    return None


def _branch_label_for_move(
    state: MissionSessionState,
    move: MissionMoveState,
) -> str:
    if move.availability == "risky" and len(state.mission.branch_labels) > 1:
        return state.mission.branch_labels[1]
    if state.mission.branch_labels:
        return state.mission.branch_labels[0]
    return "primary path"


def _focus_hint_for_domain(domain: str) -> str:
    return {
        "comm_graph": "slack",
        "doc_graph": "docs",
        "work_graph": "tickets",
        "identity_graph": "identity",
        "revenue_graph": "crm",
        "property_graph": "property",
        "campaign_graph": "campaign",
        "inventory_graph": "inventory",
    }.get(domain, "summary")


def _channel_for_graph_intent(graph_intent: str) -> str:
    domain = graph_intent.split(".", 1)[0]
    return {
        "comm_graph": "Slack",
        "doc_graph": "Docs",
        "work_graph": "Tickets",
        "identity_graph": "World",
        "revenue_graph": "CRM",
        "property_graph": "World",
        "campaign_graph": "World",
        "inventory_graph": "World",
    }.get(domain, "World")


def _snapshot_path(
    workspace_root: Path,
    run_id: str,
    branch_name: str,
    snapshot_id: int,
) -> str:
    return str(
        (
            workspace_root
            / "runs"
            / run_id
            / "state"
            / branch_name
            / "snapshots"
            / f"{int(snapshot_id):09d}.json"
        ).relative_to(workspace_root)
    )


def _contract_summary(contract_eval: ContractEvaluationResult) -> RunContractSummary:
    issues = len(contract_eval.static_validation.issues) + len(
        contract_eval.dynamic_validation.issues
    )
    total = (
        contract_eval.success_predicate_count + contract_eval.forbidden_predicate_count
    )
    passed = contract_eval.success_predicates_passed + max(
        0,
        contract_eval.forbidden_predicate_count
        - contract_eval.forbidden_predicates_failed,
    )
    return RunContractSummary(
        contract_name=contract_eval.contract_name,
        ok=contract_eval.ok,
        success_assertion_count=total,
        success_assertions_passed=passed,
        success_assertions_failed=max(0, total - passed),
        issue_count=issues,
        evaluation_path="workspace_contract_evaluation.json",
    )


def _mission_specs() -> list[PlayableMissionSpec]:
    from ._catalog import mission_specs

    return mission_specs()


def _iso_now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat()


def _render_playable_showcase_overview(result: PlayableShowcaseResult) -> str:
    lines = [
        "# VEI Playable Showcase",
        "",
        "VEI now ships a playable mission catalog on top of the same world kernel,",
        "event spine, contracts, and playback system used everywhere else.",
        "",
        f"- Hero world: `{result.hero_world}`",
        f"- Included worlds: `{len(result.worlds)}`",
        "",
        "## Included worlds",
        "",
    ]
    for item in result.worlds:
        lines.extend(
            [
                f"### {item.world_name}",
                "",
                f"- Vertical: `{item.vertical_name}`",
                f"- Mission: `{item.mission_name}`",
                f"- Objective: `{item.objective_variant}`",
                f"- Human run: `{item.human_run_id}`",
                f"- Workflow baseline: `{item.baseline_run_id or 'n/a'}`",
                f"- Comparison run: `{item.comparison_run_id or 'n/a'}`",
                f"- Fidelity: `{item.fidelity_status}`",
                f"- UI: `{item.ui_command}`",
                "",
            ]
        )
    return "\n".join(lines).rstrip() + "\n"
