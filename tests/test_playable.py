from __future__ import annotations

import json
from pathlib import Path

from vei.fidelity import load_workspace_fidelity_report
from vei.run.api import get_run_surface_state
from vei.playable import (
    activate_workspace_playable_mission,
    apply_workspace_mission_move,
    branch_workspace_mission_run,
    build_playable_mission_catalog,
    finish_workspace_mission_run,
    get_playable_mission,
    list_playable_missions,
    load_workspace_mission_state,
    load_workspace_playable_bundle,
    prepare_playable_workspace,
    render_playable_overview,
    start_workspace_mission_run,
)
from vei.workspace.api import create_workspace_from_template


def _make_workspace(tmp_path: Path) -> Path:
    root = tmp_path / "playable"
    create_workspace_from_template(
        root=root,
        source_kind="vertical",
        source_ref="real_estate_management",
    )
    return root


def _set_runs_dir(root: Path, runs_dir: str) -> None:
    project_path = root / "vei_project.json"
    payload = json.loads(project_path.read_text(encoding="utf-8"))
    payload["runs_dir"] = runs_dir
    payload["runs_index_path"] = f"{runs_dir}/index.json"
    project_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def test_mission_catalog_contains_all_verticals() -> None:
    catalog = build_playable_mission_catalog()
    assert catalog.hero_world == "real_estate_management"
    assert "real_estate_management" in catalog.included_worlds
    assert "digital_marketing_agency" in catalog.included_worlds
    assert "storage_solutions" in catalog.included_worlds
    assert "service_ops" in catalog.included_worlds


def test_list_and_get_playable_missions() -> None:
    all_missions = list_playable_missions()
    assert len(all_missions) == 15

    re_missions = list_playable_missions("real_estate_management")
    assert len(re_missions) == 5
    names = [m.mission_name for m in re_missions]
    assert "tenant_opening_conflict" in names

    service_missions = list_playable_missions("service_ops")
    assert len(service_missions) == 3
    assert "service_day_collision" in [m.mission_name for m in service_missions]

    hero = get_playable_mission("real_estate_management", "tenant_opening_conflict")
    assert hero.hero is True
    assert hero.action_budget == 7


def test_list_missions_returns_empty_for_unknown_vertical() -> None:
    assert list_playable_missions("nonexistent_vertical") == []


def test_get_mission_raises_for_unknown() -> None:
    import pytest

    with pytest.raises(KeyError, match="unknown playable mission"):
        get_playable_mission("real_estate_management", "no_such_mission")


def test_start_load_and_finish_mission_run(tmp_path: Path) -> None:
    root = _make_workspace(tmp_path)
    state = start_workspace_mission_run(
        root,
        mission_name="tenant_opening_conflict",
    )
    assert state.status == "running"
    assert state.mission.mission_name == "tenant_opening_conflict"
    assert state.turn_index == 0
    assert state.available_moves

    loaded = load_workspace_mission_state(root, state.run_id)
    assert loaded is not None
    assert loaded.run_id == state.run_id
    assert loaded.status == "running"

    finished = finish_workspace_mission_run(root, run_id=state.run_id)
    assert finished.status == "completed"

    reloaded = load_workspace_mission_state(root, state.run_id)
    assert reloaded is not None
    assert reloaded.status == "completed"


def test_playable_mission_run_respects_custom_runs_dir(tmp_path: Path) -> None:
    root = _make_workspace(tmp_path)
    _set_runs_dir(root, "runtime_runs")

    state = start_workspace_mission_run(
        root,
        mission_name="tenant_opening_conflict",
    )
    loaded = load_workspace_mission_state(root, state.run_id)

    assert loaded is not None
    assert loaded.run_id == state.run_id
    assert (root / "runtime_runs" / state.run_id / "mission_state.json").exists()


def test_apply_move_advances_state(tmp_path: Path) -> None:
    root = _make_workspace(tmp_path)
    state = start_workspace_mission_run(
        root,
        mission_name="tenant_opening_conflict",
    )
    move_id = state.available_moves[0].move_id
    updated = apply_workspace_mission_move(root, run_id=state.run_id, move_id=move_id)
    assert updated.turn_index >= 1
    assert len(updated.executed_moves) == 1
    assert updated.scorecard.move_count == 1


def test_playable_mission_move_updates_living_company_surfaces(tmp_path: Path) -> None:
    root = _make_workspace(tmp_path)
    state = start_workspace_mission_run(
        root,
        mission_name="tenant_opening_conflict",
    )
    before = get_run_surface_state(root, state.run_id)

    updated = apply_workspace_mission_move(
        root,
        run_id=state.run_id,
        move_id=state.available_moves[0].move_id,
    )
    after = get_run_surface_state(root, updated.run_id)

    before_panels = {
        panel.surface: panel.model_dump(mode="json") for panel in before.panels
    }
    after_panels = {
        panel.surface: panel.model_dump(mode="json") for panel in after.panels
    }
    changed = [
        surface
        for surface, payload in before_panels.items()
        if payload != after_panels[surface]
    ]

    assert changed
    assert any(
        surface in {"approvals", "docs", "slack", "vertical_heartbeat"}
        for surface in changed
    )


def test_branch_creates_new_run(tmp_path: Path) -> None:
    root = _make_workspace(tmp_path)
    state = start_workspace_mission_run(
        root,
        mission_name="tenant_opening_conflict",
    )
    move_id = state.available_moves[0].move_id
    apply_workspace_mission_move(root, run_id=state.run_id, move_id=move_id)

    branched = branch_workspace_mission_run(root, run_id=state.run_id)
    assert branched.run_id != state.run_id
    assert branched.run_id.startswith("human_branch")
    assert branched.status == "running"

    original = load_workspace_mission_state(root, state.run_id)
    assert original is not None
    assert original.run_id == state.run_id


def test_finish_branched_run(tmp_path: Path) -> None:
    root = _make_workspace(tmp_path)
    state = start_workspace_mission_run(
        root,
        mission_name="tenant_opening_conflict",
    )
    move_id = state.available_moves[0].move_id
    apply_workspace_mission_move(root, run_id=state.run_id, move_id=move_id)

    branched = branch_workspace_mission_run(root, run_id=state.run_id)
    usable = [m for m in branched.available_moves if m.availability != "blocked"]
    assert usable, "branched run should have at least one usable move"
    apply_workspace_mission_move(
        root, run_id=branched.run_id, move_id=usable[0].move_id
    )
    finished = finish_workspace_mission_run(root, run_id=branched.run_id)
    assert finished.status == "completed"


def test_render_playable_overview(tmp_path: Path) -> None:
    root = _make_workspace(tmp_path)
    state = start_workspace_mission_run(
        root,
        mission_name="tenant_opening_conflict",
    )
    overview = render_playable_overview(state)
    assert "Tenant Opening Conflict" in overview
    assert state.run_id in overview


def test_load_mission_state_returns_none_for_missing_run(tmp_path: Path) -> None:
    root = _make_workspace(tmp_path)
    assert load_workspace_mission_state(root, "nonexistent_run") is None


def test_prepare_playable_workspace_creates_full_bundle(tmp_path: Path) -> None:
    root = tmp_path / "prepared"
    state = prepare_playable_workspace(
        root,
        world="real_estate_management",
        mission="tenant_opening_conflict",
    )
    assert state.status == "running"
    assert (root / "playable_manifest.json").exists()
    assert (root / "fidelity_report.json").exists()


def test_activate_playable_mission_refreshes_bundle_and_fidelity(
    tmp_path: Path,
) -> None:
    root = tmp_path / "prepared"
    prepare_playable_workspace(
        root,
        world="real_estate_management",
        mission="tenant_opening_conflict",
    )

    activate_workspace_playable_mission(
        root,
        "vendor_no_show",
        objective_variant="safety_over_speed",
    )

    bundle = load_workspace_playable_bundle(root)
    fidelity = load_workspace_fidelity_report(root)

    assert bundle is not None
    assert bundle["mission"]["mission_name"] == "vendor_no_show"
    assert bundle["objective_variant"] == "safety_over_speed"
    assert bundle["run_id"] is None
    assert fidelity is not None
    assert fidelity.metadata["active_scenario_variant"] == "vendor_no_show"
