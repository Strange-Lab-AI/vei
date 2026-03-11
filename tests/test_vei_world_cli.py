from __future__ import annotations

import json
from pathlib import Path

import typer.testing

from vei.blueprint.api import (
    build_blueprint_asset_for_example,
    create_world_session_from_blueprint,
)
from vei.cli.vei_world import app as world_app
from vei.world.state import Event, StateStore


def _seed_state(base: Path) -> None:
    store = StateStore(base_dir=base, branch="main")

    def reducer(state: dict[str, object], event: Event) -> None:
        state["count"] = state.get("count", 0) + 1

    store.register_reducer("touch", reducer)
    store.append("touch", {"value": 1})
    store.take_snapshot()

    store.append("touch", {"value": 1})
    store.take_snapshot()


def test_vei_world_cli_list_and_show(tmp_path: Path) -> None:
    runner = typer.testing.CliRunner()
    base = tmp_path / "state"
    _seed_state(base)

    result = runner.invoke(world_app, ["list", "--state-dir", str(base)])
    assert result.exit_code == 0, result.output
    payload = json.loads(result.stdout)
    assert payload["branch"] == "main"
    assert len(payload["snapshots"]) == 2

    result_show = runner.invoke(
        world_app,
        [
            "show",
            "--state-dir",
            str(base),
            "--snapshot",
            "000000",
            "--include-state",
        ],
    )
    assert result_show.exit_code == 0, result_show.output
    show_payload = json.loads(result_show.stdout)
    assert show_payload["index"] == 0
    assert show_payload["data"]["count"] == 1


def test_vei_world_cli_diff(tmp_path: Path) -> None:
    runner = typer.testing.CliRunner()
    base = tmp_path / "state"
    _seed_state(base)

    result = runner.invoke(
        world_app,
        [
            "diff",
            "--state-dir",
            str(base),
            "--snapshot-from",
            "0",
            "--snapshot-to",
            "1",
        ],
    )
    assert result.exit_code == 0, result.output
    diff_payload = json.loads(result.stdout)
    assert diff_payload["diff"]["changed"]["count"]["from"] == 1
    assert diff_payload["diff"]["changed"]["count"]["to"] == 2


def test_vei_world_cli_graphs(tmp_path: Path, monkeypatch) -> None:
    runner = typer.testing.CliRunner()
    state_root = tmp_path / "state"
    monkeypatch.setenv("VEI_STATE_DIR", str(state_root))

    asset = build_blueprint_asset_for_example("acquired_user_cutover")
    session = create_world_session_from_blueprint(asset, seed=9)
    session.snapshot(label="identity-cutover")

    result = runner.invoke(
        world_app,
        [
            "graphs",
            "--state-dir",
            str(state_root),
            "--domain",
            "identity_graph",
        ],
    )
    assert result.exit_code == 0, result.output
    payload = json.loads(result.stdout)
    assert payload["domain"] == "identity_graph"
    assert payload["graph"]["policies"][0]["policy_id"] == "POL-WAVE2"
    assert len(payload["graph"]["users"]) == 2


def test_vei_world_cli_orient(tmp_path: Path, monkeypatch) -> None:
    runner = typer.testing.CliRunner()
    state_root = tmp_path / "state"
    monkeypatch.setenv("VEI_STATE_DIR", str(state_root))

    asset = build_blueprint_asset_for_example("acquired_user_cutover")
    session = create_world_session_from_blueprint(asset, seed=10)
    session.snapshot(label="orientation-demo")

    result = runner.invoke(
        world_app,
        ["orient", "--state-dir", str(state_root)],
    )
    assert result.exit_code == 0, result.output
    payload = json.loads(result.stdout)
    assert payload["organization_name"] == "MacroCompute"
    assert payload["active_policies"][0]["policy_id"] == "POL-WAVE2"
    assert "identity_graph" in payload["suggested_focuses"]
