from __future__ import annotations

import inspect
from dataclasses import replace
from pathlib import Path

from fastapi.testclient import TestClient

from vei.blueprint import get_facade_plugin, register_facade_plugin
from vei.blueprint.api import (
    build_blueprint_asset_for_example,
    compile_blueprint,
    create_world_session_from_blueprint,
)
from vei.run._surfaces import build_surface_state
from vei.run.models import RunArtifactIndex, RunManifest
from vei.twin import _gateway_routes as gateway_routes
from vei.twin import create_twin_gateway_app
from vei.twin.api import TWIN_MANIFEST_FILE, _default_gateway_surfaces
from vei.twin.models import ContextMoldConfig, CustomerTwinBundle, TwinGatewayConfig
from vei.world.session import restore_router_state
from vei.workspace.api import (
    compile_workspace,
    create_workspace_from_template,
    load_workspace,
    load_workspace_blueprint_asset,
)


def _register_proxy_agent(
    client: TestClient,
    auth_headers: dict[str, str],
    *,
    agent_id: str = "notes-proxy",
) -> dict[str, str]:
    response = client.post(
        "/api/governor/agents",
        headers=auth_headers,
        json={
            "agent_id": agent_id,
            "name": "Notes Proxy",
            "mode": "proxy",
            "allowed_surfaces": [],
        },
    )
    assert response.status_code == 201
    return {
        **auth_headers,
        "x-vei-agent-id": agent_id,
        "x-vei-agent-name": response.json()["name"],
    }


def _assert_facade_round_trip() -> None:
    asset = build_blueprint_asset_for_example("acquired_user_cutover")
    asset.requested_facades = sorted(set(asset.requested_facades) | {"notes"})
    compiled = compile_blueprint(asset)
    assert any(facade.name == "notes" for facade in compiled.facades)

    session = create_world_session_from_blueprint(
        asset, seed=42042, branch="notes.main"
    )
    created = session.call_tool(
        "notes.create_entry",
        {
            "title": "Customer-safe plan",
            "body": "Capture the rollout note and the owner.",
            "tags": ["customer", "decision"],
        },
    )
    state = session.current_state()
    assert created["entry_id"] in state.components["notes"]["entries"]

    restored = create_world_session_from_blueprint(
        asset, seed=42042, branch="notes.main"
    )
    restore_router_state(restored.router, state)
    fetched = restored.call_tool("notes.get_entry", {"entry_id": created["entry_id"]})
    assert fetched["title"] == "Customer-safe plan"


def test_notes_facade_round_trip_and_studio_projection(tmp_path: Path) -> None:
    _assert_facade_round_trip()

    workspace_root = tmp_path / "notes_projection"
    create_workspace_from_template(
        root=workspace_root,
        source_kind="example",
        source_ref="acquired_user_cutover",
        overwrite=True,
    )
    asset = build_blueprint_asset_for_example("acquired_user_cutover")
    asset.requested_facades = sorted(set(asset.requested_facades) | {"notes"})
    session = create_world_session_from_blueprint(asset, seed=42042, branch="notes.ui")
    session.call_tool(
        "notes.create_entry",
        {"title": "Decision", "body": "Panel should render this note."},
    )
    manifest = RunManifest(
        run_id="notes_ui",
        workspace_name="notes_projection",
        scenario_name="acquired_user_cutover",
        runner="scripted",
        status="ok",
        started_at="2026-04-12T12:00:00+00:00",
        seed=42042,
        branch="notes.ui",
        artifacts=RunArtifactIndex(
            run_dir="runs/notes_ui",
            artifacts_dir="runs/notes_ui/artifacts",
            state_dir="runs/notes_ui/state",
        ),
        metadata={"reproducibility": {"record_id": "notes-ui-identity"}},
    )
    surface_state = build_surface_state(
        workspace_root=workspace_root,
        run_id="notes_ui",
        state=session.current_state(),
        run_manifest=manifest,
        snapshots=[],
    )
    panel = next(item for item in surface_state.panels if item.surface == "notes")
    assert panel.title == "Notes"
    assert panel.items[0].title == "Decision"
    assert surface_state.run_identity == "notes-ui-identity"


def test_notes_facade_gateway_routes_and_metadata(tmp_path: Path) -> None:
    root = tmp_path / "notes_gateway"
    create_workspace_from_template(
        root=root,
        source_kind="example",
        source_ref="acquired_user_cutover",
        overwrite=True,
    )
    manifest = load_workspace(root)
    asset = load_workspace_blueprint_asset(root)
    asset.requested_facades = sorted(set(asset.requested_facades) | {"notes"})
    (root / manifest.blueprint_asset_path).write_text(
        asset.model_dump_json(indent=2),
        encoding="utf-8",
    )
    compile_workspace(root)

    bundle = CustomerTwinBundle(
        workspace_root=root,
        workspace_name=manifest.name,
        organization_name=manifest.title or "Acme Cloud",
        organization_domain="acme.ai",
        mold=ContextMoldConfig(archetype="b2b_saas"),
        context_snapshot_path="context_snapshot.json",
        blueprint_asset_path=manifest.blueprint_asset_path,
        gateway=TwinGatewayConfig(
            auth_token="notes-token",
            surfaces=_default_gateway_surfaces(facades=asset.requested_facades),
        ),
        summary="Notes facade proof point.",
        metadata={},
    )
    (root / TWIN_MANIFEST_FILE).write_text(
        bundle.model_dump_json(indent=2),
        encoding="utf-8",
    )

    with TestClient(create_twin_gateway_app(root)) as client:
        auth_headers = {"Authorization": "Bearer notes-token"}
        proxy_headers = _register_proxy_agent(client, auth_headers)
        root_payload = client.get("/").json()
        assert any(item["name"] == "notes" for item in root_payload["surfaces"])

        created = client.post(
            "/notes/api/entries",
            headers=proxy_headers,
            json={
                "title": "Outside agent note",
                "body": "The gateway should create and return this entry.",
                "tags": ["gateway"],
            },
        )
        assert created.status_code == 201
        entry_id = created.json()["entry_id"]

        listed = client.get("/notes/api/entries", headers=proxy_headers)
        assert listed.status_code == 200
        assert any(item["entry_id"] == entry_id for item in listed.json()["items"])

        surfaces = client.get("/api/twin/surfaces").json()
        panel_map = {panel["surface"]: panel for panel in surfaces["panels"]}
        assert panel_map["notes"]["items"][0]["title"] == "Outside agent note"


def test_builtin_facades_boot_with_typed_runtime_binding() -> None:
    asset = build_blueprint_asset_for_example("acquired_user_cutover")
    session = create_world_session_from_blueprint(
        asset, seed=42042, branch="slack.main"
    )

    slack_binding = session.router.facade_plugins["slack"]
    assert slack_binding.plugin.manifest.name == "slack"
    assert slack_binding.component is session.router.slack
    assert slack_binding.plugin.studio_panel_builder is not None

    panel = slack_binding.plugin.studio_panel_builder(
        session.current_state().components,
        {
            "run_manifest": None,
            "state": session.current_state(),
            "vertical_name": "workspace",
            "vertical_runtime_family": "",
            "workspace": None,
        },
    )
    assert panel is not None
    assert panel.surface == "slack"


def test_builtin_restore_runs_through_plugin_hook() -> None:
    original_plugin = get_facade_plugin("slack")
    restored_payloads: list[dict[str, object]] = []

    def wrapped_restore(component: object, state: dict[str, object]) -> None:
        restored_payloads.append(state)
        assert original_plugin.state_restore is not None
        original_plugin.state_restore(component, state)

    register_facade_plugin(replace(original_plugin, state_restore=wrapped_restore))
    try:
        asset = build_blueprint_asset_for_example("acquired_user_cutover")
        source = create_world_session_from_blueprint(
            asset, seed=42042, branch="slack.src"
        )

        restored = create_world_session_from_blueprint(
            asset, seed=42042, branch="slack.dest"
        )
        restore_router_state(restored.router, source.current_state())
        assert len(restored_payloads) == 1
        assert restored_payloads[0]["channels"]
    finally:
        register_facade_plugin(original_plugin)


def test_gateway_core_registers_routes_without_named_surface_cases() -> None:
    source = inspect.getsource(gateway_routes.register_gateway_routes)
    assert "register_slack_gateway_routes" not in source
    assert "register_jira_gateway_routes" not in source
    assert "register_graph_gateway_routes" not in source
    assert "register_salesforce_gateway_routes" not in source
    assert "register_notes_gateway_routes" not in source
