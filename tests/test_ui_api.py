from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

from fastapi.testclient import TestClient

from vei.dataset.models import DatasetBuildSpec, DatasetBundle, DatasetSplitManifest
from vei.exercise.models import (
    ExerciseCatalogItem,
    ExerciseComparisonRow,
    ExerciseManifest,
    ExerciseStatus,
)
from vei.imports.api import get_import_package_example_path
from vei.pilot.models import (
    PilotManifest,
    PilotOutcomeSummary,
    PilotRuntime,
    PilotServiceRecord,
    PilotStatus,
)
from vei.run.api import launch_workspace_run
from vei.twin.models import CompatibilitySurfaceSpec
from vei.ui import api as ui_api
from vei.workspace.api import (
    create_workspace_from_template,
    generate_workspace_scenarios_from_import,
    import_workspace,
    sync_workspace_source,
)


class _ImmediateThread:
    def __init__(self, *, target=None, daemon=None):
        self._target = target

    def start(self) -> None:
        if self._target is not None:
            self._target()


def test_ui_api_serves_workspace_and_run_details(tmp_path: Path) -> None:
    root = tmp_path / "workspace"
    create_workspace_from_template(
        root=root,
        source_kind="example",
        source_ref="acquired_user_cutover",
    )
    manifest = launch_workspace_run(root, runner="workflow")

    client = TestClient(ui_api.create_ui_app(root))

    workspace_response = client.get("/api/workspace")
    assert workspace_response.status_code == 200
    assert workspace_response.json()["manifest"]["name"]

    runs_response = client.get("/api/runs")
    assert runs_response.status_code == 200
    assert runs_response.json()[0]["run_id"] == manifest.run_id

    timeline_response = client.get(f"/api/runs/{manifest.run_id}/timeline")
    assert timeline_response.status_code == 200
    assert any(item["kind"] == "workflow_step" for item in timeline_response.json())

    snapshots_response = client.get(f"/api/runs/{manifest.run_id}/snapshots")
    assert snapshots_response.status_code == 200
    snapshots = snapshots_response.json()
    assert len(snapshots) >= 2

    diff_response = client.get(
        f"/api/runs/{manifest.run_id}/diff",
        params={
            "snapshot_from": snapshots[0]["snapshot_id"],
            "snapshot_to": snapshots[-1]["snapshot_id"],
        },
    )
    assert diff_response.status_code == 200
    assert isinstance(diff_response.json()["changed"], dict)

    contract_response = client.get(f"/api/runs/{manifest.run_id}/contract")
    assert contract_response.status_code == 200
    assert contract_response.json()["ok"] is True

    receipts_response = client.get(f"/api/runs/{manifest.run_id}/receipts")
    assert receipts_response.status_code == 200
    assert isinstance(receipts_response.json(), list)

    orientation_response = client.get(f"/api/runs/{manifest.run_id}/orientation")
    assert orientation_response.status_code == 200
    assert orientation_response.json()["organization_name"] == "MacroCompute"

    timeline_path = root / "runs" / manifest.run_id / "timeline.json"
    timeline_path.unlink()
    with client.stream("GET", f"/api/runs/{manifest.run_id}/stream") as response:
        body = "".join(response.iter_text())
    assert response.status_code == 200
    assert "workflow_step" in body


def test_ui_api_start_run_returns_generated_run_id(tmp_path: Path, monkeypatch) -> None:
    root = tmp_path / "workspace"
    create_workspace_from_template(
        root=root,
        source_kind="example",
        source_ref="acquired_user_cutover",
    )

    monkeypatch.setattr(ui_api, "Thread", _ImmediateThread)
    client = TestClient(ui_api.create_ui_app(root))

    response = client.post("/api/runs", json={"runner": "workflow"})
    assert response.status_code == 200
    payload = response.json()
    assert payload["ok"] is True
    assert payload["run_id"].startswith("run_")

    run_response = client.get(f"/api/runs/{payload['run_id']}")
    assert run_response.status_code == 200
    assert run_response.json()["status"] == "ok"


def test_ui_api_serves_living_company_surfaces_for_vertical_runs(
    tmp_path: Path,
) -> None:
    for vertical_name in (
        "real_estate_management",
        "digital_marketing_agency",
        "storage_solutions",
        "service_ops",
    ):
        root = tmp_path / vertical_name
        create_workspace_from_template(
            root=root,
            source_kind="vertical",
            source_ref=vertical_name,
        )
        manifest = launch_workspace_run(root, runner="workflow")
        client = TestClient(ui_api.create_ui_app(root))

        response = client.get(f"/api/runs/{manifest.run_id}/surfaces")

        assert response.status_code == 200
        payload = response.json()
        assert payload["company_name"]
        assert payload["current_tension"]
        panel_map = {panel["surface"]: panel for panel in payload["panels"]}
        assert set(panel_map) == {
            "slack",
            "mail",
            "tickets",
            "docs",
            "approvals",
            "vertical_heartbeat",
        }
        assert panel_map["mail"]["items"]


def test_ui_api_serves_exercise_and_dataset_sidecar_payloads(
    tmp_path: Path,
    monkeypatch,
) -> None:
    root = tmp_path / "workspace"
    create_workspace_from_template(
        root=root,
        source_kind="vertical",
        source_ref="real_estate_management",
    )
    client = TestClient(ui_api.create_ui_app(root))

    monkeypatch.setattr(
        ui_api,
        "build_exercise_status",
        lambda *_args, **_kwargs: ExerciseStatus(
            manifest=ExerciseManifest(
                workspace_root=root,
                workspace_name="workspace",
                company_name="Harbor Point Management",
                archetype="real_estate_management",
                crisis_name="Tenant Opening Conflict",
                scenario_variant="tenant_opening_conflict",
                contract_variant="opening_readiness",
                success_criteria=["Protect the opening date."],
                catalog=[
                    ExerciseCatalogItem(
                        scenario_variant="tenant_opening_conflict",
                        crisis_name="Tenant Opening Conflict",
                        summary="Opening is blocked.",
                        contract_variant="opening_readiness",
                        objective_summary="Keep the opening valid.",
                        active=True,
                    )
                ],
            ),
            pilot=_sample_pilot_status(root),
            comparison=[
                ExerciseComparisonRow(
                    runner="workflow",
                    label="Workflow baseline",
                    run_id="run_workflow",
                    status="ok",
                    summary="healthy",
                )
            ],
        ),
    )
    monkeypatch.setattr(
        ui_api,
        "load_workspace_dataset_bundle",
        lambda *_args, **_kwargs: DatasetBundle(
            spec=DatasetBuildSpec(output_root=root / "dataset"),
            environment_count=1,
            run_count=3,
            splits=[
                DatasetSplitManifest(
                    split="train",
                    run_count=2,
                    example_count=10,
                    run_ids=["run_a", "run_b"],
                )
            ],
            reward_summary={"success_rate": 1.0},
            generated_at="2026-03-25T18:00:00+00:00",
        ),
    )

    exercise_response = client.get("/api/exercise")
    assert exercise_response.status_code == 200
    assert (
        exercise_response.json()["manifest"]["company_name"]
        == "Harbor Point Management"
    )

    dataset_response = client.get("/api/dataset")
    assert dataset_response.status_code == 200
    assert dataset_response.json()["run_count"] == 3


def test_ui_api_rejects_invalid_runner_before_worker_starts(tmp_path: Path) -> None:
    root = tmp_path / "workspace"
    create_workspace_from_template(
        root=root,
        source_kind="example",
        source_ref="acquired_user_cutover",
    )
    client = TestClient(ui_api.create_ui_app(root))

    response = client.post("/api/runs", json={"runner": "invalid-runner"})
    assert response.status_code == 400
    assert response.json()["detail"] == "runner must be workflow, scripted, bc, or llm"
    runs_response = client.get("/api/runs")
    assert runs_response.json() == []


def test_ui_api_rejects_bc_runner_without_model(tmp_path: Path) -> None:
    root = tmp_path / "workspace"
    create_workspace_from_template(
        root=root,
        source_kind="example",
        source_ref="acquired_user_cutover",
    )
    client = TestClient(ui_api.create_ui_app(root))

    response = client.post("/api/runs", json={"runner": "bc"})
    assert response.status_code == 400
    assert response.json()["detail"] == "bc runner requires bc_model"


def test_ui_api_serves_import_diagnostics_and_provenance(tmp_path: Path) -> None:
    root = tmp_path / "workspace"
    import_workspace(
        root=root,
        package_path=get_import_package_example_path("macrocompute_identity_export"),
    )
    generate_workspace_scenarios_from_import(root)
    manifest = launch_workspace_run(
        root,
        runner="workflow",
        scenario_name="oversharing_remediation",
    )

    client = TestClient(ui_api.create_ui_app(root))

    summary_response = client.get("/api/imports/summary")
    assert summary_response.status_code == 200
    assert summary_response.json()["package_name"] == "macrocompute_identity_export"

    identity_flow_response = client.get("/api/identity/flow")
    assert identity_flow_response.status_code == 200
    assert identity_flow_response.json()["active_scenario"] == "default"

    normalization_response = client.get("/api/imports/normalization")
    assert normalization_response.status_code == 200
    assert normalization_response.json()["normalized_counts"]["identity_users"] == 2
    assert (
        normalization_response.json()["identity_reconciliation"]["resolved_count"] >= 2
    )

    review_response = client.get("/api/imports/review")
    assert review_response.status_code == 200
    assert review_response.json()["package"]["name"] == "macrocompute_identity_export"
    assert (
        review_response.json()["normalization_report"]["identity_reconciliation"][
            "subject_count"
        ]
        >= 1
    )

    scenarios_response = client.get("/api/imports/scenarios")
    assert scenarios_response.status_code == 200
    assert any(
        item["name"] == "oversharing_remediation" for item in scenarios_response.json()
    )

    activate_response = client.post(
        "/api/scenarios/activate",
        json={"scenario_name": "oversharing_remediation", "bootstrap_contract": True},
    )
    assert activate_response.status_code == 200
    assert activate_response.json()["name"] == "oversharing_remediation"

    provenance_response = client.get(
        "/api/imports/provenance", params={"object_ref": "drive_share:GDRIVE-2201"}
    )
    assert provenance_response.status_code == 200
    assert provenance_response.json()[0]["origin"] == "imported"

    timeline_response = client.get(f"/api/runs/{manifest.run_id}/timeline")
    assert timeline_response.status_code == 200
    assert any(
        "drive_share:GDRIVE-2201" in item.get("object_refs", [])
        for item in timeline_response.json()
    )
    assert any(
        item.get("graph_intent") == "doc_graph.restrict_drive_share"
        for item in timeline_response.json()
        if item.get("kind") == "workflow_step"
    )


def test_ui_api_serves_event_alias_and_import_sources(
    tmp_path: Path, monkeypatch
) -> None:
    root = tmp_path / "workspace"
    create_workspace_from_template(
        root=root,
        source_kind="example",
        source_ref="acquired_user_cutover",
    )
    package_source = get_import_package_example_path("macrocompute_identity_export")
    config_path = tmp_path / "okta.json"
    config_path.write_text(
        '{"base_url":"https://macrocompute.okta.com","token":"test"}',
        encoding="utf-8",
    )

    def fake_sync(sync_root, config, *, source_prefix="okta_live"):
        package_root = Path(sync_root)
        import shutil
        from vei.imports.api import load_import_package

        shutil.copytree(package_source, package_root, dirs_exist_ok=True)
        package = load_import_package(package_root)
        for source in package.sources:
            source.source_kind = "connector_snapshot"
            source.connector_id = source_prefix
        (package_root / "package.json").write_text(
            package.model_dump_json(indent=2), encoding="utf-8"
        )
        return SimpleNamespace(
            connector="okta",
            package_root=package_root,
            package=package,
            record_counts={"users": 2, "groups": 2, "applications": 2},
            metadata={"source_prefix": source_prefix},
        )

    monkeypatch.setattr("vei.workspace.api.sync_okta_import_package", fake_sync)
    sync_workspace_source(
        root,
        connector="okta",
        config_path=config_path,
        source_id="macro_okta",
    )
    manifest = launch_workspace_run(root, runner="workflow")

    client = TestClient(ui_api.create_ui_app(root))

    events_response = client.get(f"/api/runs/{manifest.run_id}/events")
    assert events_response.status_code == 200
    assert events_response.json()[0]["kind"] == "run_started"


def test_ui_api_exposes_vertical_variant_browser(tmp_path: Path) -> None:
    root = tmp_path / "vertical-ui"
    create_workspace_from_template(
        root=root,
        source_kind="vertical",
        source_ref="real_estate_management",
    )
    client = TestClient(ui_api.create_ui_app(root))

    scenario_variants = client.get("/api/scenario-variants")
    contract_variants = client.get("/api/contract-variants")
    assert scenario_variants.status_code == 200
    assert contract_variants.status_code == 200
    assert len(scenario_variants.json()) == 4
    assert len(contract_variants.json()) == 3

    activate_scenario = client.post(
        "/api/scenarios/activate",
        json={"variant": "vendor_no_show", "bootstrap_contract": True},
    )
    assert activate_scenario.status_code == 200
    assert activate_scenario.json()["workflow_variant"] == "vendor_no_show"

    activate_contract = client.post(
        "/api/contract-variants/activate",
        json={"variant": "safety_over_speed"},
    )
    assert activate_contract.status_code == 200
    assert activate_contract.json()["metadata"]["vertical_contract_variant"] == (
        "safety_over_speed"
    )

    preview = client.get("/api/scenarios/default/preview")
    assert preview.status_code == 200
    assert preview.json()["active_scenario_variant"] == "vendor_no_show"
    assert preview.json()["active_contract_variant"] == "safety_over_speed"

    sources_response = client.get("/api/imports/sources")
    assert sources_response.status_code == 200
    payload = sources_response.json()
    assert payload["sources"] == []
    assert payload["syncs"] == []
    assert (
        preview.json()["compiled_blueprint"]["asset"]["capability_graphs"]["metadata"][
            "active_scenario_variant"
        ]
        == "vendor_no_show"
    )


def test_ui_api_exposes_story_bundle_and_export_preview(tmp_path: Path) -> None:
    root = tmp_path / "story-ui"
    create_workspace_from_template(
        root=root,
        source_kind="vertical",
        source_ref="digital_marketing_agency",
    )
    client = TestClient(ui_api.create_ui_app(root))

    story_response = client.get("/api/story")
    assert story_response.status_code == 200
    story_payload = story_response.json()
    assert story_payload["manifest"]["company_name"] == "Northstar Growth"
    assert story_payload["scenario_variant"] == "campaign_launch_guardrail"
    assert story_payload["contract_variant"] == "launch_safely"
    assert story_payload["presentation"]["beats"][0]["studio_view"] == "presentation"

    presentation_response = client.get("/api/presentation")
    assert presentation_response.status_code == 200
    presentation_payload = presentation_response.json()
    assert presentation_payload["opening_hook"]
    assert len(presentation_payload["primitives"]) == 6

    launch_workspace_run(root, runner="workflow")
    launch_workspace_run(root, runner="scripted")

    story_response = client.get("/api/story")
    assert story_response.status_code == 200
    story_payload = story_response.json()
    assert story_payload["outcome"]["baseline_branch"]
    assert story_payload["kernel_proof"]["baseline"]["events"] > 0

    exports_response = client.get("/api/exports-preview")
    assert exports_response.status_code == 200
    exports_payload = exports_response.json()
    assert [item["name"] for item in exports_payload] == [
        "rl_episode_export",
        "continuous_eval_export",
        "agent_ops_export",
    ]


def test_ui_api_exposes_playable_mission_mode(tmp_path: Path) -> None:
    root = tmp_path / "playable-ui"
    create_workspace_from_template(
        root=root,
        source_kind="vertical",
        source_ref="real_estate_management",
    )
    client = TestClient(ui_api.create_ui_app(root))

    missions_response = client.get("/api/missions")
    assert missions_response.status_code == 200
    missions_payload = missions_response.json()
    assert len(missions_payload) == 5
    assert missions_payload[0]["vertical_name"] == "real_estate_management"

    fidelity_response = client.get("/api/fidelity")
    assert fidelity_response.status_code == 200
    fidelity_payload = fidelity_response.json()
    assert fidelity_payload["company_name"] == "Harbor Point Management"
    assert len(fidelity_payload["cases"]) == 5

    start_response = client.post(
        "/api/missions/start",
        json={"mission_name": "tenant_opening_conflict"},
    )
    assert start_response.status_code == 200
    mission_state = start_response.json()
    assert mission_state["run_id"].startswith("human_play")
    assert mission_state["scorecard"]["move_count"] == 0
    assert mission_state["available_moves"]

    move_id = mission_state["available_moves"][0]["move_id"]
    move_response = client.post(
        f"/api/missions/{mission_state['run_id']}/moves/{move_id}"
    )
    assert move_response.status_code == 200
    moved_state = move_response.json()
    assert moved_state["turn_index"] >= 1
    assert len(moved_state["executed_moves"]) == 1

    exports_response = client.get(f"/api/missions/{mission_state['run_id']}/exports")
    assert exports_response.status_code == 200
    assert [item["name"] for item in exports_response.json()] == [
        "rl",
        "eval",
        "agent_ops",
    ]

    branch_response = client.post(
        f"/api/missions/{mission_state['run_id']}/branch", json={}
    )
    assert branch_response.status_code == 200
    branch_payload = branch_response.json()
    assert branch_payload["run_id"].startswith("human_branch")

    activate_response = client.post(
        "/api/missions/activate",
        json={
            "mission_name": "vendor_no_show",
            "objective_variant": "safety_over_speed",
        },
    )
    assert activate_response.status_code == 200

    playable_response = client.get("/api/playable")
    assert playable_response.status_code == 200
    assert playable_response.json()["mission"]["mission_name"] == "vendor_no_show"
    assert playable_response.json()["run_id"] is None

    ready_state_response = client.get("/api/missions/state")
    assert ready_state_response.status_code == 200
    assert ready_state_response.json() == {}


def test_ui_api_serves_pilot_console_and_controls(tmp_path: Path, monkeypatch) -> None:
    root = tmp_path / "pilot-ui"
    create_workspace_from_template(
        root=root,
        source_kind="vertical",
        source_ref="digital_marketing_agency",
    )
    payload = _sample_pilot_status(root)

    monkeypatch.setattr(ui_api, "build_pilot_status", lambda _: payload)
    monkeypatch.setattr(
        ui_api,
        "reset_pilot_gateway",
        lambda _: payload.model_copy(update={"request_count": 0}),
    )
    monkeypatch.setattr(
        ui_api,
        "finalize_pilot_run",
        lambda _: payload.model_copy(update={"twin_status": "completed"}),
    )

    client = TestClient(ui_api.create_ui_app(root))

    page_response = client.get("/pilot")
    assert page_response.status_code == 200
    assert "Operator Console" in page_response.text

    status_response = client.get("/api/pilot")
    assert status_response.status_code == 200
    assert (
        status_response.json()["manifest"]["organization_name"] == "Pinnacle Analytics"
    )

    reset_response = client.post("/api/pilot/reset")
    assert reset_response.status_code == 200
    assert reset_response.json()["request_count"] == 0

    finalize_response = client.post("/api/pilot/finalize")
    assert finalize_response.status_code == 200
    assert finalize_response.json()["twin_status"] == "completed"


def _sample_pilot_status(root: Path) -> PilotStatus:
    return PilotStatus(
        manifest=PilotManifest(
            workspace_root=root,
            workspace_name="pinnacle",
            organization_name="Pinnacle Analytics",
            organization_domain="pinnacle.example.com",
            archetype="b2b_saas",
            crisis_name="Renewal save",
            studio_url="http://127.0.0.1:3011",
            pilot_console_url="http://127.0.0.1:3011/pilot",
            gateway_url="http://127.0.0.1:3020",
            gateway_status_url="http://127.0.0.1:3020/api/twin",
            bearer_token="pilot-token",
            supported_surfaces=[
                CompatibilitySurfaceSpec(
                    name="slack",
                    title="Slack",
                    base_path="/slack/api",
                ),
                CompatibilitySurfaceSpec(
                    name="jira",
                    title="Jira",
                    base_path="/jira/rest/api/3",
                ),
            ],
            recommended_first_exercise="Read Slack and Jira, then send one customer-safe update.",
            sample_client_path="/tmp/pilot_client.py",
        ),
        runtime=PilotRuntime(
            workspace_root=root,
            services=[
                PilotServiceRecord(
                    name="gateway",
                    host="127.0.0.1",
                    port=3020,
                    url="http://127.0.0.1:3020",
                    pid=4101,
                    state="running",
                ),
                PilotServiceRecord(
                    name="studio",
                    host="127.0.0.1",
                    port=3011,
                    url="http://127.0.0.1:3011",
                    pid=4102,
                    state="running",
                ),
            ],
            started_at="2026-03-25T18:00:00+00:00",
            updated_at="2026-03-25T18:05:00+00:00",
        ),
        active_run="external_renewal_run",
        twin_status="running",
        request_count=4,
        services_ready=True,
        outcome=PilotOutcomeSummary(
            status="running",
            contract_ok=False,
            issue_count=2,
            summary="The renewal is still at risk and needs another action.",
            latest_tool="slack.send_message",
            current_tension="Customer trust is slipping.",
            affected_surfaces=["Email", "Slack"],
        ),
    )
