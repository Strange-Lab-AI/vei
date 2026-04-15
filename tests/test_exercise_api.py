from __future__ import annotations

from pathlib import Path

from vei.pilot import exercise as exercise_api
from vei.twin.models import (
    TwinLaunchManifest,
    TwinLaunchRuntime,
    TwinLaunchStatus,
    TwinOutcomeSummary,
    TwinServiceRecord,
)
from vei.twin.models import CompatibilitySurfaceSpec
from vei.workspace.api import create_workspace_from_template, preview_workspace_scenario


def test_default_contract_variant_returns_empty_when_no_variants(
    tmp_path: Path,
    monkeypatch,
) -> None:
    """When no contract variants exist, _default_contract_variant returns ''
    instead of raising ValueError."""
    monkeypatch.setattr(
        exercise_api,
        "list_workspace_contract_variants",
        lambda _root: [],
    )
    result = exercise_api._default_contract_variant(tmp_path)
    assert result == ""


def test_start_exercise_writes_manifest_and_comparison(
    tmp_path: Path,
    monkeypatch,
) -> None:
    root = tmp_path / "exercise_workspace"
    create_workspace_from_template(
        root=root,
        source_kind="vertical",
        source_ref="real_estate_management",
    )

    monkeypatch.setattr(exercise_api, "start_pilot", lambda *args, **kwargs: None)
    monkeypatch.setattr(
        exercise_api,
        "build_pilot_status",
        lambda *_args, **_kwargs: _sample_pilot_status(root),
    )

    status = exercise_api.start_exercise(root)

    assert (root / exercise_api.EXERCISE_MANIFEST_FILE).exists()
    assert status.manifest.company_name
    assert status.manifest.supported_api_subset[0].surface == "slack"
    comparison = {item.runner: item for item in status.comparison}
    assert comparison["workflow"].run_id
    assert comparison["scripted"].run_id
    assert comparison["external"].summary == "This path has not been run yet."


def test_activate_exercise_switches_variant_and_refreshes_manifest(
    tmp_path: Path,
    monkeypatch,
) -> None:
    root = tmp_path / "exercise_switch"
    create_workspace_from_template(
        root=root,
        source_kind="vertical",
        source_ref="real_estate_management",
    )

    monkeypatch.setattr(exercise_api, "start_pilot", lambda *args, **kwargs: None)
    monkeypatch.setattr(
        exercise_api, "reset_pilot_gateway", lambda *_args, **_kwargs: None
    )
    monkeypatch.setattr(
        exercise_api,
        "build_pilot_status",
        lambda *_args, **_kwargs: _sample_pilot_status(root),
    )

    exercise_api.start_exercise(root)
    status = exercise_api.activate_exercise(root, scenario_variant="vendor_no_show")

    preview = preview_workspace_scenario(root)
    assert preview["active_scenario_variant"] == "vendor_no_show"
    assert status.manifest.scenario_variant == "vendor_no_show"
    assert any(
        item.active
        for item in status.manifest.catalog
        if item.scenario_variant == "vendor_no_show"
    )


def _sample_pilot_status(root: Path) -> TwinLaunchStatus:
    manifest = TwinLaunchManifest(
        workspace_root=root,
        workspace_name="exercise",
        organization_name="Harbor Point Management",
        organization_domain="harborpoint.example.com",
        archetype="b2b_saas",
        crisis_name="Tenant Opening Conflict",
        studio_url="http://127.0.0.1:3011",
        control_room_url="http://127.0.0.1:3011/",
        gateway_url="http://127.0.0.1:3020",
        gateway_status_url="http://127.0.0.1:3020/api/twin",
        bearer_token="pilot-token",
        supported_surfaces=[
            CompatibilitySurfaceSpec(
                name="slack",
                title="Slack",
                base_path="/slack/api",
            )
        ],
        recommended_first_move="Read Slack and Jira before taking one action.",
        sample_client_path="/tmp/governor_client.py",
    )
    runtime = TwinLaunchRuntime(
        workspace_root=root,
        services=[
            TwinServiceRecord(
                name="gateway",
                host="127.0.0.1",
                port=3020,
                url="http://127.0.0.1:3020",
                state="running",
            ),
            TwinServiceRecord(
                name="studio",
                host="127.0.0.1",
                port=3011,
                url="http://127.0.0.1:3011",
                state="running",
            ),
        ],
        started_at="2026-03-25T18:00:00+00:00",
        updated_at="2026-03-25T18:05:00+00:00",
    )
    return TwinLaunchStatus(
        manifest=manifest,
        runtime=runtime,
        active_run="external_run",
        twin_status="running",
        request_count=2,
        services_ready=True,
        outcome=TwinOutcomeSummary(
            status="running",
            contract_ok=False,
            issue_count=2,
            summary="The company still needs another move.",
        ),
    )
