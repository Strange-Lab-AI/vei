from __future__ import annotations

import json
from pathlib import Path

from typer.testing import CliRunner

from vei.cli import vei_exercise
from vei.cli.vei_exercise import app
from vei.pilot.exercise_models import (
    ExerciseCatalogItem,
    ExerciseComparisonRow,
    ExerciseCompatibilityEndpoint,
    ExerciseCompatibilitySurface,
    ExerciseManifest,
    ExerciseStatus,
)
from vei.pilot.models import (
    PilotManifest,
    PilotOutcomeSummary,
    PilotRuntime,
    PilotServiceRecord,
    PilotStatus,
)
from vei.twin.models import CompatibilitySurfaceSpec


def test_exercise_cli_commands_are_wired_into_root_app(
    tmp_path: Path,
    monkeypatch,
) -> None:
    runner = CliRunner()
    root = tmp_path / "exercise"

    monkeypatch.setattr(
        vei_exercise,
        "start_exercise",
        lambda *args, **kwargs: _sample_status(root),
    )
    monkeypatch.setattr(
        vei_exercise,
        "build_exercise_status",
        lambda *args, **kwargs: _sample_status(root),
    )
    monkeypatch.setattr(
        vei_exercise,
        "stop_exercise",
        lambda *args, **kwargs: _sample_status(root),
    )

    up_result = runner.invoke(app, ["up", "--root", str(root)])
    assert up_result.exit_code == 0, up_result.output
    up_payload = json.loads(up_result.output)
    assert up_payload["manifest"]["company_name"] == "Pinnacle Analytics"

    status_result = runner.invoke(app, ["status", "--root", str(root)])
    assert status_result.exit_code == 0, status_result.output
    status_payload = json.loads(status_result.output)
    assert status_payload["comparison"][0]["runner"] == "workflow"

    down_result = runner.invoke(app, ["down", "--root", str(root)])
    assert down_result.exit_code == 0, down_result.output


def _sample_status(root: Path) -> ExerciseStatus:
    pilot_manifest = PilotManifest(
        workspace_root=root,
        workspace_name="pilot",
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
            )
        ],
        recommended_first_exercise="Read Slack first.",
        sample_client_path="/tmp/pilot_client.py",
    )
    pilot = PilotStatus(
        manifest=pilot_manifest,
        runtime=PilotRuntime(
            workspace_root=root,
            services=[
                PilotServiceRecord(
                    name="gateway",
                    host="127.0.0.1",
                    port=3020,
                    url="http://127.0.0.1:3020",
                ),
                PilotServiceRecord(
                    name="studio",
                    host="127.0.0.1",
                    port=3011,
                    url="http://127.0.0.1:3011",
                ),
            ],
        ),
        outcome=PilotOutcomeSummary(status="running", summary="running"),
    )
    manifest = ExerciseManifest(
        workspace_root=root,
        workspace_name="exercise",
        company_name="Pinnacle Analytics",
        archetype="b2b_saas",
        crisis_name="Renewal save",
        scenario_variant="renewal_save",
        contract_variant="customer_safe_recovery",
        success_criteria=["Keep the renewal healthy."],
        supported_api_subset=[
            ExerciseCompatibilitySurface(
                surface="slack",
                title="Slack",
                base_path="/slack/api",
                endpoints=[
                    ExerciseCompatibilityEndpoint(
                        method="GET",
                        path="/conversations.list",
                        description="List channels",
                    )
                ],
            )
        ],
        catalog=[
            ExerciseCatalogItem(
                scenario_variant="renewal_save",
                crisis_name="Renewal save",
                summary="Renewal is under pressure.",
                contract_variant="customer_safe_recovery",
                objective_summary="Protect the customer relationship.",
                active=True,
            )
        ],
        recommended_first_move="Read Slack first.",
    )
    return ExerciseStatus(
        manifest=manifest,
        pilot=pilot,
        comparison=[
            ExerciseComparisonRow(
                runner="workflow",
                label="Workflow baseline",
                run_id="run_workflow",
                status="ok",
                summary="Healthy path.",
            )
        ],
    )
