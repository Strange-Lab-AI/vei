from __future__ import annotations

import json
from pathlib import Path

from typer.testing import CliRunner

from vei.cli.vei import app
from vei.verticals import (
    BusinessWorldDemoSpec,
    load_business_world_demo_bundle,
    prepare_business_world_demo,
)
from vei.whatif.models import (
    WhatIfEpisodeMaterialization,
    WhatIfEventReference,
    WhatIfExperimentArtifacts,
    WhatIfExperimentResult,
    WhatIfForecast,
    WhatIfForecastDelta,
    WhatIfForecastResult,
    WhatIfInterventionSpec,
    WhatIfLLMReplayResult,
    WhatIfReplaySummary,
    WhatIfResult,
    WhatIfScenario,
    WhatIfWorldSummary,
)


def test_prepare_business_world_demo_writes_bundle(tmp_path: Path) -> None:
    historical_root = _write_historical_fixture(tmp_path / "historical")
    rosetta_dir = tmp_path / "rosetta"
    rosetta_dir.mkdir()

    bundle = prepare_business_world_demo(
        BusinessWorldDemoSpec(
            root=tmp_path / "showcase",
            historical_result_root=historical_root,
            historical_rosetta_dir=rosetta_dir,
        )
    )

    assert bundle.manifest_path.exists()
    assert bundle.guide_path.exists()
    assert bundle.story.vertical_name == "service_ops"
    assert bundle.story.story_manifest_path.exists()
    assert bundle.historical_capstone is not None
    assert bundle.historical_capstone.metrics.external_send_delta == -29

    loaded = load_business_world_demo_bundle(bundle.root)
    assert loaded.run_id == "business_world_demo"
    assert loaded.sections[0].section_id == "live_world"

    guide = bundle.guide_path.read_text(encoding="utf-8")
    assert "VEI Business World Demo" in guide
    assert (
        "python -m vei.cli.vei quickstart run --world service_ops --governor-demo"
        in guide
    )
    assert "Historical Capstone" in guide


def test_showcase_business_world_command_generates_bundle(tmp_path: Path) -> None:
    runner = CliRunner()
    historical_root = _write_historical_fixture(tmp_path / "historical_cli")
    rosetta_dir = tmp_path / "rosetta_cli"
    rosetta_dir.mkdir()
    root = tmp_path / "showcase_cli"

    result = runner.invoke(
        app,
        [
            "showcase",
            "business-world",
            "--root",
            str(root),
            "--run-id",
            "demo_bundle",
            "--historical-root",
            str(historical_root),
            "--historical-rosetta-dir",
            str(rosetta_dir),
        ],
    )

    assert result.exit_code == 0, result.output
    payload = json.loads(result.output)
    assert payload["run_id"] == "demo_bundle"
    assert payload["story"]["vertical_name"] == "service_ops"
    assert payload["historical_capstone"]["metrics"]["external_send_delta"] == -29

    bundle_root = root / "demo_bundle"
    assert (bundle_root / "business_world_demo_manifest.json").exists()
    assert (bundle_root / "business_world_demo_guide.md").exists()
    assert (bundle_root / "service_ops_story" / "story_manifest.json").exists()
    assert (bundle_root / "service_ops_story" / "presentation_guide.md").exists()


def _write_historical_fixture(root: Path) -> Path:
    root.mkdir(parents=True, exist_ok=True)
    workspace_root = root / "workspace"
    workspace_root.mkdir()

    result_json = root / "whatif_experiment_result.json"
    overview_path = root / "whatif_experiment_overview.md"
    llm_json_path = root / "whatif_llm_result.json"
    forecast_json_path = root / "whatif_ejepa_result.json"

    branch_event = WhatIfEventReference(
        event_id="enron_evt_001",
        timestamp="2000-09-27T13:42:00Z",
        actor_id="debra.perlingiere@enron.com",
        target_id="kathy_gerken@cargill.com",
        event_type="assignment",
        thread_id="thr_master_agreement",
        subject="Master Agreement",
        snippet="Attached for your review is a draft Master Agreement.",
        to_recipients=["kathy_gerken@cargill.com"],
    )
    result = WhatIfExperimentResult(
        label="Master Agreement Internal Review",
        intervention=WhatIfInterventionSpec(
            label="Hold for review",
            prompt="Keep the draft internal and ask Gerald Nemec for review first.",
            thread_id="thr_master_agreement",
            branch_event_id=branch_event.event_id,
        ),
        selection=WhatIfResult(
            scenario=WhatIfScenario(
                scenario_id="external_dlp",
                title="External DLP",
                description="Hold external sends until review.",
            ),
            world_summary=WhatIfWorldSummary(source="enron"),
        ),
        materialization=WhatIfEpisodeMaterialization(
            manifest_path=root / "episode_manifest.json",
            bundle_path=root / "episode_bundle.json",
            context_snapshot_path=root / "context_snapshot.json",
            baseline_dataset_path=root / "baseline_dataset.jsonl",
            workspace_root=workspace_root,
            organization_name="Enron Corporation",
            organization_domain="enron.com",
            thread_id="thr_master_agreement",
            branch_event_id=branch_event.event_id,
            branch_event=branch_event,
        ),
        baseline=WhatIfReplaySummary(
            workspace_root=workspace_root,
            baseline_dataset_path=root / "baseline_dataset.jsonl",
            scheduled_event_count=84,
            delivered_event_count=84,
            forecast=WhatIfForecast(
                backend="historical",
                risk_score=1.0,
            ),
        ),
        llm_result=WhatIfLLMReplayResult(
            status="ok",
            provider="openai",
            model="gpt-5-mini",
            prompt="Keep the draft internal and ask Gerald Nemec for review first.",
            summary="Internal legal review path.",
            delivered_event_count=3,
            scheduled_event_count=3,
            inbox_count=5,
        ),
        forecast_result=WhatIfForecastResult(
            status="ok",
            backend="e_jepa_proxy",
            prompt="Keep the draft internal and ask Gerald Nemec for review first.",
            summary="Lower outside sharing risk.",
            baseline=WhatIfForecast(
                backend="historical",
                future_event_count=84,
                future_external_event_count=29,
                risk_score=1.0,
            ),
            predicted=WhatIfForecast(
                backend="e_jepa_proxy",
                future_event_count=84,
                future_external_event_count=0,
                risk_score=0.983,
            ),
            delta=WhatIfForecastDelta(
                risk_score_delta=-0.017,
                external_event_delta=-29,
            ),
        ),
        artifacts=WhatIfExperimentArtifacts(
            root=root,
            result_json_path=result_json,
            overview_markdown_path=overview_path,
            llm_json_path=llm_json_path,
            forecast_json_path=forecast_json_path,
        ),
    )

    result_json.write_text(result.model_dump_json(indent=2), encoding="utf-8")
    overview_path.write_text("# Master Agreement Internal Review\n", encoding="utf-8")
    llm_json_path.write_text("{}", encoding="utf-8")
    forecast_json_path.write_text("{}", encoding="utf-8")
    (root.parent / "run_summary.md").write_text("# What-If Summary\n", encoding="utf-8")
    return root
