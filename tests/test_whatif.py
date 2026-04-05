from __future__ import annotations

import json
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
from typer.testing import CliRunner

from vei.cli.vei import app as cli_app
from vei.data.models import VEIDataset
from vei.llm.providers import PlanResult, PlanUsage
from vei.twin import load_customer_twin
from vei.whatif import (
    load_experiment_result,
    load_episode_manifest,
    load_world,
    materialize_episode,
    replay_episode_baseline,
    search_events,
    run_counterfactual_experiment,
    run_ejepa_proxy_counterfactual,
    run_llm_counterfactual,
    run_whatif,
)
from vei.whatif.ejepa import _default_cache_root
from vei.whatif.models import (
    WhatIfEventReference,
    WhatIfForecast,
    WhatIfForecastDelta,
    WhatIfForecastResult,
)


def _write_rosetta_fixture(root: Path) -> None:
    root.mkdir(parents=True, exist_ok=True)
    metadata_rows = [
        {
            "event_id": "evt-001",
            "timestamp": "2001-05-01T10:00:00Z",
            "actor_id": "vince.kaminski@enron.com",
            "target_id": "sara.shackleton@enron.com",
            "event_type": "message",
            "thread_task_id": "thr-legal-trading",
            "artifacts": json.dumps(
                {
                    "subject": "Gas Position Limits",
                    "norm_subject": "gas position limits",
                    "to_recipients": ["sara.shackleton@enron.com"],
                    "to_count": 1,
                    "consult_legal_specialist": True,
                    "custodian_id": "kaminski-v",
                }
            ),
        },
        {
            "event_id": "evt-002",
            "timestamp": "2001-05-01T10:00:01Z",
            "actor_id": "sara.shackleton@enron.com",
            "target_id": "mark.taylor@enron.com",
            "event_type": "reply",
            "thread_task_id": "thr-legal-trading",
            "artifacts": json.dumps(
                {
                    "subject": "Gas Position Limits",
                    "norm_subject": "gas position limits",
                    "to_recipients": ["mark.taylor@enron.com"],
                    "to_count": 1,
                    "consult_trading_specialist": True,
                    "is_forward": True,
                    "custodian_id": "shackleton-s",
                }
            ),
        },
        {
            "event_id": "evt-003",
            "timestamp": "2001-05-01T10:00:02Z",
            "actor_id": "mark.taylor@enron.com",
            "target_id": "ops.review@enron.com",
            "event_type": "assignment",
            "thread_task_id": "thr-legal-trading",
            "artifacts": json.dumps(
                {
                    "subject": "Gas Position Limits",
                    "norm_subject": "gas position limits",
                    "to_recipients": ["ops.review@enron.com"],
                    "to_count": 1,
                    "custodian_id": "taylor-m",
                }
            ),
        },
        {
            "event_id": "evt-004",
            "timestamp": "2001-05-01T10:00:03Z",
            "actor_id": "assistant@enron.com",
            "target_id": "kenneth.lay@enron.com",
            "event_type": "escalation",
            "thread_task_id": "thr-exec",
            "artifacts": json.dumps(
                {
                    "subject": "Escalate to leadership",
                    "to_recipients": ["kenneth.lay@enron.com"],
                    "to_count": 1,
                    "is_escalation": True,
                }
            ),
        },
        {
            "event_id": "evt-005",
            "timestamp": "2001-05-01T10:00:04Z",
            "actor_id": "jeff.skilling@enron.com",
            "target_id": "outside@lawfirm.com",
            "event_type": "message",
            "thread_task_id": "thr-external",
            "artifacts": json.dumps(
                {
                    "subject": "Draft term sheet",
                    "to_recipients": ["outside@lawfirm.com"],
                    "to_count": 1,
                    "has_attachment_reference": True,
                }
            ),
        },
    ]
    content_rows = [
        {"event_id": "evt-001", "content": "Need legal eyes on this position update."},
        {"event_id": "evt-002", "content": "Forwarding with trading context attached."},
        {"event_id": "evt-003", "content": "Assigning ops review before we proceed."},
        {"event_id": "evt-004", "content": "Escalating to executive review."},
        {"event_id": "evt-005", "content": "External draft attached for review."},
    ]
    pq.write_table(
        pa.Table.from_pylist(metadata_rows),
        root / "enron_rosetta_events_metadata.parquet",
    )
    pq.write_table(
        pa.Table.from_pylist(content_rows),
        root / "enron_rosetta_events_content.parquet",
    )


def test_load_world_and_run_compliance_whatif(tmp_path: Path) -> None:
    rosetta_dir = tmp_path / "rosetta"
    _write_rosetta_fixture(rosetta_dir)

    world = load_world(source="enron", rosetta_dir=rosetta_dir)
    result = run_whatif(world, scenario="compliance_gateway")
    prompt_result = run_whatif(
        world,
        prompt="What if compliance reviewed every legal and trading thread?",
    )

    assert world.summary.event_count == 5
    assert world.summary.thread_count == 3
    assert result.affected_thread_count == 1
    assert result.blocked_forward_count == 1
    assert result.delayed_assignment_count == 1
    assert result.top_threads[0].thread_id == "thr-legal-trading"
    assert prompt_result.scenario.scenario_id == "compliance_gateway"


def test_materialize_episode_builds_mail_only_workspace_and_replay(
    tmp_path: Path,
) -> None:
    rosetta_dir = tmp_path / "rosetta"
    _write_rosetta_fixture(rosetta_dir)
    world = load_world(source="enron", rosetta_dir=rosetta_dir)

    workspace_root = tmp_path / "episode"
    materialization = materialize_episode(
        world,
        root=workspace_root,
        thread_id="thr-legal-trading",
    )
    manifest = load_episode_manifest(workspace_root)
    bundle = load_customer_twin(workspace_root)
    dataset = VEIDataset.model_validate_json(
        materialization.baseline_dataset_path.read_text(encoding="utf-8")
    )
    replay = replay_episode_baseline(workspace_root, tick_ms=1500)

    assert materialization.history_message_count == 1
    assert materialization.future_event_count == 2
    assert manifest.thread_id == "thr-legal-trading"
    assert manifest.branch_event_id == "evt-002"
    assert manifest.branch_event.actor_id == "sara.shackleton@enron.com"
    assert "Forwarding with trading context attached." in manifest.branch_event.snippet
    assert [surface.name for surface in bundle.gateway.surfaces] == ["graph"]
    assert bundle.organization_domain == "enron.com"
    assert len(dataset.events) == 2
    assert dataset.events[0].payload["thread_id"] == "thr-legal-trading"
    assert replay.scheduled_event_count == 2
    assert replay.delivered_event_count == 2
    assert replay.inbox_count >= 3


def test_vei_whatif_cli_explore_and_open_episode(tmp_path: Path) -> None:
    rosetta_dir = tmp_path / "rosetta"
    _write_rosetta_fixture(rosetta_dir)
    runner = CliRunner()

    explore_result = runner.invoke(
        cli_app,
        [
            "whatif",
            "explore",
            "--rosetta-dir",
            str(rosetta_dir),
            "--scenario",
            "external_dlp",
        ],
    )
    assert explore_result.exit_code == 0, explore_result.output
    explore_payload = json.loads(explore_result.output)
    assert explore_payload["affected_thread_count"] == 1
    assert explore_payload["matched_event_count"] == 1

    workspace_root = tmp_path / "episode_cli"
    open_result = runner.invoke(
        cli_app,
        [
            "whatif",
            "open-episode",
            "--rosetta-dir",
            str(rosetta_dir),
            "--root",
            str(workspace_root),
            "--thread-id",
            "thr-legal-trading",
        ],
    )
    assert open_result.exit_code == 0, open_result.output
    open_payload = json.loads(open_result.output)
    assert open_payload["future_event_count"] == 2
    assert open_payload["branch_event"]["event_id"] == "evt-002"

    replay_result = runner.invoke(
        cli_app,
        [
            "whatif",
            "replay",
            "--root",
            str(workspace_root),
            "--tick-ms",
            "1500",
        ],
    )
    assert replay_result.exit_code == 0, replay_result.output
    replay_payload = json.loads(replay_result.output)
    assert replay_payload["scheduled_event_count"] == 2
    assert replay_payload["delivered_event_count"] == 2

    events_result = runner.invoke(
        cli_app,
        [
            "whatif",
            "events",
            "--rosetta-dir",
            str(rosetta_dir),
            "--actor",
            "jeff.skilling",
            "--query",
            "draft term sheet",
            "--flagged-only",
        ],
    )
    assert events_result.exit_code == 0, events_result.output
    events_payload = json.loads(events_result.output)
    assert events_payload["match_count"] == 1
    assert events_payload["matches"][0]["event"]["event_id"] == "evt-005"


def test_search_events_finds_exact_branch_points(tmp_path: Path) -> None:
    rosetta_dir = tmp_path / "rosetta"
    _write_rosetta_fixture(rosetta_dir)
    world = load_world(source="enron", rosetta_dir=rosetta_dir)
    result = search_events(
        world,
        actor="jeff.skilling",
        query="draft term sheet",
        flagged_only=True,
    )

    assert result.match_count == 1
    assert result.matches[0].event.event_id == "evt-005"
    assert result.matches[0].reason_labels == ["attachment", "external_recipient"]
    assert "External draft attached for review." in result.matches[0].event.snippet


def test_search_events_matches_human_name_queries(tmp_path: Path) -> None:
    rosetta_dir = tmp_path / "rosetta"
    _write_rosetta_fixture(rosetta_dir)
    world = load_world(source="enron", rosetta_dir=rosetta_dir)

    result = search_events(
        world,
        query="Jeff Skilling draft term sheet",
    )

    assert result.match_count == 1
    assert result.matches[0].event.event_id == "evt-005"


def test_ejepa_cache_root_changes_with_branch_event() -> None:
    source_dir = Path("/tmp/enron_rosetta")

    first = _default_cache_root(
        source_dir,
        thread_id="thr-master-agreement",
        branch_event_id="evt-001",
    )
    second = _default_cache_root(
        source_dir,
        thread_id="thr-master-agreement",
        branch_event_id="evt-002",
    )

    assert first != second


def test_materialize_episode_can_branch_from_explicit_event_id(tmp_path: Path) -> None:
    rosetta_dir = tmp_path / "rosetta"
    _write_rosetta_fixture(rosetta_dir)
    world = load_world(source="enron", rosetta_dir=rosetta_dir)

    workspace_root = tmp_path / "episode_by_event"
    materialization = materialize_episode(
        world,
        root=workspace_root,
        event_id="evt-005",
    )

    assert materialization.thread_id == "thr-external"
    assert materialization.branch_event_id == "evt-005"
    assert materialization.history_message_count == 0
    assert materialization.future_event_count == 1


def test_llm_and_forecast_counterfactual_paths_write_artifacts(
    tmp_path: Path,
    monkeypatch,
) -> None:
    rosetta_dir = tmp_path / "rosetta"
    _write_rosetta_fixture(rosetta_dir)
    world = load_world(source="enron", rosetta_dir=rosetta_dir)

    async def fake_plan_once_with_usage(**_: object) -> PlanResult:
        return PlanResult(
            plan={
                "tool": "emit_counterfactual",
                "args": {
                    "summary": "Sara pauses forwarding and asks ops to hold the thread.",
                    "notes": ["Generated from a deterministic test stub."],
                    "messages": [
                        {
                            "actor_id": "sara.shackleton@enron.com",
                            "to": "mark.taylor@enron.com",
                            "subject": "Re: Gas Position Limits",
                            "body_text": "Pause the forward path until compliance has reviewed this.",
                            "delay_ms": 1000,
                            "rationale": "Adds a compliance gate.",
                        },
                        {
                            "actor_id": "mark.taylor@enron.com",
                            "to": "ops.review@enron.com",
                            "subject": "Re: Gas Position Limits",
                            "body_text": "Holding this assignment until legal and compliance confirm next steps.",
                            "delay_ms": 2000,
                            "rationale": "Stops the handoff.",
                        },
                    ],
                },
            },
            usage=PlanUsage(
                provider="openai",
                model="gpt-5",
                prompt_tokens=100,
                completion_tokens=50,
                total_tokens=150,
                estimated_cost_usd=0.001,
            ),
        )

    monkeypatch.setattr(
        "vei.whatif.api.providers.plan_once_with_usage",
        fake_plan_once_with_usage,
    )

    workspace_root = tmp_path / "episode"
    materialize_episode(world, root=workspace_root, thread_id="thr-legal-trading")

    llm_result = run_llm_counterfactual(
        workspace_root,
        prompt="What if Sara paused the forward and asked ops to wait for compliance?",
    )
    forecast_result = run_ejepa_proxy_counterfactual(
        workspace_root,
        prompt="Pause the forward, add compliance, and clarify the owner immediately.",
    )
    experiment = run_counterfactual_experiment(
        world,
        artifacts_root=tmp_path / "artifacts",
        label="compliance_hold",
        counterfactual_prompt=(
            "Pause the forward, add compliance, and clarify the owner immediately."
        ),
        selection_scenario="compliance_gateway",
        mode="both",
    )
    loaded = load_experiment_result(experiment.artifacts.root)

    assert llm_result.status == "ok"
    assert llm_result.delivered_event_count == 2
    assert len(llm_result.messages) == 2
    assert forecast_result.status == "ok"
    assert forecast_result.predicted.risk_score < forecast_result.baseline.risk_score
    assert experiment.llm_result is not None
    assert experiment.llm_result.status == "ok"
    assert experiment.forecast_result is not None
    assert experiment.artifacts.result_json_path.exists()
    assert experiment.artifacts.overview_markdown_path.exists()
    assert experiment.artifacts.llm_json_path is not None
    assert experiment.artifacts.llm_json_path.exists()
    assert experiment.artifacts.forecast_json_path is not None
    assert experiment.artifacts.forecast_json_path.exists()
    assert loaded.intervention.thread_id == "thr-legal-trading"


def test_llm_counterfactual_clamps_external_recipient_for_internal_only_prompt(
    tmp_path: Path,
    monkeypatch,
) -> None:
    rosetta_dir = tmp_path / "rosetta"
    _write_rosetta_fixture(rosetta_dir)
    world = load_world(source="enron", rosetta_dir=rosetta_dir)
    workspace_root = tmp_path / "episode"
    materialize_episode(world, root=workspace_root, thread_id="thr-external")

    async def fake_plan_once_with_usage(**_: object) -> PlanResult:
        return PlanResult(
            plan={
                "tool": "emit_counterfactual",
                "args": {
                    "summary": "The outside recipient is removed.",
                    "messages": [
                        {
                            "actor_id": "jeff.skilling@enron.com",
                            "to": "outside@lawfirm.com",
                            "subject": "Draft term sheet",
                            "body_text": "Keep this internal until cleared.",
                            "delay_ms": 1000,
                        }
                    ],
                },
            },
            usage=PlanUsage(provider="openai", model="gpt-5"),
        )

    monkeypatch.setattr(
        "vei.whatif.api.providers.plan_once_with_usage",
        fake_plan_once_with_usage,
    )

    result = run_llm_counterfactual(
        workspace_root,
        prompt="Remove the outside recipient and keep this internal only.",
    )

    assert result.status == "ok"
    assert result.messages[0].to == "jeff.skilling@enron.com"
    assert any("internal Enron participants" in note for note in result.notes)


def test_llm_counterfactual_fuzzy_matches_named_participants(
    tmp_path: Path,
    monkeypatch,
) -> None:
    rosetta_dir = tmp_path / "rosetta"
    _write_rosetta_fixture(rosetta_dir)
    world = load_world(source="enron", rosetta_dir=rosetta_dir)
    workspace_root = tmp_path / "episode_fuzzy"
    materialize_episode(world, root=workspace_root, thread_id="thr-legal-trading")

    async def fake_plan_once_with_usage(**_: object) -> PlanResult:
        return PlanResult(
            plan={
                "tool": "emit_counterfactual",
                "args": {
                    "summary": "Sara sends the note to Mark by name.",
                    "messages": [
                        {
                            "actor_id": "Sara Shackleton",
                            "to": "Mark Taylor",
                            "subject": "Re: Gas Position Limits",
                            "body_text": "Please pause this until we finish review.",
                            "delay_ms": 1000,
                        }
                    ],
                },
            },
            usage=PlanUsage(provider="openai", model="gpt-5"),
        )

    monkeypatch.setattr(
        "vei.whatif.api.providers.plan_once_with_usage",
        fake_plan_once_with_usage,
    )

    result = run_llm_counterfactual(
        workspace_root,
        prompt="Keep this internal and pause the handoff.",
    )

    assert result.status == "ok"
    assert result.messages[0].actor_id == "sara.shackleton@enron.com"
    assert result.messages[0].to == "mark.taylor@enron.com"


def test_vei_whatif_cli_experiment(tmp_path: Path, monkeypatch) -> None:
    rosetta_dir = tmp_path / "rosetta"
    _write_rosetta_fixture(rosetta_dir)
    runner = CliRunner()

    async def fake_plan_once_with_usage(**_: object) -> PlanResult:
        return PlanResult(
            plan={
                "tool": "emit_counterfactual",
                "args": {
                    "summary": "External recipient removed before the draft leaves.",
                    "messages": [
                        {
                            "actor_id": "jeff.skilling@enron.com",
                            "to": "jeff.skilling@enron.com",
                            "subject": "Re: Draft term sheet",
                            "body_text": "Keep this internal until the attachment is cleared.",
                            "delay_ms": 1000,
                            "rationale": "Prevents the outside send.",
                        }
                    ],
                },
            },
            usage=PlanUsage(provider="openai", model="gpt-5"),
        )

    monkeypatch.setattr(
        "vei.whatif.api.providers.plan_once_with_usage",
        fake_plan_once_with_usage,
    )

    artifacts_root = tmp_path / "whatif_out"
    result = runner.invoke(
        cli_app,
        [
            "whatif",
            "experiment",
            "--rosetta-dir",
            str(rosetta_dir),
            "--artifacts-root",
            str(artifacts_root),
            "--label",
            "external_hold",
            "--counterfactual-prompt",
            "Remove the outside recipient and strip the attachment before it leaves.",
            "--selection-scenario",
            "external_dlp",
        ],
    )

    assert result.exit_code == 0, result.output
    payload = json.loads(result.output)
    assert payload["label"] == "external_hold"
    assert payload["llm_result"]["status"] == "ok"
    assert payload["forecast_result"]["status"] == "ok"

    show_result = runner.invoke(
        cli_app,
        [
            "whatif",
            "show-result",
            "--root",
            str(artifacts_root / "external_hold"),
            "--format",
            "markdown",
        ],
    )
    assert show_result.exit_code == 0, show_result.output
    assert "Counterfactual Rollout" in show_result.output


def test_counterfactual_experiment_can_use_ejepa_backend(
    tmp_path: Path,
    monkeypatch,
) -> None:
    rosetta_dir = tmp_path / "rosetta"
    _write_rosetta_fixture(rosetta_dir)
    world = load_world(source="enron", rosetta_dir=rosetta_dir)

    async def fake_plan_once_with_usage(**_: object) -> PlanResult:
        return PlanResult(
            plan={
                "tool": "emit_counterfactual",
                "args": {
                    "summary": "Hold the outside send and keep the thread internal.",
                    "messages": [
                        {
                            "actor_id": "jeff.skilling@enron.com",
                            "to": "jeff.skilling@enron.com",
                            "subject": "Re: Draft term sheet",
                            "body_text": "Keep this inside until legal clears it.",
                            "delay_ms": 1000,
                        }
                    ],
                },
            },
            usage=PlanUsage(provider="openai", model="gpt-5"),
        )

    monkeypatch.setattr(
        "vei.whatif.api.providers.plan_once_with_usage",
        fake_plan_once_with_usage,
    )

    def fake_run_ejepa_counterfactual(
        *_: object, **kwargs: object
    ) -> WhatIfForecastResult:
        assert kwargs["epochs"] == 2
        assert kwargs["batch_size"] == 16
        assert kwargs["force_retrain"] is True
        assert kwargs["device"] == "cpu"
        return WhatIfForecastResult(
            status="ok",
            backend="e_jepa",
            prompt="Keep this internal.",
            summary="Real E-JEPA forecast completed.",
            baseline=WhatIfForecast(
                backend="historical",
                future_event_count=1,
                future_external_event_count=1,
                risk_score=0.5,
            ),
            predicted=WhatIfForecast(
                backend="e_jepa",
                future_event_count=1,
                future_external_event_count=0,
                risk_score=0.2,
            ),
            delta=WhatIfForecastDelta(
                risk_score_delta=-0.3,
                external_event_delta=-1,
            ),
            branch_event=WhatIfEventReference(
                event_id="evt-005",
                timestamp="2001-05-01T10:00:04Z",
                actor_id="jeff.skilling@enron.com",
                event_type="message",
                thread_id="thr-external",
                subject="Draft term sheet",
            ),
            notes=["Used the real E-JEPA backend path."],
        )

    monkeypatch.setattr(
        "vei.whatif.api.run_ejepa_counterfactual",
        fake_run_ejepa_counterfactual,
    )

    experiment = run_counterfactual_experiment(
        world,
        artifacts_root=tmp_path / "artifacts",
        label="ejepa_hold",
        counterfactual_prompt="Keep this internal.",
        event_id="evt-005",
        mode="both",
        forecast_backend="e_jepa",
        ejepa_epochs=2,
        ejepa_batch_size=16,
        ejepa_force_retrain=True,
        ejepa_device="cpu",
    )

    assert experiment.forecast_result is not None
    assert experiment.forecast_result.backend == "e_jepa"
    assert experiment.artifacts.forecast_json_path is not None
    assert experiment.artifacts.forecast_json_path.name == "whatif_ejepa_result.json"
