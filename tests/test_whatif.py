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
    build_branch_point_benchmark,
    evaluate_branch_point_benchmark_model,
    get_research_pack,
    judge_branch_point_benchmark,
    list_branch_point_benchmark_models,
    load_branch_point_benchmark_build_result,
    load_branch_point_benchmark_eval_result,
    load_branch_point_benchmark_judge_result,
    load_branch_point_benchmark_train_result,
    load_ranked_experiment_result,
    load_research_pack_run_result,
    load_experiment_result,
    load_episode_manifest,
    list_objective_packs,
    list_research_packs,
    load_world,
    materialize_episode,
    replay_episode_baseline,
    run_research_pack,
    search_events,
    train_branch_point_benchmark_model,
    run_counterfactual_experiment,
    run_ranked_counterfactual_experiment,
    run_ejepa_proxy_counterfactual,
    run_llm_counterfactual,
    run_whatif,
)
from vei.whatif.ejepa import _default_cache_root
from vei.whatif.models import (
    WhatIfBenchmarkCaseEvaluation,
    WhatIfBenchmarkEvalArtifacts,
    WhatIfBenchmarkEvalResult,
    WhatIfBenchmarkJudgeArtifacts,
    WhatIfBenchmarkJudgeResult,
    WhatIfBenchmarkTrainArtifacts,
    WhatIfBenchmarkTrainResult,
    WhatIfCounterfactualCandidatePrediction,
    WhatIfCounterfactualObjectiveEvaluation,
    WhatIfObservedEvidenceHeads,
    WhatIfObservedForecastMetrics,
    WhatIfCandidateIntervention,
    WhatIfAuditRecord,
    WhatIfEventReference,
    WhatIfForecast,
    WhatIfForecastDelta,
    WhatIfForecastResult,
    WhatIfLLMGeneratedMessage,
    WhatIfLLMReplayResult,
    WhatIfOutcomeSignals,
    WhatIfResearchCandidate,
    WhatIfResearchCase,
    WhatIfResearchPack,
)
from vei.whatif.benchmark_business import (
    evidence_to_business_outcomes,
    get_business_objective_pack,
    list_business_objective_packs,
    score_business_objective,
    summarize_observed_evidence,
)
from vei.whatif.ranking import (
    aggregate_outcome_signals,
    get_objective_pack,
    score_outcome_signals,
    sort_candidates_for_rank,
    summarize_llm_branch,
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


def _make_llm_replay_result(
    *,
    prompt: str,
    to: str,
    subject: str,
    body_text: str,
    delay_ms: int,
    summary: str,
    notes: list[str] | None = None,
) -> WhatIfLLMReplayResult:
    return WhatIfLLMReplayResult(
        status="ok",
        provider="openai",
        model="gpt-5-mini",
        prompt=prompt,
        summary=summary,
        messages=[
            WhatIfLLMGeneratedMessage(
                actor_id="jeff.skilling@enron.com",
                to=to,
                subject=subject,
                body_text=body_text,
                delay_ms=delay_ms,
            )
        ],
        scheduled_event_count=1,
        delivered_event_count=1,
        inbox_count=1,
        notes=notes or [],
    )


def _make_forecast_result(
    *,
    prompt: str,
    risk_score: float,
    future_event_count: int,
    future_external_event_count: int,
    summary: str,
) -> WhatIfForecastResult:
    baseline = WhatIfForecast(
        backend="historical",
        future_event_count=2,
        future_external_event_count=1,
        risk_score=0.6,
    )
    predicted = WhatIfForecast(
        backend="e_jepa_proxy",
        future_event_count=future_event_count,
        future_external_event_count=future_external_event_count,
        risk_score=risk_score,
    )
    return WhatIfForecastResult(
        status="ok",
        backend="e_jepa_proxy",
        prompt=prompt,
        summary=summary,
        baseline=baseline,
        predicted=predicted,
        delta=WhatIfForecastDelta(
            risk_score_delta=round(risk_score - baseline.risk_score, 3),
            future_event_delta=future_event_count - baseline.future_event_count,
            external_event_delta=(
                future_external_event_count - baseline.future_external_event_count
            ),
        ),
    )


def _make_research_pack() -> WhatIfResearchPack:
    return WhatIfResearchPack(
        pack_id="fixture_pack",
        title="Fixture Pack",
        summary="Small fixture pack for the research runner tests.",
        objective_pack_ids=[
            "contain_exposure",
            "reduce_delay",
            "protect_relationship",
        ],
        rollout_seeds=[42042, 42043],
        cases=[
            WhatIfResearchCase(
                case_id="external_hold_case",
                title="External Hold Case",
                event_id="evt-005",
                thread_id="thr-external",
                summary="A small held-out external thread.",
                candidates=[
                    WhatIfResearchCandidate(
                        candidate_id="legal_hold_internal",
                        label="Legal hold internal",
                        prompt=(
                            "Keep this internal, hold the send, and ask legal to "
                            "review before anything leaves Enron."
                        ),
                        expected_hypotheses={
                            "contain_exposure": "best_expected",
                            "reduce_delay": "worst_expected",
                            "protect_relationship": "middle_expected",
                        },
                    ),
                    WhatIfResearchCandidate(
                        candidate_id="narrow_external_status",
                        label="Narrow external status",
                        prompt=(
                            "Send a short external status note right away, promise a "
                            "clean draft later, and keep the attachment inside."
                        ),
                        expected_hypotheses={
                            "contain_exposure": "middle_expected",
                            "reduce_delay": "best_expected",
                            "protect_relationship": "best_expected",
                        },
                    ),
                    WhatIfResearchCandidate(
                        candidate_id="broad_external_send",
                        label="Broad external send",
                        prompt=(
                            "Send the draft outside now, include the attachment, and "
                            "widen circulation for urgent comments."
                        ),
                        expected_hypotheses={
                            "contain_exposure": "worst_expected",
                            "reduce_delay": "middle_expected",
                            "protect_relationship": "worst_expected",
                        },
                    ),
                ],
            )
        ],
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


def test_list_objective_packs_and_score_shape_cover_all_ranked_objectives() -> None:
    packs = list_objective_packs()
    pack_ids = {pack.pack_id for pack in packs}

    assert pack_ids == {
        "contain_exposure",
        "reduce_delay",
        "protect_relationship",
    }

    branch_event = WhatIfEventReference(
        event_id="evt-005",
        timestamp="2001-05-01T10:00:04Z",
        actor_id="jeff.skilling@enron.com",
        target_id="outside@lawfirm.com",
        event_type="message",
        thread_id="thr-external",
        subject="Draft term sheet",
        to_recipients=["outside@lawfirm.com"],
        has_attachment_reference=True,
    )
    llm_result = _make_llm_replay_result(
        prompt="Keep this internal and pause the send.",
        to="jeff.skilling@enron.com",
        subject="Please hold for review",
        body_text="Please keep this internal while legal reviews the draft.",
        delay_ms=1000,
        summary="The thread stays inside Enron while legal reviews it.",
    )

    outcome = summarize_llm_branch(
        branch_event=branch_event,
        llm_result=llm_result,
    )

    assert outcome.internal_only is True
    assert outcome.outside_message_count == 0

    for pack in packs:
        score = score_outcome_signals(pack=pack, outcome=outcome)
        assert score.objective_pack_id == pack.pack_id
        assert 0.0 <= score.overall_score <= 1.0
        assert len(score.evidence) >= 3


def test_aggregate_outcome_signals_and_rank_tiebreaker_use_simple_contract() -> None:
    branch_event = WhatIfEventReference(
        event_id="evt-005",
        timestamp="2001-05-01T10:00:04Z",
        actor_id="jeff.skilling@enron.com",
        target_id="outside@lawfirm.com",
        event_type="message",
        thread_id="thr-external",
        subject="Draft term sheet",
        to_recipients=["outside@lawfirm.com"],
        has_attachment_reference=True,
    )
    first = summarize_llm_branch(
        branch_event=branch_event,
        llm_result=_make_llm_replay_result(
            prompt="Keep this internal and pause the send.",
            to="jeff.skilling@enron.com",
            subject="Please hold for review",
            body_text="Please hold this internally for review.",
            delay_ms=1000,
            summary="Internal review path.",
        ),
    )
    second = summarize_llm_branch(
        branch_event=branch_event,
        llm_result=_make_llm_replay_result(
            prompt="Please keep this inside and clarify ownership.",
            to="sara.shackleton@enron.com",
            subject="Please confirm next steps",
            body_text="Please confirm ownership before the draft moves.",
            delay_ms=2000,
            summary="Internal ownership clarification path.",
        ),
    )

    aggregate = aggregate_outcome_signals([first, second])
    assert aggregate.internal_only is True
    assert aggregate.avg_delay_ms == 1500
    assert aggregate.outside_message_count == 0

    pack = get_objective_pack("contain_exposure")
    ranked = sort_candidates_for_rank(
        [
            (
                "Option B",
                WhatIfOutcomeSignals(
                    exposure_risk=0.3,
                    delay_risk=0.2,
                    relationship_protection=0.7,
                ),
                score_outcome_signals(
                    pack=pack,
                    outcome=WhatIfOutcomeSignals(
                        exposure_risk=0.2,
                        delay_risk=0.2,
                        relationship_protection=0.7,
                    ),
                ),
            ),
            (
                "Option A",
                WhatIfOutcomeSignals(
                    exposure_risk=0.1,
                    delay_risk=0.5,
                    relationship_protection=0.7,
                ),
                score_outcome_signals(
                    pack=pack,
                    outcome=WhatIfOutcomeSignals(
                        exposure_risk=0.2,
                        delay_risk=0.2,
                        relationship_protection=0.7,
                    ),
                ),
            ),
        ]
    )

    assert ranked == ["Option A", "Option B"]


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


def test_run_ranked_counterfactual_experiment_writes_artifacts_and_keeps_shadow_mode(
    tmp_path: Path,
    monkeypatch,
) -> None:
    rosetta_dir = tmp_path / "rosetta"
    _write_rosetta_fixture(rosetta_dir)
    world = load_world(source="enron", rosetta_dir=rosetta_dir)

    def fake_run_llm_counterfactual(
        *_: object,
        prompt: str,
        provider: str = "openai",
        model: str = "gpt-5-mini",
        seed: int = 42042,
    ) -> WhatIfLLMReplayResult:
        assert provider == "openai"
        assert model == "gpt-5-mini"
        if "internal" in prompt.lower():
            return _make_llm_replay_result(
                prompt=prompt,
                to="jeff.skilling@enron.com",
                subject="Please hold for review",
                body_text="Please keep this internal while legal reviews the draft.",
                delay_ms=1000 + (seed % 3) * 100,
                summary="The thread stays internal while legal reviews it.",
            )
        return _make_llm_replay_result(
            prompt=prompt,
            to="outside@lawfirm.com",
            subject="Draft term sheet attached",
            body_text="Sending the draft outside immediately.",
            delay_ms=9000 + (seed % 3) * 100,
            summary="The draft leaves Enron immediately.",
            notes=["Attachment still included."],
        )

    def fake_run_ejepa_proxy_counterfactual(
        *_: object,
        prompt: str,
    ) -> WhatIfForecastResult:
        if "internal" in prompt.lower():
            return _make_forecast_result(
                prompt=prompt,
                risk_score=0.8,
                future_event_count=3,
                future_external_event_count=1,
                summary="Shadow forecast still expects outside exposure.",
            )
        return _make_forecast_result(
            prompt=prompt,
            risk_score=0.1,
            future_event_count=1,
            future_external_event_count=0,
            summary="Shadow forecast prefers the outside send path.",
        )

    monkeypatch.setattr(
        "vei.whatif.api.run_llm_counterfactual",
        fake_run_llm_counterfactual,
    )
    monkeypatch.setattr(
        "vei.whatif.api.run_ejepa_proxy_counterfactual",
        fake_run_ejepa_proxy_counterfactual,
    )

    result = run_ranked_counterfactual_experiment(
        world,
        artifacts_root=tmp_path / "ranked_artifacts",
        label="external_ranked",
        objective_pack_id="contain_exposure",
        candidate_interventions=[
            WhatIfCandidateIntervention(
                label="Hold internal",
                prompt="Keep this internal and pause the send.",
            ),
            WhatIfCandidateIntervention(
                label="Send outside",
                prompt="Send the draft outside immediately.",
            ),
        ],
        event_id="evt-005",
        rollout_count=3,
        shadow_forecast_backend="e_jepa_proxy",
    )
    loaded = load_ranked_experiment_result(result.artifacts.root)

    assert result.recommended_candidate_label == "Hold internal"
    assert len(result.candidates) == 2
    assert [candidate.rollout_count for candidate in result.candidates] == [3, 3]
    assert result.candidates[0].intervention.label == "Hold internal"
    assert result.candidates[0].reason.startswith("Best for contain exposure")
    assert result.candidates[0].shadow is not None
    assert result.candidates[0].shadow.backend == "e_jepa_proxy"
    assert (
        result.candidates[0].shadow.outcome_score.overall_score
        < result.candidates[1].shadow.outcome_score.overall_score
    )
    assert result.artifacts.result_json_path.exists()
    assert result.artifacts.overview_markdown_path.exists()
    assert loaded.recommended_candidate_label == "Hold internal"
    assert loaded.candidates[0].shadow is not None
    assert loaded.candidates[0].shadow.outcome_score.objective_pack_id == (
        "contain_exposure"
    )


def test_vei_whatif_cli_rank_and_show_ranked_result(
    tmp_path: Path,
    monkeypatch,
) -> None:
    rosetta_dir = tmp_path / "rosetta"
    _write_rosetta_fixture(rosetta_dir)
    runner = CliRunner()

    def fake_run_llm_counterfactual(
        *_: object,
        prompt: str,
        **__: object,
    ) -> WhatIfLLMReplayResult:
        if "internal" in prompt.lower():
            return _make_llm_replay_result(
                prompt=prompt,
                to="jeff.skilling@enron.com",
                subject="Please hold for review",
                body_text="Please keep this inside while legal reviews it.",
                delay_ms=1000,
                summary="Internal review replaces the outside send.",
            )
        return _make_llm_replay_result(
            prompt=prompt,
            to="outside@lawfirm.com",
            subject="Draft term sheet attached",
            body_text="Sending the draft outside now.",
            delay_ms=9000,
            summary="The draft goes outside.",
        )

    monkeypatch.setattr(
        "vei.whatif.api.run_llm_counterfactual",
        fake_run_llm_counterfactual,
    )
    monkeypatch.setattr(
        "vei.whatif.api.run_ejepa_proxy_counterfactual",
        lambda *_args, prompt, **_kwargs: _make_forecast_result(
            prompt=prompt,
            risk_score=0.3,
            future_event_count=1,
            future_external_event_count=0,
            summary="Shadow forecast completed.",
        ),
    )

    artifacts_root = tmp_path / "whatif_ranked_out"
    result = runner.invoke(
        cli_app,
        [
            "whatif",
            "rank",
            "--rosetta-dir",
            str(rosetta_dir),
            "--artifacts-root",
            str(artifacts_root),
            "--label",
            "external_ranked",
            "--objective-pack-id",
            "contain_exposure",
            "--event-id",
            "evt-005",
            "--shadow-forecast-backend",
            "e_jepa_proxy",
            "--candidate",
            "Keep this internal and pause.",
            "--candidate",
            "Send the draft outside now.",
        ],
    )

    assert result.exit_code == 0, result.output
    payload = json.loads(result.output)
    assert payload["recommended_candidate_label"] == "Keep this internal and pause."
    assert len(payload["candidates"]) == 2
    assert payload["candidates"][0]["shadow"]["backend"] == "e_jepa_proxy"

    show_result = runner.invoke(
        cli_app,
        [
            "whatif",
            "show-ranked-result",
            "--root",
            str(artifacts_root / "external_ranked"),
            "--format",
            "markdown",
        ],
    )
    assert show_result.exit_code == 0, show_result.output
    assert "Ranked Candidates" in show_result.output
    assert "Keep this internal and pause." in show_result.output


def test_research_pack_registry_exposes_built_in_enron_pack() -> None:
    packs = list_research_packs()
    pack_ids = {pack.pack_id for pack in packs}
    built_in = get_research_pack("enron_research_v1")

    assert "enron_research_v1" in pack_ids
    assert built_in.objective_pack_ids == [
        "contain_exposure",
        "reduce_delay",
        "protect_relationship",
    ]
    assert len(built_in.rollout_seeds) == 8
    assert len(built_in.cases) == 6
    assert built_in.cases[0].event_id == "enron_bcda1b925800af8c"


def test_run_research_pack_writes_artifacts_and_scores_all_backends(
    tmp_path: Path,
    monkeypatch,
) -> None:
    rosetta_dir = tmp_path / "rosetta"
    _write_rosetta_fixture(rosetta_dir)
    world = load_world(source="enron", rosetta_dir=rosetta_dir)
    pack = _make_research_pack()

    def fake_run_llm_counterfactual(
        *_: object,
        prompt: str,
        provider: str = "openai",
        model: str = "gpt-5-mini",
        seed: int = 42042,
    ) -> WhatIfLLMReplayResult:
        assert provider == "openai"
        assert model == "gpt-5-mini"
        if "internal" in prompt.lower():
            return _make_llm_replay_result(
                prompt=prompt,
                to="jeff.skilling@enron.com",
                subject="Please hold for review",
                body_text="Please hold this internally while legal reviews it.",
                delay_ms=8_000 + (seed % 2) * 1_000,
                summary="The draft stays inside Enron during legal review.",
            )
        if "status note" in prompt.lower():
            return _make_llm_replay_result(
                prompt=prompt,
                to="outside@lawfirm.com",
                subject="Status update",
                body_text="Please expect a revised copy soon. Thanks.",
                delay_ms=900 + (seed % 2) * 200,
                summary="A short external status note goes out quickly.",
            )
        return _make_llm_replay_result(
            prompt=prompt,
            to="outside@lawfirm.com",
            subject="Draft attached urgently",
            body_text="Sending the draft attachment outside now for urgent comments.",
            delay_ms=2_200 + (seed % 2) * 400,
            summary="The draft leaves Enron with broader outside circulation.",
        )

    def fake_run_ejepa_proxy_counterfactual(
        *_: object,
        prompt: str,
    ) -> WhatIfForecastResult:
        if "internal" in prompt.lower():
            return _make_forecast_result(
                prompt=prompt,
                risk_score=0.15,
                future_event_count=2,
                future_external_event_count=0,
                summary="Proxy forecast prefers the internal legal hold.",
            )
        if "status note" in prompt.lower():
            return _make_forecast_result(
                prompt=prompt,
                risk_score=0.42,
                future_event_count=1,
                future_external_event_count=1,
                summary="Proxy forecast sees one controlled outside status touch.",
            )
        return _make_forecast_result(
            prompt=prompt,
            risk_score=0.9,
            future_event_count=3,
            future_external_event_count=2,
            summary="Proxy forecast expects broad outside circulation.",
        )

    def fake_run_ejepa_counterfactual(
        *_: object,
        prompt: str,
        **__: object,
    ) -> WhatIfForecastResult:
        if "status note" in prompt.lower():
            return WhatIfForecastResult(
                status="error",
                backend="e_jepa",
                prompt=prompt,
                summary="Real E-JEPA backend could not train on this tiny fixture.",
                error="fixture training error",
                notes=["Forced test fallback."],
            )
        if "internal" in prompt.lower():
            return WhatIfForecastResult(
                status="ok",
                backend="e_jepa",
                prompt=prompt,
                summary="Real E-JEPA backend prefers the internal hold.",
                baseline=WhatIfForecast(
                    backend="historical",
                    future_event_count=2,
                    future_external_event_count=1,
                    risk_score=0.6,
                ),
                predicted=WhatIfForecast(
                    backend="e_jepa",
                    future_event_count=2,
                    future_external_event_count=0,
                    risk_score=0.18,
                ),
                delta=WhatIfForecastDelta(
                    risk_score_delta=-0.42,
                    future_event_delta=0,
                    external_event_delta=-1,
                ),
                notes=["Real E-JEPA path used in the fixture test."],
            )
        return WhatIfForecastResult(
            status="ok",
            backend="e_jepa",
            prompt=prompt,
            summary="Real E-JEPA backend dislikes the broad outside send.",
            baseline=WhatIfForecast(
                backend="historical",
                future_event_count=2,
                future_external_event_count=1,
                risk_score=0.6,
            ),
            predicted=WhatIfForecast(
                backend="e_jepa",
                future_event_count=3,
                future_external_event_count=2,
                risk_score=0.92,
            ),
            delta=WhatIfForecastDelta(
                risk_score_delta=0.32,
                future_event_delta=1,
                external_event_delta=1,
            ),
            notes=["Real E-JEPA path used in the fixture test."],
        )

    monkeypatch.setattr(
        "vei.whatif.research.run_llm_counterfactual",
        fake_run_llm_counterfactual,
    )
    monkeypatch.setattr(
        "vei.whatif.research.run_ejepa_proxy_counterfactual",
        fake_run_ejepa_proxy_counterfactual,
    )
    monkeypatch.setattr(
        "vei.whatif.research.run_ejepa_counterfactual",
        fake_run_ejepa_counterfactual,
    )

    result = run_research_pack(
        world,
        artifacts_root=tmp_path / "research_artifacts",
        label="fixture_pack_run",
        research_pack=pack,
        provider="openai",
        model="gpt-5-mini",
    )
    loaded = load_research_pack_run_result(result.artifacts.root)

    assert result.pack.pack_id == "fixture_pack"
    assert result.dataset.heldout_thread_ids == ["thr-external"]
    assert result.dataset.historical_row_count == 1
    assert result.dataset.evaluation_row_count == 1
    assert result.hypothesis_pass_count == 3
    assert result.hypothesis_total_count == 3
    assert result.hypothesis_pass_rate == 1.0
    assert result.artifacts.result_json_path.exists()
    assert result.artifacts.overview_markdown_path.exists()
    assert result.artifacts.pilot_markdown_path.exists()
    assert loaded.pack.pack_id == "fixture_pack"

    objective_results = result.cases[0].objectives
    contain_exposure = next(
        objective
        for objective in objective_results
        if objective.objective_pack.pack_id == "contain_exposure"
    )
    reduce_delay = next(
        objective
        for objective in objective_results
        if objective.objective_pack.pack_id == "reduce_delay"
    )
    protect_relationship = next(
        objective
        for objective in objective_results
        if objective.objective_pack.pack_id == "protect_relationship"
    )

    assert contain_exposure.recommended_candidate_label == "Legal hold internal"
    assert reduce_delay.recommended_candidate_label == "Narrow external status"
    assert protect_relationship.recommended_candidate_label == "Narrow external status"

    for objective in objective_results:
        assert objective.expected_order_ok is True
        for candidate in objective.candidates:
            backend_ids = {score.backend for score in candidate.backend_scores}
            assert backend_ids == {
                "e_jepa",
                "e_jepa_proxy",
                "ft_transformer",
                "ts2vec",
                "g_transformer",
            }

    fallback_candidate = next(
        candidate
        for candidate in contain_exposure.candidates
        if candidate.candidate.candidate_id == "narrow_external_status"
    )
    fallback_score = next(
        score
        for score in fallback_candidate.backend_scores
        if score.backend == "e_jepa"
    )
    assert fallback_score.status == "fallback"
    assert fallback_score.effective_backend == "e_jepa_proxy"

    train_rows = []
    for split_name in ("train", "validation", "test"):
        split_path = Path(result.dataset.split_paths[split_name])
        if not split_path.exists():
            continue
        train_rows.extend(
            [
                json.loads(line)
                for line in split_path.read_text(encoding="utf-8").splitlines()
                if line.strip()
            ]
        )
    assert train_rows
    assert all(row["thread_id"] != "thr-external" for row in train_rows)

    scoreboard = result.artifacts.overview_markdown_path.read_text(encoding="utf-8")
    assert "Fixture Pack" in scoreboard
    assert "Hypothesis pass rate: 1.000 (3/3)" in scoreboard


def test_run_research_pack_reuses_completed_case_results(
    tmp_path: Path,
    monkeypatch,
) -> None:
    rosetta_dir = tmp_path / "rosetta"
    _write_rosetta_fixture(rosetta_dir)
    world = load_world(source="enron", rosetta_dir=rosetta_dir)
    pack = _make_research_pack()

    def fake_run_llm_counterfactual(
        *_: object,
        prompt: str,
        **__: object,
    ) -> WhatIfLLMReplayResult:
        return _make_llm_replay_result(
            prompt=prompt,
            to="jeff.skilling@enron.com",
            subject="Please hold for review",
            body_text="Please hold this internally while legal reviews it.",
            delay_ms=8_000,
            summary="The draft stays inside Enron during legal review.",
        )

    def fake_run_forecast(
        *_: object,
        prompt: str,
        **__: object,
    ) -> WhatIfForecastResult:
        return _make_forecast_result(
            prompt=prompt,
            risk_score=0.15,
            future_event_count=2,
            future_external_event_count=0,
            summary="Fixture forecast completed.",
        )

    monkeypatch.setattr(
        "vei.whatif.research.run_llm_counterfactual",
        fake_run_llm_counterfactual,
    )
    monkeypatch.setattr(
        "vei.whatif.research.run_ejepa_proxy_counterfactual",
        fake_run_forecast,
    )
    monkeypatch.setattr(
        "vei.whatif.research.run_ejepa_counterfactual",
        fake_run_forecast,
    )

    first_result = run_research_pack(
        world,
        artifacts_root=tmp_path / "research_artifacts",
        label="fixture_pack_resume",
        research_pack=pack,
        rollout_workers=2,
    )

    monkeypatch.setattr(
        "vei.whatif.research.run_llm_counterfactual",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("cached case should skip rollout regeneration")
        ),
    )
    monkeypatch.setattr(
        "vei.whatif.research.run_ejepa_proxy_counterfactual",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("cached case should skip proxy scoring")
        ),
    )
    monkeypatch.setattr(
        "vei.whatif.research.run_ejepa_counterfactual",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("cached case should skip JEPA scoring")
        ),
    )

    second_result = run_research_pack(
        world,
        artifacts_root=tmp_path / "research_artifacts",
        label="fixture_pack_resume",
        research_pack=pack,
        rollout_workers=2,
    )

    assert second_result.cases[0].case.case_id == first_result.cases[0].case.case_id
    assert second_result.hypothesis_pass_count == first_result.hypothesis_pass_count
    assert second_result.hypothesis_total_count == first_result.hypothesis_total_count
    assert (
        second_result.artifacts.root
        / "cases"
        / pack.cases[0].case_id
        / "case_result.json"
    ).exists()


def test_vei_whatif_cli_pack_run_list_and_show(tmp_path: Path, monkeypatch) -> None:
    rosetta_dir = tmp_path / "rosetta"
    _write_rosetta_fixture(rosetta_dir)
    world = load_world(source="enron", rosetta_dir=rosetta_dir)
    pack = _make_research_pack()

    monkeypatch.setattr(
        "vei.whatif.research.run_llm_counterfactual",
        lambda *_args, prompt, **_kwargs: _make_llm_replay_result(
            prompt=prompt,
            to="jeff.skilling@enron.com",
            subject="Please hold for review",
            body_text="Please hold this internally while legal reviews it.",
            delay_ms=8_000,
            summary="The draft stays inside Enron during legal review.",
        ),
    )
    monkeypatch.setattr(
        "vei.whatif.research.run_ejepa_proxy_counterfactual",
        lambda *_args, prompt, **_kwargs: _make_forecast_result(
            prompt=prompt,
            risk_score=0.15,
            future_event_count=2,
            future_external_event_count=0,
            summary="Proxy forecast completed.",
        ),
    )
    monkeypatch.setattr(
        "vei.whatif.research.run_ejepa_counterfactual",
        lambda *_args, prompt, **_kwargs: WhatIfForecastResult(
            status="ok",
            backend="e_jepa",
            prompt=prompt,
            summary="Real E-JEPA fixture forecast completed.",
            baseline=WhatIfForecast(
                backend="historical",
                future_event_count=2,
                future_external_event_count=1,
                risk_score=0.6,
            ),
            predicted=WhatIfForecast(
                backend="e_jepa",
                future_event_count=2,
                future_external_event_count=0,
                risk_score=0.2,
            ),
            delta=WhatIfForecastDelta(
                risk_score_delta=-0.4,
                future_event_delta=0,
                external_event_delta=-1,
            ),
        ),
    )

    precomputed = run_research_pack(
        world,
        artifacts_root=tmp_path / "research_artifacts",
        label="fixture_pack_run",
        research_pack=pack,
    )

    runner = CliRunner()
    list_result = runner.invoke(
        cli_app,
        ["whatif", "pack", "list", "--format", "markdown"],
    )
    assert list_result.exit_code == 0, list_result.output
    assert "enron_research_v1" in list_result.output

    monkeypatch.setattr(
        "vei.cli.vei_whatif.get_research_pack",
        lambda _pack_id: pack,
    )
    monkeypatch.setattr(
        "vei.cli.vei_whatif.run_research_pack",
        lambda *args, **kwargs: precomputed,
    )

    run_result = runner.invoke(
        cli_app,
        [
            "whatif",
            "pack",
            "run",
            "--rosetta-dir",
            str(rosetta_dir),
            "--artifacts-root",
            str(tmp_path / "cli_artifacts"),
            "--label",
            "fixture_cli",
            "--pack-id",
            "fixture_pack",
        ],
    )
    assert run_result.exit_code == 0, run_result.output
    run_payload = json.loads(run_result.output)
    assert run_payload["pack"]["pack_id"] == "fixture_pack"
    assert run_payload["cases"][0]["case"]["case_id"] == "external_hold_case"

    show_result = runner.invoke(
        cli_app,
        [
            "whatif",
            "pack",
            "show",
            "--root",
            str(precomputed.artifacts.root),
            "--format",
            "markdown",
        ],
    )
    assert show_result.exit_code == 0, show_result.output
    assert "Fixture Pack" in show_result.output
    assert "Hypothesis pass rate" in show_result.output


def test_branch_point_benchmark_build_writes_prebranch_dataset_and_dossiers(
    tmp_path: Path,
    monkeypatch,
) -> None:
    rosetta_dir = tmp_path / "rosetta"
    _write_rosetta_fixture(rosetta_dir)
    world = load_world(source="enron", rosetta_dir=rosetta_dir)
    pack = _make_research_pack()

    monkeypatch.setattr(
        "vei.whatif.benchmark.get_research_pack",
        lambda _pack_id: pack,
    )

    result = build_branch_point_benchmark(
        world,
        artifacts_root=tmp_path / "benchmark_artifacts",
        label="fixture_benchmark",
        heldout_pack_id="fixture_pack",
    )
    loaded = load_branch_point_benchmark_build_result(result.artifacts.root)

    assert list_branch_point_benchmark_models() == [
        "jepa_latent",
        "ft_transformer",
        "sequence_transformer",
        "treatment_transformer",
    ]
    assert result.dataset.split_row_counts == {
        "train": 1,
        "validation": 0,
        "test": 0,
        "heldout": 1,
    }
    assert loaded.cases[0].case_id == "external_hold_case"
    assert result.artifacts.heldout_cases_path.exists()
    assert result.artifacts.judge_template_path.exists()
    assert result.artifacts.audit_template_path.exists()
    assert (
        result.artifacts.dossier_root
        / "external_hold_case"
        / "minimize_enterprise_risk.md"
    ).exists()

    train_rows = [
        json.loads(line)
        for line in Path(result.dataset.split_paths["train"])
        .read_text(encoding="utf-8")
        .splitlines()
        if line.strip()
    ]
    heldout_rows = [
        json.loads(line)
        for line in Path(result.dataset.split_paths["heldout"])
        .read_text(encoding="utf-8")
        .splitlines()
        if line.strip()
    ]
    assert train_rows[0]["thread_id"] == "thr-legal-trading"
    assert "observed_evidence_heads" in train_rows[0]
    assert "observed_business_outcomes" in train_rows[0]
    assert all(
        not feature["name"].startswith("branch_")
        for feature in train_rows[0]["contract"]["summary_features"]
    )
    assert all(
        step["phase"] == "history"
        for step in train_rows[0]["contract"]["sequence_steps"]
    )
    assert heldout_rows[0]["contract"]["case_id"] == "external_hold_case"
    assert heldout_rows[0]["contract"]["summary_features"]
    assert set(loaded.cases[0].objective_dossier_paths) == {
        pack.pack_id for pack in list_business_objective_packs()
    }
    assert "Legal hold internal" in (
        result.artifacts.dossier_root
        / "external_hold_case"
        / "minimize_enterprise_risk.md"
    ).read_text(encoding="utf-8")
    judged_template = json.loads(
        result.artifacts.judge_template_path.read_text(encoding="utf-8")
    )
    assert len(judged_template) == len(list_business_objective_packs())
    assert judged_template[0]["objective_pack_id"] == "minimize_enterprise_risk"


def test_business_evidence_and_objective_packs_use_future_email_evidence(
    tmp_path: Path,
) -> None:
    rosetta_dir = tmp_path / "rosetta"
    _write_rosetta_fixture(rosetta_dir)
    world = load_world(source="enron", rosetta_dir=rosetta_dir)
    branch_event = next(event for event in world.events if event.event_id == "evt-001")
    future_events = [
        event
        for event in world.events
        if event.thread_id == branch_event.thread_id
        and event.timestamp_ms > branch_event.timestamp_ms
    ]

    evidence = summarize_observed_evidence(
        branch_event=branch_event,
        future_events=future_events,
    )
    business = evidence_to_business_outcomes(evidence)
    risk_pack = get_business_objective_pack("minimize_enterprise_risk")
    velocity_pack = get_business_objective_pack("maintain_execution_velocity")
    risk_score = score_business_objective(
        pack=risk_pack,
        outcomes=business,
        evidence=evidence,
    )
    velocity_score = score_business_objective(
        pack=velocity_pack,
        outcomes=business,
        evidence=evidence,
    )

    assert evidence.participant_fanout >= 2
    assert evidence.review_loop_count >= 1
    assert evidence.time_to_first_follow_up_ms > 0
    assert 0.0 <= business.enterprise_risk <= 1.0
    assert 0.0 <= business.execution_drag <= 1.0
    assert risk_score.overall_score != velocity_score.overall_score


def test_branch_point_benchmark_judge_writes_rankings_and_audit_queue(
    tmp_path: Path,
    monkeypatch,
) -> None:
    rosetta_dir = tmp_path / "rosetta"
    _write_rosetta_fixture(rosetta_dir)
    world = load_world(source="enron", rosetta_dir=rosetta_dir)
    pack = _make_research_pack()

    monkeypatch.setattr(
        "vei.whatif.benchmark.get_research_pack",
        lambda _pack_id: pack,
    )
    build = build_branch_point_benchmark(
        world,
        artifacts_root=tmp_path / "benchmark_artifacts",
        label="fixture_benchmark_judge",
        heldout_pack_id="fixture_pack",
    )

    def fake_judge_prompt(*_args, **_kwargs) -> str:
        return json.dumps(
            {
                "pairwise_comparisons": [
                    {
                        "left_candidate_id": "legal_hold_internal",
                        "right_candidate_id": "narrow_external_status",
                        "preferred_candidate_id": "legal_hold_internal",
                        "confidence": 0.45,
                        "evidence_references": ["inside Enron only"],
                        "rationale": "keep the material inside",
                    },
                    {
                        "left_candidate_id": "legal_hold_internal",
                        "right_candidate_id": "broad_external_send",
                        "preferred_candidate_id": "legal_hold_internal",
                        "confidence": 0.45,
                        "evidence_references": ["broad outside send raises spread"],
                        "rationale": "narrower spread",
                    },
                    {
                        "left_candidate_id": "narrow_external_status",
                        "right_candidate_id": "broad_external_send",
                        "preferred_candidate_id": "narrow_external_status",
                        "confidence": 0.45,
                        "evidence_references": ["status note avoids the draft"],
                        "rationale": "smaller external footprint",
                    },
                ],
                "confidence": 0.45,
                "evidence_references": ["thread stays narrower"],
                "notes": "low confidence on purpose",
            }
        )

    monkeypatch.setattr(
        "vei.whatif.benchmark.run_llm_judge_prompt",
        fake_judge_prompt,
    )

    result = judge_branch_point_benchmark(build.artifacts.root)
    loaded = load_branch_point_benchmark_judge_result(build.artifacts.root)

    assert len(result.judgments) == len(list_business_objective_packs())
    assert len(result.audit_queue) == len(result.judgments)
    assert result.artifacts.result_path.exists()
    assert result.artifacts.audit_queue_path.exists()
    assert loaded.judgments[0].ordered_candidate_ids[0] == "legal_hold_internal"
    assert loaded.audit_queue[0].status == "pending"


def test_branch_point_benchmark_train_and_eval_round_trip_with_stub_runtime(
    tmp_path: Path,
    monkeypatch,
) -> None:
    rosetta_dir = tmp_path / "rosetta"
    _write_rosetta_fixture(rosetta_dir)
    world = load_world(source="enron", rosetta_dir=rosetta_dir)
    pack = _make_research_pack()

    monkeypatch.setattr(
        "vei.whatif.benchmark.get_research_pack",
        lambda _pack_id: pack,
    )
    build = build_branch_point_benchmark(
        world,
        artifacts_root=tmp_path / "benchmark_artifacts",
        label="fixture_benchmark_eval",
        heldout_pack_id="fixture_pack",
    )

    def fake_train_runtime(**kwargs) -> WhatIfBenchmarkTrainResult:
        model_root = build.artifacts.root / "model_runs" / kwargs["model_id"]
        model_root.mkdir(parents=True, exist_ok=True)
        result = WhatIfBenchmarkTrainResult(
            model_id=kwargs["model_id"],
            dataset_root=build.dataset.root,
            train_loss=0.21,
            validation_loss=0.29,
            epoch_count=kwargs["epochs"],
            train_row_count=1,
            validation_row_count=0,
            notes=["stub runtime"],
            artifacts=WhatIfBenchmarkTrainArtifacts(
                root=model_root,
                model_path=model_root / "model.pt",
                metadata_path=model_root / "metadata.json",
                train_result_path=model_root / "train_result.json",
            ),
        )
        result.artifacts.model_path.write_text("stub", encoding="utf-8")
        result.artifacts.metadata_path.write_text("{}", encoding="utf-8")
        result.artifacts.train_result_path.write_text(
            result.model_dump_json(indent=2),
            encoding="utf-8",
        )
        return result

    def fake_eval_runtime(**kwargs) -> WhatIfBenchmarkEvalResult:
        model_root = build.artifacts.root / "model_runs" / kwargs["model_id"]
        model_root.mkdir(parents=True, exist_ok=True)
        case = build.cases[0]
        objective_results: list[WhatIfCounterfactualObjectiveEvaluation] = []
        for objective_pack in list_business_objective_packs():
            candidate_predictions: list[WhatIfCounterfactualCandidatePrediction] = []
            for index, candidate in enumerate(case.candidates, start=1):
                evidence = WhatIfObservedEvidenceHeads(
                    outside_recipient_count=index - 1,
                    outside_forward_count=max(0, index - 2),
                    outside_attachment_spread_count=max(0, index - 2),
                    legal_follow_up_count=1 if index == 1 else 0,
                    review_loop_count=index,
                    markup_loop_count=max(0, index - 1),
                    executive_escalation_count=max(0, index - 2),
                    executive_mention_count=max(0, index - 2),
                    urgency_spike_count=max(0, index - 1),
                    participant_fanout=index + 1,
                    cc_expansion_count=index - 1,
                    cross_functional_loop_count=max(0, index - 1),
                    time_to_first_follow_up_ms=500 * index,
                    time_to_thread_end_ms=1_000 * index,
                    review_delay_burden_ms=750 * index,
                    reassurance_count=3 - index,
                    apology_repair_count=max(0, 2 - index),
                    commitment_clarity_count=index,
                    blame_pressure_count=max(0, index - 2),
                    internal_disagreement_count=max(0, index - 2),
                    attachment_recirculation_count=max(0, index - 2),
                    version_turn_count=index - 1,
                )
                business = evidence_to_business_outcomes(evidence)
                candidate_predictions.append(
                    WhatIfCounterfactualCandidatePrediction(
                        candidate=candidate,
                        expected_hypothesis=candidate.expected_hypotheses.get(
                            objective_pack.pack_id,
                            "middle_expected",
                        ),
                        rank=index,
                        predicted_evidence_heads=evidence,
                        predicted_business_outcomes=business,
                        predicted_objective_score=score_business_objective(
                            pack=objective_pack,
                            outcomes=business,
                            evidence=evidence,
                        ),
                    )
                )
            objective_results.append(
                WhatIfCounterfactualObjectiveEvaluation(
                    objective_pack=objective_pack,
                    recommended_candidate_label=candidate_predictions[
                        0
                    ].candidate.label,
                    candidates=candidate_predictions,
                    expected_order_ok=True,
                )
            )
        result = WhatIfBenchmarkEvalResult(
            model_id=kwargs["model_id"],
            dataset_root=build.dataset.root,
            observed_metrics=WhatIfObservedForecastMetrics(
                auroc_any_external_spread=0.82,
                brier_any_external_spread=0.11,
                calibration_error_any_external_spread=0.07,
                evidence_head_mae={
                    "outside_recipient_count": 0.4,
                    "review_loop_count": 0.6,
                },
                business_head_mae={
                    "enterprise_risk": 0.18,
                    "execution_drag": 0.12,
                },
                objective_score_mae={"minimize_enterprise_risk": 0.09},
            ),
            cases=[
                WhatIfBenchmarkCaseEvaluation(
                    case=case,
                    objectives=objective_results,
                )
            ],
            artifacts=WhatIfBenchmarkEvalArtifacts(
                root=model_root,
                eval_result_path=model_root / "eval_result.json",
                prediction_jsonl_path=model_root / "predictions.jsonl",
            ),
        )
        result.artifacts.prediction_jsonl_path.write_text("", encoding="utf-8")
        result.artifacts.eval_result_path.write_text(
            result.model_dump_json(indent=2),
            encoding="utf-8",
        )
        return result

    monkeypatch.setattr(
        "vei.whatif.benchmark.run_branch_point_benchmark_training",
        fake_train_runtime,
    )
    monkeypatch.setattr(
        "vei.whatif.benchmark.run_branch_point_benchmark_evaluation",
        fake_eval_runtime,
    )

    train_result = train_branch_point_benchmark_model(
        build.artifacts.root,
        model_id="jepa_latent",
        epochs=5,
    )
    assert train_result.validation_loss == 0.29
    loaded_train = load_branch_point_benchmark_train_result(
        build.artifacts.root,
        model_id="jepa_latent",
    )
    assert loaded_train.model_id == "jepa_latent"

    def fake_judge_prompt(*_args, **_kwargs) -> str:
        return json.dumps(
            {
                "pairwise_comparisons": [
                    {
                        "left_candidate_id": "legal_hold_internal",
                        "right_candidate_id": "narrow_external_status",
                        "preferred_candidate_id": "legal_hold_internal",
                        "confidence": 0.45,
                        "evidence_references": ["keep the draft inside"],
                        "rationale": "narrower risk surface",
                    },
                    {
                        "left_candidate_id": "legal_hold_internal",
                        "right_candidate_id": "broad_external_send",
                        "preferred_candidate_id": "legal_hold_internal",
                        "confidence": 0.45,
                        "evidence_references": ["broad send raises spread"],
                        "rationale": "more control",
                    },
                    {
                        "left_candidate_id": "narrow_external_status",
                        "right_candidate_id": "broad_external_send",
                        "preferred_candidate_id": "narrow_external_status",
                        "confidence": 0.45,
                        "evidence_references": ["status note avoids attachment spread"],
                        "rationale": "lighter external touch",
                    },
                ],
                "confidence": 0.45,
                "evidence_references": ["keep the scope narrow"],
                "notes": "fixture judgment",
            }
        )

    monkeypatch.setattr(
        "vei.whatif.benchmark.run_llm_judge_prompt",
        fake_judge_prompt,
    )
    judge_result = judge_branch_point_benchmark(build.artifacts.root)
    audit_records = [
        WhatIfAuditRecord(
            case_id=item.case_id,
            objective_pack_id=item.objective_pack_id,
            status="completed",
            ordered_candidate_ids=item.ordered_candidate_ids,
            reviewer_id="auditor-1",
        )
        for item in judge_result.audit_queue
    ]
    audit_path = tmp_path / "audit_records.json"
    audit_path.write_text(
        json.dumps([item.model_dump(mode="json") for item in audit_records], indent=2),
        encoding="utf-8",
    )

    eval_result = evaluate_branch_point_benchmark_model(
        build.artifacts.root,
        model_id="jepa_latent",
        judged_rankings_path=judge_result.artifacts.result_path,
        audit_records_path=audit_path,
    )
    loaded_eval = load_branch_point_benchmark_eval_result(
        build.artifacts.root,
        model_id="jepa_latent",
    )

    assert eval_result.dominance_summary.pass_rate == 1.0
    assert eval_result.judge_summary.available is True
    assert eval_result.judge_summary.judgment_count == len(
        list_business_objective_packs()
    )
    assert eval_result.audit_summary.available is True
    assert eval_result.audit_summary.completed_count == len(audit_records)
    assert loaded_eval.model_id == "jepa_latent"
    assert eval_result.artifacts.eval_result_path.exists()


def test_vei_whatif_cli_benchmark_commands(tmp_path: Path, monkeypatch) -> None:
    rosetta_dir = tmp_path / "rosetta"
    _write_rosetta_fixture(rosetta_dir)
    world = load_world(source="enron", rosetta_dir=rosetta_dir)
    pack = _make_research_pack()

    monkeypatch.setattr(
        "vei.whatif.benchmark.get_research_pack",
        lambda _pack_id: pack,
    )
    build = build_branch_point_benchmark(
        world,
        artifacts_root=tmp_path / "benchmark_artifacts",
        label="fixture_benchmark_cli",
        heldout_pack_id="fixture_pack",
    )
    train_result = WhatIfBenchmarkTrainResult(
        model_id="jepa_latent",
        dataset_root=build.dataset.root,
        train_loss=0.2,
        validation_loss=0.3,
        epoch_count=4,
        train_row_count=1,
        validation_row_count=0,
        artifacts=WhatIfBenchmarkTrainArtifacts(
            root=build.artifacts.root / "model_runs" / "jepa_latent",
            model_path=build.artifacts.root / "model_runs" / "jepa_latent" / "model.pt",
            metadata_path=build.artifacts.root
            / "model_runs"
            / "jepa_latent"
            / "metadata.json",
            train_result_path=build.artifacts.root
            / "model_runs"
            / "jepa_latent"
            / "train_result.json",
        ),
    )
    eval_result = WhatIfBenchmarkEvalResult(
        model_id="jepa_latent",
        dataset_root=build.dataset.root,
        observed_metrics=WhatIfObservedForecastMetrics(
            auroc_any_external_spread=0.8,
            brier_any_external_spread=0.1,
            calibration_error_any_external_spread=0.05,
            evidence_head_mae={"outside_recipient_count": 0.2},
            business_head_mae={"enterprise_risk": 0.1},
        ),
        artifacts=WhatIfBenchmarkEvalArtifacts(
            root=build.artifacts.root / "model_runs" / "jepa_latent",
            eval_result_path=build.artifacts.root
            / "model_runs"
            / "jepa_latent"
            / "eval_result.json",
            prediction_jsonl_path=build.artifacts.root
            / "model_runs"
            / "jepa_latent"
            / "predictions.jsonl",
        ),
    )

    monkeypatch.setattr(
        "vei.cli.vei_whatif.build_branch_point_benchmark",
        lambda *args, **kwargs: build,
    )
    monkeypatch.setattr(
        "vei.cli.vei_whatif.train_branch_point_benchmark_model",
        lambda *args, **kwargs: train_result,
    )
    monkeypatch.setattr(
        "vei.cli.vei_whatif.evaluate_branch_point_benchmark_model",
        lambda *args, **kwargs: eval_result,
    )
    monkeypatch.setattr(
        "vei.cli.vei_whatif.load_branch_point_benchmark_build_result",
        lambda *_args, **_kwargs: build,
    )
    judge_result = WhatIfBenchmarkJudgeResult(
        build_root=build.artifacts.root,
        judge_model="gpt-4.1-mini",
        judgments=[],
        audit_queue=[],
        artifacts=WhatIfBenchmarkJudgeArtifacts(
            root=build.artifacts.root,
            result_path=build.artifacts.root / "judge_result.json",
            audit_queue_path=build.artifacts.root / "audit_queue.json",
        ),
    )
    monkeypatch.setattr(
        "vei.cli.vei_whatif.load_branch_point_benchmark_train_result",
        lambda *_args, **_kwargs: train_result,
    )
    monkeypatch.setattr(
        "vei.cli.vei_whatif.load_branch_point_benchmark_eval_result",
        lambda *_args, **_kwargs: eval_result,
    )
    monkeypatch.setattr(
        "vei.cli.vei_whatif.judge_branch_point_benchmark",
        lambda *_args, **_kwargs: judge_result,
    )
    monkeypatch.setattr(
        "vei.cli.vei_whatif.load_branch_point_benchmark_judge_result",
        lambda *_args, **_kwargs: judge_result,
    )

    runner = CliRunner()
    models_result = runner.invoke(
        cli_app,
        ["whatif", "benchmark", "models", "--format", "markdown"],
    )
    assert models_result.exit_code == 0, models_result.output
    assert "jepa_latent" in models_result.output

    build_result = runner.invoke(
        cli_app,
        [
            "whatif",
            "benchmark",
            "build",
            "--rosetta-dir",
            str(rosetta_dir),
            "--label",
            "fixture_cli_benchmark",
            "--heldout-pack-id",
            "fixture_pack",
        ],
    )
    assert build_result.exit_code == 0, build_result.output
    assert json.loads(build_result.output)["label"] == "fixture_benchmark_cli"

    train_cli_result = runner.invoke(
        cli_app,
        [
            "whatif",
            "benchmark",
            "train",
            "--root",
            str(build.artifacts.root),
            "--model-id",
            "jepa_latent",
        ],
    )
    assert train_cli_result.exit_code == 0, train_cli_result.output
    assert json.loads(train_cli_result.output)["model_id"] == "jepa_latent"

    judge_cli_result = runner.invoke(
        cli_app,
        [
            "whatif",
            "benchmark",
            "judge",
            "--root",
            str(build.artifacts.root),
        ],
    )
    assert judge_cli_result.exit_code == 0, judge_cli_result.output
    assert json.loads(judge_cli_result.output)["judge_model"] == "gpt-4.1-mini"

    eval_cli_result = runner.invoke(
        cli_app,
        [
            "whatif",
            "benchmark",
            "eval",
            "--root",
            str(build.artifacts.root),
            "--model-id",
            "jepa_latent",
            "--format",
            "markdown",
        ],
    )
    assert eval_cli_result.exit_code == 0, eval_cli_result.output
    assert "Observed Future Forecasting" in eval_cli_result.output

    show_judge_result = runner.invoke(
        cli_app,
        [
            "whatif",
            "benchmark",
            "show-judge",
            "--root",
            str(build.artifacts.root),
            "--format",
            "markdown",
        ],
    )
    assert show_judge_result.exit_code == 0, show_judge_result.output
    assert "Judged case-objectives" in show_judge_result.output
