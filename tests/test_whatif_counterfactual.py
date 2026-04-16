from __future__ import annotations

import asyncio
import json
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from vei.llm.providers import PlanResult, PlanUsage
from vei.project_settings import default_model_for_provider
from vei.whatif import (
    estimate_counterfactual_delta,
    load_experiment_result,
    load_ranked_experiment_result,
    load_world,
    materialize_episode,
    run_counterfactual_experiment,
    run_llm_counterfactual,
    run_ranked_counterfactual_experiment,
)
from vei.whatif.experiment import (
    run_counterfactual_experiment as run_counterfactual_experiment_module,
)
from vei.whatif.models import (
    WhatIfCandidateIntervention,
    WhatIfCounterfactualEstimateDelta,
    WhatIfCounterfactualEstimateResult,
    WhatIfEventReference,
    WhatIfHistoricalScore,
    WhatIfLLMGeneratedMessage,
    WhatIfLLMReplayResult,
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


def _write_mail_archive_fixture(root: Path) -> Path:
    root.mkdir(parents=True, exist_ok=True)
    archive_path = root / "context_snapshot.json"
    archive_path.write_text(
        json.dumps(
            {
                "organization_name": "Py Corp",
                "organization_domain": "pycorp.example.com",
                "captured_at": "2026-03-01T09:15:00Z",
                "threads": [
                    {
                        "thread_id": "py-legal-001",
                        "subject": "Pricing addendum",
                        "category": "historical",
                        "messages": [
                            {
                                "message_id": "py-msg-001",
                                "from": "emma@pycorp.example.com",
                                "to": "legal@pycorp.example.com",
                                "subject": "Pricing addendum",
                                "body_text": "Please review before we send this draft to Redwood.",
                                "timestamp": "2026-03-01T09:00:00Z",
                            },
                            {
                                "message_id": "py-msg-002",
                                "from": "legal@pycorp.example.com",
                                "to": "emma@pycorp.example.com",
                                "subject": "Re: Pricing addendum",
                                "body_text": "Hold for one markup round. Counsel wants one more pass.",
                                "timestamp": "2026-03-01T09:05:00Z",
                            },
                            {
                                "message_id": "py-msg-003",
                                "from": "emma@pycorp.example.com",
                                "to": "partner@redwoodcapital.com",
                                "subject": "Pricing addendum",
                                "body_text": "Sharing the draft addendum now.",
                                "timestamp": "2026-03-01T09:10:00Z",
                                "has_attachment_reference": True,
                            },
                        ],
                    }
                ],
                "actors": [
                    {
                        "actor_id": "emma@pycorp.example.com",
                        "email": "emma@pycorp.example.com",
                        "display_name": "Emma Rowan",
                    },
                    {
                        "actor_id": "legal@pycorp.example.com",
                        "email": "legal@pycorp.example.com",
                        "display_name": "Legal Team",
                    },
                ],
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    return archive_path


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
) -> WhatIfCounterfactualEstimateResult:
    baseline = WhatIfHistoricalScore(
        backend="historical",
        future_event_count=2,
        future_external_event_count=1,
        risk_score=0.6,
    )
    predicted = WhatIfHistoricalScore(
        backend="e_jepa_proxy",
        future_event_count=future_event_count,
        future_external_event_count=future_external_event_count,
        risk_score=risk_score,
    )
    return WhatIfCounterfactualEstimateResult(
        status="ok",
        backend="e_jepa_proxy",
        prompt=prompt,
        summary=summary,
        baseline=baseline,
        predicted=predicted,
        delta=WhatIfCounterfactualEstimateDelta(
            risk_score_delta=round(risk_score - baseline.risk_score, 3),
            future_event_delta=future_event_count - baseline.future_event_count,
            external_event_delta=(
                future_external_event_count - baseline.future_external_event_count
            ),
        ),
    )


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
    forecast_result = estimate_counterfactual_delta(
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
    assert forecast_result.business_state_change is not None
    assert forecast_result.business_state_change.summary
    assert experiment.llm_result is not None
    assert experiment.llm_result.status == "ok"
    assert experiment.forecast_result is not None
    assert experiment.forecast_result.business_state_change is not None
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


def test_llm_counterfactual_runs_inside_existing_event_loop(
    tmp_path: Path,
    monkeypatch,
) -> None:
    rosetta_dir = tmp_path / "rosetta"
    _write_rosetta_fixture(rosetta_dir)
    world = load_world(source="enron", rosetta_dir=rosetta_dir)
    workspace_root = tmp_path / "episode_nested_loop"
    materialize_episode(world, root=workspace_root, thread_id="thr-legal-trading")

    async def fake_plan_once_with_usage(**_: object) -> PlanResult:
        return PlanResult(
            plan={
                "tool": "emit_counterfactual",
                "args": {
                    "summary": "Sara pauses the handoff.",
                    "messages": [
                        {
                            "actor_id": "sara.shackleton@enron.com",
                            "to": "mark.taylor@enron.com",
                            "subject": "Re: Gas Position Limits",
                            "body_text": "Pause the forward until review completes.",
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

    async def invoke() -> WhatIfLLMReplayResult:
        return run_llm_counterfactual(
            workspace_root,
            prompt="Keep this internal and pause the handoff.",
        )

    result = asyncio.run(invoke())

    assert result.status == "ok"
    assert result.messages[0].to == "mark.taylor@enron.com"


def test_llm_counterfactual_returns_error_result_when_provider_fails(
    tmp_path: Path,
    monkeypatch,
) -> None:
    rosetta_dir = tmp_path / "rosetta"
    _write_rosetta_fixture(rosetta_dir)
    world = load_world(source="enron", rosetta_dir=rosetta_dir)
    workspace_root = tmp_path / "episode_provider_error"
    materialize_episode(world, root=workspace_root, thread_id="thr-legal-trading")

    async def fake_plan_once_with_usage(**_: object) -> PlanResult:
        raise RuntimeError("provider unavailable")

    monkeypatch.setattr(
        "vei.whatif.api.providers.plan_once_with_usage",
        fake_plan_once_with_usage,
    )

    result = run_llm_counterfactual(
        workspace_root,
        prompt="Pause the handoff until review finishes.",
    )

    assert result.status == "error"
    assert result.error == "provider unavailable"
    assert result.summary == "LLM counterfactual generation failed."


def test_llm_counterfactual_returns_error_result_for_empty_messages(
    tmp_path: Path,
    monkeypatch,
) -> None:
    rosetta_dir = tmp_path / "rosetta"
    _write_rosetta_fixture(rosetta_dir)
    world = load_world(source="enron", rosetta_dir=rosetta_dir)
    workspace_root = tmp_path / "episode_empty_messages"
    materialize_episode(world, root=workspace_root, thread_id="thr-legal-trading")

    async def fake_plan_once_with_usage(**_: object) -> PlanResult:
        return PlanResult(
            plan={
                "tool": "emit_counterfactual",
                "args": {
                    "summary": "No usable continuation.",
                    "messages": [],
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
        prompt="Pause the handoff until review finishes.",
    )

    assert result.status == "error"
    assert result.error == "LLM returned no usable messages"


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
) -> WhatIfCounterfactualEstimateResult:
    baseline = WhatIfHistoricalScore(
        backend="historical",
        future_event_count=2,
        future_external_event_count=1,
        risk_score=0.6,
    )
    predicted = WhatIfHistoricalScore(
        backend="e_jepa_proxy",
        future_event_count=future_event_count,
        future_external_event_count=future_external_event_count,
        risk_score=risk_score,
    )
    return WhatIfCounterfactualEstimateResult(
        status="ok",
        backend="e_jepa_proxy",
        prompt=prompt,
        summary=summary,
        baseline=baseline,
        predicted=predicted,
        delta=WhatIfCounterfactualEstimateDelta(
            risk_score_delta=round(risk_score - baseline.risk_score, 3),
            future_event_delta=future_event_count - baseline.future_event_count,
            external_event_delta=(
                future_external_event_count - baseline.future_external_event_count
            ),
        ),
    )


def test_split_experiment_module_writes_counterfactual_artifacts(
    tmp_path: Path,
    monkeypatch,
) -> None:
    rosetta_dir = tmp_path / "rosetta_experiment_split"
    _write_rosetta_fixture(rosetta_dir)
    world = load_world(source="enron", rosetta_dir=rosetta_dir)

    monkeypatch.setattr(
        "vei.whatif.experiment.run_llm_counterfactual",
        lambda *_args, prompt, **_kwargs: _make_llm_replay_result(
            prompt=prompt,
            to="mark.taylor@enron.com",
            subject="Re: Gas Position Limits",
            body_text="Please keep this internal while compliance reviews it.",
            delay_ms=1000,
            summary="The thread stays inside Enron.",
        ),
    )
    monkeypatch.setattr(
        "vei.whatif.experiment.estimate_counterfactual_delta",
        lambda *_args, prompt, **_kwargs: _make_forecast_result(
            prompt=prompt,
            risk_score=0.2,
            future_event_count=2,
            future_external_event_count=0,
            summary="Proxy forecast completed through the split experiment module.",
        ),
    )

    experiment = run_counterfactual_experiment_module(
        world,
        artifacts_root=tmp_path / "split_experiment_artifacts",
        label="split_module_hold",
        counterfactual_prompt="Keep this internal and pause the forward.",
        selection_scenario="compliance_gateway",
        mode="both",
    )

    assert experiment.llm_result is not None
    assert experiment.llm_result.status == "ok"
    assert experiment.forecast_result is not None
    assert experiment.forecast_result.status == "ok"
    assert experiment.artifacts.result_json_path.exists()
    assert experiment.artifacts.overview_markdown_path.exists()


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
    ) -> WhatIfCounterfactualEstimateResult:
        assert kwargs["epochs"] == 2
        assert kwargs["batch_size"] == 16
        assert kwargs["force_retrain"] is True
        assert kwargs["device"] == "cpu"
        return WhatIfCounterfactualEstimateResult(
            status="ok",
            backend="e_jepa",
            prompt="Keep this internal.",
            summary="Real E-JEPA forecast completed.",
            baseline=WhatIfHistoricalScore(
                backend="historical",
                future_event_count=1,
                future_external_event_count=1,
                risk_score=0.5,
            ),
            predicted=WhatIfHistoricalScore(
                backend="e_jepa",
                future_event_count=1,
                future_external_event_count=0,
                risk_score=0.2,
            ),
            delta=WhatIfCounterfactualEstimateDelta(
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
        "vei.whatif.experiment.run_ejepa_counterfactual",
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


def test_counterfactual_experiment_can_use_ejepa_backend_for_generic_archive(
    tmp_path: Path,
    monkeypatch,
) -> None:
    archive_path = _write_mail_archive_fixture(tmp_path / "mail_archive_ejepa")
    world = load_world(source="mail_archive", source_dir=archive_path)

    async def fake_plan_once_with_usage(**_: object) -> PlanResult:
        return PlanResult(
            plan={
                "tool": "emit_counterfactual",
                "args": {
                    "summary": "Hold the outside send and keep the draft inside Py Corp.",
                    "messages": [
                        {
                            "actor_id": "emma@pycorp.example.com",
                            "to": "legal@pycorp.example.com",
                            "subject": "Re: Pricing addendum",
                            "body_text": "Keep this internal until legal clears it.",
                            "delay_ms": 1000,
                        }
                    ],
                },
            },
            usage=PlanUsage(provider="openai", model="gpt-5"),
        )

    captured: dict[str, str] = {}

    def fake_run_ejepa_counterfactual(
        *_: object, **kwargs: object
    ) -> WhatIfCounterfactualEstimateResult:
        captured["source"] = str(kwargs["source"])
        captured["source_dir"] = str(kwargs["source_dir"])
        return WhatIfCounterfactualEstimateResult(
            status="ok",
            backend="e_jepa",
            prompt="Keep this internal.",
            summary="Generic E-JEPA forecast completed.",
            baseline=WhatIfHistoricalScore(
                backend="historical",
                future_event_count=1,
                future_external_event_count=1,
                risk_score=0.5,
            ),
            predicted=WhatIfHistoricalScore(
                backend="e_jepa",
                future_event_count=1,
                future_external_event_count=0,
                risk_score=0.2,
            ),
            delta=WhatIfCounterfactualEstimateDelta(
                risk_score_delta=-0.3,
                external_event_delta=-1,
            ),
            branch_event=WhatIfEventReference(
                event_id="py-msg-002",
                timestamp="2026-03-01T09:05:00Z",
                actor_id="legal@pycorp.example.com",
                event_type="reply",
                thread_id="py-legal-001",
                subject="Re: Pricing addendum",
            ),
        )

    monkeypatch.setattr(
        "vei.whatif.api.providers.plan_once_with_usage",
        fake_plan_once_with_usage,
    )
    monkeypatch.setattr(
        "vei.whatif.experiment.run_ejepa_counterfactual",
        fake_run_ejepa_counterfactual,
    )

    experiment = run_counterfactual_experiment(
        world,
        artifacts_root=tmp_path / "generic_artifacts",
        label="pycorp_ejepa_hold",
        counterfactual_prompt="Keep this internal.",
        event_id="py-msg-002",
        mode="both",
        forecast_backend="e_jepa",
    )

    assert captured["source"] == "mail_archive"
    assert Path(captured["source_dir"]) == archive_path.resolve()
    assert experiment.forecast_result is not None
    assert experiment.forecast_result.backend == "e_jepa"


def test_run_ranked_counterfactual_experiment_writes_artifacts_and_keeps_shadow_mode(
    tmp_path: Path,
    monkeypatch,
) -> None:
    rosetta_dir = tmp_path / "rosetta"
    _write_rosetta_fixture(rosetta_dir)
    world = load_world(source="enron", rosetta_dir=rosetta_dir)
    expected_model = default_model_for_provider("openai")

    def fake_run_llm_counterfactual(
        *_: object,
        prompt: str,
        provider: str = "openai",
        model: str = "gpt-5",
        seed: int = 42042,
    ) -> WhatIfLLMReplayResult:
        assert provider == "openai"
        assert model == expected_model
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
    ) -> WhatIfCounterfactualEstimateResult:
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
        "vei.whatif.experiment.run_llm_counterfactual",
        fake_run_llm_counterfactual,
    )
    monkeypatch.setattr(
        "vei.whatif.experiment.estimate_counterfactual_delta",
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
    assert result.candidates[0].business_state_change is not None
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


def test_run_ranked_counterfactual_experiment_can_use_ejepa_shadow_for_generic_archive(
    tmp_path: Path,
    monkeypatch,
) -> None:
    archive_path = _write_mail_archive_fixture(tmp_path / "mail_archive_ranked")
    world = load_world(source="mail_archive", source_dir=archive_path)

    def fake_run_llm_counterfactual(
        *_: object,
        prompt: str,
        provider: str = "openai",
        model: str = "gpt-5",
        seed: int = 42042,
    ) -> WhatIfLLMReplayResult:
        assert provider == "openai"
        assert model == default_model_for_provider("openai")
        if "internal" in prompt.lower():
            return _make_llm_replay_result(
                prompt=prompt,
                to="legal@pycorp.example.com",
                subject="Re: Pricing addendum",
                body_text="Keep this internal until legal signs off.",
                delay_ms=1000 + (seed % 3) * 100,
                summary="The draft stays inside Py Corp.",
            )
        return _make_llm_replay_result(
            prompt=prompt,
            to="partner@redwoodcapital.com",
            subject="Pricing addendum",
            body_text="Sending the draft outside now.",
            delay_ms=8000 + (seed % 3) * 100,
            summary="The draft leaves Py Corp immediately.",
        )

    captured_sources: list[str] = []

    def fake_run_ejepa_counterfactual(
        *_: object,
        prompt: str,
        **kwargs: object,
    ) -> WhatIfCounterfactualEstimateResult:
        captured_sources.append(str(kwargs["source"]))
        if "internal" in prompt.lower():
            return _make_forecast_result(
                prompt=prompt,
                risk_score=0.2,
                future_event_count=2,
                future_external_event_count=0,
                summary="Generic E-JEPA prefers the internal hold.",
            ).model_copy(update={"backend": "e_jepa"})
        return _make_forecast_result(
            prompt=prompt,
            risk_score=0.8,
            future_event_count=2,
            future_external_event_count=1,
            summary="Generic E-JEPA expects the draft to leave the company.",
        ).model_copy(update={"backend": "e_jepa"})

    monkeypatch.setattr(
        "vei.whatif.experiment.run_llm_counterfactual",
        fake_run_llm_counterfactual,
    )
    monkeypatch.setattr(
        "vei.whatif.experiment.run_ejepa_counterfactual",
        fake_run_ejepa_counterfactual,
    )

    result = run_ranked_counterfactual_experiment(
        world,
        artifacts_root=tmp_path / "ranked_generic_artifacts",
        label="pycorp_ranked",
        objective_pack_id="contain_exposure",
        candidate_interventions=[
            "Keep this internal and pause.",
            "Send the draft now.",
        ],
        event_id="py-msg-002",
        rollout_count=1,
        shadow_forecast_backend="e_jepa",
    )

    assert captured_sources == ["mail_archive", "mail_archive"]
    assert all(candidate.shadow is not None for candidate in result.candidates)
    assert all(candidate.shadow.backend == "e_jepa" for candidate in result.candidates)


def test_run_ranked_counterfactual_experiment_validates_artifacts(
    tmp_path: Path,
    monkeypatch,
) -> None:
    archive_path = _write_mail_archive_fixture(
        tmp_path / "mail_archive_ranked_validate"
    )
    world = load_world(source="mail_archive", source_dir=archive_path)

    def fake_run_llm_counterfactual(
        *_: object,
        prompt: str,
        provider: str = "openai",
        model: str = "gpt-5",
        seed: int = 42042,
    ) -> WhatIfLLMReplayResult:
        del provider, model
        return _make_llm_replay_result(
            prompt=prompt,
            to="legal@pycorp.example.com",
            subject="Re: Pricing addendum",
            body_text="Keep this internal until legal signs off.",
            delay_ms=1000 + (seed % 3) * 100,
            summary="The draft stays inside Py Corp.",
        )

    def fake_estimate_counterfactual_delta(
        *_: object,
        prompt: str,
        **__: object,
    ) -> WhatIfCounterfactualEstimateResult:
        return _make_forecast_result(
            prompt=prompt,
            risk_score=0.2,
            future_event_count=2,
            future_external_event_count=0,
            summary="Proxy forecast prefers internal hold.",
        )

    monkeypatch.setattr(
        "vei.whatif.experiment.run_llm_counterfactual",
        fake_run_llm_counterfactual,
    )
    monkeypatch.setattr(
        "vei.whatif.experiment.estimate_counterfactual_delta",
        fake_estimate_counterfactual_delta,
    )
    monkeypatch.setattr(
        "vei.whatif.experiment.validate_artifact_tree",
        lambda _root: ["invalid artifact fixture"],
    )

    with pytest.raises(
        ValueError,
        match="ranked experiment artifact validation failed",
    ):
        run_ranked_counterfactual_experiment(
            world,
            artifacts_root=tmp_path / "ranked_artifacts",
            label="ranked_validate",
            objective_pack_id="contain_exposure",
            candidate_interventions=["Keep this internal and pause."],
            event_id="py-msg-002",
            rollout_count=1,
            shadow_forecast_backend="e_jepa_proxy",
        )
