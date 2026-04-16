from __future__ import annotations

import json
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
from typer.testing import CliRunner

from vei.cli.vei import app as cli_app
from vei.dynamics.models import DynamicsResponse
from vei.llm.providers import PlanResult, PlanUsage
from vei.whatif import (
    list_objective_packs,
    load_world,
)
from vei.whatif.experiment import (
    run_counterfactual_experiment,
    run_ranked_counterfactual_experiment,
)
from vei.whatif.ejepa import _default_cache_root
from vei.whatif.models import (
    WhatIfCounterfactualEstimateDelta,
    WhatIfCounterfactualEstimateResult,
    WhatIfEventReference,
    WhatIfHistoricalScore,
    WhatIfLLMGeneratedMessage,
    WhatIfLLMReplayResult,
)
from vei.whatif.ranking import (
    score_outcome_signals,
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
        backend="heuristic_baseline",
        future_event_count=future_event_count,
        future_external_event_count=future_external_event_count,
        risk_score=risk_score,
    )
    return WhatIfCounterfactualEstimateResult(
        status="ok",
        backend="heuristic_baseline",
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
        "vei.whatif.experiment.run_llm_counterfactual",
        fake_run_llm_counterfactual,
    )
    monkeypatch.setattr(
        "vei.whatif.dynamics_bridge.get_backend",
        lambda name: type(
            "StubBackend",
            (),
            {
                "forecast": lambda self, request: DynamicsResponse(
                    backend_id=name,
                    backend_version="test",
                    state_delta_summary={
                        "whatif_result": _make_forecast_result(
                            prompt=request.company_graph_slice.metadata["whatif"][
                                "prompt"
                            ],
                            risk_score=0.3,
                            future_event_count=1,
                            future_external_event_count=0,
                            summary="Shadow forecast completed.",
                        ).model_dump(mode="json")
                    },
                )
            },
        )(),
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
    assert payload["candidates"][0]["shadow"]["backend"] == "heuristic_baseline"

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


def test_run_counterfactual_experiment_uses_dynamics_backend(
    tmp_path: Path,
    monkeypatch,
) -> None:
    rosetta_dir = tmp_path / "rosetta"
    _write_rosetta_fixture(rosetta_dir)
    world = load_world(source="enron", rosetta_dir=rosetta_dir)
    called_backends: list[str] = []

    class StubBackend:
        def __init__(self, name: str) -> None:
            self._name = name

        def forecast(self, request) -> DynamicsResponse:
            del request
            called_backends.append(self._name)
            return DynamicsResponse(
                backend_id=self._name,
                backend_version="test",
                state_delta_summary={
                    "whatif_result": _make_forecast_result(
                        prompt="Keep this internal.",
                        risk_score=0.2,
                        future_event_count=1,
                        future_external_event_count=0,
                        summary="Dynamics backend handled the forecast.",
                    ).model_dump(mode="json")
                },
            )

    monkeypatch.setattr(
        "vei.whatif.dynamics_bridge.get_backend",
        lambda name: StubBackend(name),
    )

    result = run_counterfactual_experiment(
        world,
        artifacts_root=tmp_path / "artifacts",
        label="dyn_main",
        counterfactual_prompt="Keep this internal.",
        selection_scenario="external_dlp",
        mode="heuristic_baseline",
    )

    assert called_backends == ["heuristic_baseline"]
    assert result.forecast_result is not None
    assert result.forecast_result.summary == "Dynamics backend handled the forecast."


def test_run_ranked_counterfactual_experiment_uses_dynamics_backend(
    tmp_path: Path,
    monkeypatch,
) -> None:
    rosetta_dir = tmp_path / "rosetta"
    _write_rosetta_fixture(rosetta_dir)
    world = load_world(source="enron", rosetta_dir=rosetta_dir)
    called_backends: list[str] = []

    class StubBackend:
        def __init__(self, name: str) -> None:
            self._name = name

        def forecast(self, request) -> DynamicsResponse:
            del request
            called_backends.append(self._name)
            return DynamicsResponse(
                backend_id=self._name,
                backend_version="test",
                state_delta_summary={
                    "whatif_result": _make_forecast_result(
                        prompt="Keep this internal and pause.",
                        risk_score=0.2,
                        future_event_count=1,
                        future_external_event_count=0,
                        summary="Dynamics shadow forecast completed.",
                    ).model_dump(mode="json")
                },
            )

    monkeypatch.setattr(
        "vei.whatif.dynamics_bridge.get_backend",
        lambda name: StubBackend(name),
    )
    monkeypatch.setattr(
        "vei.whatif.experiment.run_llm_counterfactual",
        lambda *_args, prompt, **_kwargs: _make_llm_replay_result(
            prompt=prompt,
            to="jeff.skilling@enron.com",
            subject="Please hold for review",
            body_text="Keep this inside while legal reviews it.",
            delay_ms=1000,
            summary="Internal review replaces the outside send.",
        ),
    )

    result = run_ranked_counterfactual_experiment(
        world,
        artifacts_root=tmp_path / "artifacts_ranked",
        label="dyn_ranked",
        objective_pack_id="contain_exposure",
        candidate_interventions=[
            "Keep this internal and pause.",
            "Send the draft outside now.",
        ],
        event_id="evt-005",
        rollout_count=1,
        shadow_forecast_backend="heuristic_baseline",
    )

    assert called_backends == ["heuristic_baseline", "heuristic_baseline"]
    assert result.candidates[0].shadow is not None
    assert (
        result.candidates[0].shadow.forecast_result.summary
        == "Dynamics shadow forecast completed."
    )
