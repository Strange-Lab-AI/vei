from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any

from typer.testing import CliRunner

from vei.cli import whatif_benchmark as benchmark_cli
from vei.cli.vei import app as cli_app
from vei.context.canonical_history import CanonicalHistoryReadinessReport
from vei.whatif.corpus import build_thread_summaries
from vei.whatif.models import (
    WhatIfArtifactFlags,
    WhatIfEvent,
    WhatIfWorld,
    WhatIfWorldSummary,
)
from vei.whatif.news_state_points import (
    NewsStatePointCandidateInput,
    NewsStatePointRunArtifacts,
    NewsStatePointRunResult,
    _infer_candidate_type,
    build_news_state_point,
    run_news_state_point_counterfactual,
)
from vei.whatif import strategic_state_points as strategic_module
from vei.whatif.strategic_state_points import (
    StrategicStatePointArtifacts,
    StrategicStatePointRunResult,
    StrategicStatePointSource,
    build_strategic_state_point_proposal_prompt,
    run_strategic_state_point_counterfactuals,
)


def test_news_state_point_builds_as_of_dossier_without_branch_event() -> None:
    world = _news_world()

    state_point = build_news_state_point(
        world,
        topic="banking_markets",
        as_of="1837-09-06",
        future_horizon_days=90,
        max_history_events=20,
        max_evidence_events=6,
    )

    assert state_point.branch_event.event_id == "news_state:banking_markets:1837-09-06"
    assert state_point.branch_event.event_id not in {
        event.event_id for event in world.events
    }
    assert all(
        event.timestamp <= "1837-09-06T00:00:00" for event in state_point.history_events
    )
    assert all(
        event.timestamp > "1837-09-06T00:00:00" for event in state_point.future_events
    )
    assert all(
        "labor-sermon" not in event.event_id for event in state_point.history_events
    )
    assert "future bill passage marker" not in state_point.state_summary
    assert all(
        "future bill passage marker" not in event.snippet
        for event in state_point.evidence_events
    )
    assert "Recurring signals include" in state_point.state_summary


def test_news_state_point_candidate_type_inference_handles_policy_events() -> None:
    assert (
        _infer_candidate_type(
            "Congress delays banking reform; publish a risk warning on credit."
        )
        == "commercial_reset"
    )
    assert (
        _infer_candidate_type("Do not publish until source review is complete.")
        == "hold_compliance_review"
    )


def test_news_state_point_run_writes_human_candidates_and_scores(
    tmp_path: Path,
    monkeypatch,
) -> None:
    world = _news_world()

    def fake_predict(**kwargs: Any) -> list[dict[str, Any]]:
        return [
            {
                "evidence_heads": {
                    "outside_recipient_count": 2,
                    "participant_fanout": 4,
                },
                "business_heads": {
                    "enterprise_risk": 0.25,
                    "commercial_position_proxy": 0.54,
                    "org_strain_proxy": 0.20,
                    "stakeholder_trust": 0.44,
                    "execution_drag": 0.32,
                },
                "future_state_heads": {
                    "regulatory_exposure": 0.21,
                    "accounting_control_pressure": 0.03,
                    "liquidity_stress": 0.62,
                    "governance_response": 0.15,
                    "evidence_control": 0.33,
                    "external_confidence_pressure": 0.71,
                },
                "objective_scores": {
                    "minimize_enterprise_risk": 0.70,
                    "protect_commercial_position": 0.54,
                    "reduce_org_strain": 0.68,
                    "preserve_stakeholder_trust": 0.44,
                    "maintain_execution_velocity": 0.52,
                },
            }
            for _row in kwargs["rows"]
        ]

    monkeypatch.setattr(
        "vei.whatif.news_state_points.run_branch_point_benchmark_predictions",
        fake_predict,
    )

    result = run_news_state_point_counterfactual(
        world,
        checkpoint_path=tmp_path / "model.pt",
        artifacts_root=tmp_path / "state_points",
        label="banking_bill_fixture",
        topic="banking_markets",
        as_of="1837-09-06",
        candidates=[
            NewsStatePointCandidateInput(
                label="Banking bill passes",
                action="A new banking bill passes; issue a public economy memo.",
                candidate_type="commercial_reset",
            )
        ],
    )

    payload = json.loads(result.artifacts.result_json_path.read_text(encoding="utf-8"))
    candidate = payload["candidates"][0]
    assert result.state_event_id == "news_state:banking_markets:1837-09-06"
    assert payload["state_point"]["state_point_not_historical_branch_event"] is True
    assert payload["state_point"]["no_future_context_for_state"] is True
    assert candidate["generation_source"] == "human"
    assert candidate["no_future_context"] is True
    assert candidate["candidate_type"] == "commercial_reset"
    assert candidate["future_state_heads"]["liquidity_stress"] == 0.62


def test_news_state_point_cli_accepts_human_candidates(
    tmp_path: Path,
    monkeypatch,
) -> None:
    world = _news_world()

    def fake_load_world(
        *,
        source: str,
        source_dir: Path,
        include_situation_graph: bool = True,
    ) -> WhatIfWorld:
        assert source == "company_history"
        assert include_situation_graph is False
        return world

    monkeypatch.setattr(benchmark_cli, "load_world", fake_load_world)
    monkeypatch.setattr(
        benchmark_cli,
        "build_canonical_history_readiness",
        lambda _path: CanonicalHistoryReadinessReport(
            available=True,
            readiness_label="ready",
            ready_for_world_modeling=True,
            event_count=10,
            surface_count=1,
            case_count=1,
        ),
    )

    def fake_run(*_args: Any, **kwargs: Any) -> NewsStatePointRunResult:
        assert kwargs["topic"] == "banking_markets"
        assert kwargs["as_of"] == "1837-09-06"
        assert kwargs["candidates"][0].label == "New banking bill"
        assert "credit availability" in kwargs["candidates"][0].action
        root = tmp_path / "state_point"
        root.mkdir(parents=True, exist_ok=True)
        return NewsStatePointRunResult(
            label="banking_cli_fixture",
            topic="banking_markets",
            as_of="1837-09-06T00:00:00Z",
            state_event_id="news_state:banking_markets:1837-09-06",
            history_event_count=3,
            future_event_count=1,
            candidate_count=1,
            artifacts=NewsStatePointRunArtifacts(
                root=root,
                state_point_path=root / "state_point.json",
                result_json_path=root / "result.json",
                result_csv_path=root / "result.csv",
                result_markdown_path=root / "result.md",
            ),
        )

    monkeypatch.setattr(benchmark_cli, "run_news_state_point_counterfactual", fake_run)
    (tmp_path / "model.pt").write_text("stub", encoding="utf-8")

    result = CliRunner().invoke(
        cli_app,
        [
            "whatif",
            "benchmark",
            "news-state-point",
            "--input",
            f"news={tmp_path / 'context_snapshot.json'}",
            "--checkpoint",
            str(tmp_path / "model.pt"),
            "--topic",
            "banking_markets",
            "--as-of",
            "1837-09-06",
            "--candidate",
            (
                "New banking bill::A new banking bill passes; issue a public "
                "memo on credit availability."
            ),
        ],
    )

    assert result.exit_code == 0, result.output
    payload = json.loads(result.output)
    assert payload["state_event_id"] == "news_state:banking_markets:1837-09-06"
    assert payload["candidate_count"] == 1


def test_strategic_state_point_prompt_allows_proposed_decisions_without_future() -> (
    None
):
    world = _news_world()
    prompt = build_strategic_state_point_proposal_prompt(
        tenant_id="news",
        display_name="Historical News Fixture",
        as_of=datetime.fromisoformat("1837-09-06T00:00:00+00:00"),
        doctrine_text="Mission: track public-world banking risk.",
        evidence_events=world.events[:3],
        decisions_per_source=2,
        candidates_per_decision=8,
    )

    assert "does not need to be a real email" in prompt
    assert "CEO/operator/editor/regulator-style question" in prompt
    assert "future bill passage marker" not in prompt


def test_strategic_state_point_run_scores_template_proposals(
    tmp_path: Path,
    monkeypatch,
) -> None:
    world = _news_world()

    def fake_predict(**kwargs: Any) -> list[dict[str, Any]]:
        return [
            {
                "evidence_heads": {
                    "any_external_spread": 0.31,
                    "participant_fanout": 4,
                },
                "business_heads": {
                    "enterprise_risk": 0.22,
                    "commercial_position_proxy": 0.60,
                    "org_strain_proxy": 0.18,
                    "stakeholder_trust": 0.47,
                    "execution_drag": 0.28,
                },
                "future_state_heads": {
                    "regulatory_exposure": 0.21,
                    "accounting_control_pressure": 0.03,
                    "liquidity_stress": 0.62,
                    "governance_response": 0.15,
                    "evidence_control": 0.33,
                    "external_confidence_pressure": 0.71,
                },
                "objective_scores": {},
            }
            for _row in kwargs["rows"]
        ]

    monkeypatch.setattr(
        "vei.whatif.strategic_state_points.run_branch_point_benchmark_predictions",
        fake_predict,
    )

    result = run_strategic_state_point_counterfactuals(
        [
            StrategicStatePointSource(
                tenant_id="news",
                world=world,
                display_name="Historical News Fixture",
                as_of="1837-09-06",
            )
        ],
        checkpoint_path=tmp_path / "model.pt",
        artifacts_root=tmp_path / "strategic",
        label="strategic_fixture",
        decisions_per_source=1,
        candidates_per_decision=8,
        proposal_mode="template",
        proposal_model="template-fixture",
    )

    payload = json.loads(result.artifacts.result_json_path.read_text(encoding="utf-8"))
    first = payload["candidates"][0]
    assert result.decision_count == 1
    assert result.candidate_count == 8
    assert first["decision_point_not_historical_event"] is True
    assert first["learned_model_output"] == "predicted future vector"
    assert "learned_future_score" not in first
    assert first["score_output_kind"] == "operator_utility_readout"
    assert first["operator_score_formula_version"] == "balanced_operator_v1"
    assert first["operator_score_is_learned"] is False
    assert first["baseline_action_label"]
    assert "predicted_delta_vector" in first
    assert "tradeoff_summary" in first
    assert first["prediction_uncertainty_available"] is False
    assert "objective_policy_summary" not in first
    markdown = result.artifacts.result_markdown_path.read_text(encoding="utf-8")
    assert "not learned scores" in markdown
    assert "Delta vs baseline" in markdown


def test_strategic_state_point_cli_wires_sources_and_checkpoint(
    tmp_path: Path,
    monkeypatch,
) -> None:
    world = _news_world()

    def fake_load_world(
        *,
        source: str,
        source_dir: Path,
        include_situation_graph: bool = True,
    ) -> WhatIfWorld:
        assert source == "company_history"
        assert include_situation_graph is False
        return world

    monkeypatch.setattr(benchmark_cli, "load_world", fake_load_world)
    monkeypatch.setattr(
        benchmark_cli,
        "build_canonical_history_readiness",
        lambda _path: CanonicalHistoryReadinessReport(
            available=True,
            readiness_label="ready",
            ready_for_world_modeling=True,
            event_count=10,
            surface_count=1,
            case_count=1,
        ),
    )

    def fake_run(*args: Any, **kwargs: Any) -> StrategicStatePointRunResult:
        assert kwargs["checkpoint_path"] == tmp_path / "model.pt"
        assert kwargs["proposal_mode"] == "template"
        assert kwargs["proposal_model"] == "template-fixture"
        assert kwargs["decisions_per_source"] == 2
        assert kwargs["candidates_per_decision"] == 8
        assert args[0][0].as_of == "1837-09-06"
        root = tmp_path / "strategic"
        root.mkdir(parents=True, exist_ok=True)
        return StrategicStatePointRunResult(
            label="strategic_cli_fixture",
            source_count=1,
            decision_count=2,
            candidate_count=16,
            proposal_mode="template",
            proposal_model="template-fixture",
            artifacts=StrategicStatePointArtifacts(
                root=root,
                proposal_manifest_path=root / "proposals.json",
                result_json_path=root / "result.json",
                result_csv_path=root / "result.csv",
                result_markdown_path=root / "result.md",
            ),
        )

    monkeypatch.setattr(
        benchmark_cli,
        "run_strategic_state_point_counterfactuals",
        fake_run,
    )
    (tmp_path / "model.pt").write_text("stub", encoding="utf-8")

    result = CliRunner().invoke(
        cli_app,
        [
            "whatif",
            "benchmark",
            "strategic-state-points",
            "--input",
            f"news={tmp_path / 'context_snapshot.json'}",
            "--checkpoint",
            str(tmp_path / "model.pt"),
            "--as-of",
            "news=1837-09-06",
            "--decisions-per-tenant",
            "2",
            "--candidates-per-decision",
            "8",
            "--proposal-mode",
            "template",
            "--proposal-model",
            "template-fixture",
        ],
    )

    assert result.exit_code == 0, result.output
    payload = json.loads(result.output)
    assert payload["decision_count"] == 2
    assert payload["candidate_count"] == 16


def test_strategic_state_point_cli_defaults_to_available_codex_model(
    tmp_path: Path,
    monkeypatch,
) -> None:
    world = _news_world()

    monkeypatch.setattr(
        benchmark_cli,
        "load_world",
        lambda **_kwargs: world,
    )
    monkeypatch.setattr(
        benchmark_cli,
        "build_canonical_history_readiness",
        lambda _path: CanonicalHistoryReadinessReport(
            available=True,
            readiness_label="ready",
            ready_for_world_modeling=True,
            event_count=10,
            surface_count=1,
            case_count=1,
        ),
    )

    def fake_run(*_args: Any, **kwargs: Any) -> StrategicStatePointRunResult:
        assert kwargs["proposal_mode"] == "llm"
        assert kwargs["proposal_model"] == "gpt-5.4"
        root = tmp_path / "strategic_default"
        root.mkdir(parents=True, exist_ok=True)
        return StrategicStatePointRunResult(
            label="strategic_default_fixture",
            source_count=1,
            decision_count=1,
            candidate_count=8,
            proposal_mode=kwargs["proposal_mode"],
            proposal_model=kwargs["proposal_model"],
            artifacts=StrategicStatePointArtifacts(
                root=root,
                proposal_manifest_path=root / "proposals.json",
                result_json_path=root / "result.json",
                result_csv_path=root / "result.csv",
                result_markdown_path=root / "result.md",
            ),
        )

    monkeypatch.setattr(
        benchmark_cli,
        "run_strategic_state_point_counterfactuals",
        fake_run,
    )
    (tmp_path / "model.pt").write_text("stub", encoding="utf-8")

    result = CliRunner().invoke(
        cli_app,
        [
            "whatif",
            "benchmark",
            "strategic-state-points",
            "--input",
            f"news={tmp_path / 'context_snapshot.json'}",
            "--checkpoint",
            str(tmp_path / "model.pt"),
        ],
    )

    assert result.exit_code == 0, result.output
    payload = json.loads(result.output)
    assert payload["proposal_model"] == "gpt-5.4"


def test_strategic_proposal_runner_uses_codex_by_default(monkeypatch) -> None:
    calls: dict[str, Any] = {}

    def fake_codex_json(**kwargs: Any) -> dict[str, Any]:
        calls["codex"] = kwargs
        return {"decision_points": []}

    def fake_llm_json_prompt(*_args: Any, **_kwargs: Any) -> dict[str, Any]:
        raise AssertionError("direct provider API should not be used by default")

    monkeypatch.delenv("VEI_STRATEGIC_PROPOSAL_BACKEND", raising=False)
    monkeypatch.setattr(strategic_module, "run_codex_json", fake_codex_json)
    monkeypatch.setattr(strategic_module, "run_llm_json_prompt", fake_llm_json_prompt)

    payload = strategic_module._run_proposal_json_prompt(
        "Return decisions.",
        model="gpt-5.5",
        max_tokens=100,
        output_schema={"type": "object", "properties": {}},
        temperature=None,
    )

    assert payload == {"decision_points": []}
    assert calls["codex"]["model"] == "gpt-5.5"


def test_strategic_proposal_runner_allows_explicit_api_override(
    monkeypatch,
) -> None:
    calls: dict[str, Any] = {}

    def fake_codex_json(**_kwargs: Any) -> dict[str, Any]:
        raise AssertionError("Codex should not be used for explicit API override")

    def fake_llm_json_prompt(*args: Any, **kwargs: Any) -> dict[str, Any]:
        calls["args"] = args
        calls["kwargs"] = kwargs
        return {"decision_points": []}

    monkeypatch.setenv("VEI_STRATEGIC_PROPOSAL_BACKEND", "api")
    monkeypatch.setattr(strategic_module, "run_codex_json", fake_codex_json)
    monkeypatch.setattr(strategic_module, "run_llm_json_prompt", fake_llm_json_prompt)

    payload = strategic_module._run_proposal_json_prompt(
        "Return decisions.",
        model="gpt-5.5",
        max_tokens=100,
        output_schema={"type": "object", "properties": {}},
        temperature=None,
    )

    assert payload == {"decision_points": []}
    assert calls["args"][0] == "Return decisions."
    assert calls["kwargs"]["model"] == "gpt-5.5"


def _news_world() -> WhatIfWorld:
    events = [
        _event(
            "bank-pre-1",
            "1837-05-15T00:00:00+00:00",
            "Banking Markets",
            "Panic of 1837; banks suspend specie and credit tightens.",
        ),
        _event(
            "bank-pre-2",
            "1837-07-10T00:00:00+00:00",
            "Banking Markets",
            "Commercial failures continue; treasury and deposit policy debated.",
        ),
        _event(
            "bank-pre-3",
            "1837-09-05T00:00:00+00:00",
            "Banking Markets",
            "Currency pressure and bank credit stress remain visible.",
        ),
        _event(
            "labor-sermon",
            "1837-08-01T00:00:00+00:00",
            "Church notices",
            "A minister announces a Sunday sermon and school meeting.",
            topic="labor_work",
        ),
        _event(
            "bank-future-1",
            "1837-09-25T00:00:00+00:00",
            "Banking Markets",
            "future bill passage marker: Congress debates a treasury banking bill.",
        ),
    ]
    ordered = sorted(events, key=lambda item: (item.timestamp_ms, item.event_id))
    threads = build_thread_summaries(
        ordered, organization_domain="historical-news.local"
    )
    return WhatIfWorld(
        source="company_history",
        source_dir=Path("/tmp/news_fixture"),
        summary=WhatIfWorldSummary(
            source="company_history",
            organization_name="Historical News Fixture",
            organization_domain="historical-news.local",
            event_count=len(ordered),
            thread_count=len(threads),
            actor_count=2,
            first_timestamp=ordered[0].timestamp,
            last_timestamp=ordered[-1].timestamp,
        ),
        threads=threads,
        events=ordered,
    )


def _event(
    event_id: str,
    timestamp: str,
    subject: str,
    snippet: str,
    *,
    topic: str = "banking_markets",
) -> WhatIfEvent:
    parsed = datetime.fromisoformat(timestamp)
    return WhatIfEvent(
        event_id=event_id,
        timestamp=timestamp,
        timestamp_ms=int(parsed.timestamp() * 1000),
        actor_id="newspaper@historical-news.local",
        target_id=f"news:{topic}",
        event_type="article",
        thread_id=f"docs:news:{topic}",
        case_id=f"news:{topic}",
        surface="docs",
        conversation_anchor=f"news:{topic}",
        subject=subject,
        snippet=snippet,
        flags=WhatIfArtifactFlags(
            to_recipients=[f"news:{topic}"],
            to_count=1,
            subject=subject,
        ),
    )
