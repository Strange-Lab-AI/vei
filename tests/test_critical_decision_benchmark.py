from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

import pytest
from typer.testing import CliRunner

from vei.cli import whatif_benchmark as benchmark_cli
from vei.cli.vei import app as cli_app
from vei.context.canonical_history import CanonicalHistoryReadinessReport
from vei.whatif.corpus import build_thread_summaries
from vei.whatif.critical_decision_benchmark import (
    CriticalDecisionRunArtifacts,
    CriticalDecisionRunResult,
    _news_strategic_usefulness_score,
    build_critical_decision_benchmark,
    validate_critical_candidate_diversity,
)
from vei.whatif.models import (
    WhatIfActionSchema,
    WhatIfArtifactFlags,
    WhatIfBenchmarkCandidate,
    WhatIfEvent,
    WhatIfWorld,
    WhatIfWorldSummary,
)
from vei.whatif.multitenant_benchmark import MultiTenantBenchmarkSource


def test_critical_decision_benchmark_is_replicable_and_prebranch_only(
    tmp_path: Path,
) -> None:
    world = _tenant_world(
        tmp_path=tmp_path,
        tenant_id="dispatch",
        organization_name="Dispatch",
        organization_domain="dispatch.example",
        start_ms=2_000_000,
    )

    build, result = build_critical_decision_benchmark(
        [
            MultiTenantBenchmarkSource(
                tenant_id="dispatch",
                world=world,
                display_name="Dispatch",
            )
        ],
        artifacts_root=tmp_path / "critical_decisions",
        label="critical_fixture",
        cases_per_tenant=2,
        candidates_per_decision=10,
        candidate_generation_mode="template",
        candidate_model="template-fixture",
    )

    assert result.selected_decision_count == 2
    assert result.candidate_count == 20
    assert (
        build.dataset.metadata["benchmark_kind"] == "critical_decision_counterfactuals"
    )
    assert build.dataset.split_row_counts == {
        "train": 0,
        "validation": 0,
        "test": 2,
        "heldout": 2,
    }
    assert all(len(case.candidates) == 10 for case in build.cases)
    assert all(
        candidate.metadata["no_future_context"] is True
        for case in build.cases
        for candidate in case.candidates
    )

    selection_manifest = json.loads(
        result.artifacts.selection_manifest_path.read_text(encoding="utf-8")
    )
    assert all(
        row["no_future_context_for_selection"] is True for row in selection_manifest
    )
    assert all(row["criticality_score"] > 0 for row in selection_manifest)
    assert len({row["case_id"] for row in selection_manifest}) == len(
        selection_manifest
    )

    leakage = json.loads(
        result.artifacts.leakage_report_path.read_text(encoding="utf-8")
    )
    assert all(leakage["checks"].values())
    for case in leakage["candidate_cases"]:
        assert case["candidate_prompt_future_marker_hits"] == []
        assert case["candidate_output_future_marker_hits"] == []
        assert case["judge_dossier_future_marker_hits"] == []


def test_historical_news_candidate_prompt_targets_public_actions(
    tmp_path: Path,
) -> None:
    world = _tenant_world(
        tmp_path=tmp_path,
        tenant_id="pleias",
        organization_name="PleIAs Historical News Sample",
        organization_domain="historical-news.local",
        start_ms=4_000_000,
    )

    _build, result = build_critical_decision_benchmark(
        [
            MultiTenantBenchmarkSource(
                tenant_id="pleias_news",
                world=world,
                display_name="PleIAs News",
            )
        ],
        artifacts_root=tmp_path / "critical_decisions",
        label="news_prompt_fixture",
        cases_per_tenant=1,
        candidates_per_decision=10,
        candidate_generation_mode="template",
        candidate_model="template-fixture",
    )

    manifest = json.loads(
        result.artifacts.candidate_manifest_path.read_text(encoding="utf-8")
    )
    prompt = Path(manifest[0]["prompt_path"]).read_text(encoding="utf-8")

    assert "historical news branch point" in prompt
    assert "World-model and objective boundary" in prompt
    assert "JEPA will later predict likely future heads" in prompt
    assert "The objective/ranking layer decides what useful means" in prompt
    assert "Do not optimize every candidate for the lowest immediate risk" in prompt
    assert "Treat OCR errors" in prompt
    assert "Do not propose fixing OCR, ingestion, UI, pipeline" in prompt
    assert "public-world action postures" in prompt
    assert "maximize useful public-world action under uncertainty" in prompt
    assert "At least half the candidates must be active strategic actions" in prompt
    assert "Issue a public advisory, warning, or stakeholder bulletin" in prompt
    assert "Build an actor, institution, geography, or press-network map" in prompt
    assert "Do not use placeholder tokens" in prompt
    assert "future tail marker" not in prompt


def test_news_strategic_score_breaks_close_calls_toward_active_actions() -> None:
    objective_scores = {
        "minimize_enterprise_risk": 0.66,
        "protect_commercial_position": 0.49,
        "reduce_org_strain": 0.66,
        "preserve_stakeholder_trust": 0.39,
        "maintain_execution_velocity": 0.63,
    }

    hold_score = _news_strategic_usefulness_score(
        balanced_score=0.594,
        objective_scores=objective_scores,
        candidate_type="hold_compliance_review",
        is_news=True,
    )
    advisory_score = _news_strategic_usefulness_score(
        balanced_score=0.576,
        objective_scores=objective_scores,
        candidate_type="customer_status_note",
        is_news=True,
    )

    assert advisory_score > hold_score


def test_critical_candidate_diversity_rejects_minor_variants() -> None:
    candidates = []
    for index, candidate_type in enumerate(
        [
            "assign_owner_fix_path",
            "customer_status_note",
            "product_triage_queue",
            "fast_ship_low_risk",
            "expert_review_gate",
            "hold_compliance_review",
            "executive_escalation",
            "narrow_pilot",
        ]
    ):
        candidates.append(
            WhatIfBenchmarkCandidate(
                candidate_id=candidate_type,
                label=f"Candidate {index}",
                prompt="Pause the send and keep the team aligned before responding.",
                action_schema=WhatIfActionSchema(
                    decision_posture="review",
                    review_path="business_owner",
                    coordination_breadth="narrow",
                    outside_sharing_posture="internal_only",
                ),
                metadata={"candidate_type": candidate_type},
            )
        )

    with pytest.raises(ValueError, match="not broad enough|too similar"):
        validate_critical_candidate_diversity(
            candidates,
            candidates_per_decision=8,
        )


def test_critical_decision_cli_wires_inputs_and_checkpoint(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    world = _tenant_world(
        tmp_path=tmp_path,
        tenant_id="powrofyou",
        organization_name="Powr of You",
        organization_domain="powrofyou.com",
        start_ms=3_000_000,
    )

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
            event_count=200,
            surface_count=2,
            case_count=8,
        ),
    )

    def fake_run(*_args: object, **kwargs: object) -> CriticalDecisionRunResult:
        assert kwargs["checkpoint_path"] == tmp_path / "model.pt"
        assert kwargs["candidates_per_decision"] == 10
        assert kwargs["candidate_generation_mode"] == "template"
        root = tmp_path / "critical"
        root.mkdir(parents=True, exist_ok=True)
        return CriticalDecisionRunResult(
            label="critical_cli_fixture",
            selected_decision_count=1,
            candidate_count=10,
            tenants={"powrofyou": 1},
            artifacts=CriticalDecisionRunArtifacts(
                root=root,
                build_manifest_path=root / "branch_point_benchmark_build.json",
                heldout_cases_path=root / "heldout_cases.json",
                candidate_manifest_path=root / "candidate_generation_manifest.json",
                selection_manifest_path=root
                / "critical_decision_selection_manifest.json",
                leakage_report_path=root / "leakage_report.json",
                csv_path=root / "critical_decision_scores.csv",
                markdown_path=root / "critical_decision_scores.md",
            ),
        )

    monkeypatch.setattr(benchmark_cli, "run_critical_decision_benchmark", fake_run)
    (tmp_path / "model.pt").write_text("stub", encoding="utf-8")

    result = CliRunner().invoke(
        cli_app,
        [
            "whatif",
            "benchmark",
            "critical-decisions",
            "--input",
            f"powrofyou={tmp_path / 'powrofyou_context.json'}",
            "--checkpoint",
            str(tmp_path / "model.pt"),
            "--artifacts-root",
            str(tmp_path / "critical"),
            "--label",
            "critical_cli_fixture",
        ],
    )

    assert result.exit_code == 0, result.output
    payload = json.loads(result.output)
    assert payload["selected_decision_count"] == 1
    assert payload["candidate_count"] == 10


def _tenant_world(
    *,
    tmp_path: Path,
    tenant_id: str,
    organization_name: str,
    organization_domain: str,
    start_ms: int,
) -> WhatIfWorld:
    events: list[WhatIfEvent] = []
    for thread_index in range(6):
        thread_id = f"{tenant_id}-thread-{thread_index}"
        subject = f"{organization_name} permission page issue {thread_index}"
        case_id = f"{tenant_id}-case-{thread_index}"
        base_ms = start_ms + thread_index * 10_000
        for event_index in range(3):
            is_last = event_index == 2
            events.append(
                WhatIfEvent(
                    event_id=f"{thread_id}-event-{event_index}",
                    timestamp=_timestamp(base_ms + event_index * 1_000),
                    timestamp_ms=base_ms + event_index * 1_000,
                    actor_id=f"owner@{organization_domain}",
                    target_id=(
                        f"customer-{thread_index}@external.example"
                        if is_last
                        else f"legal@{organization_domain}"
                    ),
                    event_type="forward" if is_last else "message",
                    thread_id=thread_id,
                    case_id=case_id,
                    surface="mail",
                    subject=subject,
                    snippet=(
                        f"future tail marker {tenant_id} {thread_index}"
                        if is_last
                        else f"privacy permission issue needs owner {tenant_id} {thread_index}"
                    ),
                    flags=WhatIfArtifactFlags(
                        subject=subject,
                        norm_subject=subject.lower(),
                        is_forward=is_last,
                        consult_legal_specialist=True,
                        has_attachment_reference=is_last,
                        cc_count=2 if event_index == 1 else 0,
                        to_recipients=[
                            (
                                f"customer-{thread_index}@external.example"
                                if is_last
                                else f"legal@{organization_domain}"
                            )
                        ],
                        to_count=1,
                    ),
                )
            )
    ordered = sorted(events, key=lambda item: (item.timestamp_ms, item.event_id))
    threads = build_thread_summaries(ordered, organization_domain=organization_domain)
    return WhatIfWorld(
        source="company_history",
        source_dir=tmp_path / tenant_id,
        summary=WhatIfWorldSummary(
            source="company_history",
            organization_name=organization_name,
            organization_domain=organization_domain,
            event_count=len(ordered),
            thread_count=len(threads),
            actor_count=4,
            first_timestamp=ordered[0].timestamp,
            last_timestamp=ordered[-1].timestamp,
        ),
        threads=threads,
        events=ordered,
    )


def _timestamp(timestamp_ms: int) -> str:
    return datetime.fromtimestamp(timestamp_ms / 1000, tz=timezone.utc).isoformat()
