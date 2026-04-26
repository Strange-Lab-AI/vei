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
from vei.whatif import benchmark_bridge as benchmark_bridge_runtime
from vei.whatif.benchmark import (
    evaluate_branch_point_benchmark_model,
    load_branch_point_benchmark_build_result,
    train_branch_point_benchmark_model,
)
from vei.whatif.models import (
    WhatIfActionSchema,
    WhatIfArtifactFlags,
    WhatIfBenchmarkCandidate,
    WhatIfBenchmarkEvalArtifacts,
    WhatIfBenchmarkEvalResult,
    WhatIfBenchmarkTrainArtifacts,
    WhatIfBenchmarkTrainResult,
    WhatIfEvent,
    WhatIfObservedForecastMetrics,
    WhatIfWorld,
    WhatIfWorldSummary,
)
from vei.whatif.multitenant_benchmark import (
    MultiTenantBenchmarkSource,
    build_multitenant_world_model_benchmark,
    validate_candidate_diversity,
)


def test_multitenant_benchmark_uses_temporal_holdouts_without_prompt_leakage(
    tmp_path: Path,
) -> None:
    enron_world = _tenant_world(
        tmp_path=tmp_path,
        tenant_id="enron",
        organization_name="Enron",
        organization_domain="enron.com",
        start_ms=1_000_000,
    )
    enron_world = _with_context_snapshot(
        enron_world,
        tmp_path / "enron_snapshot",
        provider="mail",
        record_counts={"messages": 18, "threads": 6},
    )
    dispatch_world = _tenant_world(
        tmp_path=tmp_path,
        tenant_id="dispatch",
        organization_name="Dispatch",
        organization_domain="dispatch.example",
        start_ms=2_000_000,
    )
    dispatch_world = _with_context_snapshot(
        dispatch_world,
        tmp_path / "dispatch_snapshot",
        provider="clickup",
        record_counts={"tasks": 6},
    )

    result = build_multitenant_world_model_benchmark(
        [
            MultiTenantBenchmarkSource(
                tenant_id="enron",
                world=enron_world,
                display_name="Enron",
            ),
            MultiTenantBenchmarkSource(
                tenant_id="dispatch",
                world=dispatch_world,
                display_name="Dispatch",
            ),
        ],
        artifacts_root=tmp_path / "world_model_multitenant_jepa",
        label="pooled_fixture",
        heldout_cases_per_tenant=1,
        candidate_generation_mode="template",
        candidate_model="template-fixture",
    )

    assert result.dataset.metadata["benchmark_kind"] == "multitenant_world_model"
    assert result.dataset.split_row_counts == {
        "train": 8,
        "validation": 2,
        "test": 2,
        "heldout": 2,
    }
    assert {case.case_id.split(":", 1)[0] for case in result.cases} == {
        "enron",
        "dispatch",
    }

    train_rows = _load_jsonl(result.dataset.split_paths["train"])
    heldout_rows = _load_jsonl(result.dataset.split_paths["heldout"])
    assert {row["thread_id"] for row in train_rows}.isdisjoint(
        {row["thread_id"] for row in heldout_rows}
    )
    assert {row["branch_event_id"] for row in train_rows}.isdisjoint(
        {row["branch_event_id"] for row in heldout_rows}
    )

    for case in result.cases:
        postures = {candidate.metadata["posture"] for candidate in case.candidates}
        assert postures == {
            "containment_hold",
            "narrow_controlled_response",
            "escalate_expert_review",
            "speed_broad_coordination",
        }
        assert all(
            candidate.metadata["no_future_context"] is True
            for candidate in case.candidates
        )
        assert all(
            candidate.metadata["pre_branch_evidence_sha256"]
            for candidate in case.candidates
        )

    leakage_report = json.loads(
        Path(result.dataset.metadata["leakage_report_path"]).read_text(encoding="utf-8")
    )
    assert all(leakage_report["checks"].values())
    assert all(
        candidate_case["future_event_count"] == 1
        for candidate_case in leakage_report["candidate_cases"]
    )

    generation_manifest = json.loads(
        Path(result.dataset.metadata["candidate_generation_manifest_path"]).read_text(
            encoding="utf-8"
        )
    )
    for item in generation_manifest:
        prompt = Path(item["prompt_path"]).read_text(encoding="utf-8")
        assert item["no_future_context"] is True
        assert "recorded future" in prompt
        assert "future tail marker" not in prompt
        assert item["pre_branch_evidence_sha256"]

    provenance_report = json.loads(
        Path(result.dataset.metadata["data_provenance_report_path"]).read_text(
            encoding="utf-8"
        )
    )
    assert provenance_report["dataset_split_counts"] == result.dataset.split_row_counts
    assert (
        provenance_report["tenants"]["enron"]["source_record_counts"][0][
            "record_counts"
        ]["messages"]
        == 18
    )
    assert provenance_report["tenants"]["dispatch"]["canonical_event_count"] == 18


def test_multitenant_benchmark_uses_rolling_branch_rows_from_long_threads(
    tmp_path: Path,
) -> None:
    world = _tenant_world(
        tmp_path=tmp_path,
        tenant_id="dispatch",
        organization_name="Dispatch",
        organization_domain="dispatch.example",
        start_ms=2_000_000,
        event_count_per_thread=6,
    )

    result = build_multitenant_world_model_benchmark(
        [
            MultiTenantBenchmarkSource(
                tenant_id="dispatch",
                world=world,
                display_name="Dispatch",
            ),
        ],
        artifacts_root=tmp_path / "world_model_multitenant_jepa",
        label="rolling_fixture",
        heldout_cases_per_tenant=1,
        candidate_generation_mode="template",
        candidate_model="template-fixture",
        future_horizon_events=2,
    )

    split_counts = result.dataset.split_row_counts
    assert sum(split_counts.values()) > world.summary.thread_count
    assert result.dataset.metadata["future_horizon_events"] == 2

    leakage_report = json.loads(
        Path(result.dataset.metadata["leakage_report_path"]).read_text(encoding="utf-8")
    )
    assert all(leakage_report["checks"].values())
    train_rows = _load_jsonl(result.dataset.split_paths["train"])
    validation_rows = _load_jsonl(result.dataset.split_paths["validation"])
    heldout_rows = _load_jsonl(result.dataset.split_paths["heldout"])
    fit_branch_event_ids = {
        row["branch_event_id"] for row in [*train_rows, *validation_rows]
    }
    heldout_branch_event_ids = {row["branch_event_id"] for row in heldout_rows}
    assert fit_branch_event_ids.isdisjoint(heldout_branch_event_ids)


def test_multitenant_benchmark_caps_branch_rows_from_very_long_threads(
    tmp_path: Path,
) -> None:
    world = _tenant_world(
        tmp_path=tmp_path,
        tenant_id="powrofyou",
        organization_name="Powr of You",
        organization_domain="powrofyou.com",
        start_ms=3_000_000,
        event_count_per_thread=50,
    )

    result = build_multitenant_world_model_benchmark(
        [
            MultiTenantBenchmarkSource(
                tenant_id="powrofyou",
                world=world,
                display_name="Powr of You",
            ),
        ],
        artifacts_root=tmp_path / "world_model_multitenant_jepa",
        label="capped_long_thread_fixture",
        heldout_cases_per_tenant=1,
        candidate_generation_mode="template",
        candidate_model="template-fixture",
        max_branch_rows_per_thread=5,
    )

    split_counts = result.dataset.split_row_counts
    assert (
        split_counts["train"] + split_counts["validation"] + split_counts["test"] <= 30
    )
    assert result.dataset.metadata["max_branch_rows_per_thread"] == 5
    provenance_report = json.loads(
        Path(result.dataset.metadata["data_provenance_report_path"]).read_text(
            encoding="utf-8"
        )
    )
    assert provenance_report["tenants"]["powrofyou"]["max_branch_rows_per_thread"] == 5


def test_multitenant_benchmark_cli_builds_from_multiple_context_inputs(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    worlds = {
        "enron_context.json": _tenant_world(
            tmp_path=tmp_path,
            tenant_id="enron",
            organization_name="Enron",
            organization_domain="enron.com",
            start_ms=1_000_000,
        ),
        "dispatch_context.json": _tenant_world(
            tmp_path=tmp_path,
            tenant_id="dispatch",
            organization_name="Dispatch",
            organization_domain="dispatch.example",
            start_ms=2_000_000,
        ),
    }

    def fake_load_world(
        *,
        source: str,
        source_dir: Path,
        include_situation_graph: bool = True,
    ) -> WhatIfWorld:
        assert source == "company_history"
        assert include_situation_graph is False
        return worlds[source_dir.name]

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

    result = CliRunner().invoke(
        cli_app,
        [
            "whatif",
            "benchmark",
            "build-multitenant",
            "--input",
            f"enron={tmp_path / 'enron_context.json'}",
            "--input",
            f"dispatch={tmp_path / 'dispatch_context.json'}",
            "--artifacts-root",
            str(tmp_path / "world_model_multitenant_jepa"),
            "--label",
            "pooled_cli_fixture",
            "--heldout-cases-per-tenant",
            "1",
        ],
    )

    assert result.exit_code == 0, result.output
    payload = json.loads(result.output)
    assert payload["dataset"]["metadata"]["benchmark_kind"] == "multitenant_world_model"
    assert payload["dataset"]["metadata"]["candidate_generation_mode"] == "template"
    assert payload["dataset"]["split_row_counts"]["heldout"] == 2


def test_multitenant_benchmark_cli_rejects_unready_tenant(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        benchmark_cli,
        "build_canonical_history_readiness",
        lambda _path: CanonicalHistoryReadinessReport(
            available=True,
            readiness_label="thin",
            ready_for_world_modeling=False,
            notes=["Many events are using derived timestamps."],
        ),
    )

    def fail_load_world(*, source: str, source_dir: Path) -> WhatIfWorld:
        raise AssertionError("load_world should not run for an unready tenant")

    monkeypatch.setattr(benchmark_cli, "load_world", fail_load_world)

    result = CliRunner().invoke(
        cli_app,
        [
            "whatif",
            "benchmark",
            "build-multitenant",
            "--input",
            f"dispatch={tmp_path / 'dispatch_context.json'}",
            "--artifacts-root",
            str(tmp_path / "world_model_multitenant_jepa"),
            "--candidate-mode",
            "template",
        ],
    )

    assert result.exit_code != 0
    assert "not ready for world-model training" in result.output


def test_multitenant_benchmark_uses_existing_train_and_eval_boundary(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    build = build_multitenant_world_model_benchmark(
        [
            MultiTenantBenchmarkSource(
                tenant_id="enron",
                world=_tenant_world(
                    tmp_path=tmp_path,
                    tenant_id="enron",
                    organization_name="Enron",
                    organization_domain="enron.com",
                    start_ms=1_000_000,
                ),
            ),
            MultiTenantBenchmarkSource(
                tenant_id="dispatch",
                world=_tenant_world(
                    tmp_path=tmp_path,
                    tenant_id="dispatch",
                    organization_name="Dispatch",
                    organization_domain="dispatch.example",
                    start_ms=2_000_000,
                ),
            ),
        ],
        artifacts_root=tmp_path / "world_model_multitenant_jepa",
        label="pooled_train_eval_fixture",
        heldout_cases_per_tenant=1,
        candidate_generation_mode="template",
        candidate_model="template-fixture",
    )

    def fake_train_runtime(**kwargs: object) -> WhatIfBenchmarkTrainResult:
        loaded = load_branch_point_benchmark_build_result(str(kwargs["build_root"]))
        assert loaded.dataset.metadata["benchmark_kind"] == "multitenant_world_model"
        assert loaded.dataset.split_row_counts["test"] == 2
        train_splits = list(kwargs.get("train_splits") or ["train"])
        validation_splits = list(kwargs.get("validation_splits") or ["validation"])
        output_root = kwargs.get("output_root")
        model_id = str(kwargs["model_id"])
        model_root = (
            Path(str(output_root))
            if output_root is not None
            else build.artifacts.root / "model_runs" / model_id
        )
        model_root.mkdir(parents=True, exist_ok=True)
        artifacts = WhatIfBenchmarkTrainArtifacts(
            root=model_root,
            model_path=model_root / "model.pt",
            metadata_path=model_root / "metadata.json",
            train_result_path=model_root / "train_result.json",
        )
        artifacts.model_path.write_text("stub", encoding="utf-8")
        artifacts.metadata_path.write_text("{}", encoding="utf-8")
        result = WhatIfBenchmarkTrainResult(
            model_id="full_context_transformer",
            dataset_root=loaded.dataset.root,
            train_loss=0.1,
            validation_loss=0.2,
            epoch_count=1,
            train_row_count=sum(
                loaded.dataset.split_row_counts[split_name]
                for split_name in train_splits
            ),
            validation_row_count=sum(
                loaded.dataset.split_row_counts[split_name]
                for split_name in validation_splits
            ),
            artifacts=artifacts,
        )
        artifacts.train_result_path.write_text(
            result.model_dump_json(indent=2),
            encoding="utf-8",
        )
        return result

    def fake_eval_runtime(**kwargs: object) -> WhatIfBenchmarkEvalResult:
        loaded = load_branch_point_benchmark_build_result(str(kwargs["build_root"]))
        output_root = kwargs.get("output_root")
        model_id = str(kwargs["model_id"])
        model_root = (
            Path(str(output_root))
            if output_root is not None
            else build.artifacts.root / "model_runs" / model_id
        )
        model_root.mkdir(parents=True, exist_ok=True)
        artifacts = WhatIfBenchmarkEvalArtifacts(
            root=model_root,
            eval_result_path=model_root / "eval_result.json",
            prediction_jsonl_path=model_root / "predictions.jsonl",
        )
        artifacts.prediction_jsonl_path.write_text("", encoding="utf-8")
        result = WhatIfBenchmarkEvalResult(
            model_id="full_context_transformer",
            dataset_root=loaded.dataset.root,
            observed_metrics=WhatIfObservedForecastMetrics(
                auroc_any_external_spread=0.75,
                brier_any_external_spread=0.2,
                calibration_error_any_external_spread=0.1,
            ),
            cases=[],
            artifacts=artifacts,
        )
        artifacts.eval_result_path.write_text(
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
        model_id="full_context_transformer",
        epochs=1,
        train_splits=["train", "validation"],
        validation_splits=["test"],
    )
    eval_result = evaluate_branch_point_benchmark_model(
        build.artifacts.root,
        model_id="full_context_transformer",
    )

    assert train_result.train_row_count == 10
    assert train_result.validation_row_count == 2
    assert eval_result.observed_metrics.auroc_any_external_spread == 0.75


def test_benchmark_split_parser_prevents_training_on_test_split() -> None:
    assert benchmark_bridge_runtime._dataset_split_names(
        ["train", "validation"],
        default=("train",),
        allowed=("train", "validation"),
    ) == ["train", "validation"]
    assert benchmark_bridge_runtime._dataset_split_names(
        "test",
        default=("validation",),
        allowed=("validation", "test"),
    ) == ["test"]
    with pytest.raises(ValueError, match="unsupported dataset split"):
        benchmark_bridge_runtime._dataset_split_names(
            "test",
            default=("train",),
            allowed=("train", "validation"),
        )


def test_multitenant_benchmark_can_eval_heuristic_baseline_comparator(
    tmp_path: Path,
) -> None:
    build = build_multitenant_world_model_benchmark(
        [
            MultiTenantBenchmarkSource(
                tenant_id="enron",
                world=_tenant_world(
                    tmp_path=tmp_path,
                    tenant_id="enron",
                    organization_name="Enron",
                    organization_domain="enron.com",
                    start_ms=1_000_000,
                ),
            ),
            MultiTenantBenchmarkSource(
                tenant_id="dispatch",
                world=_tenant_world(
                    tmp_path=tmp_path,
                    tenant_id="dispatch",
                    organization_name="Dispatch",
                    organization_domain="dispatch.example",
                    start_ms=2_000_000,
                ),
            ),
        ],
        artifacts_root=tmp_path / "world_model_multitenant_jepa",
        label="pooled_heuristic_fixture",
        heldout_cases_per_tenant=1,
        candidate_generation_mode="template",
        candidate_model="template-fixture",
    )

    train_result = train_branch_point_benchmark_model(
        build.artifacts.root,
        model_id="heuristic_baseline",
    )
    eval_result = evaluate_branch_point_benchmark_model(
        build.artifacts.root,
        model_id="heuristic_baseline",
    )

    assert train_result.epoch_count == 0
    assert train_result.train_row_count == 8
    assert eval_result.model_id == "heuristic_baseline"
    assert eval_result.observed_metrics.brier_any_external_spread >= 0.0
    assert eval_result.cases
    assert eval_result.artifacts.prediction_jsonl_path.exists()


def test_candidate_diversity_rejects_minor_variants() -> None:
    candidates = [
        _candidate(
            posture="containment_hold",
            label="Hold",
            prompt="Pause the send and keep the team aligned before responding.",
            decision_posture="hold",
            review_path="internal_legal",
            coordination_breadth="narrow",
            outside_sharing_posture="internal_only",
        ),
        _candidate(
            posture="narrow_controlled_response",
            label="Narrow",
            prompt="Pause the send and keep the team aligned before responding today.",
            decision_posture="resolve",
            review_path="business_owner",
            coordination_breadth="single_owner",
            outside_sharing_posture="status_only",
        ),
        _candidate(
            posture="escalate_expert_review",
            label="Escalate",
            prompt="Pause the send and keep the team aligned before responding soon.",
            decision_posture="escalate",
            review_path="cross_functional",
            coordination_breadth="targeted",
            outside_sharing_posture="limited_external",
        ),
        _candidate(
            posture="speed_broad_coordination",
            label="Speed",
            prompt="Pause the send and keep the team aligned before responding fast.",
            decision_posture="resolve",
            review_path="executive",
            coordination_breadth="broad",
            outside_sharing_posture="broad_external",
        ),
    ]

    with pytest.raises(ValueError, match="too similar"):
        validate_candidate_diversity(candidates)


def _tenant_world(
    *,
    tmp_path: Path,
    tenant_id: str,
    organization_name: str,
    organization_domain: str,
    start_ms: int,
    event_count_per_thread: int = 3,
) -> WhatIfWorld:
    events: list[WhatIfEvent] = []
    for thread_index in range(6):
        thread_id = f"{tenant_id}-thread-{thread_index}"
        subject = f"{organization_name} decision {thread_index}"
        case_id = f"{tenant_id}-case-{thread_index}"
        base_ms = start_ms + thread_index * 10_000
        for event_index in range(event_count_per_thread):
            is_first = event_index == 0
            is_last = event_index == event_count_per_thread - 1
            events.append(
                _event(
                    tenant_id=tenant_id,
                    thread_id=thread_id,
                    case_id=case_id,
                    event_index=event_index,
                    timestamp_ms=base_ms + (event_index * 1_000),
                    actor_id=(
                        f"owner@{organization_domain}"
                        if is_first
                        else f"ops@{organization_domain}"
                    ),
                    target_id=(
                        f"customer-{thread_index}@external.example"
                        if is_last
                        else f"legal@{organization_domain}"
                    ),
                    event_type=(
                        "forward" if is_last else ("message" if is_first else "reply")
                    ),
                    subject=subject,
                    snippet=(
                        f"future tail marker {tenant_id} {thread_index}"
                        if is_last
                        else f"branch decision marker {tenant_id} {thread_index} {event_index}"
                    ),
                    is_reply=not is_first and not is_last,
                    is_forward=is_last,
                    consult_legal=(thread_index + event_index) % 2 == 0,
                )
            )

    ordered = sorted(events, key=lambda item: (item.timestamp_ms, item.event_id))
    threads = build_thread_summaries(
        ordered,
        organization_domain=organization_domain,
    )
    summary = WhatIfWorldSummary(
        source="company_history",
        organization_name=organization_name,
        organization_domain=organization_domain,
        event_count=len(ordered),
        thread_count=len(threads),
        actor_count=len(
            {event.actor_id for event in ordered}
            | {event.target_id for event in ordered}
        ),
        first_timestamp=ordered[0].timestamp,
        last_timestamp=ordered[-1].timestamp,
    )
    return WhatIfWorld(
        source="company_history",
        source_dir=tmp_path / tenant_id,
        summary=summary,
        threads=threads,
        events=ordered,
    )


def _with_context_snapshot(
    world: WhatIfWorld,
    path: Path,
    *,
    provider: str,
    record_counts: dict[str, int],
) -> WhatIfWorld:
    path.mkdir(parents=True, exist_ok=True)
    (path / "context_snapshot.json").write_text(
        json.dumps(
            {
                "sources": [
                    {
                        "provider": provider,
                        "status": "ok",
                        "record_counts": record_counts,
                    }
                ]
            }
        ),
        encoding="utf-8",
    )
    return world.model_copy(update={"source_dir": path / "context_snapshot.json"})


def _event(
    *,
    tenant_id: str,
    thread_id: str,
    case_id: str,
    event_index: int,
    timestamp_ms: int,
    actor_id: str,
    target_id: str,
    event_type: str,
    subject: str,
    snippet: str,
    is_reply: bool = False,
    is_forward: bool = False,
    consult_legal: bool = False,
) -> WhatIfEvent:
    return WhatIfEvent(
        event_id=f"{thread_id}-event-{event_index}",
        timestamp=_timestamp(timestamp_ms),
        timestamp_ms=timestamp_ms,
        actor_id=actor_id,
        target_id=target_id,
        event_type=event_type,
        thread_id=thread_id,
        case_id=case_id,
        surface="mail",
        subject=subject,
        snippet=snippet,
        flags=WhatIfArtifactFlags(
            subject=subject,
            norm_subject=subject.lower(),
            is_reply=is_reply,
            is_forward=is_forward,
            consult_legal_specialist=consult_legal,
            to_recipients=[target_id],
            to_count=1,
        ),
    )


def _candidate(
    *,
    posture: str,
    label: str,
    prompt: str,
    decision_posture: str,
    review_path: str,
    coordination_breadth: str,
    outside_sharing_posture: str,
) -> WhatIfBenchmarkCandidate:
    return WhatIfBenchmarkCandidate(
        candidate_id=posture,
        label=label,
        prompt=prompt,
        action_schema=WhatIfActionSchema(
            decision_posture=decision_posture,
            review_path=review_path,
            coordination_breadth=coordination_breadth,
            outside_sharing_posture=outside_sharing_posture,
        ),
        metadata={"posture": posture},
    )


def _load_jsonl(path: str) -> list[dict[str, object]]:
    return [
        json.loads(line)
        for line in Path(path).read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]


def _timestamp(timestamp_ms: int) -> str:
    return datetime.fromtimestamp(timestamp_ms / 1000, tz=timezone.utc).isoformat()
