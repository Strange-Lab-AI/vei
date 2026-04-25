from __future__ import annotations

import json
from collections import defaultdict
from dataclasses import dataclass
from hashlib import sha256
from pathlib import Path
from typing import Sequence

from .benchmark_business import (
    evidence_to_business_outcomes,
    summarize_future_state_heads,
    summarize_observed_evidence,
)
from ._benchmark_constants import (
    BENCHMARK_MODELS as _BENCHMARK_MODELS,
    BUSINESS_OBJECTIVE_PACK_IDS as _BUSINESS_OBJECTIVE_PACK_IDS,
    HOLD_TERMS as _HOLD_TERMS,
    MULTI_PARTY_TERMS as _MULTI_PARTY_TERMS,
    SINGLE_PARTY_TERMS as _SINGLE_PARTY_TERMS,
)
from ._benchmark_case_packs import (
    BENCHMARK_CASE_PACKS as _BENCHMARK_CASE_PACKS,
    BenchmarkCaseSeed as _BenchmarkCaseSeed,
    DEFAULT_BENCHMARK_PACK_ID as _DEFAULT_BENCHMARK_PACK_ID,
)
from ._benchmark_dossiers import build_dossier_files as _write_case_dossiers
from ._enron_history import build_enron_branch_history
from ._benchmark_utils import (
    clamp as _clamp,
    coordination_breadth_for_event as _coordination_breadth_for_event,
    coordination_breadth_for_prompt as _coordination_breadth_for_prompt,
    decision_posture_for_text as _decision_posture_for_text,
    delay_norm as _delay_norm,
    escalation_level_for_text as _escalation_level_for_text,
    event_external_count as _event_external_count,
    event_has_commitment_signal as _event_has_commitment_signal,
    event_has_conflict_signal as _event_has_conflict_signal,
    event_has_cross_functional_signal as _event_has_cross_functional_signal,
    event_has_executive_signal as _event_has_executive_signal,
    event_has_review_signal as _event_has_review_signal,
    event_has_urgency_signal as _event_has_urgency_signal,
    event_reassurance_count as _event_reassurance_count,
    event_scope as _event_scope,
    historical_branch_tags as _historical_branch_tags,
    kendall_tau as _kendall_tau,
    message_count_norm as _message_count_norm,
    metric_summary as _metric_summary,
    optional_metric_summary as _optional_metric_summary,
    outside_sharing_posture_for_event as _outside_sharing_posture_for_event,
    outside_sharing_posture_for_prompt as _outside_sharing_posture_for_prompt,
    pairwise_hits as _pairwise_hits,
    rank_study_models as _rank_study_models,
    reassurance_style_for_text as _reassurance_style_for_text,
    review_path_for_prompt as _review_path_for_prompt,
    review_path_from_text as _review_path_from_text,
    slug as _slug,
    write_jsonl as _write_jsonl,
)
from .benchmark_runtime import (
    run_branch_point_benchmark_evaluation,
    run_branch_point_benchmark_training,
)
from ..score_frontier import run_llm_judge_prompt
from .corpus import (
    choose_branch_event,
    event_by_id,
    event_reference,
    hydrate_event_snippets,
)
from ._helpers import intervention_tags
from .models import (
    WhatIfActionSchema,
    WhatIfAuditRecord,
    WhatIfBenchmarkBuildArtifacts,
    WhatIfBenchmarkBuildResult,
    WhatIfBenchmarkCase,
    WhatIfBenchmarkCaseEvaluation,
    WhatIfBenchmarkCandidate,
    WhatIfBenchmarkDatasetManifest,
    WhatIfBenchmarkDatasetRow,
    WhatIfBenchmarkEvalResult,
    WhatIfBenchmarkJudgeArtifacts,
    WhatIfBenchmarkJudgeResult,
    WhatIfBenchmarkModelId,
    WhatIfBenchmarkSplit,
    WhatIfBenchmarkStudyArtifacts,
    WhatIfBenchmarkStudyModelSummary,
    WhatIfBenchmarkStudyResult,
    WhatIfBenchmarkStudyRun,
    WhatIfBenchmarkTrainResult,
    WhatIfBusinessObjectivePackId,
    WhatIfBranchSummaryFeature,
    WhatIfDominanceSummary,
    WhatIfEvent,
    WhatIfJudgeSummary,
    WhatIfJudgedPairwiseComparison,
    WhatIfJudgedRanking,
    WhatIfObservedEvidenceHeads,
    WhatIfObservedOutcomeTargets,
    WhatIfOutcomeSignals,
    WhatIfPackRunResult,
    WhatIfPanelJudgment,
    WhatIfPanelSummary,
    WhatIfPreBranchContract,
    WhatIfResearchCase,
    WhatIfResearchHypothesisLabel,
    WhatIfResearchPack,
    WhatIfRolloutStressSummary,
    WhatIfSequenceStep,
    WhatIfWorld,
    WhatIfAuditSummary,
)
from .research import get_research_pack
from vei.context.api import slice_public_context_to_branch


@dataclass(frozen=True)
class _ResolvedBenchmarkCaseSpec:
    case_id: str
    title: str
    event_id: str
    thread_id: str
    summary: str
    case_family: str
    candidates: list[WhatIfBenchmarkCandidate]


def list_branch_point_benchmark_models() -> list[WhatIfBenchmarkModelId]:
    return list(_BENCHMARK_MODELS)


def build_branch_point_benchmark(
    world: WhatIfWorld,
    *,
    artifacts_root: str | Path,
    label: str,
    heldout_pack_id: str = _DEFAULT_BENCHMARK_PACK_ID,
) -> WhatIfBenchmarkBuildResult:
    if heldout_pack_id == _DEFAULT_BENCHMARK_PACK_ID and world.source != "enron":
        raise ValueError(
            "enron_business_outcome_v1 requires an Enron historical source"
        )
    root = Path(artifacts_root).expanduser().resolve() / _slug(label)
    root.mkdir(parents=True, exist_ok=True)
    build_path = root / "branch_point_benchmark_build.json"
    heldout_cases_path = root / "heldout_cases.json"
    judge_template_path = root / "judged_ranking_template.json"
    audit_template_path = root / "audit_record_template.json"
    dossier_root = root / "dossiers"
    dataset_root = root / "dataset"
    dataset_root.mkdir(parents=True, exist_ok=True)
    dossier_root.mkdir(parents=True, exist_ok=True)

    events_by_thread = _group_events_by_thread(world.events)
    benchmark_cases, heldout_rows = _build_benchmark_cases(
        world=world,
        heldout_pack_id=heldout_pack_id,
        events_by_thread=events_by_thread,
        dossier_root=dossier_root,
    )
    heldout_thread_ids = {case.thread_id for case in benchmark_cases if case.thread_id}

    factual_rows: list[tuple[int, WhatIfBenchmarkDatasetRow]] = []
    for thread in world.threads:
        if thread.thread_id in heldout_thread_ids:
            continue
        timeline = events_by_thread.get(thread.thread_id, [])
        if len(timeline) < 2:
            continue
        branch_event = choose_branch_event(
            timeline,
            requested_event_id=None,
        )
        history_events, future_events = _split_timeline(
            timeline=timeline,
            branch_event_id=branch_event.event_id,
        )
        if not history_events or not future_events:
            continue
        if world.source == "enron":
            history_events = build_enron_branch_history(
                public_context=world.public_context,
                branch_event=branch_event,
                past_events=history_events,
            )
        contract = _build_pre_branch_contract(
            case_id=thread.thread_id,
            thread_id=thread.thread_id,
            branch_event=branch_event,
            history_events=history_events,
            organization_domain=world.summary.organization_domain,
            action_schema=_action_schema_from_event(
                branch_event,
                organization_domain=world.summary.organization_domain,
            ),
            notes=["Observed historical branch row."],
        )
        targets = summarize_observed_targets(
            branch_event=branch_event,
            future_events=future_events,
            organization_domain=world.summary.organization_domain,
        )
        evidence = summarize_observed_evidence(
            branch_event=branch_event,
            future_events=future_events,
        )
        business = evidence_to_business_outcomes(evidence)
        future_state = summarize_future_state_heads(
            future_events=future_events,
            evidence=evidence,
        )
        row = WhatIfBenchmarkDatasetRow(
            row_id=f"{thread.thread_id}:{branch_event.event_id}",
            split="train",
            thread_id=thread.thread_id,
            branch_event_id=branch_event.event_id,
            contract=contract,
            observed_evidence_heads=evidence,
            observed_business_outcomes=business,
            observed_future_state=future_state,
            observed_targets=targets,
            observed_outcome_signals=outcome_targets_to_signals(targets),
        )
        factual_rows.append((branch_event.timestamp_ms, row))

    split_rows = _assign_temporal_splits(factual_rows)
    split_paths: dict[str, str] = {}
    split_counts: dict[str, int] = {}
    for split_name in ("train", "validation", "test"):
        path = dataset_root / f"{split_name}_rows.jsonl"
        rows = split_rows.get(split_name, [])
        _write_jsonl(path, rows)
        split_paths[split_name] = str(path)
        split_counts[split_name] = len(rows)

    heldout_path = dataset_root / "heldout_rows.jsonl"
    _write_jsonl(heldout_path, heldout_rows)
    split_paths["heldout"] = str(heldout_path)
    split_counts["heldout"] = len(heldout_rows)

    judged_template = _judge_template_rows(benchmark_cases)
    judge_template_path.write_text(
        json.dumps([row.model_dump(mode="json") for row in judged_template], indent=2),
        encoding="utf-8",
    )
    audit_template = _audit_template_rows(benchmark_cases)
    audit_template_path.write_text(
        json.dumps([row.model_dump(mode="json") for row in audit_template], indent=2),
        encoding="utf-8",
    )
    heldout_cases_path.write_text(
        json.dumps(
            [case.model_dump(mode="json") for case in benchmark_cases], indent=2
        ),
        encoding="utf-8",
    )

    dataset_manifest = WhatIfBenchmarkDatasetManifest(
        root=dataset_root,
        split_row_counts=split_counts,
        split_paths=split_paths,
        heldout_cases_path=str(heldout_cases_path),
        judge_template_path=str(judge_template_path),
        audit_template_path=str(audit_template_path),
        dossier_root=str(dossier_root),
        heldout_thread_ids=sorted(heldout_thread_ids),
    )
    (dataset_root / "dataset_manifest.json").write_text(
        dataset_manifest.model_dump_json(indent=2),
        encoding="utf-8",
    )
    result = WhatIfBenchmarkBuildResult(
        label=label,
        heldout_pack_id=heldout_pack_id,
        dataset=dataset_manifest,
        cases=benchmark_cases,
        artifacts=WhatIfBenchmarkBuildArtifacts(
            root=root,
            manifest_path=build_path,
            heldout_cases_path=heldout_cases_path,
            judge_template_path=judge_template_path,
            audit_template_path=audit_template_path,
            dossier_root=dossier_root,
        ),
    )
    build_path.write_text(result.model_dump_json(indent=2), encoding="utf-8")
    return result


def load_branch_point_benchmark_build_result(
    root: str | Path,
) -> WhatIfBenchmarkBuildResult:
    resolved = Path(root).expanduser().resolve()
    result_path = (
        resolved
        if resolved.name == "branch_point_benchmark_build.json"
        else resolved / "branch_point_benchmark_build.json"
    )
    return WhatIfBenchmarkBuildResult.model_validate_json(
        result_path.read_text(encoding="utf-8")
    )


def load_branch_point_benchmark_train_result(
    root: str | Path,
    *,
    model_id: WhatIfBenchmarkModelId,
) -> WhatIfBenchmarkTrainResult:
    result_path = (
        Path(root).expanduser().resolve()
        / "model_runs"
        / str(model_id)
        / "train_result.json"
    )
    return WhatIfBenchmarkTrainResult.model_validate_json(
        result_path.read_text(encoding="utf-8")
    )


def load_branch_point_benchmark_eval_result(
    root: str | Path,
    *,
    model_id: WhatIfBenchmarkModelId,
) -> WhatIfBenchmarkEvalResult:
    result_path = (
        Path(root).expanduser().resolve()
        / "model_runs"
        / str(model_id)
        / "eval_result.json"
    )
    return WhatIfBenchmarkEvalResult.model_validate_json(
        result_path.read_text(encoding="utf-8")
    )


def load_branch_point_benchmark_judge_result(
    root: str | Path,
) -> WhatIfBenchmarkJudgeResult:
    resolved = Path(root).expanduser().resolve()
    result_path = (
        resolved
        if resolved.name == "judge_result.json"
        else resolved / "judge_result.json"
    )
    return WhatIfBenchmarkJudgeResult.model_validate_json(
        result_path.read_text(encoding="utf-8")
    )


def load_branch_point_benchmark_study_result(
    root: str | Path,
) -> WhatIfBenchmarkStudyResult:
    resolved = Path(root).expanduser().resolve()
    result_path = (
        resolved
        if resolved.name == "benchmark_study_result.json"
        else resolved / "benchmark_study_result.json"
    )
    return WhatIfBenchmarkStudyResult.model_validate_json(
        result_path.read_text(encoding="utf-8")
    )


def train_branch_point_benchmark_model(
    root: str | Path,
    *,
    model_id: WhatIfBenchmarkModelId,
    epochs: int = 12,
    batch_size: int = 64,
    learning_rate: float = 1e-3,
    seed: int = 42042,
    device: str | None = None,
    runtime_root: str | Path | None = None,
    output_root: str | Path | None = None,
) -> WhatIfBenchmarkTrainResult:
    return run_branch_point_benchmark_training(
        build_root=root,
        model_id=model_id,
        epochs=epochs,
        batch_size=batch_size,
        learning_rate=learning_rate,
        seed=seed,
        device=device,
        runtime_root=runtime_root,
        output_root=output_root,
    )


def judge_branch_point_benchmark(
    root: str | Path,
    *,
    model: str = "gpt-4.1-mini",
    judge_id: str = "benchmark_llm_judge",
) -> WhatIfBenchmarkJudgeResult:
    build = load_branch_point_benchmark_build_result(root)
    judgments: list[WhatIfJudgedRanking] = []
    for case in build.cases:
        for objective_pack_id in _BUSINESS_OBJECTIVE_PACK_IDS:
            judgments.append(
                _judge_case_objective(
                    build_root=build.artifacts.root,
                    case=case,
                    objective_pack_id=objective_pack_id,
                    model=model,
                    judge_id=judge_id,
                )
            )
    audit_queue = _build_audit_queue(judgments)
    result_path = build.artifacts.root / "judge_result.json"
    audit_queue_path = build.artifacts.root / "audit_queue.json"
    result = WhatIfBenchmarkJudgeResult(
        build_root=build.artifacts.root,
        judge_model=model,
        judgments=judgments,
        audit_queue=audit_queue,
        notes=[
            f"judgments={len(judgments)}",
            f"audit_queue={len(audit_queue)}",
        ],
        artifacts=WhatIfBenchmarkJudgeArtifacts(
            root=build.artifacts.root,
            result_path=result_path,
            audit_queue_path=audit_queue_path,
        ),
    )
    result_path.write_text(result.model_dump_json(indent=2), encoding="utf-8")
    audit_queue_path.write_text(
        json.dumps([item.model_dump(mode="json") for item in audit_queue], indent=2),
        encoding="utf-8",
    )
    return result


def evaluate_branch_point_benchmark_model(
    root: str | Path,
    *,
    model_id: WhatIfBenchmarkModelId,
    judged_rankings_path: str | Path | None = None,
    audit_records_path: str | Path | None = None,
    panel_judgments_path: str | Path | None = None,
    research_pack_root: str | Path | None = None,
    device: str | None = None,
    runtime_root: str | Path | None = None,
    output_root: str | Path | None = None,
) -> WhatIfBenchmarkEvalResult:
    result = run_branch_point_benchmark_evaluation(
        build_root=root,
        model_id=model_id,
        device=device,
        runtime_root=runtime_root,
        output_root=output_root,
    )
    judged_rankings = list_judged_rankings(judged_rankings_path)
    audit_records = list_audit_records(audit_records_path)
    result = result.model_copy(
        update={
            "dominance_summary": _dominance_summary(result.cases),
            "judge_summary": _judge_summary(
                result.cases,
                judged_rankings=judged_rankings,
            ),
            "audit_summary": _audit_summary(
                judged_rankings=judged_rankings,
                audit_records=audit_records,
            ),
            "panel_summary": _panel_summary(
                result.cases,
                panel_judgments_path=panel_judgments_path,
            ),
            "rollout_stress_summary": _rollout_stress_summary(
                result.cases,
                research_pack_root=research_pack_root,
            ),
        }
    )
    eval_path = result.artifacts.eval_result_path
    eval_path.write_text(result.model_dump_json(indent=2), encoding="utf-8")
    return result


def run_branch_point_benchmark_study(
    root: str | Path,
    *,
    label: str,
    model_ids: Sequence[WhatIfBenchmarkModelId],
    seeds: Sequence[int],
    epochs: int = 12,
    batch_size: int = 64,
    learning_rate: float = 1e-3,
    judged_rankings_path: str | Path | None = None,
    audit_records_path: str | Path | None = None,
    panel_judgments_path: str | Path | None = None,
    research_pack_root: str | Path | None = None,
    device: str | None = None,
    runtime_root: str | Path | None = None,
) -> WhatIfBenchmarkStudyResult:
    build = load_branch_point_benchmark_build_result(root)
    study_root = build.artifacts.root / "studies" / _slug(label)
    runs_root = study_root / "runs"
    runs_root.mkdir(parents=True, exist_ok=True)

    runs: list[WhatIfBenchmarkStudyRun] = []
    for model_id in model_ids:
        for seed in seeds:
            run_root = runs_root / str(model_id) / f"seed_{seed}"
            run_root.mkdir(parents=True, exist_ok=True)
            train_result = train_branch_point_benchmark_model(
                build.artifacts.root,
                model_id=model_id,
                epochs=epochs,
                batch_size=batch_size,
                learning_rate=learning_rate,
                seed=seed,
                device=device,
                runtime_root=runtime_root,
                output_root=run_root,
            )
            eval_result = evaluate_branch_point_benchmark_model(
                build.artifacts.root,
                model_id=model_id,
                judged_rankings_path=judged_rankings_path,
                audit_records_path=audit_records_path,
                panel_judgments_path=panel_judgments_path,
                research_pack_root=research_pack_root,
                device=device,
                runtime_root=runtime_root,
                output_root=run_root,
            )
            runs.append(
                _benchmark_study_run(
                    model_id=model_id,
                    seed=seed,
                    train_result=train_result,
                    eval_result=eval_result,
                )
            )

    summaries = [
        _study_model_summary(model_id=model_id, runs=runs) for model_id in model_ids
    ]
    result_path = study_root / "benchmark_study_result.json"
    overview_path = study_root / "benchmark_study_overview.md"
    result = WhatIfBenchmarkStudyResult(
        label=label,
        build_root=build.artifacts.root,
        models=list(model_ids),
        seeds=list(seeds),
        runs=runs,
        summaries=summaries,
        ranked_model_ids=_rank_study_models(summaries),
        notes=[
            f"run_count={len(runs)}",
            f"model_count={len(model_ids)}",
            f"seed_count={len(seeds)}",
            "ranking_metric=dominance_pass_rate_mean",
        ],
        artifacts=WhatIfBenchmarkStudyArtifacts(
            root=study_root,
            result_path=result_path,
            overview_path=overview_path,
        ),
    )
    result_path.write_text(result.model_dump_json(indent=2), encoding="utf-8")
    from .render import render_benchmark_study

    overview_path.write_text(render_benchmark_study(result), encoding="utf-8")
    return result


def outcome_targets_to_signals(
    targets: WhatIfObservedOutcomeTargets,
) -> WhatIfOutcomeSignals:
    future_count = max(targets.future_message_count, 1)
    outside_ratio = targets.external_send_count / future_count
    attachment_ratio = targets.attachment_recirculation_count / future_count
    escalation_ratio = targets.escalation_count / future_count
    reassurance_ratio = targets.reassurance_count / max(targets.future_message_count, 2)
    exposure = _clamp(
        (outside_ratio * 0.65) + (attachment_ratio * 0.2) + (escalation_ratio * 0.15)
    )
    delay = _clamp(
        (_delay_norm(targets.first_follow_up_delay_ms) * 0.35)
        + (_delay_norm(targets.avg_follow_up_delay_ms) * 0.35)
        + (_message_count_norm(targets.future_message_count) * 0.15)
        + (_delay_norm(targets.thread_end_duration_ms) * 0.15)
    )
    relationship = _clamp(
        (reassurance_ratio * 0.3) + ((1.0 - delay) * 0.35) + ((1.0 - exposure) * 0.35)
    )
    return WhatIfOutcomeSignals(
        exposure_risk=round(exposure, 3),
        delay_risk=round(delay, 3),
        relationship_protection=round(relationship, 3),
        message_count=targets.future_message_count,
        outside_message_count=targets.external_send_count,
        avg_delay_ms=targets.avg_follow_up_delay_ms,
        internal_only=not targets.any_external_send,
        reassurance_count=targets.reassurance_count,
    )


def summarize_observed_targets(
    *,
    branch_event: WhatIfEvent,
    future_events: Sequence[WhatIfEvent],
    organization_domain: str,
) -> WhatIfObservedOutcomeTargets:
    if not future_events:
        return WhatIfObservedOutcomeTargets()
    external_send_count = sum(
        _event_external_count(
            event,
            organization_domain=organization_domain,
        )
        for event in future_events
    )
    delays = [
        max(0, event.timestamp_ms - branch_event.timestamp_ms)
        for event in future_events
    ]
    return WhatIfObservedOutcomeTargets(
        any_external_send=external_send_count > 0,
        external_send_count=external_send_count,
        future_message_count=len(future_events),
        thread_end_duration_ms=max(
            0, future_events[-1].timestamp_ms - branch_event.timestamp_ms
        ),
        first_follow_up_delay_ms=max(
            0,
            future_events[0].timestamp_ms - branch_event.timestamp_ms,
        ),
        avg_follow_up_delay_ms=round(sum(delays) / max(len(delays), 1)),
        escalation_count=sum(
            1
            for event in future_events
            if event.flags.is_escalation or event.event_type == "escalation"
        ),
        legal_involvement_count=sum(
            1 for event in future_events if event.flags.consult_legal_specialist
        ),
        attachment_recirculation_count=sum(
            1 for event in future_events if event.flags.has_attachment_reference
        ),
        reassurance_count=sum(
            _event_reassurance_count(event) for event in future_events
        ),
    )


def list_panel_judgments(
    path: str | Path | None,
) -> list[WhatIfPanelJudgment]:
    if path is None:
        return []
    judgment_path = Path(path).expanduser().resolve()
    if not judgment_path.exists():
        return []
    payload = json.loads(judgment_path.read_text(encoding="utf-8"))
    return [WhatIfPanelJudgment.model_validate(item) for item in payload]


def list_judged_rankings(
    path: str | Path | None,
) -> list[WhatIfJudgedRanking]:
    if path is None:
        return []
    result_path = Path(path).expanduser().resolve()
    if not result_path.exists():
        return []
    payload = json.loads(result_path.read_text(encoding="utf-8"))
    if isinstance(payload, dict) and "judgments" in payload:
        payload = payload["judgments"]
    return [WhatIfJudgedRanking.model_validate(item) for item in payload]


def list_audit_records(
    path: str | Path | None,
) -> list[WhatIfAuditRecord]:
    if path is None:
        return []
    result_path = Path(path).expanduser().resolve()
    if not result_path.exists():
        return []
    payload = json.loads(result_path.read_text(encoding="utf-8"))
    if isinstance(payload, dict) and "audit_queue" in payload:
        payload = payload["audit_queue"]
    return [WhatIfAuditRecord.model_validate(item) for item in payload]


def _judge_case_objective(
    *,
    build_root: Path,
    case: WhatIfBenchmarkCase,
    objective_pack_id: WhatIfBusinessObjectivePackId,
    model: str,
    judge_id: str,
) -> WhatIfJudgedRanking:
    dossier_path = case.objective_dossier_paths.get(objective_pack_id)
    if not dossier_path:
        raise ValueError(
            f"missing objective dossier for {case.case_id} {objective_pack_id}"
        )
    dossier_text = Path(dossier_path).read_text(encoding="utf-8")
    candidate_pairs = _candidate_pairs(case.candidates)
    pair_lines = [
        f"- {left.candidate_id} vs {right.candidate_id}"
        for left, right in candidate_pairs
    ]
    prompt = "\n".join(
        [
            dossier_text,
            "",
            "## Pairwise Comparisons",
            *pair_lines,
            "",
            "Return valid JSON with this shape:",
            "{",
            '  "pairwise_comparisons": [',
            "    {",
            '      "left_candidate_id": "candidate_a",',
            '      "right_candidate_id": "candidate_b",',
            '      "preferred_candidate_id": "candidate_a",',
            '      "confidence": 0.0,',
            '      "evidence_references": ["short quote or fact"],',
            '      "rationale": "short reason"',
            "    }",
            "  ],",
            '  "confidence": 0.0,',
            '  "evidence_references": ["short quote or fact"],',
            '  "notes": "short note"',
            "}",
            "Choose only from the listed candidate ids. Use pairwise judgments only. Keep every evidence reference brief.",
        ]
    )
    raw = run_llm_judge_prompt(
        prompt,
        model=model,
        max_tokens=1400,
        temperature=0.0,
        json_mode=True,
    )
    payload = json.loads(raw)
    pairwise = [
        WhatIfJudgedPairwiseComparison.model_validate(item)
        for item in payload.get("pairwise_comparisons", [])
    ]
    ordered_candidate_ids = _aggregate_pairwise_rank(
        pairwise=pairwise,
        candidate_ids=[candidate.candidate_id for candidate in case.candidates],
    )
    confidence = _coerce_confidence(payload.get("confidence"))
    uncertainty_flag = _ranking_is_uncertain(
        ordered_candidate_ids=ordered_candidate_ids,
        candidate_ids=[candidate.candidate_id for candidate in case.candidates],
        pairwise=pairwise,
        confidence=confidence,
    )
    return WhatIfJudgedRanking(
        case_id=case.case_id,
        objective_pack_id=objective_pack_id,
        judge_id=judge_id,
        judge_model=model,
        ordered_candidate_ids=ordered_candidate_ids,
        pairwise_comparisons=pairwise,
        confidence=confidence,
        uncertainty_flag=uncertainty_flag,
        evidence_references=[
            str(item) for item in payload.get("evidence_references", [])[:6]
        ],
        notes=str(payload.get("notes", "") or ""),
    )


def _candidate_pairs(
    candidates: Sequence[WhatIfBenchmarkCandidate],
) -> list[tuple[WhatIfBenchmarkCandidate, WhatIfBenchmarkCandidate]]:
    pairs: list[tuple[WhatIfBenchmarkCandidate, WhatIfBenchmarkCandidate]] = []
    for index, left in enumerate(candidates):
        for right in candidates[index + 1 :]:
            pairs.append((left, right))
    return pairs


def _aggregate_pairwise_rank(
    *,
    pairwise: Sequence[WhatIfJudgedPairwiseComparison],
    candidate_ids: Sequence[str],
) -> list[str]:
    wins = {candidate_id: 0 for candidate_id in candidate_ids}
    evidence_counts = {candidate_id: 0 for candidate_id in candidate_ids}
    for comparison in pairwise:
        preferred = comparison.preferred_candidate_id
        if preferred not in wins:
            continue
        wins[preferred] += 1
        evidence_counts[preferred] += len(comparison.evidence_references)
    return sorted(
        candidate_ids,
        key=lambda candidate_id: (
            -wins[candidate_id],
            -evidence_counts[candidate_id],
            candidate_id,
        ),
    )


def _coerce_confidence(value: object) -> float | None:
    if value is None:
        return None
    try:
        return round(max(0.0, min(1.0, float(value))), 3)
    except (TypeError, ValueError):
        return None


def _ranking_is_uncertain(
    *,
    ordered_candidate_ids: Sequence[str],
    candidate_ids: Sequence[str],
    pairwise: Sequence[WhatIfJudgedPairwiseComparison],
    confidence: float | None,
) -> bool:
    if confidence is not None and confidence < 0.65:
        return True
    if len(ordered_candidate_ids) < len(candidate_ids):
        return True
    seen_pairs = {
        tuple(sorted((comparison.left_candidate_id, comparison.right_candidate_id)))
        for comparison in pairwise
        if comparison.preferred_candidate_id
    }
    expected_pairs = {
        tuple(sorted((left, right)))
        for index, left in enumerate(candidate_ids)
        for right in candidate_ids[index + 1 :]
    }
    return seen_pairs != expected_pairs


def _build_audit_queue(
    judgments: Sequence[WhatIfJudgedRanking],
) -> list[WhatIfAuditRecord]:
    queue: list[WhatIfAuditRecord] = []
    for judgment in judgments:
        if judgment.uncertainty_flag or (
            judgment.confidence is not None and judgment.confidence < 0.65
        ):
            queue.append(
                WhatIfAuditRecord(
                    case_id=judgment.case_id,
                    objective_pack_id=judgment.objective_pack_id,
                    status="pending",
                )
            )
            continue
        sample_key = sha256(
            f"{judgment.case_id}:{judgment.objective_pack_id}".encode("utf-8")
        ).hexdigest()
        if int(sample_key[:8], 16) % 4 == 0:
            queue.append(
                WhatIfAuditRecord(
                    case_id=judgment.case_id,
                    objective_pack_id=judgment.objective_pack_id,
                    status="pending",
                )
            )
    return queue


def _resolve_case_threads(
    pack: WhatIfResearchPack,
    *,
    world: WhatIfWorld,
) -> WhatIfResearchPack:
    resolved_cases: list[WhatIfResearchCase] = []
    for case in pack.cases:
        if case.thread_id:
            resolved_cases.append(case)
            continue
        event = event_by_id(world.events, case.event_id)
        if event is None:
            raise ValueError(f"event not found in world: {case.event_id}")
        resolved_cases.append(case.model_copy(update={"thread_id": event.thread_id}))
    return pack.model_copy(update={"cases": resolved_cases}, deep=True)


def _resolve_benchmark_case_specs(
    heldout_pack_id: str,
    *,
    world: WhatIfWorld,
) -> list[_ResolvedBenchmarkCaseSpec]:
    if heldout_pack_id in _BENCHMARK_CASE_PACKS:
        return _resolved_cases_from_seed_pack(
            _BENCHMARK_CASE_PACKS[heldout_pack_id],
            world=world,
            organization_domain=world.summary.organization_domain,
        )
    return _resolved_cases_from_research_pack(
        _resolve_case_threads(get_research_pack(heldout_pack_id), world=world)
    )


def _resolved_cases_from_seed_pack(
    seeds: Sequence[_BenchmarkCaseSeed],
    *,
    world: WhatIfWorld,
    organization_domain: str,
) -> list[_ResolvedBenchmarkCaseSpec]:
    resolved: list[_ResolvedBenchmarkCaseSpec] = []
    for seed in seeds:
        event = event_by_id(world.events, seed.event_id)
        if event is None:
            raise ValueError(f"benchmark case event not found: {seed.event_id}")
        historical_action = _action_schema_from_event(
            event,
            organization_domain=organization_domain,
        )
        resolved.append(
            _ResolvedBenchmarkCaseSpec(
                case_id=seed.case_id,
                title=seed.title,
                event_id=seed.event_id,
                thread_id=event.thread_id,
                summary=seed.summary,
                case_family=seed.family,
                candidates=_family_candidates(
                    seed.family,
                    historical_action=historical_action,
                ),
            )
        )
    return resolved


def _resolved_cases_from_research_pack(
    pack: WhatIfResearchPack,
) -> list[_ResolvedBenchmarkCaseSpec]:
    resolved: list[_ResolvedBenchmarkCaseSpec] = []
    for case in pack.cases:
        resolved.append(
            _ResolvedBenchmarkCaseSpec(
                case_id=case.case_id,
                title=case.title,
                event_id=case.event_id,
                thread_id=case.thread_id or "",
                summary=case.summary,
                case_family="legacy",
                candidates=[
                    WhatIfBenchmarkCandidate(
                        candidate_id=candidate.candidate_id,
                        label=candidate.label,
                        prompt=candidate.prompt,
                        action_schema=WhatIfActionSchema(
                            event_type="email",
                            recipient_scope="mixed",
                            review_path="business_owner",
                            coordination_breadth="targeted",
                            outside_sharing_posture="limited_external",
                            decision_posture="review",
                            action_tags=sorted(intervention_tags(candidate.prompt)),
                        ),
                        expected_hypotheses=_default_business_hypotheses(
                            "middle_expected"
                        ),
                    )
                    for candidate in case.candidates
                ],
            )
        )
    return resolved


def _family_candidates(
    family: str,
    *,
    historical_action: WhatIfActionSchema,
) -> list[WhatIfBenchmarkCandidate]:
    if family == "outside_sharing":
        return [
            _benchmark_candidate(
                candidate_id="internal_hold_legal",
                label="Internal hold and legal review",
                prompt="Keep the material inside Enron, route it through legal first, and hold the outside send until a clean version is ready.",
                action_schema=_copy_action(
                    historical_action,
                    recipient_scope="internal",
                    external_recipient_count=0,
                    hold_required=True,
                    legal_review_required=True,
                    review_path="internal_legal",
                    coordination_breadth="narrow",
                    outside_sharing_posture="internal_only",
                    decision_posture="hold",
                    action_tags=["hold", "legal", "internal_only"],
                ),
                expected_hypotheses={
                    "minimize_enterprise_risk": "best_expected",
                    "protect_commercial_position": "middle_expected",
                    "reduce_org_strain": "middle_expected",
                    "preserve_stakeholder_trust": "middle_expected",
                    "maintain_execution_velocity": "worst_expected",
                },
            ),
            _benchmark_candidate(
                candidate_id="sanitized_status_note",
                label="Sanitized status note",
                prompt="Send one short status note with no attachment, set a clear owner, and promise the cleaned-up next step.",
                action_schema=_copy_action(
                    historical_action,
                    recipient_scope="external",
                    external_recipient_count=1,
                    attachment_policy="sanitized",
                    owner_clarity="single_owner",
                    reassurance_style="high",
                    review_path="business_owner",
                    coordination_breadth="single_owner",
                    outside_sharing_posture="status_only",
                    decision_posture="resolve",
                    action_tags=["status_note", "single_owner", "sanitized"],
                ),
                expected_hypotheses={
                    "minimize_enterprise_risk": "middle_expected",
                    "protect_commercial_position": "best_expected",
                    "reduce_org_strain": "best_expected",
                    "preserve_stakeholder_trust": "best_expected",
                    "maintain_execution_velocity": "best_expected",
                },
            ),
            _benchmark_candidate(
                candidate_id="limited_external_review",
                label="Limited external review",
                prompt="Keep the external loop tight, send only the minimum needed material, and ask one outside reviewer for targeted comments.",
                action_schema=_copy_action(
                    historical_action,
                    recipient_scope="external",
                    external_recipient_count=1,
                    review_path="outside_counsel",
                    coordination_breadth="targeted",
                    outside_sharing_posture="limited_external",
                    decision_posture="review",
                    action_tags=["outside_counsel", "targeted_review"],
                ),
                expected_hypotheses=_default_business_hypotheses("middle_expected"),
            ),
            _benchmark_candidate(
                candidate_id="broad_external_send",
                label="Broad external send",
                prompt="Send the material now, widen the outside circulation, and ask for fast comments from the broader group.",
                action_schema=_copy_action(
                    historical_action,
                    recipient_scope="mixed",
                    external_recipient_count=max(
                        3, historical_action.external_recipient_count or 2
                    ),
                    attachment_policy="present",
                    escalation_level="manager",
                    owner_clarity="multi_owner",
                    review_path="cross_functional",
                    coordination_breadth="broad",
                    outside_sharing_posture="broad_external",
                    decision_posture="resolve",
                    action_tags=["broad", "external", "multi_owner"],
                ),
                expected_hypotheses={
                    "minimize_enterprise_risk": "worst_expected",
                    "protect_commercial_position": "worst_expected",
                    "reduce_org_strain": "worst_expected",
                    "preserve_stakeholder_trust": "worst_expected",
                    "maintain_execution_velocity": "middle_expected",
                },
            ),
        ]
    if family == "legal_contract":
        return [
            _benchmark_candidate(
                candidate_id="legal_hold_consolidate",
                label="Legal hold and consolidate",
                prompt="Keep the thread in the legal lane, consolidate comments internally, and move only one clean answer back out.",
                action_schema=_copy_action(
                    historical_action,
                    hold_required=True,
                    legal_review_required=True,
                    review_path="internal_legal",
                    coordination_breadth="narrow",
                    outside_sharing_posture="internal_only",
                    decision_posture="hold",
                    action_tags=["legal", "hold", "consolidated"],
                ),
                expected_hypotheses={
                    "minimize_enterprise_risk": "best_expected",
                    "protect_commercial_position": "middle_expected",
                    "reduce_org_strain": "best_expected",
                    "preserve_stakeholder_trust": "middle_expected",
                    "maintain_execution_velocity": "worst_expected",
                },
            ),
            _benchmark_candidate(
                candidate_id="outside_counsel_only",
                label="Outside counsel only",
                prompt="Limit the outside loop to one counsel contact and one Enron owner, then ask for a single written view.",
                action_schema=_copy_action(
                    historical_action,
                    recipient_scope="external",
                    external_recipient_count=1,
                    review_path="outside_counsel",
                    coordination_breadth="targeted",
                    outside_sharing_posture="limited_external",
                    decision_posture="review",
                    action_tags=["outside_counsel", "single_owner"],
                ),
                expected_hypotheses={
                    "minimize_enterprise_risk": "middle_expected",
                    "protect_commercial_position": "middle_expected",
                    "reduce_org_strain": "middle_expected",
                    "preserve_stakeholder_trust": "best_expected",
                    "maintain_execution_velocity": "middle_expected",
                },
            ),
            _benchmark_candidate(
                candidate_id="business_owner_alignment",
                label="Business owner alignment",
                prompt="Keep one business owner with legal, align internally fast, and send the clean next step once the owner is ready.",
                action_schema=_copy_action(
                    historical_action,
                    owner_clarity="single_owner",
                    review_path="business_owner",
                    coordination_breadth="targeted",
                    outside_sharing_posture="status_only",
                    decision_posture="resolve",
                    action_tags=["business_owner", "single_owner", "fast_align"],
                ),
                expected_hypotheses={
                    "minimize_enterprise_risk": "middle_expected",
                    "protect_commercial_position": "best_expected",
                    "reduce_org_strain": "middle_expected",
                    "preserve_stakeholder_trust": "middle_expected",
                    "maintain_execution_velocity": "best_expected",
                },
            ),
            _benchmark_candidate(
                candidate_id="broad_comment_round",
                label="Broad comment round",
                prompt="Recirculate the draft broadly and ask multiple reviewers to comment in parallel for speed.",
                action_schema=_copy_action(
                    historical_action,
                    recipient_scope="mixed",
                    external_recipient_count=max(
                        2, historical_action.external_recipient_count or 1
                    ),
                    owner_clarity="multi_owner",
                    review_path="cross_functional",
                    coordination_breadth="broad",
                    outside_sharing_posture="broad_external",
                    decision_posture="review",
                    action_tags=["broad", "comments", "multi_owner"],
                ),
                expected_hypotheses={
                    "minimize_enterprise_risk": "worst_expected",
                    "protect_commercial_position": "worst_expected",
                    "reduce_org_strain": "worst_expected",
                    "preserve_stakeholder_trust": "worst_expected",
                    "maintain_execution_velocity": "middle_expected",
                },
            ),
        ]
    if family == "commercial_counterparty":
        return [
            _benchmark_candidate(
                candidate_id="owner_led_internal_alignment",
                label="Owner-led internal alignment",
                prompt="Name one owner, align the internal team first, and delay the external move until the message is clean.",
                action_schema=_copy_action(
                    historical_action,
                    recipient_scope="internal",
                    external_recipient_count=0,
                    owner_clarity="single_owner",
                    hold_required=True,
                    review_path="business_owner",
                    coordination_breadth="single_owner",
                    outside_sharing_posture="internal_only",
                    decision_posture="hold",
                    action_tags=["single_owner", "internal_align", "hold"],
                ),
                expected_hypotheses={
                    "minimize_enterprise_risk": "best_expected",
                    "protect_commercial_position": "middle_expected",
                    "reduce_org_strain": "best_expected",
                    "preserve_stakeholder_trust": "middle_expected",
                    "maintain_execution_velocity": "worst_expected",
                },
            ),
            _benchmark_candidate(
                candidate_id="narrow_counterparty_status",
                label="Narrow counterparty status",
                prompt="Send one short counterpart update, keep the scope narrow, and give a clear commitment on the next step.",
                action_schema=_copy_action(
                    historical_action,
                    recipient_scope="external",
                    external_recipient_count=1,
                    attachment_policy="sanitized",
                    owner_clarity="single_owner",
                    reassurance_style="high",
                    review_path="business_owner",
                    coordination_breadth="single_owner",
                    outside_sharing_posture="status_only",
                    decision_posture="resolve",
                    action_tags=["counterparty", "status_note", "single_owner"],
                ),
                expected_hypotheses={
                    "minimize_enterprise_risk": "middle_expected",
                    "protect_commercial_position": "best_expected",
                    "reduce_org_strain": "best_expected",
                    "preserve_stakeholder_trust": "best_expected",
                    "maintain_execution_velocity": "best_expected",
                },
            ),
            _benchmark_candidate(
                candidate_id="send_clean_draft_fast",
                label="Send clean draft fast",
                prompt="Move quickly with a clean draft to the counterpart after one fast internal check.",
                action_schema=_copy_action(
                    historical_action,
                    recipient_scope="external",
                    external_recipient_count=max(
                        1, historical_action.external_recipient_count
                    ),
                    attachment_policy="present",
                    owner_clarity="single_owner",
                    review_path="business_owner",
                    coordination_breadth="targeted",
                    outside_sharing_posture="limited_external",
                    decision_posture="resolve",
                    action_tags=["clean_draft", "fast_send"],
                ),
                expected_hypotheses=_default_business_hypotheses("middle_expected"),
            ),
            _benchmark_candidate(
                candidate_id="broad_counterparty_escalation",
                label="Broad counterparty escalation",
                prompt="Escalate broadly across the counterpart and internal teams to force a faster answer.",
                action_schema=_copy_action(
                    historical_action,
                    recipient_scope="mixed",
                    external_recipient_count=max(
                        3, historical_action.external_recipient_count or 2
                    ),
                    escalation_level="executive",
                    owner_clarity="multi_owner",
                    review_path="executive",
                    coordination_breadth="broad",
                    outside_sharing_posture="broad_external",
                    decision_posture="escalate",
                    action_tags=["executive_gate", "broad", "counterparty"],
                ),
                expected_hypotheses={
                    "minimize_enterprise_risk": "worst_expected",
                    "protect_commercial_position": "worst_expected",
                    "reduce_org_strain": "worst_expected",
                    "preserve_stakeholder_trust": "worst_expected",
                    "maintain_execution_velocity": "middle_expected",
                },
            ),
        ]
    if family == "whistleblower":
        return [
            _benchmark_candidate(
                candidate_id="audit_committee_and_andersen",
                label="Escalate to the audit committee",
                prompt="Escalate the warning to the audit committee, copy one Andersen contact, and force a formal accounting review before anything else moves.",
                action_schema=_copy_action(
                    historical_action,
                    recipient_scope="mixed",
                    external_recipient_count=1,
                    hold_required=True,
                    legal_review_required=True,
                    escalation_level="executive",
                    owner_clarity="single_owner",
                    review_path="executive",
                    coordination_breadth="targeted",
                    outside_sharing_posture="limited_external",
                    decision_posture="escalate",
                    action_tags=["audit_committee", "executive_gate", "accounting"],
                ),
                expected_hypotheses={
                    "minimize_enterprise_risk": "best_expected",
                    "protect_commercial_position": "middle_expected",
                    "reduce_org_strain": "middle_expected",
                    "preserve_stakeholder_trust": "best_expected",
                    "maintain_execution_velocity": "worst_expected",
                },
            ),
            _benchmark_candidate(
                candidate_id="hold_for_vinson_review",
                label="Hold for Vinson & Elkins review",
                prompt="Hold the issue inside a small legal circle, route it to Vinson & Elkins, and wait for outside-counsel guidance before wider disclosure.",
                action_schema=_copy_action(
                    historical_action,
                    recipient_scope="mixed",
                    external_recipient_count=1,
                    hold_required=True,
                    legal_review_required=True,
                    review_path="outside_counsel",
                    coordination_breadth="targeted",
                    outside_sharing_posture="limited_external",
                    decision_posture="hold",
                    action_tags=["outside_counsel", "hold", "targeted_review"],
                ),
                expected_hypotheses=_default_business_hypotheses("middle_expected"),
            ),
            _benchmark_candidate(
                candidate_id="anonymous_warning",
                label="Send an anonymous warning",
                prompt="Push a narrow warning to Ken Lay, keep the loop tiny, and ask for a direct answer without opening a larger circulation list.",
                action_schema=_copy_action(
                    historical_action,
                    recipient_scope="internal",
                    external_recipient_count=0,
                    escalation_level="executive",
                    owner_clarity="single_owner",
                    review_path="executive",
                    coordination_breadth="targeted",
                    outside_sharing_posture="status_only",
                    decision_posture="escalate",
                    action_tags=["executive_gate", "single_owner", "quiet_escalation"],
                ),
                expected_hypotheses={
                    "minimize_enterprise_risk": "middle_expected",
                    "protect_commercial_position": "middle_expected",
                    "reduce_org_strain": "best_expected",
                    "preserve_stakeholder_trust": "middle_expected",
                    "maintain_execution_velocity": "middle_expected",
                },
            ),
            _benchmark_candidate(
                candidate_id="suppress_and_monitor",
                label="Suppress and monitor",
                prompt="Keep the concern private, avoid wider disclosure, and monitor the situation without opening a formal review.",
                action_schema=_copy_action(
                    historical_action,
                    recipient_scope="internal",
                    external_recipient_count=0,
                    hold_required=False,
                    legal_review_required=False,
                    owner_clarity="unclear",
                    review_path="none",
                    coordination_breadth="single_owner",
                    outside_sharing_posture="internal_only",
                    decision_posture="hold",
                    action_tags=["private", "monitor", "suppress"],
                ),
                expected_hypotheses={
                    "minimize_enterprise_risk": "worst_expected",
                    "protect_commercial_position": "worst_expected",
                    "reduce_org_strain": "best_expected",
                    "preserve_stakeholder_trust": "worst_expected",
                    "maintain_execution_velocity": "best_expected",
                },
            ),
        ]
    if family == "market_manipulation":
        return [
            _benchmark_candidate(
                candidate_id="halt_and_self_report",
                label="Halt and self-report",
                prompt="Stop the strategy, preserve the record, and self-report the exposure to FERC and internal legal review immediately.",
                action_schema=_copy_action(
                    historical_action,
                    recipient_scope="mixed",
                    external_recipient_count=1,
                    hold_required=True,
                    legal_review_required=True,
                    escalation_level="executive",
                    owner_clarity="single_owner",
                    review_path="internal_legal",
                    coordination_breadth="targeted",
                    outside_sharing_posture="limited_external",
                    decision_posture="escalate",
                    action_tags=["hold", "legal", "regulatory", "self_report"],
                ),
                expected_hypotheses={
                    "minimize_enterprise_risk": "best_expected",
                    "protect_commercial_position": "middle_expected",
                    "reduce_org_strain": "middle_expected",
                    "preserve_stakeholder_trust": "best_expected",
                    "maintain_execution_velocity": "worst_expected",
                },
            ),
            _benchmark_candidate(
                candidate_id="skilling_signoff",
                label="Ask for Skilling sign-off",
                prompt="Keep the thread tight, write a short internal memo to Skilling, and require explicit executive sign-off before the strategy continues.",
                action_schema=_copy_action(
                    historical_action,
                    recipient_scope="internal",
                    external_recipient_count=0,
                    escalation_level="executive",
                    owner_clarity="single_owner",
                    review_path="executive",
                    coordination_breadth="targeted",
                    outside_sharing_posture="internal_only",
                    decision_posture="review",
                    action_tags=["executive_gate", "single_owner", "trading"],
                ),
                expected_hypotheses=_default_business_hypotheses("middle_expected"),
            ),
            _benchmark_candidate(
                candidate_id="proceed_narrow",
                label="Proceed with narrow circulation",
                prompt="Proceed with the strategy, keep the distribution list tight, and avoid language that widens the compliance footprint.",
                action_schema=_copy_action(
                    historical_action,
                    recipient_scope="internal",
                    external_recipient_count=0,
                    owner_clarity="single_owner",
                    review_path="business_owner",
                    coordination_breadth="narrow",
                    outside_sharing_posture="status_only",
                    decision_posture="resolve",
                    action_tags=["single_owner", "status_only", "narrow_loop"],
                ),
                expected_hypotheses={
                    "minimize_enterprise_risk": "worst_expected",
                    "protect_commercial_position": "best_expected",
                    "reduce_org_strain": "middle_expected",
                    "preserve_stakeholder_trust": "worst_expected",
                    "maintain_execution_velocity": "best_expected",
                },
            ),
            _benchmark_candidate(
                candidate_id="proceed_and_widen",
                label="Proceed and widen circulation",
                prompt="Proceed with the strategy, widen the trading and management loop, and push for a fast answer across the broader group.",
                action_schema=_copy_action(
                    historical_action,
                    recipient_scope="mixed",
                    external_recipient_count=max(
                        2, historical_action.external_recipient_count or 1
                    ),
                    escalation_level="executive",
                    owner_clarity="multi_owner",
                    review_path="cross_functional",
                    coordination_breadth="broad",
                    outside_sharing_posture="broad_external",
                    decision_posture="resolve",
                    action_tags=["send_now", "widen_loop", "broad"],
                ),
                expected_hypotheses={
                    "minimize_enterprise_risk": "worst_expected",
                    "protect_commercial_position": "worst_expected",
                    "reduce_org_strain": "worst_expected",
                    "preserve_stakeholder_trust": "worst_expected",
                    "maintain_execution_velocity": "middle_expected",
                },
            ),
        ]
    if family == "crisis_communication":
        return [
            _benchmark_candidate(
                candidate_id="board_audit_disclosure",
                label="Board and audit disclosure",
                prompt="Escalate the draft to the board and audit committee, keep one owner on the message, and align disclosure language before the broader release.",
                action_schema=_copy_action(
                    historical_action,
                    recipient_scope="internal",
                    external_recipient_count=0,
                    hold_required=True,
                    escalation_level="executive",
                    owner_clarity="single_owner",
                    review_path="executive",
                    coordination_breadth="targeted",
                    outside_sharing_posture="internal_only",
                    decision_posture="escalate",
                    action_tags=["executive_gate", "hold", "single_owner"],
                ),
                expected_hypotheses={
                    "minimize_enterprise_risk": "best_expected",
                    "protect_commercial_position": "middle_expected",
                    "reduce_org_strain": "middle_expected",
                    "preserve_stakeholder_trust": "best_expected",
                    "maintain_execution_velocity": "worst_expected",
                },
            ),
            _benchmark_candidate(
                candidate_id="controlled_internal_draft",
                label="Controlled internal draft",
                prompt="Keep the communications draft internal, use one owner and one legal reviewer, and hold external statements until the draft is settled.",
                action_schema=_copy_action(
                    historical_action,
                    recipient_scope="internal",
                    external_recipient_count=0,
                    hold_required=True,
                    legal_review_required=True,
                    owner_clarity="single_owner",
                    review_path="internal_legal",
                    coordination_breadth="narrow",
                    outside_sharing_posture="internal_only",
                    decision_posture="hold",
                    action_tags=["hold", "legal", "single_owner"],
                ),
                expected_hypotheses=_default_business_hypotheses("middle_expected"),
            ),
            _benchmark_candidate(
                candidate_id="narrow_external_line",
                label="Narrow external line",
                prompt="Send one narrow status line to the outside audience, keep the detail light, and give one clear next-step owner.",
                action_schema=_copy_action(
                    historical_action,
                    recipient_scope="external",
                    external_recipient_count=1,
                    attachment_policy="sanitized",
                    owner_clarity="single_owner",
                    reassurance_style="high",
                    review_path="business_owner",
                    coordination_breadth="single_owner",
                    outside_sharing_posture="status_only",
                    decision_posture="resolve",
                    action_tags=["status_only", "single_owner", "reassurance"],
                ),
                expected_hypotheses={
                    "minimize_enterprise_risk": "middle_expected",
                    "protect_commercial_position": "best_expected",
                    "reduce_org_strain": "best_expected",
                    "preserve_stakeholder_trust": "middle_expected",
                    "maintain_execution_velocity": "best_expected",
                },
            ),
            _benchmark_candidate(
                candidate_id="broad_reassurance_loop",
                label="Broad reassurance loop",
                prompt="Push the talking points broadly, reassure everyone quickly, and let multiple leaders speak into the message at once.",
                action_schema=_copy_action(
                    historical_action,
                    recipient_scope="mixed",
                    external_recipient_count=max(
                        2, historical_action.external_recipient_count or 1
                    ),
                    escalation_level="executive",
                    owner_clarity="multi_owner",
                    review_path="cross_functional",
                    coordination_breadth="broad",
                    outside_sharing_posture="broad_external",
                    decision_posture="resolve",
                    action_tags=["broad", "multi_owner", "reassurance"],
                ),
                expected_hypotheses={
                    "minimize_enterprise_risk": "worst_expected",
                    "protect_commercial_position": "worst_expected",
                    "reduce_org_strain": "worst_expected",
                    "preserve_stakeholder_trust": "worst_expected",
                    "maintain_execution_velocity": "middle_expected",
                },
            ),
        ]
    if family == "accounting_disclosure":
        return [
            _benchmark_candidate(
                candidate_id="disclose_and_restate",
                label="Disclose and restate",
                prompt="Disclose the issue, route it through legal and accounting immediately, and prepare the restatement path before quarter-end pressure widens.",
                action_schema=_copy_action(
                    historical_action,
                    recipient_scope="internal",
                    external_recipient_count=0,
                    hold_required=True,
                    legal_review_required=True,
                    escalation_level="executive",
                    owner_clarity="single_owner",
                    review_path="internal_legal",
                    coordination_breadth="targeted",
                    outside_sharing_posture="internal_only",
                    decision_posture="escalate",
                    action_tags=["hold", "legal", "accounting", "restatement"],
                ),
                expected_hypotheses={
                    "minimize_enterprise_risk": "best_expected",
                    "protect_commercial_position": "worst_expected",
                    "reduce_org_strain": "middle_expected",
                    "preserve_stakeholder_trust": "best_expected",
                    "maintain_execution_velocity": "worst_expected",
                },
            ),
            _benchmark_candidate(
                candidate_id="hold_for_reconciliation",
                label="Hold for clean reconciliation",
                prompt="Keep the issue inside a small finance and legal loop, reconcile the numbers, and hold the wider message until the clean story is ready.",
                action_schema=_copy_action(
                    historical_action,
                    recipient_scope="internal",
                    external_recipient_count=0,
                    hold_required=True,
                    legal_review_required=True,
                    owner_clarity="single_owner",
                    review_path="cross_functional",
                    coordination_breadth="narrow",
                    outside_sharing_posture="internal_only",
                    decision_posture="hold",
                    action_tags=["hold", "cross_functional", "single_owner"],
                ),
                expected_hypotheses=_default_business_hypotheses("middle_expected"),
            ),
            _benchmark_candidate(
                candidate_id="limited_restructure",
                label="Restructure in a limited circle",
                prompt="Keep the discussion to a limited finance circle, restructure the path quietly, and avoid opening a broader disclosure loop yet.",
                action_schema=_copy_action(
                    historical_action,
                    recipient_scope="internal",
                    external_recipient_count=0,
                    owner_clarity="single_owner",
                    review_path="business_owner",
                    coordination_breadth="targeted",
                    outside_sharing_posture="status_only",
                    decision_posture="review",
                    action_tags=["single_owner", "targeted_review", "quiet_fix"],
                ),
                expected_hypotheses={
                    "minimize_enterprise_risk": "worst_expected",
                    "protect_commercial_position": "middle_expected",
                    "reduce_org_strain": "middle_expected",
                    "preserve_stakeholder_trust": "worst_expected",
                    "maintain_execution_velocity": "best_expected",
                },
            ),
            _benchmark_candidate(
                candidate_id="push_through_quarter_close",
                label="Push through quarter close",
                prompt="Push the item through quarter close, keep the loop broad enough to execute fast, and postpone the harder accounting clean-up.",
                action_schema=_copy_action(
                    historical_action,
                    recipient_scope="mixed",
                    external_recipient_count=max(
                        1, historical_action.external_recipient_count
                    ),
                    owner_clarity="multi_owner",
                    review_path="cross_functional",
                    coordination_breadth="broad",
                    outside_sharing_posture="broad_external",
                    decision_posture="resolve",
                    action_tags=["send_now", "widen_loop", "quarter_close"],
                ),
                expected_hypotheses={
                    "minimize_enterprise_risk": "worst_expected",
                    "protect_commercial_position": "middle_expected",
                    "reduce_org_strain": "worst_expected",
                    "preserve_stakeholder_trust": "worst_expected",
                    "maintain_execution_velocity": "best_expected",
                },
            ),
        ]
    if family == "executive_regulatory":
        return [
            _benchmark_candidate(
                candidate_id="named_owner_internal",
                label="Named owner internal",
                prompt="Rewrite the issue into one internal note, name one owner, and keep the thread out of a broad escalation loop.",
                action_schema=_copy_action(
                    historical_action,
                    recipient_scope="internal",
                    external_recipient_count=0,
                    owner_clarity="single_owner",
                    review_path="business_owner",
                    coordination_breadth="single_owner",
                    outside_sharing_posture="internal_only",
                    decision_posture="resolve",
                    action_tags=["single_owner", "internal_only", "owner_clarity"],
                ),
                expected_hypotheses={
                    "minimize_enterprise_risk": "middle_expected",
                    "protect_commercial_position": "middle_expected",
                    "reduce_org_strain": "best_expected",
                    "preserve_stakeholder_trust": "middle_expected",
                    "maintain_execution_velocity": "best_expected",
                },
            ),
            _benchmark_candidate(
                candidate_id="legal_regulatory_gate",
                label="Legal and regulatory gate",
                prompt="Keep the issue in the legal and regulatory lane until the answer is settled before sending it wider.",
                action_schema=_copy_action(
                    historical_action,
                    hold_required=True,
                    legal_review_required=True,
                    review_path="internal_legal",
                    coordination_breadth="narrow",
                    outside_sharing_posture="internal_only",
                    decision_posture="hold",
                    action_tags=["legal", "regulatory", "hold"],
                ),
                expected_hypotheses={
                    "minimize_enterprise_risk": "best_expected",
                    "protect_commercial_position": "middle_expected",
                    "reduce_org_strain": "middle_expected",
                    "preserve_stakeholder_trust": "middle_expected",
                    "maintain_execution_velocity": "worst_expected",
                },
            ),
            _benchmark_candidate(
                candidate_id="executive_briefing_only",
                label="Executive briefing only",
                prompt="Brief leadership quietly, keep the working thread narrow, and avoid a larger alarm loop.",
                action_schema=_copy_action(
                    historical_action,
                    escalation_level="executive",
                    review_path="executive",
                    coordination_breadth="targeted",
                    outside_sharing_posture="status_only",
                    decision_posture="escalate",
                    action_tags=["executive_gate", "briefing_only"],
                ),
                expected_hypotheses={
                    "minimize_enterprise_risk": "middle_expected",
                    "protect_commercial_position": "middle_expected",
                    "reduce_org_strain": "worst_expected",
                    "preserve_stakeholder_trust": "middle_expected",
                    "maintain_execution_velocity": "middle_expected",
                },
            ),
            _benchmark_candidate(
                candidate_id="wide_alarm",
                label="Wide alarm",
                prompt="Send the issue broadly with urgent language and ask every stakeholder to weigh in immediately.",
                action_schema=_copy_action(
                    historical_action,
                    escalation_level="executive",
                    owner_clarity="multi_owner",
                    review_path="cross_functional",
                    coordination_breadth="broad",
                    outside_sharing_posture="broad_external",
                    decision_posture="escalate",
                    action_tags=["urgent", "broad", "escalation"],
                ),
                expected_hypotheses={
                    "minimize_enterprise_risk": "worst_expected",
                    "protect_commercial_position": "worst_expected",
                    "reduce_org_strain": "worst_expected",
                    "preserve_stakeholder_trust": "worst_expected",
                    "maintain_execution_velocity": "middle_expected",
                },
            ),
        ]
    if family == "coordination_strain":
        return [
            _benchmark_candidate(
                candidate_id="single_owner_consolidation",
                label="Single-owner consolidation",
                prompt="Collapse the review into one owner, collect comments privately, and move one consolidated answer.",
                action_schema=_copy_action(
                    historical_action,
                    owner_clarity="single_owner",
                    review_path="business_owner",
                    coordination_breadth="single_owner",
                    decision_posture="resolve",
                    action_tags=["single_owner", "consolidated", "internal_align"],
                ),
                expected_hypotheses={
                    "minimize_enterprise_risk": "middle_expected",
                    "protect_commercial_position": "middle_expected",
                    "reduce_org_strain": "best_expected",
                    "preserve_stakeholder_trust": "middle_expected",
                    "maintain_execution_velocity": "best_expected",
                },
            ),
            _benchmark_candidate(
                candidate_id="small_review_circle",
                label="Small review circle",
                prompt="Keep a small review circle, avoid broad recirculation, and use one controlled comment pass.",
                action_schema=_copy_action(
                    historical_action,
                    hold_required=True,
                    owner_clarity="single_owner",
                    review_path="cross_functional",
                    coordination_breadth="narrow",
                    outside_sharing_posture="internal_only",
                    decision_posture="review",
                    action_tags=["small_circle", "review", "single_owner"],
                ),
                expected_hypotheses={
                    "minimize_enterprise_risk": "best_expected",
                    "protect_commercial_position": "middle_expected",
                    "reduce_org_strain": "best_expected",
                    "preserve_stakeholder_trust": "middle_expected",
                    "maintain_execution_velocity": "middle_expected",
                },
            ),
            _benchmark_candidate(
                candidate_id="parallel_review_request",
                label="Parallel review request",
                prompt="Ask a few key reviewers to comment in parallel and keep the review window explicit and short.",
                action_schema=_copy_action(
                    historical_action,
                    owner_clarity="multi_owner",
                    review_path="cross_functional",
                    coordination_breadth="targeted",
                    decision_posture="review",
                    action_tags=["parallel_review", "targeted"],
                ),
                expected_hypotheses=_default_business_hypotheses("middle_expected"),
            ),
            _benchmark_candidate(
                candidate_id="broad_comment_blast",
                label="Broad comment blast",
                prompt="Open the draft to a broad comment round and ask the wider group to respond quickly.",
                action_schema=_copy_action(
                    historical_action,
                    owner_clarity="multi_owner",
                    review_path="cross_functional",
                    coordination_breadth="broad",
                    outside_sharing_posture="broad_external",
                    decision_posture="review",
                    action_tags=["broad", "comments", "blast"],
                ),
                expected_hypotheses={
                    "minimize_enterprise_risk": "worst_expected",
                    "protect_commercial_position": "worst_expected",
                    "reduce_org_strain": "worst_expected",
                    "preserve_stakeholder_trust": "worst_expected",
                    "maintain_execution_velocity": "worst_expected",
                },
            ),
        ]
    return [
        _benchmark_candidate(
            candidate_id="private_manager_channel",
            label="Private manager channel",
            prompt="Keep the issue private, route it through the direct manager, and avoid a broad personnel loop.",
            action_schema=_copy_action(
                historical_action,
                recipient_scope="internal",
                external_recipient_count=0,
                owner_clarity="single_owner",
                review_path="business_owner",
                coordination_breadth="single_owner",
                outside_sharing_posture="internal_only",
                decision_posture="hold",
                action_tags=["private", "manager", "single_owner"],
            ),
            expected_hypotheses={
                "minimize_enterprise_risk": "best_expected",
                "protect_commercial_position": "middle_expected",
                "reduce_org_strain": "best_expected",
                "preserve_stakeholder_trust": "middle_expected",
                "maintain_execution_velocity": "middle_expected",
            },
        ),
        _benchmark_candidate(
            candidate_id="hr_partner_review",
            label="HR partner review",
            prompt="Bring in one HR partner, keep the loop tight, and give a clear path for the next personnel step.",
            action_schema=_copy_action(
                historical_action,
                review_path="hr",
                coordination_breadth="narrow",
                reassurance_style="medium",
                outside_sharing_posture="internal_only",
                decision_posture="review",
                action_tags=["hr", "tight_loop"],
            ),
            expected_hypotheses={
                "minimize_enterprise_risk": "best_expected",
                "protect_commercial_position": "middle_expected",
                "reduce_org_strain": "middle_expected",
                "preserve_stakeholder_trust": "best_expected",
                "maintain_execution_velocity": "middle_expected",
            },
        ),
        _benchmark_candidate(
            candidate_id="small_panel_alignment",
            label="Small panel alignment",
            prompt="Use a small panel of relevant leaders, align the message, and avoid a wider emotional escalation.",
            action_schema=_copy_action(
                historical_action,
                review_path="cross_functional",
                coordination_breadth="narrow",
                owner_clarity="single_owner",
                decision_posture="review",
                action_tags=["small_panel", "alignment"],
            ),
            expected_hypotheses={
                "minimize_enterprise_risk": "middle_expected",
                "protect_commercial_position": "middle_expected",
                "reduce_org_strain": "best_expected",
                "preserve_stakeholder_trust": "best_expected",
                "maintain_execution_velocity": "middle_expected",
            },
        ),
        _benchmark_candidate(
            candidate_id="broad_org_escalation",
            label="Broad organizational escalation",
            prompt="Escalate the issue broadly across the organization to get a faster answer and broader buy-in.",
            action_schema=_copy_action(
                historical_action,
                escalation_level="executive",
                owner_clarity="multi_owner",
                review_path="executive",
                coordination_breadth="broad",
                decision_posture="escalate",
                action_tags=["broad", "executive_gate", "org_heat"],
            ),
            expected_hypotheses={
                "minimize_enterprise_risk": "worst_expected",
                "protect_commercial_position": "worst_expected",
                "reduce_org_strain": "worst_expected",
                "preserve_stakeholder_trust": "worst_expected",
                "maintain_execution_velocity": "worst_expected",
            },
        ),
    ]


def _benchmark_candidate(
    *,
    candidate_id: str,
    label: str,
    prompt: str,
    action_schema: WhatIfActionSchema,
    expected_hypotheses: dict[WhatIfBusinessObjectivePackId, str],
) -> WhatIfBenchmarkCandidate:
    return WhatIfBenchmarkCandidate(
        candidate_id=candidate_id,
        label=label,
        prompt=prompt,
        action_schema=action_schema,
        expected_hypotheses=expected_hypotheses,
    )


def _copy_action(
    base: WhatIfActionSchema,
    **updates: object,
) -> WhatIfActionSchema:
    return base.model_copy(update=updates)


def _default_business_hypotheses(
    default_label: WhatIfResearchHypothesisLabel,
) -> dict[WhatIfBusinessObjectivePackId, WhatIfResearchHypothesisLabel]:
    return {
        objective_pack_id: default_label
        for objective_pack_id in _BUSINESS_OBJECTIVE_PACK_IDS
    }


def _group_events_by_thread(
    events: Sequence[WhatIfEvent],
) -> dict[str, list[WhatIfEvent]]:
    grouped: dict[str, list[WhatIfEvent]] = defaultdict(list)
    for event in sorted(events, key=lambda item: (item.timestamp_ms, item.event_id)):
        grouped[event.thread_id].append(event)
    return grouped


def _split_timeline(
    *,
    timeline: Sequence[WhatIfEvent],
    branch_event_id: str,
) -> tuple[list[WhatIfEvent], list[WhatIfEvent]]:
    branch_index = next(
        (
            index
            for index, event in enumerate(timeline)
            if event.event_id == branch_event_id
        ),
        None,
    )
    if branch_index is None:
        raise ValueError(f"branch event not found in timeline: {branch_event_id}")
    return list(timeline[:branch_index]), list(timeline[branch_index:])


def _assign_temporal_splits(
    rows: Sequence[tuple[int, WhatIfBenchmarkDatasetRow]],
) -> dict[str, list[WhatIfBenchmarkDatasetRow]]:
    ordered = sorted(rows, key=lambda item: item[0])
    count = len(ordered)
    if count <= 1:
        train_cutoff = count
        validation_cutoff = count
    else:
        train_cutoff = max(1, int(count * 0.7))
        validation_cutoff = max(train_cutoff, int(count * 0.85))
    buckets: dict[str, list[WhatIfBenchmarkDatasetRow]] = defaultdict(list)
    for index, (_, row) in enumerate(ordered):
        if index < train_cutoff:
            split: WhatIfBenchmarkSplit = "train"
        elif index < validation_cutoff:
            split = "validation"
        else:
            split = "test"
        buckets[str(split)].append(row.model_copy(update={"split": split}))
    return buckets


def _build_benchmark_cases(
    *,
    world: WhatIfWorld,
    heldout_pack_id: str,
    events_by_thread: dict[str, list[WhatIfEvent]],
    dossier_root: Path,
) -> tuple[list[WhatIfBenchmarkCase], list[WhatIfBenchmarkDatasetRow]]:
    cases: list[WhatIfBenchmarkCase] = []
    heldout_rows: list[WhatIfBenchmarkDatasetRow] = []
    case_specs = _resolve_benchmark_case_specs(heldout_pack_id, world=world)
    for case in case_specs:
        timeline = hydrate_event_snippets(
            rosetta_dir=world.rosetta_dir,
            events=events_by_thread.get(case.thread_id or "", []),
        )
        branch_event = event_by_id(timeline, case.event_id)
        if branch_event is None:
            raise ValueError(f"heldout branch event not found: {case.event_id}")
        history_events, _ = _split_timeline(
            timeline=timeline,
            branch_event_id=branch_event.event_id,
        )
        if world.source == "enron":
            history_events = build_enron_branch_history(
                public_context=world.public_context,
                branch_event=branch_event,
                past_events=history_events,
            )
        historical_action = _action_schema_from_event(
            branch_event,
            organization_domain=world.summary.organization_domain,
        )
        base_contract = _build_pre_branch_contract(
            case_id=case.case_id,
            thread_id=case.thread_id or "",
            branch_event=branch_event,
            history_events=history_events,
            organization_domain=world.summary.organization_domain,
            action_schema=historical_action,
            notes=["Held-out branch-point base contract."],
        )
        branch_public_context = slice_public_context_to_branch(
            world.public_context,
            branch_timestamp=branch_event.timestamp,
        )
        benchmark_case = WhatIfBenchmarkCase(
            case_id=case.case_id,
            title=case.title,
            event_id=case.event_id,
            thread_id=case.thread_id or "",
            summary=case.summary,
            case_family=case.case_family,
            branch_event=event_reference(branch_event),
            history_preview=[event_reference(event) for event in history_events],
            candidates=case.candidates,
            public_context=branch_public_context,
        )
        case_dossier_root = dossier_root / benchmark_case.case_id
        case_dossier_root.mkdir(parents=True, exist_ok=True)
        dossier_paths = _write_case_dossiers(
            case=benchmark_case,
            dossier_root=case_dossier_root,
        )
        benchmark_case = benchmark_case.model_copy(
            update={
                "summary": benchmark_case.summary
                or "Held-out Enron branch-point case.",
                "objective_dossier_paths": dossier_paths,
            }
        )
        cases.append(benchmark_case)
        heldout_rows.append(
            WhatIfBenchmarkDatasetRow(
                row_id=f"{benchmark_case.thread_id}:{benchmark_case.event_id}:heldout",
                split="heldout",
                thread_id=benchmark_case.thread_id,
                branch_event_id=benchmark_case.event_id,
                contract=base_contract,
                observed_evidence_heads=WhatIfObservedEvidenceHeads(),
                observed_business_outcomes=evidence_to_business_outcomes(
                    WhatIfObservedEvidenceHeads()
                ),
            )
        )
    return cases, heldout_rows


def _judge_template_rows(
    cases: Sequence[WhatIfBenchmarkCase],
) -> list[WhatIfJudgedRanking]:
    template: list[WhatIfJudgedRanking] = []
    for case in cases:
        for objective_pack_id in _BUSINESS_OBJECTIVE_PACK_IDS:
            template.append(
                WhatIfJudgedRanking(
                    case_id=case.case_id,
                    objective_pack_id=objective_pack_id,
                    ordered_candidate_ids=[
                        candidate.candidate_id for candidate in case.candidates
                    ],
                    confidence=None,
                )
            )
    return template


def _audit_template_rows(
    cases: Sequence[WhatIfBenchmarkCase],
) -> list[WhatIfAuditRecord]:
    template: list[WhatIfAuditRecord] = []
    for case in cases:
        for objective_pack_id in _BUSINESS_OBJECTIVE_PACK_IDS:
            template.append(
                WhatIfAuditRecord(
                    case_id=case.case_id,
                    objective_pack_id=objective_pack_id,
                    status="pending",
                )
            )
    return template


def _build_pre_branch_contract(
    *,
    case_id: str,
    thread_id: str,
    branch_event: WhatIfEvent,
    history_events: Sequence[WhatIfEvent],
    organization_domain: str,
    action_schema: WhatIfActionSchema,
    notes: Sequence[str],
) -> WhatIfPreBranchContract:
    participants = {
        actor_id
        for event in list(history_events) + [branch_event]
        for actor_id in (
            [event.actor_id, event.target_id] + list(event.flags.to_recipients)
        )
        if actor_id
    }
    summary_values = {
        "thread_age_hours": (
            round(
                max(0, branch_event.timestamp_ms - history_events[0].timestamp_ms)
                / 3_600_000,
                3,
            )
            if history_events
            else 0.0
        ),
        "history_event_count": float(len(history_events)),
        "history_external_count": float(
            sum(
                _event_external_count(
                    event,
                    organization_domain=organization_domain,
                )
                for event in history_events
            )
        ),
        "history_attachment_count": float(
            sum(1 for event in history_events if event.flags.has_attachment_reference)
        ),
        "history_escalation_count": float(
            sum(
                1
                for event in history_events
                if event.flags.is_escalation or event.event_type == "escalation"
            )
        ),
        "history_legal_count": float(
            sum(1 for event in history_events if event.flags.consult_legal_specialist)
        ),
        "history_trading_count": float(
            sum(1 for event in history_events if event.flags.consult_trading_specialist)
        ),
        "participant_count": float(len(participants)),
        "history_reassurance_count": float(
            sum(_event_reassurance_count(event) for event in history_events)
        ),
        "history_cc_count": float(
            sum(int(event.flags.cc_count) for event in history_events)
        ),
        "history_review_loop_count": float(
            sum(1 for event in history_events if _event_has_review_signal(event))
        ),
        "history_external_attachment_count": float(
            sum(
                1
                for event in history_events
                if event.flags.has_attachment_reference
                and _event_external_count(
                    event,
                    organization_domain=organization_domain,
                )
                > 0
            )
        ),
        "history_executive_mention_count": float(
            sum(1 for event in history_events if _event_has_executive_signal(event))
        ),
        "history_cross_functional_count": float(
            sum(
                1
                for event in history_events
                if _event_has_cross_functional_signal(event)
            )
        ),
        "history_conflict_count": float(
            sum(1 for event in history_events if _event_has_conflict_signal(event))
        ),
        "history_commitment_count": float(
            sum(1 for event in history_events if _event_has_commitment_signal(event))
        ),
        "actor_repeat_count": float(
            sum(
                1 for event in history_events if event.actor_id == branch_event.actor_id
            )
        ),
    }
    sequence_steps: list[WhatIfSequenceStep] = []
    for index, event in enumerate(history_events[-10:], start=1):
        sequence_steps.append(
            WhatIfSequenceStep(
                step_index=index,
                phase="history",
                event_type=event.event_type,
                actor_id=event.actor_id,
                subject=event.subject,
                delay_ms=max(0, event.timestamp_ms - history_events[0].timestamp_ms),
                recipient_scope=_event_scope(
                    event,
                    organization_domain=organization_domain,
                ),
                external_recipient_count=_event_external_count(
                    event,
                    organization_domain=organization_domain,
                ),
                cc_recipient_count=int(event.flags.cc_count),
                attachment_flag=event.flags.has_attachment_reference,
                escalation_flag=event.flags.is_escalation
                or event.event_type == "escalation",
                approval_flag=event.event_type == "approval",
                legal_flag=event.flags.consult_legal_specialist,
                trading_flag=event.flags.consult_trading_specialist,
                review_flag=_event_has_review_signal(event),
                urgency_flag=_event_has_urgency_signal(event),
                conflict_flag=_event_has_conflict_signal(event),
            )
        )
    return WhatIfPreBranchContract(
        case_id=case_id,
        thread_id=thread_id,
        branch_event_id=branch_event.event_id,
        branch_event=event_reference(branch_event),
        action_schema=action_schema,
        summary_features=[
            WhatIfBranchSummaryFeature(name=name, value=round(value, 3))
            for name, value in sorted(summary_values.items())
        ],
        sequence_steps=sequence_steps,
        notes=list(notes),
    )


def _action_schema_from_event(
    event: WhatIfEvent,
    *,
    organization_domain: str,
) -> WhatIfActionSchema:
    text = " ".join([event.subject, event.snippet, event.target_id]).lower()
    return WhatIfActionSchema(
        event_type=event.event_type,
        recipient_scope=_event_scope(
            event,
            organization_domain=organization_domain,
        ),
        external_recipient_count=_event_external_count(
            event,
            organization_domain=organization_domain,
        ),
        attachment_policy="present" if event.flags.has_attachment_reference else "none",
        hold_required=False,
        legal_review_required=event.flags.consult_legal_specialist,
        trading_review_required=event.flags.consult_trading_specialist,
        escalation_level=_escalation_level_for_text(
            text,
            event.flags.is_escalation or event.event_type == "escalation",
        ),
        owner_clarity="single_owner" if event.target_id else "unclear",
        reassurance_style=_reassurance_style_for_text(text),
        review_path=_review_path_from_text(text, event.flags.consult_legal_specialist),
        coordination_breadth=_coordination_breadth_for_event(
            event,
            organization_domain=organization_domain,
        ),
        outside_sharing_posture=_outside_sharing_posture_for_event(
            event,
            organization_domain=organization_domain,
        ),
        decision_posture=_decision_posture_for_text(
            text,
            hold_required=False,
            escalated=event.flags.is_escalation or event.event_type == "escalation",
        ),
        action_tags=sorted(
            _historical_branch_tags(
                event,
                organization_domain=organization_domain,
            )
        ),
    )


def _action_schema_from_prompt(
    prompt: str,
    *,
    branch_event: WhatIfEvent,
    historical_action: WhatIfActionSchema,
) -> WhatIfActionSchema:
    lowered = prompt.strip().lower()
    tags = intervention_tags(prompt)
    recipient_scope = historical_action.recipient_scope
    external_recipient_count = historical_action.external_recipient_count
    if "external_removed" in tags or "hold" in tags:
        recipient_scope = "internal"
        external_recipient_count = 0
    elif any(token in lowered for token in _MULTI_PARTY_TERMS):
        recipient_scope = "mixed" if branch_event.flags.to_recipients else "external"
        external_recipient_count = max(
            2, historical_action.external_recipient_count or 1
        )
    elif (
        any(token in lowered for token in _SINGLE_PARTY_TERMS)
        or "status note" in lowered
    ):
        recipient_scope = "external"
        external_recipient_count = 1

    attachment_policy = historical_action.attachment_policy
    if (
        "attachment_removed" in tags
        or "no-pdf" in lowered
        or "no attachment" in lowered
    ):
        attachment_policy = "sanitized"
    elif any(token in lowered for token in ("attachment", "draft", "pdf")):
        attachment_policy = "present"

    owner_clarity = historical_action.owner_clarity
    if "clarify_owner" in tags or "named owner" in lowered or "one owner" in lowered:
        owner_clarity = "single_owner"
    elif any(token in lowered for token in _MULTI_PARTY_TERMS):
        owner_clarity = "multi_owner"

    return WhatIfActionSchema(
        event_type=branch_event.event_type,
        recipient_scope=recipient_scope,
        external_recipient_count=external_recipient_count,
        attachment_policy=attachment_policy,
        hold_required=("hold" in tags)
        or any(token in lowered for token in _HOLD_TERMS),
        legal_review_required=("legal" in tags)
        or any(token in lowered for token in ("counsel", "gerald", "review")),
        trading_review_required=("trading" in tags) or "trading" in lowered,
        escalation_level=_escalation_level_for_text(lowered, "executive_gate" in tags),
        owner_clarity=owner_clarity,
        reassurance_style=_reassurance_style_for_text(lowered),
        review_path=_review_path_for_prompt(lowered, tags),
        coordination_breadth=_coordination_breadth_for_prompt(lowered, tags),
        outside_sharing_posture=_outside_sharing_posture_for_prompt(
            recipient_scope=recipient_scope,
            attachment_policy=attachment_policy,
            lowered=lowered,
        ),
        decision_posture=_decision_posture_for_text(
            lowered,
            hold_required=("hold" in tags)
            or any(token in lowered for token in _HOLD_TERMS),
            escalated="executive_gate" in tags,
        ),
        action_tags=sorted(tags),
    )


def _dominance_summary(
    cases: Sequence[WhatIfBenchmarkCaseEvaluation],
) -> WhatIfDominanceSummary:
    total = 0
    passed = 0
    for case in cases:
        for objective in case.objectives:
            total += 1
            if objective.expected_order_ok:
                passed += 1
    return WhatIfDominanceSummary(
        total_checks=total,
        passed_checks=passed,
        pass_rate=round(passed / max(total, 1), 3),
    )


def _judge_summary(
    cases: Sequence[WhatIfBenchmarkCaseEvaluation],
    *,
    judged_rankings: Sequence[WhatIfJudgedRanking],
) -> WhatIfJudgeSummary:
    if not judged_rankings:
        return WhatIfJudgeSummary(available=False, judgment_count=0)

    top1_hits = 0
    pairwise_total = 0
    pairwise_hits = 0
    taus: list[float] = []
    uncertainty_count = 0
    low_confidence_count = 0
    objective_by_key = {
        (case.case.case_id, objective.objective_pack.pack_id): objective
        for case in cases
        for objective in case.objectives
    }
    for judgment in judged_rankings:
        objective = objective_by_key.get((judgment.case_id, judgment.objective_pack_id))
        if objective is None or not objective.candidates:
            continue
        predicted_order = [item.candidate.candidate_id for item in objective.candidates]
        judged_order = list(judgment.ordered_candidate_ids)
        if predicted_order and judged_order and predicted_order[0] == judged_order[0]:
            top1_hits += 1
        pair_hits, pair_count = _pairwise_hits(predicted_order, judged_order)
        pairwise_hits += pair_hits
        pairwise_total += pair_count
        tau = _kendall_tau(predicted_order, judged_order)
        if tau is not None:
            taus.append(tau)
        if judgment.uncertainty_flag:
            uncertainty_count += 1
        if judgment.confidence is not None and judgment.confidence < 0.65:
            low_confidence_count += 1
    return WhatIfJudgeSummary(
        available=True,
        judgment_count=len(judged_rankings),
        top1_agreement=round(top1_hits / max(len(judged_rankings), 1), 3),
        pairwise_accuracy=(
            round(pairwise_hits / max(pairwise_total, 1), 3) if pairwise_total else None
        ),
        kendall_tau=round(sum(taus) / max(len(taus), 1), 3) if taus else None,
        uncertainty_count=uncertainty_count,
        low_confidence_count=low_confidence_count,
    )


def _audit_summary(
    *,
    judged_rankings: Sequence[WhatIfJudgedRanking],
    audit_records: Sequence[WhatIfAuditRecord],
) -> WhatIfAuditSummary:
    if not audit_records:
        return WhatIfAuditSummary(available=False, queue_count=0, completed_count=0)
    judgment_by_key = {
        (item.case_id, item.objective_pack_id): item for item in judged_rankings
    }
    completed = [item for item in audit_records if item.status == "completed"]
    agreement_hits = 0
    agreement_total = 0
    for audit in completed:
        judgment = judgment_by_key.get((audit.case_id, audit.objective_pack_id))
        if judgment is None:
            continue
        agreement_total += 1
        if list(audit.ordered_candidate_ids) == list(judgment.ordered_candidate_ids):
            agreement_hits += 1
    return WhatIfAuditSummary(
        available=True,
        queue_count=len(audit_records),
        completed_count=len(completed),
        agreement_rate=(
            round(agreement_hits / max(agreement_total, 1), 3)
            if agreement_total
            else None
        ),
    )


def _panel_summary(
    cases: Sequence[WhatIfBenchmarkCaseEvaluation],
    *,
    panel_judgments_path: str | Path | None,
) -> WhatIfPanelSummary:
    judgments = list_panel_judgments(panel_judgments_path)
    if not judgments:
        return WhatIfPanelSummary(available=False, judgment_count=0)

    top1_hits = 0
    pairwise_total = 0
    pairwise_hits = 0
    taus: list[float] = []
    objective_by_key = {
        (case.case.case_id, objective.objective_pack.pack_id): objective
        for case in cases
        for objective in case.objectives
    }
    for judgment in judgments:
        if judgment.abstained:
            continue
        objective = objective_by_key.get((judgment.case_id, judgment.objective_pack_id))
        if objective is None or not objective.candidates:
            continue
        predicted_order = [item.candidate.candidate_id for item in objective.candidates]
        judged_order = list(judgment.ordered_candidate_ids)
        if predicted_order and judged_order and predicted_order[0] == judged_order[0]:
            top1_hits += 1
        pair_hits, pair_count = _pairwise_hits(predicted_order, judged_order)
        pairwise_hits += pair_hits
        pairwise_total += pair_count
        tau = _kendall_tau(predicted_order, judged_order)
        if tau is not None:
            taus.append(tau)
    judged = max(
        0,
        sum(1 for item in judgments if not item.abstained),
    )
    return WhatIfPanelSummary(
        available=True,
        judgment_count=judged,
        top1_agreement=round(top1_hits / max(judged, 1), 3) if judged else None,
        pairwise_accuracy=(
            round(pairwise_hits / max(pairwise_total, 1), 3) if pairwise_total else None
        ),
        kendall_tau=round(sum(taus) / max(len(taus), 1), 3) if taus else None,
    )


def _rollout_stress_summary(
    cases: Sequence[WhatIfBenchmarkCaseEvaluation],
    *,
    research_pack_root: str | Path | None,
) -> WhatIfRolloutStressSummary:
    if research_pack_root is None:
        return WhatIfRolloutStressSummary(available=False)
    root = Path(research_pack_root).expanduser().resolve()
    if not root.exists():
        return WhatIfRolloutStressSummary(available=False)
    payload = WhatIfPackRunResult.model_validate_json(
        (root / "research_pack_result.json").read_text(encoding="utf-8")
    )
    recommendations = {
        (
            case.case.case_id,
            objective.objective_pack.pack_id,
        ): objective.recommended_candidate_label
        for case in payload.cases
        for objective in case.objectives
    }
    compared = 0
    hits = 0
    for case in cases:
        for objective in case.objectives:
            key = (case.case.case_id, objective.objective_pack.pack_id)
            rollout_choice = recommendations.get(key)
            if not rollout_choice:
                continue
            compared += 1
            if objective.recommended_candidate_label == rollout_choice:
                hits += 1
    return WhatIfRolloutStressSummary(
        available=compared > 0,
        compared_case_objectives=compared,
        agreement_count=hits,
        agreement_rate=round(hits / max(compared, 1), 3) if compared else None,
    )


def _benchmark_study_run(
    *,
    model_id: WhatIfBenchmarkModelId,
    seed: int,
    train_result: WhatIfBenchmarkTrainResult,
    eval_result: WhatIfBenchmarkEvalResult,
) -> WhatIfBenchmarkStudyRun:
    return WhatIfBenchmarkStudyRun(
        model_id=model_id,
        seed=seed,
        train_loss=train_result.train_loss,
        validation_loss=train_result.validation_loss,
        observed_auroc_any_external_spread=(
            eval_result.observed_metrics.auroc_any_external_spread or 0.0
        ),
        dominance_passed_checks=eval_result.dominance_summary.passed_checks,
        dominance_total_checks=eval_result.dominance_summary.total_checks,
        dominance_pass_rate=eval_result.dominance_summary.pass_rate,
        judge_top1_agreement=eval_result.judge_summary.top1_agreement,
        judge_pairwise_accuracy=eval_result.judge_summary.pairwise_accuracy,
        business_head_mae=dict(eval_result.observed_metrics.business_head_mae),
        objective_pass_rates=_objective_pass_rates(eval_result.cases),
        train_result_path=train_result.artifacts.train_result_path,
        eval_result_path=eval_result.artifacts.eval_result_path,
    )


def _objective_pass_rates(
    cases: Sequence[WhatIfBenchmarkCaseEvaluation],
) -> dict[WhatIfBusinessObjectivePackId, float]:
    totals: defaultdict[str, int] = defaultdict(int)
    passed: defaultdict[str, int] = defaultdict(int)
    for case in cases:
        for objective in case.objectives:
            pack_id = objective.objective_pack.pack_id
            totals[pack_id] += 1
            if objective.expected_order_ok:
                passed[pack_id] += 1
    return {
        pack_id: round(passed[pack_id] / max(total, 1), 3)
        for pack_id, total in sorted(totals.items())
    }


def _study_model_summary(
    *,
    model_id: WhatIfBenchmarkModelId,
    runs: Sequence[WhatIfBenchmarkStudyRun],
) -> WhatIfBenchmarkStudyModelSummary:
    model_runs = [run for run in runs if run.model_id == model_id]
    business_head_names = sorted(
        {name for run in model_runs for name in run.business_head_mae}
    )
    objective_pack_ids = sorted(
        {pack_id for run in model_runs for pack_id in run.objective_pass_rates}
    )
    return WhatIfBenchmarkStudyModelSummary(
        model_id=model_id,
        run_count=len(model_runs),
        seeds=sorted(run.seed for run in model_runs),
        train_loss=_metric_summary([run.train_loss for run in model_runs]),
        validation_loss=_metric_summary([run.validation_loss for run in model_runs]),
        observed_auroc_any_external_spread=_metric_summary(
            [run.observed_auroc_any_external_spread for run in model_runs]
        ),
        dominance_passed_checks=_metric_summary(
            [float(run.dominance_passed_checks) for run in model_runs]
        ),
        dominance_pass_rate=_metric_summary(
            [run.dominance_pass_rate for run in model_runs]
        ),
        judge_top1_agreement=_optional_metric_summary(
            [run.judge_top1_agreement for run in model_runs]
        ),
        judge_pairwise_accuracy=_optional_metric_summary(
            [run.judge_pairwise_accuracy for run in model_runs]
        ),
        business_head_mae={
            name: _metric_summary(
                [
                    run.business_head_mae[name]
                    for run in model_runs
                    if name in run.business_head_mae
                ]
            )
            for name in business_head_names
        },
        objective_pass_rates={
            pack_id: _metric_summary(
                [
                    run.objective_pass_rates[pack_id]
                    for run in model_runs
                    if pack_id in run.objective_pass_rates
                ]
            )
            for pack_id in objective_pack_ids
        },
    )


__all__ = [
    "build_branch_point_benchmark",
    "evaluate_branch_point_benchmark_model",
    "judge_branch_point_benchmark",
    "list_branch_point_benchmark_models",
    "load_branch_point_benchmark_build_result",
    "load_branch_point_benchmark_eval_result",
    "load_branch_point_benchmark_judge_result",
    "load_branch_point_benchmark_study_result",
    "load_branch_point_benchmark_train_result",
    "list_audit_records",
    "list_judged_rankings",
    "outcome_targets_to_signals",
    "run_branch_point_benchmark_study",
    "summarize_observed_targets",
    "train_branch_point_benchmark_model",
]
