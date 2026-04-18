from __future__ import annotations
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime
from hashlib import sha1
from math import sqrt
from pathlib import Path
from typing import Collection, Sequence

from vei.project_settings import default_model_for_provider

from ._research_packs import build_research_packs
from ._research_constants import (
    DEFAULT_ROLLOUT_SEEDS as _DEFAULT_ROLLOUT_SEEDS,
    INTEGRATED_BACKENDS as _INTEGRATED_BACKENDS,
    PILOT_BACKENDS as _PILOT_BACKENDS,
)
from .corpus import (
    event_by_id,
    external_recipient_count,
    hydrate_event_snippets,
    recipient_scope,
)
from .counterfactual import estimate_counterfactual_delta, run_llm_counterfactual
from .dynamics_bridge import run_dynamics_counterfactual
from .episode import (
    score_historical_tail,
    materialize_episode,
    replay_episode_baseline,
)
from ._helpers import intervention_tags
from .models import (
    WhatIfBackendBranchContract,
    WhatIfBackendScore,
    WhatIfBackendScoreStatus,
    WhatIfBranchSummaryFeature,
    WhatIfEvent,
    WhatIfHistoricalScore,
    WhatIfCounterfactualEstimateResult,
    WhatIfLLMReplayResult,
    WhatIfOutcomeBackendId,
    WhatIfOutcomeSignals,
    WhatIfPackCandidateResult,
    WhatIfPackCaseResult,
    WhatIfPackObjectiveResult,
    WhatIfPackRunArtifacts,
    WhatIfPackRunResult,
    WhatIfRankedRolloutResult,
    WhatIfResearchCandidate,
    WhatIfResearchCase,
    WhatIfResearchDatasetManifest,
    WhatIfResearchDatasetRow,
    WhatIfResearchPack,
    WhatIfSequenceStep,
    WhatIfTreatmentTraceStep,
    WhatIfWorld,
)
from .ranking import (
    aggregate_outcome_signals,
    get_objective_pack,
    recommendation_reason,
    score_outcome_signals,
    sort_candidates_for_rank,
    summarize_forecast_branch,
    summarize_llm_branch,
)
from .render import render_research_pack_run
from vei.whatif.filenames import WORKSPACE_DIRECTORY

_RESEARCH_PACKS = build_research_packs(_DEFAULT_ROLLOUT_SEEDS)


@dataclass(frozen=True)
class _BackendEvaluation:
    backend: WhatIfOutcomeBackendId
    status: WhatIfBackendScoreStatus
    signals: WhatIfOutcomeSignals
    confidence: float | None
    notes: list[str]
    artifact_paths: dict[str, str]
    effective_backend: str | None = None
    error: str | None = None


@dataclass(frozen=True)
class _CandidateExecution:
    candidate: WhatIfResearchCandidate
    rollouts: list[WhatIfRankedRolloutResult]
    rollout_seeds: list[int]
    average_signals: WhatIfOutcomeSignals
    rank_stability: float
    contract: WhatIfBackendBranchContract
    contract_path: Path
    backend_evaluations: list[_BackendEvaluation]


run_ejepa_counterfactual = lambda **kwargs: run_dynamics_counterfactual(  # noqa: E731
    forecast_backend="e_jepa",
    allow_proxy_fallback=False,
    **kwargs,
)


def list_research_packs() -> list[WhatIfResearchPack]:
    return [pack.model_copy(deep=True) for pack in _RESEARCH_PACKS.values()]


def get_research_pack(pack_id: str) -> WhatIfResearchPack:
    candidate = pack_id.strip()
    normalized = candidate.lower()
    if normalized in _RESEARCH_PACKS:
        return _RESEARCH_PACKS[normalized].model_copy(deep=True)

    path = Path(candidate).expanduser().resolve()
    if not path.exists():
        raise KeyError(f"unknown research pack: {pack_id}")
    return WhatIfResearchPack.model_validate_json(path.read_text(encoding="utf-8"))


def load_research_pack_run_result(root: str | Path) -> WhatIfPackRunResult:
    result_path = Path(root).expanduser().resolve() / "research_pack_result.json"
    if not result_path.exists():
        raise ValueError(f"research pack result not found: {result_path}")
    return WhatIfPackRunResult.model_validate_json(
        result_path.read_text(encoding="utf-8")
    )


def run_research_pack(
    world: WhatIfWorld,
    *,
    artifacts_root: str | Path,
    label: str,
    pack_id: str = "enron_research_v1",
    research_pack: WhatIfResearchPack | None = None,
    provider: str = "openai",
    model: str = default_model_for_provider("openai"),
    ejepa_epochs: int = 4,
    ejepa_batch_size: int = 64,
    ejepa_force_retrain: bool = False,
    ejepa_device: str | None = None,
    rollout_workers: int = 4,
) -> WhatIfPackRunResult:
    if research_pack is not None:
        pack = research_pack.model_copy(deep=True)
    else:
        pack = get_research_pack(pack_id)
    if pack.pack_id == "enron_research_v1" and world.source != "enron":
        raise ValueError("enron_research_v1 requires an Enron historical source")
    root = Path(artifacts_root).expanduser().resolve() / pack.pack_id / _slug(label)
    result_path = root / "research_pack_result.json"
    overview_path = root / "research_pack_scoreboard.md"
    dataset_root = root / "dataset"
    pilot_path = root / "pilot_backends.md"
    root.mkdir(parents=True, exist_ok=True)
    events_by_thread = _group_events_by_thread(world.events)

    resolved_cases = _resolve_case_threads(pack=pack, world=world)
    dataset_manifest = _build_research_dataset(
        world,
        pack=resolved_cases,
        root=dataset_root,
        events_by_thread=events_by_thread,
    )
    calibration_rows = _load_training_rows(dataset_manifest)

    case_results: list[WhatIfPackCaseResult] = []
    hypothesis_pass_count = 0
    hypothesis_total_count = 0
    for case in resolved_cases.cases:
        case_root = root / "cases" / case.case_id
        case_root.mkdir(parents=True, exist_ok=True)
        case_result_path = case_root / "case_result.json"
        cached_case_result = _load_case_result(case_result_path)
        if cached_case_result is not None:
            case_pass_count, case_total_count = _count_case_hypotheses(
                cached_case_result
            )
            hypothesis_pass_count += case_pass_count
            hypothesis_total_count += case_total_count
            case_results.append(cached_case_result)
            continue
        materialization = materialize_episode(
            world,
            root=case_root / WORKSPACE_DIRECTORY,
            thread_id=case.thread_id,
            event_id=case.event_id,
        )
        baseline = replay_episode_baseline(
            materialization.workspace_root,
            tick_ms=0,
            seed=pack.rollout_seeds[0],
        )
        timeline = hydrate_event_snippets(
            rosetta_dir=world.rosetta_dir,
            events=events_by_thread.get(materialization.thread_id, []),
        )
        history_events, future_events = _split_timeline(
            timeline=timeline,
            branch_event_id=materialization.branch_event_id,
        )
        historical_outcome = _summarize_historical_future(
            branch_event=materialization.branch_event,
            future_events=future_events,
            organization_domain=world.summary.organization_domain,
        )
        candidate_executions = [
            _execute_candidate(
                world=world,
                case=case,
                materialization=materialization,
                baseline=baseline,
                history_events=history_events,
                future_events=future_events,
                candidate=candidate,
                rollout_seeds=pack.rollout_seeds,
                case_root=case_root,
                provider=provider,
                model=model,
                calibration_rows=calibration_rows,
                historical_outcome=historical_outcome,
                ejepa_epochs=ejepa_epochs,
                ejepa_batch_size=ejepa_batch_size,
                ejepa_force_retrain=ejepa_force_retrain,
                ejepa_device=ejepa_device,
                rollout_workers=rollout_workers,
            )
            for candidate in case.candidates
        ]
        objective_results: list[WhatIfPackObjectiveResult] = []
        for objective_pack_id in pack.objective_pack_ids:
            objective_pack = get_objective_pack(objective_pack_id)
            candidates = [
                _objective_candidate_result(
                    execution=execution,
                    objective_pack_id=objective_pack_id,
                )
                for execution in candidate_executions
            ]
            _assign_primary_ranks(candidates, objective_pack_id=objective_pack_id)
            backend_recommendations = _assign_backend_ranks(
                candidates,
                objective_pack_id=objective_pack_id,
            )
            expected_order_ok = _expected_order_ok(candidates)
            hypothesis_total_count += 1
            if expected_order_ok:
                hypothesis_pass_count += 1
            objective_results.append(
                WhatIfPackObjectiveResult(
                    objective_pack=objective_pack,
                    recommended_candidate_label=(
                        candidates[0].candidate.label if candidates else ""
                    ),
                    candidates=candidates,
                    backend_recommendations=backend_recommendations,
                    expected_order_ok=expected_order_ok,
                )
            )
        case_result = WhatIfPackCaseResult(
            case=case,
            materialization=materialization,
            baseline=baseline,
            historical_outcome_signals=historical_outcome,
            objectives=objective_results,
            artifacts_root=case_root,
        )
        case_result_path.write_text(
            case_result.model_dump_json(indent=2),
            encoding="utf-8",
        )
        case_results.append(case_result)

    artifacts = WhatIfPackRunArtifacts(
        root=root,
        result_json_path=result_path,
        overview_markdown_path=overview_path,
        dataset_root=dataset_root,
        pilot_markdown_path=pilot_path,
    )
    result = WhatIfPackRunResult(
        pack=resolved_cases,
        integrated_backends=list(_INTEGRATED_BACKENDS),
        pilot_backends=list(_PILOT_BACKENDS),
        dataset=dataset_manifest,
        cases=case_results,
        hypothesis_pass_rate=round(
            hypothesis_pass_count / max(hypothesis_total_count, 1),
            3,
        ),
        hypothesis_pass_count=hypothesis_pass_count,
        hypothesis_total_count=hypothesis_total_count,
        artifacts=artifacts,
    )
    result_path.write_text(result.model_dump_json(indent=2), encoding="utf-8")
    overview_path.write_text(render_research_pack_run(result), encoding="utf-8")
    pilot_path.write_text(_render_pilot_backends_note(), encoding="utf-8")
    return result


def _load_case_result(path: Path) -> WhatIfPackCaseResult | None:
    if not path.exists():
        return None
    return WhatIfPackCaseResult.model_validate_json(path.read_text(encoding="utf-8"))


def _count_case_hypotheses(case_result: WhatIfPackCaseResult) -> tuple[int, int]:
    total = len(case_result.objectives)
    passed = sum(
        1 for objective in case_result.objectives if objective.expected_order_ok
    )
    return passed, total


def _resolve_case_threads(
    *,
    pack: WhatIfResearchPack,
    world: WhatIfWorld,
) -> WhatIfResearchPack:
    resolved_cases: list[WhatIfResearchCase] = []
    for case in pack.cases:
        thread_id = case.thread_id
        if thread_id:
            resolved_cases.append(case)
            continue
        event = event_by_id(world.events, case.event_id)
        if event is None:
            raise ValueError(f"event not found in world: {case.event_id}")
        resolved_cases.append(case.model_copy(update={"thread_id": event.thread_id}))
    return pack.model_copy(update={"cases": resolved_cases}, deep=True)


def _build_research_dataset(
    world: WhatIfWorld,
    *,
    pack: WhatIfResearchPack,
    root: Path,
    events_by_thread: dict[str, list[WhatIfEvent]],
) -> WhatIfResearchDatasetManifest:
    root.mkdir(parents=True, exist_ok=True)
    heldout_thread_ids = {case.thread_id for case in pack.cases if case.thread_id}
    rows_by_split: dict[str, list[WhatIfResearchDatasetRow]] = defaultdict(list)
    evaluation_rows: list[WhatIfResearchDatasetRow] = []
    for thread in world.threads:
        if thread.thread_id in heldout_thread_ids:
            continue
        timeline = events_by_thread.get(thread.thread_id, [])
        if len(timeline) < 2:
            continue
        branch_event = _choose_dataset_branch_event(timeline)
        history_events, future_events = _split_timeline(
            timeline=timeline,
            branch_event_id=branch_event.event_id,
        )
        if not future_events:
            continue
        historical_outcome = _summarize_historical_future(
            branch_event=branch_event,
            future_events=future_events,
            organization_domain=world.summary.organization_domain,
        )
        forecast = score_historical_tail(
            future_events,
            organization_domain=world.summary.organization_domain,
            branch_timestamp=branch_event.timestamp,
            public_context=world.public_context,
        )
        contract = _build_branch_contract(
            case_id=thread.thread_id,
            intervention_label="historical_branch",
            branch_event=branch_event,
            history_events=history_events,
            organization_domain=world.summary.organization_domain,
            baseline_forecast=forecast,
            average_rollout_signals=historical_outcome,
            historical_outcome_signals=historical_outcome,
            prompt_tags=_historical_branch_tags(
                branch_event,
                organization_domain=world.summary.organization_domain,
            ),
            generated_messages=[],
            notes=["Historical calibration row built from the actual future."],
        )
        split = _assign_split(thread.thread_id)
        rows_by_split[split].append(
            WhatIfResearchDatasetRow(
                row_id=f"{thread.thread_id}:{branch_event.event_id}",
                split=split,
                source_kind="historical",
                thread_id=thread.thread_id,
                branch_event_id=branch_event.event_id,
                contract=contract,
                outcome_signals=historical_outcome,
            )
        )

    for case in pack.cases:
        evaluation_rows.append(
            WhatIfResearchDatasetRow(
                row_id=f"{case.thread_id}:{case.event_id}:evaluation",
                split="evaluation",
                source_kind="evaluation",
                thread_id=case.thread_id or "",
                branch_event_id=case.event_id,
                contract=WhatIfBackendBranchContract(
                    case_id=case.case_id,
                    objective_pack_id=pack.objective_pack_ids[0],
                    intervention_label="heldout_case",
                ),
                outcome_signals=WhatIfOutcomeSignals(),
            )
        )

    split_paths: dict[str, str] = {}
    split_counts: dict[str, int] = {}
    for split in ("train", "validation", "test"):
        path = root / f"{split}_rows.jsonl"
        _write_jsonl(path, rows_by_split[split])
        split_paths[split] = str(path)
        split_counts[split] = len(rows_by_split[split])
    evaluation_path = root / "evaluation_rows.jsonl"
    _write_jsonl(evaluation_path, evaluation_rows)
    split_paths["evaluation"] = str(evaluation_path)
    split_counts["evaluation"] = len(evaluation_rows)
    manifest = WhatIfResearchDatasetManifest(
        root=root,
        historical_row_count=sum(len(rows) for rows in rows_by_split.values()),
        counterfactual_row_count=0,
        evaluation_row_count=len(evaluation_rows),
        split_row_counts=split_counts,
        split_paths=split_paths,
        heldout_thread_ids=sorted(heldout_thread_ids),
    )
    (root / "dataset_manifest.json").write_text(
        manifest.model_dump_json(indent=2),
        encoding="utf-8",
    )
    return manifest


def _group_events_by_thread(
    events: Sequence[WhatIfEvent],
) -> dict[str, list[WhatIfEvent]]:
    grouped: dict[str, list[WhatIfEvent]] = defaultdict(list)
    for event in sorted(events, key=lambda item: (item.timestamp_ms, item.event_id)):
        grouped[event.thread_id].append(event)
    return grouped


def _load_training_rows(
    manifest: WhatIfResearchDatasetManifest,
) -> list[WhatIfResearchDatasetRow]:
    rows: list[WhatIfResearchDatasetRow] = []
    for split in ("train", "validation"):
        path_text = manifest.split_paths.get(split)
        if not path_text:
            continue
        path = Path(path_text)
        if not path.exists():
            continue
        for line in path.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            rows.append(WhatIfResearchDatasetRow.model_validate_json(line))
    return rows


def _execute_candidate(
    *,
    world: WhatIfWorld,
    case: WhatIfResearchCase,
    materialization,
    baseline,
    history_events: Sequence[WhatIfEvent],
    future_events: Sequence[WhatIfEvent],
    candidate: WhatIfResearchCandidate,
    rollout_seeds: Sequence[int],
    case_root: Path,
    provider: str,
    model: str,
    calibration_rows: Sequence[WhatIfResearchDatasetRow],
    historical_outcome: WhatIfOutcomeSignals,
    ejepa_epochs: int,
    ejepa_batch_size: int,
    ejepa_force_retrain: bool,
    ejepa_device: str | None,
    rollout_workers: int,
) -> _CandidateExecution:
    rollout_records = _run_candidate_rollouts(
        workspace_root=materialization.workspace_root,
        branch_event=materialization.branch_event,
        prompt=candidate.prompt,
        provider=provider,
        model=model,
        rollout_seeds=rollout_seeds,
        rollout_workers=rollout_workers,
    )
    rollouts = [record[3] for record in rollout_records]
    outcomes = [record[2] for record in rollout_records]
    first_rollout = rollout_records[0][1] if rollout_records else None
    average_signals = aggregate_outcome_signals(outcomes)
    prompt_tags = _research_intervention_tags(candidate.prompt)
    contract = _build_branch_contract(
        case_id=case.case_id,
        intervention_label=candidate.label,
        branch_event=materialization.branch_event,
        history_events=history_events,
        organization_domain=world.summary.organization_domain,
        baseline_forecast=baseline.forecast,
        average_rollout_signals=average_signals,
        historical_outcome_signals=historical_outcome,
        prompt_tags=prompt_tags,
        generated_messages=list(first_rollout.messages if first_rollout else []),
        notes=[
            f"Historical future events: {len(future_events)}.",
            f"Rollout seeds: {', '.join(str(seed) for seed in rollout_seeds)}.",
        ],
    )
    candidate_root = case_root / "candidates"
    candidate_root.mkdir(parents=True, exist_ok=True)
    contract_path = candidate_root / f"{candidate.candidate_id}_contract.json"
    contract_path.write_text(contract.model_dump_json(indent=2), encoding="utf-8")
    backend_evaluations = [
        _score_backend(
            backend=backend,
            world=world,
            materialization=materialization,
            prompt=candidate.prompt,
            llm_messages=list(first_rollout.messages if first_rollout else []),
            average_signals=average_signals,
            historical_outcome=historical_outcome,
            contract=contract,
            calibration_rows=calibration_rows,
            contract_path=contract_path,
            ejepa_epochs=ejepa_epochs,
            ejepa_batch_size=ejepa_batch_size,
            ejepa_force_retrain=ejepa_force_retrain,
            ejepa_device=ejepa_device,
        )
        for backend in _INTEGRATED_BACKENDS
    ]
    return _CandidateExecution(
        candidate=candidate,
        rollouts=rollouts,
        rollout_seeds=list(rollout_seeds),
        average_signals=average_signals,
        rank_stability=_rank_stability(rollouts),
        contract=contract,
        contract_path=contract_path,
        backend_evaluations=backend_evaluations,
    )


def _run_candidate_rollouts(
    *,
    workspace_root: Path,
    branch_event,
    prompt: str,
    provider: str,
    model: str,
    rollout_seeds: Sequence[int],
    rollout_workers: int,
) -> list[
    tuple[int, WhatIfLLMReplayResult, WhatIfOutcomeSignals, WhatIfRankedRolloutResult]
]:
    max_workers = max(1, min(int(rollout_workers), len(rollout_seeds)))
    indexed_seeds = list(enumerate(rollout_seeds, start=1))
    if max_workers == 1:
        return [
            _execute_rollout(
                workspace_root=workspace_root,
                branch_event=branch_event,
                prompt=prompt,
                provider=provider,
                model=model,
                rollout_index=rollout_index,
                seed=seed,
            )
            for rollout_index, seed in indexed_seeds
        ]

    completed: list[
        tuple[
            int,
            WhatIfLLMReplayResult,
            WhatIfOutcomeSignals,
            WhatIfRankedRolloutResult,
        ]
    ] = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(
                _execute_rollout,
                workspace_root=workspace_root,
                branch_event=branch_event,
                prompt=prompt,
                provider=provider,
                model=model,
                rollout_index=rollout_index,
                seed=seed,
            ): rollout_index
            for rollout_index, seed in indexed_seeds
        }
        for future in as_completed(futures):
            completed.append(future.result())
    completed.sort(key=lambda item: item[0])
    return completed


def _execute_rollout(
    *,
    workspace_root: Path,
    branch_event,
    prompt: str,
    provider: str,
    model: str,
    rollout_index: int,
    seed: int,
) -> tuple[int, WhatIfLLMReplayResult, WhatIfOutcomeSignals, WhatIfRankedRolloutResult]:
    llm_result = run_llm_counterfactual(
        workspace_root,
        prompt=prompt,
        provider=provider,
        model=model,
        seed=seed,
    )
    outcome = summarize_llm_branch(
        branch_event=branch_event,
        llm_result=llm_result,
    )
    rollout = WhatIfRankedRolloutResult(
        rollout_index=rollout_index,
        seed=seed,
        llm_result=llm_result,
        outcome_signals=outcome,
        outcome_score=score_outcome_signals(
            pack=get_objective_pack("contain_exposure"),
            outcome=outcome,
        ),
    )
    return rollout_index, llm_result, outcome, rollout


def _objective_candidate_result(
    *,
    execution: _CandidateExecution,
    objective_pack_id: str,
) -> WhatIfPackCandidateResult:
    objective_pack = get_objective_pack(objective_pack_id)
    outcome_score = score_outcome_signals(
        pack=objective_pack,
        outcome=execution.average_signals,
    )
    backend_scores = [
        WhatIfBackendScore(
            backend=evaluation.backend,
            status=evaluation.status,
            effective_backend=evaluation.effective_backend,
            outcome_signals=evaluation.signals,
            outcome_score=score_outcome_signals(
                pack=objective_pack,
                outcome=evaluation.signals,
            ),
            confidence=evaluation.confidence,
            notes=list(evaluation.notes),
            artifact_paths=dict(evaluation.artifact_paths),
            error=evaluation.error,
        )
        for evaluation in execution.backend_evaluations
    ]
    expected = execution.candidate.expected_hypotheses.get(
        objective_pack_id,
        "middle_expected",
    )
    return WhatIfPackCandidateResult(
        candidate=execution.candidate,
        expected_hypothesis=expected,
        rollout_seeds=list(execution.rollout_seeds),
        rollout_count=len(execution.rollouts),
        average_outcome_signals=execution.average_signals,
        outcome_score=outcome_score,
        rank_stability=execution.rank_stability,
        rollouts=list(execution.rollouts),
        backend_scores=backend_scores,
        contract_path=str(execution.contract_path),
    )


def _assign_primary_ranks(
    candidates: list[WhatIfPackCandidateResult],
    *,
    objective_pack_id: str,
) -> None:
    ordered = sort_candidates_for_rank(
        [
            (
                candidate.candidate.label,
                candidate.average_outcome_signals,
                candidate.outcome_score,
            )
            for candidate in candidates
        ]
    )
    rank_map = {label: index + 1 for index, label in enumerate(ordered)}
    objective_pack = get_objective_pack(objective_pack_id)
    recommended = ordered[0] if ordered else ""
    for candidate in candidates:
        candidate.rank = rank_map.get(candidate.candidate.label, 0)
        is_best = candidate.candidate.label == recommended
        candidate.reason = _candidate_reason(
            pack=objective_pack,
            candidate=candidate,
            is_best=is_best,
        )
    candidates.sort(key=lambda item: item.rank)


def _assign_backend_ranks(
    candidates: list[WhatIfPackCandidateResult],
    *,
    objective_pack_id: str,
) -> dict[str, str]:
    recommendations: dict[str, str] = {}
    for backend in _INTEGRATED_BACKENDS:
        ordered = sort_candidates_for_rank(
            [
                (
                    candidate.candidate.label,
                    _backend_score_for(candidate, backend).outcome_signals,
                    _backend_score_for(candidate, backend).outcome_score,
                )
                for candidate in candidates
            ]
        )
        rank_map = {label: index + 1 for index, label in enumerate(ordered)}
        for candidate in candidates:
            _backend_score_for(candidate, backend).rank = rank_map.get(
                candidate.candidate.label,
                0,
            )
        recommendations[str(backend)] = ordered[0] if ordered else ""
    return recommendations


def _expected_order_ok(candidates: Sequence[WhatIfPackCandidateResult]) -> bool:
    best = None
    worst = None
    for candidate in candidates:
        if candidate.expected_hypothesis == "best_expected":
            best = candidate
        if candidate.expected_hypothesis == "worst_expected":
            worst = candidate
    if best is None or worst is None:
        return False
    return best.outcome_score.overall_score > worst.outcome_score.overall_score


def _score_backend(
    *,
    backend: WhatIfOutcomeBackendId,
    world: WhatIfWorld,
    materialization,
    prompt: str,
    llm_messages,
    average_signals: WhatIfOutcomeSignals,
    historical_outcome: WhatIfOutcomeSignals,
    contract: WhatIfBackendBranchContract,
    calibration_rows: Sequence[WhatIfResearchDatasetRow],
    contract_path: Path,
    ejepa_epochs: int,
    ejepa_batch_size: int,
    ejepa_force_retrain: bool,
    ejepa_device: str | None,
) -> _BackendEvaluation:
    if backend == "e_jepa":
        forecast = run_ejepa_counterfactual(
            world=world,
            materialization=materialization,
            prompt=prompt,
            llm_messages=llm_messages,
            seed=42042,
            ejepa_epochs=ejepa_epochs,
            ejepa_batch_size=ejepa_batch_size,
            ejepa_force_retrain=ejepa_force_retrain,
            ejepa_device=ejepa_device,
        )
        if forecast.status == "ok" and forecast.backend == "e_jepa":
            return _forecast_backend_evaluation(
                backend=backend,
                forecast=forecast,
                contract_path=contract_path,
            )
        fallback = estimate_counterfactual_delta(
            materialization.workspace_root,
            prompt=prompt,
        )
        notes = [
            "Real E-JEPA backend failed and the pack used the proxy fallback.",
            *(forecast.notes or []),
        ]
        if forecast.error:
            notes.append(f"Original error: {forecast.error}")
        return _forecast_backend_evaluation(
            backend=backend,
            forecast=fallback,
            contract_path=contract_path,
            status="fallback",
            effective_backend=fallback.backend,
            notes=notes,
            error=forecast.error,
        )
    if backend == "heuristic_baseline":
        forecast = estimate_counterfactual_delta(
            materialization.workspace_root,
            prompt=prompt,
        )
        return _forecast_backend_evaluation(
            backend=backend,
            forecast=forecast,
            contract_path=contract_path,
        )
    if backend == "ft_transformer":
        neighbor_signals, confidence, notes = _summary_table_surrogate(
            contract=contract,
            calibration_rows=calibration_rows,
            average_signals=average_signals,
            historical_outcome=historical_outcome,
        )
        return _heuristic_backend_evaluation(
            backend=backend,
            signals=neighbor_signals,
            confidence=confidence,
            notes=notes,
            contract_path=contract_path,
        )
    if backend == "ts2vec":
        sequence_signals, confidence, notes = _sequence_surrogate(
            contract=contract,
            calibration_rows=calibration_rows,
            average_signals=average_signals,
            historical_outcome=historical_outcome,
        )
        return _heuristic_backend_evaluation(
            backend=backend,
            signals=sequence_signals,
            confidence=confidence,
            notes=notes,
            contract_path=contract_path,
        )
    if backend == "g_transformer":
        trace_signals, confidence, notes = _treatment_trace_surrogate(
            contract=contract,
            calibration_rows=calibration_rows,
            average_signals=average_signals,
            historical_outcome=historical_outcome,
        )
        return _heuristic_backend_evaluation(
            backend=backend,
            signals=trace_signals,
            confidence=confidence,
            notes=notes,
            contract_path=contract_path,
        )
    return _BackendEvaluation(
        backend=backend,
        status="skipped",
        signals=average_signals,
        confidence=0.0,
        notes=["Backend is tracked as a pilot and is not integrated in this run."],
        artifact_paths={"contract": str(contract_path)},
    )


def _forecast_backend_evaluation(
    *,
    backend: WhatIfOutcomeBackendId,
    forecast: WhatIfCounterfactualEstimateResult,
    contract_path: Path,
    status: WhatIfBackendScoreStatus = "ok",
    effective_backend: str | None = None,
    notes: Sequence[str] | None = None,
    error: str | None = None,
) -> _BackendEvaluation:
    signals = summarize_forecast_branch(forecast)
    return _BackendEvaluation(
        backend=backend,
        status=status,
        signals=signals,
        confidence=0.9 if forecast.status == "ok" else 0.35,
        notes=list(notes or forecast.notes),
        artifact_paths={
            "contract": str(contract_path),
            **_forecast_artifact_paths(forecast),
        },
        effective_backend=effective_backend,
        error=error,
    )


def _heuristic_backend_evaluation(
    *,
    backend: WhatIfOutcomeBackendId,
    signals: WhatIfOutcomeSignals,
    confidence: float | None,
    notes: Sequence[str],
    contract_path: Path,
) -> _BackendEvaluation:
    return _BackendEvaluation(
        backend=backend,
        status="ok",
        signals=signals,
        confidence=confidence,
        notes=list(notes),
        artifact_paths={"contract": str(contract_path)},
    )


def _summary_table_surrogate(
    *,
    contract: WhatIfBackendBranchContract,
    calibration_rows: Sequence[WhatIfResearchDatasetRow],
    average_signals: WhatIfOutcomeSignals,
    historical_outcome: WhatIfOutcomeSignals,
) -> tuple[WhatIfOutcomeSignals, float, list[str]]:
    neighbors, distance = _nearest_rows(
        calibration_rows=calibration_rows,
        vector=_summary_vector(contract),
        vector_fn=lambda row: _summary_vector(row.contract),
    )
    neighbor_mean = _neighbor_mean(neighbors, fallback=historical_outcome)
    blended = _blend_signals(average_signals, neighbor_mean, alpha=0.7)
    blended = _apply_research_tag_adjustments(
        blended,
        tags=_contract_tags(contract),
        strength=0.4,
    )
    notes = [
        "ft_transformer uses the summary-table branch contract and train-split neighbors.",
        f"Matched {len(neighbors)} calibration rows with mean distance {distance:.3f}.",
    ]
    return blended, _distance_confidence(distance), notes


def _sequence_surrogate(
    *,
    contract: WhatIfBackendBranchContract,
    calibration_rows: Sequence[WhatIfResearchDatasetRow],
    average_signals: WhatIfOutcomeSignals,
    historical_outcome: WhatIfOutcomeSignals,
) -> tuple[WhatIfOutcomeSignals, float, list[str]]:
    neighbors, distance = _nearest_rows(
        calibration_rows=calibration_rows,
        vector=_sequence_vector(contract),
        vector_fn=lambda row: _sequence_vector(row.contract),
    )
    neighbor_mean = _neighbor_mean(neighbors, fallback=historical_outcome)
    blended = _blend_signals(average_signals, neighbor_mean, alpha=0.6)
    blended = blended.model_copy(
        update={
            "delay_risk": round(
                _clamp(
                    (blended.delay_risk * 0.75)
                    + (_sequence_delay_bias(contract) * 0.25)
                ),
                3,
            )
        }
    )
    blended = _apply_research_tag_adjustments(
        blended,
        tags=_contract_tags(contract),
        strength=0.25,
    )
    notes = [
        "ts2vec uses the stepwise event sequence contract and sequence-neighbor averaging.",
        f"Matched {len(neighbors)} calibration rows with mean distance {distance:.3f}.",
    ]
    return blended, _distance_confidence(distance), notes


def _treatment_trace_surrogate(
    *,
    contract: WhatIfBackendBranchContract,
    calibration_rows: Sequence[WhatIfResearchDatasetRow],
    average_signals: WhatIfOutcomeSignals,
    historical_outcome: WhatIfOutcomeSignals,
) -> tuple[WhatIfOutcomeSignals, float, list[str]]:
    tags = _contract_tags(contract)
    matched_rows = [
        row for row in calibration_rows if _contract_tags(row.contract) & tags
    ]
    baseline = _blend_signals(historical_outcome, average_signals, alpha=0.45)
    if matched_rows:
        neighbor_mean = _neighbor_mean(matched_rows[:6], fallback=historical_outcome)
        baseline = _blend_signals(baseline, neighbor_mean, alpha=0.65)
    adjusted = _apply_research_tag_adjustments(
        baseline,
        tags=tags,
        strength=0.65,
    )
    notes = [
        "g_transformer uses the treatment trace and branch deltas as a causal-style surrogate.",
        f"Matched {len(matched_rows)} calibration rows with overlapping treatment tags.",
    ]
    confidence = 0.55 if matched_rows else 0.35
    return adjusted, confidence, notes


def _build_branch_contract(
    *,
    case_id: str,
    intervention_label: str,
    branch_event,
    history_events: Sequence[WhatIfEvent],
    organization_domain: str,
    baseline_forecast: WhatIfHistoricalScore,
    average_rollout_signals: WhatIfOutcomeSignals,
    historical_outcome_signals: WhatIfOutcomeSignals,
    prompt_tags: Collection[str],
    generated_messages,
    notes: Sequence[str],
) -> WhatIfBackendBranchContract:
    summary_features = _summary_features(
        branch_event=branch_event,
        history_events=history_events,
        organization_domain=organization_domain,
        baseline_forecast=baseline_forecast,
        average_rollout_signals=average_rollout_signals,
        historical_outcome_signals=historical_outcome_signals,
        prompt_tags=prompt_tags,
    )
    sequence_steps = _build_sequence_steps(
        history_events=history_events,
        branch_event=branch_event,
        organization_domain=organization_domain,
        generated_messages=generated_messages,
    )
    treatment_trace = _build_treatment_trace(
        prompt_tags=prompt_tags,
        average_rollout_signals=average_rollout_signals,
        historical_outcome_signals=historical_outcome_signals,
    )
    return WhatIfBackendBranchContract(
        case_id=case_id,
        objective_pack_id="contain_exposure",
        intervention_label=intervention_label,
        summary_features=summary_features,
        sequence_steps=sequence_steps,
        treatment_trace=treatment_trace,
        average_rollout_signals=average_rollout_signals,
        historical_outcome_signals=historical_outcome_signals,
        baseline_forecast=baseline_forecast,
        notes=list(notes),
    )


def _summary_features(
    *,
    branch_event,
    history_events: Sequence[WhatIfEvent],
    organization_domain: str,
    baseline_forecast: WhatIfHistoricalScore,
    average_rollout_signals: WhatIfOutcomeSignals,
    historical_outcome_signals: WhatIfOutcomeSignals,
    prompt_tags: Collection[str],
) -> list[WhatIfBranchSummaryFeature]:
    participants = {
        actor_id
        for event in list(history_events) + [branch_event]
        for actor_id in (
            [event.actor_id, event.target_id] + _reference_recipients(event)
        )
        if actor_id
    }
    history_external = sum(
        _event_external_count(
            event,
            organization_domain=organization_domain,
        )
        for event in history_events
    )
    history_attachment = sum(
        1 for event in history_events if event.flags.has_attachment_reference
    )
    history_escalation = sum(
        1
        for event in history_events
        if event.flags.is_escalation or event.event_type == "escalation"
    )
    history_forward = sum(1 for event in history_events if event.flags.is_forward)
    history_legal = sum(
        1 for event in history_events if event.flags.consult_legal_specialist
    )
    history_trading = sum(
        1 for event in history_events if event.flags.consult_trading_specialist
    )
    features = {
        "history_event_count": float(len(history_events)),
        "history_external_count": float(history_external),
        "history_attachment_count": float(history_attachment),
        "history_escalation_count": float(history_escalation),
        "history_forward_count": float(history_forward),
        "history_legal_count": float(history_legal),
        "history_trading_count": float(history_trading),
        "participant_count": float(len(participants)),
        "branch_external_count": float(
            _reference_external_count(
                branch_event,
                organization_domain=organization_domain,
            )
        ),
        "branch_attachment_flag": (
            1.0 if _reference_attachment_flag(branch_event) else 0.0
        ),
        "branch_escalation_flag": (
            1.0 if _reference_escalation_flag(branch_event) else 0.0
        ),
        "branch_forward_flag": 1.0 if _reference_forward_flag(branch_event) else 0.0,
        "baseline_future_event_count": float(baseline_forecast.future_event_count),
        "baseline_future_external_count": float(
            baseline_forecast.future_external_event_count
        ),
        "baseline_risk_score": float(baseline_forecast.risk_score),
        "rollout_message_count": float(average_rollout_signals.message_count),
        "rollout_outside_count": float(average_rollout_signals.outside_message_count),
        "rollout_delay_hours": round(
            average_rollout_signals.avg_delay_ms / 3_600_000, 3
        ),
        "rollout_reassurance_count": float(average_rollout_signals.reassurance_count),
        "rollout_hold_count": float(average_rollout_signals.hold_count),
        "historical_future_message_count": float(
            historical_outcome_signals.message_count
        ),
        "historical_outside_count": float(
            historical_outcome_signals.outside_message_count
        ),
        "historical_delay_hours": round(
            historical_outcome_signals.avg_delay_ms / 3_600_000,
            3,
        ),
    }
    for tag in sorted(prompt_tags):
        features[f"tag__{tag}"] = 1.0
    return [
        WhatIfBranchSummaryFeature(name=name, value=round(value, 3))
        for name, value in sorted(features.items())
    ]


def _build_sequence_steps(
    *,
    history_events: Sequence[WhatIfEvent],
    branch_event,
    organization_domain: str,
    generated_messages,
) -> list[WhatIfSequenceStep]:
    steps: list[WhatIfSequenceStep] = []
    for index, event in enumerate(history_events[-8:], start=1):
        steps.append(
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
                attachment_flag=event.flags.has_attachment_reference,
                escalation_flag=event.flags.is_escalation
                or event.event_type == "escalation",
                approval_flag=event.event_type == "approval",
            )
        )
    steps.append(
        WhatIfSequenceStep(
            step_index=len(steps) + 1,
            phase="branch",
            event_type=branch_event.event_type,
            actor_id=branch_event.actor_id,
            subject=branch_event.subject,
            delay_ms=0,
            recipient_scope=_reference_scope(
                branch_event,
                organization_domain=organization_domain,
            ),
            external_recipient_count=_reference_external_count(
                branch_event,
                organization_domain=organization_domain,
            ),
            attachment_flag=_reference_attachment_flag(branch_event),
            escalation_flag=_reference_escalation_flag(branch_event),
            approval_flag=branch_event.event_type == "approval",
        )
    )
    for message in list(generated_messages)[:3]:
        generated_scope = _recipient_scope(
            message.to,
            organization_domain=organization_domain,
        )
        steps.append(
            WhatIfSequenceStep(
                step_index=len(steps) + 1,
                phase="generated",
                event_type="message",
                actor_id=message.actor_id,
                subject=message.subject,
                delay_ms=message.delay_ms,
                recipient_scope=generated_scope,
                external_recipient_count=0 if generated_scope == "internal" else 1,
                attachment_flag="attach" in message.body_text.lower()
                or "draft" in message.body_text.lower(),
                escalation_flag="urgent" in message.body_text.lower()
                or "escalate" in message.body_text.lower(),
                approval_flag="approval" in message.body_text.lower(),
            )
        )
    return steps


def _build_treatment_trace(
    *,
    prompt_tags: Collection[str],
    average_rollout_signals: WhatIfOutcomeSignals,
    historical_outcome_signals: WhatIfOutcomeSignals,
) -> list[WhatIfTreatmentTraceStep]:
    trace: list[WhatIfTreatmentTraceStep] = []
    for tag in sorted(prompt_tags):
        trace.append(
            WhatIfTreatmentTraceStep(
                step_index=len(trace) + 1,
                source="prompt",
                tag=tag,
                value=1.0,
            )
        )
    delta_features = {
        "delta_exposure": average_rollout_signals.exposure_risk
        - historical_outcome_signals.exposure_risk,
        "delta_delay": average_rollout_signals.delay_risk
        - historical_outcome_signals.delay_risk,
        "delta_relationship": average_rollout_signals.relationship_protection
        - historical_outcome_signals.relationship_protection,
    }
    for tag, value in delta_features.items():
        trace.append(
            WhatIfTreatmentTraceStep(
                step_index=len(trace) + 1,
                source="rollout_delta",
                tag=tag,
                value=round(float(value), 3),
            )
        )
    return trace


def _summarize_historical_future(
    *,
    branch_event,
    future_events: Sequence[WhatIfEvent],
    organization_domain: str,
) -> WhatIfOutcomeSignals:
    if not future_events:
        return WhatIfOutcomeSignals(
            exposure_risk=0.0,
            delay_risk=0.0,
            relationship_protection=1.0,
            internal_only=True,
        )
    future_count = len(future_events)
    outside_count = sum(
        _event_external_count(
            event,
            organization_domain=organization_domain,
        )
        for event in future_events
    )
    escalation_count = sum(
        1
        for event in future_events
        if event.flags.is_escalation or event.event_type == "escalation"
    )
    attachment_count = sum(
        1 for event in future_events if event.flags.has_attachment_reference
    )
    first_delay = max(
        0,
        future_events[0].timestamp_ms - _reference_timestamp_ms(branch_event),
    )
    avg_delay = round(
        sum(
            max(0, event.timestamp_ms - _reference_timestamp_ms(branch_event))
            for event in future_events
        )
        / future_count
    )
    exposure = _clamp(
        ((outside_count / max(future_count, 1)) * 0.65)
        + ((attachment_count / max(future_count, 1)) * 0.2)
        + ((escalation_count / max(future_count, 1)) * 0.15)
    )
    delay = _clamp(
        (_delay_norm(first_delay) * 0.45)
        + (_delay_norm(avg_delay) * 0.35)
        + (_message_count_norm(future_count) * 0.2)
    )
    relationship = _clamp(1.0 - ((exposure * 0.45) + (delay * 0.55)))
    return WhatIfOutcomeSignals(
        exposure_risk=round(exposure, 3),
        delay_risk=round(delay, 3),
        relationship_protection=round(relationship, 3),
        message_count=future_count,
        outside_message_count=outside_count,
        avg_delay_ms=avg_delay,
        internal_only=outside_count == 0,
    )


def _candidate_reason(
    *,
    pack,
    candidate: WhatIfPackCandidateResult,
    is_best: bool,
) -> str:
    if is_best:
        return recommendation_reason(
            pack=pack,
            outcome=candidate.average_outcome_signals,
            score=candidate.outcome_score,
            rollout_count=candidate.rollout_count,
        )
    if pack.pack_id == "contain_exposure":
        return "Lower-ranked because it leaves more outside exposure in the simulated branch."
    if pack.pack_id == "reduce_delay":
        return "Lower-ranked because the follow-up pattern still looks slower."
    return "Lower-ranked because it protects the relationship less consistently."


def _choose_dataset_branch_event(events: Sequence[WhatIfEvent]) -> WhatIfEvent:
    for event in events[:-1]:
        if (
            event.flags.has_attachment_reference
            or event.flags.is_forward
            or event.flags.is_escalation
            or event.event_type in {"assignment", "approval"}
        ):
            return event
    return events[max(0, (len(events) // 2) - 1)]


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


def _assign_split(thread_id: str) -> str:
    digest = sha1(thread_id.encode("utf-8"), usedforsecurity=False).hexdigest()
    bucket = int(digest[:2], 16)
    if bucket < 179:
        return "train"
    if bucket < 217:
        return "validation"
    return "test"


def _write_jsonl(path: Path, rows: Sequence[WhatIfResearchDatasetRow]) -> None:
    lines = [row.model_dump_json() for row in rows]
    path.write_text("\n".join(lines) + ("\n" if lines else ""), encoding="utf-8")


def _nearest_rows(
    *,
    calibration_rows: Sequence[WhatIfResearchDatasetRow],
    vector: dict[str, float],
    vector_fn,
    top_k: int = 5,
) -> tuple[list[WhatIfResearchDatasetRow], float]:
    if not calibration_rows:
        return [], 1.0
    distances: list[tuple[float, WhatIfResearchDatasetRow]] = []
    for row in calibration_rows:
        candidate_vector = vector_fn(row)
        distance = _vector_distance(vector, candidate_vector)
        distances.append((distance, row))
    distances.sort(key=lambda item: item[0])
    chosen = distances[:top_k]
    mean_distance = (
        round(sum(distance for distance, _ in chosen) / max(len(chosen), 1), 3)
        if chosen
        else 1.0
    )
    return [row for _, row in chosen], mean_distance


def _neighbor_mean(
    neighbors: Sequence[WhatIfResearchDatasetRow],
    *,
    fallback: WhatIfOutcomeSignals,
) -> WhatIfOutcomeSignals:
    if not neighbors:
        return fallback
    return aggregate_outcome_signals([row.outcome_signals for row in neighbors])


def _summary_vector(contract: WhatIfBackendBranchContract) -> dict[str, float]:
    return {feature.name: float(feature.value) for feature in contract.summary_features}


def _sequence_vector(contract: WhatIfBackendBranchContract) -> dict[str, float]:
    counts = Counter(step.event_type for step in contract.sequence_steps)
    generated = [step for step in contract.sequence_steps if step.phase == "generated"]
    history = [step for step in contract.sequence_steps if step.phase == "history"]
    return {
        "history_steps": float(len(history)),
        "generated_steps": float(len(generated)),
        "message_steps": float(counts.get("message", 0)),
        "assignment_steps": float(counts.get("assignment", 0)),
        "approval_steps": float(counts.get("approval", 0)),
        "escalation_steps": float(counts.get("escalation", 0)),
        "external_steps": float(
            sum(step.external_recipient_count > 0 for step in contract.sequence_steps)
        ),
        "attachment_steps": float(
            sum(step.attachment_flag for step in contract.sequence_steps)
        ),
        "delay_hours": round(
            sum(step.delay_ms for step in generated)
            / max(len(generated), 1)
            / 3_600_000,
            3,
        ),
    }


def _contract_tags(contract: WhatIfBackendBranchContract) -> set[str]:
    return {step.tag for step in contract.treatment_trace if step.source == "prompt"}


def _blend_signals(
    primary: WhatIfOutcomeSignals,
    secondary: WhatIfOutcomeSignals,
    *,
    alpha: float,
) -> WhatIfOutcomeSignals:
    beta = 1.0 - alpha
    return WhatIfOutcomeSignals(
        exposure_risk=round(
            (primary.exposure_risk * alpha) + (secondary.exposure_risk * beta), 3
        ),
        delay_risk=round(
            (primary.delay_risk * alpha) + (secondary.delay_risk * beta), 3
        ),
        relationship_protection=round(
            (primary.relationship_protection * alpha)
            + (secondary.relationship_protection * beta),
            3,
        ),
        message_count=round(
            (primary.message_count * alpha) + (secondary.message_count * beta)
        ),
        outside_message_count=round(
            (primary.outside_message_count * alpha)
            + (secondary.outside_message_count * beta)
        ),
        avg_delay_ms=round(
            (primary.avg_delay_ms * alpha) + (secondary.avg_delay_ms * beta)
        ),
        internal_only=primary.internal_only and secondary.internal_only,
        reassurance_count=round(
            (primary.reassurance_count * alpha) + (secondary.reassurance_count * beta)
        ),
        hold_count=round((primary.hold_count * alpha) + (secondary.hold_count * beta)),
    )


def _apply_research_tag_adjustments(
    signals: WhatIfOutcomeSignals,
    *,
    tags: set[str],
    strength: float,
) -> WhatIfOutcomeSignals:
    exposure = float(signals.exposure_risk)
    delay = float(signals.delay_risk)
    relationship = float(signals.relationship_protection)
    if {"hold", "pause_forward", "internal_summary", "external_removed"} & tags:
        exposure -= 0.18 * strength
        delay += 0.06 * strength
        relationship += 0.04 * strength
    if {"legal", "compliance", "legal_gate"} & tags:
        exposure -= 0.12 * strength
        delay += 0.08 * strength
        relationship += 0.03 * strength
    if {"reply_immediately", "clarify_owner", "owner_assignment", "status_note"} & tags:
        delay -= 0.16 * strength
        relationship += 0.08 * strength
    if {"attachment_removed", "sanitized"} & tags:
        exposure -= 0.14 * strength
        relationship += 0.05 * strength
    if {
        "broad_external",
        "coalition_blast",
        "wide_policy",
        "raw_attachment_forward",
    } & tags:
        exposure += 0.2 * strength
        delay += 0.06 * strength
        relationship -= 0.12 * strength
    if {"executive_blast", "wide_alarm"} & tags:
        exposure += 0.08 * strength
        delay += 0.15 * strength
        relationship -= 0.1 * strength
    if "outside_counsel" in tags:
        exposure += 0.04 * strength
        delay += 0.04 * strength
        relationship += 0.05 * strength
    return WhatIfOutcomeSignals(
        exposure_risk=round(_clamp(exposure), 3),
        delay_risk=round(_clamp(delay), 3),
        relationship_protection=round(_clamp(relationship), 3),
        message_count=signals.message_count,
        outside_message_count=signals.outside_message_count,
        avg_delay_ms=signals.avg_delay_ms,
        internal_only=signals.internal_only,
        reassurance_count=signals.reassurance_count,
        hold_count=signals.hold_count,
    )


def _research_intervention_tags(prompt: str) -> set[str]:
    lowered = prompt.strip().lower()
    tags = set(intervention_tags(prompt))
    if any(token in lowered for token in ("summary", "consolidated", "single owner")):
        tags.add("internal_summary")
    if any(token in lowered for token in ("status note", "status update")):
        tags.add("status_note")
    if any(token in lowered for token in ("legal gate", "regulatory review")):
        tags.add("legal_gate")
    if any(token in lowered for token in ("owner assignment", "named owner")):
        tags.add("owner_assignment")
    if any(token in lowered for token in ("outside counsel", "sullivan", "counsel")):
        tags.add("outside_counsel")
    if any(
        token in lowered
        for token in ("sanitize", "sanitized", "no-pdf", "no attachment")
    ):
        tags.add("sanitized")
    if any(
        token in lowered
        for token in ("widen", "broad", "everyone", "all comments", "broadly", "blast")
    ):
        tags.update({"broad_external", "coalition_blast"})
    if any(token in lowered for token in ("wide policy", "broadly across trading")):
        tags.add("wide_policy")
    if "raw attachment" in lowered or "unchanged" in lowered:
        tags.add("raw_attachment_forward")
    if any(token in lowered for token in ("urgent", "wide alarm")):
        tags.add("wide_alarm")
    if any(
        token in lowered
        for token in ("fast consensus", "business leaders", "broad exec")
    ):
        tags.add("executive_blast")
    return tags


def _historical_branch_tags(
    event: WhatIfEvent,
    *,
    organization_domain: str,
) -> set[str]:
    tags: set[str] = set()
    if event.flags.consult_legal_specialist:
        tags.add("legal")
    if event.flags.consult_trading_specialist:
        tags.add("trading")
    if event.flags.has_attachment_reference:
        tags.add("attachment_present")
    if event.flags.is_forward:
        tags.add("forward_present")
    if event.flags.is_escalation or event.event_type == "escalation":
        tags.add("escalation_present")
    if _event_external_count(event, organization_domain=organization_domain) > 0:
        tags.add("external_present")
    return tags


def _rank_stability(rollouts: Sequence[WhatIfRankedRolloutResult]) -> float:
    if not rollouts:
        return 0.0
    scores = [rollout.outcome_score.overall_score for rollout in rollouts]
    return round(1.0 - min(1.0, max(scores) - min(scores)), 3)


def _backend_score_for(
    candidate: WhatIfPackCandidateResult,
    backend: WhatIfOutcomeBackendId,
) -> WhatIfBackendScore:
    for score in candidate.backend_scores:
        if score.backend == backend:
            return score
    raise KeyError(f"backend score missing: {backend}")


def _forecast_artifact_paths(
    forecast: WhatIfCounterfactualEstimateResult,
) -> dict[str, str]:
    if forecast.artifacts is None:
        return {}
    paths: dict[str, str] = {}
    if forecast.artifacts.cache_root is not None:
        paths["cache_root"] = str(forecast.artifacts.cache_root)
    if forecast.artifacts.dataset_root is not None:
        paths["dataset_root"] = str(forecast.artifacts.dataset_root)
    if forecast.artifacts.checkpoint_path is not None:
        paths["checkpoint_path"] = str(forecast.artifacts.checkpoint_path)
    if forecast.artifacts.decoder_path is not None:
        paths["decoder_path"] = str(forecast.artifacts.decoder_path)
    return paths


def _render_pilot_backends_note() -> str:
    return "\n".join(
        [
            "# Pilot Backends",
            "",
            "- `decision_transformer` is reserved for direct action ranking from state-action-return traces.",
            "- `trajectory_transformer` is reserved for sequence-level planning over state and action traces.",
            "- `dreamer_v3` stays design-only for now because it fits better once the world is interactive and reward-bearing beyond archive replay.",
        ]
    )


def _slug(value: str) -> str:
    cleaned = "".join(ch.lower() if ch.isalnum() else "_" for ch in value.strip())
    collapsed = "_".join(part for part in cleaned.split("_") if part)
    return collapsed or "research_pack"


def _vector_distance(left: dict[str, float], right: dict[str, float]) -> float:
    keys = sorted(set(left) | set(right))
    if not keys:
        return 1.0
    squared = 0.0
    for key in keys:
        squared += (left.get(key, 0.0) - right.get(key, 0.0)) ** 2
    return sqrt(squared / len(keys))


def _distance_confidence(distance: float) -> float:
    return round(_clamp(1.0 / (1.0 + distance)), 3)


def _event_external_count(
    event: WhatIfEvent,
    *,
    organization_domain: str,
) -> int:
    recipients = list(event.flags.to_recipients) + list(event.flags.cc_recipients)
    return external_recipient_count(
        recipients,
        organization_domain=organization_domain,
    )


def _event_scope(
    event: WhatIfEvent,
    *,
    organization_domain: str,
) -> str:
    external = _event_external_count(
        event,
        organization_domain=organization_domain,
    )
    recipient_count = len(event.flags.to_recipients) + len(event.flags.cc_recipients)
    if recipient_count == 0:
        return "unknown"
    if external == 0:
        return "internal"
    if external == recipient_count:
        return "external"
    return "mixed"


def _reference_external_count(
    event,
    *,
    organization_domain: str,
) -> int:
    recipients = _reference_recipients(event)
    return external_recipient_count(
        recipients,
        organization_domain=organization_domain,
    )


def _reference_scope(
    event,
    *,
    organization_domain: str,
) -> str:
    external = _reference_external_count(
        event,
        organization_domain=organization_domain,
    )
    recipient_count = len(_reference_recipients(event))
    if recipient_count == 0:
        return "unknown"
    if external == 0:
        return "internal"
    if external == recipient_count:
        return "external"
    return "mixed"


def _reference_recipients(event) -> list[str]:
    if hasattr(event, "flags"):
        return list(event.flags.to_recipients) + list(event.flags.cc_recipients)
    return list(getattr(event, "to_recipients", [])) + list(
        getattr(event, "cc_recipients", [])
    )


def _reference_attachment_flag(event) -> bool:
    if hasattr(event, "flags"):
        return bool(event.flags.has_attachment_reference)
    return bool(getattr(event, "has_attachment_reference", False))


def _reference_escalation_flag(event) -> bool:
    if hasattr(event, "flags"):
        return bool(event.flags.is_escalation or event.event_type == "escalation")
    return bool(
        getattr(event, "is_escalation", False) or event.event_type == "escalation"
    )


def _reference_forward_flag(event) -> bool:
    if hasattr(event, "flags"):
        return bool(event.flags.is_forward)
    return bool(getattr(event, "is_forward", False))


def _recipient_scope(
    recipient: str,
    *,
    organization_domain: str,
) -> str:
    if not recipient:
        return "unknown"
    return recipient_scope(
        [recipient],
        organization_domain=organization_domain,
    )


def _reference_timestamp_ms(event) -> int:
    timestamp_text = str(getattr(event, "timestamp", "") or "")
    if hasattr(event, "timestamp_ms"):
        return int(getattr(event, "timestamp_ms"))
    if not timestamp_text:
        return 0
    parsed = datetime.fromisoformat(timestamp_text.replace("Z", "+00:00"))
    return int(parsed.timestamp() * 1000)


def _sequence_delay_bias(contract: WhatIfBackendBranchContract) -> float:
    generated = [
        step.delay_ms for step in contract.sequence_steps if step.phase == "generated"
    ]
    if not generated:
        return contract.average_rollout_signals.delay_risk
    average_delay = sum(generated) / len(generated)
    return _clamp(average_delay / 14_400_000)


def _delay_norm(delay_ms: int) -> float:
    return _clamp(delay_ms / 14_400_000)


def _message_count_norm(message_count: int) -> float:
    if message_count <= 1:
        return 0.0
    return _clamp((message_count - 1) / 3)


def _clamp(value: float) -> float:
    return max(0.0, min(1.0, float(value)))
