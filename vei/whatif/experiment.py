from __future__ import annotations

from pathlib import Path
from typing import Sequence

from vei.project_settings import default_model_for_provider

from .models import (
    WhatIfCandidateIntervention,
    WhatIfCandidateRanking,
    WhatIfEpisodeMaterialization,
    WhatIfExperimentArtifacts,
    WhatIfExperimentMode,
    WhatIfExperimentResult,
    WhatIfForecastBackend,
    WhatIfForecastResult,
    WhatIfInterventionSpec,
    WhatIfLLMReplayResult,
    WhatIfObjectivePackId,
    WhatIfOutcomeSignals,
    WhatIfRankedExperimentArtifacts,
    WhatIfRankedExperimentResult,
    WhatIfRankedRolloutResult,
    WhatIfShadowOutcomeScore,
    WhatIfWorld,
)
from .corpus import event_by_id
from .episode import (
    materialize_episode,
    replay_episode_baseline,
)
from .counterfactual import (
    run_llm_counterfactual,
    run_ejepa_proxy_counterfactual,
    _attach_business_state_to_forecast_result,
)
from .ejepa import default_forecast_backend, run_ejepa_counterfactual
from .ranking import (
    aggregate_outcome_signals,
    get_objective_pack,
    recommendation_reason,
    score_outcome_signals,
    sort_candidates_for_rank,
    summarize_forecast_branch,
    summarize_llm_branch,
)
from .artifacts import (
    render_experiment_overview as _render_experiment_overview,
    render_ranked_experiment_overview as _render_ranked_experiment_overview,
    slug_artifact_label as _slug,
)


def run_counterfactual_experiment(
    world: WhatIfWorld,
    *,
    artifacts_root: str | Path,
    label: str,
    counterfactual_prompt: str,
    selection_scenario: str | None = None,
    selection_prompt: str | None = None,
    thread_id: str | None = None,
    event_id: str | None = None,
    mode: WhatIfExperimentMode = "both",
    forecast_backend: WhatIfForecastBackend | None = None,
    allow_proxy_fallback: bool = True,
    provider: str = "openai",
    model: str = default_model_for_provider("openai"),
    seed: int = 42042,
    ejepa_epochs: int = 4,
    ejepa_batch_size: int = 64,
    ejepa_force_retrain: bool = False,
    ejepa_device: str | None = None,
) -> WhatIfExperimentResult:
    from .api import _selection_for_specific_event, run_whatif, _baseline_tick_ms

    selection = (
        run_whatif(
            world,
            scenario=selection_scenario,
            prompt=selection_prompt,
        )
        if selection_scenario or selection_prompt
        else _selection_for_specific_event(
            world,
            thread_id=thread_id,
            event_id=event_id,
            prompt=counterfactual_prompt,
        )
    )
    selected_thread_id = thread_id
    if selected_thread_id is None and event_id:
        selected_event = event_by_id(world.events, event_id)
        if selected_event is None:
            raise ValueError(f"event not found in world: {event_id}")
        selected_thread_id = selected_event.thread_id
    if selected_thread_id is None:
        selected_thread_id = (
            selection.top_threads[0].thread_id if selection.top_threads else None
        )
    if not selected_thread_id:
        raise ValueError("no matching thread available for the counterfactual run")

    root = Path(artifacts_root).expanduser().resolve() / _slug(label)
    workspace_root = root / "workspace"
    materialization = materialize_episode(
        world,
        root=workspace_root,
        thread_id=selected_thread_id,
        event_id=event_id,
    )
    baseline = replay_episode_baseline(
        workspace_root,
        tick_ms=_baseline_tick_ms(materialization.baseline_dataset_path),
        seed=seed,
    )
    llm_result: WhatIfLLMReplayResult | None = None
    if mode in {"llm", "both"}:
        llm_result = run_llm_counterfactual(
            workspace_root,
            prompt=counterfactual_prompt,
            provider=provider,
            model=model,
            seed=seed,
        )
    forecast_result: WhatIfForecastResult | None = None
    resolved_forecast_backend = forecast_backend or (
        mode if mode in {"e_jepa", "e_jepa_proxy"} else default_forecast_backend()
    )
    if mode in {"e_jepa", "e_jepa_proxy", "both"}:
        if resolved_forecast_backend == "e_jepa":
            forecast_result = run_ejepa_counterfactual(
                workspace_root,
                prompt=counterfactual_prompt,
                source=world.source,
                source_dir=world.source_dir,
                thread_id=selected_thread_id,
                branch_event_id=materialization.branch_event_id,
                llm_messages=llm_result.messages if llm_result is not None else None,
                epochs=ejepa_epochs,
                batch_size=ejepa_batch_size,
                force_retrain=ejepa_force_retrain,
                device=ejepa_device,
            )
            if forecast_result.status == "error" and allow_proxy_fallback:
                proxy_result = run_ejepa_proxy_counterfactual(
                    workspace_root,
                    prompt=counterfactual_prompt,
                )
                proxy_result.notes.insert(
                    0,
                    "Real E-JEPA forecast failed, so this experiment fell back to the proxy forecast.",
                )
                if forecast_result.error:
                    proxy_result.notes.append(
                        f"Original E-JEPA error: {forecast_result.error}"
                    )
                forecast_result = proxy_result
        else:
            forecast_result = run_ejepa_proxy_counterfactual(
                workspace_root,
                prompt=counterfactual_prompt,
            )
    if forecast_result is not None:
        forecast_result = _attach_business_state_to_forecast_result(
            forecast_result,
            branch_event=materialization.branch_event,
            organization_domain=materialization.organization_domain,
            public_context=materialization.public_context,
        )

    result_path = root / "whatif_experiment_result.json"
    overview_path = root / "whatif_experiment_overview.md"
    llm_path = root / "whatif_llm_result.json" if llm_result is not None else None
    forecast_path = None
    if forecast_result is not None:
        forecast_filename = (
            "whatif_ejepa_result.json"
            if forecast_result.backend == "e_jepa"
            else "whatif_ejepa_proxy_result.json"
        )
        forecast_path = root / forecast_filename
    root.mkdir(parents=True, exist_ok=True)

    artifacts = WhatIfExperimentArtifacts(
        root=root,
        result_json_path=result_path,
        overview_markdown_path=overview_path,
        llm_json_path=llm_path,
        forecast_json_path=forecast_path,
    )
    result = WhatIfExperimentResult(
        mode=mode,
        label=label,
        intervention=WhatIfInterventionSpec(
            label=label,
            prompt=counterfactual_prompt,
            objective=(
                selection.scenario.description
                if selection.scenario.description
                else "counterfactual replay"
            ),
            scenario_id=selection.scenario.scenario_id,
            thread_id=selected_thread_id,
            branch_event_id=materialization.branch_event_id,
        ),
        selection=selection,
        materialization=materialization,
        baseline=baseline,
        llm_result=llm_result,
        forecast_result=forecast_result,
        artifacts=artifacts,
    )
    result_path.write_text(result.model_dump_json(indent=2), encoding="utf-8")
    if llm_result is not None and llm_path is not None:
        llm_path.write_text(llm_result.model_dump_json(indent=2), encoding="utf-8")
    if forecast_result is not None and forecast_path is not None:
        forecast_path.write_text(
            forecast_result.model_dump_json(indent=2),
            encoding="utf-8",
        )
    overview_path.write_text(
        _render_experiment_overview(result),
        encoding="utf-8",
    )
    return result


def run_ranked_counterfactual_experiment(
    world: WhatIfWorld,
    *,
    artifacts_root: str | Path,
    label: str,
    objective_pack_id: WhatIfObjectivePackId | str,
    candidate_interventions: Sequence[str | WhatIfCandidateIntervention],
    selection_scenario: str | None = None,
    selection_prompt: str | None = None,
    thread_id: str | None = None,
    event_id: str | None = None,
    rollout_count: int = 4,
    provider: str = "openai",
    model: str = default_model_for_provider("openai"),
    seed: int = 42042,
    shadow_forecast_backend: WhatIfForecastBackend | None = None,
    allow_proxy_fallback: bool = True,
    ejepa_epochs: int = 4,
    ejepa_batch_size: int = 64,
    ejepa_force_retrain: bool = False,
    ejepa_device: str | None = None,
) -> WhatIfRankedExperimentResult:
    from .api import _selection_for_specific_event, run_whatif, _baseline_tick_ms

    if rollout_count < 1 or rollout_count > 16:
        raise ValueError("rollout_count must be between 1 and 16")

    normalized_candidates = _normalize_candidate_interventions(candidate_interventions)
    if not normalized_candidates:
        raise ValueError("at least one candidate intervention is required")
    if len(normalized_candidates) > 5:
        raise ValueError("ranked what-if supports at most 5 candidate interventions")

    objective_pack = get_objective_pack(str(objective_pack_id))
    selection = (
        run_whatif(
            world,
            scenario=selection_scenario,
            prompt=selection_prompt,
        )
        if selection_scenario or selection_prompt
        else _selection_for_specific_event(
            world,
            thread_id=thread_id,
            event_id=event_id,
            prompt=normalized_candidates[0].prompt,
        )
    )
    selected_thread_id = thread_id
    if selected_thread_id is None and event_id:
        selected_event = event_by_id(world.events, event_id)
        if selected_event is None:
            raise ValueError(f"event not found in world: {event_id}")
        selected_thread_id = selected_event.thread_id
    if selected_thread_id is None:
        selected_thread_id = (
            selection.top_threads[0].thread_id if selection.top_threads else None
        )
    if not selected_thread_id:
        raise ValueError("no matching thread available for the counterfactual run")

    root = Path(artifacts_root).expanduser().resolve() / _slug(label)
    workspace_root = root / "workspace"
    materialization = materialize_episode(
        world,
        root=workspace_root,
        thread_id=selected_thread_id,
        event_id=event_id,
    )
    baseline = replay_episode_baseline(
        workspace_root,
        tick_ms=_baseline_tick_ms(materialization.baseline_dataset_path),
        seed=seed,
    )

    candidate_results: list[WhatIfCandidateRanking] = []
    resolved_shadow_backend = shadow_forecast_backend or default_forecast_backend()
    for candidate_index, intervention in enumerate(normalized_candidates):
        rollouts: list[WhatIfRankedRolloutResult] = []
        rollout_signals: list[WhatIfOutcomeSignals] = []
        first_rollout: WhatIfLLMReplayResult | None = None
        for rollout_index in range(rollout_count):
            rollout_seed = seed + (candidate_index * 100) + rollout_index
            llm_result = run_llm_counterfactual(
                workspace_root,
                prompt=intervention.prompt,
                provider=provider,
                model=model,
                seed=rollout_seed,
            )
            if first_rollout is None:
                first_rollout = llm_result
            outcome_signals = summarize_llm_branch(
                branch_event=materialization.branch_event,
                llm_result=llm_result,
                organization_domain=materialization.organization_domain,
            )
            outcome_score = score_outcome_signals(
                pack=objective_pack,
                outcome=outcome_signals,
            )
            rollout_signals.append(outcome_signals)
            rollouts.append(
                WhatIfRankedRolloutResult(
                    rollout_index=rollout_index + 1,
                    seed=rollout_seed,
                    llm_result=llm_result,
                    outcome_signals=outcome_signals,
                    outcome_score=outcome_score,
                )
            )

        average_signals = aggregate_outcome_signals(rollout_signals)
        outcome_score = score_outcome_signals(
            pack=objective_pack,
            outcome=average_signals,
        )
        shadow = _run_ranked_shadow_score(
            world=world,
            workspace_root=workspace_root,
            materialization=materialization,
            objective_pack=objective_pack,
            prompt=intervention.prompt,
            llm_result=first_rollout,
            forecast_backend=resolved_shadow_backend,
            allow_proxy_fallback=allow_proxy_fallback,
            ejepa_epochs=ejepa_epochs,
            ejepa_batch_size=ejepa_batch_size,
            ejepa_force_retrain=ejepa_force_retrain,
            ejepa_device=ejepa_device,
        )
        candidate_results.append(
            WhatIfCandidateRanking(
                intervention=intervention,
                rollout_count=len(rollouts),
                average_outcome_signals=average_signals,
                outcome_score=outcome_score,
                reason="",
                rollouts=rollouts,
                shadow=shadow,
                business_state_change=shadow.forecast_result.business_state_change,
            )
        )

    ordered_labels = sort_candidates_for_rank(
        [
            (
                item.intervention.label,
                item.average_outcome_signals,
                item.outcome_score,
            )
            for item in candidate_results
        ]
    )
    rank_map = {label: index + 1 for index, label in enumerate(ordered_labels)}
    recommended_label = ordered_labels[0] if ordered_labels else ""
    for item in candidate_results:
        item.rank = rank_map[item.intervention.label]
        item.reason = _candidate_ranking_reason(
            candidate=item,
            objective_pack_id=objective_pack.pack_id,
            is_best=item.intervention.label == recommended_label,
        )
    candidate_results.sort(key=lambda item: item.rank)

    result_path = root / "whatif_ranked_result.json"
    overview_path = root / "whatif_ranked_overview.md"
    root.mkdir(parents=True, exist_ok=True)
    artifacts = WhatIfRankedExperimentArtifacts(
        root=root,
        result_json_path=result_path,
        overview_markdown_path=overview_path,
    )
    result = WhatIfRankedExperimentResult(
        label=label,
        objective_pack=objective_pack,
        selection=selection,
        materialization=materialization,
        baseline=baseline,
        candidates=candidate_results,
        recommended_candidate_label=recommended_label,
        artifacts=artifacts,
    )
    result_path.write_text(result.model_dump_json(indent=2), encoding="utf-8")
    overview_path.write_text(
        _render_ranked_experiment_overview(result),
        encoding="utf-8",
    )
    return result


def load_experiment_result(root: str | Path) -> WhatIfExperimentResult:
    result_path = Path(root).expanduser().resolve() / "whatif_experiment_result.json"
    if not result_path.exists():
        raise ValueError(f"what-if experiment result not found: {result_path}")
    return WhatIfExperimentResult.model_validate_json(
        result_path.read_text(encoding="utf-8")
    )


def load_ranked_experiment_result(root: str | Path) -> WhatIfRankedExperimentResult:
    result_path = Path(root).expanduser().resolve() / "whatif_ranked_result.json"
    if not result_path.exists():
        raise ValueError(f"ranked what-if result not found: {result_path}")
    return WhatIfRankedExperimentResult.model_validate_json(
        result_path.read_text(encoding="utf-8")
    )


def _normalize_candidate_interventions(
    values: Sequence[str | WhatIfCandidateIntervention],
) -> list[WhatIfCandidateIntervention]:
    normalized: list[WhatIfCandidateIntervention] = []
    for index, value in enumerate(values, start=1):
        if isinstance(value, WhatIfCandidateIntervention):
            prompt = value.prompt.strip()
            label = value.label.strip() or _candidate_label(prompt, index=index)
        else:
            prompt = str(value).strip()
            label = _candidate_label(prompt, index=index)
        if not prompt:
            continue
        normalized.append(
            WhatIfCandidateIntervention(
                label=label,
                prompt=prompt,
            )
        )
    return normalized


def _candidate_label(prompt: str, *, index: int) -> str:
    cleaned = " ".join(prompt.split())
    if not cleaned:
        return f"Option {index}"
    words = cleaned.split()
    preview = " ".join(words[:5])
    if len(words) > 5:
        preview += "..."
    return preview


def _run_ranked_shadow_score(
    *,
    world: WhatIfWorld,
    workspace_root: Path,
    materialization: WhatIfEpisodeMaterialization,
    objective_pack,
    prompt: str,
    llm_result: WhatIfLLMReplayResult | None,
    forecast_backend: WhatIfForecastBackend,
    allow_proxy_fallback: bool,
    ejepa_epochs: int,
    ejepa_batch_size: int,
    ejepa_force_retrain: bool,
    ejepa_device: str | None,
) -> WhatIfShadowOutcomeScore:
    if forecast_backend == "e_jepa":
        forecast_result = run_ejepa_counterfactual(
            workspace_root,
            prompt=prompt,
            source=world.source,
            source_dir=world.source_dir,
            thread_id=materialization.thread_id,
            branch_event_id=materialization.branch_event_id,
            llm_messages=llm_result.messages if llm_result is not None else None,
            epochs=ejepa_epochs,
            batch_size=ejepa_batch_size,
            force_retrain=ejepa_force_retrain,
            device=ejepa_device,
        )
        if forecast_result.status == "error" and allow_proxy_fallback:
            proxy_result = run_ejepa_proxy_counterfactual(
                workspace_root,
                prompt=prompt,
            )
            proxy_result.notes.insert(
                0,
                "Real E-JEPA shadow scoring failed, so this candidate used the proxy forecast.",
            )
            if forecast_result.error:
                proxy_result.notes.append(
                    f"Original E-JEPA error: {forecast_result.error}"
                )
            forecast_result = proxy_result
    else:
        forecast_result = run_ejepa_proxy_counterfactual(
            workspace_root,
            prompt=prompt,
        )

    outcome_signals = summarize_forecast_branch(forecast_result)
    outcome_score = score_outcome_signals(
        pack=objective_pack,
        outcome=outcome_signals,
    )
    forecast_result = _attach_business_state_to_forecast_result(
        forecast_result,
        branch_event=materialization.branch_event,
        organization_domain=materialization.organization_domain,
        public_context=materialization.public_context,
    )
    return WhatIfShadowOutcomeScore(
        backend=forecast_result.backend,
        outcome_signals=outcome_signals,
        outcome_score=outcome_score,
        forecast_result=forecast_result,
    )


def _candidate_ranking_reason(
    *,
    candidate: WhatIfCandidateRanking,
    objective_pack_id: WhatIfObjectivePackId,
    is_best: bool,
) -> str:
    if is_best:
        objective_pack = get_objective_pack(objective_pack_id)
        return recommendation_reason(
            pack=objective_pack,
            outcome=candidate.average_outcome_signals,
            score=candidate.outcome_score,
            rollout_count=candidate.rollout_count,
        )
    if objective_pack_id == "contain_exposure":
        return "Lower-ranked because it leaves more exposure in the simulated branches."
    if objective_pack_id == "reduce_delay":
        return "Lower-ranked because it still carries a slower follow-up pattern."
    return "Lower-ranked because it protects the relationship less consistently."
