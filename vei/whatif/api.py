from __future__ import annotations

import importlib
from typing import Any

from .analysis import (
    list_branch_candidates,
    list_objective_packs,
    load_world,
    recommend_branch_thread,
    run_whatif,
    search_events,
    select_specific_event,
)
from .benchmark import (
    build_branch_point_benchmark,
    evaluate_branch_point_benchmark_model,
    judge_branch_point_benchmark,
    list_branch_point_benchmark_models,
    load_branch_point_benchmark_build_result,
    load_branch_point_benchmark_eval_result,
    load_branch_point_benchmark_judge_result,
    load_branch_point_benchmark_study_result,
    load_branch_point_benchmark_train_result,
    run_branch_point_benchmark_study,
    train_branch_point_benchmark_model,
)
from .benchmark_runtime import run_branch_point_benchmark_prediction
from .multitenant_benchmark import (
    MultiTenantBenchmarkSource,
    build_candidate_generation_prompt,
    build_multitenant_world_model_benchmark,
    validate_candidate_diversity,
)
from .counterfactual import (
    estimate_counterfactual_delta,
    run_llm_counterfactual,
)
from .critical_decision_benchmark import (
    CriticalDecisionRunResult,
    build_critical_candidate_generation_prompt,
    build_critical_decision_benchmark,
    run_critical_decision_benchmark,
    validate_critical_candidate_diversity,
)
from .news_state_points import (
    NewsStatePointCandidateInput,
    NewsStatePointRunResult,
    build_news_state_point,
    run_news_state_point_counterfactual,
)
from .models import (
    WhatIfActionSchema,
    WhatIfAuditRecord,
    WhatIfBenchmarkDatasetRow,
    WhatIfBranchSummaryFeature,
    WhatIfCandidateIntervention,
    WhatIfEventReference,
    WhatIfExperimentMode,
    WhatIfJudgedPairwiseComparison,
    WhatIfObjectivePackId,
    WhatIfObservedEvidenceHeads,
    WhatIfPreBranchContract,
    WhatIfSequenceStep,
)
from .filenames import (
    BUSINESS_STATE_COMPARISON_FILE,
    BUSINESS_STATE_COMPARISON_OVERVIEW_FILE,
    CONTEXT_SNAPSHOT_FILE,
    EJEPA_RESULT_FILE,
    EPISODE_MANIFEST_FILE,
    EXPERIMENT_OVERVIEW_FILE,
    EXPERIMENT_RESULT_FILE,
    HEURISTIC_FORECAST_FILE,
    LLM_RESULT_FILE,
    PUBLIC_CONTEXT_FILE,
    REFERENCE_FORECAST_FILE,
    RANKED_OVERVIEW_FILE,
    RANKED_RESULT_FILE,
    SCRUBBED_PATH_PLACEHOLDER,
    STUDIO_SAVED_FORECAST_FILES,
    WORKSPACE_DIRECTORY,
)
from .decision import build_decision_scene, build_saved_decision_scene
from .ejepa import default_forecast_backend, run_ejepa_counterfactual
from .macro_outcomes import (
    MACRO_CALIBRATION_METRICS,
    MACRO_CALIBRATION_REPORT_PATH,
    attach_macro_outcomes_to_forecast_result,
    attach_macro_outcomes_to_historical_score,
    macro_delta_from_prompt,
    preview_macro_outcomes_for_prompt,
)
from ._saved_bundle import (
    build_saved_ranked_result_payload,
    resolve_saved_whatif_bundle,
)
from ._source_locator import (
    resolve_whatif_company_history_path,
    resolve_whatif_mail_archive_path,
    resolve_whatif_rosetta_dir,
    resolve_whatif_source_path,
)
from .episode import (
    load_episode_manifest,
    materialize_episode,
    replay_episode_baseline,
    score_historical_tail,
)
from .experiment import (
    load_experiment_result,
    load_ranked_experiment_result,
    run_counterfactual_experiment,
    run_ranked_counterfactual_experiment,
)
from .research import (
    get_research_pack,
    list_research_packs,
    load_research_pack_run_result,
    run_research_pack,
)
from vei.context.api import build_public_context, empty_public_context
from .scenario_registry import list_supported_scenarios
from .workspace_export import export_workspace_history_snapshot

_BOUNDARY_EXPORTS = (
    WhatIfActionSchema,
    WhatIfAuditRecord,
    WhatIfBenchmarkDatasetRow,
    WhatIfBranchSummaryFeature,
    WhatIfCandidateIntervention,
    WhatIfEventReference,
    WhatIfExperimentMode,
    WhatIfJudgedPairwiseComparison,
    WhatIfObjectivePackId,
    WhatIfObservedEvidenceHeads,
    WhatIfPreBranchContract,
    WhatIfSequenceStep,
)

__all__ = [
    "build_branch_point_benchmark",
    "build_candidate_generation_prompt",
    "build_critical_candidate_generation_prompt",
    "build_critical_decision_benchmark",
    "build_decision_scene",
    "build_multitenant_world_model_benchmark",
    "build_news_state_point",
    "build_saved_decision_scene",
    "build_public_context",
    "CriticalDecisionRunResult",
    "NewsStatePointCandidateInput",
    "NewsStatePointRunResult",
    "MACRO_CALIBRATION_METRICS",
    "MACRO_CALIBRATION_REPORT_PATH",
    "attach_macro_outcomes_to_forecast_result",
    "attach_macro_outcomes_to_historical_score",
    "default_forecast_backend",
    "empty_public_context",
    "estimate_counterfactual_delta",
    "evaluate_branch_point_benchmark_model",
    "export_workspace_history_snapshot",
    "get_research_pack",
    "judge_branch_point_benchmark",
    "list_branch_candidates",
    "list_branch_point_benchmark_models",
    "list_objective_packs",
    "list_research_packs",
    "list_supported_scenarios",
    "load_branch_point_benchmark_build_result",
    "load_branch_point_benchmark_eval_result",
    "load_branch_point_benchmark_judge_result",
    "load_branch_point_benchmark_study_result",
    "load_branch_point_benchmark_train_result",
    "load_episode_manifest",
    "load_experiment_result",
    "load_ranked_experiment_result",
    "load_research_pack_run_result",
    "load_world",
    "macro_delta_from_prompt",
    "materialize_episode",
    "MultiTenantBenchmarkSource",
    "preview_macro_outcomes_for_prompt",
    "build_saved_ranked_result_payload",
    "BUSINESS_STATE_COMPARISON_FILE",
    "BUSINESS_STATE_COMPARISON_OVERVIEW_FILE",
    "CONTEXT_SNAPSHOT_FILE",
    "EJEPA_RESULT_FILE",
    "EPISODE_MANIFEST_FILE",
    "EXPERIMENT_OVERVIEW_FILE",
    "EXPERIMENT_RESULT_FILE",
    "HEURISTIC_FORECAST_FILE",
    "LLM_RESULT_FILE",
    "PUBLIC_CONTEXT_FILE",
    "REFERENCE_FORECAST_FILE",
    "RANKED_OVERVIEW_FILE",
    "RANKED_RESULT_FILE",
    "SCRUBBED_PATH_PLACEHOLDER",
    "STUDIO_SAVED_FORECAST_FILES",
    "WORKSPACE_DIRECTORY",
    "providers",  # noqa: F822 — lazy-loaded via __getattr__
    "recommend_branch_thread",
    "replay_episode_baseline",
    "resolve_saved_whatif_bundle",
    "resolve_whatif_company_history_path",
    "resolve_whatif_mail_archive_path",
    "resolve_whatif_rosetta_dir",
    "resolve_whatif_source_path",
    "run_branch_point_benchmark_study",
    "run_branch_point_benchmark_prediction",
    "run_counterfactual_experiment",
    "run_critical_decision_benchmark",
    "run_ejepa_counterfactual",
    "run_llm_counterfactual",
    "run_news_state_point_counterfactual",
    "run_ranked_counterfactual_experiment",
    "run_research_pack",
    "run_whatif",
    "score_historical_tail",
    "search_events",
    "select_specific_event",
    "train_branch_point_benchmark_model",
    "validate_candidate_diversity",
    "validate_critical_candidate_diversity",
]


def __getattr__(name: str) -> Any:
    if name == "providers":
        return importlib.import_module("vei.llm.providers")
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
