from __future__ import annotations

from importlib import import_module
from typing import Any

from vei.whatif.models import (
    WhatIfBenchmarkBuildResult,
    WhatIfBenchmarkEvalResult,
    WhatIfBenchmarkJudgeResult,
    WhatIfBenchmarkTrainResult,
    WhatIfBackendScore,
    WhatIfCandidateIntervention,
    WhatIfCandidateRanking,
    WhatIfEpisodeManifest,
    WhatIfEpisodeMaterialization,
    WhatIfEventSearchResult,
    WhatIfExperimentResult,
    WhatIfForecastResult,
    WhatIfLLMReplayResult,
    WhatIfObjectivePack,
    WhatIfOutcomeScore,
    WhatIfOutcomeSignals,
    WhatIfPackRunResult,
    WhatIfRankedExperimentResult,
    WhatIfReplaySummary,
    WhatIfResearchPack,
    WhatIfResult,
    WhatIfScenario,
    WhatIfWorld,
)

__all__ = [
    "WhatIfEpisodeManifest",
    "WhatIfEpisodeMaterialization",
    "WhatIfEventSearchResult",
    "WhatIfExperimentResult",
    "WhatIfForecastResult",
    "WhatIfBenchmarkBuildResult",
    "WhatIfBenchmarkEvalResult",
    "WhatIfBenchmarkJudgeResult",
    "WhatIfBenchmarkTrainResult",
    "WhatIfLLMReplayResult",
    "WhatIfBackendScore",
    "WhatIfCandidateIntervention",
    "WhatIfCandidateRanking",
    "WhatIfObjectivePack",
    "WhatIfOutcomeScore",
    "WhatIfOutcomeSignals",
    "WhatIfPackRunResult",
    "WhatIfRankedExperimentResult",
    "WhatIfReplaySummary",
    "WhatIfResearchPack",
    "WhatIfResult",
    "WhatIfScenario",
    "WhatIfWorld",
    "default_forecast_backend",
    "build_branch_point_benchmark",
    "evaluate_branch_point_benchmark_model",
    "forecast_episode",
    "get_research_pack",
    "judge_branch_point_benchmark",
    "list_branch_point_benchmark_models",
    "list_objective_packs",
    "list_research_packs",
    "list_supported_scenarios",
    "load_branch_point_benchmark_build_result",
    "load_branch_point_benchmark_eval_result",
    "load_branch_point_benchmark_judge_result",
    "load_branch_point_benchmark_train_result",
    "load_experiment_result",
    "load_episode_manifest",
    "load_research_pack_run_result",
    "load_ranked_experiment_result",
    "load_world",
    "materialize_episode",
    "replay_episode_baseline",
    "run_research_pack",
    "search_events",
    "train_branch_point_benchmark_model",
    "run_counterfactual_experiment",
    "run_ranked_counterfactual_experiment",
    "run_ejepa_counterfactual",
    "run_ejepa_proxy_counterfactual",
    "run_llm_counterfactual",
    "run_whatif",
]

_API_EXPORTS = {
    "forecast_episode",
    "list_objective_packs",
    "list_supported_scenarios",
    "load_experiment_result",
    "load_episode_manifest",
    "load_ranked_experiment_result",
    "load_world",
    "materialize_episode",
    "replay_episode_baseline",
    "search_events",
    "run_counterfactual_experiment",
    "run_ranked_counterfactual_experiment",
    "run_ejepa_proxy_counterfactual",
    "run_llm_counterfactual",
    "run_whatif",
}
_EJEPA_EXPORTS = {"default_forecast_backend", "run_ejepa_counterfactual"}
_RESEARCH_EXPORTS = {
    "get_research_pack",
    "list_research_packs",
    "load_research_pack_run_result",
    "run_research_pack",
}
_BENCHMARK_EXPORTS = {
    "build_branch_point_benchmark",
    "evaluate_branch_point_benchmark_model",
    "judge_branch_point_benchmark",
    "list_branch_point_benchmark_models",
    "load_branch_point_benchmark_build_result",
    "load_branch_point_benchmark_eval_result",
    "load_branch_point_benchmark_judge_result",
    "load_branch_point_benchmark_train_result",
    "train_branch_point_benchmark_model",
}


def __getattr__(name: str) -> Any:
    if name in _API_EXPORTS:
        module = import_module("vei.whatif.api")
        return getattr(module, name)
    if name in _EJEPA_EXPORTS:
        module = import_module("vei.whatif.ejepa")
        return getattr(module, name)
    if name in _RESEARCH_EXPORTS:
        module = import_module("vei.whatif.research")
        return getattr(module, name)
    if name in _BENCHMARK_EXPORTS:
        module = import_module("vei.whatif.benchmark")
        return getattr(module, name)
    raise AttributeError(f"module 'vei.whatif' has no attribute {name!r}")
