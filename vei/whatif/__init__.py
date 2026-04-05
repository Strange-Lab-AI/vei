from __future__ import annotations

from importlib import import_module
from typing import Any

from vei.whatif.models import (
    WhatIfEpisodeManifest,
    WhatIfEpisodeMaterialization,
    WhatIfEventSearchResult,
    WhatIfExperimentResult,
    WhatIfForecastResult,
    WhatIfLLMReplayResult,
    WhatIfReplaySummary,
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
    "WhatIfLLMReplayResult",
    "WhatIfReplaySummary",
    "WhatIfResult",
    "WhatIfScenario",
    "WhatIfWorld",
    "default_forecast_backend",
    "forecast_episode",
    "list_supported_scenarios",
    "load_experiment_result",
    "load_episode_manifest",
    "load_world",
    "materialize_episode",
    "replay_episode_baseline",
    "search_events",
    "run_counterfactual_experiment",
    "run_ejepa_counterfactual",
    "run_ejepa_proxy_counterfactual",
    "run_llm_counterfactual",
    "run_whatif",
]

_API_EXPORTS = {
    "forecast_episode",
    "list_supported_scenarios",
    "load_experiment_result",
    "load_episode_manifest",
    "load_world",
    "materialize_episode",
    "replay_episode_baseline",
    "search_events",
    "run_counterfactual_experiment",
    "run_ejepa_proxy_counterfactual",
    "run_llm_counterfactual",
    "run_whatif",
}
_EJEPA_EXPORTS = {"default_forecast_backend", "run_ejepa_counterfactual"}


def __getattr__(name: str) -> Any:
    if name in _API_EXPORTS:
        module = import_module("vei.whatif.api")
        return getattr(module, name)
    if name in _EJEPA_EXPORTS:
        module = import_module("vei.whatif.ejepa")
        return getattr(module, name)
    raise AttributeError(f"module 'vei.whatif' has no attribute {name!r}")
