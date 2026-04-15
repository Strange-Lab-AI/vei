from __future__ import annotations

from .models import WhatIfOutcomeBackendId

INTEGRATED_BACKENDS: tuple[WhatIfOutcomeBackendId, ...] = (
    "e_jepa",
    "e_jepa_proxy",
    "ft_transformer",
    "ts2vec",
    "g_transformer",
)
PILOT_BACKENDS: tuple[WhatIfOutcomeBackendId, ...] = (
    "decision_transformer",
    "trajectory_transformer",
    "dreamer_v3",
)
DEFAULT_ROLLOUT_SEEDS = list(range(42042, 42050))

__all__ = [
    "DEFAULT_ROLLOUT_SEEDS",
    "INTEGRATED_BACKENDS",
    "PILOT_BACKENDS",
]
