"""Deprecated — use ``vei.rl.policy_frequency`` instead.

This module re-exports ``FrequencyPolicy`` as ``BCPPolicy`` for backward
compatibility.  It will be removed in a future release.
"""

from __future__ import annotations

import warnings

from .policy_frequency import FrequencyPolicy, run_policy  # noqa: F401

warnings.warn(
    "vei.rl.policy_bc is deprecated; use vei.rl.policy_frequency.FrequencyPolicy instead.",
    DeprecationWarning,
    stacklevel=2,
)

BCPPolicy = FrequencyPolicy

__all__ = ["BCPPolicy", "run_policy"]
