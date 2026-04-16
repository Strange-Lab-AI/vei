from __future__ import annotations

from typing import Literal

WhatIfSourceName = Literal["enron", "mail_archive", "company_history"]
WhatIfScenarioId = Literal[
    "compliance_gateway",
    "escalation_firewall",
    "external_dlp",
    "approval_chain_enforcement",
]
WhatIfRenderFormat = Literal["json", "markdown"]
WhatIfExperimentMode = Literal[
    "llm", "e_jepa", "e_jepa_proxy", "heuristic_baseline", "both"
]
WhatIfForecastBackend = Literal[
    "e_jepa",
    "e_jepa_proxy",
    "heuristic_baseline",
    "reference",
]
WhatIfOutcomeBackendId = Literal[
    "e_jepa",
    "e_jepa_proxy",
    "heuristic_baseline",
    "ft_transformer",
    "ts2vec",
    "g_transformer",
    "decision_transformer",
    "trajectory_transformer",
    "dreamer_v3",
]
WhatIfObjectivePackId = Literal[
    "contain_exposure",
    "reduce_delay",
    "protect_relationship",
]
WhatIfResearchHypothesisLabel = Literal[
    "best_expected",
    "middle_expected",
    "worst_expected",
]
WhatIfBackendScoreStatus = Literal["ok", "skipped", "error", "fallback"]
WhatIfBenchmarkModelId = Literal[
    "jepa_latent",
    "full_context_transformer",
    "ft_transformer",
    "sequence_transformer",
    "treatment_transformer",
]
WhatIfBusinessObjectivePackId = Literal[
    "minimize_enterprise_risk",
    "protect_commercial_position",
    "reduce_org_strain",
    "preserve_stakeholder_trust",
    "maintain_execution_velocity",
]
WhatIfBenchmarkSplit = Literal["train", "validation", "test", "heldout"]
WhatIfAttachmentPolicy = Literal["none", "present", "sanitized"]
WhatIfEscalationLevel = Literal["none", "manager", "executive"]
WhatIfOwnerClarity = Literal["unclear", "single_owner", "multi_owner"]
WhatIfReassuranceStyle = Literal["low", "medium", "high"]
WhatIfReviewPath = Literal[
    "none",
    "internal_legal",
    "outside_counsel",
    "business_owner",
    "cross_functional",
    "hr",
    "executive",
]
WhatIfCoordinationBreadth = Literal["single_owner", "narrow", "targeted", "broad"]
WhatIfOutsideSharingPosture = Literal[
    "internal_only",
    "status_only",
    "limited_external",
    "broad_external",
]
WhatIfDecisionPosture = Literal["hold", "review", "resolve", "escalate"]
WhatIfBusinessConfidence = Literal["low", "medium", "high"]
WhatIfBusinessImpactEffect = Literal["better", "worse", "flat"]
WhatIfBusinessImpactMagnitude = Literal["flat", "slight", "moderate", "strong"]
WhatIfBusinessStateLevel = Literal["very_low", "low", "medium", "high", "very_high"]

__all__ = [
    "WhatIfAttachmentPolicy",
    "WhatIfBackendScoreStatus",
    "WhatIfBenchmarkModelId",
    "WhatIfBenchmarkSplit",
    "WhatIfBusinessConfidence",
    "WhatIfBusinessImpactEffect",
    "WhatIfBusinessImpactMagnitude",
    "WhatIfBusinessObjectivePackId",
    "WhatIfBusinessStateLevel",
    "WhatIfCoordinationBreadth",
    "WhatIfDecisionPosture",
    "WhatIfEscalationLevel",
    "WhatIfExperimentMode",
    "WhatIfForecastBackend",
    "WhatIfObjectivePackId",
    "WhatIfOutcomeBackendId",
    "WhatIfOutsideSharingPosture",
    "WhatIfOwnerClarity",
    "WhatIfReassuranceStyle",
    "WhatIfRenderFormat",
    "WhatIfResearchHypothesisLabel",
    "WhatIfReviewPath",
    "WhatIfScenarioId",
    "WhatIfSourceName",
]
