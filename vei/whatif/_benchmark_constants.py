from __future__ import annotations

from .models import WhatIfBenchmarkModelId, WhatIfBusinessObjectivePackId

BENCHMARK_MODELS: tuple[WhatIfBenchmarkModelId, ...] = (
    "jepa_latent",
    "full_context_transformer",
    "ft_transformer",
    "sequence_transformer",
    "treatment_transformer",
)

REASSURANCE_TERMS = (
    "please",
    "thanks",
    "thank",
    "appreciate",
    "sorry",
    "confirm",
    "update",
    "review",
)
HOLD_TERMS = ("hold", "pause", "wait", "until", "review")
EXECUTIVE_TERMS = ("executive", "leadership", "kenneth", "lay", "skilling")
MULTI_PARTY_TERMS = ("broad", "everyone", "all", "blast", "coalition", "widely")
SINGLE_PARTY_TERMS = ("only", "single", "one owner", "named owner", "gerald")
BUSINESS_OBJECTIVE_PACK_IDS: tuple[WhatIfBusinessObjectivePackId, ...] = (
    "minimize_enterprise_risk",
    "protect_commercial_position",
    "reduce_org_strain",
    "preserve_stakeholder_trust",
    "maintain_execution_velocity",
)
BENCHMARK_CASE_FAMILIES: tuple[str, ...] = (
    "outside_sharing",
    "legal_contract",
    "commercial_counterparty",
    "executive_regulatory",
    "coordination_strain",
    "org_heat",
    "whistleblower",
    "market_manipulation",
    "crisis_communication",
    "accounting_disclosure",
)

__all__ = [
    "BENCHMARK_CASE_FAMILIES",
    "BENCHMARK_MODELS",
    "BUSINESS_OBJECTIVE_PACK_IDS",
    "EXECUTIVE_TERMS",
    "HOLD_TERMS",
    "MULTI_PARTY_TERMS",
    "REASSURANCE_TERMS",
    "SINGLE_PARTY_TERMS",
]
