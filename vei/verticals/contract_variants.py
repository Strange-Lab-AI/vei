from __future__ import annotations

from copy import deepcopy
from typing import Any

from pydantic import BaseModel, Field

from vei.contract.models import (
    ContractPredicateSpec,
    ContractSpec,
    PolicyInvariantSpec,
    RewardTermSpec,
)


class VerticalContractVariantSpec(BaseModel):
    vertical_name: str
    name: str
    title: str
    description: str
    objective_summary: str
    rationale: str
    success_predicates: list[ContractPredicateSpec] = Field(default_factory=list)
    forbidden_predicates: list[ContractPredicateSpec] = Field(default_factory=list)
    policy_invariants: list[PolicyInvariantSpec] = Field(default_factory=list)
    reward_terms: list[RewardTermSpec] = Field(default_factory=list)
    metadata: dict[str, Any] = Field(default_factory=dict)


def list_vertical_contract_variants(
    vertical_name: str,
) -> list[VerticalContractVariantSpec]:
    key = vertical_name.strip().lower()
    if key not in _VERTICAL_CONTRACT_VARIANTS:
        raise KeyError(f"unknown vertical contract variants: {vertical_name}")
    return list(_VERTICAL_CONTRACT_VARIANTS[key].values())


def get_vertical_contract_variant(
    vertical_name: str, variant_name: str
) -> VerticalContractVariantSpec:
    key = vertical_name.strip().lower()
    variant_key = variant_name.strip().lower()
    variants = _VERTICAL_CONTRACT_VARIANTS.get(key)
    if variants is None or variant_key not in variants:
        raise KeyError(f"unknown contract variant: {vertical_name}/{variant_name}")
    return variants[variant_key]


def default_vertical_contract_variant(
    vertical_name: str,
) -> VerticalContractVariantSpec:
    key = vertical_name.strip().lower()
    return list_vertical_contract_variants(key)[0]


def apply_vertical_contract_variant(
    contract: ContractSpec,
    variant: VerticalContractVariantSpec,
) -> ContractSpec:
    payload = deepcopy(contract.model_dump(mode="json"))
    payload["success_predicates"].extend(
        item.model_dump(mode="json") for item in variant.success_predicates
    )
    payload["forbidden_predicates"].extend(
        item.model_dump(mode="json") for item in variant.forbidden_predicates
    )
    payload["policy_invariants"].extend(
        item.model_dump(mode="json") for item in variant.policy_invariants
    )
    payload["reward_terms"].extend(
        item.model_dump(mode="json") for item in variant.reward_terms
    )
    payload["metadata"] = {
        **dict(payload.get("metadata") or {}),
        "vertical_contract_variant": variant.name,
        "vertical_contract_variant_title": variant.title,
        "vertical_contract_objective_summary": variant.objective_summary,
        "vertical_contract_rationale": variant.rationale,
        **dict(variant.metadata),
    }
    return ContractSpec.model_validate(payload)


_VERTICAL_CONTRACT_VARIANTS: dict[str, dict[str, VerticalContractVariantSpec]] = {
    "real_estate_management": {
        "opening_readiness": VerticalContractVariantSpec(
            vertical_name="real_estate_management",
            name="opening_readiness",
            title="Opening Readiness",
            description="Default business contract: make the tenant opening safe and complete on time.",
            objective_summary="Prioritize a valid, on-time opening with aligned lease, vendor, and unit state.",
            rationale="This is the default Harbor Point business objective and the right baseline for the world pack.",
            policy_invariants=[
                PolicyInvariantSpec(
                    name="tenant_opening_validity",
                    description="Tenant opening artifacts, lease execution, and unit reservation must all align before opening.",
                    metadata={"origin": "simulated", "variant": "opening_readiness"},
                )
            ],
            reward_terms=[
                RewardTermSpec(
                    name="opening_readiness_bias",
                    weight=2.0,
                    description="Reward getting to a valid opening state without missing the deadline.",
                    metadata={"origin": "simulated", "variant": "opening_readiness"},
                )
            ],
        ),
        "minimize_tenant_disruption": VerticalContractVariantSpec(
            vertical_name="real_estate_management",
            name="minimize_tenant_disruption",
            title="Minimize Tenant Disruption",
            description="Bias toward tenant continuity and communication quality when there is a tradeoff.",
            objective_summary="Prefer mitigations that preserve tenant trust and minimize move-in disruption.",
            rationale="Useful when the same property problem should be solved with a softer customer-experience objective.",
            policy_invariants=[
                PolicyInvariantSpec(
                    name="tenant_experience_bias",
                    description="When a tradeoff exists, prefer actions that reduce tenant disruption and keep the communication trail current.",
                    metadata={
                        "origin": "simulated",
                        "variant": "minimize_tenant_disruption",
                    },
                )
            ],
            reward_terms=[
                RewardTermSpec(
                    name="tenant_disruption_penalty",
                    weight=1.5,
                    term_type="penalty",
                    description="Penalize outcomes that leave tenant-facing artifacts or reservations inconsistent.",
                    metadata={
                        "origin": "simulated",
                        "variant": "minimize_tenant_disruption",
                    },
                )
            ],
        ),
        "safety_over_speed": VerticalContractVariantSpec(
            vertical_name="real_estate_management",
            name="safety_over_speed",
            title="Safety Over Speed",
            description="If the opening cannot be made safe, escalate rather than force completion.",
            objective_summary="Favor safe escalation and readiness integrity over preserving the original opening date.",
            rationale="Shows that the same world can optimize for a more conservative executive preference.",
            policy_invariants=[
                PolicyInvariantSpec(
                    name="safe_opening_bias",
                    description="Never preserve schedule at the cost of an invalid tenant opening.",
                    metadata={"origin": "simulated", "variant": "safety_over_speed"},
                )
            ],
            reward_terms=[
                RewardTermSpec(
                    name="safety_bias",
                    weight=2.5,
                    description="Reward conservative resolution paths that protect readiness and escalation discipline.",
                    metadata={"origin": "simulated", "variant": "safety_over_speed"},
                )
            ],
        ),
    },
    "digital_marketing_agency": {
        "launch_safely": VerticalContractVariantSpec(
            vertical_name="digital_marketing_agency",
            name="launch_safely",
            title="Launch Safely",
            description="Default business contract: launch only when approvals, pacing, and artifacts are safe.",
            objective_summary="Protect launch integrity and prevent unapproved or unsafe spend from reaching the client.",
            rationale="This is the default Northstar Growth launch objective.",
            policy_invariants=[
                PolicyInvariantSpec(
                    name="approval_before_spend",
                    description="Creative approval and refreshed reporting must both exist before launch can be considered safe.",
                    metadata={"origin": "simulated", "variant": "launch_safely"},
                )
            ],
            reward_terms=[
                RewardTermSpec(
                    name="launch_integrity_bias",
                    weight=2.0,
                    description="Reward launch completion only when approval and pacing are safe together.",
                    metadata={"origin": "simulated", "variant": "launch_safely"},
                )
            ],
        ),
        "protect_budget": VerticalContractVariantSpec(
            vertical_name="digital_marketing_agency",
            name="protect_budget",
            title="Protect Budget",
            description="Bias toward spend control and pacing discipline when performance pressure rises.",
            objective_summary="Prefer budget protection and pacing hygiene over preserving launch velocity.",
            rationale="Useful for demonstrating how the same campaign world changes when finance is the top concern.",
            policy_invariants=[
                PolicyInvariantSpec(
                    name="budget_first_bias",
                    description="Spend should only continue when pacing is inside the safe operating envelope.",
                    metadata={"origin": "simulated", "variant": "protect_budget"},
                )
            ],
            reward_terms=[
                RewardTermSpec(
                    name="budget_protection_bias",
                    weight=2.5,
                    description="Reward actions that reduce overspend risk quickly.",
                    metadata={"origin": "simulated", "variant": "protect_budget"},
                )
            ],
        ),
        "client_comms_first": VerticalContractVariantSpec(
            vertical_name="digital_marketing_agency",
            name="client_comms_first",
            title="Client Comms First",
            description="Bias toward artifact clarity and client-facing truthfulness when uncertainty remains.",
            objective_summary="Prefer accurate client artifacts and communication integrity over internal launch optimism.",
            rationale="Shows that the same launch world can be optimized around trust and communication quality.",
            policy_invariants=[
                PolicyInvariantSpec(
                    name="artifact_truthfulness",
                    description="Client-facing artifacts must stay aligned with live approval and pacing state.",
                    metadata={"origin": "simulated", "variant": "client_comms_first"},
                )
            ],
            reward_terms=[
                RewardTermSpec(
                    name="artifact_integrity_bias",
                    weight=2.0,
                    description="Reward keeping launch briefs and reports aligned before spend is released.",
                    metadata={"origin": "simulated", "variant": "client_comms_first"},
                )
            ],
        ),
    },
    "storage_solutions": {
        "no_overcommit": VerticalContractVariantSpec(
            vertical_name="storage_solutions",
            name="no_overcommit",
            title="No Overcommit",
            description="Default business contract: keep commitments feasible before they reach the customer.",
            objective_summary="Protect feasibility first; the customer should never receive an impossible capacity promise.",
            rationale="This is the default Atlas Storage Systems business objective.",
            policy_invariants=[
                PolicyInvariantSpec(
                    name="feasible_commitment_only",
                    description="Quotes and downstream ops state must stay feasible together before commitment is sent.",
                    metadata={"origin": "simulated", "variant": "no_overcommit"},
                )
            ],
            reward_terms=[
                RewardTermSpec(
                    name="feasibility_bias",
                    weight=2.0,
                    description="Reward capacity reservation and quote revision only when they remain feasible together.",
                    metadata={"origin": "simulated", "variant": "no_overcommit"},
                )
            ],
        ),
        "maximize_feasible_revenue": VerticalContractVariantSpec(
            vertical_name="storage_solutions",
            name="maximize_feasible_revenue",
            title="Maximize Feasible Revenue",
            description="Bias toward preserving as much revenue as possible while still staying feasible.",
            objective_summary="Preserve as much commitment value as possible without crossing into overcommit.",
            rationale="Demonstrates a more commercially aggressive objective over the same storage world.",
            policy_invariants=[
                PolicyInvariantSpec(
                    name="revenue_within_feasibility",
                    description="Increase commitment value only inside proven feasible capacity and dispatch coverage.",
                    metadata={
                        "origin": "simulated",
                        "variant": "maximize_feasible_revenue",
                    },
                )
            ],
            reward_terms=[
                RewardTermSpec(
                    name="revenue_preservation_bias",
                    weight=2.4,
                    description="Reward keeping feasible customer value high without breaking feasibility.",
                    metadata={
                        "origin": "simulated",
                        "variant": "maximize_feasible_revenue",
                    },
                )
            ],
        ),
        "ops_consistency": VerticalContractVariantSpec(
            vertical_name="storage_solutions",
            name="ops_consistency",
            title="Ops Consistency",
            description="Bias toward downstream operational consistency and dispatch certainty.",
            objective_summary="Prefer quotes that operations can execute cleanly, even if that means a more conservative commitment.",
            rationale="Useful when the same customer request should be solved with operations as the primary decision-maker.",
            policy_invariants=[
                PolicyInvariantSpec(
                    name="dispatch_consistency",
                    description="Vendor assignment, order state, and quote commitment should agree before the deal is advanced.",
                    metadata={"origin": "simulated", "variant": "ops_consistency"},
                )
            ],
            reward_terms=[
                RewardTermSpec(
                    name="ops_alignment_bias",
                    weight=2.2,
                    description="Reward dispatch certainty and rollout-plan coherence.",
                    metadata={"origin": "simulated", "variant": "ops_consistency"},
                )
            ],
        ),
    },
    "b2b_saas": {
        "save_the_renewal": VerticalContractVariantSpec(
            vertical_name="b2b_saas",
            name="save_the_renewal",
            title="Save the Renewal",
            description="Default business contract: retain the customer at any reasonable cost.",
            objective_summary="Prioritize retention: fix the product, rebuild trust, and get the renewal signed.",
            rationale="This is the default Pinnacle Analytics objective for the Apex renewal.",
            policy_invariants=[
                PolicyInvariantSpec(
                    name="renewal_retention",
                    description="Integration must be fixed and stakeholder trust rebuilt before the renewal conversation advances.",
                    metadata={"origin": "simulated", "variant": "save_the_renewal"},
                )
            ],
            reward_terms=[
                RewardTermSpec(
                    name="retention_bias",
                    weight=2.0,
                    description="Reward actions that move the customer from at-risk to renewed.",
                    metadata={"origin": "simulated", "variant": "save_the_renewal"},
                )
            ],
        ),
        "protect_revenue": VerticalContractVariantSpec(
            vertical_name="b2b_saas",
            name="protect_revenue",
            title="Protect Revenue",
            description="Bias toward maximizing contract value without losing the deal.",
            objective_summary="Preserve ARR and minimize discount depth while still closing the renewal.",
            rationale="Useful when the same renewal should be solved with a commercially aggressive posture.",
            policy_invariants=[
                PolicyInvariantSpec(
                    name="revenue_preservation",
                    description="Discount authorization should be minimized; prefer value demonstration over price concession.",
                    metadata={"origin": "simulated", "variant": "protect_revenue"},
                )
            ],
            reward_terms=[
                RewardTermSpec(
                    name="revenue_maximization_bias",
                    weight=2.5,
                    description="Reward renewals that preserve or grow ARR.",
                    metadata={"origin": "simulated", "variant": "protect_revenue"},
                )
            ],
        ),
        "customer_health_first": VerticalContractVariantSpec(
            vertical_name="b2b_saas",
            name="customer_health_first",
            title="Customer Health First",
            description="Bias toward product stability and support quality over commercial pressure.",
            objective_summary="Fix the product and support issues first; the commercial conversation follows trust.",
            rationale="Shows that the same renewal world can be optimized for long-term customer health.",
            policy_invariants=[
                PolicyInvariantSpec(
                    name="health_over_commerce",
                    description="Product fix and support resolution must precede any commercial conversation.",
                    metadata={
                        "origin": "simulated",
                        "variant": "customer_health_first",
                    },
                )
            ],
            reward_terms=[
                RewardTermSpec(
                    name="health_score_bias",
                    weight=2.2,
                    description="Reward actions that improve customer health score before advancing the deal.",
                    metadata={
                        "origin": "simulated",
                        "variant": "customer_health_first",
                    },
                )
            ],
        ),
    },
    "service_ops": {
        "protect_sla": VerticalContractVariantSpec(
            vertical_name="service_ops",
            name="protect_sla",
            title="Protect SLA",
            description="Default business contract: recover dispatch fast enough to protect the VIP service window.",
            objective_summary="Prioritize rapid, valid dispatch recovery so the service request reaches a credible field response on time.",
            rationale="This is the default Clearwater business objective and the clearest service-ops baseline.",
            policy_invariants=[
                PolicyInvariantSpec(
                    name="dispatch_before_breach",
                    description="The service loop should restore a valid technician assignment before the SLA window is lost.",
                    metadata={"origin": "simulated", "variant": "protect_sla"},
                )
            ],
            reward_terms=[
                RewardTermSpec(
                    name="dispatch_recovery_bias",
                    weight=2.3,
                    description="Reward clean dispatch recovery that restores the field response without improvising unsafe state.",
                    metadata={"origin": "simulated", "variant": "protect_sla"},
                )
            ],
        ),
        "protect_revenue": VerticalContractVariantSpec(
            vertical_name="service_ops",
            name="protect_revenue",
            title="Protect Revenue",
            description="Bias toward billing safety and account preservation when service and finance pressure collide.",
            objective_summary="Prefer actions that prevent billing damage and preserve the customer relationship while the field response unfolds.",
            rationale="Useful when the same service day should optimize for revenue preservation instead of raw dispatch speed.",
            policy_invariants=[
                PolicyInvariantSpec(
                    name="billing_safety_first",
                    description="Disputed accounts must not receive unsafe billing follow-through while the case is active.",
                    metadata={"origin": "simulated", "variant": "protect_revenue"},
                )
            ],
            reward_terms=[
                RewardTermSpec(
                    name="billing_hold_bias",
                    weight=2.1,
                    description="Reward holding or containing billing risk before it compounds customer churn pressure.",
                    metadata={"origin": "simulated", "variant": "protect_revenue"},
                )
            ],
        ),
        "protect_customer_trust": VerticalContractVariantSpec(
            vertical_name="service_ops",
            name="protect_customer_trust",
            title="Protect Customer Trust",
            description="Bias toward one coherent customer story across dispatch, billing, and escalation surfaces.",
            objective_summary="Keep the customer-facing truth clean: dispatch, billing, and manager escalation should all tell the same story.",
            rationale="Shows the same service loop optimized around trust, coordination, and low managerial thrash.",
            policy_invariants=[
                PolicyInvariantSpec(
                    name="single_customer_story",
                    description="Customer-facing updates and internal billing posture must remain aligned through the incident.",
                    metadata={
                        "origin": "simulated",
                        "variant": "protect_customer_trust",
                    },
                )
            ],
            reward_terms=[
                RewardTermSpec(
                    name="trust_preservation_bias",
                    weight=2.0,
                    description="Reward aligned dispatch, billing, and escalation trails that reduce customer confusion.",
                    metadata={
                        "origin": "simulated",
                        "variant": "protect_customer_trust",
                    },
                )
            ],
        ),
    },
}


__all__ = [
    "VerticalContractVariantSpec",
    "apply_vertical_contract_variant",
    "default_vertical_contract_variant",
    "get_vertical_contract_variant",
    "list_vertical_contract_variants",
]
