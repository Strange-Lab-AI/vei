"""Scenario catalog and helpers.

Scenarios are split across sub-modules by domain:
  _base       – foundational / default scenarios
  _identity   – identity & access management scenarios
  _verticals  – industry-vertical scenarios (real estate, growth, storage, B2B SaaS)
  _frontier   – progressive difficulty & frontier eval scenarios
  _generation – template-based scenario generation utilities
"""

from __future__ import annotations

import json
import logging
import os
import random
from copy import deepcopy
from pathlib import Path
from typing import Dict, Optional

from vei.world.scenario import Scenario

from ._base import (
    scenario_macrocompute_default,
    scenario_extended_store,
    scenario_multi_channel,
    scenario_multi_channel_compliance,
)
from ._identity import (
    scenario_identity_access,
    scenario_oauth_app_containment,
)
from ._verticals import (
    _b2b_saas_scenario,
    _service_ops_scenario,
    scenario_acquired_sales_onboarding,
    scenario_checkout_spike_mitigation,
    scenario_tenant_opening_conflict,
    scenario_campaign_launch_guardrail,
    scenario_capacity_quote_commitment,
    scenario_service_day_collision,
)
from ._frontier import (
    scenario_p0_easy,
    scenario_p1_moderate,
    scenario_p2_hard,
    scenario_pX_adversarial,
    scenario_f1_budget_reconciliation,
    scenario_f2_knowledge_qa,
    scenario_f3_vague_urgent_request,
    scenario_f4_contradictory_requirements,
    scenario_f5_vendor_comparison,
    scenario_f7_compliance_audit,
    scenario_f9_cascading_failure,
    scenario_f13_ethical_dilemma,
    scenario_f14_data_privacy,
)
from ._generation import generate_scenario

logger = logging.getLogger(__name__)

_CATALOG: Dict[str, Scenario] = {
    "macrocompute_default": scenario_macrocompute_default(),
    "extended_store": scenario_extended_store(),
    "multi_channel": scenario_multi_channel(),
    "multi_channel_compliance": scenario_multi_channel_compliance(),
    "identity_access": scenario_identity_access(),
    "oauth_app_containment": scenario_oauth_app_containment(),
    "acquired_sales_onboarding": scenario_acquired_sales_onboarding(),
    "checkout_spike_mitigation": scenario_checkout_spike_mitigation(),
    "tenant_opening_conflict": scenario_tenant_opening_conflict(),
    "campaign_launch_guardrail": scenario_campaign_launch_guardrail(),
    "capacity_quote_commitment": scenario_capacity_quote_commitment(),
    "service_day_collision": scenario_service_day_collision(),
    "technician_no_show": _service_ops_scenario(
        "technician_no_show",
        "Clearwater dispatch recovery mission is live.",
    ),
    "billing_dispute_reopened": _service_ops_scenario(
        "billing_dispute_reopened",
        "Clearwater finance containment review is live.",
    ),
    "enterprise_renewal_risk": _b2b_saas_scenario(
        "enterprise_renewal_risk",
        "Apex renewal war room is live.",
    ),
    "support_escalation_spiral": _b2b_saas_scenario(
        "support_escalation_spiral",
        "Apex P1 escalation review starts now.",
    ),
    "pricing_negotiation_deadlock": _b2b_saas_scenario(
        "pricing_negotiation_deadlock",
        "Apex pricing negotiation is stalled.",
    ),
    # Progressive difficulty
    "p0_easy": scenario_p0_easy(),
    "p1_moderate": scenario_p1_moderate(),
    "p2_hard": scenario_p2_hard(),
    "pX_adversarial": scenario_pX_adversarial(),
    # Frontier scenarios
    "f1_budget_reconciliation": scenario_f1_budget_reconciliation(),
    "f2_knowledge_qa": scenario_f2_knowledge_qa(),
    "f3_vague_urgent_request": scenario_f3_vague_urgent_request(),
    "f4_contradictory_requirements": scenario_f4_contradictory_requirements(),
    "f5_vendor_comparison": scenario_f5_vendor_comparison(),
    "f7_compliance_audit": scenario_f7_compliance_audit(),
    "f9_cascading_failure": scenario_f9_cascading_failure(),
    "f13_ethical_dilemma": scenario_f13_ethical_dilemma(),
    "f14_data_privacy": scenario_f14_data_privacy(),
}


def get_scenario(name: str) -> Scenario:
    key = name.strip().lower()
    if key in _CATALOG:
        scenario = deepcopy(_CATALOG[key])
        metadata = dict(scenario.metadata or {})
        metadata.setdefault("scenario_name", key)
        scenario.metadata = metadata
        return scenario
    raise KeyError(f"Unknown scenario: {name}")


def list_scenarios() -> Dict[str, Scenario]:
    return dict(_CATALOG)


def load_from_env(seed: Optional[int] = None) -> Scenario:
    """Load a Scenario based on environment variables.

    VEI_SCENARIO selects a named scenario from the catalog.
    VEI_SCENARIO_CONFIG provides a JSON string or path to a JSON file
    defining a parameter template for :func:`generate_scenario`.
    VEI_SCENARIO_RANDOM=1 randomly chooses a catalog scenario when none
    of the above are provided.
    """

    blueprint_asset_path = os.environ.get("VEI_BLUEPRINT_ASSET")
    if blueprint_asset_path:
        try:
            from vei.blueprint.api import materialize_scenario_from_blueprint
            from vei.blueprint.models import BlueprintAsset

            asset = BlueprintAsset.model_validate(
                json.loads(Path(blueprint_asset_path).read_text(encoding="utf-8"))
            )
            return materialize_scenario_from_blueprint(asset)
        except Exception:
            logger.warning(
                "Failed to materialise blueprint from %s, falling through",
                blueprint_asset_path,
                exc_info=True,
            )

    name = os.environ.get("VEI_SCENARIO")
    if name:
        try:
            return get_scenario(name)
        except KeyError:
            pass

    cfg = os.environ.get("VEI_SCENARIO_CONFIG")
    if cfg:
        try:
            if os.path.exists(cfg):
                with open(cfg, "r", encoding="utf-8") as f:
                    template = json.load(f)
            else:
                template = json.loads(cfg)
            return generate_scenario(template, seed=seed)
        except Exception:
            logger.warning(
                "Failed to generate scenario from VEI_SCENARIO_CONFIG=%s, returning default",
                cfg,
                exc_info=True,
            )
            return Scenario()

    if os.environ.get("VEI_SCENARIO_RANDOM", "0") == "1":
        rng = random.Random(seed)
        key = rng.choice(list(_CATALOG.keys()))
        return _CATALOG[key]

    return Scenario()


__all__ = [
    "Scenario",
    "generate_scenario",
    "get_scenario",
    "list_scenarios",
    "load_from_env",
]
