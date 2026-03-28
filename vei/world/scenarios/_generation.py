from __future__ import annotations

import random
from typing import Any, Dict, List, Optional

from vei.world.scenario import Scenario


def _rand_from_range(rng: random.Random, val: Any) -> Any:
    if isinstance(val, list) and len(val) == 2:
        lo, hi = val
        if isinstance(lo, int) and isinstance(hi, int):
            return rng.randint(lo, hi)
        try:
            return rng.uniform(float(lo), float(hi))
        except Exception:
            return val
    return val


def generate_scenario(template: Dict[str, Any], seed: Optional[int] = None) -> Scenario:
    """Generate a Scenario from a parameter template.

    The template may define:
      - budget_cap_usd, derail_prob, slack_initial_message
      - vendors: list of {name, price: [lo,hi], eta_days: [lo,hi]}
      - browser_nodes
      - derail_events: list of {dt_ms, target, payload}
    Prices/ETAs are randomized within ranges using the given seed.
    """

    rng = random.Random(seed)
    variants: List[str] = []
    for v in template.get("vendors", []):
        name = v.get("name", "Vendor")
        price = _rand_from_range(rng, v.get("price"))
        eta = _rand_from_range(rng, v.get("eta_days"))
        variants.append(f"{name} quote: ${int(price)}, ETA: {int(eta)} days.")

    return Scenario(
        budget_cap_usd=template.get("budget_cap_usd"),
        derail_prob=template.get("derail_prob"),
        slack_initial_message=template.get("slack_initial_message"),
        vendor_reply_variants=variants or None,
        browser_nodes=template.get("browser_nodes"),
        derail_events=template.get("derail_events"),
        database_tables=template.get("database_tables"),
    )
