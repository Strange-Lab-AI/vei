from __future__ import annotations

from typing import Final

from .corpus import branch_has_external_sharing
from .models import (
    WhatIfBusinessConsequenceEstimate,
    WhatIfBusinessImpactEffect,
    WhatIfBusinessImpactMagnitude,
    WhatIfBusinessStateAssessment,
    WhatIfBusinessStateChange,
    WhatIfBusinessStateImpact,
    WhatIfBusinessStateIndicator,
    WhatIfBusinessStateLevel,
    WhatIfBusinessStateSnapshot,
    WhatIfEventReference,
    WhatIfForecast,
    WhatIfForecastResult,
    WhatIfPublicContext,
)

_NEGATIVE_STATES: Final[set[str]] = {
    "exposure",
    "coordination_load",
    "execution_delay",
    "governance_pressure",
}
_STATE_WEIGHTS: Final[dict[str, float]] = {
    "exposure": 0.30,
    "deal_position": 0.20,
    "trust": 0.20,
    "execution_delay": 0.15,
    "coordination_load": 0.10,
    "governance_pressure": 0.05,
}
_STATE_ORDER: Final[tuple[str, ...]] = (
    "exposure",
    "deal_position",
    "trust",
    "execution_delay",
    "coordination_load",
    "governance_pressure",
)


def assess_historical_business_state(
    *,
    branch_event: WhatIfEventReference,
    forecast: WhatIfForecast,
    organization_domain: str,
    public_context: WhatIfPublicContext | None,
) -> WhatIfBusinessStateAssessment:
    snapshot = business_state_snapshot_from_forecast(
        branch_event=branch_event,
        forecast=forecast,
        organization_domain=organization_domain,
        public_context=public_context,
    )
    indicators = _build_indicators(
        snapshot=snapshot,
        branch_event=branch_event,
        organization_domain=organization_domain,
    )
    implications = [indicator.summary for indicator in indicators[:3]]
    summary = _historical_summary(indicators)
    return WhatIfBusinessStateAssessment(
        method="historical_v1",
        confidence="medium",
        summary=summary,
        snapshot=snapshot,
        indicators=indicators,
        implications=implications,
    )


def describe_forecast_business_change(
    *,
    branch_event: WhatIfEventReference,
    forecast_result: WhatIfForecastResult,
    organization_domain: str,
    public_context: WhatIfPublicContext | None,
) -> WhatIfBusinessStateChange:
    baseline = business_state_snapshot_from_forecast(
        branch_event=branch_event,
        forecast=forecast_result.baseline,
        organization_domain=organization_domain,
        public_context=public_context,
    )
    predicted = business_state_snapshot_from_forecast(
        branch_event=branch_event,
        forecast=forecast_result.predicted,
        organization_domain=organization_domain,
        public_context=public_context,
    )
    impacts = _build_impacts(
        baseline=baseline,
        predicted=predicted,
        branch_event=branch_event,
        organization_domain=organization_domain,
    )
    consequence_estimates = _build_consequence_estimates(
        impacts=impacts,
        baseline=baseline,
        predicted=predicted,
    )
    tradeoffs = [impact.summary for impact in impacts if impact.effect == "worse"][:2]
    positives = [impact.summary for impact in impacts if impact.effect == "better"]
    if positives and tradeoffs:
        summary = f"{positives[0]} Trade-off: {tradeoffs[0]}"
    elif positives:
        summary = positives[0]
    elif tradeoffs:
        summary = tradeoffs[0]
    else:
        summary = "This move stays close to the historical business path."
    return WhatIfBusinessStateChange(
        method=f"{forecast_result.backend}_business_state_v1",
        confidence="high" if forecast_result.backend == "e_jepa" else "medium",
        summary=summary,
        baseline=baseline,
        predicted=predicted,
        impacts=impacts,
        consequence_estimates=consequence_estimates,
        tradeoffs=tradeoffs,
        net_effect_score=round(_net_effect_score(impacts), 3),
    )


def business_state_snapshot_from_forecast(
    *,
    branch_event: WhatIfEventReference,
    forecast: WhatIfForecast,
    organization_domain: str,
    public_context: WhatIfPublicContext | None,
) -> WhatIfBusinessStateSnapshot:
    future_events = max(forecast.future_event_count, 1)
    external_ratio = min(forecast.future_external_event_count / future_events, 1.0)
    escalation_ratio = min(forecast.future_escalation_count / future_events, 1.0)
    assignment_ratio = min(forecast.future_assignment_count / future_events, 1.0)
    approval_ratio = min(forecast.future_approval_count / future_events, 1.0)
    event_load = _clamp(forecast.future_event_count / 96.0)
    branch_external = (
        1.0 if _branch_has_external_sharing(branch_event, organization_domain) else 0.0
    )
    attachment_risk = 1.0 if branch_event.has_attachment_reference else 0.0
    public_pressure = _public_pressure(public_context)

    exposure = _clamp(
        (forecast.risk_score * 0.55)
        + (external_ratio * 0.25)
        + (branch_external * 0.10)
        + (attachment_risk * 0.10)
    )
    governance_pressure = _clamp(
        (escalation_ratio * 0.45)
        + (approval_ratio * 0.25)
        + (public_pressure * 0.20)
        + ((1.0 if branch_event.is_escalation else 0.0) * 0.10)
    )
    coordination_load = _clamp(
        (assignment_ratio * 0.45)
        + (approval_ratio * 0.20)
        + (event_load * 0.20)
        + (governance_pressure * 0.15)
    )
    execution_delay = _clamp(
        (event_load * 0.45)
        + (assignment_ratio * 0.30)
        + (approval_ratio * 0.15)
        + (governance_pressure * 0.10)
    )
    trust = _clamp(
        1.0
        - (
            (exposure * 0.45)
            + (execution_delay * 0.25)
            + (governance_pressure * 0.15)
            + (coordination_load * 0.15)
        )
    )
    deal_position = _clamp(
        (trust * 0.45) + ((1.0 - execution_delay) * 0.30) + ((1.0 - exposure) * 0.25)
    )
    return WhatIfBusinessStateSnapshot(
        exposure=round(exposure, 3),
        trust=round(trust, 3),
        coordination_load=round(coordination_load, 3),
        execution_delay=round(execution_delay, 3),
        deal_position=round(deal_position, 3),
        governance_pressure=round(governance_pressure, 3),
    )


def _build_indicators(
    *,
    snapshot: WhatIfBusinessStateSnapshot,
    branch_event: WhatIfEventReference,
    organization_domain: str,
) -> list[WhatIfBusinessStateIndicator]:
    indicators: list[WhatIfBusinessStateIndicator] = []
    for state_id in _STATE_ORDER:
        value = getattr(snapshot, state_id)
        label = _state_label(
            state_id=state_id,
            branch_event=branch_event,
            organization_domain=organization_domain,
        )
        level = _level_for_value(value)
        indicators.append(
            WhatIfBusinessStateIndicator(
                state_id=state_id,
                label=label,
                value=round(value, 3),
                level=level,
                summary=_indicator_summary(
                    state_id=state_id,
                    label=label,
                    level=level,
                ),
            )
        )
    indicators.sort(
        key=lambda item: (
            -_indicator_priority(item.state_id, item.value),
            _STATE_ORDER.index(item.state_id),
        )
    )
    return indicators


def _build_impacts(
    *,
    baseline: WhatIfBusinessStateSnapshot,
    predicted: WhatIfBusinessStateSnapshot,
    branch_event: WhatIfEventReference,
    organization_domain: str,
) -> list[WhatIfBusinessStateImpact]:
    impacts: list[WhatIfBusinessStateImpact] = []
    for state_id in _STATE_ORDER:
        baseline_value = getattr(baseline, state_id)
        predicted_value = getattr(predicted, state_id)
        delta = round(predicted_value - baseline_value, 3)
        oriented_delta = _oriented_delta(state_id=state_id, delta=delta)
        effect, magnitude = _effect_and_magnitude(oriented_delta)
        label = _state_label(
            state_id=state_id,
            branch_event=branch_event,
            organization_domain=organization_domain,
        )
        impacts.append(
            WhatIfBusinessStateImpact(
                state_id=state_id,
                label=label,
                baseline_value=round(baseline_value, 3),
                predicted_value=round(predicted_value, 3),
                delta=delta,
                effect=effect,
                magnitude=magnitude,
                summary=_impact_summary(
                    state_id=state_id,
                    label=label,
                    effect=effect,
                    magnitude=magnitude,
                ),
            )
        )
    impacts.sort(
        key=lambda item: (
            -abs(_oriented_delta(state_id=item.state_id, delta=item.delta)),
            _STATE_ORDER.index(item.state_id),
        )
    )
    return impacts


def _build_consequence_estimates(
    *,
    impacts: list[WhatIfBusinessStateImpact],
    baseline: WhatIfBusinessStateSnapshot,
    predicted: WhatIfBusinessStateSnapshot,
) -> list[WhatIfBusinessConsequenceEstimate]:
    impact_by_id = {impact.state_id: impact for impact in impacts}
    review_delta = round(
        (
            _oriented_delta(
                state_id="coordination_load",
                delta=predicted.coordination_load - baseline.coordination_load,
            )
            * 0.6
        )
        + (
            _oriented_delta(
                state_id="governance_pressure",
                delta=predicted.governance_pressure - baseline.governance_pressure,
            )
            * 0.4
        ),
        3,
    )
    review_effect, review_magnitude = _effect_and_magnitude(review_delta)
    execution_impact = impact_by_id["execution_delay"]
    exposure_impact = impact_by_id["exposure"]
    deal_impact = impact_by_id["deal_position"]

    return [
        WhatIfBusinessConsequenceEstimate(
            consequence_id="containment",
            label="Containment",
            effect=exposure_impact.effect,
            magnitude=exposure_impact.magnitude,
            summary=_consequence_summary(
                label="Containment",
                effect=exposure_impact.effect,
                magnitude=exposure_impact.magnitude,
                positive_text=(
                    "The thread looks much safer to contain."
                    if exposure_impact.magnitude == "strong"
                    else "The thread looks safer to contain."
                ),
                negative_text=(
                    "The thread looks easier to leak or widen."
                    if exposure_impact.magnitude == "strong"
                    else "The thread carries slightly more exposure."
                ),
                flat_text="Containment stays close to the historical path.",
            ),
        ),
        WhatIfBusinessConsequenceEstimate(
            consequence_id="review_burden",
            label="Handling burden",
            effect=review_effect,
            magnitude=review_magnitude,
            summary=_consequence_summary(
                label="Handling burden",
                effect=review_effect,
                magnitude=review_magnitude,
                positive_text=(
                    "Internal handling looks lighter."
                    if review_magnitude != "strong"
                    else "Internal handling looks much lighter."
                ),
                negative_text=(
                    "Internal handling looks heavier."
                    if review_magnitude != "strong"
                    else "Internal handling looks much heavier."
                ),
                flat_text="Handling burden stays close to the historical path.",
            ),
        ),
        WhatIfBusinessConsequenceEstimate(
            consequence_id="execution_path",
            label="Execution path",
            effect=execution_impact.effect,
            magnitude=execution_impact.magnitude,
            summary=_consequence_summary(
                label="Execution path",
                effect=execution_impact.effect,
                magnitude=execution_impact.magnitude,
                positive_text="Near-term execution looks faster.",
                negative_text="Near-term execution looks slower.",
                flat_text="Execution pace stays close to the historical path.",
            ),
        ),
        WhatIfBusinessConsequenceEstimate(
            consequence_id="commercial_position",
            label="Commercial footing",
            effect=deal_impact.effect,
            magnitude=deal_impact.magnitude,
            summary=_consequence_summary(
                label="Commercial footing",
                effect=deal_impact.effect,
                magnitude=deal_impact.magnitude,
                positive_text="Commercial footing looks stronger.",
                negative_text="Commercial footing looks weaker.",
                flat_text="Commercial footing stays close to the historical path.",
            ),
        ),
    ]


_branch_has_external_sharing = branch_has_external_sharing


def _public_pressure(context: WhatIfPublicContext | None) -> float:
    if context is None:
        return 0.0
    item_count = len(context.financial_snapshots) + len(context.public_news_events)
    if item_count <= 0:
        return 0.0
    return _clamp(item_count / 12.0)


def _state_label(
    *,
    state_id: str,
    branch_event: WhatIfEventReference,
    organization_domain: str,
) -> str:
    if state_id == "exposure":
        if branch_event.has_attachment_reference or _branch_has_external_sharing(
            branch_event,
            organization_domain,
        ):
            return "Outside spread risk"
        return "Exposure risk"
    if state_id == "trust":
        return "Relationship stability"
    if state_id == "coordination_load":
        return "Internal handling load"
    if state_id == "execution_delay":
        return "Execution delay"
    if state_id == "deal_position":
        return "Commercial position"
    return "Approval and escalation pressure"


def _indicator_summary(
    *,
    state_id: str,
    label: str,
    level: WhatIfBusinessStateLevel,
) -> str:
    if state_id in _NEGATIVE_STATES:
        if level in {"very_high", "high"}:
            return f"{label} is high in the recorded path."
        if level == "medium":
            return f"{label} is moderate in the recorded path."
        return f"{label} stays limited in the recorded path."
    if level in {"very_high", "high"}:
        return f"{label} looks strong in the recorded path."
    if level == "medium":
        return f"{label} stays mixed in the recorded path."
    return f"{label} looks fragile in the recorded path."


def _historical_summary(
    indicators: list[WhatIfBusinessStateIndicator],
) -> str:
    if not indicators:
        return "Recorded business effects stay close to neutral."
    labels = [indicator.label.lower() for indicator in indicators[:2]]
    if len(labels) == 1:
        return f"Recorded business state centers on {labels[0]}."
    return f"Recorded business state centers on {labels[0]} and {labels[1]}."


def _indicator_priority(state_id: str, value: float) -> float:
    if state_id in _NEGATIVE_STATES:
        return value
    return 1.0 - value


def _impact_summary(
    *,
    state_id: str,
    label: str,
    effect: WhatIfBusinessImpactEffect,
    magnitude: WhatIfBusinessImpactMagnitude,
) -> str:
    if effect == "flat":
        return f"{label} stays close to the historical path."
    if effect == "better":
        if state_id == "exposure":
            return _magnitude_prefix(magnitude, "lower") + f" {label.lower()}."
        if state_id == "trust":
            return _magnitude_prefix(magnitude, "stronger") + f" {label.lower()}."
        if state_id == "deal_position":
            return _magnitude_prefix(magnitude, "stronger") + f" {label.lower()}."
        return _magnitude_prefix(magnitude, "lower") + f" {label.lower()}."
    if state_id in {"trust", "deal_position"}:
        return _magnitude_prefix(magnitude, "weaker") + f" {label.lower()}."
    return _magnitude_prefix(magnitude, "higher") + f" {label.lower()}."


def _consequence_summary(
    *,
    label: str,
    effect: WhatIfBusinessImpactEffect,
    magnitude: WhatIfBusinessImpactMagnitude,
    positive_text: str,
    negative_text: str,
    flat_text: str,
) -> str:
    if effect == "better":
        return positive_text
    if effect == "worse":
        return negative_text
    return flat_text


def _magnitude_prefix(magnitude: WhatIfBusinessImpactMagnitude, direction: str) -> str:
    if magnitude == "strong":
        return f"Much {direction}"
    if magnitude == "moderate":
        return f"Moderately {direction}"
    return f"Slightly {direction}"


def _effect_and_magnitude(
    oriented_delta: float,
) -> tuple[WhatIfBusinessImpactEffect, WhatIfBusinessImpactMagnitude]:
    absolute = abs(oriented_delta)
    if absolute < 0.03:
        return "flat", "flat"
    if absolute < 0.08:
        magnitude: WhatIfBusinessImpactMagnitude = "slight"
    elif absolute < 0.15:
        magnitude = "moderate"
    else:
        magnitude = "strong"
    effect: WhatIfBusinessImpactEffect = "better" if oriented_delta > 0 else "worse"
    return effect, magnitude


def _oriented_delta(*, state_id: str, delta: float) -> float:
    if state_id in _NEGATIVE_STATES:
        return -delta
    return delta


def _net_effect_score(impacts: list[WhatIfBusinessStateImpact]) -> float:
    total = 0.0
    for impact in impacts:
        total += _STATE_WEIGHTS.get(impact.state_id, 0.0) * _oriented_delta(
            state_id=impact.state_id,
            delta=impact.delta,
        )
    return total


def _level_for_value(value: float) -> WhatIfBusinessStateLevel:
    if value < 0.2:
        return "very_low"
    if value < 0.4:
        return "low"
    if value < 0.6:
        return "medium"
    if value < 0.8:
        return "high"
    return "very_high"


def _clamp(value: float) -> float:
    return max(0.0, min(1.0, value))
