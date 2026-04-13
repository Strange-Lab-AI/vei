from __future__ import annotations

from typing import Sequence

from .corpus import ENRON_DOMAIN, has_external_recipients
from .models import (
    WhatIfEventReference,
    WhatIfCounterfactualEstimateResult,
    WhatIfLLMReplayResult,
    WhatIfObjectivePack,
    WhatIfObjectivePackId,
    WhatIfOutcomeScore,
    WhatIfOutcomeSignals,
)

_OBJECTIVE_PACKS: dict[WhatIfObjectivePackId, WhatIfObjectivePack] = {
    "contain_exposure": WhatIfObjectivePack(
        pack_id="contain_exposure",
        title="Contain Exposure",
        summary="Prefer branches that keep the thread internal and reduce leak risk.",
        weights={
            "exposure_control": 0.6,
            "speed": 0.15,
            "relationship_protection": 0.25,
        },
        evidence_labels=[
            "outside-addressed messages",
            "attachment references",
            "escalation language",
        ],
    ),
    "reduce_delay": WhatIfObjectivePack(
        pack_id="reduce_delay",
        title="Reduce Delay",
        summary="Prefer branches that move quickly without creating obvious exposure.",
        weights={
            "exposure_control": 0.2,
            "speed": 0.6,
            "relationship_protection": 0.2,
        },
        evidence_labels=[
            "average response delay",
            "follow-up count",
            "hold language",
        ],
    ),
    "protect_relationship": WhatIfObjectivePack(
        pack_id="protect_relationship",
        title="Protect Relationship",
        summary="Prefer branches that keep communication responsive and low-friction.",
        weights={
            "exposure_control": 0.2,
            "speed": 0.25,
            "relationship_protection": 0.55,
        },
        evidence_labels=[
            "reassurance language",
            "response speed",
            "communication load",
        ],
    ),
}

_REASSURANCE_TERMS = (
    "please",
    "thanks",
    "thank",
    "appreciate",
    "sorry",
    "review",
    "confirm",
    "update",
)
_HOLD_TERMS = ("hold", "pause", "wait", "defer", "until", "review")
_ATTACHMENT_TERMS = ("attach", "attachment", "draft", "term sheet")
_ESCALATION_TERMS = ("escalate", "leadership", "urgent", "executive")


def list_objective_packs() -> list[WhatIfObjectivePack]:
    return [pack.model_copy(deep=True) for pack in _OBJECTIVE_PACKS.values()]


def get_objective_pack(pack_id: str) -> WhatIfObjectivePack:
    normalized = pack_id.strip().lower()
    if normalized not in _OBJECTIVE_PACKS:
        raise KeyError(f"unknown objective pack: {pack_id}")
    return _OBJECTIVE_PACKS[normalized].model_copy(deep=True)


def summarize_llm_branch(
    *,
    branch_event: WhatIfEventReference,
    llm_result: WhatIfLLMReplayResult,
    organization_domain: str = ENRON_DOMAIN,
) -> WhatIfOutcomeSignals:
    if llm_result.status != "ok" or not llm_result.messages:
        return WhatIfOutcomeSignals(
            exposure_risk=1.0,
            delay_risk=1.0,
            relationship_protection=0.0,
            internal_only=True,
        )

    messages = list(llm_result.messages)
    message_count = len(messages)
    outside_message_count = sum(
        1
        for message in messages
        if _is_external_email(message.to, organization_domain=organization_domain)
    )
    avg_delay_ms = round(sum(message.delay_ms for message in messages) / message_count)
    hold_count = sum(
        1
        for message in messages
        if _contains_any(message.subject, _HOLD_TERMS)
        or _contains_any(message.body_text, _HOLD_TERMS)
    )
    reassurance_count = sum(
        1
        for message in messages
        if _contains_any(message.subject, _REASSURANCE_TERMS)
        or _contains_any(message.body_text, _REASSURANCE_TERMS)
    )
    attachment_mentions = sum(
        1
        for message in messages
        if _contains_any(message.subject, _ATTACHMENT_TERMS)
        or _contains_any(message.body_text, _ATTACHMENT_TERMS)
    )
    escalation_mentions = sum(
        1
        for message in messages
        if _contains_any(message.subject, _ESCALATION_TERMS)
        or _contains_any(message.body_text, _ESCALATION_TERMS)
    )

    outside_ratio = outside_message_count / message_count
    attachment_ratio = attachment_mentions / message_count
    escalation_ratio = escalation_mentions / message_count
    hold_ratio = hold_count / message_count

    exposure_risk = _clamp(
        (outside_ratio * 0.7) + (attachment_ratio * 0.2) + (escalation_ratio * 0.1)
    )
    delay_risk = _clamp(
        (_delay_norm(avg_delay_ms) * 0.55)
        + (_message_count_norm(message_count) * 0.25)
        + (hold_ratio * 0.2)
    )

    responsiveness = 1.0 - delay_risk
    tone_score = _clamp(reassurance_count / max(message_count, 2))
    brevity_score = 1.0 - (_message_count_norm(message_count) * 0.5)
    relationship_protection = _clamp(
        (tone_score * 0.4) + (responsiveness * 0.35) + (brevity_score * 0.25)
    )

    if (
        has_external_recipients(
            branch_event.to_recipients,
            organization_domain=organization_domain,
        )
        and outside_message_count == 0
    ):
        relationship_protection = _clamp(relationship_protection - 0.15)

    return WhatIfOutcomeSignals(
        exposure_risk=round(exposure_risk, 3),
        delay_risk=round(delay_risk, 3),
        relationship_protection=round(relationship_protection, 3),
        message_count=message_count,
        outside_message_count=outside_message_count,
        avg_delay_ms=avg_delay_ms,
        internal_only=outside_message_count == 0,
        reassurance_count=reassurance_count,
        hold_count=hold_count,
    )


def summarize_forecast_branch(
    forecast_result: WhatIfCounterfactualEstimateResult,
) -> WhatIfOutcomeSignals:
    predicted = forecast_result.predicted
    baseline = forecast_result.baseline
    future_events = max(predicted.future_event_count, 1)
    baseline_events = max(baseline.future_event_count, 1)

    exposure_risk = _clamp(
        (predicted.risk_score * 0.55)
        + ((predicted.future_external_event_count / future_events) * 0.45)
    )
    delay_risk = _clamp(
        (min(predicted.future_event_count / baseline_events, 1.0) * 0.5)
        + (
            min(
                (
                    predicted.future_assignment_count
                    + predicted.future_approval_count
                    + predicted.future_escalation_count
                )
                / future_events,
                1.0,
            )
            * 0.5
        )
    )
    relationship_protection = _clamp(
        1.0 - ((exposure_risk * 0.55) + (delay_risk * 0.45))
    )
    return WhatIfOutcomeSignals(
        exposure_risk=round(exposure_risk, 3),
        delay_risk=round(delay_risk, 3),
        relationship_protection=round(relationship_protection, 3),
        message_count=predicted.future_event_count,
        outside_message_count=predicted.future_external_event_count,
        avg_delay_ms=0,
        internal_only=predicted.future_external_event_count == 0,
    )


def aggregate_outcome_signals(
    outcomes: Sequence[WhatIfOutcomeSignals],
) -> WhatIfOutcomeSignals:
    if not outcomes:
        return WhatIfOutcomeSignals(
            exposure_risk=1.0,
            delay_risk=1.0,
            relationship_protection=0.0,
            internal_only=True,
        )

    count = len(outcomes)
    return WhatIfOutcomeSignals(
        exposure_risk=round(sum(item.exposure_risk for item in outcomes) / count, 3),
        delay_risk=round(sum(item.delay_risk for item in outcomes) / count, 3),
        relationship_protection=round(
            sum(item.relationship_protection for item in outcomes) / count,
            3,
        ),
        message_count=round(sum(item.message_count for item in outcomes) / count),
        outside_message_count=round(
            sum(item.outside_message_count for item in outcomes) / count
        ),
        avg_delay_ms=round(sum(item.avg_delay_ms for item in outcomes) / count),
        internal_only=all(item.internal_only for item in outcomes),
        reassurance_count=round(
            sum(item.reassurance_count for item in outcomes) / count
        ),
        hold_count=round(sum(item.hold_count for item in outcomes) / count),
    )


def score_outcome_signals(
    *,
    pack: WhatIfObjectivePack,
    outcome: WhatIfOutcomeSignals,
) -> WhatIfOutcomeScore:
    components = {
        "exposure_control": round(1.0 - outcome.exposure_risk, 3),
        "speed": round(1.0 - outcome.delay_risk, 3),
        "relationship_protection": round(outcome.relationship_protection, 3),
    }
    total = 0.0
    for key, weight in pack.weights.items():
        total += components.get(key, 0.0) * float(weight)

    evidence = _build_evidence(pack=pack, outcome=outcome, components=components)
    return WhatIfOutcomeScore(
        objective_pack_id=pack.pack_id,
        overall_score=round(total, 3),
        components=components,
        evidence=evidence,
    )


def recommendation_reason(
    *,
    pack: WhatIfObjectivePack,
    outcome: WhatIfOutcomeSignals,
    score: WhatIfOutcomeScore,
    rollout_count: int,
) -> str:
    if pack.pack_id == "contain_exposure":
        internal_text = (
            "keeps the thread internal"
            if outcome.internal_only
            else "still sends messages outside"
        )
        return (
            f"Best for {pack.title.lower()} because it {internal_text} across "
            f"{rollout_count} rollout{'s' if rollout_count != 1 else ''} and has the lowest exposure score."
        )
    if pack.pack_id == "reduce_delay":
        return (
            f"Best for {pack.title.lower()} because it shows the fastest average response "
            f"pattern and the strongest speed score."
        )
    return (
        f"Best for {pack.title.lower()} because it balances response speed, lighter "
        f"communication load, and the strongest relationship score."
    )


def sort_candidates_for_rank(
    items: Sequence[tuple[str, WhatIfOutcomeSignals, WhatIfOutcomeScore]],
) -> list[str]:
    ranked = sorted(
        items,
        key=lambda item: (
            -item[2].overall_score,
            item[1].exposure_risk,
            item[1].delay_risk,
            item[0].lower(),
        ),
    )
    return [label for label, _, _ in ranked]


def _build_evidence(
    *,
    pack: WhatIfObjectivePack,
    outcome: WhatIfOutcomeSignals,
    components: dict[str, float],
) -> list[str]:
    lines = [
        f"Exposure control {components['exposure_control']:.3f}",
        f"Speed {components['speed']:.3f}",
        f"Relationship protection {components['relationship_protection']:.3f}",
    ]
    if outcome.internal_only:
        lines.append("All simulated messages stayed inside the company.")
    elif outcome.outside_message_count:
        lines.append(
            f"{outcome.outside_message_count} simulated message"
            f"{'' if outcome.outside_message_count == 1 else 's'} still went outside the company."
        )
    if pack.pack_id == "reduce_delay":
        lines.append(f"Average response delay {outcome.avg_delay_ms} ms.")
    if pack.pack_id == "protect_relationship":
        lines.append(
            f"Reassurance language appeared {outcome.reassurance_count} time"
            f"{'' if outcome.reassurance_count == 1 else 's'}."
        )
    return lines


def _contains_any(text: str, terms: Sequence[str]) -> bool:
    lowered = text.lower()
    return any(term in lowered for term in terms)


def _delay_norm(delay_ms: int) -> float:
    return _clamp(delay_ms / 14_400_000)


def _message_count_norm(message_count: int) -> float:
    if message_count <= 1:
        return 0.0
    return _clamp((message_count - 1) / 3)


def _is_external_email(value: str, *, organization_domain: str = ENRON_DOMAIN) -> bool:
    email = value.strip().lower()
    if not email:
        return False
    normalized_domain = organization_domain.strip().lower()
    if not normalized_domain:
        return "@" in email
    return not email.endswith(f"@{normalized_domain}")


def _clamp(value: float) -> float:
    return max(0.0, min(1.0, float(value)))
