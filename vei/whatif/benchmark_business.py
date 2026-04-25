from __future__ import annotations

from collections import Counter
from statistics import mean
from typing import Sequence

from .corpus import ENRON_DOMAIN, EXECUTIVE_MARKERS
from .models import (
    WhatIfBusinessObjectivePack,
    WhatIfBusinessObjectivePackId,
    WhatIfBusinessObjectiveScore,
    WhatIfBusinessOutcomeHeads,
    WhatIfEvent,
    WhatIfFutureStateHeads,
    WhatIfJudgeRubric,
    WhatIfObservedEvidenceHeads,
)

_NEGATIVE_HEADS = {
    "enterprise_risk",
    "org_strain_proxy",
    "execution_drag",
}
_REASSURANCE_TERMS = (
    "please",
    "thanks",
    "thank you",
    "appreciate",
    "confirm",
    "update",
    "review",
    "happy to",
)
_APOLOGY_TERMS = ("sorry", "apolog", "regret", "repair", "make this right")
_COMMITMENT_TERMS = (
    "i will",
    "we will",
    "by eod",
    "next step",
    "plan",
    "timeline",
    "owner",
    "i'll",
)
_BLAME_TERMS = (
    "missed",
    "failure",
    "fault",
    "blame",
    "problem",
    "issue",
    "concern",
    "late",
    "delay",
)
_DISAGREEMENT_TERMS = (
    "disagree",
    "do not agree",
    "not comfortable",
    "cannot approve",
    "object",
    "concerned",
    "push back",
)
_REVIEW_TERMS = (
    "review",
    "comments",
    "comment",
    "draft",
    "approve",
    "approval",
    "redline",
    "markup",
)
_MARKUP_TERMS = (
    "markup",
    "mark up",
    "mark-up",
    "redline",
    "clean draft",
    "revised draft",
    "updated draft",
)
_URGENCY_TERMS = ("urgent", "asap", "immediately", "today", "tonight")
_VERSION_TERMS = (
    "version",
    "v2",
    "v3",
    "revised",
    "updated",
    "clean draft",
    "new draft",
)
_CROSS_FUNCTIONAL_MARKERS = (
    "legal",
    "trading",
    "credit",
    "risk",
    "regulatory",
    "hr",
    "business",
    "commercial",
)


def list_business_objective_packs() -> list[WhatIfBusinessObjectivePack]:
    return [pack.model_copy(deep=True) for pack in _PACKS.values()]


def get_business_objective_pack(
    pack_id: WhatIfBusinessObjectivePackId | str,
) -> WhatIfBusinessObjectivePack:
    normalized = str(pack_id).strip().lower()
    if normalized not in _PACKS:
        raise KeyError(f"unknown business objective pack: {pack_id}")
    return _PACKS[normalized].model_copy(deep=True)


def get_business_judge_rubric(
    pack_id: WhatIfBusinessObjectivePackId | str,
) -> WhatIfJudgeRubric:
    pack = get_business_objective_pack(pack_id)
    return _RUBRICS[pack.pack_id].model_copy(deep=True)


def summarize_observed_evidence(
    *,
    branch_event: WhatIfEvent,
    future_events: Sequence[WhatIfEvent],
) -> WhatIfObservedEvidenceHeads:
    if not future_events:
        return WhatIfObservedEvidenceHeads()

    external_events = 0
    outside_recipients = 0
    outside_attachment_spread = 0
    legal_follow_ups = 0
    review_loops = 0
    markup_loops = 0
    executive_escalations = 0
    executive_mentions = 0
    urgency_spikes = 0
    cc_expansions = 0
    cross_functional_loops = 0
    reassurance_count = 0
    apology_repair_count = 0
    commitment_clarity_count = 0
    blame_pressure_count = 0
    internal_disagreement_count = 0
    attachment_recirculation_count = 0
    version_turn_count = 0
    participant_ids: set[str] = set()
    review_delays: list[int] = []
    start_ms = branch_event.timestamp_ms

    for event in future_events:
        text = _event_text(event)
        external_count = _event_external_count(event)
        participant_ids.update(_event_participants(event))
        outside_recipients += external_count
        if external_count > 0:
            external_events += 1
            if event.flags.has_attachment_reference:
                outside_attachment_spread += 1
        if (
            _has_any(text, ("legal", "counsel", "paralegal"))
            or event.flags.consult_legal_specialist
        ):
            legal_follow_ups += 1
        if _has_any(text, _REVIEW_TERMS):
            review_loops += 1
            review_delays.append(max(0, event.timestamp_ms - start_ms))
        if _has_any(text, _MARKUP_TERMS):
            markup_loops += 1
        if _has_any(
            text,
            EXECUTIVE_MARKERS
            + ("executive", "leadership", "ken.skilling", "kenneth.lay"),
        ):
            executive_mentions += 1
            if event.flags.is_escalation or event.event_type == "escalation":
                executive_escalations += 1
        if _has_any(text, _URGENCY_TERMS):
            urgency_spikes += 1
        if int(event.flags.cc_count) >= 2 or len(event.flags.cc_recipients) >= 2:
            cc_expansions += 1
        if _cross_functional_marker_count(text) >= 2:
            cross_functional_loops += 1
        if _has_any(text, _REASSURANCE_TERMS):
            reassurance_count += 1
        if _has_any(text, _APOLOGY_TERMS):
            apology_repair_count += 1
        if _has_any(text, _COMMITMENT_TERMS):
            commitment_clarity_count += 1
        if _has_any(text, _BLAME_TERMS):
            blame_pressure_count += 1
        if _has_any(text, _DISAGREEMENT_TERMS):
            internal_disagreement_count += 1
        if event.flags.has_attachment_reference:
            attachment_recirculation_count += 1
        if _has_any(text, _VERSION_TERMS):
            version_turn_count += 1

    delays = [max(0, event.timestamp_ms - start_ms) for event in future_events]
    return WhatIfObservedEvidenceHeads(
        any_external_spread=outside_recipients > 0,
        outside_recipient_count=outside_recipients,
        outside_forward_count=external_events,
        outside_attachment_spread_count=outside_attachment_spread,
        legal_follow_up_count=legal_follow_ups,
        review_loop_count=review_loops,
        markup_loop_count=markup_loops,
        executive_escalation_count=executive_escalations,
        executive_mention_count=executive_mentions,
        urgency_spike_count=urgency_spikes,
        participant_fanout=len(participant_ids),
        cc_expansion_count=cc_expansions,
        cross_functional_loop_count=cross_functional_loops,
        time_to_first_follow_up_ms=delays[0] if delays else 0,
        time_to_thread_end_ms=max(0, future_events[-1].timestamp_ms - start_ms),
        review_delay_burden_ms=round(mean(review_delays)) if review_delays else 0,
        reassurance_count=reassurance_count,
        apology_repair_count=apology_repair_count,
        commitment_clarity_count=commitment_clarity_count,
        blame_pressure_count=blame_pressure_count,
        internal_disagreement_count=internal_disagreement_count,
        attachment_recirculation_count=attachment_recirculation_count,
        version_turn_count=version_turn_count,
    )


def summarize_future_state_heads(
    *,
    future_events: Sequence[WhatIfEvent],
    evidence: WhatIfObservedEvidenceHeads,
) -> WhatIfFutureStateHeads:
    """Summarize the recorded future tail into broad state targets.

    These are learned outcome labels, not inference-time rules. They let the
    world model learn that some futures are regulatory/accounting/liquidity
    regimes where review and governance activity can be protective rather than
    mere execution drag.
    """

    if not future_events:
        return WhatIfFutureStateHeads()

    future_text = " ".join(_event_text(event) for event in future_events)
    regulatory_hits = _keyword_count(
        future_text,
        (
            "sec",
            "regulator",
            "regulatory",
            "investigation",
            "formal investigation",
            "subpoena",
            "consent",
            "permission",
            "privacy",
            "compliance",
        ),
    )
    accounting_hits = _keyword_count(
        future_text,
        (
            "accounting",
            "audit",
            "auditor",
            "controller",
            "ledger",
            "reconciliation",
            "restatement",
            "q3",
            "third quarter",
            "non-recurring",
            "charge",
            "trial balance",
        ),
    )
    liquidity_hits = _keyword_count(
        future_text,
        (
            "liquidity",
            "credit",
            "downgrade",
            "moody",
            "fitch",
            "debt",
            "cash",
            "treasury",
            "bankruptcy",
            "stock",
            "runway",
            "fundraising",
            "investor",
        ),
    )
    governance_hits = _keyword_count(
        future_text,
        (
            "board",
            "committee",
            "executive",
            "ceo",
            "cfo",
            "gc",
            "general counsel",
            "outside counsel",
            "legal",
            "approval",
            "gate",
        ),
    )
    evidence_hits = _keyword_count(
        future_text,
        (
            "document",
            "evidence",
            "packet",
            "inventory",
            "review",
            "reconcile",
            "memo",
            "minutes",
            "data room",
            "vsa",
            "soc 2",
            "dashboard",
        ),
    )
    confidence_hits = _keyword_count(
        future_text,
        (
            "investor",
            "creditor",
            "client",
            "customer",
            "downgrade",
            "stock",
            "press",
            "public",
            "external",
            "trust",
            "confidence",
        ),
    )
    return WhatIfFutureStateHeads(
        regulatory_exposure=_clamp(
            _count_norm(regulatory_hits, 4)
            + (_count_norm(evidence.legal_follow_up_count, 6) * 0.25)
            + (_count_norm(evidence.outside_attachment_spread_count, 4) * 0.20)
        ),
        accounting_control_pressure=_clamp(
            _count_norm(accounting_hits, 4)
            + (_count_norm(evidence.review_loop_count, 8) * 0.25)
            + (_count_norm(evidence.markup_loop_count, 6) * 0.15)
        ),
        liquidity_stress=_clamp(
            _count_norm(liquidity_hits, 5)
            + (_count_norm(evidence.urgency_spike_count, 5) * 0.20)
            + (_count_norm(evidence.executive_escalation_count, 4) * 0.15)
        ),
        governance_response=_clamp(
            _count_norm(governance_hits, 5)
            + (_count_norm(evidence.executive_mention_count, 6) * 0.20)
            + (_count_norm(evidence.cross_functional_loop_count, 6) * 0.15)
        ),
        evidence_control=_clamp(
            _count_norm(evidence_hits, 6)
            + (_count_norm(evidence.review_loop_count, 8) * 0.25)
            + (_count_norm(evidence.commitment_clarity_count, 5) * 0.20)
        ),
        external_confidence_pressure=_clamp(
            _count_norm(confidence_hits, 5)
            + (_count_norm(evidence.outside_recipient_count, 12) * 0.20)
            + (_count_norm(evidence.blame_pressure_count, 5) * 0.15)
        ),
    )


def evidence_to_business_outcomes(
    evidence: WhatIfObservedEvidenceHeads,
) -> WhatIfBusinessOutcomeHeads:
    external_spread = _clamp(
        (_count_norm(evidence.outside_recipient_count, 10) * 0.45)
        + (_count_norm(evidence.outside_forward_count, 6) * 0.25)
        + (_count_norm(evidence.outside_attachment_spread_count, 5) * 0.30)
    )
    legal_burden = _clamp(
        (_count_norm(evidence.legal_follow_up_count, 6) * 0.4)
        + (_count_norm(evidence.review_loop_count, 8) * 0.35)
        + (_count_norm(evidence.markup_loop_count, 6) * 0.25)
    )
    executive_heat = _clamp(
        (_count_norm(evidence.executive_escalation_count, 4) * 0.55)
        + (_count_norm(evidence.executive_mention_count, 6) * 0.25)
        + (_count_norm(evidence.urgency_spike_count, 5) * 0.20)
    )
    coordination_load = _clamp(
        (_count_norm(evidence.participant_fanout, 10) * 0.35)
        + (_count_norm(evidence.cc_expansion_count, 5) * 0.25)
        + (_count_norm(evidence.cross_functional_loop_count, 6) * 0.40)
    )
    decision_drag = _clamp(
        (_delay_norm(evidence.time_to_first_follow_up_ms, 72) * 0.30)
        + (_delay_norm(evidence.time_to_thread_end_ms, 240) * 0.35)
        + (_delay_norm(evidence.review_delay_burden_ms, 120) * 0.35)
    )
    trust_support = _clamp(
        (_count_norm(evidence.reassurance_count, 5) * 0.35)
        + (_count_norm(evidence.apology_repair_count, 4) * 0.20)
        + (_count_norm(evidence.commitment_clarity_count, 5) * 0.45)
    )
    conflict_heat = _clamp(
        (_count_norm(evidence.blame_pressure_count, 5) * 0.5)
        + (_count_norm(evidence.internal_disagreement_count, 4) * 0.5)
    )
    churn = _clamp(
        (_count_norm(evidence.attachment_recirculation_count, 6) * 0.5)
        + (_count_norm(evidence.version_turn_count, 6) * 0.5)
    )

    enterprise_risk = _clamp(
        (external_spread * 0.33)
        + (legal_burden * 0.22)
        + (executive_heat * 0.18)
        + (conflict_heat * 0.17)
        + (churn * 0.10)
    )
    org_strain = _clamp(
        (coordination_load * 0.40)
        + (executive_heat * 0.20)
        + (conflict_heat * 0.20)
        + (legal_burden * 0.20)
    )
    stakeholder_trust = _clamp(
        (trust_support * 0.55)
        + ((1.0 - decision_drag) * 0.20)
        + ((1.0 - conflict_heat) * 0.15)
        + ((1.0 - external_spread) * 0.10)
    )
    commercial_position = _clamp(
        (stakeholder_trust * 0.40)
        + ((1.0 - enterprise_risk) * 0.25)
        + ((1.0 - decision_drag) * 0.25)
        + ((1.0 - org_strain) * 0.10)
    )
    execution_drag = _clamp(
        (decision_drag * 0.55)
        + (coordination_load * 0.25)
        + (legal_burden * 0.10)
        + (churn * 0.10)
    )
    return WhatIfBusinessOutcomeHeads(
        enterprise_risk=round(enterprise_risk, 3),
        commercial_position_proxy=round(commercial_position, 3),
        org_strain_proxy=round(org_strain, 3),
        stakeholder_trust=round(stakeholder_trust, 3),
        execution_drag=round(execution_drag, 3),
    )


def score_business_objective(
    *,
    pack: WhatIfBusinessObjectivePack,
    outcomes: WhatIfBusinessOutcomeHeads,
    evidence: WhatIfObservedEvidenceHeads,
) -> WhatIfBusinessObjectiveScore:
    components: dict[str, float] = {}
    total_weight = 0.0
    weighted_total = 0.0
    for head_name, weight in pack.weights.items():
        raw_value = float(getattr(outcomes, head_name))
        component = 1.0 - raw_value if head_name in _NEGATIVE_HEADS else raw_value
        component = _clamp(component)
        components[head_name] = round(component, 3)
        total_weight += weight
        weighted_total += component * weight
    overall = weighted_total / max(total_weight, 1.0)
    return WhatIfBusinessObjectiveScore(
        objective_pack_id=pack.pack_id,
        overall_score=round(overall, 3),
        components=components,
        evidence=_score_evidence(pack.pack_id, outcomes, evidence),
    )


def _score_evidence(
    pack_id: WhatIfBusinessObjectivePackId,
    outcomes: WhatIfBusinessOutcomeHeads,
    evidence: WhatIfObservedEvidenceHeads,
) -> list[str]:
    lines = [
        f"enterprise risk {outcomes.enterprise_risk:.3f}",
        f"commercial proxy {outcomes.commercial_position_proxy:.3f}",
        f"org strain {outcomes.org_strain_proxy:.3f}",
        f"stakeholder trust {outcomes.stakeholder_trust:.3f}",
        f"execution drag {outcomes.execution_drag:.3f}",
    ]
    if pack_id == "minimize_enterprise_risk":
        lines.extend(
            [
                f"outside recipients {evidence.outside_recipient_count}",
                f"outside attachment spread {evidence.outside_attachment_spread_count}",
                f"executive escalations {evidence.executive_escalation_count}",
            ]
        )
    elif pack_id == "protect_commercial_position":
        lines.extend(
            [
                f"commitment clarity {evidence.commitment_clarity_count}",
                f"reassurance messages {evidence.reassurance_count}",
                f"time to first follow-up {evidence.time_to_first_follow_up_ms}",
            ]
        )
    elif pack_id == "reduce_org_strain":
        lines.extend(
            [
                f"participant fanout {evidence.participant_fanout}",
                f"cross-functional loops {evidence.cross_functional_loop_count}",
                f"blame pressure {evidence.blame_pressure_count}",
            ]
        )
    elif pack_id == "preserve_stakeholder_trust":
        lines.extend(
            [
                f"apology or repair language {evidence.apology_repair_count}",
                f"commitment clarity {evidence.commitment_clarity_count}",
                f"internal disagreement {evidence.internal_disagreement_count}",
            ]
        )
    else:
        lines.extend(
            [
                f"review delay burden {evidence.review_delay_burden_ms}",
                f"time to thread end {evidence.time_to_thread_end_ms}",
                f"review loops {evidence.review_loop_count}",
            ]
        )
    return lines


def _event_text(event: WhatIfEvent) -> str:
    return " ".join([event.subject, event.snippet, event.target_id]).lower()


def _event_external_count(event: WhatIfEvent) -> int:
    recipients = [
        item.strip().lower() for item in event.flags.to_recipients if item.strip()
    ]
    recipients.extend(
        item.strip().lower() for item in event.flags.cc_recipients if item.strip()
    )
    if event.target_id:
        recipients.append(event.target_id.strip().lower())
    return sum(1 for item in recipients if not item.endswith(f"@{ENRON_DOMAIN}"))


def _event_participants(event: WhatIfEvent) -> set[str]:
    participants = {event.actor_id, event.target_id}
    participants.update(event.flags.to_recipients)
    participants.update(event.flags.cc_recipients)
    return {item for item in participants if item}


def _cross_functional_marker_count(text: str) -> int:
    counts = Counter(marker for marker in _CROSS_FUNCTIONAL_MARKERS if marker in text)
    return len(counts)


def _keyword_count(text: str, keywords: Sequence[str]) -> int:
    return sum(1 for keyword in keywords if keyword in text)


def _has_any(text: str, terms: Sequence[str]) -> bool:
    return any(term in text for term in terms)


def _delay_norm(delay_ms: int, horizon_hours: int) -> float:
    hours = max(0.0, delay_ms / 3_600_000)
    return _clamp(hours / max(horizon_hours, 1))


def _count_norm(value: int, ceiling: int) -> float:
    return _clamp(float(value) / max(ceiling, 1))


def _clamp(value: float) -> float:
    return max(0.0, min(1.0, float(value)))


_PACKS: dict[str, WhatIfBusinessObjectivePack] = {
    "minimize_enterprise_risk": WhatIfBusinessObjectivePack(
        pack_id="minimize_enterprise_risk",
        title="Minimize Enterprise Risk",
        summary="Favor choices that reduce outside spread, escalation heat, and legal or conflict burden.",
        weights={
            "enterprise_risk": 0.55,
            "org_strain_proxy": 0.20,
            "execution_drag": 0.10,
            "stakeholder_trust": 0.15,
        },
        evidence_labels=[
            "outside spread",
            "attachment spread",
            "executive escalation",
            "legal burden",
        ],
    ),
    "protect_commercial_position": WhatIfBusinessObjectivePack(
        pack_id="protect_commercial_position",
        title="Protect Commercial Position",
        summary="Favor choices that preserve counterpart trust, keep the thread moving, and avoid self-inflicted commercial risk.",
        weights={
            "commercial_position_proxy": 0.55,
            "stakeholder_trust": 0.20,
            "enterprise_risk": 0.15,
            "execution_drag": 0.10,
        },
        evidence_labels=[
            "commitment clarity",
            "timely follow-up",
            "limited exposure",
            "repair language",
        ],
    ),
    "reduce_org_strain": WhatIfBusinessObjectivePack(
        pack_id="reduce_org_strain",
        title="Reduce Organizational Strain",
        summary="Favor choices that keep coordination narrow, reduce pressure, and avoid broad internal churn.",
        weights={
            "org_strain_proxy": 0.60,
            "execution_drag": 0.20,
            "enterprise_risk": 0.10,
            "stakeholder_trust": 0.10,
        },
        evidence_labels=[
            "participant fanout",
            "cross-functional loops",
            "blame pressure",
            "executive heat",
        ],
    ),
    "preserve_stakeholder_trust": WhatIfBusinessObjectivePack(
        pack_id="preserve_stakeholder_trust",
        title="Preserve Stakeholder Trust",
        summary="Favor choices that communicate clearly, repair confidence, and avoid visible conflict or drift.",
        weights={
            "stakeholder_trust": 0.60,
            "commercial_position_proxy": 0.20,
            "enterprise_risk": 0.10,
            "execution_drag": 0.10,
        },
        evidence_labels=[
            "reassurance",
            "repair language",
            "commitment clarity",
            "conflict heat",
        ],
    ),
    "maintain_execution_velocity": WhatIfBusinessObjectivePack(
        pack_id="maintain_execution_velocity",
        title="Maintain Execution Velocity",
        summary="Favor choices that keep the thread moving with fewer review loops and less coordination drag.",
        weights={
            "execution_drag": 0.60,
            "org_strain_proxy": 0.15,
            "commercial_position_proxy": 0.15,
            "enterprise_risk": 0.10,
        },
        evidence_labels=[
            "time to first follow-up",
            "time to thread end",
            "review delay burden",
            "review loops",
        ],
    ),
}

_RUBRICS: dict[str, WhatIfJudgeRubric] = {
    "minimize_enterprise_risk": WhatIfJudgeRubric(
        objective_pack_id="minimize_enterprise_risk",
        title="Enterprise Risk",
        question="Which candidate best reduces enterprise risk from this point in the thread?",
        criteria=[
            "Reduce outside spread of sensitive information",
            "Limit legal and escalation burden",
            "Avoid avoidable conflict and artifact churn",
        ],
        decision_rule="Prefer the candidate that most safely narrows exposure while keeping the thread governable.",
    ),
    "protect_commercial_position": WhatIfJudgeRubric(
        objective_pack_id="protect_commercial_position",
        title="Commercial Position",
        question="Which candidate best protects Enron's commercial position from this point?",
        criteria=[
            "Preserve counterpart confidence",
            "Keep commitments and next steps clear",
            "Avoid commercial harm from delay or risky oversharing",
        ],
        decision_rule="Prefer the candidate that best preserves the relationship and negotiation posture without creating larger risk.",
    ),
    "reduce_org_strain": WhatIfJudgeRubric(
        objective_pack_id="reduce_org_strain",
        title="Organizational Strain",
        question="Which candidate best reduces internal coordination strain from this point?",
        criteria=[
            "Keep the review loop narrow",
            "Reduce unnecessary fanout and pressure",
            "Preserve a clear owner and path to resolution",
        ],
        decision_rule="Prefer the candidate that keeps work concentrated and avoids broad internal churn.",
    ),
    "preserve_stakeholder_trust": WhatIfJudgeRubric(
        objective_pack_id="preserve_stakeholder_trust",
        title="Stakeholder Trust",
        question="Which candidate best preserves trust with the people affected by this thread?",
        criteria=[
            "Communicate clearly and credibly",
            "Show repair or reassurance when needed",
            "Avoid conflict, blame, or avoidable surprise",
        ],
        decision_rule="Prefer the candidate that most credibly maintains trust without hiding material risk.",
    ),
    "maintain_execution_velocity": WhatIfJudgeRubric(
        objective_pack_id="maintain_execution_velocity",
        title="Execution Velocity",
        question="Which candidate best keeps the business moving from this point?",
        criteria=[
            "Reduce delay to the next useful action",
            "Avoid needless review loops",
            "Keep a clear owner and fast path to thread resolution",
        ],
        decision_rule="Prefer the candidate that most directly moves the thread toward resolution without creating larger downstream drag.",
    ),
}


__all__ = [
    "evidence_to_business_outcomes",
    "get_business_judge_rubric",
    "get_business_objective_pack",
    "list_business_objective_packs",
    "score_business_objective",
    "summarize_observed_evidence",
]
