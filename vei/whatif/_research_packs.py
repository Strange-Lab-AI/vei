from __future__ import annotations

from typing import Sequence

from .models import (
    WhatIfResearchCandidate,
    WhatIfResearchCase,
    WhatIfResearchHypothesisLabel,
    WhatIfResearchPack,
)


def build_research_packs(
    rollout_seeds: Sequence[int],
) -> dict[str, WhatIfResearchPack]:
    return {
        "enron_research_v1": WhatIfResearchPack(
            pack_id="enron_research_v1",
            title="Enron Research Pack v1",
            summary=(
                "Six historical Enron branch points with creative candidate moves, eight fixed LLM rollout seeds, and a multi-backend outcome scoreboard."
            ),
            objective_pack_ids=[
                "contain_exposure",
                "reduce_delay",
                "protect_relationship",
            ],
            rollout_seeds=list(rollout_seeds),
            cases=[
                WhatIfResearchCase(
                    case_id="master_agreement",
                    title="Master Agreement",
                    event_id="enron_bcda1b925800af8c",
                    thread_id="thr_e565b47423d035c9",
                    summary="Debra Perlingiere sends a draft Master Agreement to Cargill.",
                    candidates=[
                        _candidate(
                            candidate_id="legal_hold_internal",
                            label="Legal hold internal",
                            prompt="Keep the draft inside Enron, ask Gerald Nemec for review, and hold the Cargill send until the clean draft is approved.",
                            contain_exposure="best_expected",
                            reduce_delay="worst_expected",
                            protect_relationship="middle_expected",
                        ),
                        _candidate(
                            candidate_id="narrow_external_status",
                            label="Narrow external status",
                            prompt="Send Kathy a short no-attachment status note immediately and promise a clean draft after internal review.",
                            contain_exposure="middle_expected",
                            reduce_delay="best_expected",
                            protect_relationship="best_expected",
                        ),
                        _candidate(
                            candidate_id="broad_external_send",
                            label="Broad external send",
                            prompt="Send the draft now and widen outside circulation for fast turnaround and broader comments.",
                            contain_exposure="worst_expected",
                            reduce_delay="middle_expected",
                            protect_relationship="worst_expected",
                        ),
                    ],
                ),
                WhatIfResearchCase(
                    case_id="btu_weekly",
                    title="BTU Weekly",
                    event_id="enron_7e7afce27432edae",
                    thread_id="thr_68db3d4f8c43d4cf",
                    summary="Vince Kaminski forwards BTU Weekly and its PDF to a personal address.",
                    candidates=[
                        _candidate(
                            candidate_id="internal_summary_only",
                            label="Internal summary only",
                            prompt="Remove the outside recipient and attachment, send only an internal summary, and keep the issue internal.",
                            contain_exposure="best_expected",
                            reduce_delay="worst_expected",
                            protect_relationship="middle_expected",
                        ),
                        _candidate(
                            candidate_id="sanitized_personal_forward",
                            label="Sanitized personal forward",
                            prompt="Send a short sanitized no-PDF status note to the personal address and keep the original attachment internal.",
                            contain_exposure="middle_expected",
                            reduce_delay="best_expected",
                            protect_relationship="best_expected",
                        ),
                        _candidate(
                            candidate_id="raw_attachment_forward",
                            label="Raw attachment forward",
                            prompt="Forward the full BTU Weekly PDF unchanged to the outside address for speed.",
                            contain_exposure="worst_expected",
                            reduce_delay="middle_expected",
                            protect_relationship="worst_expected",
                        ),
                    ],
                ),
                WhatIfResearchCase(
                    case_id="draft_position_paper",
                    title="Draft Position Paper",
                    event_id="enron_0a8a8985b6ae0d47",
                    thread_id="thr_1d6e90d6c8697401",
                    summary="A draft position paper is circulated to a broad outside group with an attachment.",
                    candidates=[
                        _candidate(
                            candidate_id="internal_red_team",
                            label="Internal red team",
                            prompt="Hold outside replies, route the draft through Enron legal and trading first, and keep comments inside the company.",
                            contain_exposure="best_expected",
                            reduce_delay="worst_expected",
                            protect_relationship="middle_expected",
                        ),
                        _candidate(
                            candidate_id="outside_counsel_only",
                            label="Outside counsel only",
                            prompt="Limit the outside loop to Sullivan & Cromwell and one Enron owner, and ask for consolidated comments only.",
                            contain_exposure="middle_expected",
                            reduce_delay="best_expected",
                            protect_relationship="best_expected",
                        ),
                        _candidate(
                            candidate_id="coalition_blast",
                            label="Coalition blast",
                            prompt="Keep the broad outside recipient set, attach the draft again, and request comments from everyone in the coalition.",
                            contain_exposure="worst_expected",
                            reduce_delay="middle_expected",
                            protect_relationship="worst_expected",
                        ),
                    ],
                ),
                WhatIfResearchCase(
                    case_id="ferc_weekly_report",
                    title="Weekly FERC Gas Regulatory Report",
                    event_id="enron_405ee04fb4ce3ff4",
                    thread_id="thr_c79fc41dcab28f9c",
                    summary="A regulatory report is forwarded with legal and trading cues.",
                    candidates=[
                        _candidate(
                            candidate_id="owner_assignment",
                            label="Owner assignment",
                            prompt="Rewrite the report into one internal note with one named owner and one required action.",
                            contain_exposure="middle_expected",
                            reduce_delay="best_expected",
                            protect_relationship="best_expected",
                        ),
                        _candidate(
                            candidate_id="legal_gate",
                            label="Legal gate",
                            prompt="Keep the report in legal and regulatory review before wider desk circulation.",
                            contain_exposure="best_expected",
                            reduce_delay="worst_expected",
                            protect_relationship="middle_expected",
                        ),
                        _candidate(
                            candidate_id="wide_alarm",
                            label="Wide alarm",
                            prompt="Forward the full report broadly with urgent language and no single owner.",
                            contain_exposure="worst_expected",
                            reduce_delay="middle_expected",
                            protect_relationship="worst_expected",
                        ),
                    ],
                ),
                WhatIfResearchCase(
                    case_id="credit_derivatives_confidentiality",
                    title="Credit Derivatives Confidentiality",
                    event_id="enron_466a009e2ef0589f",
                    thread_id="thr_cb6ca499db205b16",
                    summary="Draft confidentiality policies and procedures arrive from outside counsel.",
                    candidates=[
                        _candidate(
                            candidate_id="legal_only_markups",
                            label="Legal only markups",
                            prompt="Keep the confidentiality draft in a tiny Enron legal circle and send one consolidated markup back.",
                            contain_exposure="best_expected",
                            reduce_delay="worst_expected",
                            protect_relationship="middle_expected",
                        ),
                        _candidate(
                            candidate_id="trading_alignment",
                            label="Trading alignment",
                            prompt="Share with one business owner plus legal, then respond with consolidated questions and markups.",
                            contain_exposure="middle_expected",
                            reduce_delay="best_expected",
                            protect_relationship="best_expected",
                        ),
                        _candidate(
                            candidate_id="wide_policy_circulation",
                            label="Wide policy circulation",
                            prompt="Forward the draft policies broadly across trading for rapid comments and wider policy circulation.",
                            contain_exposure="worst_expected",
                            reduce_delay="middle_expected",
                            protect_relationship="worst_expected",
                        ),
                    ],
                ),
                WhatIfResearchCase(
                    case_id="arbitration_guidance",
                    title="Arbitration Guidance",
                    event_id="enron_9ae972719b28ab61",
                    thread_id="thr_5c00adfa02940001",
                    summary="Arbitration guidance from outside counsel enters the Enron legal loop.",
                    candidates=[
                        _candidate(
                            candidate_id="outside_counsel_hold",
                            label="Outside counsel hold",
                            prompt="Ask outside counsel for a written recommendation and keep the thread in legal only until the answer is final.",
                            contain_exposure="middle_expected",
                            reduce_delay="worst_expected",
                            protect_relationship="middle_expected",
                        ),
                        _candidate(
                            candidate_id="internal_policy_answer",
                            label="Internal policy answer",
                            prompt="Answer from internal policy immediately, keep distribution narrow, and avoid broader escalation.",
                            contain_exposure="best_expected",
                            reduce_delay="best_expected",
                            protect_relationship="best_expected",
                        ),
                        _candidate(
                            candidate_id="broad_exec_escalation",
                            label="Broad executive escalation",
                            prompt="Forward the arbitration question broadly to business leaders and counsel for a fast consensus.",
                            contain_exposure="worst_expected",
                            reduce_delay="middle_expected",
                            protect_relationship="worst_expected",
                        ),
                    ],
                ),
            ],
        )
    }


def _candidate(
    *,
    candidate_id: str,
    label: str,
    prompt: str,
    contain_exposure: WhatIfResearchHypothesisLabel,
    reduce_delay: WhatIfResearchHypothesisLabel,
    protect_relationship: WhatIfResearchHypothesisLabel,
) -> WhatIfResearchCandidate:
    return WhatIfResearchCandidate(
        candidate_id=candidate_id,
        label=label,
        prompt=prompt,
        expected_hypotheses={
            "contain_exposure": contain_exposure,
            "reduce_delay": reduce_delay,
            "protect_relationship": protect_relationship,
        },
    )


__all__ = ["build_research_packs"]
