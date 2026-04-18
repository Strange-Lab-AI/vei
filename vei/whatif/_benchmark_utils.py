from __future__ import annotations

from hashlib import sha256
from math import sqrt
from pathlib import Path
from typing import Sequence

from ._benchmark_constants import (
    EXECUTIVE_TERMS as _EXECUTIVE_TERMS,
    MULTI_PARTY_TERMS as _MULTI_PARTY_TERMS,
    REASSURANCE_TERMS as _REASSURANCE_TERMS,
)
from .corpus import external_recipient_count, recipient_scope
from .models import (
    WhatIfBenchmarkDatasetRow,
    WhatIfBenchmarkModelId,
    WhatIfBenchmarkMetricSummary,
    WhatIfBenchmarkStudyModelSummary,
    WhatIfEvent,
)


def kendall_tau(
    predicted_order: Sequence[str],
    judged_order: Sequence[str],
) -> float | None:
    hits, total = pairwise_hits(predicted_order, judged_order)
    if total == 0:
        return None
    discordant = total - hits
    return (hits - discordant) / total


def historical_branch_tags(
    event: WhatIfEvent,
    *,
    organization_domain: str,
) -> set[str]:
    tags: set[str] = set()
    if event.flags.consult_legal_specialist:
        tags.add("legal")
    if event.flags.consult_trading_specialist:
        tags.add("trading")
    if event.flags.has_attachment_reference:
        tags.add("attachment_present")
    if event_external_count(event, organization_domain=organization_domain) == 0:
        tags.add("internal_only")
    if event.flags.is_forward:
        tags.add("forward")
    if event.flags.is_escalation or event.event_type == "escalation":
        tags.add("escalation")
    return tags


def event_scope(
    event: WhatIfEvent,
    *,
    organization_domain: str,
) -> str:
    recipients = [
        item.strip().lower() for item in event.flags.to_recipients if item.strip()
    ]
    if event.target_id:
        recipients.append(event.target_id.strip().lower())
    if not recipients:
        return "unknown"
    return recipient_scope(
        recipients,
        organization_domain=organization_domain,
    )


def event_external_count(
    event: WhatIfEvent,
    *,
    organization_domain: str,
) -> int:
    recipients = [
        item.strip().lower() for item in event.flags.to_recipients if item.strip()
    ]
    if event.target_id:
        recipients.append(event.target_id.strip().lower())
    return external_recipient_count(
        recipients,
        organization_domain=organization_domain,
    )


def event_reassurance_count(event: WhatIfEvent) -> int:
    text = " ".join([event.subject, event.snippet]).lower()
    return int(any(token in text for token in _REASSURANCE_TERMS))


def event_has_review_signal(event: WhatIfEvent) -> bool:
    text = " ".join([event.subject, event.snippet]).lower()
    return any(token in text for token in ("review", "draft", "comment", "redline"))


def event_has_executive_signal(event: WhatIfEvent) -> bool:
    text = " ".join([event.subject, event.snippet, event.target_id]).lower()
    return any(token in text for token in _EXECUTIVE_TERMS)


def event_has_cross_functional_signal(event: WhatIfEvent) -> bool:
    text = " ".join([event.subject, event.snippet, event.target_id]).lower()
    marker_count = sum(
        1
        for token in ("legal", "trading", "risk", "credit", "regulatory", "hr")
        if token in text
    )
    return marker_count >= 2


def event_has_conflict_signal(event: WhatIfEvent) -> bool:
    text = " ".join([event.subject, event.snippet]).lower()
    return any(
        token in text
        for token in ("problem", "concern", "disagree", "cannot", "delay", "failure")
    )


def event_has_commitment_signal(event: WhatIfEvent) -> bool:
    text = " ".join([event.subject, event.snippet]).lower()
    return any(
        token in text
        for token in ("we will", "i will", "next step", "timeline", "owner", "plan")
    )


def event_has_urgency_signal(event: WhatIfEvent) -> bool:
    text = " ".join([event.subject, event.snippet]).lower()
    return any(token in text for token in ("urgent", "asap", "immediately", "today"))


def delay_norm(delay_ms: int) -> float:
    hours = max(0.0, delay_ms / 3_600_000)
    return clamp(hours / 72.0)


def message_count_norm(message_count: int) -> float:
    return clamp(message_count / 12.0)


def clamp(value: float) -> float:
    return max(0.0, min(1.0, float(value)))


def escalation_level_for_text(text: str, escalated: bool) -> str:
    if escalated and any(token in text for token in _EXECUTIVE_TERMS):
        return "executive"
    if escalated:
        return "manager"
    return "none"


def review_path_from_text(text: str, legal_flag: bool) -> str:
    if legal_flag or "counsel" in text or "legal" in text:
        return "internal_legal"
    if "hr" in text or "personnel" in text:
        return "hr"
    if "executive" in text or "leadership" in text:
        return "executive"
    if "review" in text or "comments" in text:
        return "cross_functional"
    return "business_owner"


def review_path_for_prompt(text: str, tags: set[str]) -> str:
    if "legal" in tags or "counsel" in text or "legal" in text:
        return "internal_legal"
    if "hr" in text:
        return "hr"
    if "executive_gate" in tags or "executive" in text:
        return "executive"
    if any(token in text for token in ("comments", "review", "circulation", "panel")):
        return "cross_functional"
    return "business_owner"


def coordination_breadth_for_event(
    event: WhatIfEvent,
    *,
    organization_domain: str,
) -> str:
    recipient_total = (
        event_external_count(
            event,
            organization_domain=organization_domain,
        )
        + len(event.flags.to_recipients)
        + len(event.flags.cc_recipients)
    )
    if recipient_total <= 1:
        return "single_owner"
    if recipient_total <= 3:
        return "narrow"
    if recipient_total <= 6:
        return "targeted"
    return "broad"


def coordination_breadth_for_prompt(text: str, tags: set[str]) -> str:
    if any(token in text for token in ("one owner", "single owner")):
        return "single_owner"
    if "broad" in tags or any(token in text for token in _MULTI_PARTY_TERMS):
        return "broad"
    if any(token in text for token in ("small", "tight", "narrow")):
        return "narrow"
    return "targeted"


def outside_sharing_posture_for_event(
    event: WhatIfEvent,
    *,
    organization_domain: str,
) -> str:
    external_count = event_external_count(
        event,
        organization_domain=organization_domain,
    )
    if external_count == 0:
        return "internal_only"
    if not event.flags.has_attachment_reference:
        return "status_only"
    if external_count == 1:
        return "limited_external"
    return "broad_external"


def outside_sharing_posture_for_prompt(
    *,
    recipient_scope: str,
    attachment_policy: str,
    lowered: str,
) -> str:
    if recipient_scope == "internal":
        return "internal_only"
    if attachment_policy == "sanitized" or "status note" in lowered:
        return "status_only"
    if recipient_scope == "external":
        return "limited_external"
    return "broad_external"


def decision_posture_for_text(
    text: str,
    *,
    hold_required: bool,
    escalated: bool,
) -> str:
    if hold_required:
        return "hold"
    if escalated:
        return "escalate"
    if any(token in text for token in ("resolve", "send", "answer", "confirm")):
        return "resolve"
    return "review"


def reassurance_style_for_text(text: str) -> str:
    hits = sum(1 for token in _REASSURANCE_TERMS if token in text)
    if hits >= 2:
        return "high"
    if hits == 1:
        return "medium"
    return "low"


def write_jsonl(path: Path, rows: Sequence[WhatIfBenchmarkDatasetRow]) -> None:
    lines = [row.model_dump_json() for row in rows]
    path.write_text("\n".join(lines) + ("\n" if lines else ""), encoding="utf-8")


def slug(label: str) -> str:
    pieces = [
        character.lower() if character.isalnum() else "_" for character in label.strip()
    ]
    text = "".join(pieces).strip("_")
    while "__" in text:
        text = text.replace("__", "_")
    return text or sha256(label.encode("utf-8")).hexdigest()[:10]


def pairwise_hits(
    predicted_order: Sequence[str],
    judged_order: Sequence[str],
) -> tuple[int, int]:
    judged_positions = {label: index for index, label in enumerate(judged_order)}
    common = [label for label in predicted_order if label in judged_positions]
    hits = 0
    total = 0
    for left_index, left_label in enumerate(common):
        left_position = judged_positions[left_label]
        for right_label in common[left_index + 1 :]:
            total += 1
            if left_position < judged_positions[right_label]:
                hits += 1
    return hits, total


def metric_summary(values: Sequence[float]) -> WhatIfBenchmarkMetricSummary:
    cleaned = [float(value) for value in values]
    if not cleaned:
        return WhatIfBenchmarkMetricSummary()
    mean = sum(cleaned) / len(cleaned)
    variance = sum((value - mean) ** 2 for value in cleaned) / len(cleaned)
    return WhatIfBenchmarkMetricSummary(
        count=len(cleaned),
        mean=round(mean, 6),
        std=round(sqrt(variance), 6),
        min=round(min(cleaned), 6),
        max=round(max(cleaned), 6),
    )


def optional_metric_summary(
    values: Sequence[float | None],
) -> WhatIfBenchmarkMetricSummary | None:
    cleaned = [float(value) for value in values if value is not None]
    if not cleaned:
        return None
    return metric_summary(cleaned)


def rank_study_models(
    summaries: Sequence[WhatIfBenchmarkStudyModelSummary],
) -> list[WhatIfBenchmarkModelId]:
    return [
        summary.model_id
        for summary in sorted(
            summaries,
            key=lambda summary: (
                -summary.dominance_pass_rate.mean,
                -(
                    summary.judge_top1_agreement.mean
                    if summary.judge_top1_agreement is not None
                    else -1.0
                ),
                -summary.observed_auroc_any_external_spread.mean,
                summary.model_id,
            ),
        )
    ]
