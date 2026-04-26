from __future__ import annotations

import csv
import json
import re
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, Sequence

from pydantic import BaseModel, Field

from .benchmark import (
    _build_pre_branch_contract,
    outcome_targets_to_signals,
    summarize_observed_targets,
)
from .benchmark_runtime import run_branch_point_benchmark_prediction
from .benchmark_business import (
    evidence_to_business_outcomes,
    summarize_future_state_heads,
    summarize_observed_evidence,
)
from .critical_decision_benchmark import (
    _ACTIVE_NEWS_PUBLIC_WORLD_POLICY,
    _balanced_score,
    _objective_policy_action_adjustment,
    _objective_policy_score,
)
from .models import (
    WhatIfActionSchema,
    WhatIfArtifactFlags,
    WhatIfBenchmarkDatasetRow,
    WhatIfEvent,
    WhatIfWorld,
)

_TOPIC_KEYWORDS: dict[str, tuple[str, ...]] = {
    "banking_markets": (
        "bank",
        "banks",
        "banking",
        "specie",
        "currency",
        "credit",
        "treasury",
        "deposit",
        "deposits",
        "payment",
        "payments",
        "panic",
        "suspend",
        "suspended",
        "resumed",
        "resumption",
        "bill",
        "bills",
        "money",
        "loan",
        "loans",
        "discount",
        "discounts",
        "failures",
        "bankruptcy",
        "prices",
        "employment",
    ),
    "government_policy": (
        "president",
        "congress",
        "senate",
        "legislature",
        "bill",
        "treasury",
        "policy",
        "act",
        "law",
        "cabinet",
        "administration",
    ),
    "labor_work": (
        "labor",
        "work",
        "employment",
        "wages",
        "minister",
        "church",
        "factory",
        "mechanic",
    ),
}


class NewsStatePointCandidateInput(BaseModel):
    label: str
    action: str
    candidate_type: str = ""


class NewsStatePointRunArtifacts(BaseModel):
    root: Path
    state_point_path: Path
    result_json_path: Path
    result_csv_path: Path
    result_markdown_path: Path


class NewsStatePointRunResult(BaseModel):
    version: str = "1"
    label: str
    topic: str
    as_of: str
    state_event_id: str
    history_event_count: int
    future_event_count: int
    candidate_count: int
    artifacts: NewsStatePointRunArtifacts
    notes: list[str] = Field(default_factory=list)


@dataclass(frozen=True)
class _NewsStatePoint:
    topic: str
    as_of: str
    branch_event: WhatIfEvent
    history_events: list[WhatIfEvent]
    future_events: list[WhatIfEvent]
    state_summary: str
    evidence_events: list[WhatIfEvent]


def run_news_state_point_counterfactual(
    world: WhatIfWorld,
    *,
    checkpoint_path: str | Path,
    artifacts_root: str | Path,
    label: str,
    topic: str,
    as_of: str,
    candidates: Sequence[NewsStatePointCandidateInput],
    future_horizon_days: int = 90,
    max_history_events: int = 240,
    max_evidence_events: int = 12,
    device: str | None = None,
    runtime_root: str | Path | None = None,
) -> NewsStatePointRunResult:
    candidate_inputs = [
        (
            candidate
            if isinstance(candidate, NewsStatePointCandidateInput)
            else NewsStatePointCandidateInput.model_validate(candidate)
        )
        for candidate in candidates
    ]
    if not candidate_inputs:
        raise ValueError("at least one candidate action is required")
    state_point = build_news_state_point(
        world,
        topic=topic,
        as_of=as_of,
        future_horizon_days=future_horizon_days,
        max_history_events=max_history_events,
        max_evidence_events=max_evidence_events,
    )
    root = Path(artifacts_root).expanduser().resolve() / _slug(label)
    root.mkdir(parents=True, exist_ok=True)
    state_point_path = root / "state_point.json"
    result_json_path = root / "result.json"
    result_csv_path = root / "result.csv"
    result_markdown_path = root / "result.md"

    rows = _score_state_point_candidates(
        state_point,
        checkpoint_path=Path(checkpoint_path).expanduser().resolve(),
        candidates=candidate_inputs,
        organization_domain=world.summary.organization_domain,
        device=device,
        runtime_root=runtime_root,
        prediction_output_root=root / "prediction_runtime",
    )
    rows.sort(
        key=lambda item: (
            -float(item["strategic_usefulness_score"]),
            -float(item["balanced_ceo_score"]),
            str(item["label"]).lower(),
        )
    )
    balanced_order = sorted(
        rows,
        key=lambda item: (
            -float(item["balanced_ceo_score"]),
            str(item["label"]).lower(),
        ),
    )
    balanced_ranks = {
        str(item["candidate_id"]): index
        for index, item in enumerate(balanced_order, start=1)
    }
    for index, row in enumerate(rows, start=1):
        row["strategic_rank"] = index
        row["balanced_rank"] = balanced_ranks[str(row["candidate_id"])]

    state_payload = _state_point_payload(state_point)
    result_payload = {
        "label": label,
        "topic": state_point.topic,
        "as_of": state_point.as_of,
        "state_event_id": state_point.branch_event.event_id,
        "objective_policy_id": _ACTIVE_NEWS_PUBLIC_WORLD_POLICY.policy_id,
        "objective_policy_summary": _ACTIVE_NEWS_PUBLIC_WORLD_POLICY.summary,
        "state_point": state_payload,
        "candidates": rows,
        "no_future_context_for_state": True,
    }
    state_point_path.write_text(json.dumps(state_payload, indent=2), encoding="utf-8")
    result_json_path.write_text(
        json.dumps(result_payload, indent=2),
        encoding="utf-8",
    )
    _write_rows_csv(rows, result_csv_path)
    _write_markdown_result(
        rows=rows,
        state_payload=state_payload,
        path=result_markdown_path,
    )
    return NewsStatePointRunResult(
        label=label,
        topic=state_point.topic,
        as_of=state_point.as_of,
        state_event_id=state_point.branch_event.event_id,
        history_event_count=len(state_point.history_events),
        future_event_count=len(state_point.future_events),
        candidate_count=len(rows),
        artifacts=NewsStatePointRunArtifacts(
            root=root,
            state_point_path=state_point_path,
            result_json_path=result_json_path,
            result_csv_path=result_csv_path,
            result_markdown_path=result_markdown_path,
        ),
        notes=[
            "State-point run: as-of topic state plus supplied candidate actions.",
            "Candidate actions are human/API supplied, not inferred from a historical branch event.",
            "JEPA predicts future heads; objective policy ranks the predicted outcomes.",
        ],
    )


def build_news_state_point(
    world: WhatIfWorld,
    *,
    topic: str,
    as_of: str,
    future_horizon_days: int = 90,
    max_history_events: int = 240,
    max_evidence_events: int = 12,
) -> _NewsStatePoint:
    topic = _normalize_topic(topic)
    as_of_dt = _parse_datetime(as_of)
    horizon_dt = as_of_dt + timedelta(days=future_horizon_days)
    topic_events = sorted(
        [event for event in world.events if _event_matches_topic(event, topic)],
        key=lambda event: (event.timestamp_ms, event.event_id),
    )
    history_events = [
        event for event in topic_events if _parse_datetime(event.timestamp) <= as_of_dt
    ][-max_history_events:]
    if not history_events:
        raise ValueError(f"no pre-as-of events found for topic={topic!r} as_of={as_of}")
    future_events = [
        event
        for event in topic_events
        if as_of_dt < _parse_datetime(event.timestamp) <= horizon_dt
    ]
    state_summary = _summarize_state(
        topic=topic,
        as_of=as_of_dt,
        history_events=history_events,
    )
    branch_event = WhatIfEvent(
        event_id=f"news_state:{topic}:{as_of_dt.date().isoformat()}",
        timestamp=as_of_dt.replace(microsecond=0).isoformat().replace("+00:00", "Z"),
        timestamp_ms=int(as_of_dt.timestamp() * 1000),
        actor_id="vei.state_point",
        target_id="operator@local",
        event_type="state_point",
        thread_id=f"news_state:{topic}",
        case_id=f"news_state:{topic}:{as_of_dt.date().isoformat()}",
        surface="state",
        conversation_anchor=f"news_state:{topic}",
        subject=f"{topic.replace('_', ' ').title()} state as of {as_of_dt.date()}",
        snippet=state_summary,
        flags=WhatIfArtifactFlags(
            to_recipients=["operator@local"],
            to_count=1,
            subject=f"{topic.replace('_', ' ').title()} state point",
        ),
    )
    evidence_events = _select_evidence_events(
        history_events,
        topic=topic,
        max_events=max_evidence_events,
    )
    return _NewsStatePoint(
        topic=topic,
        as_of=branch_event.timestamp,
        branch_event=branch_event,
        history_events=history_events,
        future_events=future_events,
        state_summary=state_summary,
        evidence_events=evidence_events,
    )


def _score_state_point_candidates(
    state_point: _NewsStatePoint,
    *,
    checkpoint_path: Path,
    candidates: Sequence[NewsStatePointCandidateInput],
    organization_domain: str,
    device: str | None,
    runtime_root: str | Path | None,
    prediction_output_root: Path,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for index, candidate in enumerate(candidates, start=1):
        candidate_type = candidate.candidate_type or _infer_candidate_type(
            candidate.action
        )
        action_schema = _action_schema_for_candidate(
            action=candidate.action,
            candidate_type=candidate_type,
        )
        contract = _build_pre_branch_contract(
            case_id=state_point.branch_event.case_id,
            thread_id=state_point.branch_event.thread_id,
            branch_event=state_point.branch_event,
            history_events=state_point.history_events,
            organization_domain=organization_domain,
            action_schema=action_schema,
            notes=[
                "News state-point row.",
                "no_future_context=true",
                "state_point_not_historical_branch_event=true",
            ],
        )
        evidence = summarize_observed_evidence(
            branch_event=state_point.branch_event,
            future_events=state_point.future_events,
        )
        observed_targets = summarize_observed_targets(
            branch_event=state_point.branch_event,
            future_events=state_point.future_events,
            organization_domain=organization_domain,
        )
        row = WhatIfBenchmarkDatasetRow(
            row_id=f"{state_point.branch_event.event_id}:candidate_{index}",
            split="heldout",
            thread_id=state_point.branch_event.thread_id,
            branch_event_id=state_point.branch_event.event_id,
            contract=contract,
            observed_evidence_heads=evidence,
            observed_business_outcomes=evidence_to_business_outcomes(evidence),
            observed_future_state=summarize_future_state_heads(
                future_events=state_point.future_events,
                evidence=evidence,
            ),
            observed_targets=observed_targets,
            observed_outcome_signals=outcome_targets_to_signals(observed_targets),
        )
        prediction = run_branch_point_benchmark_prediction(
            checkpoint_path=checkpoint_path,
            row=row,
            device=device,
            runtime_root=runtime_root,
            output_root=prediction_output_root,
        )
        predicted_evidence = prediction["evidence_heads"]
        predicted_business = prediction["business_heads"]
        predicted_future_state = prediction["future_state_heads"]
        objective_scores = {
            str(key): float(value)
            for key, value in dict(prediction["objective_scores"]).items()
        }
        balanced = _balanced_score(objective_scores)
        strategic = _objective_policy_score(
            balanced_score=balanced,
            objective_scores=objective_scores,
            candidate_type=candidate_type,
            policy=_ACTIVE_NEWS_PUBLIC_WORLD_POLICY,
        )
        rows.append(
            {
                "candidate_id": f"candidate_{index}",
                "candidate_type": candidate_type,
                "label": candidate.label,
                "action": candidate.action,
                "generation_source": "human",
                "generation_model": "operator_supplied",
                "no_future_context": True,
                "strategic_usefulness_score": strategic,
                "balanced_ceo_score": balanced,
                "strategic_action_adjustment": _objective_policy_action_adjustment(
                    candidate_type,
                    policy=_ACTIVE_NEWS_PUBLIC_WORLD_POLICY,
                ),
                "objective_policy_id": _ACTIVE_NEWS_PUBLIC_WORLD_POLICY.policy_id,
                "objective_scores": objective_scores,
                "business_heads": predicted_business,
                "evidence_heads": predicted_evidence,
                "future_state_heads": predicted_future_state,
                "action_schema": action_schema.model_dump(mode="json"),
            }
        )
    return rows


def _action_schema_for_candidate(
    *,
    action: str,
    candidate_type: str,
) -> WhatIfActionSchema:
    lowered = action.lower()
    external = any(
        token in lowered
        for token in (
            "public",
            "readers",
            "market",
            "policy",
            "publish",
            "memo",
            "advisory",
            "warning",
            "congress",
            "treasury",
            "bank",
        )
    )
    broad = any(
        token in lowered
        for token in ("broad", "coordinate", "regional", "congress", "treasury")
    )
    hold = any(token in lowered for token in ("hold", "defer", "do not"))
    review = any(token in lowered for token in ("review", "legal", "expert"))
    tags = {candidate_type}
    if external:
        tags.add("status_only")
        tags.add("send_now")
    if broad:
        tags.add("widen_loop")
    if hold:
        tags.add("hold")
    if review:
        tags.add("expert_review")
    if any(
        token in lowered
        for token in (
            "bank",
            "banking",
            "credit",
            "specie",
            "treasury",
            "liquidity",
            "deposit",
            "currency",
        )
    ):
        tags.add("macro_finance")
    return WhatIfActionSchema(
        event_type="state_point",
        recipient_scope="mixed" if broad else ("external" if external else "internal"),
        external_recipient_count=2 if broad else (1 if external else 0),
        attachment_policy="sanitized" if external else "none",
        hold_required=hold,
        legal_review_required=review and "legal" in lowered,
        trading_review_required=False,
        escalation_level="manager" if review else "none",
        owner_clarity="single_owner",
        reassurance_style="low",
        review_path=(
            "cross_functional" if broad else ("business_owner" if external else "none")
        ),
        coordination_breadth=(
            "targeted" if broad else ("narrow" if external else "single_owner")
        ),
        outside_sharing_posture="limited_external" if external else "internal_only",
        decision_posture="hold" if hold else ("escalate" if review else "resolve"),
        action_tags=sorted(tags),
    )


def _infer_candidate_type(action: str) -> str:
    lowered = action.lower()
    if any(token in lowered for token in ("hold", "defer", "do not")):
        return "hold_compliance_review"
    if any(
        token in lowered
        for token in (
            "market",
            "policy",
            "memo",
            "bank",
            "banking",
            "treasury",
            "credit",
            "specie",
            "liquidity",
            "deposit",
            "currency",
            "economy",
            "economic",
            "congress",
        )
    ):
        return "commercial_reset"
    if any(token in lowered for token in ("coordinate", "agency", "institution")):
        return "cross_function_war_room"
    if any(token in lowered for token in ("advisory", "warning", "bulletin")):
        return "customer_status_note"
    if any(token in lowered for token in ("watch", "monitor", "indicator")):
        return "narrow_pilot"
    return "fast_ship_low_risk"


def _state_point_payload(state_point: _NewsStatePoint) -> dict[str, Any]:
    return {
        "topic": state_point.topic,
        "as_of": state_point.as_of,
        "state_event_id": state_point.branch_event.event_id,
        "state_point_not_historical_branch_event": True,
        "no_future_context_for_state": True,
        "state_subject": state_point.branch_event.subject,
        "state_summary": state_point.state_summary,
        "history_event_count": len(state_point.history_events),
        "future_event_count": len(state_point.future_events),
        "evidence_events": [
            {
                "event_id": event.event_id,
                "timestamp": event.timestamp,
                "subject": event.subject,
                "snippet": event.snippet,
                "actor_id": event.actor_id,
                "surface": event.surface,
            }
            for event in state_point.evidence_events
        ],
    }


def _summarize_state(
    *,
    topic: str,
    as_of: datetime,
    history_events: Sequence[WhatIfEvent],
) -> str:
    keyword_counts = {
        keyword: sum(1 for event in history_events if keyword in _event_text(event))
        for keyword in _keywords_for_topic(topic)
    }
    top_terms = [
        keyword
        for keyword, count in sorted(
            keyword_counts.items(),
            key=lambda item: (-item[1], item[0]),
        )
        if count
    ][:8]
    first = history_events[0].timestamp[:10]
    last = history_events[-1].timestamp[:10]
    topic_label = topic.replace("_", " ")
    terms = ", ".join(top_terms) if top_terms else "no high-confidence terms"
    return (
        f"As of {as_of.date().isoformat()}, the {topic_label} state is built from "
        f"{len(history_events)} topic-matched news events dated {first} through {last}. "
        f"Recurring signals include: {terms}."
    )


def _select_evidence_events(
    history_events: Sequence[WhatIfEvent],
    *,
    topic: str,
    max_events: int,
) -> list[WhatIfEvent]:
    keywords = _keywords_for_topic(topic)
    scored = sorted(
        history_events,
        key=lambda event: (
            -sum(1 for keyword in keywords if keyword in _event_text(event)),
            -event.timestamp_ms,
            event.event_id,
        ),
    )
    selected = sorted(
        scored[:max_events], key=lambda event: (event.timestamp_ms, event.event_id)
    )
    return selected


def _event_matches_topic(event: WhatIfEvent, topic: str) -> bool:
    haystack = " ".join(
        [
            event.thread_id,
            event.case_id,
            event.conversation_anchor,
            event.subject,
            event.snippet,
        ]
    ).lower()
    normalized = topic.lower()
    if normalized in haystack or normalized.replace("_", " ") in haystack:
        return True
    return any(keyword in haystack for keyword in _keywords_for_topic(normalized))


def _keywords_for_topic(topic: str) -> tuple[str, ...]:
    normalized = _normalize_topic(topic)
    return _TOPIC_KEYWORDS.get(normalized, tuple(_topic_words(normalized)))


def _topic_words(topic: str) -> list[str]:
    return [part for part in re.split(r"[_\\W]+", topic.lower()) if part]


def _event_text(event: WhatIfEvent) -> str:
    return " ".join([event.subject, event.snippet, event.target_id]).lower()


def _parse_datetime(value: str) -> datetime:
    text = value.strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    if "T" not in text:
        text = f"{text}T00:00:00+00:00"
    parsed = datetime.fromisoformat(text)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _normalize_topic(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", value.strip().lower()).strip("_")


def _slug(value: str) -> str:
    slug = re.sub(r"[^a-z0-9]+", "_", value.strip().lower()).strip("_")
    return slug or "news_state_point"


def _write_rows_csv(rows: Sequence[dict[str, Any]], path: Path) -> None:
    fieldnames = [
        "strategic_rank",
        "balanced_rank",
        "candidate_id",
        "candidate_type",
        "label",
        "strategic_usefulness_score",
        "balanced_ceo_score",
        "enterprise_risk",
        "commercial_position_proxy",
        "org_strain_proxy",
        "stakeholder_trust",
        "execution_drag",
        "regulatory_exposure",
        "liquidity_stress",
        "external_confidence_pressure",
        "outside_recipient_count",
        "participant_fanout",
        "action",
    ]
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            business = row["business_heads"]
            future = row["future_state_heads"]
            evidence = row["evidence_heads"]
            writer.writerow(
                {
                    "strategic_rank": row["strategic_rank"],
                    "balanced_rank": row["balanced_rank"],
                    "candidate_id": row["candidate_id"],
                    "candidate_type": row["candidate_type"],
                    "label": row["label"],
                    "strategic_usefulness_score": row["strategic_usefulness_score"],
                    "balanced_ceo_score": row["balanced_ceo_score"],
                    "enterprise_risk": business["enterprise_risk"],
                    "commercial_position_proxy": business["commercial_position_proxy"],
                    "org_strain_proxy": business["org_strain_proxy"],
                    "stakeholder_trust": business["stakeholder_trust"],
                    "execution_drag": business["execution_drag"],
                    "regulatory_exposure": future["regulatory_exposure"],
                    "liquidity_stress": future["liquidity_stress"],
                    "external_confidence_pressure": future[
                        "external_confidence_pressure"
                    ],
                    "outside_recipient_count": evidence["outside_recipient_count"],
                    "participant_fanout": evidence["participant_fanout"],
                    "action": row["action"],
                }
            )


def _write_markdown_result(
    *,
    rows: Sequence[dict[str, Any]],
    state_payload: dict[str, Any],
    path: Path,
) -> None:
    lines = [
        "# News State-Point Counterfactual",
        "",
        f"- Topic: `{state_payload['topic']}`",
        f"- As of: `{state_payload['as_of']}`",
        f"- State event: `{state_payload['state_event_id']}`",
        f"- Objective policy: `{_ACTIVE_NEWS_PUBLIC_WORLD_POLICY.policy_id}`",
        "",
        "## State Dossier",
        "",
        state_payload["state_summary"],
        "",
        "## Evidence Events",
        "",
    ]
    for event in state_payload["evidence_events"]:
        lines.append(f"- `{event['timestamp']}` {event['subject']}: {event['snippet']}")
    lines.extend(
        [
            "",
            "## Candidate Predictions",
            "",
            "| Strategic Rank | Balanced Rank | Candidate | Strategic | Balanced | Risk | Commercial | Trust | Drag | Liquidity | External Confidence |",
            "|---:|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|",
        ]
    )
    for row in rows:
        business = row["business_heads"]
        future = row["future_state_heads"]
        lines.append(
            "| {strategic_rank} | {balanced_rank} | {label} | {strategic:.3f} | {balanced:.3f} | {risk:.3f} | {commercial:.3f} | {trust:.3f} | {drag:.3f} | {liquidity:.3f} | {confidence:.3f} |".format(
                strategic_rank=row["strategic_rank"],
                balanced_rank=row["balanced_rank"],
                label=_md(str(row["label"])),
                strategic=float(row["strategic_usefulness_score"]),
                balanced=float(row["balanced_ceo_score"]),
                risk=float(business["enterprise_risk"]),
                commercial=float(business["commercial_position_proxy"]),
                trust=float(business["stakeholder_trust"]),
                drag=float(business["execution_drag"]),
                liquidity=float(future["liquidity_stress"]),
                confidence=float(future["external_confidence_pressure"]),
            )
        )
    lines.extend(["", "## Candidate Actions", ""])
    for row in rows:
        lines.extend(
            [
                f"### {row['strategic_rank']}. {_md(str(row['label']))}",
                "",
                str(row["action"]),
                "",
            ]
        )
    path.write_text("\n".join(lines).strip() + "\n", encoding="utf-8")


def _md(value: str) -> str:
    return value.replace("|", "\\|").replace("\n", " ")


__all__ = [
    "NewsStatePointCandidateInput",
    "NewsStatePointRunArtifacts",
    "NewsStatePointRunResult",
    "build_news_state_point",
    "run_news_state_point_counterfactual",
]
