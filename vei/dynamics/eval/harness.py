"""Held-out-company evaluation harness.

Runs any registered DynamicsBackend against a held-out tenant corpus.
Produces factual metrics and counterfactual ranking (where available).
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field

from vei.dynamics.api import get_backend
from vei.dynamics.models import DynamicsRequest
from vei.events.api import CanonicalEvent, InternalExternal


class FactualMetrics(BaseModel):
    """Factual forecast evaluation metrics."""

    next_event_type_accuracy: float = 0.0
    next_event_time_mae_ms: float = 0.0
    recipient_spread_accuracy: float = 0.0
    escalation_auroc: Optional[float] = None
    approval_auroc: Optional[float] = None
    sample_count: int = 0


class CounterfactualMetrics(BaseModel):
    """Counterfactual ranking metrics (where available)."""

    rank_accuracy: float = 0.0
    rank_correlation: float = 0.0
    sample_count: int = 0


class CalibrationReport(BaseModel):
    """Calibration assessment."""

    ece: Optional[float] = None
    interval_coverage: Optional[float] = None
    notes: List[str] = Field(default_factory=list)


class EvalResult(BaseModel):
    """Full evaluation result for one backend on one tenant."""

    backend_id: str = ""
    backend_version: str = ""
    heldout_tenant: str = ""
    factual: FactualMetrics = Field(default_factory=FactualMetrics)
    counterfactual: CounterfactualMetrics = Field(
        default_factory=CounterfactualMetrics,
    )
    calibration: CalibrationReport = Field(default_factory=CalibrationReport)
    determinism_manifest: Dict[str, Any] = Field(default_factory=dict)


def _payload(event: CanonicalEvent) -> dict[str, Any]:
    if event.delta is None or not isinstance(event.delta.data, dict):
        return {}
    return dict(event.delta.data)


def _haystack(event: CanonicalEvent) -> str:
    payload_txt = ""
    try:
        payload_txt = json.dumps(_payload(event), sort_keys=True, default=str)
    except Exception:  # noqa: BLE001
        payload_txt = ""
    tokens = [
        event.kind or "",
        payload_txt,
        " ".join(event.policy_tags),
    ]
    return " ".join(tokens).lower()


def _external_spread_label(event: CanonicalEvent) -> bool:
    """Proxy for outbound / external-heavy next step (truth from observed event)."""

    if event.internal_external == InternalExternal.EXTERNAL:
        return True
    hay = _haystack(event)
    return "external" in hay or "outside" in hay or "bcc" in hay


def _escalation_label(event: CanonicalEvent) -> int:
    return int("escalat" in _haystack(event))


def _approval_label(event: CanonicalEvent) -> int:
    return int("approv" in _haystack(event))


def _binary_auroc(labels: List[int], scores: List[float]) -> float | None:
    if len(labels) != len(scores) or len(labels) < 2:
        return None
    positives = [score for label, score in zip(labels, scores) if label == 1]
    negatives = [score for label, score in zip(labels, scores) if label == 0]
    total_pairs = len(positives) * len(negatives)
    if total_pairs == 0:
        return None
    wins = 0.0
    for pos in positives:
        for neg in negatives:
            if pos > neg:
                wins += 1.0
            elif pos == neg:
                wins += 0.5
    return wins / total_pairs


def _expected_calibration_error(
    probs: List[float],
    outcomes: List[int],
    *,
    num_bins: int = 10,
) -> float | None:
    """Discrete-bin ECE between predicted probs and matching binary outcomes."""

    if len(probs) != len(outcomes):
        return None
    n = len(probs)
    if n < 2:
        return None
    used_bins = max(2, min(num_bins, max(2, n // 2)))
    ece = 0.0
    for b in range(used_bins):
        lo = b / used_bins
        hi = (b + 1) / used_bins
        masks: List[int] = []
        for idx, prob in enumerate(probs):
            in_bin = lo <= prob < hi if hi < 1 else lo <= prob <= hi
            if in_bin:
                masks.append(idx)
        if not masks:
            continue
        prop = len(masks) / n
        avg_confidence = sum(probs[idx] for idx in masks) / len(masks)
        avg_accuracy = sum(outcomes[idx] for idx in masks) / len(masks)
        ece += prop * abs(avg_accuracy - avg_confidence)
    return round(float(ece), 6)


def run_evaluation(
    *,
    backend_name: str,
    heldout_events: List[CanonicalEvent],
    heldout_tenant: str = "",
    window_size: int = 10,
    horizon: int = 5,
) -> EvalResult:
    """Run a dynamics backend against held-out events and produce metrics."""

    backend = get_backend(backend_name)
    info = backend.describe()
    manifest = backend.determinism_manifest()

    correct_type = 0
    total_predictions = 0
    time_errors: List[float] = []

    spread_correct = 0
    spread_evaluated = 0

    escalation_labels: List[int] = []
    escalation_scores: List[float] = []
    approval_labels: List[int] = []
    approval_scores: List[float] = []

    probs_for_ece: List[float] = []
    type_match_outcomes: List[int] = []

    for idx in range(len(heldout_events) - window_size - horizon + 1):
        recent = heldout_events[idx : idx + window_size]
        actual_next = heldout_events[idx + window_size : idx + window_size + horizon]

        request = DynamicsRequest(
            recent_events=recent,
            horizon=horizon,
        )
        response = backend.forecast(request)

        if not actual_next:
            continue
        actual_first = actual_next[0]

        if not response.predicted_events:
            continue

        total_predictions += 1
        prediction = response.predicted_events[0]
        predicted_kind = prediction.event.kind
        predicted_ts = prediction.event.ts_ms
        predicted_prob = prediction.probability

        type_match_outcomes.append(int(predicted_kind == actual_first.kind))
        probs_for_ece.append(predicted_prob)

        if predicted_kind == actual_first.kind:
            correct_type += 1

        time_errors.append(abs(predicted_ts - actual_first.ts_ms))

        spread_pred = float(response.business_heads.spread.point) > 1e-3
        spread_actual = _external_spread_label(actual_first)
        spread_correct += int(spread_pred == spread_actual)
        spread_evaluated += 1

        escalation_scores.append(float(response.business_heads.escalation.point))
        escalation_labels.append(_escalation_label(actual_first))
        approval_scores.append(float(response.business_heads.approval.point))
        approval_labels.append(_approval_label(actual_first))

    factual = FactualMetrics(
        next_event_type_accuracy=(
            correct_type / total_predictions if total_predictions > 0 else 0.0
        ),
        next_event_time_mae_ms=(
            sum(time_errors) / len(time_errors) if time_errors else 0.0
        ),
        recipient_spread_accuracy=(
            spread_correct / spread_evaluated if spread_evaluated > 0 else 0.0
        ),
        escalation_auroc=_binary_auroc(escalation_labels, escalation_scores),
        approval_auroc=_binary_auroc(approval_labels, approval_scores),
        sample_count=total_predictions,
    )

    notes: List[str] = []
    calibration_ece: float | None = None
    if probs_for_ece and type_match_outcomes:
        calibration_ece = _expected_calibration_error(
            probs_for_ece, type_match_outcomes
        )
    if total_predictions == 0:
        notes.append(
            "No predicted_events from backend across sliding windows;"
            " optional metrics omitted."
        )

    calibration = CalibrationReport(ece=calibration_ece, notes=notes)

    return EvalResult(
        backend_id=info.name,
        backend_version=info.version,
        heldout_tenant=heldout_tenant,
        factual=factual,
        calibration=calibration,
        determinism_manifest=manifest.model_dump(),
    )


def save_eval_result(result: EvalResult, path: Path) -> Path:
    """Write evaluation result to disk."""

    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(result.model_dump_json(indent=2), encoding="utf-8")
    return path
