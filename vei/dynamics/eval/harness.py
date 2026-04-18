"""Held-out-company evaluation harness.

Runs any registered DynamicsBackend against a held-out tenant corpus.
Produces factual metrics and counterfactual ranking (where available).
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field

from vei.dynamics.api import get_backend
from vei.dynamics.models import DynamicsRequest
from vei.events.api import CanonicalEvent


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
    total_type = 0
    time_errors: List[float] = []

    for i in range(len(heldout_events) - window_size - horizon + 1):
        recent = heldout_events[i : i + window_size]
        actual_next = heldout_events[i + window_size : i + window_size + horizon]

        request = DynamicsRequest(
            recent_events=recent,
            horizon=horizon,
        )
        response = backend.forecast(request)

        if response.predicted_events and actual_next:
            predicted_kind = response.predicted_events[0].event.kind
            actual_kind = actual_next[0].kind
            if predicted_kind == actual_kind:
                correct_type += 1
            total_type += 1

            predicted_ts = response.predicted_events[0].event.ts_ms
            actual_ts = actual_next[0].ts_ms
            time_errors.append(abs(predicted_ts - actual_ts))
        else:
            total_type += 1

    factual = FactualMetrics(
        next_event_type_accuracy=(correct_type / total_type if total_type > 0 else 0.0),
        next_event_time_mae_ms=(
            sum(time_errors) / len(time_errors) if time_errors else 0.0
        ),
        sample_count=total_type,
    )

    return EvalResult(
        backend_id=info.name,
        backend_version=info.version,
        heldout_tenant=heldout_tenant,
        factual=factual,
        determinism_manifest=manifest.model_dump(),
    )


def save_eval_result(result: EvalResult, path: Path) -> Path:
    """Write evaluation result to disk."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(result.model_dump_json(indent=2), encoding="utf-8")
    return path
