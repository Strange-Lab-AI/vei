from __future__ import annotations

import json

from vei.dynamics.api import (
    ensure_builtin_backends_registered,
    register_backend,
    reset_registry,
)
from vei.dynamics.eval.harness import run_evaluation
from vei.dynamics.eval.reporting import (
    DYNAMICS_EVAL_METRICS_PATH,
    write_dynamics_eval_metrics,
)
from vei.dynamics.models import (
    BackendInfo,
    BusinessHeads,
    DeterminismManifest,
    DynamicsResponse,
    PointInterval,
    PredictedEvent,
)
from vei.events.api import build_event
from vei.events.models import ActorRef, EventDomain, InternalExternal


def test_dynamics_eval_writes_metrics_json() -> None:
    metrics_path = write_dynamics_eval_metrics()
    metrics = json.loads(metrics_path.read_text(encoding="utf-8"))

    assert metrics_path == DYNAMICS_EVAL_METRICS_PATH
    assert metrics["factual_next_event_auroc"] >= 0.8
    assert metrics["counterfactual_rank_pct"] >= 0.65
    assert metrics["calibration_ece"] <= 0.08
    assert "macro_stock_spearman" in metrics
    assert "macro_credit_auroc" in metrics
    assert "macro_credit_brier" in metrics
    assert "macro_ferc_auroc" in metrics
    assert "macro_ferc_brier" in metrics
    assert metrics_path.exists()


class _HarnessStubBackend:
    """Returns synthetic predictions aligned with sliding-window stubs."""

    def forecast(self, request):
        recent = request.recent_events
        last = recent[-1]
        escalate = "escalat" in (last.kind or "").lower()
        approve = "approv" in (last.kind or "").lower()

        pseudo = build_event(
            domain=last.domain or EventDomain.COMM_GRAPH,
            kind=str(last.kind or "mail.event"),
            ts_ms=int(last.ts_ms) + 1_000,
            internal_external=last.internal_external,
            actor_ref=(last.actor_ref or ActorRef(actor_id="stub")),
            delta_data=(
                dict(last.delta.data)
                if last.delta is not None and isinstance(last.delta.data, dict)
                else {}
            ),
        )
        pseudo = pseudo.model_copy(
            update={
                "event_id": f"{last.event_id}:forecast",
                "delta": pseudo.delta,
                "kind": pseudo.kind,
            },
        )

        predicted_spread_point = (
            0.92 if last.internal_external == InternalExternal.EXTERNAL else 0.06
        )
        return DynamicsResponse(
            backend_id="__harness_stub__",
            backend_version="test",
            predicted_events=[
                PredictedEvent(event=pseudo, probability=0.71),
            ],
            business_heads=BusinessHeads(
                risk=PointInterval(point=0.33),
                spread=PointInterval(point=predicted_spread_point),
                escalation=PointInterval(point=1.0 if escalate else 0.1),
                approval=PointInterval(point=1.0 if approve else 0.06),
                load=PointInterval(point=0.25),
                drag=PointInterval(point=0.15),
            ),
        )

    def describe(self) -> BackendInfo:
        return BackendInfo(
            name="__harness_stub__",
            version="test",
            backend_type="test",
            deterministic=True,
            metadata={},
        )

    def determinism_manifest(self) -> DeterminismManifest:
        return DeterminismManifest(
            backend_id="__harness_stub__",
            backend_version="test",
            notes=["harness_fixture"],
        )


def test_run_evaluation_populates_calibration_when_predictions_exist() -> None:
    reset_registry()
    register_backend("__harness_stub__", _HarnessStubBackend)

    events = []
    for ix in range(26):
        ext = ix % 3 == 0
        body_text = (
            "exec escalat board escalation"
            if ix % 4 == 0
            else ("legal approv meeting" if ix % 5 == 0 else "steady update status")
        )
        boundary = InternalExternal.EXTERNAL if ext else InternalExternal.INTERNAL
        events.append(
            build_event(
                domain=EventDomain.COMM_GRAPH,
                kind="mail.received",
                ts_ms=1_000_000 + ix * 97_531,
                internal_external=boundary,
                actor_ref=ActorRef(actor_id=f"alice{ix}@corp"),
                delta_data={
                    "subj": f"evt-{ix}",
                    "body_text": body_text,
                    "to": ["team@corp"],
                },
            )
        )

    try:
        result = run_evaluation(
            backend_name="__harness_stub__",
            heldout_events=events,
            window_size=5,
            horizon=1,
        )
    finally:
        reset_registry()
        ensure_builtin_backends_registered()

    assert result.backend_id == "__harness_stub__"
    assert result.factual.sample_count > 0
    assert result.factual.next_event_time_mae_ms >= 0.0
    assert isinstance(result.calibration.ece, float)
    assert result.factual.recipient_spread_accuracy >= 0.0
