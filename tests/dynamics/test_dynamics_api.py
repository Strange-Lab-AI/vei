"""Tests for vei.dynamics contract, registry, and golden fixtures."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from vei.dynamics.api import (
    DynamicsBackend,
    get_backend,
    list_backends,
    register_backend,
    reset_registry,
)
from vei.dynamics.backends.heuristic import HeuristicBaseline
from vei.dynamics.backends.null import NullBackend
from vei.dynamics.backends.reference import ReferenceBackend
from vei.dynamics.models import DynamicsRequest
from vei.events.api import build_event
from vei.events.models import ActorRef, EventDomain

GOLDENS_DIR = Path(__file__).parent / "goldens"


def _reference_prediction_stub(**_kwargs: object) -> dict[str, object]:
    return {
        "model_id": "ft_transformer",
        "binary_probability": 0.73,
        "regression_values": [0.0] * 22,
        "evidence_heads": {
            "any_external_spread": True,
            "outside_recipient_count": 2,
            "outside_forward_count": 1,
            "outside_attachment_spread_count": 0,
            "legal_follow_up_count": 1,
            "review_loop_count": 1,
            "markup_loop_count": 0,
            "executive_escalation_count": 0,
            "executive_mention_count": 0,
            "urgency_spike_count": 0,
            "participant_fanout": 3,
            "cc_expansion_count": 0,
            "cross_functional_loop_count": 0,
            "time_to_first_follow_up_ms": 2000,
            "time_to_thread_end_ms": 4000,
            "review_delay_burden_ms": 2000,
            "reassurance_count": 0,
            "apology_repair_count": 0,
            "commitment_clarity_count": 1,
            "blame_pressure_count": 0,
            "internal_disagreement_count": 0,
            "attachment_recirculation_count": 0,
            "version_turn_count": 0,
        },
    }


@pytest.fixture(autouse=True)
def _clean_registry():
    reset_registry()
    register_backend("null", NullBackend)
    register_backend("heuristic_baseline", HeuristicBaseline)
    yield
    reset_registry()


class TestNullBackend:
    def test_satisfies_protocol(self) -> None:
        backend = NullBackend()
        assert isinstance(backend, DynamicsBackend)

    def test_forecast_returns_empty(self) -> None:
        backend = NullBackend()
        request = DynamicsRequest(seed=42042)
        response = backend.forecast(request)
        assert response.backend_id == "null"
        assert response.predicted_events == []

    def test_golden_roundtrip(self) -> None:
        request_path = GOLDENS_DIR / "null_request.json"
        response_path = GOLDENS_DIR / "null_response.json"
        request = DynamicsRequest.model_validate_json(request_path.read_text())
        backend = NullBackend()
        response = backend.forecast(request)
        expected = json.loads(response_path.read_text())
        actual = response.model_dump(mode="json")
        assert actual == expected

    def test_describe(self) -> None:
        info = NullBackend().describe()
        assert info.name == "null"
        assert info.deterministic is True

    def test_determinism_manifest(self) -> None:
        manifest = NullBackend().determinism_manifest()
        assert manifest.backend_id == "null"


class TestHeuristicBaseline:
    def test_satisfies_protocol(self) -> None:
        backend = HeuristicBaseline()
        assert isinstance(backend, DynamicsBackend)

    def test_hold_reduces_risk(self) -> None:
        from vei.dynamics.models import CandidateAction

        backend = HeuristicBaseline()
        request = DynamicsRequest(
            candidate_action=CandidateAction(
                label="hold draft",
                description="hold pause_forward",
            ),
        )
        response = backend.forecast(request)
        assert response.business_heads.risk.point < 0

    def test_send_now_increases_risk(self) -> None:
        from vei.dynamics.models import CandidateAction

        backend = HeuristicBaseline()
        request = DynamicsRequest(
            candidate_action=CandidateAction(
                label="send now",
                description="send_now widen_loop",
            ),
        )
        response = backend.forecast(request)
        assert response.business_heads.risk.point > 0

    def test_describe(self) -> None:
        info = HeuristicBaseline().describe()
        assert info.name == "heuristic_baseline"


class TestRegistry:
    def test_list_backends(self) -> None:
        backends = list_backends()
        assert "null" in backends
        assert "heuristic_baseline" in backends

    def test_get_backend_restores_builtin_defaults_after_reset(self) -> None:
        reset_registry()
        backend = get_backend("heuristic_baseline")
        assert isinstance(backend, HeuristicBaseline)

    def test_get_backend_unknown(self) -> None:
        with pytest.raises(KeyError, match="no_such_backend"):
            get_backend("no_such_backend")

    def test_get_backend_caches(self) -> None:
        a = get_backend("null")
        b = get_backend("null")
        assert a is b


class TestReferenceBackend:
    def test_uses_repo_checkpoint_when_available(
        self, tmp_path: Path, monkeypatch
    ) -> None:
        checkpoint_path = tmp_path / "reference-model.pt"
        checkpoint_path.write_bytes(b"placeholder")
        monkeypatch.delenv("VEI_REFERENCE_BACKEND_CHECKPOINT", raising=False)
        monkeypatch.setattr(
            "vei.dynamics.backends.reference._DEFAULT_REFERENCE_CHECKPOINT",
            checkpoint_path,
        )
        monkeypatch.setattr(
            "vei.dynamics.backends.reference.run_branch_point_benchmark_prediction",
            _reference_prediction_stub,
        )
        backend = ReferenceBackend()

        response = backend.forecast(DynamicsRequest(seed=42042))

        assert response.backend_id == "reference"
        assert response.state_delta_summary["checkpoint_path"] == str(
            checkpoint_path.resolve()
        )

    def test_returns_explicit_error_without_checkpoint(self, tmp_path: Path) -> None:
        missing_path = tmp_path / "missing-model.pt"
        backend = ReferenceBackend(checkpoint_path=str(missing_path))

        response = backend.forecast(DynamicsRequest(seed=42042))

        assert response.backend_id == "reference"
        assert "checkpoint" in response.state_delta_summary["error"]

    def test_returns_checkpoint_error_before_torch_error(
        self, tmp_path: Path, monkeypatch
    ) -> None:
        monkeypatch.delenv("VEI_REFERENCE_BACKEND_CHECKPOINT", raising=False)
        missing_path = tmp_path / "missing-model.pt"
        monkeypatch.setattr(
            "vei.dynamics.backends.reference.run_branch_point_benchmark_prediction",
            lambda **_kwargs: (_ for _ in ()).throw(
                AssertionError("bridge should not run")
            ),
        )
        backend = ReferenceBackend(checkpoint_path=str(missing_path))

        response = backend.forecast(DynamicsRequest(seed=42042))

        assert response.backend_id == "reference"
        assert "checkpoint" in response.state_delta_summary["error"]

    def test_surfaces_bridge_runtime_error_when_checkpoint_is_configured(
        self,
        tmp_path: Path,
        monkeypatch,
    ) -> None:
        checkpoint_path = tmp_path / "reference-model.pt"
        checkpoint_path.write_bytes(b"placeholder")
        monkeypatch.setattr(
            "vei.dynamics.backends.reference.run_branch_point_benchmark_prediction",
            lambda **_kwargs: (_ for _ in ()).throw(
                RuntimeError("runtime unavailable")
            ),
        )
        backend = ReferenceBackend(checkpoint_path=str(checkpoint_path))

        response = backend.forecast(DynamicsRequest(seed=42042))

        assert response.backend_id == "reference"
        assert "runtime unavailable" in response.state_delta_summary["error"]

    def test_loads_checkpoint_and_predicts(self, tmp_path: Path, monkeypatch) -> None:
        checkpoint_path = tmp_path / "reference-model.pt"
        checkpoint_path.write_bytes(b"placeholder")
        monkeypatch.setattr(
            "vei.dynamics.backends.reference.run_branch_point_benchmark_prediction",
            _reference_prediction_stub,
        )
        backend = ReferenceBackend(checkpoint_path=str(checkpoint_path))
        request = DynamicsRequest(
            recent_events=[
                build_event(
                    domain=EventDomain.COMM_GRAPH,
                    kind="mail.received",
                    ts_ms=1_000,
                    actor_ref=ActorRef(actor_id="vendor@example.com"),
                    delta_data={
                        "target": "mail",
                        "from": "vendor@example.com",
                        "to": ["me@example"],
                        "subj": "Quote",
                        "body_text": "Please review with legal.",
                    },
                )
            ],
        )

        response = backend.forecast(request)

        assert response.backend_id == "reference"
        assert response.state_delta_summary["model_id"] == "ft_transformer"
        assert "evidence_heads" in response.state_delta_summary
        assert response.predicted_events
