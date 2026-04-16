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
    def test_returns_explicit_error_without_checkpoint(self) -> None:
        backend = ReferenceBackend()

        response = backend.forecast(DynamicsRequest(seed=42042))

        assert response.backend_id == "reference"
        assert "checkpoint" in response.state_delta_summary["error"]

    def test_returns_checkpoint_error_before_torch_error(self, monkeypatch) -> None:
        monkeypatch.delenv("VEI_REFERENCE_BACKEND_CHECKPOINT", raising=False)
        monkeypatch.setattr("vei.dynamics.backends.reference._TORCH_AVAILABLE", False)
        backend = ReferenceBackend()

        response = backend.forecast(DynamicsRequest(seed=42042))

        assert response.backend_id == "reference"
        assert "checkpoint" in response.state_delta_summary["error"]

    def test_returns_torch_error_when_checkpoint_is_configured(
        self,
        tmp_path: Path,
        monkeypatch,
    ) -> None:
        checkpoint_path = tmp_path / "reference-model.pt"
        checkpoint_path.write_bytes(b"placeholder")
        monkeypatch.setattr("vei.dynamics.backends.reference._TORCH_AVAILABLE", False)
        backend = ReferenceBackend(checkpoint_path=str(checkpoint_path))

        response = backend.forecast(DynamicsRequest(seed=42042))

        assert response.backend_id == "reference"
        assert response.state_delta_summary["error"] == "torch not available"

    def test_loads_checkpoint_and_predicts(self, tmp_path: Path) -> None:
        torch = pytest.importorskip("torch")
        from vei.whatif.benchmark_bridge import _BenchmarkPreprocessor, _TorchTrainer

        torch.manual_seed(42042)
        preprocessor = _BenchmarkPreprocessor(
            summary_feature_names=["history_event_count", "participant_count"],
            summary_mean=[0.0, 0.0],
            summary_std=[1.0, 1.0],
            action_tag_names=["hold", "legal"],
            event_type_names=["__summary__", "mail.received", "mail.send"],
            target_mean=[0.0] * 22,
            target_std=[1.0] * 22,
        )
        trainer = _TorchTrainer(model_id="ft_transformer", preprocessor=preprocessor)
        model = trainer.build_model(device="cpu")
        checkpoint_path = tmp_path / "reference-model.pt"
        torch.save(
            {
                "state_dict": model.state_dict(),
                "metadata": preprocessor.to_metadata(),
                "model_id": "ft_transformer",
            },
            checkpoint_path,
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
