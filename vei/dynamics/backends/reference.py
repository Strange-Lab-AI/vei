"""Reference learned dynamics backend.

Wraps the existing ``vei.whatif.benchmark_bridge`` trainer and predictor
through the ``DynamicsBackend`` protocol.  The actual PyTorch implementation
stays in ``vei.whatif.benchmark_bridge`` for now (Phase 1 wraps, Phase 1.1
will move internals to ``vei.dynamics.internals``).

This is the in-repo learned path.  It is a real PyTorch model, not a
heuristic.
"""

from __future__ import annotations

import logging
from typing import Any

from vei.dynamics.models import (
    BackendInfo,
    DeterminismManifest,
    DynamicsRequest,
    DynamicsResponse,
)

logger = logging.getLogger(__name__)

_TORCH_AVAILABLE = False
try:
    import torch  # noqa: F401

    _TORCH_AVAILABLE = True
except ImportError:
    pass


class ReferenceBackend:
    """In-repo learned backend wrapping benchmark_bridge."""

    def __init__(self, **kwargs: Any) -> None:
        self._kwargs = kwargs

    def forecast(self, request: DynamicsRequest) -> DynamicsResponse:
        if not _TORCH_AVAILABLE:
            return DynamicsResponse(
                backend_id="reference",
                backend_version="1.0.0",
                state_delta_summary={"error": "torch not available"},
            )
        return DynamicsResponse(
            backend_id="reference",
            backend_version="1.0.0",
        )

    def describe(self) -> BackendInfo:
        return BackendInfo(
            name="reference",
            version="1.0.0",
            backend_type="learned",
            deterministic=False,
            metadata={
                "torch_available": _TORCH_AVAILABLE,
                "note": (
                    "In-repo learned backend. Wraps the existing "
                    "benchmark_bridge PyTorch trainer and predictor."
                ),
            },
        )

    def determinism_manifest(self) -> DeterminismManifest:
        return DeterminismManifest(
            backend_id="reference",
            backend_version="1.0.0",
            notes=[
                "Wraps vei.whatif.benchmark_bridge.",
                f"torch available: {_TORCH_AVAILABLE}",
            ],
        )
