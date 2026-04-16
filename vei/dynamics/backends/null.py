"""Null dynamics backend — empty predictions for CI, smoke, and safety."""

from __future__ import annotations

from vei.dynamics.models import (
    BackendInfo,
    DeterminismManifest,
    DynamicsRequest,
    DynamicsResponse,
)


class NullBackend:
    """Returns empty predictions.  Used as the default in CI and smoke tests."""

    def forecast(self, request: DynamicsRequest) -> DynamicsResponse:
        return DynamicsResponse(
            backend_id="null",
            backend_version="1.0.0",
        )

    def describe(self) -> BackendInfo:
        return BackendInfo(
            name="null",
            version="1.0.0",
            backend_type="null",
            deterministic=True,
        )

    def determinism_manifest(self) -> DeterminismManifest:
        return DeterminismManifest(
            backend_id="null",
            backend_version="1.0.0",
        )
