"""Public API for vei.dynamics.

All VEI modules that need forecast / learned-dynamics capabilities must go
through ``get_backend(name)``.  Direct imports of model code are forbidden
outside ``vei.dynamics``.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, Protocol, runtime_checkable

from .models import (
    BackendInfo,
    BusinessHeads,
    CalibrationMetrics,
    DeterminismManifest,
    DynamicsRequest,
    DynamicsResponse,
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Protocol
# ---------------------------------------------------------------------------


@runtime_checkable
class DynamicsBackend(Protocol):
    """The single interface for calling into learned enterprise dynamics.

    Implementations live under ``vei.dynamics.backends.*``.  External
    processes (e.g. ARP_Jepa_exp) plug in via ``ExternalSubprocessBackend``.
    """

    def forecast(self, request: DynamicsRequest) -> DynamicsResponse: ...

    def describe(self) -> BackendInfo: ...

    def determinism_manifest(self) -> DeterminismManifest: ...


# ---------------------------------------------------------------------------
# Registry
# ---------------------------------------------------------------------------

_BackendFactory = type  # callable() -> DynamicsBackend

_registry: Dict[str, _BackendFactory] = {}
_instances: Dict[str, DynamicsBackend] = {}


def register_backend(name: str, factory: _BackendFactory) -> None:
    """Register a backend factory under a name."""
    _registry[name] = factory
    _instances.pop(name, None)
    logger.info("dynamics_backend_registered", extra={"backend": name})


def ensure_builtin_backends_registered() -> None:
    """Re-register built-in backends after tests or helpers clear the registry."""
    _auto_register()


def get_backend(name: str, **kwargs: Any) -> DynamicsBackend:
    """Get (or create) a named backend instance."""
    if name in _instances:
        return _instances[name]
    factory = _registry.get(name)
    if factory is None:
        ensure_builtin_backends_registered()
        factory = _registry.get(name)
    if factory is None:
        available = sorted(_registry.keys())
        raise KeyError(
            f"No dynamics backend registered as {name!r}. " f"Available: {available}"
        )
    instance = factory(**kwargs)
    _instances[name] = instance
    return instance


def list_backends() -> Dict[str, BackendInfo]:
    """Return info for all registered backends."""
    result: Dict[str, BackendInfo] = {}
    for name in sorted(_registry.keys()):
        backend = get_backend(name)
        result[name] = backend.describe()
    return result


def reset_registry() -> None:
    """Clear all registrations (test helper)."""
    _registry.clear()
    _instances.clear()


# ---------------------------------------------------------------------------
# Auto-register built-in backends on import
# ---------------------------------------------------------------------------


def _auto_register() -> None:
    from vei.dynamics.backends.null import NullBackend

    register_backend("null", NullBackend)

    try:
        from vei.dynamics.backends.heuristic import HeuristicBaseline

        register_backend("heuristic_baseline", HeuristicBaseline)
        register_backend("e_jepa_proxy", HeuristicBaseline)
    except Exception:
        pass

    try:
        from vei.dynamics.backends.reference import ReferenceBackend

        register_backend("reference", ReferenceBackend)
    except Exception:
        pass


_auto_register()


__all__ = [
    "BackendInfo",
    "BusinessHeads",
    "CalibrationMetrics",
    "DeterminismManifest",
    "DynamicsBackend",
    "DynamicsRequest",
    "DynamicsResponse",
    "ensure_builtin_backends_registered",
    "get_backend",
    "list_backends",
    "register_backend",
    "reset_registry",
]
