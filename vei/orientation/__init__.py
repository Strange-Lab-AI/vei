from __future__ import annotations

from importlib import import_module

__all__ = [
    "OrientationObject",
    "OrientationPolicyHint",
    "WorldOrientation",
    "build_world_orientation",
]


def __getattr__(name: str):  # pragma: no cover - thin import facade
    if name == "build_world_orientation":
        module = import_module("vei.orientation.api")
        return getattr(module, name)
    module = import_module("vei.orientation.models")
    if hasattr(module, name):
        return getattr(module, name)
    raise AttributeError(name)
