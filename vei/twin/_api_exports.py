from __future__ import annotations

from importlib import import_module
from typing import Any

_LAZY_MODEL_EXPORTS = frozenset(
    {
        "ExternalAgentIdentity",
        "TwinActivityItem",
        "TwinLaunchManifest",
        "TwinLaunchRuntime",
        "TwinLaunchSnippet",
        "TwinLaunchStatus",
        "TwinOutcomeSummary",
        "TwinServiceName",
        "TwinServiceRecord",
        "TwinServiceState",
        "TwinVariantSpec",
    }
)


def load_api_export(name: str) -> Any:
    if name in _LAZY_MODEL_EXPORTS:
        return getattr(import_module("vei.twin.models"), name)
    raise AttributeError(f"module 'vei.twin.api' has no attribute {name!r}")
