from __future__ import annotations

from importlib import import_module

__all__ = [
    "GroundingBundleManifest",
    "IdentityGovernanceBundle",
    "IdentityGovernanceWorkflowSeed",
    "build_grounding_bundle_example",
    "compile_identity_governance_bundle",
    "list_grounding_bundle_examples",
]


def __getattr__(name: str):  # pragma: no cover - import facade
    if name in {
        "build_grounding_bundle_example",
        "compile_identity_governance_bundle",
        "list_grounding_bundle_examples",
    }:
        module = import_module("vei.grounding.api")
        return getattr(module, name)
    if name in {
        "GroundingBundleManifest",
        "IdentityGovernanceBundle",
        "IdentityGovernanceWorkflowSeed",
    }:
        module = import_module("vei.grounding.models")
        return getattr(module, name)
    raise AttributeError(name)
