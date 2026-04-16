from __future__ import annotations

from importlib import import_module

__all__ = [
    "CompatibilitySurfaceSpec",
    "ContextMoldConfig",
    "CustomerTwinBundle",
    "ExternalAgentIdentity",
    "TWIN_MANIFEST_FILE",
    "TwinCrisisLevel",
    "TwinDensityLevel",
    "TwinGatewayConfig",
    "TwinMatrixBundle",
    "WorkspaceGovernorStatus",
    "TwinTemplateSpec",
    "TwinRuntimeStatus",
    "TwinVariantSpec",
    "build_customer_twin",
    "build_customer_twin_asset",
    "build_twin_matrix",
    "create_twin_gateway_app",
    "load_customer_twin",
    "load_saved_governor_payload",
    "load_saved_workforce_payload",
    "load_twin_matrix",
    "serve_customer_twin",
]


def __getattr__(name: str):
    if name in {
        "TWIN_MANIFEST_FILE",
        "build_customer_twin",
        "build_customer_twin_asset",
        "build_twin_matrix",
        "create_twin_gateway_app",
        "load_customer_twin",
        "load_twin_matrix",
    }:
        module = import_module("vei.twin.api")
        return getattr(module, name)
    if name in {"load_saved_governor_payload", "load_saved_workforce_payload"}:
        module = import_module("vei.twin._saved_payloads")
        return getattr(module, name)
    if name == "serve_customer_twin":
        module = import_module("vei.twin.app")
        return getattr(module, name)
    if name in {
        "CompatibilitySurfaceSpec",
        "ContextMoldConfig",
        "CustomerTwinBundle",
        "ExternalAgentIdentity",
        "TwinCrisisLevel",
        "TwinDensityLevel",
        "TwinGatewayConfig",
        "TwinMatrixBundle",
        "TwinRuntimeStatus",
        "TwinTemplateSpec",
        "TwinVariantSpec",
        "WorkspaceGovernorStatus",
    }:
        module = import_module("vei.twin.models")
        return getattr(module, name)
    raise AttributeError(name)
