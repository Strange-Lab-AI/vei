from __future__ import annotations

from importlib import import_module

__all__ = [
    "build_blueprint_asset_for_example",
    "build_blueprint_asset_for_family",
    "build_blueprint_asset_for_scenario",
    "build_blueprint_for_family",
    "build_blueprint_for_scenario",
    "compile_blueprint",
    "create_world_session_from_blueprint",
    "get_facade_manifest",
    "list_blueprint_builder_examples",
    "list_blueprint_specs",
    "list_facade_manifest",
    "get_facade_plugin",
    "list_facade_plugins",
    "BlueprintAsset",
    "BlueprintContractSummary",
    "BlueprintContractDefaults",
    "BlueprintCapabilityGraphsAsset",
    "BlueprintCommGraphAsset",
    "BlueprintEnvironmentAsset",
    "BlueprintEnvironmentSummary",
    "BlueprintDocGraphAsset",
    "BlueprintIdentityGraphAsset",
    "BlueprintIdentityPolicyAsset",
    "BlueprintRevenueGraphAsset",
    "BlueprintRunDefaults",
    "BlueprintScenarioSummary",
    "BlueprintSpec",
    "BlueprintWorkGraphAsset",
    "CapabilityGraphSummary",
    "BlueprintWorkflowDefaults",
    "CompiledBlueprint",
    "FacadeManifest",
    "FacadePlugin",
]


def __getattr__(name: str):  # pragma: no cover - thin import facade
    if name in {
        "build_blueprint_asset_for_family",
        "build_blueprint_asset_for_example",
        "build_blueprint_asset_for_scenario",
        "build_blueprint_for_family",
        "build_blueprint_for_scenario",
        "compile_blueprint",
        "create_world_session_from_blueprint",
        "get_facade_manifest",
        "list_blueprint_builder_examples",
        "list_blueprint_specs",
        "list_facade_manifest",
    }:
        module = import_module("vei.blueprint.api")
        return getattr(module, name)
    if name in {
        "BlueprintAsset",
        "BlueprintCapabilityGraphsAsset",
        "BlueprintCommGraphAsset",
        "BlueprintContractSummary",
        "BlueprintContractDefaults",
        "BlueprintDocGraphAsset",
        "BlueprintIdentityGraphAsset",
        "BlueprintIdentityPolicyAsset",
        "BlueprintRevenueGraphAsset",
        "BlueprintRunDefaults",
        "BlueprintScenarioSummary",
        "BlueprintSpec",
        "BlueprintWorkGraphAsset",
        "CapabilityGraphSummary",
        "BlueprintWorkflowDefaults",
        "CompiledBlueprint",
        "BlueprintEnvironmentAsset",
        "BlueprintEnvironmentSummary",
        "FacadeManifest",
    }:
        module = import_module("vei.blueprint.models")
        return getattr(module, name)
    if name in {"FacadePlugin", "get_facade_plugin", "list_facade_plugins"}:
        module = import_module("vei.blueprint.plugins")
        return getattr(module, name)
    raise AttributeError(name)
