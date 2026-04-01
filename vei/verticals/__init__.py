from importlib import import_module
from typing import Any

from .packs import (
    VerticalPackDefinition,
    VerticalPackManifest,
    build_vertical_blueprint_asset,
    get_vertical_pack_manifest,
    list_vertical_pack_manifests,
    list_vertical_pack_names,
)
from .scenario_variants import (
    VerticalScenarioVariantSpec,
    default_vertical_scenario_variant,
    get_vertical_scenario_variant,
    list_vertical_scenario_variants,
)
from .contract_variants import (
    VerticalContractVariantSpec,
    apply_vertical_contract_variant,
    default_vertical_contract_variant,
    get_vertical_contract_variant,
    list_vertical_contract_variants,
)
from .faults import FaultOverlaySpec

__all__ = [
    "VerticalPackDefinition",
    "VerticalPackManifest",
    "build_vertical_blueprint_asset",
    "get_vertical_pack_manifest",
    "list_vertical_pack_manifests",
    "list_vertical_pack_names",
    "VerticalScenarioVariantSpec",
    "default_vertical_scenario_variant",
    "get_vertical_scenario_variant",
    "list_vertical_scenario_variants",
    "VerticalContractVariantSpec",
    "apply_vertical_contract_variant",
    "default_vertical_contract_variant",
    "get_vertical_contract_variant",
    "list_vertical_contract_variants",
    "FaultOverlaySpec",
    "VerticalDemoResult",
    "VerticalDemoSpec",
    "VerticalShowcaseResult",
    "VerticalShowcaseSpec",
    "VerticalVariantMatrixResult",
    "VerticalVariantMatrixSpec",
    "apply_fault_overlays",
    "load_workspace_exports_preview",
    "load_workspace_presentation",
    "load_workspace_story_manifest",
    "overlay_summaries",
    "prepare_vertical_demo",
    "prepare_vertical_story",
    "run_vertical_showcase",
    "run_vertical_variant_matrix",
]


def __getattr__(name: str) -> Any:  # pragma: no cover - thin import facade
    if name in {"apply_fault_overlays", "overlay_summaries"}:
        module = import_module("vei.verticals.faults")
        return getattr(module, name)
    if name in {
        "VerticalDemoResult",
        "VerticalDemoSpec",
        "VerticalShowcaseResult",
        "VerticalShowcaseSpec",
        "VerticalVariantMatrixResult",
        "VerticalVariantMatrixSpec",
        "load_workspace_exports_preview",
        "load_workspace_presentation",
        "load_workspace_story_manifest",
        "prepare_vertical_demo",
        "prepare_vertical_story",
        "run_vertical_showcase",
        "run_vertical_variant_matrix",
    }:
        module = import_module("vei.verticals.demo")
        return getattr(module, name)
    raise AttributeError(name)
