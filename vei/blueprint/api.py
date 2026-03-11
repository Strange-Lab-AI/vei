from __future__ import annotations

from typing import Any, Dict, List, Optional

from vei.benchmark.families import (
    get_benchmark_family_manifest,
    list_benchmark_family_manifest,
)
from vei.benchmark.workflows import (
    get_benchmark_family_workflow_spec,
    get_benchmark_family_workflow_variant,
    resolve_benchmark_workflow_name,
)
from vei.contract.api import build_contract_from_workflow
from vei.scenario_engine.api import compile_workflow
from vei.world.manifest import get_scenario_manifest

from .models import (
    BlueprintAsset,
    BlueprintContractDefaults,
    BlueprintContractSummary,
    BlueprintRunDefaults,
    BlueprintScenarioSummary,
    BlueprintSpec,
    BlueprintWorkflowDefaults,
    CompiledBlueprint,
    FacadeManifest,
)
from .plugins import (
    get_facade_plugin,
    list_facade_plugins,
    resolve_facade_plugins_for_tool_families,
)


def get_facade_manifest(name: str) -> FacadeManifest:
    return get_facade_plugin(name).manifest


def list_facade_manifest() -> List[FacadeManifest]:
    return [plugin.manifest for plugin in list_facade_plugins()]


def build_blueprint_asset_for_family(
    family_name: str,
    *,
    variant_name: Optional[str] = None,
) -> BlueprintAsset:
    family = get_benchmark_family_manifest(family_name)
    workflow_name = family.workflow_name
    if workflow_name is None:
        raise ValueError(f"benchmark family {family_name} has no workflow")
    workflow_variant = variant_name or family.primary_workflow_variant
    scenario_name = family.scenario_names[0]
    if workflow_variant is not None:
        scenario_name = get_benchmark_family_workflow_variant(
            workflow_name, workflow_variant
        ).scenario_name
    return BlueprintAsset(
        name=f"{family.name}.blueprint",
        title=family.title,
        description=family.description,
        scenario_name=scenario_name,
        family_name=family.name,
        workflow_name=workflow_name,
        workflow_variant=workflow_variant,
        metadata={
            "primary_dimensions": list(family.primary_dimensions),
            "family_tags": list(family.tags),
        },
    )


def build_blueprint_asset_for_scenario(
    scenario_name: str,
    *,
    family_name: Optional[str] = None,
    workflow_name: Optional[str] = None,
    workflow_variant: Optional[str] = None,
    title: Optional[str] = None,
    description: Optional[str] = None,
    requested_facades: Optional[List[str]] = None,
    metadata: Optional[dict] = None,
) -> BlueprintAsset:
    scenario = get_scenario_manifest(scenario_name)
    resolved_family_name = family_name or scenario.benchmark_family
    return BlueprintAsset(
        name=f"{scenario.name}.blueprint",
        title=title or scenario.name.replace("_", " ").title(),
        description=description or f"Compiled blueprint for scenario {scenario.name}.",
        scenario_name=scenario.name,
        family_name=resolved_family_name,
        workflow_name=workflow_name,
        workflow_variant=workflow_variant,
        requested_facades=list(requested_facades or []),
        metadata=dict(metadata or {}),
    )


def compile_blueprint(asset: BlueprintAsset) -> CompiledBlueprint:
    scenario = get_scenario_manifest(asset.scenario_name)
    resolved_family_name = asset.family_name or scenario.benchmark_family
    resolved_workflow_name = asset.workflow_name or resolve_benchmark_workflow_name(
        family_name=resolved_family_name,
        scenario_name=scenario.name,
    )
    workflow_variant = asset.workflow_variant

    scenario_summary = BlueprintScenarioSummary(
        name=scenario.name,
        difficulty=scenario.difficulty,
        benchmark_family=resolved_family_name,
        tool_families=list(scenario.tool_families),
        expected_steps_min=scenario.expected_steps_min,
        expected_steps_max=scenario.expected_steps_max,
        tags=list(scenario.tags),
    )

    contract_summary: Optional[BlueprintContractSummary] = None
    workflow_defaults = BlueprintWorkflowDefaults(
        workflow_name=resolved_workflow_name,
        workflow_variant=workflow_variant,
        expected_steps_min=scenario.expected_steps_min,
        expected_steps_max=scenario.expected_steps_max,
    )
    contract_defaults = BlueprintContractDefaults()
    workflow_tool_families: List[str] = []
    if resolved_workflow_name:
        workflow_spec = get_benchmark_family_workflow_spec(
            resolved_workflow_name, variant_name=workflow_variant
        )
        compiled = compile_workflow(workflow_spec)
        contract = build_contract_from_workflow(compiled)
        contract_summary = BlueprintContractSummary(
            name=contract.name,
            workflow_name=contract.workflow_name,
            success_predicate_count=len(contract.success_predicates),
            forbidden_predicate_count=len(contract.forbidden_predicates),
            policy_invariant_count=len(contract.policy_invariants),
            intervention_rule_count=len(contract.intervention_rules),
            observation_focus_hints=list(contract.observation_boundary.focus_hints),
            hidden_state_fields=list(contract.observation_boundary.hidden_state_fields),
        )
        workflow_defaults.allowed_tools = list(
            contract.observation_boundary.allowed_tools
        )
        workflow_defaults.focus_hints = [
            item
            for item in contract.observation_boundary.focus_hints
            if item and item != "summary"
        ]
        workflow_tool_families = sorted(
            {step.tool.split(".", 1)[0].strip().lower() for step in compiled.steps}
        )
        contract_defaults = BlueprintContractDefaults(
            contract_name=contract.name,
            success_predicate_count=len(contract.success_predicates),
            forbidden_predicate_count=len(contract.forbidden_predicates),
            policy_invariant_count=len(contract.policy_invariants),
            intervention_rule_count=len(contract.intervention_rules),
            hidden_state_fields=list(contract.observation_boundary.hidden_state_fields),
            observation_focus_hints=list(contract.observation_boundary.focus_hints),
        )

    facade_names = _resolve_facade_names(
        requested_facades=asset.requested_facades,
        tool_families=scenario_summary.tool_families + workflow_tool_families,
    )
    facade_plugins = [get_facade_plugin(name) for name in facade_names]
    facades = [plugin.manifest for plugin in facade_plugins]

    metadata: Dict[str, Any] = dict(asset.metadata)
    metadata.update(
        {
            "resolved_tool_families": sorted(
                set(scenario_summary.tool_families + workflow_tool_families)
            ),
            "compiled_from_asset": asset.name,
        }
    )
    state_roots = sorted(
        {
            root
            for plugin in facade_plugins
            for root in plugin.manifest.state_roots
            if root
        }
    )
    surfaces = sorted(
        {
            surface
            for plugin in facade_plugins
            for surface in plugin.manifest.surfaces
            if surface
        }
    )
    capability_domains = sorted(
        {plugin.manifest.domain for plugin in facade_plugins if plugin.manifest.domain}
    )
    scenario_seed_fields = sorted(
        {
            field_name
            for plugin in facade_plugins
            for field_name in plugin.scenario_seed_fields
            if field_name
        }
    )
    focus_hints = workflow_defaults.focus_hints or [
        plugin.manifest.name for plugin in facade_plugins
    ]
    inspection_focuses = sorted(
        {focus for plugin in facade_plugins for focus in plugin.focuses if focus}
    )
    if not inspection_focuses:
        inspection_focuses = ["browser"]
    run_defaults = BlueprintRunDefaults(
        scenario_name=scenario.name,
        benchmark_family=resolved_family_name,
        inspection_focus=(
            focus_hints[0]
            if focus_hints
            else (inspection_focuses[0] if inspection_focuses else "browser")
        ),
        inspection_focuses=sorted(set(focus_hints + inspection_focuses)),
        suggested_branch_prefix=resolved_family_name or scenario.name,
    )

    return CompiledBlueprint(
        name=asset.name,
        title=asset.title,
        description=asset.description,
        family_name=resolved_family_name,
        workflow_name=resolved_workflow_name,
        workflow_variant=workflow_variant,
        scenario=scenario_summary,
        contract=contract_summary,
        capability_domains=capability_domains,
        facades=facades,
        state_roots=state_roots,
        surfaces=surfaces,
        metadata=metadata,
        asset=asset,
        scenario_seed_fields=scenario_seed_fields,
        workflow_defaults=workflow_defaults,
        contract_defaults=contract_defaults,
        run_defaults=run_defaults,
    )


def build_blueprint_for_family(
    family_name: str,
    *,
    variant_name: Optional[str] = None,
) -> CompiledBlueprint:
    asset = build_blueprint_asset_for_family(family_name, variant_name=variant_name)
    return compile_blueprint(asset)


def build_blueprint_for_scenario(
    scenario_name: str,
    *,
    family_name: Optional[str] = None,
    workflow_name: Optional[str] = None,
    workflow_variant: Optional[str] = None,
    title: Optional[str] = None,
    description: Optional[str] = None,
    requested_facades: Optional[List[str]] = None,
    metadata: Optional[dict] = None,
) -> CompiledBlueprint:
    asset = build_blueprint_asset_for_scenario(
        scenario_name,
        family_name=family_name,
        workflow_name=workflow_name,
        workflow_variant=workflow_variant,
        title=title,
        description=description,
        requested_facades=requested_facades,
        metadata=metadata,
    )
    return compile_blueprint(asset)


def list_blueprint_specs() -> List[BlueprintSpec]:
    return [
        build_blueprint_for_family(item.name)
        for item in list_benchmark_family_manifest()
        if item.workflow_name is not None
    ]


def _resolve_facade_names(
    *,
    requested_facades: List[str],
    tool_families: List[str],
) -> List[str]:
    resolved: List[str] = []
    seen: set[str] = set()
    for plugin in resolve_facade_plugins_for_tool_families(tool_families):
        if plugin.manifest.name not in seen:
            resolved.append(plugin.manifest.name)
            seen.add(plugin.manifest.name)
    for requested in requested_facades:
        key = requested.strip().lower()
        try:
            plugin = get_facade_plugin(key)
        except KeyError as exc:
            raise ValueError(f"unknown requested facade: {requested}") from exc
        if plugin.manifest.name not in seen:
            resolved.append(plugin.manifest.name)
            seen.add(plugin.manifest.name)
    if not resolved:
        raise ValueError("compiled blueprint resolved no facades")
    return sorted(resolved)


__all__ = [
    "BlueprintAsset",
    "CompiledBlueprint",
    "build_blueprint_asset_for_family",
    "build_blueprint_asset_for_scenario",
    "build_blueprint_for_family",
    "build_blueprint_for_scenario",
    "compile_blueprint",
    "get_facade_manifest",
    "list_blueprint_specs",
    "list_facade_manifest",
]
