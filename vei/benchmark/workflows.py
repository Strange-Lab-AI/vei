from __future__ import annotations

import json
from typing import Any, Dict, List, Optional

from vei.benchmark.models import (
    BenchmarkWorkflowParameter,
    BenchmarkWorkflowVariantManifest,
)
from vei.benchmark.workflow_catalog import (
    _PARAMETER_DESCRIPTIONS,
    _SCENARIO_TO_WORKFLOW,
    _VARIANT_CATALOG,
    _VariantDefinition,
)
from vei.scenario_engine.models import WorkflowScenarioSpec

from .workflow_specs import (
    _build_b2b_saas_spec,
    _build_digital_marketing_agency_spec,
    _build_enterprise_onboarding_spec,
    _build_identity_access_governance_spec,
    _build_knowledge_authoring_spec,
    _build_real_estate_management_spec,
    _build_revenue_incident_spec,
    _build_security_containment_spec,
    _build_service_ops_spec,
    _build_storage_solutions_spec,
)

_WORKFLOW_BUILDERS = {
    "security_containment": _build_security_containment_spec,
    "enterprise_onboarding_migration": _build_enterprise_onboarding_spec,
    "revenue_incident_mitigation": _build_revenue_incident_spec,
    "identity_access_governance": _build_identity_access_governance_spec,
    "real_estate_management": _build_real_estate_management_spec,
    "digital_marketing_agency": _build_digital_marketing_agency_spec,
    "storage_solutions": _build_storage_solutions_spec,
    "b2b_saas": _build_b2b_saas_spec,
    "service_ops": _build_service_ops_spec,
    "knowledge_authoring": _build_knowledge_authoring_spec,
}


def _parameter_value_type(value: str | int | float | bool) -> str:
    if isinstance(value, bool):
        return "bool"
    if isinstance(value, int):
        return "int"
    if isinstance(value, float):
        return "float"
    return "str"


def _parameter_manifest_value(value: Any) -> tuple[str | int | float | bool, str]:
    if isinstance(value, (str, int, float, bool)):
        return value, _parameter_value_type(value)
    return json.dumps(value, sort_keys=True), "str"


def _resolve_variant_metadata(
    family_name: str, definition: _VariantDefinition
) -> tuple[str, str]:
    """Return (title, description), falling back to the vertical variant."""
    if definition.title and definition.description:
        return definition.title, definition.description
    try:
        from vei.verticals import get_vertical_scenario_variant

        vsv = get_vertical_scenario_variant(family_name, definition.name)
        title = definition.title or vsv.title
        desc = definition.description or vsv.description
        return title, desc
    except (KeyError, ImportError):
        return definition.title or definition.name, definition.description or ""


def _variant_manifest(
    family_name: str, definition: _VariantDefinition
) -> BenchmarkWorkflowVariantManifest:
    descriptions = _PARAMETER_DESCRIPTIONS[family_name]
    parameters = [
        (
            lambda manifest_value, manifest_type: BenchmarkWorkflowParameter(
                name=name,
                value=manifest_value,
                value_type=manifest_type,
                description=descriptions.get(name),
            )
        )(*_parameter_manifest_value(value))
        for name, value in definition.parameters.model_dump(mode="python").items()
    ]
    title, desc = _resolve_variant_metadata(family_name, definition)
    return BenchmarkWorkflowVariantManifest(
        family_name=family_name,
        workflow_name=family_name,
        variant_name=definition.name,
        title=title,
        description=desc,
        scenario_name=definition.scenario_name,
        parameters=parameters,
    )


def _resolve_variant_name(family_name: str, variant_name: Optional[str]) -> str:
    catalog = _VARIANT_CATALOG[family_name]
    if variant_name is None:
        return next(iter(catalog))
    key = variant_name.strip().lower()
    if key not in catalog:
        raise KeyError(f"unknown workflow variant for {family_name}: {variant_name}")
    return key


def get_benchmark_family_workflow_spec(
    name: str,
    variant_name: Optional[str] = None,
    parameter_overrides: Optional[Dict[str, Any]] = None,
) -> WorkflowScenarioSpec:
    key = name.strip().lower()
    if key not in _VARIANT_CATALOG:
        raise KeyError(f"unknown benchmark family workflow: {name}")
    resolved_variant = _resolve_variant_name(key, variant_name)
    definition = _VARIANT_CATALOG[key][resolved_variant]
    builder = _WORKFLOW_BUILDERS[key]
    params = definition.parameters.model_copy(deep=True)
    if parameter_overrides:
        params = params.model_copy(update=dict(parameter_overrides))
    return builder(params, variant_name=resolved_variant)


def list_benchmark_family_workflow_specs() -> List[WorkflowScenarioSpec]:
    return [
        get_benchmark_family_workflow_spec(name) for name in sorted(_VARIANT_CATALOG)
    ]


def get_benchmark_family_workflow_variant(
    family_name: str, variant_name: str
) -> BenchmarkWorkflowVariantManifest:
    key = family_name.strip().lower()
    if key not in _VARIANT_CATALOG:
        raise KeyError(f"unknown benchmark family workflow: {family_name}")
    resolved_variant = _resolve_variant_name(key, variant_name)
    return _variant_manifest(key, _VARIANT_CATALOG[key][resolved_variant])


def list_benchmark_family_workflow_variants(
    family_name: Optional[str] = None,
) -> List[BenchmarkWorkflowVariantManifest]:
    family_names = (
        [family_name.strip().lower()]
        if family_name is not None
        else sorted(_VARIANT_CATALOG)
    )
    variants: List[BenchmarkWorkflowVariantManifest] = []
    for key in family_names:
        if key not in _VARIANT_CATALOG:
            raise KeyError(f"unknown benchmark family workflow: {family_name}")
        for variant_name in _VARIANT_CATALOG[key]:
            variants.append(_variant_manifest(key, _VARIANT_CATALOG[key][variant_name]))
    return variants


def resolve_benchmark_workflow_name(
    *,
    family_name: Optional[str] = None,
    scenario_name: Optional[str] = None,
) -> Optional[str]:
    if family_name:
        key = family_name.strip().lower()
        return key if key in _VARIANT_CATALOG else None
    if scenario_name:
        return _SCENARIO_TO_WORKFLOW.get(scenario_name.strip())
    return None


__all__ = [
    "BenchmarkWorkflowVariantManifest",
    "get_benchmark_family_workflow_spec",
    "get_benchmark_family_workflow_variant",
    "list_benchmark_family_workflow_specs",
    "list_benchmark_family_workflow_variants",
    "resolve_benchmark_workflow_name",
]
