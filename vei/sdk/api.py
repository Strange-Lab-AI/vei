from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, Optional, Protocol

from vei.capability_graph.models import (
    CapabilityGraphActionInput,
    CapabilityGraphActionResult,
    CapabilityGraphPlan,
    RuntimeCapabilityGraphs,
)
from vei.fidelity import (
    TwinFidelityReport,
    get_or_build_workspace_fidelity_report as _get_or_build_workspace_fidelity_report,
)
from vei.orientation.models import WorldOrientation
from vei.playable import (
    MissionCatalog,
    MissionSessionState,
    PlayableMissionSpec,
    PlayableShowcaseResult,
    activate_workspace_playable_mission as _activate_workspace_playable_mission,
    build_playable_mission_catalog as _build_playable_mission_catalog,
    export_mission_run as _export_mission_run,
    get_playable_mission as _get_playable_mission,
    list_playable_missions as _list_playable_missions,
    list_workspace_playable_missions as _list_workspace_playable_missions,
    load_workspace_mission_state as _load_workspace_mission_state,
    prepare_playable_workspace as _prepare_playable_workspace,
    run_playable_showcase as _run_playable_showcase,
    start_workspace_mission_run as _start_workspace_mission_run,
)
from vei.blueprint.api import (
    build_blueprint_asset_for_example as _build_blueprint_asset_for_example,
    build_blueprint_asset_for_family as _build_blueprint_asset_for_family,
    build_blueprint_asset_for_scenario as _build_blueprint_asset_for_scenario,
    build_blueprint_for_family as _build_blueprint_for_family,
    build_blueprint_for_scenario as _build_blueprint_for_scenario,
    compile_blueprint as _compile_blueprint,
    create_world_session_from_blueprint as _create_world_session_from_blueprint,
    get_facade_manifest as _get_facade_manifest,
    list_blueprint_builder_examples as _list_blueprint_builder_examples,
    list_blueprint_specs as _list_blueprint_specs,
    list_facade_manifest as _list_facade_manifest,
)
from vei.blueprint.models import (
    BlueprintAsset,
    BlueprintSpec,
    CompiledBlueprint,
    FacadeManifest,
)
from vei.blueprint import (
    FacadePlugin,
    get_facade_plugin as _get_facade_plugin,
    list_facade_plugins as _list_facade_plugins,
    register_facade_plugin as _register_facade_plugin,
)
from vei.corpus.api import CorpusBundle, GeneratedWorkflowSpec, generate_corpus
from vei.benchmark.api import (
    BenchmarkFamilyManifest,
    BenchmarkWorkflowVariantManifest,
    get_benchmark_family_manifest,
    list_benchmark_family_manifest,
)
from vei.contract.models import ContractSpec
from vei.benchmark import (
    BenchmarkShowcaseExample,
    get_showcase_example as _get_showcase_example,
    list_showcase_examples as _list_showcase_examples,
    get_benchmark_family_workflow_spec as _get_benchmark_family_workflow_spec,
    get_benchmark_family_workflow_variant as _get_benchmark_family_workflow_variant,
    list_benchmark_family_workflow_specs as _list_benchmark_family_workflow_specs,
    list_benchmark_family_workflow_variants as _list_benchmark_family_workflow_variants,
)
from vei.quality.api import QualityFilterReport, filter_workflow_corpus
from vei.release.api import (
    BenchmarkReleaseResult,
    DatasetReleaseResult,
    NightlyReleaseResult,
    build_release_version as _build_release_version,
    export_dataset_release,
    run_nightly_release,
    snapshot_benchmark_release,
)
from vei.router.api import RouterAPI, RouterToolProvider
from vei.scenario_engine.api import compile_workflow
from vei.scenario_engine.api import CompiledWorkflow
from vei.scenario_engine.models import WorkflowScenarioSpec
from vei.scenario_runner.api import run_workflow, validate_workflow
from vei.scenario_runner.models import ScenarioRunResult, ValidationReport
from vei.grounding.api import (
    build_grounding_bundle_example as _build_grounding_bundle_example,
    compile_identity_governance_bundle as _compile_identity_governance_bundle,
    list_grounding_bundle_examples as _list_grounding_bundle_examples,
)
from vei.grounding.models import GroundingBundleManifest, IdentityGovernanceBundle
from vei.imports.api import (
    bootstrap_contract_from_import_bundle as _bootstrap_contract_from_import_bundle,
    generate_identity_scenario_candidates as _generate_identity_scenario_candidates,
    get_import_package_example_path as _get_import_package_example_path,
    list_import_package_examples as _list_import_package_examples,
    load_import_package as _load_import_package,
    normalize_identity_import_package as _normalize_identity_import_package,
    review_import_package as _review_import_package,
    scaffold_mapping_override as _scaffold_mapping_override,
    validate_import_package as _validate_import_package,
)
from vei.imports import (
    OktaConnectorConfig,
    load_okta_connector_config as _load_okta_connector_config,
)
from vei.imports.models import (
    GeneratedScenarioCandidate,
    ImportPackage,
    ImportPackageArtifacts,
    ImportReview,
    MappingOverrideSpec,
    NormalizationReport,
    ProvenanceRecord,
)
from vei.run.api import (
    diff_run_snapshots as _diff_run_snapshots,
    get_run_capability_graphs as _get_run_capability_graphs,
    get_run_orientation as _get_run_orientation,
    launch_workspace_run as _launch_workspace_run,
    load_run_events_for_run as _load_run_events_for_run,
    list_run_manifests as _list_run_manifests,
    list_run_snapshots as _list_run_snapshots,
    load_run_manifest as _load_run_manifest,
    load_run_timeline as _load_run_timeline,
)
from vei.run.models import RunManifest, RunSnapshotRef, RunTimelineEvent
from vei.workspace.api import (
    activate_workspace_contract_variant as _activate_workspace_contract_variant,
    activate_workspace_scenario as _activate_workspace_scenario,
    activate_workspace_scenario_variant as _activate_workspace_scenario_variant,
    bootstrap_workspace_contract as _bootstrap_workspace_contract,
    compile_workspace as _compile_workspace,
    create_workspace_from_template as _create_workspace_from_template,
    create_workspace_scenario as _create_workspace_scenario,
    diff_workspace_contract as _diff_workspace_contract,
    generate_workspace_scenarios_from_import as _generate_workspace_scenarios_from_import,
    import_workspace as _import_workspace,
    list_workspace_source_syncs as _list_workspace_source_syncs,
    list_workspace_sources as _list_workspace_sources,
    list_workspace_contract_variants as _list_workspace_contract_variants,
    list_workspace_runs as _list_workspace_runs,
    list_workspace_scenario_variants as _list_workspace_scenario_variants,
    list_workspace_scenarios as _list_workspace_scenarios,
    load_workspace as _load_workspace,
    load_workspace_contract as _load_workspace_contract,
    load_workspace_generated_scenarios as _load_workspace_generated_scenarios,
    load_workspace_import_report as _load_workspace_import_report,
    load_workspace_import_review as _load_workspace_import_review,
    load_workspace_provenance as _load_workspace_provenance,
    preview_workspace_scenario as _preview_workspace_scenario,
    show_workspace as _show_workspace,
    sync_workspace_source as _sync_workspace_source,
    validate_workspace_contract as _validate_workspace_contract,
)
from vei.workspace import (
    build_identity_flow_summary as _build_identity_flow_summary,
    prepare_identity_workspace_flow as _prepare_identity_workspace_flow,
)
from vei.workspace.models import (
    WorkspaceIdentityFlowSummary,
    WorkspaceManifest,
    WorkspaceRunEntry,
    WorkspaceScenarioSpec,
    WorkspaceSourceConfig,
    WorkspaceSourceSyncRecord,
    WorkspaceSummary,
)
from vei.verticals import (
    VerticalContractVariantSpec,
    VerticalPackManifest,
    VerticalScenarioVariantSpec,
    get_vertical_contract_variant as _get_vertical_contract_variant,
    get_vertical_pack_manifest as _get_vertical_pack_manifest,
    get_vertical_scenario_variant as _get_vertical_scenario_variant,
    list_vertical_contract_variants as _list_vertical_contract_variants,
    list_vertical_pack_manifests as _list_vertical_pack_manifests,
    list_vertical_scenario_variants as _list_vertical_scenario_variants,
    VerticalDemoResult,
    VerticalDemoSpec,
    VerticalShowcaseResult,
    VerticalShowcaseSpec,
    VerticalVariantMatrixResult,
    VerticalVariantMatrixSpec,
    prepare_vertical_demo as _prepare_vertical_demo,
    run_vertical_showcase as _run_vertical_showcase,
    run_vertical_variant_matrix as _run_vertical_variant_matrix,
)
from vei.world.api import (
    WorldSessionAPI,
    create_world_session,
    get_catalog_scenario,
    get_catalog_scenario_manifest,
    list_catalog_scenario_manifest,
)
from vei.world import ScenarioManifest


@dataclass(frozen=True)
class SessionConfig:
    seed: int = 42042
    artifacts_dir: str | None = None
    connector_mode: str = "sim"
    scenario_name: str = "multi_channel"
    scenario: Any | None = None
    branch: str = "main"


class SessionHook(Protocol):
    """Optional callback hooks for SDK embedding telemetry and control."""

    def before_call(self, tool: str, args: Dict[str, Any]) -> None: ...

    def after_call(
        self, tool: str, args: Dict[str, Any], result: Dict[str, Any]
    ) -> None: ...


class EnterpriseSession:
    """Stable high-level embedding API for VEI office simulations."""

    def __init__(self, config: SessionConfig):
        self.config = config
        scenario_obj = config.scenario
        if scenario_obj is None and config.scenario_name:
            scenario_obj = get_catalog_scenario(config.scenario_name)
        self._world: WorldSessionAPI = create_world_session(
            seed=config.seed,
            artifacts_dir=config.artifacts_dir,
            scenario=scenario_obj,
            connector_mode=config.connector_mode,
            branch=config.branch,
        )
        self._hooks: list[SessionHook] = []

    @property
    def router(self) -> RouterAPI:
        return self._world.router

    def observe(self, focus_hint: str | None = None) -> Dict[str, Any]:
        return self._world.observe(focus_hint=focus_hint)

    def call_tool(
        self, tool: str, args: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        payload = dict(args or {})
        self._run_before_hooks(tool, payload)
        result = self._world.call_tool(tool, payload)
        self._run_after_hooks(tool, payload, result)
        return result

    def act_and_observe(
        self, tool: str, args: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        payload = dict(args or {})
        self._run_before_hooks(tool, payload)
        result = self._world.act_and_observe(tool, payload)
        self._run_after_hooks(tool, payload, result)
        return result

    def pending(self) -> Dict[str, int]:
        return self._world.pending()

    def capability_graphs(self) -> RuntimeCapabilityGraphs:
        return self._world.capability_graphs()

    def graph_plan(
        self, *, domain: str | None = None, limit: int = 12
    ) -> CapabilityGraphPlan:
        return self._world.graph_plan(domain=domain, limit=limit)

    def graph_action(
        self, action: CapabilityGraphActionInput | Dict[str, Any]
    ) -> CapabilityGraphActionResult:
        payload = (
            action
            if isinstance(action, CapabilityGraphActionInput)
            else CapabilityGraphActionInput.model_validate(action)
        )
        hook_payload = payload.model_dump(mode="json")
        self._run_before_hooks("vei.graph_action", hook_payload)
        result = self._world.graph_action(payload)
        self._run_after_hooks(
            "vei.graph_action", hook_payload, result.model_dump(mode="json")
        )
        return result

    def orientation(self) -> WorldOrientation:
        return self._world.orientation()

    def register_tool_provider(self, provider: RouterToolProvider) -> None:
        self.router.register_tool_provider(provider)

    def register_hook(self, hook: SessionHook) -> None:
        self._hooks.append(hook)

    @property
    def world(self) -> WorldSessionAPI:
        return self._world

    def _run_before_hooks(self, tool: str, args: Dict[str, Any]) -> None:
        for hook in self._hooks:
            hook.before_call(tool, dict(args))

    def _run_after_hooks(
        self, tool: str, args: Dict[str, Any], result: Dict[str, Any]
    ) -> None:
        for hook in self._hooks:
            hook.after_call(tool, dict(args), dict(result))


def create_session(
    *,
    seed: int = 42042,
    artifacts_dir: str | None = None,
    connector_mode: str = "sim",
    scenario_name: str = "multi_channel",
    scenario: Any | None = None,
    branch: str = "main",
) -> EnterpriseSession:
    return EnterpriseSession(
        SessionConfig(
            seed=seed,
            artifacts_dir=artifacts_dir,
            connector_mode=connector_mode,
            scenario_name=scenario_name,
            scenario=scenario,
            branch=branch,
        )
    )


def compile_workflow_spec(spec: Any, *, seed: int = 42042) -> CompiledWorkflow:
    return compile_workflow(spec, seed=seed)


def validate_workflow_spec(
    spec: Any,
    *,
    seed: int = 42042,
    available_tools: Iterable[str] | None = None,
) -> ValidationReport:
    workflow = compile_workflow(spec, seed=seed)
    return validate_workflow(workflow, available_tools=available_tools)


def run_workflow_spec(
    spec: Any,
    *,
    seed: int = 42042,
    artifacts_dir: str | None = None,
    connector_mode: str = "sim",
) -> ScenarioRunResult:
    workflow = compile_workflow(spec, seed=seed)
    return run_workflow(
        workflow,
        seed=seed,
        artifacts_dir=artifacts_dir,
        connector_mode=connector_mode,
    )


def generate_enterprise_corpus(
    *,
    seed: int = 42042,
    environment_count: int = 10,
    scenarios_per_environment: int = 10,
) -> CorpusBundle:
    return generate_corpus(
        seed=seed,
        environment_count=environment_count,
        scenarios_per_environment=scenarios_per_environment,
    )


def filter_enterprise_corpus(
    bundle: CorpusBundle,
    *,
    realism_threshold: float = 0.55,
) -> QualityFilterReport:
    workflows: list[GeneratedWorkflowSpec] = [
        GeneratedWorkflowSpec.model_validate(workflow.model_dump())
        for workflow in bundle.workflows
    ]
    return filter_workflow_corpus(workflows, realism_threshold=realism_threshold)


def get_scenario_manifest(name: str) -> ScenarioManifest:
    return get_catalog_scenario_manifest(name)


def list_scenario_manifest() -> list[ScenarioManifest]:
    return list_catalog_scenario_manifest()


def get_benchmark_family_manifest_entry(name: str) -> BenchmarkFamilyManifest:
    return get_benchmark_family_manifest(name)


def list_benchmark_family_manifest_entries() -> list[BenchmarkFamilyManifest]:
    return list_benchmark_family_manifest()


def get_showcase_example_entry(name: str) -> BenchmarkShowcaseExample:
    return _get_showcase_example(name)


def list_showcase_example_entries() -> list[BenchmarkShowcaseExample]:
    return _list_showcase_examples()


def get_facade_manifest_entry(name: str) -> FacadeManifest:
    return _get_facade_manifest(name)


def list_facade_manifest_entries() -> list[FacadeManifest]:
    return _list_facade_manifest()


def get_facade_plugin_entry(name: str) -> FacadePlugin:
    return _get_facade_plugin(name)


def list_facade_plugin_entries() -> list[FacadePlugin]:
    return _list_facade_plugins()


def register_facade_plugin_entry(plugin: FacadePlugin) -> FacadePlugin:
    return _register_facade_plugin(plugin)


def build_blueprint_asset_for_family_entry(
    family_name: str, *, variant_name: str | None = None
) -> BlueprintAsset:
    return _build_blueprint_asset_for_family(family_name, variant_name=variant_name)


def build_blueprint_asset_for_example_entry(name: str) -> BlueprintAsset:
    return _build_blueprint_asset_for_example(name)


def build_blueprint_asset_for_scenario_entry(
    scenario_name: str,
    *,
    family_name: str | None = None,
    workflow_name: str | None = None,
    workflow_variant: str | None = None,
) -> BlueprintAsset:
    return _build_blueprint_asset_for_scenario(
        scenario_name,
        family_name=family_name,
        workflow_name=workflow_name,
        workflow_variant=workflow_variant,
    )


def build_blueprint_for_family_entry(
    family_name: str, *, variant_name: str | None = None
) -> BlueprintSpec:
    return _build_blueprint_for_family(family_name, variant_name=variant_name)


def build_blueprint_for_scenario_entry(
    scenario_name: str,
    *,
    family_name: str | None = None,
    workflow_name: str | None = None,
    workflow_variant: str | None = None,
) -> BlueprintSpec:
    return _build_blueprint_for_scenario(
        scenario_name,
        family_name=family_name,
        workflow_name=workflow_name,
        workflow_variant=workflow_variant,
    )


def list_blueprint_entries() -> list[BlueprintSpec]:
    return _list_blueprint_specs()


def list_blueprint_builder_examples_entries() -> list[str]:
    return _list_blueprint_builder_examples()


def build_grounding_bundle_example_entry(name: str) -> IdentityGovernanceBundle:
    return _build_grounding_bundle_example(name)


def list_grounding_bundle_example_entries() -> list[GroundingBundleManifest]:
    return _list_grounding_bundle_examples()


def list_import_package_example_entries() -> list[str]:
    return _list_import_package_examples()


def get_import_package_example_path_entry(name: str) -> str:
    return str(_get_import_package_example_path(name))


def load_import_package_entry(path: str) -> ImportPackage:
    return _load_import_package(path)


def validate_import_package_entry(path: str) -> NormalizationReport:
    return _validate_import_package(path)


def normalize_import_package_entry(path: str) -> ImportPackageArtifacts:
    return _normalize_identity_import_package(path)


def review_import_package_entry(path: str) -> ImportReview:
    return _review_import_package(path)


def load_okta_connector_config_entry(path: str) -> OktaConnectorConfig:
    return _load_okta_connector_config(path)


def scaffold_mapping_override_entry(
    path: str,
    *,
    source_id: str,
    output_path: str | None = None,
) -> tuple[str, MappingOverrideSpec]:
    destination, payload = _scaffold_mapping_override(
        path,
        source_id=source_id,
        output_path=output_path,
    )
    return str(destination), payload


def generate_identity_scenario_candidates_entry(
    bundle: IdentityGovernanceBundle,
) -> list[GeneratedScenarioCandidate]:
    return _generate_identity_scenario_candidates(bundle)


def bootstrap_contract_from_import_bundle_entry(
    *,
    bundle: IdentityGovernanceBundle,
    contract_payload: Dict[str, Any],
    scenario_name: str,
    workflow_parameters: Dict[str, Any],
) -> Dict[str, Any]:
    return _bootstrap_contract_from_import_bundle(
        bundle=bundle,
        contract_payload=contract_payload,
        scenario_name=scenario_name,
        workflow_parameters=workflow_parameters,
    )


def compile_identity_governance_bundle_entry(
    bundle: IdentityGovernanceBundle,
) -> BlueprintAsset:
    return _compile_identity_governance_bundle(bundle)


def compile_blueprint_entry(asset: BlueprintAsset) -> CompiledBlueprint:
    return _compile_blueprint(asset)


def create_world_session_from_blueprint_entry(
    asset: BlueprintAsset,
    *,
    seed: int = 42042,
    artifacts_dir: str | None = None,
    connector_mode: str | None = None,
    branch: str = "main",
) -> WorldSessionAPI:
    return _create_world_session_from_blueprint(
        asset,
        seed=seed,
        artifacts_dir=artifacts_dir,
        connector_mode=connector_mode,
        branch=branch,
    )


def create_workspace_from_template_entry(
    *,
    root: str,
    source_kind: str,
    source_ref: str,
    name: str | None = None,
    title: str | None = None,
    description: str | None = None,
    workflow_name: str | None = None,
    workflow_variant: str | None = None,
    overwrite: bool = False,
) -> WorkspaceManifest:
    return _create_workspace_from_template(
        root=root,
        source_kind=source_kind,  # type: ignore[arg-type]
        source_ref=source_ref,
        name=name,
        title=title,
        description=description,
        workflow_name=workflow_name,
        workflow_variant=workflow_variant,
        overwrite=overwrite,
    )


def get_vertical_pack_manifest_entry(name: str) -> VerticalPackManifest:
    return _get_vertical_pack_manifest(name)


def list_vertical_pack_manifest_entries() -> list[VerticalPackManifest]:
    return _list_vertical_pack_manifests()


def list_vertical_scenario_variant_entries(
    vertical_name: str,
) -> list[VerticalScenarioVariantSpec]:
    return _list_vertical_scenario_variants(vertical_name)


def get_vertical_scenario_variant_entry(
    vertical_name: str, variant_name: str
) -> VerticalScenarioVariantSpec:
    return _get_vertical_scenario_variant(vertical_name, variant_name)


def list_vertical_contract_variant_entries(
    vertical_name: str,
) -> list[VerticalContractVariantSpec]:
    return _list_vertical_contract_variants(vertical_name)


def get_vertical_contract_variant_entry(
    vertical_name: str, variant_name: str
) -> VerticalContractVariantSpec:
    return _get_vertical_contract_variant(vertical_name, variant_name)


def prepare_vertical_demo_entry(
    *,
    vertical_name: str,
    workspace_root: str,
    compare_runner: str = "scripted",
    overwrite: bool = True,
    seed: int = 42042,
    max_steps: int = 18,
    compare_model: str | None = None,
    compare_provider: str | None = None,
    compare_bc_model_path: str | None = None,
    compare_task: str | None = None,
    scenario_variant: str | None = None,
    contract_variant: str | None = None,
) -> VerticalDemoResult:
    return _prepare_vertical_demo(
        VerticalDemoSpec(
            vertical_name=vertical_name,
            workspace_root=workspace_root,
            scenario_variant=scenario_variant,
            contract_variant=contract_variant,
            compare_runner=compare_runner,  # type: ignore[arg-type]
            overwrite=overwrite,
            seed=seed,
            max_steps=max_steps,
            compare_model=compare_model,
            compare_provider=compare_provider,
            compare_bc_model_path=(
                Path(compare_bc_model_path)
                if compare_bc_model_path is not None
                else None
            ),
            compare_task=compare_task,
        )
    )


def run_vertical_showcase_entry(
    *,
    root: str,
    vertical_names: list[str] | None = None,
    compare_runner: str = "scripted",
    overwrite: bool = True,
    seed: int = 42042,
    max_steps: int = 18,
    compare_model: str | None = None,
    compare_provider: str | None = None,
    compare_bc_model_path: str | None = None,
    run_id: str = "vertical_showcase",
) -> VerticalShowcaseResult:
    return _run_vertical_showcase(
        VerticalShowcaseSpec(
            vertical_names=list(vertical_names or []),
            root=Path(root),
            compare_runner=compare_runner,  # type: ignore[arg-type]
            overwrite=overwrite,
            seed=seed,
            max_steps=max_steps,
            compare_model=compare_model,
            compare_provider=compare_provider,
            compare_bc_model_path=(
                Path(compare_bc_model_path)
                if compare_bc_model_path is not None
                else None
            ),
            run_id=run_id,
        )
    )


def run_vertical_variant_matrix_entry(
    *,
    root: str,
    vertical_names: list[str] | None = None,
    compare_runner: str = "scripted",
    overwrite: bool = True,
    seed: int = 42042,
    max_steps: int = 18,
    compare_model: str | None = None,
    compare_provider: str | None = None,
    compare_bc_model_path: str | None = None,
    run_id: str = "variant_matrix",
) -> VerticalVariantMatrixResult:
    return _run_vertical_variant_matrix(
        VerticalVariantMatrixSpec(
            vertical_names=list(vertical_names or []),
            root=Path(root),
            compare_runner=compare_runner,  # type: ignore[arg-type]
            overwrite=overwrite,
            seed=seed,
            max_steps=max_steps,
            compare_model=compare_model,
            compare_provider=compare_provider,
            compare_bc_model_path=(
                Path(compare_bc_model_path)
                if compare_bc_model_path is not None
                else None
            ),
            run_id=run_id,
        )
    )


def import_workspace_entry(
    *,
    root: str,
    package_path: str | None = None,
    bundle_path: str | None = None,
    blueprint_asset_path: str | None = None,
    compiled_blueprint_path: str | None = None,
    name: str | None = None,
    title: str | None = None,
    description: str | None = None,
    overwrite: bool = False,
) -> WorkspaceManifest:
    return _import_workspace(
        root=root,
        package_path=package_path,
        bundle_path=bundle_path,
        blueprint_asset_path=blueprint_asset_path,
        compiled_blueprint_path=compiled_blueprint_path,
        name=name,
        title=title,
        description=description,
        overwrite=overwrite,
    )


def load_workspace_entry(root: str) -> WorkspaceManifest:
    return _load_workspace(root)


def show_workspace_entry(root: str) -> WorkspaceSummary:
    return _show_workspace(root)


def compile_workspace_entry(root: str) -> WorkspaceSummary:
    return _compile_workspace(root)


def list_workspace_sources_entry(root: str) -> list[WorkspaceSourceConfig]:
    return _list_workspace_sources(root)


def list_workspace_scenario_variants_entry(root: str) -> list[dict[str, Any]]:
    return _list_workspace_scenario_variants(root)


def activate_workspace_scenario_variant_entry(
    root: str,
    variant_name: str,
    *,
    bootstrap_contract: bool = False,
) -> WorkspaceScenarioSpec:
    return _activate_workspace_scenario_variant(
        root,
        variant_name,
        bootstrap_contract=bootstrap_contract,
    )


def list_workspace_contract_variants_entry(root: str) -> list[dict[str, Any]]:
    return _list_workspace_contract_variants(root)


def activate_workspace_contract_variant_entry(
    root: str, variant_name: str
) -> ContractSpec:
    return _activate_workspace_contract_variant(root, variant_name)


def list_workspace_source_syncs_entry(root: str) -> list[WorkspaceSourceSyncRecord]:
    return _list_workspace_source_syncs(root)


def sync_workspace_source_entry(
    root: str,
    *,
    connector: str,
    config_path: str,
    source_id: str | None = None,
) -> WorkspaceSourceSyncRecord:
    return _sync_workspace_source(
        root,
        connector=connector,
        config_path=config_path,
        source_id=source_id,
    )


def build_identity_flow_summary_entry(root: str) -> WorkspaceIdentityFlowSummary:
    return _build_identity_flow_summary(root)


def prepare_identity_workspace_flow_entry(
    root: str,
    *,
    package_path: str | None = None,
    scenario_name: str | None = None,
    overwrite: bool = False,
    replace_generated: bool = True,
    run_workflow: bool = False,
    run_scripted: bool = False,
    workflow_run_id: str | None = None,
    scripted_run_id: str | None = None,
) -> WorkspaceIdentityFlowSummary:
    return _prepare_identity_workspace_flow(
        root,
        package_path=package_path,
        scenario_name=scenario_name,
        overwrite=overwrite,
        replace_generated=replace_generated,
        run_workflow=run_workflow,
        run_scripted=run_scripted,
        workflow_run_id=workflow_run_id,
        scripted_run_id=scripted_run_id,
    )


def list_workspace_scenarios_entry(root: str) -> list[WorkspaceScenarioSpec]:
    return _list_workspace_scenarios(root)


def create_workspace_scenario_entry(
    root: str,
    *,
    name: str,
    title: str | None = None,
    description: str | None = None,
    scenario_name: str | None = None,
    workflow_name: str | None = None,
    workflow_variant: str | None = None,
    workflow_parameters: Dict[str, Any] | None = None,
    inspection_focus: str | None = None,
    tags: list[str] | None = None,
    hidden_faults: Dict[str, Any] | None = None,
    actor_hints: list[str] | None = None,
    contract_overrides: Dict[str, Any] | None = None,
    metadata: Dict[str, Any] | None = None,
) -> WorkspaceScenarioSpec:
    return _create_workspace_scenario(
        root,
        name=name,
        title=title,
        description=description,
        scenario_name=scenario_name,
        workflow_name=workflow_name,
        workflow_variant=workflow_variant,
        workflow_parameters=workflow_parameters,
        inspection_focus=inspection_focus,
        tags=tags,
        hidden_faults=hidden_faults,
        actor_hints=actor_hints,
        contract_overrides=contract_overrides,
        metadata=metadata,
    )


def preview_workspace_scenario_entry(
    root: str, scenario_name: str | None = None
) -> Dict[str, Any]:
    return _preview_workspace_scenario(root, scenario_name)


def load_workspace_import_report_entry(root: str) -> NormalizationReport | None:
    return _load_workspace_import_report(root)


def load_workspace_import_review_entry(root: str) -> ImportReview | None:
    return _load_workspace_import_review(root)


def load_workspace_provenance_entry(
    root: str, object_ref: str | None = None
) -> list[ProvenanceRecord]:
    return _load_workspace_provenance(root, object_ref)


def load_workspace_generated_scenarios_entry(
    root: str,
) -> list[GeneratedScenarioCandidate]:
    return _load_workspace_generated_scenarios(root)


def generate_workspace_scenarios_from_import_entry(
    root: str, *, replace_generated: bool = False
) -> list[WorkspaceScenarioSpec]:
    return _generate_workspace_scenarios_from_import(
        root, replace_generated=replace_generated
    )


def activate_workspace_scenario_entry(
    root: str,
    *,
    scenario_name: str,
    bootstrap_contract: bool = False,
) -> WorkspaceScenarioSpec:
    return _activate_workspace_scenario(
        root,
        scenario_name,
        bootstrap_contract=bootstrap_contract,
    )


def load_workspace_contract_entry(
    root: str, scenario_name: str | None = None
) -> ContractSpec:
    return _load_workspace_contract(root, scenario_name)


def validate_workspace_contract_entry(
    root: str, scenario_name: str | None = None
) -> Dict[str, Any]:
    return _validate_workspace_contract(root, scenario_name)


def diff_workspace_contract_entry(
    root: str,
    *,
    scenario_name: str | None = None,
    other_path: str | None = None,
) -> Dict[str, Any]:
    return _diff_workspace_contract(
        root,
        scenario_name=scenario_name,
        other_path=other_path,
    )


def bootstrap_workspace_contract_entry(
    root: str,
    *,
    scenario_name: str | None = None,
    overwrite: bool = False,
) -> ContractSpec:
    return _bootstrap_workspace_contract(
        root,
        scenario_name=scenario_name,
        overwrite=overwrite,
    )


def list_workspace_runs_entry(root: str) -> list[WorkspaceRunEntry]:
    return _list_workspace_runs(root)


def build_playable_mission_catalog_entry() -> MissionCatalog:
    return _build_playable_mission_catalog()


def list_playable_missions_entry(
    vertical_name: str | None = None,
) -> list[PlayableMissionSpec]:
    return _list_playable_missions(vertical_name)


def get_playable_mission_entry(
    vertical_name: str,
    mission_name: str,
) -> PlayableMissionSpec:
    return _get_playable_mission(vertical_name, mission_name)


def list_workspace_playable_missions_entry(root: str) -> list[PlayableMissionSpec]:
    return _list_workspace_playable_missions(root)


def activate_workspace_playable_mission_entry(
    root: str,
    mission_name: str,
    *,
    objective_variant: str | None = None,
) -> Dict[str, Any]:
    return _activate_workspace_playable_mission(
        root,
        mission_name,
        objective_variant=objective_variant,
    )


def start_workspace_mission_run_entry(
    root: str,
    *,
    mission_name: str,
    objective_variant: str | None = None,
    run_id: str | None = None,
    seed: int = 42042,
) -> MissionSessionState:
    return _start_workspace_mission_run(
        root,
        mission_name=mission_name,
        objective_variant=objective_variant,
        run_id=run_id,
        seed=seed,
    )


def load_workspace_mission_state_entry(
    root: str,
    run_id: str | None = None,
) -> MissionSessionState | None:
    return _load_workspace_mission_state(root, run_id)


def prepare_playable_workspace_entry(
    root: str,
    *,
    world: str,
    mission: str | None = None,
    objective: str | None = None,
    compare_runner: str = "scripted",
    overwrite: bool = True,
    seed: int = 42042,
    max_steps: int = 18,
) -> MissionSessionState:
    return _prepare_playable_workspace(
        root,
        world=world,
        mission=mission,
        objective=objective,
        compare_runner=compare_runner,
        overwrite=overwrite,
        seed=seed,
        max_steps=max_steps,
    )


def export_mission_run_entry(
    root: str,
    *,
    run_id: str,
    export_format: str,
) -> Dict[str, Any]:
    return _export_mission_run(root, run_id=run_id, export_format=export_format)


def run_playable_showcase_entry(
    root: str,
    *,
    world_names: list[str] | None = None,
    mission_name: str | None = None,
    objective_variant: str | None = None,
    compare_runner: str = "scripted",
    run_id: str = "playable_showcase",
    overwrite: bool = True,
    seed: int = 42042,
    max_steps: int = 18,
) -> PlayableShowcaseResult:
    return _run_playable_showcase(
        root,
        world_names=world_names,
        mission_name=mission_name,
        objective_variant=objective_variant,
        compare_runner=compare_runner,
        run_id=run_id,
        overwrite=overwrite,
        seed=seed,
        max_steps=max_steps,
    )


def get_workspace_fidelity_report_entry(root: str) -> TwinFidelityReport:
    return _get_or_build_workspace_fidelity_report(root)


def launch_workspace_run_entry(
    root: str,
    *,
    runner: str,
    scenario_name: str | None = None,
    run_id: str | None = None,
    seed: int = 42042,
    branch: str | None = None,
    model: str | None = None,
    provider: str | None = None,
    bc_model_path: str | None = None,
    task: str | None = None,
    max_steps: int = 12,
) -> RunManifest:
    return _launch_workspace_run(
        root,
        runner=runner,
        scenario_name=scenario_name,
        run_id=run_id,
        seed=seed,
        branch=branch,
        model=model,
        provider=provider,
        bc_model_path=bc_model_path,
        task=task,
        max_steps=max_steps,
    )


def list_run_manifests_entry(root: str) -> list[RunManifest]:
    return _list_run_manifests(root)


def load_run_manifest_entry(path: str) -> RunManifest:
    return _load_run_manifest(path)


def load_run_timeline_entry(path: str) -> list[RunTimelineEvent]:
    return _load_run_timeline(path)


def load_run_events_entry(root: str, run_id: str) -> list[RunTimelineEvent]:
    return _load_run_events_for_run(root, run_id)


def get_run_orientation_entry(root: str, run_id: str) -> Dict[str, Any]:
    return _get_run_orientation(root, run_id)


def get_run_capability_graphs_entry(root: str, run_id: str) -> Dict[str, Any]:
    return _get_run_capability_graphs(root, run_id)


def list_run_snapshots_entry(root: str, run_id: str) -> list[RunSnapshotRef]:
    return _list_run_snapshots(root, run_id)


def diff_run_snapshots_entry(
    root: str, run_id: str, *, snapshot_from: int, snapshot_to: int
) -> Dict[str, Any]:
    return _diff_run_snapshots(
        root,
        run_id,
        snapshot_from=snapshot_from,
        snapshot_to=snapshot_to,
    )


def get_benchmark_family_workflow_spec(name: str) -> WorkflowScenarioSpec:
    return _get_benchmark_family_workflow_spec(name)


def list_benchmark_family_workflow_specs() -> list[WorkflowScenarioSpec]:
    return _list_benchmark_family_workflow_specs()


def get_benchmark_family_workflow_variant(
    family_name: str, variant_name: str
) -> BenchmarkWorkflowVariantManifest:
    return _get_benchmark_family_workflow_variant(family_name, variant_name)


def list_benchmark_family_workflow_variants(
    family_name: str | None = None,
) -> list[BenchmarkWorkflowVariantManifest]:
    return _list_benchmark_family_workflow_variants(family_name)


def validate_benchmark_family_workflow(
    name: str, *, seed: int = 42042, available_tools: Iterable[str] | None = None
) -> ValidationReport:
    spec = _get_benchmark_family_workflow_spec(name)
    return validate_workflow_spec(spec, seed=seed, available_tools=available_tools)


def run_benchmark_family_workflow(
    name: str,
    *,
    variant_name: str | None = None,
    seed: int = 42042,
    artifacts_dir: str | None = None,
    connector_mode: str = "sim",
) -> ScenarioRunResult:
    spec = _get_benchmark_family_workflow_spec(name, variant_name=variant_name)
    return run_workflow_spec(
        spec,
        seed=seed,
        artifacts_dir=artifacts_dir,
        connector_mode=connector_mode,
    )


def build_release_version(*, prefix: str | None = None) -> str:
    return _build_release_version(prefix=prefix)


def export_release_dataset(
    *,
    input_path: str,
    release_root: str,
    version: str,
    label: str,
    dataset_kind: str = "auto",
) -> DatasetReleaseResult:
    from pathlib import Path

    return export_dataset_release(
        input_path=Path(input_path),
        release_root=Path(release_root),
        version=version,
        label=label,
        dataset_kind=dataset_kind,  # type: ignore[arg-type]
    )


def export_release_benchmark(
    *,
    benchmark_dir: str,
    release_root: str,
    version: str,
    label: str,
) -> BenchmarkReleaseResult:
    from pathlib import Path

    return snapshot_benchmark_release(
        benchmark_dir=Path(benchmark_dir),
        release_root=Path(release_root),
        version=version,
        label=label,
    )


def run_release_nightly(
    *,
    release_root: str,
    workspace_root: str,
    version: str,
    seed: int = 42042,
    environment_count: int = 25,
    scenarios_per_environment: int = 20,
    realism_threshold: float = 0.55,
    rollout_episodes: int = 3,
    rollout_scenario: str = "multi_channel",
    benchmark_scenarios: Iterable[str] | None = None,
    llm_model: str | None = None,
    llm_provider: str = "auto",
) -> NightlyReleaseResult:
    from pathlib import Path

    return run_nightly_release(
        release_root=Path(release_root),
        workspace_root=Path(workspace_root),
        version=version,
        seed=seed,
        environment_count=environment_count,
        scenarios_per_environment=scenarios_per_environment,
        realism_threshold=realism_threshold,
        rollout_episodes=rollout_episodes,
        rollout_scenario=rollout_scenario,
        benchmark_scenarios=list(benchmark_scenarios or ["multi_channel"]),
        llm_model=llm_model,
        llm_provider=llm_provider,
    )
