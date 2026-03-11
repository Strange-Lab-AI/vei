from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Iterable, Optional, Protocol

from vei.capability_graph.models import RuntimeCapabilityGraphs
from vei.orientation.models import WorldOrientation
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
from vei.blueprint.plugins import (
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
from vei.benchmark.showcase import (
    BenchmarkShowcaseExample,
    get_showcase_example as _get_showcase_example,
    list_showcase_examples as _list_showcase_examples,
)
from vei.benchmark.workflows import (
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
from vei.scenario_engine.compiler import CompiledWorkflow
from vei.scenario_engine.models import WorkflowScenarioSpec
from vei.scenario_runner.api import run_workflow, validate_workflow
from vei.scenario_runner.models import ScenarioRunResult, ValidationReport
from vei.grounding.api import (
    build_grounding_bundle_example as _build_grounding_bundle_example,
    compile_identity_governance_bundle as _compile_identity_governance_bundle,
    list_grounding_bundle_examples as _list_grounding_bundle_examples,
)
from vei.grounding.models import GroundingBundleManifest, IdentityGovernanceBundle
from vei.world.api import (
    WorldSessionAPI,
    create_world_session,
    get_catalog_scenario,
    get_catalog_scenario_manifest,
    list_catalog_scenario_manifest,
)
from vei.world.manifest import ScenarioManifest


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
