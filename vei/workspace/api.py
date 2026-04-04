from __future__ import annotations

import json
from dataclasses import asdict
from pathlib import Path
from typing import Any, Dict, Iterable, Literal, Optional

from vei.blueprint.api import (
    build_blueprint_asset_for_example,
    build_blueprint_asset_for_family,
    build_blueprint_asset_for_scenario,
    compile_blueprint,
    materialize_scenario_from_blueprint,
)
from vei.blueprint.models import BlueprintAsset, CompiledBlueprint
from vei.benchmark import get_benchmark_family_workflow_spec
from vei.contract.api import build_contract_from_workflow, evaluate_contract
from vei.contract.models import ContractEvaluationResult, ContractSpec
from vei.grounding.api import compile_identity_governance_bundle
from vei.grounding.models import IdentityGovernanceBundle
from vei.imports.api import normalize_identity_import_package
from vei.imports import (
    bootstrap_contract_from_import_bundle,
    load_okta_connector_config,
    sync_okta_import_package,
)
from vei.imports.models import (
    GeneratedScenarioCandidate,
    ImportReview,
    ImportPackage,
    ImportPackageArtifacts,
    NormalizationReport,
    MappingOverrideSpec,
    ProvenanceRecord,
    RedactionReport,
)
from vei.verticals import (
    build_vertical_blueprint_asset,
    default_vertical_contract_variant,
    get_vertical_contract_variant,
    get_vertical_scenario_variant,
    list_vertical_contract_variants,
    list_vertical_scenario_variants,
    apply_fault_overlays,
    overlay_summaries,
)
from vei.world import build_scenario_manifest

from .models import (
    WorkspaceCompileRecord,
    WorkspaceManifest,
    WorkspaceSourceConfig,
    WorkspaceSourceSyncRecord,
    WorkspaceRunEntry,
    WorkspaceScenarioSpec,
    WorkspaceSummary,
)
from ._support import (
    _active_vertical_contract_variant_name,
    _active_vertical_scenario_variant_name,
    _append_workspace_source_sync,
    _apply_workspace_contract_variant,
    _bootstrap_workspace,
    _contract_bootstrapped,
    _copy_import_sources,
    _deep_merge,
    _ensure_workspace_root,
    _iso_now,
    _json_diff,
    _load_precompiled_workspace_blueprint,
    _load_workspace_grounding_bundle,
    _load_workspace_import_summary,
    _read_model,
    _resolve_contract_path,
    _resolve_workspace_vertical_scenario_variant,
    _scenario_entry_path,
    _upsert_workspace_source,
    _workspace_vertical_name,
    _write_json,
    temporary_env as temporary_env,  # noqa: F401
)

WORKSPACE_MANIFEST = "vei_project.json"


def create_workspace_from_template(
    *,
    root: str | Path,
    source_kind: Literal["example", "family", "scenario", "vertical"],
    source_ref: str,
    name: Optional[str] = None,
    title: Optional[str] = None,
    description: Optional[str] = None,
    workflow_name: Optional[str] = None,
    workflow_variant: Optional[str] = None,
    overwrite: bool = False,
) -> WorkspaceManifest:
    path = _ensure_workspace_root(root, overwrite=overwrite)
    if source_kind == "example":
        asset = build_blueprint_asset_for_example(source_ref)
    elif source_kind == "family":
        asset = build_blueprint_asset_for_family(
            source_ref, variant_name=workflow_variant
        )
    elif source_kind == "vertical":
        asset = build_vertical_blueprint_asset(source_ref)
    else:
        asset = build_blueprint_asset_for_scenario(
            source_ref,
            workflow_name=workflow_name,
            workflow_variant=workflow_variant,
        )
    manifest = _bootstrap_workspace(
        root=path,
        asset=asset,
        source_kind=source_kind,
        source_ref=source_ref,
        name=name,
        title=title,
        description=description,
    )
    compile_workspace(path)
    return manifest


def import_workspace(
    *,
    root: str | Path,
    package_path: str | Path | None = None,
    bundle_path: str | Path | None = None,
    blueprint_asset_path: str | Path | None = None,
    compiled_blueprint_path: str | Path | None = None,
    name: Optional[str] = None,
    title: Optional[str] = None,
    description: Optional[str] = None,
    overwrite: bool = False,
) -> WorkspaceManifest:
    selected = sum(
        value is not None
        for value in (
            package_path,
            bundle_path,
            blueprint_asset_path,
            compiled_blueprint_path,
        )
    )
    if selected != 1:
        raise ValueError(
            "import_workspace requires exactly one of package_path, bundle_path, blueprint_asset_path, or compiled_blueprint_path"
        )
    path = _ensure_workspace_root(root, overwrite=overwrite)

    grounding_bundle: IdentityGovernanceBundle | None = None
    precompiled_blueprint: CompiledBlueprint | None = None
    import_artifacts: ImportPackageArtifacts | None = None
    if package_path is not None:
        import_artifacts = normalize_identity_import_package(package_path)
        grounding_bundle = import_artifacts.normalized_bundle
        if grounding_bundle is None:
            raise ValueError(
                "Import package could not be compiled into a workspace; review normalization diagnostics and mapping overrides first"
            )
        asset = compile_identity_governance_bundle(grounding_bundle)
        source_kind = "import_package"
        source_ref = str(package_path)
    elif bundle_path is not None:
        grounding_bundle = _read_model(Path(bundle_path), IdentityGovernanceBundle)
        asset = compile_identity_governance_bundle(grounding_bundle)
        source_kind = "grounding_bundle"
        source_ref = str(bundle_path)
    elif blueprint_asset_path is not None:
        asset = _read_model(Path(blueprint_asset_path), BlueprintAsset)
        source_kind = "blueprint_asset"
        source_ref = str(blueprint_asset_path)
    else:
        precompiled_blueprint = _read_model(
            Path(compiled_blueprint_path or ""), CompiledBlueprint
        )
        asset = precompiled_blueprint.asset
        source_kind = "compiled_blueprint"
        source_ref = str(compiled_blueprint_path)

    manifest = _bootstrap_workspace(
        root=path,
        asset=asset,
        grounding_bundle=grounding_bundle,
        import_artifacts=import_artifacts,
        precompiled_blueprint=precompiled_blueprint,
        source_kind=source_kind,
        source_ref=source_ref,
        name=name,
        title=title,
        description=description,
    )
    if import_artifacts is not None and package_path is not None:
        _copy_import_sources(Path(package_path), path, manifest, import_artifacts)
    compile_workspace(path)
    return manifest


def load_workspace(root: str | Path) -> WorkspaceManifest:
    path = Path(root).expanduser().resolve()
    return _read_model(path / WORKSPACE_MANIFEST, WorkspaceManifest)


def write_workspace(root: str | Path, manifest: WorkspaceManifest) -> WorkspaceManifest:
    path = Path(root).expanduser().resolve()
    _write_json(path / WORKSPACE_MANIFEST, manifest.model_dump(mode="json"))
    return manifest


def show_workspace(root: str | Path) -> WorkspaceSummary:
    path = Path(root).expanduser().resolve()
    manifest = load_workspace(path)
    compiled_root = path / manifest.compiled_root
    compiled_records: list[WorkspaceCompileRecord] = []
    for scenario in manifest.scenarios:
        scenario_root = compiled_root / scenario.name
        blueprint_path = scenario_root / "blueprint.json"
        contract_path = _resolve_contract_path(path, manifest, scenario)
        scenario_seed_path = scenario_root / "scenario_seed.json"
        if (
            blueprint_path.exists()
            and contract_path.exists()
            and scenario_seed_path.exists()
        ):
            compiled_records.append(
                WorkspaceCompileRecord(
                    scenario_name=scenario.name,
                    compiled_blueprint_path=str(blueprint_path.relative_to(path)),
                    contract_path=str(contract_path.relative_to(path)),
                    scenario_seed_path=str(scenario_seed_path.relative_to(path)),
                    contract_bootstrapped=_contract_bootstrapped(path, scenario_root),
                )
            )
    runs = list_workspace_runs(path)
    return WorkspaceSummary(
        manifest=manifest,
        compiled_scenarios=compiled_records,
        run_count=len(runs),
        latest_run_id=(runs[0].run_id if runs else None),
        imports=_load_workspace_import_summary(path, manifest),
    )


def list_workspace_sources(root: str | Path) -> list[WorkspaceSourceConfig]:
    path = Path(root).expanduser().resolve()
    manifest = load_workspace(path)
    if not manifest.source_registry_path:
        return []
    registry_path = path / manifest.source_registry_path
    if not registry_path.exists():
        return []
    payload = json.loads(registry_path.read_text(encoding="utf-8"))
    return [WorkspaceSourceConfig.model_validate(item) for item in payload]


def list_workspace_source_syncs(root: str | Path) -> list[WorkspaceSourceSyncRecord]:
    path = Path(root).expanduser().resolve()
    manifest = load_workspace(path)
    if not manifest.source_sync_history_path:
        return []
    history_path = path / manifest.source_sync_history_path
    if not history_path.exists():
        return []
    payload = json.loads(history_path.read_text(encoding="utf-8"))
    return [WorkspaceSourceSyncRecord.model_validate(item) for item in payload]


def sync_workspace_source(
    root: str | Path,
    *,
    connector: str,
    config_path: str | Path,
    source_id: str | None = None,
) -> WorkspaceSourceSyncRecord:
    path = Path(root).expanduser().resolve()
    manifest = load_workspace(path)
    normalized_connector = connector.strip().lower()
    resolved_source_id = source_id or f"{normalized_connector}_live"
    now = _iso_now()
    sync_root = (
        path
        / manifest.imports_dir
        / "source_syncs"
        / resolved_source_id
        / now.replace(":", "-")
    )
    if normalized_connector != "okta":
        raise ValueError("sync_workspace_source currently supports only connector=okta")

    config = load_okta_connector_config(config_path)
    source_entry = WorkspaceSourceConfig(
        source_id=resolved_source_id,
        connector=normalized_connector,
        config_path=str(Path(config_path).expanduser().resolve()),
        created_at=now,
        updated_at=now,
        metadata={"sync_root": str(sync_root.relative_to(path))},
    )
    _upsert_workspace_source(path, manifest, source_entry)

    try:
        sync_result = sync_okta_import_package(
            sync_root,
            config,
            source_prefix=resolved_source_id,
        )
        _refresh_workspace_import_from_package(
            path,
            package_path=sync_root / "package.json",
        )
        record = WorkspaceSourceSyncRecord(
            source_id=resolved_source_id,
            connector=normalized_connector,
            synced_at=now,
            status="ok",
            package_path=str((sync_root / "package.json").relative_to(path)),
            record_counts=dict(sync_result.record_counts),
            metadata={"sync_root": str(sync_root.relative_to(path))},
        )
    except Exception as exc:
        record = WorkspaceSourceSyncRecord(
            source_id=resolved_source_id,
            connector=normalized_connector,
            synced_at=now,
            status="error",
            package_path=str((sync_root / "package.json").relative_to(path)),
            message=str(exc),
            metadata={"sync_root": str(sync_root.relative_to(path))},
        )
        _append_workspace_source_sync(path, manifest, record)
        raise

    _append_workspace_source_sync(path, manifest, record)
    return record


def compile_workspace(root: str | Path) -> WorkspaceSummary:
    path = Path(root).expanduser().resolve()
    manifest = load_workspace(path)
    asset = load_workspace_blueprint_asset(path)
    for scenario in manifest.scenarios:
        scenario_root = path / manifest.compiled_root / scenario.name
        scenario_root.mkdir(parents=True, exist_ok=True)
        scenario_asset = build_workspace_scenario_asset(asset, scenario)
        compiled = _load_precompiled_workspace_blueprint(
            path, manifest, scenario, scenario_asset
        ) or compile_blueprint(scenario_asset)
        contract, bootstrapped = load_or_bootstrap_contract(
            path, manifest, scenario, compiled
        )
        scenario_seed = materialize_scenario_from_blueprint(scenario_asset)
        scenario_manifest = build_scenario_manifest(
            compiled.scenario.name, scenario_seed
        )
        _write_json(
            scenario_root / "blueprint_asset.json",
            scenario_asset.model_dump(mode="json"),
        )
        _write_json(
            scenario_root / "blueprint.json",
            compiled.model_dump(mode="json"),
        )
        _write_json(
            scenario_root / "contract_effective.json",
            contract.model_dump(mode="json"),
        )
        marker = scenario_root / ".contract_bootstrapped"
        if bootstrapped:
            marker.write_text("1", encoding="utf-8")
        elif marker.exists():
            marker.unlink()
        _write_json(
            scenario_root / "scenario_seed.json",
            asdict(scenario_seed),
        )
        _write_json(
            scenario_root / "scenario_manifest.json",
            scenario_manifest.model_dump(mode="json"),
        )
    return show_workspace(path)


def list_workspace_scenarios(root: str | Path) -> list[WorkspaceScenarioSpec]:
    return load_workspace(root).scenarios


def list_workspace_scenario_variants(root: str | Path) -> list[dict[str, Any]]:
    path = Path(root).expanduser().resolve()
    manifest = load_workspace(path)
    vertical_name = _workspace_vertical_name(manifest)
    if vertical_name is None:
        return []
    active = _active_vertical_scenario_variant_name(path, manifest)
    return [
        {
            **item.model_dump(mode="json"),
            "active": item.name == active,
        }
        for item in list_vertical_scenario_variants(vertical_name)
    ]


def activate_workspace_scenario_variant(
    root: str | Path,
    variant_name: str,
    *,
    bootstrap_contract: bool = False,
) -> WorkspaceScenarioSpec:
    path = Path(root).expanduser().resolve()
    manifest = load_workspace(path)
    vertical_name = _workspace_vertical_name(manifest)
    if vertical_name is None:
        raise ValueError("workspace does not support vertical scenario variants")
    variant = get_vertical_scenario_variant(vertical_name, variant_name)
    scenario = resolve_workspace_scenario(path, manifest)
    asset = load_workspace_blueprint_asset(path)
    scenario.title = variant.title
    scenario.description = variant.description
    # Variants are overlays on the base company world, not separate catalog worlds.
    scenario.scenario_name = asset.scenario_name
    scenario.workflow_name = asset.workflow_name
    scenario.workflow_variant = variant.workflow_variant
    scenario.workflow_parameters = {
        **dict(asset.workflow_parameters),
        **dict(variant.workflow_parameter_overrides),
    }
    scenario.hidden_faults = {
        item.name: {
            "label": item.label,
            "origin": item.origin,
            "visibility": item.visibility,
            "rationale": item.rationale,
        }
        for item in variant.fault_overlays
    }
    metadata = {
        **dict(scenario.metadata),
        "vertical": vertical_name,
        "vertical_scenario_variant": variant.name,
        "vertical_scenario_variant_title": variant.title,
        "vertical_scenario_variant_rationale": variant.rationale,
        "vertical_scenario_variant_branches": list(variant.branch_labels),
        "vertical_scenario_variant_change_summary": list(variant.change_summary),
        "vertical_problem_name": variant.scenario_name,
        "vertical_scenario_fault_overlays": [
            item.model_dump(mode="json") for item in variant.fault_overlays
        ],
    }
    if "vertical_contract_variant" not in metadata:
        metadata["vertical_contract_variant"] = default_vertical_contract_variant(
            vertical_name
        ).name
    scenario.metadata = metadata
    write_workspace(path, manifest)
    _write_json(
        _scenario_entry_path(path, manifest, scenario), scenario.model_dump(mode="json")
    )
    bootstrap_workspace_contract(path, scenario_name=scenario.name, overwrite=True)
    compile_workspace(path)
    return resolve_workspace_scenario(path, scenario_name=scenario.name)


def list_workspace_contract_variants(root: str | Path) -> list[dict[str, Any]]:
    path = Path(root).expanduser().resolve()
    manifest = load_workspace(path)
    vertical_name = _workspace_vertical_name(manifest)
    if vertical_name is None:
        return []
    active = _active_vertical_contract_variant_name(path, manifest)
    return [
        {
            **item.model_dump(mode="json"),
            "active": item.name == active,
        }
        for item in list_vertical_contract_variants(vertical_name)
    ]


def activate_workspace_contract_variant(
    root: str | Path,
    variant_name: str,
) -> ContractSpec:
    path = Path(root).expanduser().resolve()
    manifest = load_workspace(path)
    vertical_name = _workspace_vertical_name(manifest)
    if vertical_name is None:
        raise ValueError("workspace does not support vertical contract variants")
    variant = get_vertical_contract_variant(vertical_name, variant_name)
    scenario = resolve_workspace_scenario(path, manifest)
    scenario.metadata = {
        **dict(scenario.metadata),
        "vertical": vertical_name,
        "vertical_contract_variant": variant.name,
        "vertical_contract_variant_title": variant.title,
        "vertical_contract_objective_summary": variant.objective_summary,
        "vertical_contract_rationale": variant.rationale,
    }
    write_workspace(path, manifest)
    _write_json(
        _scenario_entry_path(path, manifest, scenario), scenario.model_dump(mode="json")
    )
    compile_workspace(path)
    return load_workspace_contract(path, scenario.name)


def create_workspace_scenario(
    root: str | Path,
    *,
    name: str,
    title: Optional[str] = None,
    description: Optional[str] = None,
    scenario_name: Optional[str] = None,
    workflow_name: Optional[str] = None,
    workflow_variant: Optional[str] = None,
    workflow_parameters: Optional[Dict[str, Any]] = None,
    inspection_focus: Optional[str] = None,
    tags: Optional[list[str]] = None,
    hidden_faults: Optional[Dict[str, Any]] = None,
    actor_hints: Optional[list[str]] = None,
    contract_overrides: Optional[Dict[str, Any]] = None,
    metadata: Optional[Dict[str, Any]] = None,
) -> WorkspaceScenarioSpec:
    path = Path(root).expanduser().resolve()
    manifest = load_workspace(path)
    if any(item.name == name for item in manifest.scenarios):
        raise ValueError(f"workspace scenario already exists: {name}")
    base_asset = load_workspace_blueprint_asset(path)
    entry = WorkspaceScenarioSpec(
        name=name,
        title=title or name.replace("_", " ").title(),
        description=description
        or f"Workspace scenario {name} derived from {base_asset.scenario_name}.",
        scenario_name=scenario_name or base_asset.scenario_name,
        workflow_name=workflow_name or base_asset.workflow_name,
        workflow_variant=workflow_variant or base_asset.workflow_variant,
        workflow_parameters=dict(workflow_parameters or {}),
        inspection_focus=inspection_focus,
        tags=list(tags or []),
        hidden_faults=dict(hidden_faults or {}),
        actor_hints=list(actor_hints or []),
        contract_overrides=dict(contract_overrides or {}),
        metadata=dict(metadata or {}),
    )
    manifest.scenarios.append(entry)
    write_workspace(path, manifest)
    _write_json(
        _scenario_entry_path(path, manifest, entry), entry.model_dump(mode="json")
    )
    return entry


def preview_workspace_scenario(
    root: str | Path, scenario_name: Optional[str] = None
) -> Dict[str, Any]:
    path = Path(root).expanduser().resolve()
    manifest = load_workspace(path)
    entry = resolve_workspace_scenario(path, manifest, scenario_name)
    asset = build_workspace_scenario_asset(load_workspace_blueprint_asset(path), entry)
    compiled = _load_precompiled_workspace_blueprint(
        path, manifest, entry, asset
    ) or compile_blueprint(asset)
    contract, _ = load_or_bootstrap_contract(path, manifest, entry, compiled)
    scenario_seed = materialize_scenario_from_blueprint(asset)
    vertical_name = _workspace_vertical_name(manifest)
    return {
        "workspace": manifest.model_dump(mode="json"),
        "scenario": entry.model_dump(mode="json"),
        "compiled_blueprint": compiled.model_dump(mode="json"),
        "contract": contract.model_dump(mode="json"),
        "scenario_seed": asdict(scenario_seed),
        "active_scenario_variant": (
            _active_vertical_scenario_variant_name(path, manifest)
            if vertical_name is not None
            else None
        ),
        "active_contract_variant": (
            _active_vertical_contract_variant_name(path, manifest)
            if vertical_name is not None
            else None
        ),
        "available_scenario_variants": list_workspace_scenario_variants(path),
        "available_contract_variants": list_workspace_contract_variants(path),
    }


def load_workspace_blueprint_asset(root: str | Path) -> BlueprintAsset:
    manifest = load_workspace(root)
    path = Path(root).expanduser().resolve() / manifest.blueprint_asset_path
    return _read_model(path, BlueprintAsset)


def load_workspace_contract(
    root: str | Path, scenario_name: Optional[str] = None
) -> ContractSpec:
    path = Path(root).expanduser().resolve()
    manifest = load_workspace(path)
    scenario = resolve_workspace_scenario(path, manifest, scenario_name)
    contract = _read_model(
        _resolve_contract_path(path, manifest, scenario), ContractSpec
    )
    return _apply_workspace_contract_variant(path, manifest, scenario, contract)


def validate_workspace_contract(
    root: str | Path, scenario_name: Optional[str] = None
) -> Dict[str, Any]:
    path = Path(root).expanduser().resolve()
    manifest = load_workspace(path)
    scenario = resolve_workspace_scenario(path, manifest, scenario_name)
    asset = build_workspace_scenario_asset(
        load_workspace_blueprint_asset(path), scenario
    )
    compiled = _load_precompiled_workspace_blueprint(
        path, manifest, scenario, asset
    ) or compile_blueprint(asset)
    contract = load_workspace_contract(path, scenario.name)
    focus_hints = set(compiled.workflow_defaults.focus_hints)
    missing_tools = sorted(
        tool
        for tool in contract.observation_boundary.allowed_tools
        if tool not in set(compiled.workflow_defaults.allowed_tools)
    )
    unsupported_focuses = sorted(
        focus
        for focus in contract.observation_boundary.focus_hints
        if focus not in focus_hints and focus != "summary"
    )
    return {
        "ok": not missing_tools and not unsupported_focuses,
        "missing_tools": missing_tools,
        "unsupported_focuses": unsupported_focuses,
        "compiled_allowed_tools": list(compiled.workflow_defaults.allowed_tools),
        "compiled_focus_hints": list(compiled.workflow_defaults.focus_hints),
    }


def diff_workspace_contract(
    root: str | Path,
    *,
    scenario_name: Optional[str] = None,
    other_path: Optional[str | Path] = None,
) -> Dict[str, Any]:
    current = load_workspace_contract(root, scenario_name).model_dump(mode="json")
    if other_path is None:
        path = Path(root).expanduser().resolve()
        manifest = load_workspace(path)
        scenario = resolve_workspace_scenario(path, manifest, scenario_name)
        compiled_contract = _read_model(
            path / manifest.compiled_root / scenario.name / "contract_effective.json",
            ContractSpec,
        ).model_dump(mode="json")
    else:
        compiled_contract = _read_model(Path(other_path), ContractSpec).model_dump(
            mode="json"
        )
    return _json_diff(current, compiled_contract)


def evaluate_workspace_contract_against_state(
    *,
    root: str | Path,
    scenario_name: Optional[str] = None,
    oracle_state: Dict[str, Any],
    visible_observation: Optional[Dict[str, Any]] = None,
    result: object | None = None,
    pending: Optional[Dict[str, int]] = None,
    time_ms: int = 0,
    available_tools: Optional[Iterable[str]] = None,
) -> ContractEvaluationResult:
    contract = load_workspace_contract(root, scenario_name)
    return evaluate_contract(
        contract,
        oracle_state=oracle_state,
        visible_observation=visible_observation or {},
        result=result,
        pending=pending or {},
        time_ms=time_ms,
        available_tools=available_tools,
        validation_mode="workspace",
    )


def load_workspace_import_report(root: str | Path) -> NormalizationReport | None:
    path = Path(root).expanduser().resolve()
    manifest = load_workspace(path)
    if not manifest.normalization_report_path:
        return None
    report_path = path / manifest.normalization_report_path
    if not report_path.exists():
        return None
    return _read_model(report_path, NormalizationReport)


def load_workspace_redaction_reports(root: str | Path) -> list[RedactionReport]:
    path = Path(root).expanduser().resolve()
    manifest = load_workspace(path)
    if not manifest.redaction_report_path:
        return []
    report_path = path / manifest.redaction_report_path
    if not report_path.exists():
        return []
    payload = json.loads(report_path.read_text(encoding="utf-8"))
    return [RedactionReport.model_validate(item) for item in payload]


def load_workspace_provenance(
    root: str | Path, object_ref: Optional[str] = None
) -> list[ProvenanceRecord]:
    path = Path(root).expanduser().resolve()
    manifest = load_workspace(path)
    if not manifest.provenance_path:
        return []
    provenance_path = path / manifest.provenance_path
    if not provenance_path.exists():
        return []
    payload = json.loads(provenance_path.read_text(encoding="utf-8"))
    records = [ProvenanceRecord.model_validate(item) for item in payload]
    if object_ref:
        return [item for item in records if item.object_ref == object_ref]
    return records


def load_workspace_import_review(root: str | Path) -> ImportReview | None:
    path = Path(root).expanduser().resolve()
    manifest = load_workspace(path)
    if not manifest.import_package_path:
        return None
    package_path = path / manifest.import_package_path
    if not package_path.exists():
        return None
    report = load_workspace_import_report(path)
    if report is None:
        return None
    package = _read_model(package_path, ImportPackage)
    overrides_root = path / manifest.imports_dir / "overrides"
    overrides = []
    if overrides_root.exists():
        for item in sorted(overrides_root.glob("*.json")):
            overrides.append(_read_model(item, MappingOverrideSpec))
    return ImportReview(
        package=package,
        normalization_report=report,
        redaction_reports=load_workspace_redaction_reports(path),
        generated_scenarios=load_workspace_generated_scenarios(path),
        source_overrides=overrides,
        suggested_override_paths={
            source.source_id: str(
                Path(manifest.imports_dir) / "overrides" / f"{source.source_id}.json"
            )
            for source in package.sources
        },
    )


def _refresh_workspace_import_from_package(
    root: Path,
    *,
    package_path: Path,
) -> WorkspaceManifest:
    manifest = load_workspace(root)
    import_artifacts = normalize_identity_import_package(package_path)
    grounding_bundle = import_artifacts.normalized_bundle
    if grounding_bundle is None:
        raise ValueError(
            "synced source package could not be normalized into a grounding bundle"
        )
    asset = compile_identity_governance_bundle(grounding_bundle)
    manifest.source_kind = "import_package"
    manifest.source_ref = str(package_path.relative_to(root))
    manifest.import_package_path = str(package_path.relative_to(root))
    manifest.grounding_bundle_path = "imports/normalized_bundle.json"
    manifest.normalization_report_path = "imports/normalization_report.json"
    manifest.provenance_path = "imports/provenance.json"
    manifest.redaction_report_path = "imports/redaction_reports.json"
    manifest.generated_scenarios_path = "imports/generated_scenarios.json"
    _write_json(root / WORKSPACE_MANIFEST, manifest.model_dump(mode="json"))
    _write_json(root / manifest.blueprint_asset_path, asset.model_dump(mode="json"))
    _write_json(
        root / manifest.grounding_bundle_path,
        grounding_bundle.model_dump(mode="json"),
    )
    _write_json(
        root / manifest.normalization_report_path,
        import_artifacts.normalization_report.model_dump(mode="json"),
    )
    _write_json(
        root / manifest.provenance_path,
        [item.model_dump(mode="json") for item in import_artifacts.provenance],
    )
    _write_json(
        root / manifest.redaction_report_path,
        [item.model_dump(mode="json") for item in import_artifacts.redaction_reports],
    )
    _write_json(
        root / manifest.generated_scenarios_path,
        [item.model_dump(mode="json") for item in import_artifacts.generated_scenarios],
    )
    if manifest.scenarios:
        default = manifest.scenarios[0]
        default.title = asset.title
        default.description = asset.description
        default.scenario_name = asset.scenario_name
        default.workflow_name = asset.workflow_name
        default.workflow_variant = asset.workflow_variant
        default.workflow_parameters = dict(asset.workflow_parameters)
        default.metadata.update(
            {
                "source_kind": manifest.source_kind,
                "source_ref": manifest.source_ref,
                "refreshed_from_sync": True,
            }
        )
        _write_json(
            _scenario_entry_path(root, manifest, default),
            default.model_dump(mode="json"),
        )
    write_workspace(root, manifest)
    compile_workspace(root)
    return manifest


def load_workspace_generated_scenarios(
    root: str | Path,
) -> list[GeneratedScenarioCandidate]:
    path = Path(root).expanduser().resolve()
    manifest = load_workspace(path)
    if not manifest.generated_scenarios_path:
        return []
    scenarios_path = path / manifest.generated_scenarios_path
    if not scenarios_path.exists():
        return []
    payload = json.loads(scenarios_path.read_text(encoding="utf-8"))
    return [GeneratedScenarioCandidate.model_validate(item) for item in payload]


def generate_workspace_scenarios_from_import(
    root: str | Path,
    *,
    replace_generated: bool = False,
) -> list[WorkspaceScenarioSpec]:
    path = Path(root).expanduser().resolve()
    manifest = load_workspace(path)
    candidates = load_workspace_generated_scenarios(path)
    if not candidates:
        raise ValueError("workspace has no generated scenario candidates")
    generated_specs = [
        WorkspaceScenarioSpec(
            name=item.name,
            title=item.title,
            description=item.description,
            scenario_name=item.scenario_name,
            workflow_name=item.workflow_name,
            workflow_variant=item.workflow_variant,
            workflow_parameters=dict(item.workflow_parameters),
            inspection_focus=item.inspection_focus,
            tags=list(item.tags),
            hidden_faults=dict(item.hidden_faults),
            actor_hints=list(item.actor_hints),
            contract_overrides=dict(item.contract_overrides),
            metadata={"generated_from_import": True, **dict(item.metadata)},
        )
        for item in candidates
    ]
    preserved = [manifest.scenarios[0]]
    if not replace_generated:
        preserved.extend(
            item
            for item in manifest.scenarios[1:]
            if not item.metadata.get("generated_from_import")
        )
    manifest.scenarios = preserved + generated_specs
    write_workspace(path, manifest)
    for scenario in manifest.scenarios:
        _write_json(
            _scenario_entry_path(path, manifest, scenario),
            scenario.model_dump(mode="json"),
        )
    compile_workspace(path)
    return generated_specs


def activate_workspace_scenario(
    root: str | Path,
    scenario_name: str,
    *,
    bootstrap_contract: bool = False,
) -> WorkspaceScenarioSpec:
    path = Path(root).expanduser().resolve()
    manifest = load_workspace(path)
    scenario = resolve_workspace_scenario(path, manifest, scenario_name)
    manifest.active_scenario = scenario.name
    write_workspace(path, manifest)
    if bootstrap_contract:
        bootstrap_workspace_contract(path, scenario_name=scenario.name, overwrite=True)
    compile_workspace(path)
    return resolve_workspace_scenario(path, scenario_name=scenario.name)


def bootstrap_workspace_contract(
    root: str | Path,
    *,
    scenario_name: Optional[str] = None,
    overwrite: bool = False,
) -> ContractSpec:
    path = Path(root).expanduser().resolve()
    manifest = load_workspace(path)
    scenario = resolve_workspace_scenario(path, manifest, scenario_name)
    contract_path = _resolve_contract_path(path, manifest, scenario)
    if contract_path.exists() and not overwrite:
        return load_workspace_contract(path, scenario.name)
    if contract_path.exists():
        contract_path.unlink()
    compiled = _read_model(
        path / manifest.compiled_root / scenario.name / "blueprint.json",
        CompiledBlueprint,
    )
    contract, _ = load_or_bootstrap_contract(path, manifest, scenario, compiled)
    compile_workspace(path)
    return contract


def list_workspace_runs(root: str | Path) -> list[WorkspaceRunEntry]:
    path = Path(root).expanduser().resolve()
    manifest = load_workspace(path)
    index_path = path / manifest.runs_index_path
    if not index_path.exists():
        return []
    payload = json.loads(index_path.read_text(encoding="utf-8"))
    return [WorkspaceRunEntry.model_validate(item) for item in payload]


def write_workspace_runs(root: str | Path, entries: list[WorkspaceRunEntry]) -> None:
    path = Path(root).expanduser().resolve()
    manifest = load_workspace(path)
    index_path = path / manifest.runs_index_path
    index_path.parent.mkdir(parents=True, exist_ok=True)
    _write_json(index_path, [item.model_dump(mode="json") for item in entries])


def upsert_workspace_run(
    root: str | Path, entry: WorkspaceRunEntry
) -> WorkspaceRunEntry:
    entries = list_workspace_runs(root)
    others = [item for item in entries if item.run_id != entry.run_id]
    others.append(entry)
    others.sort(key=lambda item: item.started_at, reverse=True)
    write_workspace_runs(root, others)
    return entry


def resolve_workspace_scenario(
    root: str | Path,
    manifest: Optional[WorkspaceManifest] = None,
    scenario_name: Optional[str] = None,
) -> WorkspaceScenarioSpec:
    path = Path(root).expanduser().resolve()
    resolved_manifest = manifest or load_workspace(path)
    key = scenario_name or resolved_manifest.active_scenario
    for scenario in resolved_manifest.scenarios:
        if scenario.name == key:
            return scenario
    raise ValueError(f"workspace scenario not found: {key}")


def build_workspace_scenario_asset(
    asset: BlueprintAsset, scenario: WorkspaceScenarioSpec
) -> BlueprintAsset:
    payload = asset.model_dump(mode="python")
    metadata = dict(scenario.metadata or {})
    variant = _resolve_workspace_vertical_scenario_variant(metadata)
    if variant is not None:
        payload = apply_fault_overlays(payload, variant.fault_overlays)
    payload.update(
        {
            "scenario_name": scenario.scenario_name or asset.scenario_name,
            "workflow_name": scenario.workflow_name or asset.workflow_name,
            "workflow_variant": scenario.workflow_variant or asset.workflow_variant,
            "workflow_parameters": {
                **dict(asset.workflow_parameters),
                **dict(scenario.workflow_parameters),
            },
        }
    )
    payload["metadata"] = {
        **dict(asset.metadata),
        "workspace_scenario": scenario.name,
        "workspace_scenario_title": scenario.title,
        "workspace_scenario_description": scenario.description,
        "hidden_faults": dict(scenario.hidden_faults),
        "actor_hints": list(scenario.actor_hints),
        "contract_overrides": dict(scenario.contract_overrides),
        "scenario_change_summary": (
            list(variant.change_summary) or overlay_summaries(variant.fault_overlays)
            if variant is not None
            else []
        ),
        **dict(scenario.metadata),
    }
    if payload.get("capability_graphs") and variant is not None:
        payload["capability_graphs"]["metadata"] = {
            **dict(payload["capability_graphs"].get("metadata") or {}),
            "active_scenario_variant": variant.name,
            "variant_branch_labels": list(variant.branch_labels),
            "variant_rationale": variant.rationale,
            "variant_change_summary": list(variant.change_summary)
            or overlay_summaries(variant.fault_overlays),
        }
        if variant.branch_labels:
            payload["capability_graphs"]["metadata"]["what_if_branches"] = list(
                variant.branch_labels
            )
    return BlueprintAsset.model_validate(payload)


def load_or_bootstrap_contract(
    root: str | Path,
    manifest: WorkspaceManifest,
    scenario: WorkspaceScenarioSpec,
    compiled: CompiledBlueprint,
) -> tuple[ContractSpec, bool]:
    resolved_root = Path(root).expanduser().resolve()
    contract_path = _resolve_contract_path(resolved_root, manifest, scenario)
    if contract_path.exists():
        return _read_model(contract_path, ContractSpec), False
    if compiled.workflow_name is None:
        contract = ContractSpec(
            name=f"{scenario.name}.contract",
            workflow_name=scenario.workflow_name or "workspace",
            scenario_name=compiled.scenario.name,
            metadata={"source": "workspace_bootstrap"},
        )
    else:
        workflow_spec = get_benchmark_family_workflow_spec(
            compiled.workflow_name,
            variant_name=compiled.workflow_variant,
            parameter_overrides=scenario.workflow_parameters,
        )
        contract = build_contract_from_workflow(workflow_spec)
    bundle = _load_workspace_grounding_bundle(resolved_root, manifest)
    if bundle is not None and manifest.source_kind in {
        "import_package",
        "grounding_bundle",
    }:
        contract = ContractSpec.model_validate(
            bootstrap_contract_from_import_bundle(
                bundle=bundle,
                contract_payload=contract.model_dump(mode="json"),
                scenario_name=scenario.name,
                workflow_parameters={
                    **dict(compiled.asset.workflow_parameters),
                    **dict(scenario.workflow_parameters),
                },
            )
        )
    if scenario.contract_overrides:
        contract = ContractSpec.model_validate(
            _deep_merge(contract.model_dump(mode="json"), scenario.contract_overrides)
        )
    base_contract = contract
    contract = _apply_workspace_contract_variant(
        resolved_root, manifest, scenario, contract
    )
    contract_path.parent.mkdir(parents=True, exist_ok=True)
    base_payload = base_contract.model_dump(mode="json")
    base_payload["metadata"] = {
        **dict(base_payload.get("metadata") or {}),
        "base_contract": True,
    }
    for key in (
        "vertical_contract_variant",
        "vertical_contract_variant_title",
        "vertical_contract_objective_summary",
        "vertical_contract_rationale",
    ):
        base_payload["metadata"].pop(key, None)
    base_contract = ContractSpec.model_validate(base_payload)
    _write_json(contract_path, base_contract.model_dump(mode="json"))
    return contract, True
