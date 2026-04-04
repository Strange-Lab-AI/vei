from __future__ import annotations

import json
import shutil
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Dict, Iterator, Optional, TypeVar

from pydantic import BaseModel

from vei.blueprint.models import BlueprintAsset, CompiledBlueprint
from vei.contract.models import ContractSpec
from vei.grounding.models import IdentityGovernanceBundle
from vei.imports.models import ImportPackageArtifacts
from vei.verticals import (
    apply_vertical_contract_variant,
    default_vertical_contract_variant,
    default_vertical_scenario_variant,
    get_vertical_contract_variant,
    get_vertical_scenario_variant,
)

from .models import (
    WorkspaceImportSummary,
    WorkspaceManifest,
    WorkspaceScenarioSpec,
    WorkspaceSourceConfig,
    WorkspaceSourceSyncRecord,
)

WORKSPACE_MANIFEST = "vei_project.json"
_MODEL_T = TypeVar("_MODEL_T", bound=BaseModel)


@contextmanager
def temporary_env(name: str, value: str | None) -> Iterator[None]:
    import os

    previous = os.environ.get(name)
    if value is None:
        os.environ.pop(name, None)
    else:
        os.environ[name] = value
    try:
        yield
    finally:
        if previous is None:
            os.environ.pop(name, None)
        else:
            os.environ[name] = previous


def _bootstrap_workspace(
    *,
    root: Path,
    asset: BlueprintAsset,
    source_kind: str,
    source_ref: Optional[str],
    grounding_bundle: IdentityGovernanceBundle | None = None,
    import_artifacts: ImportPackageArtifacts | None = None,
    precompiled_blueprint: CompiledBlueprint | None = None,
    name: Optional[str] = None,
    title: Optional[str] = None,
    description: Optional[str] = None,
) -> WorkspaceManifest:
    created_at = _iso_now()
    workspace_name = name or asset.name.replace(".blueprint", "").replace(".", "_")
    builder_environment_metadata: dict[str, Any] = {}
    if asset.capability_graphs is not None:
        builder_environment_metadata = dict(asset.capability_graphs.metadata or {})
    manifest = WorkspaceManifest(
        name=workspace_name,
        title=title or asset.title,
        description=description or asset.description,
        created_at=created_at,
        source_kind=source_kind,  # type: ignore[arg-type]
        source_ref=source_ref,
        grounding_bundle_path=(
            "imports/normalized_bundle.json"
            if import_artifacts is not None
            else (
                "sources/grounding_bundle.json"
                if grounding_bundle is not None
                else None
            )
        ),
        import_package_path=(
            "imports/package_manifest.json" if import_artifacts is not None else None
        ),
        normalization_report_path=(
            "imports/normalization_report.json"
            if import_artifacts is not None
            else None
        ),
        provenance_path=(
            "imports/provenance.json" if import_artifacts is not None else None
        ),
        redaction_report_path=(
            "imports/redaction_reports.json" if import_artifacts is not None else None
        ),
        generated_scenarios_path=(
            "imports/generated_scenarios.json" if import_artifacts is not None else None
        ),
        scenarios=[
            WorkspaceScenarioSpec(
                name="default",
                title=asset.title,
                description=asset.description,
                scenario_name=asset.scenario_name,
                workflow_name=asset.workflow_name,
                workflow_variant=asset.workflow_variant,
                workflow_parameters=dict(asset.workflow_parameters),
                contract_path="contracts/default.contract.json",
                inspection_focus="summary",
                metadata={
                    "source_kind": source_kind,
                    "source_ref": source_ref,
                    **(
                        {"builder_environment": builder_environment_metadata}
                        if builder_environment_metadata
                        else {}
                    ),
                    **(
                        {
                            "precompiled_blueprint_path": "sources/compiled_blueprint.json"
                        }
                        if precompiled_blueprint is not None
                        else {}
                    ),
                },
            )
        ],
        metadata=(
            {"precompiled_blueprint_path": "sources/compiled_blueprint.json"}
            if precompiled_blueprint is not None
            else {}
        ),
    )
    (root / "sources").mkdir(parents=True, exist_ok=True)
    (root / manifest.imports_dir).mkdir(parents=True, exist_ok=True)
    (root / manifest.contracts_dir).mkdir(parents=True, exist_ok=True)
    (root / manifest.scenarios_dir).mkdir(parents=True, exist_ok=True)
    (root / manifest.compiled_root).mkdir(parents=True, exist_ok=True)
    (root / manifest.runs_dir).mkdir(parents=True, exist_ok=True)
    _write_json(root / WORKSPACE_MANIFEST, manifest.model_dump(mode="json"))
    _write_json(root / manifest.blueprint_asset_path, asset.model_dump(mode="json"))
    if grounding_bundle is not None and manifest.grounding_bundle_path is not None:
        _write_json(
            root / manifest.grounding_bundle_path,
            grounding_bundle.model_dump(mode="json"),
        )
    if import_artifacts is not None:
        _write_json(
            root / manifest.import_package_path,
            import_artifacts.package.model_dump(mode="json"),
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
            [
                item.model_dump(mode="json")
                for item in import_artifacts.redaction_reports
            ],
        )
        _write_json(
            root / manifest.generated_scenarios_path,
            [
                item.model_dump(mode="json")
                for item in import_artifacts.generated_scenarios
            ],
        )
    if precompiled_blueprint is not None:
        _write_json(
            root / "sources" / "compiled_blueprint.json",
            precompiled_blueprint.model_dump(mode="json"),
        )
    _write_json(
        _scenario_entry_path(root, manifest, manifest.scenarios[0]),
        manifest.scenarios[0].model_dump(mode="json"),
    )
    _write_json(root / manifest.runs_index_path, [])
    if manifest.source_registry_path:
        _write_json(root / manifest.source_registry_path, [])
    if manifest.source_sync_history_path:
        _write_json(root / manifest.source_sync_history_path, [])
    return manifest


def _ensure_workspace_root(root: str | Path, *, overwrite: bool) -> Path:
    path = Path(root).expanduser().resolve()
    if path.exists():
        if not path.is_dir():
            raise ValueError(f"workspace root is not a directory: {path}")
        if any(path.iterdir()):
            if not overwrite:
                raise ValueError(
                    f"workspace root already exists and is not empty: {path}"
                )
            shutil.rmtree(path)
    path.mkdir(parents=True, exist_ok=True)
    return path


def _load_precompiled_workspace_blueprint(
    root: Path,
    manifest: WorkspaceManifest,
    scenario: WorkspaceScenarioSpec,
    asset: BlueprintAsset,
) -> CompiledBlueprint | None:
    compiled_path = scenario.metadata.get(
        "precompiled_blueprint_path"
    ) or manifest.metadata.get("precompiled_blueprint_path")
    if not compiled_path:
        return None
    if not _scenario_matches_blueprint_asset(asset, scenario):
        return None
    path = root / str(compiled_path)
    if not path.exists():
        return None
    return _read_model(path, CompiledBlueprint)


def _scenario_matches_blueprint_asset(
    asset: BlueprintAsset, scenario: WorkspaceScenarioSpec
) -> bool:
    return (
        (scenario.scenario_name or asset.scenario_name) == asset.scenario_name
        and (scenario.workflow_name or asset.workflow_name) == asset.workflow_name
        and (scenario.workflow_variant or asset.workflow_variant)
        == asset.workflow_variant
        and dict(scenario.workflow_parameters) == dict(asset.workflow_parameters)
    )


def _resolve_contract_path(
    root: Path, manifest: WorkspaceManifest, scenario: WorkspaceScenarioSpec
) -> Path:
    contract_path = (
        scenario.contract_path
        or f"{manifest.contracts_dir}/{scenario.name}.contract.json"
    )
    return root / contract_path


def _workspace_vertical_name(manifest: WorkspaceManifest) -> str | None:
    if manifest.source_kind != "vertical" or not manifest.source_ref:
        return None
    return manifest.source_ref.strip().lower()


def _active_vertical_scenario_variant_name(
    root: str | Path, manifest: WorkspaceManifest
) -> str | None:
    from .api import resolve_workspace_scenario

    vertical_name = _workspace_vertical_name(manifest)
    if vertical_name is None:
        return None
    scenario = resolve_workspace_scenario(root, manifest)
    return str(
        scenario.metadata.get("vertical_scenario_variant")
        or scenario.workflow_variant
        or default_vertical_scenario_variant(vertical_name).name
    )


def _active_vertical_contract_variant_name(
    root: str | Path, manifest: WorkspaceManifest
) -> str | None:
    from .api import resolve_workspace_scenario

    vertical_name = _workspace_vertical_name(manifest)
    if vertical_name is None:
        return None
    scenario = resolve_workspace_scenario(root, manifest)
    return str(
        scenario.metadata.get("vertical_contract_variant")
        or default_vertical_contract_variant(vertical_name).name
    )


def _resolve_workspace_vertical_scenario_variant(
    metadata: dict[str, Any],
) -> Any | None:
    vertical_name = metadata.get("vertical")
    variant_name = metadata.get("vertical_scenario_variant")
    if not vertical_name or not variant_name:
        return None
    try:
        return get_vertical_scenario_variant(str(vertical_name), str(variant_name))
    except KeyError:
        return None


def _resolve_workspace_vertical_contract_variant(
    manifest: WorkspaceManifest, scenario: WorkspaceScenarioSpec
):
    vertical_name = _workspace_vertical_name(manifest)
    if vertical_name is None:
        return None
    variant_name = (
        scenario.metadata.get("vertical_contract_variant")
        or default_vertical_contract_variant(vertical_name).name
    )
    try:
        return get_vertical_contract_variant(vertical_name, str(variant_name))
    except KeyError:
        return None


def _apply_workspace_contract_variant(
    root: Path,
    manifest: WorkspaceManifest,
    scenario: WorkspaceScenarioSpec,
    contract: ContractSpec,
) -> ContractSpec:
    del root
    variant = _resolve_workspace_vertical_contract_variant(manifest, scenario)
    if variant is None:
        return contract
    return apply_vertical_contract_variant(contract, variant)


def _contract_bootstrapped(root: Path, scenario_root: Path) -> bool:
    del root
    return (scenario_root / ".contract_bootstrapped").exists()


def _scenario_entry_path(
    root: Path, manifest: WorkspaceManifest, scenario: WorkspaceScenarioSpec
) -> Path:
    return root / manifest.scenarios_dir / f"{scenario.name}.json"


def _read_model(path: Path, model: type[_MODEL_T]) -> _MODEL_T:
    return model.model_validate(json.loads(path.read_text(encoding="utf-8")))


def _write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def _load_workspace_grounding_bundle(
    root: Path, manifest: WorkspaceManifest
) -> IdentityGovernanceBundle | None:
    if not manifest.grounding_bundle_path:
        return None
    path = root / manifest.grounding_bundle_path
    if not path.exists():
        return None
    return _read_model(path, IdentityGovernanceBundle)


def _copy_import_sources(
    package_path: Path,
    workspace_root: Path,
    manifest: WorkspaceManifest,
    artifacts: ImportPackageArtifacts,
) -> None:
    package_root = package_path if package_path.is_dir() else package_path.parent
    raw_root = workspace_root / manifest.imports_dir / "raw_sources"
    raw_root.mkdir(parents=True, exist_ok=True)
    for source in artifacts.package.sources:
        source_path = package_root / source.relative_path
        target_path = raw_root / source.relative_path
        target_path.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source_path, target_path)
    overrides_root = package_root / "overrides"
    if overrides_root.exists():
        target_root = workspace_root / manifest.imports_dir / "overrides"
        for source in overrides_root.rglob("*.json"):
            relative = source.relative_to(overrides_root)
            destination = target_root / relative
            destination.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(source, destination)


def _load_workspace_import_summary(
    root: Path, manifest: WorkspaceManifest
) -> WorkspaceImportSummary | None:
    from .api import (
        list_workspace_source_syncs,
        list_workspace_sources,
        load_workspace_generated_scenarios,
        load_workspace_import_report,
        load_workspace_provenance,
    )

    report = load_workspace_import_report(root)
    if report is None:
        return None
    package_name = "import"
    source_count = 0
    if manifest.import_package_path:
        package_path = root / manifest.import_package_path
        if package_path.exists():
            payload = json.loads(package_path.read_text(encoding="utf-8"))
            package_name = str(payload.get("name", package_name))
            source_count = len(payload.get("sources", []))
    provenance = load_workspace_provenance(root)
    origin_counts: Dict[str, int] = {"imported": 0, "derived": 0, "simulated": 0}
    for record in provenance:
        origin_counts[str(record.origin)] = origin_counts.get(str(record.origin), 0) + 1
    generated = load_workspace_generated_scenarios(root)
    sources = list_workspace_sources(root)
    syncs = list_workspace_source_syncs(root)
    return WorkspaceImportSummary(
        package_name=package_name,
        source_count=source_count,
        connected_source_count=len(sources),
        source_sync_count=len(syncs),
        issue_count=report.issue_count,
        warning_count=report.warning_count,
        error_count=report.error_count,
        provenance_count=len(provenance),
        generated_scenario_count=len(generated),
        normalized_counts=dict(report.normalized_counts),
        origin_counts=origin_counts,
    )


def _upsert_workspace_source(
    root: Path, manifest: WorkspaceManifest, entry: WorkspaceSourceConfig
) -> WorkspaceSourceConfig:
    from .api import list_workspace_sources

    entries = list_workspace_sources(root)
    updated = False
    for index, item in enumerate(entries):
        if item.source_id == entry.source_id:
            entries[index] = item.model_copy(
                update={
                    "connector": entry.connector,
                    "config_path": entry.config_path,
                    "updated_at": entry.updated_at,
                    "metadata": _deep_merge(dict(item.metadata), dict(entry.metadata)),
                }
            )
            updated = True
            break
    if not updated:
        entries.append(entry)
    registry_path = root / str(manifest.source_registry_path)
    _write_json(registry_path, [item.model_dump(mode="json") for item in entries])
    return entry


def _append_workspace_source_sync(
    root: Path, manifest: WorkspaceManifest, entry: WorkspaceSourceSyncRecord
) -> WorkspaceSourceSyncRecord:
    from .api import list_workspace_source_syncs

    history = list_workspace_source_syncs(root)
    history.append(entry)
    history_path = root / str(manifest.source_sync_history_path)
    _write_json(history_path, [item.model_dump(mode="json") for item in history])
    return entry


def _deep_merge(left: Dict[str, Any], right: Dict[str, Any]) -> Dict[str, Any]:
    merged = dict(left)
    for key, value in right.items():
        if key in merged and isinstance(merged[key], dict) and isinstance(value, dict):
            merged[key] = _deep_merge(merged[key], value)
        else:
            merged[key] = value
    return merged


def _json_diff(left: Dict[str, Any], right: Dict[str, Any]) -> Dict[str, Any]:
    left_flat: Dict[str, Any] = {}
    right_flat: Dict[str, Any] = {}
    _flatten_json("", left, left_flat)
    _flatten_json("", right, right_flat)
    keys = sorted(set(left_flat) | set(right_flat))
    added = {key: right_flat[key] for key in keys if key not in left_flat}
    removed = {key: left_flat[key] for key in keys if key not in right_flat}
    changed = {
        key: {"from": left_flat[key], "to": right_flat[key]}
        for key in keys
        if key in left_flat and key in right_flat and left_flat[key] != right_flat[key]
    }
    return {"added": added, "removed": removed, "changed": changed}


def _flatten_json(prefix: str, value: Any, out: Dict[str, Any]) -> None:
    if isinstance(value, dict):
        for key, item in value.items():
            next_prefix = f"{prefix}.{key}" if prefix else str(key)
            _flatten_json(next_prefix, item, out)
        return
    out[prefix] = value


def _iso_now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat()
