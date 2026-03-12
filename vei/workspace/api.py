from __future__ import annotations

import json
import shutil
from contextlib import contextmanager
from dataclasses import asdict
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Dict, Iterable, Literal, Optional, TypeVar

from pydantic import BaseModel

from vei.blueprint.api import (
    build_blueprint_asset_for_example,
    build_blueprint_asset_for_family,
    build_blueprint_asset_for_scenario,
    compile_blueprint,
    materialize_scenario_from_blueprint,
)
from vei.blueprint.models import BlueprintAsset, CompiledBlueprint
from vei.benchmark.workflows import get_benchmark_family_workflow_spec
from vei.contract.api import build_contract_from_workflow, evaluate_contract
from vei.contract.models import ContractEvaluationResult, ContractSpec
from vei.grounding.api import compile_identity_governance_bundle
from vei.grounding.models import IdentityGovernanceBundle
from vei.world.manifest import build_scenario_manifest

from .models import (
    WorkspaceCompileRecord,
    WorkspaceManifest,
    WorkspaceRunEntry,
    WorkspaceScenarioSpec,
    WorkspaceSummary,
)


WORKSPACE_MANIFEST = "vei_project.json"
_MODEL_T = TypeVar("_MODEL_T", bound=BaseModel)


def create_workspace_from_template(
    *,
    root: str | Path,
    source_kind: Literal["example", "family", "scenario"],
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
        for value in (bundle_path, blueprint_asset_path, compiled_blueprint_path)
    )
    if selected != 1:
        raise ValueError(
            "import_workspace requires exactly one of bundle_path, blueprint_asset_path, or compiled_blueprint_path"
        )
    path = _ensure_workspace_root(root, overwrite=overwrite)

    grounding_bundle: IdentityGovernanceBundle | None = None
    precompiled_blueprint: CompiledBlueprint | None = None
    if bundle_path is not None:
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
        precompiled_blueprint=precompiled_blueprint,
        source_kind=source_kind,
        source_ref=source_ref,
        name=name,
        title=title,
        description=description,
    )
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
    )


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
    return {
        "workspace": manifest.model_dump(mode="json"),
        "scenario": entry.model_dump(mode="json"),
        "compiled_blueprint": compiled.model_dump(mode="json"),
        "contract": contract.model_dump(mode="json"),
        "scenario_seed": asdict(scenario_seed),
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
    return _read_model(_resolve_contract_path(path, manifest, scenario), ContractSpec)


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
        **dict(scenario.metadata),
    }
    return BlueprintAsset.model_validate(payload)


def load_or_bootstrap_contract(
    root: str | Path,
    manifest: WorkspaceManifest,
    scenario: WorkspaceScenarioSpec,
    compiled: CompiledBlueprint,
) -> tuple[ContractSpec, bool]:
    contract_path = _resolve_contract_path(
        Path(root).expanduser().resolve(), manifest, scenario
    )
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
        )
        contract = build_contract_from_workflow(workflow_spec)
    contract_path.parent.mkdir(parents=True, exist_ok=True)
    _write_json(contract_path, contract.model_dump(mode="json"))
    return contract, True


@contextmanager
def temporary_env(name: str, value: str | None):
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
    precompiled_blueprint: CompiledBlueprint | None = None,
    name: Optional[str] = None,
    title: Optional[str] = None,
    description: Optional[str] = None,
) -> WorkspaceManifest:
    created_at = _iso_now()
    workspace_name = name or asset.name.replace(".blueprint", "").replace(".", "_")
    manifest = WorkspaceManifest(
        name=workspace_name,
        title=title or asset.title,
        description=description or asset.description,
        created_at=created_at,
        source_kind=source_kind,  # type: ignore[arg-type]
        source_ref=source_ref,
        grounding_bundle_path=(
            "sources/grounding_bundle.json" if grounding_bundle is not None else None
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


def _contract_bootstrapped(root: Path, scenario_root: Path) -> bool:
    marker = scenario_root / ".contract_bootstrapped"
    return marker.exists()


def _scenario_entry_path(
    root: Path, manifest: WorkspaceManifest, scenario: WorkspaceScenarioSpec
) -> Path:
    return root / manifest.scenarios_dir / f"{scenario.name}.json"


def _read_model(path: Path, model: type[_MODEL_T]) -> _MODEL_T:
    return model.model_validate(json.loads(path.read_text(encoding="utf-8")))


def _write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


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
