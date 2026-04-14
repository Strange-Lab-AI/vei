from __future__ import annotations

import json
from datetime import UTC, datetime
from hashlib import sha1
from pathlib import Path
from typing import Any

from fastapi.testclient import TestClient

from vei.run.api import (
    get_workspace_run_manifest_path,
    launch_workspace_run,
    load_run_manifest,
)
from vei.synthesis.api import synthesize_training_set
from vei.synthesis.models import TrainingFormat
from vei.twin import (
    build_twin_matrix,
    create_twin_gateway_app,
    load_customer_twin,
)
from vei.context.api import ContextSnapshot
from vei.twin.models import (
    ContextMoldConfig,
    TwinArchetype,
    TwinMatrixBundle,
    TwinVariantSpec,
)
from vei.workspace.api import load_workspace, preview_workspace_scenario

from .models import (
    DatasetBuildSpec,
    DatasetBundle,
    DatasetExampleManifest,
    DatasetRunRecord,
    DatasetSplitManifest,
    DatasetSplitName,
)

DATASET_BUNDLE_FILE = "dataset_bundle.json"
WORKSPACE_DATASET_FILE = "dataset_latest.json"


def build_dataset_bundle(spec: DatasetBuildSpec) -> DatasetBundle:
    output_root = spec.output_root.expanduser().resolve()
    output_root.mkdir(parents=True, exist_ok=True)

    matrix = _resolve_matrix(spec)
    variants = _resolve_variants(spec, matrix)
    formats = spec.formats or [
        "conversations",
        "trajectories",
        "demonstrations",
    ]

    aggregated_examples: dict[
        tuple[TrainingFormat, DatasetSplitName], list[dict[str, Any]]
    ] = {}
    export_run_ids: dict[tuple[TrainingFormat, DatasetSplitName], set[str]] = {}
    run_records: list[DatasetRunRecord] = []

    for variant in variants:
        split = _assign_split(variant.variant_id)
        run_ids = _run_variant_paths(
            variant, include_external=spec.include_external_sample
        )
        for run_id in run_ids:
            manifest = load_run_manifest(
                get_workspace_run_manifest_path(variant.workspace_root, run_id)
            )
            record = DatasetRunRecord(
                workspace_root=variant.workspace_root,
                variant_id=variant.variant_id,
                archetype=variant.archetype,
                scenario_variant=variant.scenario_variant,
                contract_variant=variant.contract_variant,
                density_level=variant.density_level,
                crisis_level=variant.crisis_level,
                run_id=run_id,
                runner=manifest.runner,
                split=split,
                status=manifest.status,
                success=manifest.success,
                contract_ok=manifest.contract.ok,
                issue_count=manifest.contract.issue_count,
                action_count=int(manifest.metrics.actions or 0),
            )
            run_records.append(record)
            for format_name in formats:
                training_set = synthesize_training_set(
                    variant.workspace_root,
                    [run_id],
                    format=format_name,
                )
                key = (format_name, split)
                aggregated_examples.setdefault(key, [])
                export_run_ids.setdefault(key, set())
                export_run_ids[key].add(run_id)
                for example in training_set.examples:
                    payload = example.model_dump(mode="json")
                    payload["variant_id"] = variant.variant_id
                    payload["archetype"] = variant.archetype
                    payload["split"] = split
                    aggregated_examples[key].append(payload)

    exports = _write_exports(output_root, aggregated_examples, export_run_ids)
    splits = _build_split_manifests(run_records, exports)
    bundle = DatasetBundle(
        spec=spec,
        environment_count=len(variants),
        run_count=len(run_records),
        runs=run_records,
        exports=exports,
        splits=splits,
        reward_summary=_reward_summary(run_records),
        matrix_path=_matrix_path(output_root, matrix),
        generated_at=_iso_now(),
    )
    (output_root / DATASET_BUNDLE_FILE).write_text(
        bundle.model_dump_json(indent=2),
        encoding="utf-8",
    )
    _write_workspace_dataset_views(bundle, variants)
    return bundle


def load_dataset_bundle(root: str | Path) -> DatasetBundle:
    path = Path(root).expanduser().resolve()
    direct_path = path / DATASET_BUNDLE_FILE
    if direct_path.exists():
        return DatasetBundle.model_validate_json(
            direct_path.read_text(encoding="utf-8")
        )
    workspace_path = path / WORKSPACE_DATASET_FILE
    if workspace_path.exists():
        return DatasetBundle.model_validate_json(
            workspace_path.read_text(encoding="utf-8")
        )
    raise FileNotFoundError(f"dataset bundle not found under {path}")


def load_workspace_dataset_bundle(root: str | Path) -> DatasetBundle | None:
    path = Path(root).expanduser().resolve() / WORKSPACE_DATASET_FILE
    if not path.exists():
        return None
    return DatasetBundle.model_validate_json(path.read_text(encoding="utf-8"))


def _resolve_matrix(spec: DatasetBuildSpec) -> TwinMatrixBundle | None:
    if spec.workspace_roots:
        return None
    snapshot = None
    if spec.snapshot_path:
        snapshot = ContextSnapshot.model_validate_json(
            Path(spec.snapshot_path).expanduser().resolve().read_text(encoding="utf-8")
        )
    return build_twin_matrix(
        spec.output_root / "matrix",
        snapshot=snapshot,
        organization_name=spec.organization_name or None,
        organization_domain=spec.organization_domain,
        archetypes=spec.archetypes,
        density_levels=spec.density_levels,
        crisis_levels=spec.crisis_levels,
        seeds=spec.seeds,
    )


def _resolve_variants(
    spec: DatasetBuildSpec,
    matrix: TwinMatrixBundle | None,
) -> list[TwinVariantSpec]:
    if matrix is not None:
        return matrix.variants
    variants: list[TwinVariantSpec] = []
    workspace_roots = [item.expanduser().resolve() for item in spec.workspace_roots]
    for index, workspace_root in enumerate(workspace_roots):
        preview = preview_workspace_scenario(workspace_root)
        workspace = load_workspace(workspace_root)
        variants.append(
            TwinVariantSpec(
                variant_id=f"{workspace_root.name}-{index + 1}",
                workspace_root=workspace_root,
                organization_name=workspace.title or workspace.name,
                archetype=_workspace_archetype(workspace.source_ref),
                density_level=(
                    spec.density_levels[0] if spec.density_levels else "medium"
                ),
                crisis_level=(
                    spec.crisis_levels[0] if spec.crisis_levels else "escalated"
                ),
                mold=ContextMoldConfig(
                    archetype=_workspace_archetype(workspace.source_ref),
                    density_level=(
                        spec.density_levels[0] if spec.density_levels else "medium"
                    ),
                    crisis_family=(
                        spec.crisis_levels[0] if spec.crisis_levels else "escalated"
                    ),
                ),
                scenario_variant=str(
                    preview.get("active_scenario_variant") or workspace.active_scenario
                ),
                contract_variant=str(preview.get("active_contract_variant") or "")
                or None,
            )
        )
    return variants


def _run_variant_paths(
    variant: TwinVariantSpec,
    *,
    include_external: bool,
) -> list[str]:
    run_ids = [
        launch_workspace_run(variant.workspace_root, runner="workflow").run_id,
        launch_workspace_run(variant.workspace_root, runner="scripted").run_id,
    ]
    if include_external:
        external_run_id = _run_external_sample(variant.workspace_root)
        if external_run_id:
            run_ids.append(external_run_id)
    return run_ids


def _run_external_sample(workspace_root: Path) -> str | None:
    try:
        bundle = load_customer_twin(workspace_root)
    except FileNotFoundError:
        return None

    app = create_twin_gateway_app(workspace_root)
    headers = {
        "Authorization": f"Bearer {bundle.gateway.auth_token}",
        "X-VEI-Agent-Name": "dataset-builder",
        "X-VEI-Agent-Role": "synthetic-evaluator",
        "X-VEI-Agent-Team": "generate",
        "User-Agent": "vei-dataset-builder/1.0",
    }
    run_id: str | None = None
    with TestClient(app) as client:
        channels = client.get("/slack/api/conversations.list", headers=headers).json()
        client.get("/jira/rest/api/3/search", headers=headers)
        client.get("/graph/v1.0/me/messages", headers=headers)
        client.get(
            "/salesforce/services/data/v60.0/query",
            params={"q": "SELECT Id, Name FROM Opportunity LIMIT 2"},
            headers=headers,
        )
        channel_items = (
            channels.get("channels", []) if isinstance(channels, dict) else []
        )
        if channel_items:
            channel_id = str(channel_items[0].get("id", ""))
            client.post(
                "/slack/api/chat.postMessage",
                headers=headers,
                json={
                    "channel": channel_id,
                    "text": "Dataset capture check-in: systems responded and the company state changed.",
                },
            )
        status_payload = client.get("/api/twin", headers=headers).json()
        if isinstance(status_payload, dict):
            runtime = status_payload.get("runtime", {})
            if isinstance(runtime, dict):
                run_id = str(runtime.get("run_id", "")) or None
    return run_id


def _assign_split(variant_id: str) -> DatasetSplitName:
    digest = sha1(variant_id.encode("utf-8"), usedforsecurity=False).hexdigest()
    bucket = int(digest[:2], 16)
    if bucket < 179:
        return "train"
    if bucket < 217:
        return "validation"
    return "test"


def _write_exports(
    output_root: Path,
    aggregated_examples: dict[
        tuple[TrainingFormat, DatasetSplitName], list[dict[str, Any]]
    ],
    export_run_ids: dict[tuple[TrainingFormat, DatasetSplitName], set[str]],
) -> list[DatasetExampleManifest]:
    exports_dir = output_root / "exports"
    exports_dir.mkdir(parents=True, exist_ok=True)
    exports: list[DatasetExampleManifest] = []
    for (format_name, split), examples in sorted(aggregated_examples.items()):
        file_name = f"{split}_{format_name}.json"
        export_path = exports_dir / file_name
        payload = {
            "format": format_name,
            "split": split,
            "example_count": len(examples),
            "run_ids": sorted(export_run_ids.get((format_name, split), set())),
            "examples": examples,
        }
        export_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
        exports.append(
            DatasetExampleManifest(
                format=format_name,
                split=split,
                example_count=len(examples),
                path=str(export_path.relative_to(output_root)),
            )
        )
    return exports


def _build_split_manifests(
    runs: list[DatasetRunRecord],
    exports: list[DatasetExampleManifest],
) -> list[DatasetSplitManifest]:
    manifests: list[DatasetSplitManifest] = []
    for split in ("train", "validation", "test"):
        split_runs = [item for item in runs if item.split == split]
        split_exports = [item for item in exports if item.split == split]
        manifests.append(
            DatasetSplitManifest(
                split=split,
                run_count=len(split_runs),
                example_count=sum(item.example_count for item in split_exports),
                run_ids=[item.run_id for item in split_runs],
            )
        )
    return manifests


def _reward_summary(runs: list[DatasetRunRecord]) -> dict[str, float]:
    if not runs:
        return {"success_rate": 0.0, "contract_ok_rate": 0.0, "avg_action_count": 0.0}
    success_count = sum(1 for item in runs if item.success is True)
    contract_ok_count = sum(1 for item in runs if item.contract_ok is True)
    action_total = sum(item.action_count for item in runs)
    return {
        "success_rate": round(success_count / len(runs), 4),
        "contract_ok_rate": round(contract_ok_count / len(runs), 4),
        "avg_action_count": round(action_total / len(runs), 2),
    }


def _matrix_path(output_root: Path, matrix: TwinMatrixBundle | None) -> str | None:
    if matrix is None:
        return None
    return str((output_root / "matrix" / "twin_matrix.json").relative_to(output_root))


def _write_workspace_dataset_views(
    bundle: DatasetBundle,
    variants: list[TwinVariantSpec],
) -> None:
    seen: set[Path] = set()
    for variant in variants:
        workspace_root = variant.workspace_root.resolve()
        if workspace_root in seen:
            continue
        seen.add(workspace_root)
        (workspace_root / WORKSPACE_DATASET_FILE).write_text(
            bundle.model_dump_json(indent=2),
            encoding="utf-8",
        )


def _workspace_archetype(source_ref: str | None) -> TwinArchetype:
    if source_ref == "b2b_saas":
        return "b2b_saas"
    if source_ref == "digital_marketing_agency":
        return "digital_marketing_agency"
    if source_ref == "real_estate_management":
        return "real_estate_management"
    if source_ref == "storage_solutions":
        return "storage_solutions"
    return "b2b_saas"


def _iso_now() -> str:
    return datetime.now(UTC).isoformat()
