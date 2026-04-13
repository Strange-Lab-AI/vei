from __future__ import annotations

import argparse
import json
import shutil
from pathlib import Path
from typing import Any

from vei.whatif.artifacts import render_experiment_overview
from vei.whatif.business_state import (
    assess_historical_business_state,
    describe_forecast_business_change,
)
from vei.whatif.models import (
    WhatIfEpisodeManifest,
    WhatIfExperimentResult,
    WhatIfCounterfactualEstimateResult,
)
from scripts.build_enron_business_state_example import (
    build_example as build_business_state_example,
)

EXAMPLE_PLACEHOLDER = "not-included-in-repo-example"


def _read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")


def _copy_file(source: Path, target: Path) -> None:
    target.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(source, target)


def _preserve_readme(output_root: Path) -> str | None:
    readme_path = output_root / "README.md"
    if not readme_path.exists():
        return None
    return readme_path.read_text(encoding="utf-8")


def _rewrite_manifest(payload: dict[str, Any]) -> dict[str, Any]:
    updated = dict(payload)
    updated["source_dir"] = EXAMPLE_PLACEHOLDER
    updated["workspace_root"] = "workspace"
    return updated


def _rewrite_forecast_result(payload: dict[str, Any]) -> dict[str, Any]:
    updated = dict(payload)
    artifacts = updated.get("artifacts")
    if isinstance(artifacts, dict):
        updated["artifacts"] = {key: EXAMPLE_PLACEHOLDER for key in artifacts.keys()}
    return updated


def _resolve_forecast_filename(
    source_root: Path,
    *,
    experiment_payload: dict[str, Any] | None = None,
) -> str:
    artifacts = experiment_payload.get("artifacts") if experiment_payload else None
    if isinstance(artifacts, dict):
        raw_path = artifacts.get("forecast_json_path")
        if isinstance(raw_path, str):
            filename = Path(raw_path).name
            if filename and (source_root / filename).exists():
                return filename
    for filename in ("whatif_ejepa_result.json", "whatif_ejepa_proxy_result.json"):
        if (source_root / filename).exists():
            return filename
    raise FileNotFoundError(f"forecast result not found under {source_root}")


def _rewrite_experiment_result(
    payload: dict[str, Any],
    *,
    forecast_filename: str,
) -> dict[str, Any]:
    updated = dict(payload)
    materialization = dict(updated.get("materialization") or {})
    if materialization:
        materialization["manifest_path"] = "workspace/episode_manifest.json"
        materialization["bundle_path"] = EXAMPLE_PLACEHOLDER
        materialization["context_snapshot_path"] = "workspace/context_snapshot.json"
        materialization["baseline_dataset_path"] = (
            "workspace/whatif_baseline_dataset.json"
        )
        materialization["workspace_root"] = "workspace"
        updated["materialization"] = materialization

    baseline = dict(updated.get("baseline") or {})
    if baseline:
        baseline["workspace_root"] = "workspace"
        baseline["baseline_dataset_path"] = "workspace/whatif_baseline_dataset.json"
        updated["baseline"] = baseline

    forecast_result = updated.get("forecast_result")
    if isinstance(forecast_result, dict):
        updated["forecast_result"] = _rewrite_forecast_result(forecast_result)

    artifacts = dict(updated.get("artifacts") or {})
    if artifacts:
        artifacts["root"] = "."
        artifacts["result_json_path"] = "whatif_experiment_result.json"
        artifacts["overview_markdown_path"] = "whatif_experiment_overview.md"
        artifacts["llm_json_path"] = "whatif_llm_result.json"
        artifacts["forecast_json_path"] = forecast_filename
        updated["artifacts"] = artifacts
    return updated


def _enrich_packaged_business_state(
    output_root: Path, *, forecast_filename: str
) -> None:
    manifest_path = output_root / "workspace" / "episode_manifest.json"
    forecast_path = output_root / forecast_filename
    result_path = output_root / "whatif_experiment_result.json"
    context_path = output_root / "workspace" / "context_snapshot.json"

    manifest = WhatIfEpisodeManifest.model_validate_json(
        manifest_path.read_text(encoding="utf-8")
    )
    historical_business_state = assess_historical_business_state(
        branch_event=manifest.branch_event,
        forecast=manifest.forecast,
        organization_domain=manifest.organization_domain,
        public_context=manifest.public_context,
    )
    manifest.historical_business_state = historical_business_state
    manifest_path.write_text(manifest.model_dump_json(indent=2), encoding="utf-8")

    forecast_result = WhatIfCounterfactualEstimateResult.model_validate_json(
        forecast_path.read_text(encoding="utf-8")
    )
    forecast_result.business_state_change = describe_forecast_business_change(
        branch_event=manifest.branch_event,
        forecast_result=forecast_result,
        organization_domain=manifest.organization_domain,
        public_context=manifest.public_context,
    )
    forecast_path.write_text(
        forecast_result.model_dump_json(indent=2),
        encoding="utf-8",
    )

    experiment_result = WhatIfExperimentResult.model_validate_json(
        result_path.read_text(encoding="utf-8")
    )
    experiment_result.materialization.historical_business_state = (
        historical_business_state
    )
    experiment_result.forecast_result = forecast_result
    result_path.write_text(
        experiment_result.model_dump_json(indent=2),
        encoding="utf-8",
    )
    (output_root / "whatif_experiment_overview.md").write_text(
        render_experiment_overview(experiment_result),
        encoding="utf-8",
    )

    context_payload = _read_json(context_path)
    metadata = context_payload.setdefault("metadata", {})
    whatif_metadata = metadata.setdefault("whatif", {})
    whatif_metadata["historical_business_state"] = historical_business_state.model_dump(
        mode="json"
    )
    _write_json(context_path, context_payload)


def package_example(source_root: Path, output_root: Path) -> None:
    workspace_root = source_root / "workspace"
    target_workspace = output_root / "workspace"
    experiment_payload = _read_json(source_root / "whatif_experiment_result.json")
    forecast_filename = _resolve_forecast_filename(
        source_root,
        experiment_payload=experiment_payload,
    )
    preserved_readme = _preserve_readme(output_root)
    if output_root.exists():
        shutil.rmtree(output_root)
    target_workspace.mkdir(parents=True, exist_ok=True)
    if preserved_readme:
        (output_root / "README.md").write_text(preserved_readme, encoding="utf-8")

    _copy_file(
        source_root / "whatif_experiment_overview.md",
        output_root / "whatif_experiment_overview.md",
    )
    _copy_file(
        source_root / "whatif_llm_result.json", output_root / "whatif_llm_result.json"
    )
    _write_json(
        output_root / forecast_filename,
        _rewrite_forecast_result(_read_json(source_root / forecast_filename)),
    )
    _write_json(
        output_root / "whatif_experiment_result.json",
        _rewrite_experiment_result(
            experiment_payload,
            forecast_filename=forecast_filename,
        ),
    )

    for relative_path in (
        "context_snapshot.json",
        "whatif_baseline_dataset.json",
        "vei_project.json",
        "contracts/default.contract.json",
        "scenarios/default.json",
        "imports/source_registry.json",
        "imports/source_sync_history.json",
        "runs/index.json",
        "sources/blueprint_asset.json",
    ):
        _copy_file(workspace_root / relative_path, target_workspace / relative_path)

    _write_json(
        target_workspace / "episode_manifest.json",
        _rewrite_manifest(_read_json(workspace_root / "episode_manifest.json")),
    )
    _enrich_packaged_business_state(output_root, forecast_filename=forecast_filename)
    build_business_state_example(output_root)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Package the repo-owned Enron Master Agreement example bundle."
    )
    parser.add_argument(
        "--source-root",
        type=Path,
        default=Path(
            "_vei_out/whatif_repo_examples/master_agreement_internal_review_public_context_20260412"
        ),
        help="Fresh local what-if experiment root to package.",
    )
    parser.add_argument(
        "--output-root",
        type=Path,
        default=Path("docs/examples/enron-master-agreement-public-context"),
        help="Tracked repo path for the packaged example bundle.",
    )
    args = parser.parse_args()
    package_example(
        args.source_root.expanduser().resolve(), args.output_root.expanduser().resolve()
    )


if __name__ == "__main__":
    main()
