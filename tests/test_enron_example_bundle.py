from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

from fastapi.testclient import TestClient

from scripts import package_enron_master_agreement_example as enron_example_packager
from vei.whatif.artifact_validation import (
    detect_validation_mode,
    validate_artifact_tree,
    validate_packaged_example_bundle,
)
from vei.ui import api as ui_api

EXAMPLE_ROOT = (
    Path(__file__).resolve().parents[1]
    / "docs"
    / "examples"
    / "enron-master-agreement-public-context"
)


def _write_packaging_source_fixture(root: Path, *, forecast_filename: str) -> Path:
    source_root = root / "source"
    workspace_root = source_root / "workspace"
    workspace_root.mkdir(parents=True)
    (source_root / "whatif_experiment_overview.md").write_text(
        "# Example\n",
        encoding="utf-8",
    )
    (source_root / "whatif_llm_result.json").write_text("{}", encoding="utf-8")
    (source_root / forecast_filename).write_text("{}", encoding="utf-8")
    (source_root / "whatif_experiment_result.json").write_text(
        json.dumps({"artifacts": {"forecast_json_path": forecast_filename}}),
        encoding="utf-8",
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
        "episode_manifest.json",
    ):
        path = workspace_root / relative_path
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("{}", encoding="utf-8")
    return source_root


def test_repo_owned_enron_example_bundle_is_present_and_clean() -> None:
    assert EXAMPLE_ROOT.exists()
    assert validate_packaged_example_bundle(EXAMPLE_ROOT) == []

    required_paths = [
        EXAMPLE_ROOT / "README.md",
        EXAMPLE_ROOT / "whatif_experiment_overview.md",
        EXAMPLE_ROOT / "whatif_experiment_result.json",
        EXAMPLE_ROOT / "whatif_llm_result.json",
        EXAMPLE_ROOT / "whatif_ejepa_result.json",
        EXAMPLE_ROOT / "whatif_business_state_comparison.json",
        EXAMPLE_ROOT / "whatif_business_state_comparison.md",
        EXAMPLE_ROOT / "workspace" / "vei_project.json",
        EXAMPLE_ROOT / "workspace" / "context_snapshot.json",
        EXAMPLE_ROOT / "workspace" / "episode_manifest.json",
    ]
    for path in required_paths:
        assert path.exists(), path

    manifest = json.loads(
        (EXAMPLE_ROOT / "workspace" / "episode_manifest.json").read_text(
            encoding="utf-8"
        )
    )
    assert manifest["source"] == "enron"
    assert manifest["source_dir"] == "not-included-in-repo-example"
    assert manifest["workspace_root"] == "workspace"
    assert manifest["history_message_count"] == 6
    assert manifest["future_event_count"] == 84
    assert manifest["historical_business_state"]["summary"]
    assert [
        item["label"] for item in manifest["public_context"]["financial_snapshots"]
    ] == [
        "FY1998 selected financial data",
        "FY1999 selected financial data",
    ]
    assert manifest["public_context"]["public_news_events"] == []

    for relative_path in (
        "whatif_experiment_result.json",
        "whatif_ejepa_result.json",
        "workspace/episode_manifest.json",
    ):
        text = (EXAMPLE_ROOT / relative_path).read_text(encoding="utf-8")
        assert "/Users/" not in text

    overview_text = (EXAMPLE_ROOT / "whatif_experiment_overview.md").read_text(
        encoding="utf-8"
    )
    assert "External-send delta: -29" in overview_text
    assert "Predicted risk: 0.983" in overview_text
    assert "## Business State Change" in overview_text

    comparison_payload = json.loads(
        (EXAMPLE_ROOT / "whatif_business_state_comparison.json").read_text(
            encoding="utf-8"
        )
    )
    assert [item["label"] for item in comparison_payload["candidates"]] == [
        "Hold for internal review",
        "Send a narrow status note",
        "Push for fast turnaround",
    ]
    assert (
        comparison_payload["candidates"][0]["business_state_change"]["net_effect_score"]
        > 0
    )
    assert (
        comparison_payload["candidates"][-1]["business_state_change"][
            "net_effect_score"
        ]
        < 0
    )


def test_package_example_accepts_proxy_forecast_bundle(
    tmp_path: Path,
    monkeypatch,
) -> None:
    source_root = _write_packaging_source_fixture(
        tmp_path,
        forecast_filename="whatif_ejepa_proxy_result.json",
    )
    output_root = tmp_path / "packaged"

    monkeypatch.setattr(
        enron_example_packager,
        "_enrich_packaged_business_state",
        lambda *args, **kwargs: None,
    )
    monkeypatch.setattr(
        enron_example_packager,
        "build_business_state_example",
        lambda *args, **kwargs: None,
    )

    enron_example_packager.package_example(source_root, output_root)

    assert (output_root / "whatif_ejepa_proxy_result.json").exists()
    assert not (output_root / "whatif_ejepa_result.json").exists()
    experiment_payload = json.loads(
        (output_root / "whatif_experiment_result.json").read_text(encoding="utf-8")
    )
    assert (
        experiment_payload["artifacts"]["forecast_json_path"]
        == "whatif_ejepa_proxy_result.json"
    )
    assert validate_packaged_example_bundle(output_root) == []


def test_validate_artifact_tree_flags_workspace_root_mismatch(tmp_path: Path) -> None:
    workspace_root = tmp_path / "workspace"
    workspace_root.mkdir(parents=True)
    (workspace_root / "context_snapshot.json").write_text("{}", encoding="utf-8")
    (workspace_root / "episode_manifest.json").write_text(
        json.dumps({"workspace_root": str(tmp_path / "other")}),
        encoding="utf-8",
    )

    issues = validate_artifact_tree(tmp_path)

    assert any("workspace_root mismatch" in issue for issue in issues)


def test_validate_artifact_tree_flags_unexpected_manifest_name(tmp_path: Path) -> None:
    workspace_root = tmp_path / "workspace"
    workspace_root.mkdir(parents=True)
    (workspace_root / "context_snapshot.json").write_text("{}", encoding="utf-8")
    (workspace_root / "stale_episode_manifest.json").write_text(
        json.dumps({"workspace_root": str(workspace_root)}),
        encoding="utf-8",
    )

    issues = validate_artifact_tree(tmp_path)

    assert any("unexpected workspace manifest present" in issue for issue in issues)


def test_validate_artifact_tree_ignores_non_episode_manifest_files(
    tmp_path: Path,
) -> None:
    workspace_root = tmp_path / "workspace"
    workspace_root.mkdir(parents=True)
    (workspace_root / "context_snapshot.json").write_text("{}", encoding="utf-8")
    (workspace_root / "episode_manifest.json").write_text(
        json.dumps({"workspace_root": str(workspace_root)}),
        encoding="utf-8",
    )
    (workspace_root / "twin_manifest.json").write_text("{}", encoding="utf-8")
    compiled_root = workspace_root / "compiled" / "default"
    compiled_root.mkdir(parents=True)
    (compiled_root / "scenario_manifest.json").write_text("{}", encoding="utf-8")

    issues = validate_artifact_tree(tmp_path)

    assert issues == []


def test_repo_owned_enron_example_workspace_loads_saved_scene() -> None:
    workspace_root = EXAMPLE_ROOT / "workspace"
    client = TestClient(ui_api.create_ui_app(workspace_root))

    status_response = client.get("/api/workspace/whatif")
    assert status_response.status_code == 200
    status_payload = status_response.json()
    assert status_payload["available"] is True
    assert status_payload["source"] == "mail_archive"

    historical_response = client.get("/api/workspace/historical")
    assert historical_response.status_code == 200
    historical_payload = historical_response.json()
    assert historical_payload["organization_name"] == "Enron Corporation"

    scene_response = client.post(
        "/api/workspace/whatif/scene",
        json={
            "source": "auto",
            "event_id": historical_payload["branch_event_id"],
            "thread_id": historical_payload["thread_id"],
        },
    )
    assert scene_response.status_code == 200
    scene_payload = scene_response.json()
    assert scene_payload["organization_name"] == "Enron Corporation"
    assert scene_payload["branch_event_id"] == "enron_bcda1b925800af8c"
    assert scene_payload["history_message_count"] == 6
    assert scene_payload["future_event_count"] == 84
    assert scene_payload["historical_business_state"]["summary"]
    assert [
        item["label"] for item in scene_payload["public_context"]["financial_snapshots"]
    ] == [
        "FY1998 selected financial data",
        "FY1999 selected financial data",
    ]
    assert scene_payload["public_context"]["public_news_events"] == []


def test_repo_owned_enron_example_workspace_uses_saved_experiment_without_rosetta(
    monkeypatch,
) -> None:
    monkeypatch.delenv("VEI_WHATIF_ROSETTA_DIR", raising=False)
    monkeypatch.delenv("VEI_WHATIF_SOURCE", raising=False)
    monkeypatch.delenv("VEI_WHATIF_SOURCE_DIR", raising=False)
    workspace_root = EXAMPLE_ROOT / "workspace"
    client = TestClient(ui_api.create_ui_app(workspace_root))

    historical_payload = client.get("/api/workspace/historical").json()
    response = client.post(
        "/api/workspace/whatif/run",
        json={
            "source": "auto",
            "event_id": historical_payload["branch_event_id"],
            "thread_id": historical_payload["thread_id"],
            "label": "ignored-for-saved-bundle",
            "prompt": "Keep the draft inside Enron and hold the outside send.",
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert (
        payload["label"] == "master_agreement_internal_review_public_context_20260412"
    )
    assert payload["materialization"]["branch_event_id"] == "enron_bcda1b925800af8c"
    assert payload["forecast_result"]["business_state_change"]["summary"]


def test_repo_owned_enron_example_workspace_uses_saved_ranked_result_without_rosetta(
    monkeypatch,
) -> None:
    monkeypatch.delenv("VEI_WHATIF_ROSETTA_DIR", raising=False)
    monkeypatch.delenv("VEI_WHATIF_SOURCE", raising=False)
    monkeypatch.delenv("VEI_WHATIF_SOURCE_DIR", raising=False)
    workspace_root = EXAMPLE_ROOT / "workspace"
    client = TestClient(ui_api.create_ui_app(workspace_root))

    historical_payload = client.get("/api/workspace/historical").json()
    response = client.post(
        "/api/workspace/whatif/rank",
        json={
            "source": "auto",
            "event_id": historical_payload["branch_event_id"],
            "thread_id": historical_payload["thread_id"],
            "label": "ignored-for-saved-bundle",
            "objective_pack_id": "contain_exposure",
            "candidates": [
                {
                    "label": "Hold for internal review",
                    "prompt": "Keep the draft inside Enron and hold the outside send.",
                }
            ],
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["recommended_candidate_label"] == "Hold for internal review"
    assert payload["objective_pack"]["pack_id"] == "contain_exposure"
    assert (
        payload["candidates"][0]["intervention"]["label"] == "Hold for internal review"
    )
    assert payload["candidates"][0]["saved_result"] is True


def test_validate_whatif_artifacts_script_auto_detects_bundle_mode() -> None:
    result = subprocess.run(
        [
            sys.executable,
            "scripts/validate_whatif_artifacts.py",
            str(EXAMPLE_ROOT),
        ],
        cwd=EXAMPLE_ROOT.parents[2],
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0, result.stdout + result.stderr
    assert "ok:" in result.stdout


def test_detect_validation_mode_prefers_workspace_and_bundle(tmp_path: Path) -> None:
    workspace_root = tmp_path / "workspace"
    workspace_root.mkdir(parents=True)
    (workspace_root / "episode_manifest.json").write_text("{}", encoding="utf-8")

    bundle_root = tmp_path / "bundle"
    (bundle_root / "workspace").mkdir(parents=True)
    (bundle_root / "workspace" / "episode_manifest.json").write_text(
        "{}",
        encoding="utf-8",
    )

    assert detect_validation_mode(workspace_root) == "workspace"
    assert detect_validation_mode(bundle_root) == "bundle"
    assert detect_validation_mode(tmp_path / "other") == "tree"
