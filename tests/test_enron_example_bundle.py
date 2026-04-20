from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

from fastapi.testclient import TestClient

from scripts import package_enron_master_agreement_example as enron_example_packager
from vei.whatif.api import (
    build_saved_ranked_result_payload,
    resolve_saved_whatif_bundle,
)
from vei.whatif.artifact_validation import (
    detect_validation_mode,
    validate_artifact_tree,
    validate_packaged_example_bundle,
)
from vei.whatif.filenames import (
    BUSINESS_STATE_COMPARISON_FILE,
    BUSINESS_STATE_COMPARISON_OVERVIEW_FILE,
    CONTEXT_SNAPSHOT_FILE,
    EJEPA_RESULT_FILE,
    EPISODE_MANIFEST_FILE,
    EXPERIMENT_OVERVIEW_FILE,
    EXPERIMENT_RESULT_FILE,
    HEURISTIC_FORECAST_FILE,
    LLM_RESULT_FILE,
    PUBLIC_CONTEXT_FILE,
    REFERENCE_FORECAST_FILE,
    STUDIO_SAVED_FORECAST_FILES,
)
from vei.ui import api as ui_api

EXAMPLE_ROOT = (
    Path(__file__).resolve().parents[1]
    / "docs"
    / "examples"
    / "enron-master-agreement-public-context"
)
TIMELINE_IMAGE = (
    Path(__file__).resolve().parents[1]
    / "docs"
    / "assets"
    / "enron-whatif"
    / "enron-bankruptcy-arc-timeline.png"
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
    (source_root / forecast_filename).write_text(
        json.dumps({"cache_root": "not-included-in-repo-example"}),
        encoding="utf-8",
    )
    (source_root / "whatif_experiment_result.json").write_text(
        json.dumps({"artifacts": {"forecast_json_path": forecast_filename}}),
        encoding="utf-8",
    )
    for relative_path in (
        "vei_project.json",
        "contracts/default.contract.json",
        "scenarios/default.json",
        "imports/source_registry.json",
        "imports/source_sync_history.json",
        "runs/index.json",
        "sources/blueprint_asset.json",
    ):
        path = workspace_root / relative_path
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("{}", encoding="utf-8")
    _write_minimal_valid_saved_workspace(
        workspace_root,
        workspace_root_value="workspace",
    )
    return source_root


def _write_minimal_valid_saved_workspace(
    workspace_root: Path,
    *,
    workspace_root_value: str,
) -> None:
    (workspace_root / "context_snapshot.json").write_text(
        json.dumps(
            {
                "version": "1",
                "organization_name": "Acme Cloud",
                "organization_domain": "acme.example.com",
                "captured_at": "2026-01-01T00:00:00Z",
                "sources": [],
            }
        ),
        encoding="utf-8",
    )
    (workspace_root / "whatif_baseline_dataset.json").write_text(
        json.dumps({"events": []}),
        encoding="utf-8",
    )
    (workspace_root / "episode_manifest.json").write_text(
        json.dumps(
            {
                "version": "2",
                "source": "mail_archive",
                "source_dir": "not-included-in-repo-example",
                "workspace_root": workspace_root_value,
                "organization_name": "Acme Cloud",
                "organization_domain": "acme.example.com",
                "thread_id": "thr-1",
                "thread_subject": "Contract",
                "branch_event_id": "evt-1",
                "branch_timestamp": "2026-01-01T00:00:00Z",
                "branch_event": {
                    "event_id": "evt-1",
                    "timestamp": "2026-01-01T00:00:00Z",
                    "actor_id": "maya@acme.example.com",
                    "event_type": "message",
                    "thread_id": "thr-1",
                },
                "baseline_dataset_path": "whatif_baseline_dataset.json",
                "content_notice": "fixture",
            }
        ),
        encoding="utf-8",
    )
    (workspace_root / "whatif_public_context.json").write_text(
        json.dumps({}),
        encoding="utf-8",
    )


def test_repo_owned_enron_example_bundle_is_present_and_clean() -> None:
    assert EXAMPLE_ROOT.exists()
    assert validate_packaged_example_bundle(EXAMPLE_ROOT) == []

    saved_forecast_paths = [
        EXAMPLE_ROOT / filename for filename in STUDIO_SAVED_FORECAST_FILES
    ]
    assert any(path.exists() for path in saved_forecast_paths), saved_forecast_paths

    required_paths = [
        EXAMPLE_ROOT / "README.md",
        EXAMPLE_ROOT / "timeline_arc.md",
        EXAMPLE_ROOT / "whatif_experiment_overview.md",
        EXAMPLE_ROOT / "whatif_experiment_result.json",
        EXAMPLE_ROOT / "whatif_llm_result.json",
        EXAMPLE_ROOT / "whatif_business_state_comparison.json",
        EXAMPLE_ROOT / "whatif_business_state_comparison.md",
        EXAMPLE_ROOT / "workspace" / "vei_project.json",
        EXAMPLE_ROOT / "workspace" / "context_snapshot.json",
        EXAMPLE_ROOT / "workspace" / "episode_manifest.json",
        EXAMPLE_ROOT / "workspace" / "whatif_public_context.json",
        TIMELINE_IMAGE,
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
    assert manifest["history_message_count"] >= 30
    assert manifest["future_event_count"] == 84
    assert manifest["historical_business_state"]["summary"]
    assert [
        item["label"] for item in manifest["public_context"]["financial_snapshots"]
    ] == [
        "FY1998 selected financial data",
        "FY1999 selected financial data",
        "Q4 1999 annual earnings release",
        "Q1 2000 earnings release",
        "Q2 2000 earnings release",
    ]
    assert [
        item["event_id"] for item in manifest["public_context"]["public_news_events"]
    ] == [
        "enrononline_emissions_auction_launch",
        "ibm_energy_services_agreement",
        "mg_plc_cash_offer",
        "enrononline_fifty_billion_milestone",
        "blockbuster_on_demand_launch",
        "clickpaper_launch",
    ]

    forecast_path = next(path for path in saved_forecast_paths if path.exists())
    for path in (
        EXAMPLE_ROOT / "whatif_experiment_result.json",
        forecast_path,
        EXAMPLE_ROOT / "workspace" / "episode_manifest.json",
    ):
        text = path.read_text(encoding="utf-8")
        assert "/Users/" not in text

    overview_text = (EXAMPLE_ROOT / "whatif_experiment_overview.md").read_text(
        encoding="utf-8"
    )
    assert "External-send delta:" in overview_text
    assert "Predicted risk:" in overview_text
    assert "## Business State Change" in overview_text
    assert "## Macro Outcomes" in overview_text

    comparison_payload = json.loads(
        (EXAMPLE_ROOT / "whatif_business_state_comparison.json").read_text(
            encoding="utf-8"
        )
    )
    assert [item["label"] for item in comparison_payload["candidates"]] == [
        "Internal legal review",
        "Narrow status note",
        "Controlled external send",
        "Fast outside circulation",
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


def test_canonical_saved_forecast_filenames_are_stable() -> None:
    assert CONTEXT_SNAPSHOT_FILE == "context_snapshot.json"
    assert EPISODE_MANIFEST_FILE == "episode_manifest.json"
    assert PUBLIC_CONTEXT_FILE == "whatif_public_context.json"
    assert EXPERIMENT_RESULT_FILE == "whatif_experiment_result.json"
    assert EXPERIMENT_OVERVIEW_FILE == "whatif_experiment_overview.md"
    assert LLM_RESULT_FILE == "whatif_llm_result.json"
    assert EJEPA_RESULT_FILE == "whatif_ejepa_result.json"
    assert REFERENCE_FORECAST_FILE == "whatif_reference_result.json"
    assert HEURISTIC_FORECAST_FILE == "whatif_heuristic_baseline_result.json"
    assert BUSINESS_STATE_COMPARISON_FILE == "whatif_business_state_comparison.json"
    assert (
        BUSINESS_STATE_COMPARISON_OVERVIEW_FILE == "whatif_business_state_comparison.md"
    )
    assert STUDIO_SAVED_FORECAST_FILES == (
        EJEPA_RESULT_FILE,
        REFERENCE_FORECAST_FILE,
        HEURISTIC_FORECAST_FILE,
    )


def test_repo_owned_enron_example_bundle_exposes_generic_saved_bundle_loader() -> None:
    bundle = resolve_saved_whatif_bundle(EXAMPLE_ROOT / "workspace")

    assert bundle is not None
    assert bundle.bundle_root == EXAMPLE_ROOT
    assert bundle.source_dir_text().endswith("context_snapshot.json")

    ranked_payload = build_saved_ranked_result_payload(bundle)

    assert ranked_payload is not None
    assert ranked_payload["objective_pack"]["pack_id"] == "protect_company_default"
    assert ranked_payload["candidates"][0]["saved_result"] is True


def test_saved_ranked_result_payload_sorts_candidates_and_uses_requested_objective_for_legacy_saved_comparisons(
    tmp_path: Path,
) -> None:
    bundle_root = tmp_path / "bundle"
    workspace_root = bundle_root / "workspace"
    workspace_root.mkdir(parents=True)
    (bundle_root / "whatif_experiment_result.json").write_text(
        json.dumps({"selection": {}, "baseline": {}, "materialization": {}}),
        encoding="utf-8",
    )
    (bundle_root / "whatif_business_state_comparison.json").write_text(
        json.dumps(
            {
                "label": "saved_ranked",
                "candidates": [
                    {
                        "label": "Second",
                        "prompt": "Prompt B",
                        "rank": 2,
                        "business_state_change": {"net_effect_score": 0.1},
                    },
                    {
                        "label": "First",
                        "prompt": "Prompt A",
                        "rank": 1,
                        "business_state_change": {"net_effect_score": 0.3},
                    },
                ],
            }
        ),
        encoding="utf-8",
    )
    bundle = resolve_saved_whatif_bundle(workspace_root)

    assert bundle is not None
    ranked_payload = build_saved_ranked_result_payload(
        bundle,
        objective_pack_id="reduce_delay",
    )
    assert ranked_payload is not None
    assert ranked_payload["objective_pack"]["pack_id"] == "reduce_delay"
    candidate_ranks = [
        candidate["rank"] for candidate in ranked_payload.get("candidates", [])
    ]
    assert candidate_ranks == sorted(candidate_ranks)
    assert (
        ranked_payload["recommended_candidate_label"]
        == ranked_payload["candidates"][0]["intervention"]["label"]
    )


def test_saved_ranked_result_payload_keeps_saved_objective_pack(
    tmp_path: Path,
) -> None:
    bundle_root = tmp_path / "bundle"
    workspace_root = bundle_root / "workspace"
    workspace_root.mkdir(parents=True)
    (bundle_root / "whatif_experiment_result.json").write_text(
        json.dumps({"selection": {}, "baseline": {}, "materialization": {}}),
        encoding="utf-8",
    )
    (bundle_root / "whatif_business_state_comparison.json").write_text(
        json.dumps(
            {
                "objective_pack": {"pack_id": "contain_exposure"},
                "candidates": [
                    {
                        "label": "Hold",
                        "prompt": "Hold internal.",
                        "rank": 1,
                        "business_state_change": {"net_effect_score": 0.3},
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    bundle = resolve_saved_whatif_bundle(workspace_root)

    assert bundle is not None
    ranked_payload = build_saved_ranked_result_payload(
        bundle,
        objective_pack_id="reduce_delay",
    )

    assert ranked_payload is not None
    assert ranked_payload["objective_pack"]["pack_id"] == "contain_exposure"
    assert (
        ranked_payload["candidates"][0]["outcome_score"]["objective_pack_id"]
        == "contain_exposure"
    )


def test_validate_packaged_example_bundle_requires_openable_workspace_inputs(
    tmp_path: Path,
) -> None:
    bundle_root = tmp_path / "bundle"
    workspace_root = bundle_root / "workspace"
    workspace_root.mkdir(parents=True)
    (bundle_root / "whatif_experiment_result.json").write_text(
        json.dumps(
            {
                "artifacts": {
                    "result_json_path": "whatif_experiment_result.json",
                    "overview_markdown_path": "whatif_experiment_overview.md",
                    "llm_json_path": "whatif_llm_result.json",
                },
                "materialization": {
                    "manifest_path": "workspace/episode_manifest.json",
                    "context_snapshot_path": "workspace/context_snapshot.json",
                    "workspace_root": "workspace",
                },
            }
        ),
        encoding="utf-8",
    )
    (bundle_root / "whatif_experiment_overview.md").write_text(
        "# Example\n",
        encoding="utf-8",
    )
    (bundle_root / "whatif_llm_result.json").write_text("{}", encoding="utf-8")
    (bundle_root / "whatif_ejepa_result.json").write_text(
        json.dumps({"cache_root": "not-included-in-repo-example"}),
        encoding="utf-8",
    )
    _write_minimal_valid_saved_workspace(
        workspace_root,
        workspace_root_value="workspace",
    )

    issues = validate_packaged_example_bundle(bundle_root)

    assert issues == []


def test_validate_packaged_example_bundle_flags_partial_ranked_sidecars(
    tmp_path: Path,
) -> None:
    bundle_root = tmp_path / "bundle"
    workspace_root = bundle_root / "workspace"
    workspace_root.mkdir(parents=True)
    (bundle_root / "whatif_experiment_result.json").write_text(
        json.dumps(
            {
                "artifacts": {
                    "result_json_path": "whatif_experiment_result.json",
                    "overview_markdown_path": "whatif_experiment_overview.md",
                    "llm_json_path": "whatif_llm_result.json",
                },
                "materialization": {
                    "manifest_path": "workspace/episode_manifest.json",
                    "context_snapshot_path": "workspace/context_snapshot.json",
                    "workspace_root": "workspace",
                },
            }
        ),
        encoding="utf-8",
    )
    (bundle_root / "whatif_experiment_overview.md").write_text(
        "# Example\n",
        encoding="utf-8",
    )
    (bundle_root / "whatif_llm_result.json").write_text("{}", encoding="utf-8")
    (bundle_root / "whatif_ejepa_result.json").write_text(
        json.dumps({"cache_root": "not-included-in-repo-example"}),
        encoding="utf-8",
    )
    (bundle_root / "whatif_business_state_comparison.json").write_text(
        json.dumps({"candidates": []}),
        encoding="utf-8",
    )
    _write_minimal_valid_saved_workspace(
        workspace_root,
        workspace_root_value="workspace",
    )

    issues = validate_packaged_example_bundle(bundle_root)

    assert any(
        "whatif_business_state_comparison.md" in issue for issue in issues
    ), issues


def test_package_example_accepts_heuristic_forecast_bundle(
    tmp_path: Path,
    monkeypatch,
) -> None:
    source_root = _write_packaging_source_fixture(
        tmp_path,
        forecast_filename="whatif_heuristic_baseline_result.json",
    )
    output_root = tmp_path / "packaged"

    monkeypatch.setattr(
        enron_example_packager,
        "_enrich_packaged_business_state",
        lambda *args, **kwargs: None,
    )

    def fake_build_business_state_example(output_root: Path) -> None:
        (output_root / "whatif_business_state_comparison.json").write_text(
            json.dumps({"candidates": []}),
            encoding="utf-8",
        )
        (output_root / "whatif_business_state_comparison.md").write_text(
            "# Comparison\n",
            encoding="utf-8",
        )

    monkeypatch.setattr(
        enron_example_packager,
        "build_business_state_example",
        fake_build_business_state_example,
    )

    enron_example_packager.package_example(source_root, output_root)

    assert (output_root / "whatif_heuristic_baseline_result.json").exists()
    assert not (output_root / "whatif_ejepa_result.json").exists()
    experiment_payload = json.loads(
        (output_root / "whatif_experiment_result.json").read_text(encoding="utf-8")
    )
    assert (
        experiment_payload["artifacts"]["forecast_json_path"]
        == "whatif_heuristic_baseline_result.json"
    )
    assert validate_packaged_example_bundle(output_root) == []


def test_validate_artifact_tree_flags_workspace_root_mismatch(tmp_path: Path) -> None:
    workspace_root = tmp_path / "workspace"
    workspace_root.mkdir(parents=True)
    _write_minimal_valid_saved_workspace(
        workspace_root,
        workspace_root_value=str(tmp_path / "other"),
    )

    issues = validate_artifact_tree(tmp_path)

    assert any("workspace_root mismatch" in issue for issue in issues)


def test_validate_artifact_tree_flags_unexpected_manifest_name(tmp_path: Path) -> None:
    workspace_root = tmp_path / "workspace"
    workspace_root.mkdir(parents=True)
    (workspace_root / "context_snapshot.json").write_text(
        json.dumps(
            {
                "version": "1",
                "organization_name": "Acme Cloud",
                "organization_domain": "acme.example.com",
                "captured_at": "2026-01-01T00:00:00Z",
                "sources": [],
            }
        ),
        encoding="utf-8",
    )
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
    _write_minimal_valid_saved_workspace(
        workspace_root,
        workspace_root_value="workspace",
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
    assert status_payload["source"] == "enron"

    historical_response = client.get("/api/workspace/historical")
    assert historical_response.status_code == 200
    historical_payload = historical_response.json()
    assert historical_payload["organization_name"] == "Enron Corporation"

    scene_response = client.post(
        "/api/workspace/whatif/scene",
        json={
            "source": status_payload["source"],
            "event_id": historical_payload["branch_event_id"],
            "thread_id": historical_payload["thread_id"],
        },
    )
    assert scene_response.status_code == 200
    scene_payload = scene_response.json()
    assert scene_payload["organization_name"] == "Enron Corporation"
    assert scene_payload["branch_event_id"] == "enron_bcda1b925800af8c"
    assert scene_payload["history_message_count"] >= 30
    assert scene_payload["future_event_count"] == 84
    assert scene_payload["historical_business_state"]["summary"]
    assert [
        item["label"] for item in scene_payload["public_context"]["financial_snapshots"]
    ] == [
        "FY1998 selected financial data",
        "FY1999 selected financial data",
        "Q4 1999 annual earnings release",
        "Q1 2000 earnings release",
        "Q2 2000 earnings release",
    ]
    assert [
        item["event_id"]
        for item in scene_payload["public_context"]["public_news_events"]
    ] == [
        "enrononline_emissions_auction_launch",
        "ibm_energy_services_agreement",
        "mg_plc_cash_offer",
        "enrononline_fifty_billion_milestone",
        "blockbuster_on_demand_launch",
        "clickpaper_launch",
    ]


def test_repo_owned_enron_example_workspace_uses_saved_experiment_without_rosetta(
    monkeypatch,
) -> None:
    monkeypatch.delenv("VEI_WHATIF_ROSETTA_DIR", raising=False)
    monkeypatch.delenv("VEI_WHATIF_SOURCE", raising=False)
    monkeypatch.delenv("VEI_WHATIF_SOURCE_DIR", raising=False)
    workspace_root = EXAMPLE_ROOT / "workspace"
    client = TestClient(ui_api.create_ui_app(workspace_root))

    status_payload = client.get("/api/workspace/whatif").json()
    historical_payload = client.get("/api/workspace/historical").json()
    response = client.post(
        "/api/workspace/whatif/run",
        json={
            "source": status_payload["source"],
            "event_id": historical_payload["branch_event_id"],
            "thread_id": historical_payload["thread_id"],
            "label": "ignored-for-saved-bundle",
            "prompt": "Keep the draft inside Enron and hold the outside send.",
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["label"] == "master_agreement_saved_bundle_20260419"
    assert payload["saved_result"] is True
    assert "saved reference result" in payload["saved_bundle_notice"]
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

    status_payload = client.get("/api/workspace/whatif").json()
    historical_payload = client.get("/api/workspace/historical").json()
    response = client.post(
        "/api/workspace/whatif/rank",
        json={
            "source": status_payload["source"],
            "event_id": historical_payload["branch_event_id"],
            "thread_id": historical_payload["thread_id"],
            "label": "ignored-for-saved-bundle",
            "objective_pack_id": "reduce_delay",
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
    assert payload["recommended_candidate_label"] == "Internal legal review"
    assert payload["objective_pack"]["pack_id"] == "protect_company_default"
    assert payload["candidates"][0]["outcome_score"]["objective_pack_id"] == (
        "protect_company_default"
    )
    assert payload["candidates"][0]["intervention"]["label"] == "Internal legal review"
    assert payload["saved_result"] is True
    assert "saved reference ranking" in payload["saved_bundle_notice"]
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
    (bundle_root / EXPERIMENT_RESULT_FILE).write_text(
        json.dumps(
            {
                "materialization": {
                    "manifest_path": "workspace/episode_manifest.json",
                    "context_snapshot_path": "workspace/context_snapshot.json",
                    "workspace_root": "workspace",
                },
                "artifacts": {
                    "result_json_path": "whatif_experiment_result.json",
                    "overview_markdown_path": "whatif_experiment_overview.md",
                },
            }
        ),
        encoding="utf-8",
    )

    assert detect_validation_mode(workspace_root) == "workspace"
    assert detect_validation_mode(bundle_root) == "bundle"
    assert detect_validation_mode(tmp_path / "other") == "tree"


def test_detect_validation_mode_treats_live_experiment_bundle_as_tree(
    tmp_path: Path,
) -> None:
    bundle_root = tmp_path / "live_experiment"
    workspace_root = bundle_root / "workspace"
    workspace_root.mkdir(parents=True)
    _write_minimal_valid_saved_workspace(
        workspace_root,
        workspace_root_value="workspace",
    )
    (bundle_root / "whatif_experiment_result.json").write_text(
        json.dumps(
            {
                "materialization": {
                    "manifest_path": str(
                        (workspace_root / "episode_manifest.json").resolve()
                    ),
                    "context_snapshot_path": str(
                        (workspace_root / "context_snapshot.json").resolve()
                    ),
                    "workspace_root": str(workspace_root.resolve()),
                },
                "artifacts": {
                    "result_json_path": str(
                        (bundle_root / "whatif_experiment_result.json").resolve()
                    ),
                    "overview_markdown_path": str(
                        (bundle_root / "whatif_experiment_overview.md").resolve()
                    ),
                },
            }
        ),
        encoding="utf-8",
    )

    assert detect_validation_mode(bundle_root) == "tree"
