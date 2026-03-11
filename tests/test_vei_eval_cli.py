from __future__ import annotations

import json
from pathlib import Path

import typer.testing

from vei.benchmark.api import list_benchmark_family_manifest
from vei.cli.vei_eval import app as eval_app
from vei.cli.vei_eval import scripted, bc as eval_bc
from vei.cli.vei_train import bc as train_bc
from vei.data.rollout import rollout_procurement


def test_vei_eval_scripted_creates_score(tmp_path: Path) -> None:
    artifacts = tmp_path / "eval"
    scripted(seed=101, dataset=Path("-"), artifacts=artifacts)
    score_path = artifacts / "score.json"
    assert score_path.exists()
    data = json.loads(score_path.read_text(encoding="utf-8"))
    assert "success" in data


def test_vei_eval_bc(tmp_path: Path) -> None:
    dataset = rollout_procurement(episodes=1, seed=555)
    dataset_path = tmp_path / "dataset.json"
    dataset_path.write_text(json.dumps(dataset.model_dump()), encoding="utf-8")

    model_path = tmp_path / "policy.json"
    train_bc(dataset=[str(dataset_path)], output=model_path)

    artifacts = tmp_path / "eval_bc"
    eval_bc(
        model=model_path,
        seed=555,
        dataset=dataset_path,
        artifacts=artifacts,
        max_steps=10,
    )
    score_path = artifacts / "score.json"
    assert score_path.exists()


def test_vei_eval_demo_cli_creates_report_and_state_artifacts(tmp_path: Path) -> None:
    runner = typer.testing.CliRunner()
    result = runner.invoke(
        eval_app,
        [
            "demo",
            "--family",
            "security_containment",
            "--artifacts-root",
            str(tmp_path),
            "--run-id",
            "security_demo",
        ],
    )

    assert result.exit_code == 0, result.output
    demo_dir = tmp_path / "security_demo"
    assert (demo_dir / "aggregate_results.json").exists()
    assert (demo_dir / "benchmark_summary.json").exists()
    assert (demo_dir / "leaderboard.md").exists()
    assert (demo_dir / "leaderboard.csv").exists()
    assert (demo_dir / "leaderboard.json").exists()
    demo_result = json.loads(
        (demo_dir / "demo_result.json").read_text(encoding="utf-8")
    )
    assert demo_result["family_name"] == "security_containment"
    assert demo_result["compare_runner"] == "scripted"
    assert demo_result["baseline_workflow_variant"] == "customer_notify"
    assert demo_result["summary"]["total_runs"] == 2
    assert demo_result["baseline_branch"]
    assert demo_result["comparison_branch"]
    assert demo_result["inspection_commands"]
    assert demo_result["baseline_blueprint_asset_path"].endswith("blueprint_asset.json")
    assert demo_result["comparison_blueprint_asset_path"].endswith(
        "blueprint_asset.json"
    )
    assert demo_result["baseline_blueprint_path"].endswith("blueprint.json")
    assert demo_result["comparison_blueprint_path"].endswith("blueprint.json")
    assert demo_result["baseline_contract_path"].endswith("contract.json")
    assert demo_result["comparison_contract_path"].endswith("contract.json")
    assert (demo_dir / "state").exists()
    assert (demo_dir / "baseline" / "oauth_app_containment" / "blueprint.json").exists()
    assert (
        demo_dir / "baseline" / "oauth_app_containment" / "blueprint_asset.json"
    ).exists()
    assert (demo_dir / "baseline" / "oauth_app_containment" / "contract.json").exists()
    assert (
        demo_dir / "comparison" / "oauth_app_containment" / "blueprint.json"
    ).exists()
    assert (
        demo_dir / "comparison" / "oauth_app_containment" / "blueprint_asset.json"
    ).exists()
    assert (
        demo_dir / "comparison" / "oauth_app_containment" / "contract.json"
    ).exists()


def test_vei_eval_suite_cli_creates_canonical_suite_artifacts(tmp_path: Path) -> None:
    runner = typer.testing.CliRunner()
    result = runner.invoke(
        eval_app,
        [
            "suite",
            "--artifacts-root",
            str(tmp_path),
            "--run-id",
            "canonical_suite",
        ],
    )

    assert result.exit_code == 0, result.output
    suite_dir = tmp_path / "canonical_suite"
    assert (suite_dir / "aggregate_results.json").exists()
    assert (suite_dir / "benchmark_summary.json").exists()
    assert (suite_dir / "leaderboard.md").exists()
    assert (suite_dir / "leaderboard.csv").exists()
    assert (suite_dir / "leaderboard.json").exists()
    suite_result = json.loads(
        (suite_dir / "suite_result.json").read_text(encoding="utf-8")
    )
    expected_families = {item.name for item in list_benchmark_family_manifest()}
    assert set(suite_result["family_names"]) == expected_families
    assert suite_result["summary"]["total_runs"] == len(expected_families)
    assert set(suite_result["scenario_names"]) == expected_families
    assert set(suite_result["case_artifacts_dirs"]) == expected_families
    assert set(suite_result["blueprint_asset_paths"]) == expected_families
    assert set(suite_result["blueprint_paths"]) == expected_families
    assert set(suite_result["contract_paths"]) == expected_families


def test_vei_eval_showcase_cli_creates_multi_example_bundle(tmp_path: Path) -> None:
    runner = typer.testing.CliRunner()
    result = runner.invoke(
        eval_app,
        [
            "showcase",
            "--example",
            "oauth_incident_chain",
            "--example",
            "checkout_revenue_flightdeck",
            "--artifacts-root",
            str(tmp_path),
            "--run-id",
            "complex_showcase",
        ],
    )

    assert result.exit_code == 0, result.output
    showcase_dir = tmp_path / "complex_showcase"
    assert (showcase_dir / "showcase_result.json").exists()
    assert (showcase_dir / "showcase_overview.md").exists()
    payload = json.loads(
        (showcase_dir / "showcase_result.json").read_text(encoding="utf-8")
    )
    assert payload["example_count"] == 2
    assert payload["baseline_success_count"] == 2
    assert len(payload["examples"]) == 2
    assert payload["examples"][0]["demo"]["baseline_score"] >= 0.9
    assert payload["examples"][0]["demo"]["report_markdown_path"].endswith(
        "leaderboard.md"
    )
