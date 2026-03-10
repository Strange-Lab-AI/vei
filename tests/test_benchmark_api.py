from __future__ import annotations

import csv
import json
import subprocess
from pathlib import Path

import pytest
import typer.testing

from vei.benchmark.api import (
    get_benchmark_family_workflow_variant,
    get_benchmark_family_manifest,
    list_benchmark_family_manifest,
    list_benchmark_family_workflow_variants,
    resolve_scenarios,
    run_benchmark_batch,
    run_benchmark_case,
    score_enterprise_dimensions,
)
from vei.benchmark.models import BenchmarkCaseSpec
from vei.benchmark.workflows import get_benchmark_family_workflow_spec
from vei.cli.vei_eval import app as eval_app
from vei.cli.vei_report import (
    generate_csv_report,
    generate_markdown_leaderboard,
    load_all_results,
)
from vei.data.rollout import rollout_procurement
from vei.rl.policy_bc import BCPPolicy
from vei.world.api import create_world_session, get_catalog_scenario


def _build_security_report_results(root_dir: Path) -> list[dict[str, object]]:
    run_benchmark_case(
        BenchmarkCaseSpec(
            runner="workflow",
            scenario_name="oauth_app_containment",
            workflow_name="security_containment",
            workflow_variant="internal_only_review",
            seed=700,
            artifacts_dir=root_dir / "internal_only_review",
        )
    )
    run_benchmark_case(
        BenchmarkCaseSpec(
            runner="workflow",
            scenario_name="oauth_app_containment",
            workflow_name="security_containment",
            workflow_variant="customer_notify",
            seed=701,
            artifacts_dir=root_dir / "customer_notify",
        )
    )
    run_benchmark_case(
        BenchmarkCaseSpec(
            runner="scripted",
            scenario_name="oauth_app_containment",
            seed=702,
            artifacts_dir=root_dir / "scripted_case",
            score_mode="full",
        )
    )
    return load_all_results(root_dir)


def test_run_benchmark_case_scripted_writes_kernel_diagnostics(tmp_path: Path) -> None:
    artifacts = tmp_path / "scripted_case"
    spec = BenchmarkCaseSpec(
        runner="scripted",
        scenario_name="multi_channel",
        seed=101,
        artifacts_dir=artifacts,
        score_mode="email",
    )

    result = run_benchmark_case(spec)

    assert result.status == "ok"
    assert result.diagnostics.branch == "multi_channel"
    assert result.diagnostics.snapshot_count >= 2
    assert "dimensions" in result.score
    assert (artifacts / "benchmark_result.json").exists()
    assert (artifacts / "score.json").exists()


def test_run_benchmark_case_overlay_replay_uses_dataset_events(tmp_path: Path) -> None:
    dataset = rollout_procurement(episodes=1, seed=222, scenario_name="multi_channel")
    dataset_path = tmp_path / "overlay_dataset.json"
    dataset_path.write_text(
        json.dumps(dataset.model_dump(), indent=2), encoding="utf-8"
    )

    artifacts = tmp_path / "overlay_case"
    spec = BenchmarkCaseSpec(
        runner="scripted",
        scenario_name="multi_channel",
        seed=222,
        artifacts_dir=artifacts,
        dataset_path=dataset_path,
        replay_mode="overlay",
        score_mode="email",
    )

    result = run_benchmark_case(spec)

    assert result.status == "ok"
    assert result.diagnostics.replay_mode == "overlay"
    assert result.diagnostics.replay_scheduled > 0


def test_run_benchmark_batch_writes_report_compatible_aggregate(tmp_path: Path) -> None:
    run_dir = tmp_path / "batch"
    batch = run_benchmark_batch(
        [
            BenchmarkCaseSpec(
                runner="scripted",
                scenario_name="multi_channel",
                seed=303,
                artifacts_dir=run_dir / "multi_channel",
                score_mode="email",
            )
        ],
        run_id="scripted_suite",
        output_dir=run_dir,
    )

    assert batch.summary.total_runs == 1
    aggregate = json.loads(
        (run_dir / "aggregate_results.json").read_text(encoding="utf-8")
    )
    assert isinstance(aggregate, list)
    assert aggregate[0]["scenario"] == "multi_channel"
    assert aggregate[0]["model"] == "scripted"
    assert aggregate[0]["provider"] == "baseline"
    assert "score" in aggregate[0]
    assert "dimensions" in aggregate[0]["score"]


def test_vei_eval_benchmark_cli_scripted(tmp_path: Path) -> None:
    runner = typer.testing.CliRunner()
    result = runner.invoke(
        eval_app,
        [
            "benchmark",
            "--runner",
            "scripted",
            "--scenario",
            "multi_channel",
            "--artifacts-root",
            str(tmp_path),
            "--run-id",
            "scripted_cli",
        ],
    )

    assert result.exit_code == 0, result.output
    assert (tmp_path / "scripted_cli" / "aggregate_results.json").exists()
    assert (tmp_path / "scripted_cli" / "benchmark_summary.json").exists()


def test_report_loads_batch_results_without_double_counting(tmp_path: Path) -> None:
    run_dir = tmp_path / "report_batch"
    run_benchmark_batch(
        [
            BenchmarkCaseSpec(
                runner="scripted",
                scenario_name="multi_channel",
                seed=404,
                artifacts_dir=run_dir / "multi_channel",
                score_mode="email",
            )
        ],
        run_id="report_suite",
        output_dir=run_dir,
    )

    results = load_all_results(run_dir)

    assert len(results) == 1
    assert results[0]["model"] == "scripted"
    assert results[0]["provider"] == "baseline"
    assert results[0]["runner"] == "scripted"


def test_benchmark_family_catalog_and_resolution() -> None:
    families = {item.name: item for item in list_benchmark_family_manifest()}

    assert "security_containment" in families
    assert "enterprise_onboarding_migration" in families
    assert "revenue_incident_mitigation" in families
    assert families["security_containment"].workflow_name == "security_containment"
    assert families["security_containment"].primary_workflow_variant == (
        "customer_notify"
    )
    assert families["security_containment"].workflow_variants == [
        "customer_notify",
        "internal_only_review",
    ]
    assert get_benchmark_family_manifest("security_containment").scenario_names == [
        "oauth_app_containment"
    ]
    assert resolve_scenarios(family_names=["revenue_incident_mitigation"]) == [
        "checkout_spike_mitigation"
    ]
    assert (
        get_benchmark_family_workflow_spec("revenue_incident_mitigation").name
        == "revenue_incident_mitigation"
    )
    assert (
        get_benchmark_family_workflow_variant(
            "security_containment", "internal_only_review"
        ).variant_name
        == "internal_only_review"
    )
    assert {
        item.variant_name for item in list_benchmark_family_workflow_variants()
    } >= {
        "manager_cutover",
        "canary_floor",
    }


def test_enterprise_dimension_scoring_for_security_containment(
    tmp_path: Path, monkeypatch
) -> None:
    monkeypatch.setenv("VEI_STATE_DIR", str(tmp_path / "state"))
    artifacts = tmp_path / "oauth_dims"
    session = create_world_session(
        seed=42042,
        artifacts_dir=str(artifacts),
        scenario=get_catalog_scenario("oauth_app_containment"),
    )
    session.call_tool(
        "google_admin.preserve_oauth_evidence",
        {"app_id": "OAUTH-9001", "note": "preserve before containment"},
    )
    session.call_tool(
        "google_admin.suspend_oauth_app",
        {"app_id": "OAUTH-9001", "reason": "containment"},
    )
    session.call_tool(
        "siem.update_case",
        {
            "case_id": "CASE-0001",
            "customer_notification_required": True,
            "note": "Will notify impacted customers.",
        },
    )
    session.snapshot("final")

    score = score_enterprise_dimensions(
        scenario_name="oauth_app_containment",
        artifacts_dir=artifacts,
        raw_score={},
        state=session.current_state(),
    )

    assert score["benchmark_family"] == "security_containment"
    assert score["success"] is True
    assert score["dimensions"]["evidence_preservation"] == 1.0
    assert score["dimensions"]["blast_radius_minimization"] >= 0.75


def test_run_benchmark_case_for_family_scenario_includes_family_dimensions(
    tmp_path: Path,
) -> None:
    artifacts = tmp_path / "family_case"
    spec = BenchmarkCaseSpec(
        runner="scripted",
        scenario_name="oauth_app_containment",
        seed=202,
        artifacts_dir=artifacts,
        score_mode="full",
    )

    result = run_benchmark_case(spec)

    assert result.status == "ok"
    assert result.score["benchmark_family"] == "security_containment"
    assert result.diagnostics.benchmark_family == "security_containment"
    assert "evidence_preservation" in result.score["dimensions"]
    assert result.score["workflow_name"] == "security_containment"
    assert result.score["workflow_variant"] == "customer_notify"
    assert result.score["workflow_validation"]["validation_mode"] == "state"
    assert result.score["workflow_validation"]["success_assertion_count"] == 5
    assert result.diagnostics.workflow_name == "security_containment"
    assert result.diagnostics.workflow_variant == "customer_notify"
    assert (artifacts / "workflow_validation.json").exists()


def test_run_benchmark_case_workflow_runner(tmp_path: Path) -> None:
    artifacts = tmp_path / "workflow_case"
    spec = BenchmarkCaseSpec(
        runner="workflow",
        scenario_name="oauth_app_containment",
        workflow_name="security_containment",
        seed=909,
        artifacts_dir=artifacts,
    )

    result = run_benchmark_case(spec)

    assert result.status == "ok"
    assert result.success is True
    assert result.score["workflow_name"] == "security_containment"
    assert result.diagnostics.workflow_name == "security_containment"
    assert result.diagnostics.workflow_variant == "customer_notify"
    assert result.diagnostics.workflow_valid is True
    assert result.diagnostics.workflow_step_count == 5
    assert result.diagnostics.initial_snapshot_id is not None
    assert result.diagnostics.final_snapshot_id is not None
    assert result.diagnostics.latest_snapshot_label == "workflow.final"
    assert result.score["workflow_validation"]["validation_mode"] == "workflow"
    assert result.score["workflow_validation"]["success_assertion_count"] == 5
    assert result.score["workflow_validation"]["success_assertions_failed"] == 0
    assert (artifacts / "workflow_result.json").exists()
    assert (artifacts / "workflow_score.json").exists()
    assert (artifacts / "workflow_validation.json").exists()


def test_run_benchmark_case_bc_family_includes_workflow_validation(
    tmp_path: Path,
) -> None:
    policy_path = tmp_path / "bc_policy.json"
    BCPPolicy(
        tool_counts={"browser.read": 1},
        arg_templates={"browser.read": {}},
    ).save(policy_path)

    result = run_benchmark_case(
        BenchmarkCaseSpec(
            runner="bc",
            scenario_name="oauth_app_containment",
            seed=913,
            artifacts_dir=tmp_path / "bc_family_case",
            bc_model_path=policy_path,
            score_mode="full",
        )
    )

    assert result.status == "ok"
    assert result.score["workflow_name"] == "security_containment"
    assert result.score["workflow_validation"]["validation_mode"] == "state"
    assert result.score["workflow_validation"]["success_assertion_count"] == 5
    assert "success_assertions_failed" in result.score["workflow_validation"]


def test_run_benchmark_case_llm_family_includes_workflow_validation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    def fake_run(
        cmd: list[str],
        *,
        env: dict[str, str],
        capture_output: bool,
        text: bool,
        timeout: int,
    ) -> subprocess.CompletedProcess[str]:
        del capture_output, text, timeout
        artifacts = Path(cmd[cmd.index("--artifacts") + 1])
        session = create_world_session(
            seed=int(env["VEI_SEED"]),
            artifacts_dir=str(artifacts),
            scenario=get_catalog_scenario(env["VEI_SCENARIO"]),
            branch=env["VEI_SCENARIO"],
        )
        session.call_tool(
            "google_admin.preserve_oauth_evidence",
            {"app_id": "OAUTH-9001", "note": "preserve before containment"},
        )
        session.call_tool(
            "google_admin.suspend_oauth_app",
            {"app_id": "OAUTH-9001", "reason": "containment"},
        )
        session.call_tool(
            "siem.preserve_evidence",
            {
                "case_id": "CASE-0001",
                "alert_id": "ALT-9001",
                "note": "preserve alert evidence before notification decision",
            },
        )
        session.call_tool(
            "siem.update_case",
            {
                "case_id": "CASE-0001",
                "customer_notification_required": True,
                "note": "Will notify impacted customers.",
            },
        )
        snapshot = session.snapshot("llm.final")
        snapshots_dir = artifacts / "snapshots"
        snapshots_dir.mkdir(parents=True, exist_ok=True)
        (snapshots_dir / f"{snapshot.snapshot_id:09d}.json").write_text(
            json.dumps(
                {
                    "index": snapshot.snapshot_id,
                    "branch": snapshot.branch,
                    "clock_ms": snapshot.time_ms,
                    "data": snapshot.data.model_dump(mode="json"),
                    "label": snapshot.label,
                }
            ),
            encoding="utf-8",
        )
        (artifacts / "llm_metrics.json").write_text(
            json.dumps(
                {
                    "calls": 3,
                    "prompt_tokens": 120,
                    "completion_tokens": 45,
                    "total_tokens": 165,
                }
            ),
            encoding="utf-8",
        )
        return subprocess.CompletedProcess(cmd, 0, stdout="ok", stderr="")

    monkeypatch.setattr("vei.benchmark.api.subprocess.run", fake_run)

    result = run_benchmark_case(
        BenchmarkCaseSpec(
            runner="llm",
            scenario_name="oauth_app_containment",
            seed=914,
            artifacts_dir=tmp_path / "llm_family_case",
            model="fake-gpt",
            provider="openai",
            score_mode="full",
        )
    )

    assert result.status == "ok"
    assert result.score["workflow_name"] == "security_containment"
    assert result.score["workflow_validation"]["validation_mode"] == "state"
    assert result.score["workflow_validation"]["ok"] is True
    assert result.score["workflow_validation"]["success_assertions_failed"] == 0
    assert result.diagnostics.workflow_valid is True
    assert (tmp_path / "llm_family_case" / "workflow_validation.json").exists()


def test_run_benchmark_case_workflow_runner_variant(tmp_path: Path) -> None:
    artifacts = tmp_path / "workflow_variant_case"
    spec = BenchmarkCaseSpec(
        runner="workflow",
        scenario_name="checkout_spike_mitigation",
        workflow_name="revenue_incident_mitigation",
        workflow_variant="canary_floor",
        seed=910,
        artifacts_dir=artifacts,
    )

    result = run_benchmark_case(spec)

    assert result.status == "ok"
    assert result.success is True
    assert result.score["workflow_variant"] == "canary_floor"
    assert result.diagnostics.workflow_variant == "canary_floor"
    assert (
        result.diagnostics.scenario_metadata["workflow_parameters"]["rollout_pct"] == 5
    )


def test_onboarding_workflow_uses_negative_count_and_deadline_assertions(
    tmp_path: Path,
) -> None:
    workflow = get_benchmark_family_workflow_spec("enterprise_onboarding_migration")
    success_kinds = {item.kind for item in workflow.success_assertions}
    assert {"state_not_contains", "state_count_equals", "time_max_ms"} <= success_kinds

    result = run_benchmark_case(
        BenchmarkCaseSpec(
            runner="workflow",
            scenario_name="acquired_sales_onboarding",
            workflow_name="enterprise_onboarding_migration",
            seed=912,
            artifacts_dir=tmp_path / "workflow_onboarding_case",
        )
    )

    assert result.status == "ok"
    assert result.success is True


def test_vei_eval_benchmark_cli_workflow_runner(tmp_path: Path) -> None:
    runner = typer.testing.CliRunner()
    result = runner.invoke(
        eval_app,
        [
            "benchmark",
            "--runner",
            "workflow",
            "--family",
            "security_containment",
            "--artifacts-root",
            str(tmp_path),
            "--run-id",
            "workflow_cli",
        ],
    )

    assert result.exit_code == 0, result.output
    assert (tmp_path / "workflow_cli" / "aggregate_results.json").exists()
    assert (tmp_path / "workflow_cli" / "oauth_app_containment").exists()


def test_vei_eval_benchmark_cli_workflow_runner_with_explicit_workflow_name(
    tmp_path: Path,
) -> None:
    runner = typer.testing.CliRunner()
    result = runner.invoke(
        eval_app,
        [
            "benchmark",
            "--runner",
            "workflow",
            "--scenario",
            "oauth_app_containment",
            "--workflow-name",
            "security_containment",
            "--workflow-variant",
            "internal_only_review",
            "--artifacts-root",
            str(tmp_path),
            "--run-id",
            "workflow_named_cli",
        ],
    )

    assert result.exit_code == 0, result.output
    aggregate = json.loads(
        (tmp_path / "workflow_named_cli" / "aggregate_results.json").read_text(
            encoding="utf-8"
        )
    )
    assert aggregate[0]["diagnostics"]["workflow_name"] == "security_containment"
    assert aggregate[0]["diagnostics"]["workflow_variant"] == "internal_only_review"
    assert aggregate[0]["provider"] == "baseline"


def test_report_loads_workflow_benchmark_diagnostics(tmp_path: Path) -> None:
    run_dir = tmp_path / "workflow_report"
    run_benchmark_case(
        BenchmarkCaseSpec(
            runner="workflow",
            scenario_name="oauth_app_containment",
            workflow_name="security_containment",
            seed=515,
            artifacts_dir=run_dir / "oauth_app_containment",
        )
    )

    results = load_all_results(run_dir)

    assert len(results) == 1
    assert results[0]["runner"] == "workflow"
    assert results[0]["provider"] == "baseline"
    assert results[0]["diagnostics"]["workflow_name"] == "security_containment"
    assert results[0]["diagnostics"]["initial_snapshot_id"] is not None
    assert results[0]["metrics"]["elapsed_ms"] >= 0


def test_report_attaches_workflow_baseline_deltas(tmp_path: Path) -> None:
    results = _build_security_report_results(tmp_path / "baseline_delta")

    baseline_row = next(
        item
        for item in results
        if item["runner"] == "workflow"
        and item["diagnostics"]["workflow_variant"] == "customer_notify"
    )
    variant_row = next(
        item
        for item in results
        if item["runner"] == "workflow"
        and item["diagnostics"]["workflow_variant"] == "internal_only_review"
    )
    scripted_row = next(item for item in results if item["runner"] == "scripted")

    assert baseline_row["baseline"]["available"] is True
    assert baseline_row["baseline"]["workflow_variant"] == "customer_notify"
    assert baseline_row["baseline"]["workflow_valid"] is True
    assert baseline_row["baseline"]["workflow_issue_count"] == 0
    assert baseline_row["baseline_delta"]["composite_score_delta"] == pytest.approx(0.0)
    assert baseline_row["baseline_delta"]["workflow_valid_delta"] == 0
    assert baseline_row["baseline_delta"]["workflow_issue_count_delta"] == 0
    assert baseline_row["baseline_delta"]["steps_taken_delta"] == 0
    assert baseline_row["baseline_delta"]["time_ms_delta"] == 0

    assert variant_row["baseline"]["workflow_variant"] == "customer_notify"
    assert variant_row["baseline_delta"]["workflow_valid_delta"] == 0
    assert variant_row["baseline_delta"]["workflow_issue_count_delta"] == 0
    assert variant_row["baseline_delta"]["composite_score_delta"] == pytest.approx(
        variant_row["score"]["composite_score"]
        - baseline_row["score"]["composite_score"]
    )
    assert variant_row["baseline_delta"]["steps_taken_delta"] == (
        variant_row["score"]["steps_taken"] - baseline_row["score"]["steps_taken"]
    )

    assert scripted_row["baseline"]["workflow_variant"] == "customer_notify"
    assert scripted_row["baseline_delta"]["workflow_valid_delta"] == -1
    assert scripted_row["baseline_delta"]["workflow_issue_count_delta"] is not None
    assert scripted_row["baseline_delta"]["composite_score_delta"] == pytest.approx(
        scripted_row["score"]["composite_score"]
        - baseline_row["score"]["composite_score"]
    )


def test_csv_report_includes_workflow_baseline_delta_columns(tmp_path: Path) -> None:
    results = _build_security_report_results(tmp_path / "csv_delta")
    output_path = tmp_path / "leaderboard.csv"

    generate_csv_report(results, output_path)

    rows = list(csv.DictReader(output_path.open(encoding="utf-8")))
    variant_row = next(
        item
        for item in rows
        if item["runner"] == "workflow"
        and item["workflow_variant"] == "internal_only_review"
    )
    variant_result = next(
        item
        for item in results
        if item["runner"] == "workflow"
        and item["diagnostics"]["workflow_variant"] == "internal_only_review"
    )

    assert rows
    assert "delta_evidence_preservation" in rows[0]
    assert "workflow_issue_count_delta" in rows[0]
    assert variant_row["baseline_available"] == "True"
    assert variant_row["baseline_workflow_variant"] == "customer_notify"
    assert variant_row["baseline_workflow_valid"] == "True"
    assert variant_row["workflow_issue_count_delta"] == "0"
    assert float(variant_row["composite_score_delta"]) == pytest.approx(
        variant_result["baseline_delta"]["composite_score_delta"]
    )


def test_markdown_report_includes_workflow_baseline_section(tmp_path: Path) -> None:
    results = _build_security_report_results(tmp_path / "markdown_delta")

    markdown = generate_markdown_leaderboard(results)

    assert "## Workflow Baselines" in markdown
    assert "security_containment (customer_notify)" in markdown
    assert (
        "| Model | Success | Score | Δ Score | Steps | Δ Steps | Baseline | Dimensions |"
        in markdown
    )
