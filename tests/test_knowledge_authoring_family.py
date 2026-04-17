from __future__ import annotations

from pathlib import Path

from vei.benchmark.api import run_benchmark_case
from vei.benchmark.families import get_benchmark_family_manifest
from vei.benchmark.models import BenchmarkCaseSpec
from vei.benchmark.workflows import get_benchmark_family_workflow_spec
from vei.workspace.api import create_workspace_from_template, load_workspace


def test_knowledge_authoring_family_manifest() -> None:
    manifest = get_benchmark_family_manifest("knowledge_authoring")

    assert manifest.workflow_name == "knowledge_authoring"
    assert manifest.primary_workflow_variant == "northstar_proposal_drafting"


def test_knowledge_authoring_workflow_spec_uses_knowledge_graph() -> None:
    spec = get_benchmark_family_workflow_spec("knowledge_authoring")

    assert spec.steps[0].graph_domain == "knowledge_graph"
    assert spec.steps[0].graph_action == "compose_artifact"
    assertion_kinds = {item.kind for item in spec.success_assertions}
    assert "citations_present" in assertion_kinds
    assert "format_matches_template" in assertion_kinds


def test_knowledge_authoring_family_builds_vertical_workspace(tmp_path: Path) -> None:
    workspace_root = tmp_path / "knowledge_authoring_workspace"
    create_workspace_from_template(
        root=workspace_root,
        source_kind="family",
        source_ref="knowledge_authoring",
        overwrite=True,
    )

    manifest = load_workspace(workspace_root)
    assert manifest.scenarios[0].workflow_name == "knowledge_authoring"


def test_knowledge_authoring_workflow_runner_completes(tmp_path: Path) -> None:
    artifacts_dir = tmp_path / "artifacts"
    result = run_benchmark_case(
        BenchmarkCaseSpec(
            runner="workflow",
            scenario_name="campaign_launch_guardrail",
            family_name="knowledge_authoring",
            workflow_name="knowledge_authoring",
            workflow_variant="northstar_proposal_drafting",
            artifacts_dir=artifacts_dir,
        )
    )

    assert result.success is True
