from __future__ import annotations

from pathlib import Path

from vei.imports.api import get_import_package_example_path

from .api import (
    activate_workspace_scenario,
    generate_workspace_scenarios_from_import,
    import_workspace,
    list_workspace_runs,
    show_workspace,
)
from .models import WorkspaceIdentityFlowSummary, WorkspaceScenarioSpec


def prepare_identity_workspace_flow(
    root: str | Path,
    *,
    package_path: str | Path | None = None,
    scenario_name: str | None = None,
    overwrite: bool = False,
    replace_generated: bool = True,
    run_workflow: bool = False,
    run_scripted: bool = False,
    workflow_run_id: str | None = None,
    scripted_run_id: str | None = None,
) -> WorkspaceIdentityFlowSummary:
    from vei.run.api import launch_workspace_run

    resolved_root = Path(root).expanduser().resolve()
    selected_package = (
        Path(package_path).expanduser().resolve()
        if package_path is not None
        else get_import_package_example_path("macrocompute_identity_export")
    )
    import_workspace(
        root=resolved_root,
        package_path=selected_package,
        overwrite=overwrite,
    )
    generated = generate_workspace_scenarios_from_import(
        resolved_root, replace_generated=replace_generated
    )
    selected_scenario = scenario_name or _preferred_identity_scenario_name(generated)
    active = activate_workspace_scenario(
        resolved_root,
        selected_scenario,
        bootstrap_contract=True,
    )
    if run_workflow:
        launch_workspace_run(
            resolved_root,
            runner="workflow",
            scenario_name=active.name,
            run_id=workflow_run_id,
        )
    if run_scripted:
        launch_workspace_run(
            resolved_root,
            runner="scripted",
            scenario_name=active.name,
            run_id=scripted_run_id,
        )
    return build_identity_flow_summary(resolved_root)


def build_identity_flow_summary(
    root: str | Path,
) -> WorkspaceIdentityFlowSummary:
    resolved_root = Path(root).expanduser().resolve()
    workspace = show_workspace(resolved_root)
    manifest = workspace.manifest
    imports = workspace.imports
    if (
        manifest.source_kind not in {"import_package", "grounding_bundle"}
        or imports is None
    ):
        raise ValueError("workspace does not expose an imported identity flow")
    active = _resolve_active_scenario(manifest.scenarios, manifest.active_scenario)
    runs = list_workspace_runs(resolved_root)
    generated = [
        scenario.name
        for scenario in manifest.scenarios
        if scenario.metadata.get("generated_from_import")
    ]
    recommended = [
        "Review import diagnostics and source sync history.",
        "Preview the active scenario and contract before launching a freer runner.",
        "Launch a workflow baseline, then compare against scripted or llm behavior in the UI.",
        "Inspect provenance and snapshot diffs on the objects touched by the run.",
    ]
    if runs:
        recommended = [
            "Open the latest run in the UI and inspect event-stream playback.",
            "Compare the workflow baseline with the freer runner on contract delta and provenance.",
            "Review graph-native mutations and snapshot diffs for the active scenario.",
        ]
    return WorkspaceIdentityFlowSummary(
        workspace_name=manifest.name,
        package_name=(imports.package_name if imports is not None else None),
        generated_scenario_count=len(generated),
        active_scenario=active.name,
        contract_path=active.contract_path
        or f"{manifest.contracts_dir}/{active.name}.contract.json",
        origin_counts=dict(imports.origin_counts) if imports is not None else {},
        selected_candidate_family=(
            str(active.metadata.get("candidate_family"))
            if active.metadata.get("candidate_family") is not None
            else None
        ),
        generated_candidates=generated,
        recommended_next_steps=recommended,
        run_ids=[item.run_id for item in runs[:4]],
    )


def _preferred_identity_scenario_name(
    scenarios: list[WorkspaceScenarioSpec],
) -> str:
    preferred = [
        "oversharing_remediation",
        "approval_bottleneck",
        "stale_entitlement_cleanup",
        "acquired_user_cutover",
    ]
    index = {scenario.name: scenario for scenario in scenarios}
    for name in preferred:
        if name in index:
            return name
    if not scenarios:
        raise ValueError("no generated identity scenarios available")
    return scenarios[0].name


def _resolve_active_scenario(
    scenarios: list[WorkspaceScenarioSpec], active_name: str
) -> WorkspaceScenarioSpec:
    for scenario in scenarios:
        if scenario.name == active_name:
            return scenario
    if not scenarios:
        raise ValueError("workspace has no scenarios")
    return scenarios[0]
