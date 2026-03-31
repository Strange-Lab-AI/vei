from __future__ import annotations

import json
from pathlib import Path
from typing import Iterable, Literal

from pydantic import BaseModel, Field

from vei.run.api import (
    launch_workspace_run,
    list_run_manifests,
    load_run_contract_evaluation,
    load_run_manifest,
)
from vei.verticals import (
    default_vertical_contract_variant,
    default_vertical_scenario_variant,
    get_vertical_contract_variant,
    get_vertical_scenario_variant,
)
from vei.verticals.packs import (
    VerticalPackManifest,
    get_vertical_pack_manifest,
    list_vertical_pack_manifests,
)
from vei.workspace.api import (
    activate_workspace_contract_variant,
    activate_workspace_scenario_variant,
    create_workspace_from_template,
    preview_workspace_scenario,
    show_workspace,
)


VerticalCompareRunner = Literal["scripted", "bc", "llm"]


class VerticalDemoSpec(BaseModel):
    vertical_name: str
    workspace_root: Path
    scenario_variant: str | None = None
    contract_variant: str | None = None
    compare_runner: VerticalCompareRunner = "scripted"
    overwrite: bool = True
    seed: int = 42042
    max_steps: int = 18
    compare_model: str | None = None
    compare_provider: str | None = None
    compare_bc_model_path: Path | None = None
    compare_task: str | None = None


class VerticalDemoResult(BaseModel):
    manifest: VerticalPackManifest
    workspace_root: Path
    scenario_name: str
    scenario_variant: str | None = None
    contract_variant: str | None = None
    workflow_run_id: str
    comparison_run_id: str
    compare_runner: VerticalCompareRunner
    workflow_manifest_path: Path
    comparison_manifest_path: Path
    contract_path: Path
    overview_path: Path
    ui_command: str
    what_if_branches: list[str] = Field(default_factory=list)
    baseline_success: bool = False
    comparison_success: bool = False
    baseline_contract_ok: bool | None = None
    comparison_contract_ok: bool | None = None
    baseline_event_count: int = 0
    comparison_event_count: int = 0
    baseline_graph_action_count: int = 0
    comparison_graph_action_count: int = 0
    baseline_snapshot_count: int = 0
    comparison_snapshot_count: int = 0
    baseline_graph_domains: list[str] = Field(default_factory=list)
    comparison_graph_domains: list[str] = Field(default_factory=list)
    baseline_resolved_tools: list[str] = Field(default_factory=list)
    comparison_resolved_tools: list[str] = Field(default_factory=list)
    kernel_thesis: str = ""
    platform_uses: list[str] = Field(default_factory=list)


class VerticalShowcaseSpec(BaseModel):
    vertical_names: list[str] = Field(default_factory=list)
    root: Path
    compare_runner: VerticalCompareRunner = "scripted"
    overwrite: bool = True
    seed: int = 42042
    max_steps: int = 18
    compare_model: str | None = None
    compare_provider: str | None = None
    compare_bc_model_path: Path | None = None
    compare_task: str | None = None
    run_id: str = "vertical_showcase"


class VerticalShowcaseResult(BaseModel):
    run_id: str
    root: Path
    compare_runner: VerticalCompareRunner
    overview_path: Path
    result_path: Path
    demos: list[VerticalDemoResult] = Field(default_factory=list)
    kernel_thesis: str = ""
    platform_uses: list[str] = Field(default_factory=list)


class VerticalVariantMatrixCombination(BaseModel):
    name: str
    title: str
    scenario_variant: str
    contract_variant: str
    rationale: str


class VerticalVariantMatrixSpec(BaseModel):
    vertical_names: list[str] = Field(default_factory=list)
    root: Path
    compare_runner: VerticalCompareRunner = "scripted"
    overwrite: bool = True
    seed: int = 42042
    max_steps: int = 18
    compare_model: str | None = None
    compare_provider: str | None = None
    compare_bc_model_path: Path | None = None
    compare_task: str | None = None
    run_id: str = "variant_matrix"


class VerticalVariantMatrixRun(BaseModel):
    vertical_name: str
    company_name: str
    workspace_root: Path
    combination: VerticalVariantMatrixCombination
    workflow_run_id: str
    comparison_run_id: str
    workflow_contract_ok: bool | None = None
    comparison_contract_ok: bool | None = None
    workflow_event_count: int = 0
    comparison_event_count: int = 0
    kernel_thesis: str = ""
    ui_command: str


class VerticalVariantMatrixResult(BaseModel):
    run_id: str
    root: Path
    compare_runner: VerticalCompareRunner
    overview_path: Path
    result_path: Path
    runs: list[VerticalVariantMatrixRun] = Field(default_factory=list)
    kernel_thesis: str = ""
    platform_uses: list[str] = Field(default_factory=list)


class StoryExportPreview(BaseModel):
    name: str
    title: str
    summary: str
    payload: dict[str, object] = Field(default_factory=dict)


class StoryOutcomeSummary(BaseModel):
    base_world: str
    chosen_situation: str
    chosen_objective: str
    baseline_branch: str
    comparison_branch: str
    what_changed: list[str] = Field(default_factory=list)
    why_it_matters: list[str] = Field(default_factory=list)


class StoryPresentationPrimitive(BaseModel):
    name: str
    title: str
    current_value: str
    summary: str
    kernel_mapping: str


class StoryPresentationBeat(BaseModel):
    step: int
    title: str
    studio_view: str
    operator_action: str
    presenter_note: str
    proof_point: str
    audience_takeaway: str


class StoryPresentation(BaseModel):
    opening_hook: str = ""
    demo_goal: str = ""
    presenter_setup: list[str] = Field(default_factory=list)
    primitives: list[StoryPresentationPrimitive] = Field(default_factory=list)
    beats: list[StoryPresentationBeat] = Field(default_factory=list)
    closing_argument: str = ""
    operator_commands: list[str] = Field(default_factory=list)


class VerticalStoryBundle(BaseModel):
    manifest: VerticalPackManifest
    available_worlds: list[VerticalPackManifest] = Field(default_factory=list)
    workspace_root: Path
    scenario_name: str
    scenario_variant: str
    contract_variant: str
    compare_runner: VerticalCompareRunner
    workflow_run_id: str
    comparison_run_id: str
    kernel_thesis: str
    platform_uses: list[str] = Field(default_factory=list)
    company_briefing: str
    situation_briefing: str
    failure_impact: str
    objective_briefing: str
    branch_labels: list[str] = Field(default_factory=list)
    outcome: StoryOutcomeSummary
    exports_preview: list[StoryExportPreview] = Field(default_factory=list)
    overview_path: Path
    story_manifest_path: Path
    exports_preview_path: Path
    presentation: StoryPresentation = Field(default_factory=StoryPresentation)
    presentation_manifest_path: Path | None = None
    presentation_guide_path: Path | None = None
    ui_command: str
    workflow_contract_ok: bool | None = None
    comparison_contract_ok: bool | None = None
    kernel_proof: dict[str, object] = Field(default_factory=dict)


class VerticalStoryShowcaseSpec(BaseModel):
    vertical_names: list[str] = Field(default_factory=list)
    root: Path
    compare_runner: VerticalCompareRunner = "scripted"
    overwrite: bool = True
    seed: int = 42042
    max_steps: int = 18
    compare_model: str | None = None
    compare_provider: str | None = None
    compare_bc_model_path: Path | None = None
    compare_task: str | None = None
    run_id: str = "story_showcase"
    scenario_variant: str | None = None
    contract_variant: str | None = None


class VerticalStoryShowcaseResult(BaseModel):
    run_id: str
    root: Path
    compare_runner: VerticalCompareRunner
    overview_path: Path
    result_path: Path
    stories: list[VerticalStoryBundle] = Field(default_factory=list)
    kernel_thesis: str = ""
    platform_uses: list[str] = Field(default_factory=list)


def prepare_vertical_demo(spec: VerticalDemoSpec) -> VerticalDemoResult:
    if spec.compare_runner == "llm" and not spec.compare_model:
        raise ValueError("llm comparison requires compare_model")
    if spec.compare_runner == "bc" and spec.compare_bc_model_path is None:
        raise ValueError("bc comparison requires compare_bc_model_path")
    manifest = get_vertical_pack_manifest(spec.vertical_name)
    create_workspace_from_template(
        root=spec.workspace_root,
        source_kind="vertical",
        source_ref=manifest.name,
        overwrite=spec.overwrite,
    )
    if spec.scenario_variant:
        activate_workspace_scenario_variant(
            spec.workspace_root,
            spec.scenario_variant,
            bootstrap_contract=True,
        )
    if spec.contract_variant:
        activate_workspace_contract_variant(spec.workspace_root, spec.contract_variant)
    workflow_manifest = launch_workspace_run(
        spec.workspace_root,
        runner="workflow",
        run_id="workflow_baseline",
        seed=spec.seed,
        max_steps=spec.max_steps,
    )
    comparison_manifest = launch_workspace_run(
        spec.workspace_root,
        runner=spec.compare_runner,
        run_id=f"{spec.compare_runner}_comparison",
        seed=spec.seed,
        model=spec.compare_model,
        provider=spec.compare_provider,
        bc_model_path=spec.compare_bc_model_path,
        task=spec.compare_task,
        max_steps=spec.max_steps,
    )
    preview = preview_workspace_scenario(spec.workspace_root)
    workspace_root = Path(spec.workspace_root).expanduser().resolve()
    run_root = workspace_root / "runs"
    overview_path = workspace_root / "vertical_demo_overview.md"
    what_if_branches = _extract_what_if_branches(preview) or list(
        manifest.what_if_branches
    )
    baseline_summary = _summarize_run_spine(run_root / workflow_manifest.run_id)
    comparison_summary = _summarize_run_spine(run_root / comparison_manifest.run_id)
    result = VerticalDemoResult(
        manifest=manifest,
        workspace_root=workspace_root,
        scenario_name=str(preview["scenario"]["name"]),
        scenario_variant=preview.get("active_scenario_variant"),
        contract_variant=preview.get("active_contract_variant"),
        workflow_run_id=workflow_manifest.run_id,
        comparison_run_id=comparison_manifest.run_id,
        compare_runner=spec.compare_runner,
        workflow_manifest_path=run_root
        / workflow_manifest.run_id
        / "run_manifest.json",
        comparison_manifest_path=run_root
        / comparison_manifest.run_id
        / "run_manifest.json",
        contract_path=workspace_root
        / "contracts"
        / f"{preview['scenario']['name']}.contract.json",
        overview_path=overview_path,
        ui_command=(
            "python -m vei.cli.vei ui serve "
            f"--root {workspace_root} --host 127.0.0.1 --port 3011"
        ),
        what_if_branches=what_if_branches,
        baseline_success=workflow_manifest.success,
        comparison_success=comparison_manifest.success,
        baseline_contract_ok=(
            workflow_manifest.contract.ok if workflow_manifest.contract else None
        ),
        comparison_contract_ok=(
            comparison_manifest.contract.ok if comparison_manifest.contract else None
        ),
        baseline_event_count=int(baseline_summary["event_count"]),
        comparison_event_count=int(comparison_summary["event_count"]),
        baseline_graph_action_count=int(baseline_summary["graph_action_count"]),
        comparison_graph_action_count=int(comparison_summary["graph_action_count"]),
        baseline_snapshot_count=len(workflow_manifest.snapshots),
        comparison_snapshot_count=len(comparison_manifest.snapshots),
        baseline_graph_domains=list(baseline_summary["graph_domains"]),
        comparison_graph_domains=list(comparison_summary["graph_domains"]),
        baseline_resolved_tools=list(baseline_summary["resolved_tools"]),
        comparison_resolved_tools=list(comparison_summary["resolved_tools"]),
        kernel_thesis=_kernel_thesis_statement(),
        platform_uses=_platform_uses(),
    )
    overview_path.write_text(render_vertical_demo_overview(result), encoding="utf-8")
    (workspace_root / "vertical_demo_result.json").write_text(
        json.dumps(result.model_dump(mode="json"), indent=2),
        encoding="utf-8",
    )
    return result


def run_vertical_showcase(spec: VerticalShowcaseSpec) -> VerticalShowcaseResult:
    showcase_root = spec.root.expanduser().resolve() / spec.run_id
    showcase_root.mkdir(parents=True, exist_ok=True)
    selected = (
        [get_vertical_pack_manifest(name) for name in spec.vertical_names]
        if spec.vertical_names
        else list_vertical_pack_manifests()
    )
    demos: list[VerticalDemoResult] = []
    for item in selected:
        demos.append(
            prepare_vertical_demo(
                VerticalDemoSpec(
                    vertical_name=item.name,
                    workspace_root=showcase_root / item.name,
                    compare_runner=spec.compare_runner,
                    overwrite=spec.overwrite,
                    seed=spec.seed,
                    max_steps=spec.max_steps,
                    compare_model=spec.compare_model,
                    compare_provider=spec.compare_provider,
                    compare_bc_model_path=spec.compare_bc_model_path,
                    compare_task=spec.compare_task,
                )
            )
        )
    result = VerticalShowcaseResult(
        run_id=spec.run_id,
        root=showcase_root,
        compare_runner=spec.compare_runner,
        overview_path=showcase_root / "vertical_showcase_overview.md",
        result_path=showcase_root / "vertical_showcase_result.json",
        demos=demos,
        kernel_thesis=_kernel_thesis_statement(),
        platform_uses=_platform_uses(),
    )
    result.overview_path.write_text(
        render_vertical_showcase_overview(result), encoding="utf-8"
    )
    result.result_path.write_text(
        json.dumps(result.model_dump(mode="json"), indent=2), encoding="utf-8"
    )
    return result


def run_vertical_variant_matrix(
    spec: VerticalVariantMatrixSpec,
) -> VerticalVariantMatrixResult:
    showcase_root = spec.root.expanduser().resolve() / spec.run_id
    showcase_root.mkdir(parents=True, exist_ok=True)
    selected = (
        [get_vertical_pack_manifest(name) for name in spec.vertical_names]
        if spec.vertical_names
        else list_vertical_pack_manifests()
    )
    runs: list[VerticalVariantMatrixRun] = []
    for manifest in selected:
        for combo in _curated_variant_matrix(manifest.name):
            workspace_root = showcase_root / manifest.name / combo.name
            create_workspace_from_template(
                root=workspace_root,
                source_kind="vertical",
                source_ref=manifest.name,
                overwrite=spec.overwrite,
            )
            activate_workspace_scenario_variant(
                workspace_root,
                combo.scenario_variant,
                bootstrap_contract=True,
            )
            activate_workspace_contract_variant(workspace_root, combo.contract_variant)
            workflow_manifest = launch_workspace_run(
                workspace_root,
                runner="workflow",
                run_id=f"{combo.name}_workflow",
                seed=spec.seed,
                max_steps=spec.max_steps,
            )
            comparison_manifest = launch_workspace_run(
                workspace_root,
                runner=spec.compare_runner,
                run_id=f"{combo.name}_{spec.compare_runner}",
                seed=spec.seed,
                model=spec.compare_model,
                provider=spec.compare_provider,
                bc_model_path=spec.compare_bc_model_path,
                task=spec.compare_task,
                max_steps=spec.max_steps,
            )
            workflow_summary = _summarize_run_spine(
                workspace_root / "runs" / workflow_manifest.run_id
            )
            comparison_summary = _summarize_run_spine(
                workspace_root / "runs" / comparison_manifest.run_id
            )
            runs.append(
                VerticalVariantMatrixRun(
                    vertical_name=manifest.name,
                    company_name=manifest.company_name,
                    workspace_root=workspace_root,
                    combination=combo,
                    workflow_run_id=workflow_manifest.run_id,
                    comparison_run_id=comparison_manifest.run_id,
                    workflow_contract_ok=(
                        workflow_manifest.contract.ok
                        if workflow_manifest.contract
                        else None
                    ),
                    comparison_contract_ok=(
                        comparison_manifest.contract.ok
                        if comparison_manifest.contract
                        else None
                    ),
                    workflow_event_count=int(workflow_summary["event_count"]),
                    comparison_event_count=int(comparison_summary["event_count"]),
                    kernel_thesis=_kernel_thesis_statement(),
                    ui_command=(
                        "python -m vei.cli.vei ui serve "
                        f"--root {workspace_root} --host 127.0.0.1 --port 3011"
                    ),
                )
            )
    result = VerticalVariantMatrixResult(
        run_id=spec.run_id,
        root=showcase_root,
        compare_runner=spec.compare_runner,
        overview_path=showcase_root / "vertical_variant_matrix_overview.md",
        result_path=showcase_root / "vertical_variant_matrix_result.json",
        runs=runs,
        kernel_thesis=_kernel_thesis_statement(),
        platform_uses=_platform_uses(),
    )
    result.overview_path.write_text(
        render_vertical_variant_matrix_overview(result), encoding="utf-8"
    )
    result.result_path.write_text(
        json.dumps(result.model_dump(mode="json"), indent=2), encoding="utf-8"
    )
    return result


def prepare_vertical_story(spec: VerticalDemoSpec) -> VerticalStoryBundle:
    manifest = get_vertical_pack_manifest(spec.vertical_name)
    scenario_variant_name = (
        spec.scenario_variant or default_vertical_scenario_variant(manifest.name).name
    )
    contract_variant_name = (
        spec.contract_variant or default_vertical_contract_variant(manifest.name).name
    )
    demo = prepare_vertical_demo(
        VerticalDemoSpec(
            vertical_name=manifest.name,
            workspace_root=spec.workspace_root,
            scenario_variant=scenario_variant_name,
            contract_variant=contract_variant_name,
            compare_runner=spec.compare_runner,
            overwrite=spec.overwrite,
            seed=spec.seed,
            max_steps=spec.max_steps,
            compare_model=spec.compare_model,
            compare_provider=spec.compare_provider,
            compare_bc_model_path=spec.compare_bc_model_path,
            compare_task=spec.compare_task,
        )
    )
    workspace_root = Path(demo.workspace_root).expanduser().resolve()
    scenario_variant = get_vertical_scenario_variant(
        manifest.name,
        demo.scenario_variant or scenario_variant_name,
    )
    contract_variant = get_vertical_contract_variant(
        manifest.name,
        demo.contract_variant or contract_variant_name,
    )
    branch_labels = list(scenario_variant.branch_labels or demo.what_if_branches)
    if len(branch_labels) < 2:
        branch_labels = branch_labels + [
            "Stay on the baseline operating path",
            "Take the alternate agent-led path",
        ]
    workflow_contract = (
        load_run_contract_evaluation(workspace_root, demo.workflow_run_id) or {}
    )
    comparison_contract = (
        load_run_contract_evaluation(workspace_root, demo.comparison_run_id) or {}
    )
    exports_preview = _build_story_exports_preview(
        demo,
        workflow_contract,
        comparison_contract,
        branch_labels,
    )
    outcome = _build_story_outcome_summary(
        manifest,
        scenario_variant,
        contract_variant,
        demo,
        workflow_contract,
        comparison_contract,
        branch_labels,
    )
    kernel_proof = {
        "baseline": {
            "events": demo.baseline_event_count,
            "graph_actions": demo.baseline_graph_action_count,
            "snapshots": demo.baseline_snapshot_count,
            "domains": demo.baseline_graph_domains,
            "resolved_tools": demo.baseline_resolved_tools,
        },
        "comparison": {
            "events": demo.comparison_event_count,
            "graph_actions": demo.comparison_graph_action_count,
            "snapshots": demo.comparison_snapshot_count,
            "domains": demo.comparison_graph_domains,
            "resolved_tools": demo.comparison_resolved_tools,
        },
    }
    presentation = _build_story_presentation(
        manifest,
        scenario_variant,
        contract_variant,
        demo,
        branch_labels,
    )
    story = VerticalStoryBundle(
        manifest=manifest,
        available_worlds=list_vertical_pack_manifests(),
        workspace_root=workspace_root,
        scenario_name=demo.scenario_name,
        scenario_variant=scenario_variant.name,
        contract_variant=contract_variant.name,
        compare_runner=demo.compare_runner,
        workflow_run_id=demo.workflow_run_id,
        comparison_run_id=demo.comparison_run_id,
        kernel_thesis=demo.kernel_thesis,
        platform_uses=demo.platform_uses,
        company_briefing=manifest.company_briefing,
        situation_briefing=(
            f"{scenario_variant.description} Why this matters: {scenario_variant.rationale}"
        ),
        failure_impact=manifest.failure_impact,
        objective_briefing=(
            f"{contract_variant.objective_summary} Why it exists: {contract_variant.rationale}"
        ),
        branch_labels=branch_labels,
        outcome=outcome,
        exports_preview=exports_preview,
        overview_path=workspace_root / "story_overview.md",
        story_manifest_path=workspace_root / "story_manifest.json",
        exports_preview_path=workspace_root / "exports_preview.json",
        presentation=presentation,
        presentation_manifest_path=workspace_root / "presentation_manifest.json",
        presentation_guide_path=workspace_root / "presentation_guide.md",
        ui_command=demo.ui_command,
        workflow_contract_ok=demo.baseline_contract_ok,
        comparison_contract_ok=demo.comparison_contract_ok,
        kernel_proof=kernel_proof,
    )
    story.story_manifest_path.write_text(
        json.dumps(story.model_dump(mode="json"), indent=2),
        encoding="utf-8",
    )
    story.exports_preview_path.write_text(
        json.dumps(
            [item.model_dump(mode="json") for item in story.exports_preview], indent=2
        ),
        encoding="utf-8",
    )
    if story.presentation_manifest_path is not None:
        story.presentation_manifest_path.write_text(
            json.dumps(story.presentation.model_dump(mode="json"), indent=2),
            encoding="utf-8",
        )
    if story.presentation_guide_path is not None:
        story.presentation_guide_path.write_text(
            render_vertical_story_presentation_guide(story),
            encoding="utf-8",
        )
    story.overview_path.write_text(
        render_vertical_story_overview(story), encoding="utf-8"
    )
    return story


def run_vertical_story_showcase(
    spec: VerticalStoryShowcaseSpec,
) -> VerticalStoryShowcaseResult:
    showcase_root = spec.root.expanduser().resolve() / spec.run_id
    showcase_root.mkdir(parents=True, exist_ok=True)
    selected = (
        [get_vertical_pack_manifest(name) for name in spec.vertical_names]
        if spec.vertical_names
        else list_vertical_pack_manifests()
    )
    if (spec.scenario_variant or spec.contract_variant) and len(selected) != 1:
        raise ValueError(
            "scenario_variant and contract_variant can only be overridden when exactly one vertical is selected"
        )
    stories: list[VerticalStoryBundle] = []
    for manifest in selected:
        stories.append(
            prepare_vertical_story(
                VerticalDemoSpec(
                    vertical_name=manifest.name,
                    workspace_root=showcase_root / manifest.name,
                    scenario_variant=spec.scenario_variant,
                    contract_variant=spec.contract_variant,
                    compare_runner=spec.compare_runner,
                    overwrite=spec.overwrite,
                    seed=spec.seed,
                    max_steps=spec.max_steps,
                    compare_model=spec.compare_model,
                    compare_provider=spec.compare_provider,
                    compare_bc_model_path=spec.compare_bc_model_path,
                    compare_task=spec.compare_task,
                )
            )
        )
    result = VerticalStoryShowcaseResult(
        run_id=spec.run_id,
        root=showcase_root,
        compare_runner=spec.compare_runner,
        overview_path=showcase_root / "story_showcase_overview.md",
        result_path=showcase_root / "story_showcase_result.json",
        stories=stories,
        kernel_thesis=_kernel_thesis_statement(),
        platform_uses=_platform_uses(),
    )
    result.overview_path.write_text(
        render_vertical_story_showcase_overview(result),
        encoding="utf-8",
    )
    result.result_path.write_text(
        json.dumps(result.model_dump(mode="json"), indent=2),
        encoding="utf-8",
    )
    return result


def load_workspace_story_manifest(root: str | Path) -> VerticalStoryBundle | None:
    workspace_root = Path(root).expanduser().resolve()
    path = workspace_root / "story_manifest.json"
    if path.exists():
        story = VerticalStoryBundle.model_validate_json(
            path.read_text(encoding="utf-8")
        )
        return _ensure_story_presentation(story)
    workspace = show_workspace(workspace_root)
    manifest = workspace.manifest
    if manifest.source_kind != "vertical":
        return None
    vertical_name = manifest.source_ref
    if not vertical_name:
        return None
    pack_manifest = get_vertical_pack_manifest(vertical_name)
    preview = preview_workspace_scenario(workspace_root)
    scenario_variant_name = (
        str(preview.get("active_scenario_variant"))
        if preview.get("active_scenario_variant")
        else default_vertical_scenario_variant(vertical_name).name
    )
    contract_variant_name = (
        str(preview.get("active_contract_variant"))
        if preview.get("active_contract_variant")
        else default_vertical_contract_variant(vertical_name).name
    )
    scenario_variant = get_vertical_scenario_variant(
        vertical_name, scenario_variant_name
    )
    contract_variant = get_vertical_contract_variant(
        vertical_name, contract_variant_name
    )
    workflow_manifest, comparison_manifest = _find_story_runs(workspace_root)
    branch_labels = list(
        scenario_variant.branch_labels or pack_manifest.what_if_branches
    )
    if len(branch_labels) < 2:
        branch_labels = branch_labels + [
            "Stay on the baseline operating path",
            "Take the alternate agent-led path",
        ]
    baseline_summary = (
        _summarize_run_spine(workspace_root / "runs" / workflow_manifest.run_id)
        if workflow_manifest
        else _empty_run_spine()
    )
    comparison_summary = (
        _summarize_run_spine(workspace_root / "runs" / comparison_manifest.run_id)
        if comparison_manifest
        else _empty_run_spine()
    )
    demo_like = VerticalDemoResult(
        manifest=pack_manifest,
        workspace_root=workspace_root,
        scenario_name=str(preview["scenario"]["name"]),
        scenario_variant=scenario_variant.name,
        contract_variant=contract_variant.name,
        workflow_run_id=(
            workflow_manifest.run_id if workflow_manifest else "workflow_baseline"
        ),
        comparison_run_id=(
            comparison_manifest.run_id if comparison_manifest else "comparison_run"
        ),
        compare_runner=(
            comparison_manifest.runner if comparison_manifest else "scripted"
        ),
        workflow_manifest_path=workspace_root
        / "runs"
        / (workflow_manifest.run_id if workflow_manifest else "workflow_baseline")
        / "run_manifest.json",
        comparison_manifest_path=workspace_root
        / "runs"
        / (comparison_manifest.run_id if comparison_manifest else "comparison_run")
        / "run_manifest.json",
        contract_path=workspace_root
        / "contracts"
        / f"{preview['scenario']['name']}.contract.json",
        overview_path=workspace_root / "vertical_demo_overview.md",
        ui_command=(
            "python -m vei.cli.vei ui serve "
            f"--root {workspace_root} --host 127.0.0.1 --port 3011"
        ),
        what_if_branches=branch_labels,
        baseline_success=(
            bool(workflow_manifest.success) if workflow_manifest else False
        ),
        comparison_success=(
            bool(comparison_manifest.success) if comparison_manifest else False
        ),
        baseline_contract_ok=(
            workflow_manifest.contract.ok if workflow_manifest else None
        ),
        comparison_contract_ok=(
            comparison_manifest.contract.ok if comparison_manifest else None
        ),
        baseline_event_count=int(baseline_summary["event_count"]),
        comparison_event_count=int(comparison_summary["event_count"]),
        baseline_graph_action_count=int(baseline_summary["graph_action_count"]),
        comparison_graph_action_count=int(comparison_summary["graph_action_count"]),
        baseline_snapshot_count=(
            len(workflow_manifest.snapshots) if workflow_manifest else 0
        ),
        comparison_snapshot_count=(
            len(comparison_manifest.snapshots) if comparison_manifest else 0
        ),
        baseline_graph_domains=list(baseline_summary["graph_domains"]),
        comparison_graph_domains=list(comparison_summary["graph_domains"]),
        baseline_resolved_tools=list(baseline_summary["resolved_tools"]),
        comparison_resolved_tools=list(comparison_summary["resolved_tools"]),
        kernel_thesis=_kernel_thesis_statement(),
        platform_uses=_platform_uses(),
    )
    workflow_contract = (
        load_run_contract_evaluation(workspace_root, workflow_manifest.run_id) or {}
        if workflow_manifest
        else {}
    )
    comparison_contract = (
        load_run_contract_evaluation(workspace_root, comparison_manifest.run_id) or {}
        if comparison_manifest
        else {}
    )
    presentation = _build_story_presentation(
        pack_manifest,
        scenario_variant,
        contract_variant,
        demo_like,
        branch_labels,
    )
    story = VerticalStoryBundle(
        manifest=pack_manifest,
        available_worlds=list_vertical_pack_manifests(),
        workspace_root=workspace_root,
        scenario_name=str(preview["scenario"]["name"]),
        scenario_variant=scenario_variant.name,
        contract_variant=contract_variant.name,
        compare_runner=(
            comparison_manifest.runner if comparison_manifest else "scripted"
        ),
        workflow_run_id=demo_like.workflow_run_id,
        comparison_run_id=demo_like.comparison_run_id,
        kernel_thesis=_kernel_thesis_statement(),
        platform_uses=_platform_uses(),
        company_briefing=pack_manifest.company_briefing,
        situation_briefing=(
            f"{scenario_variant.description} Why this matters: {scenario_variant.rationale}"
        ),
        failure_impact=pack_manifest.failure_impact,
        objective_briefing=(
            f"{contract_variant.objective_summary} Why it exists: {contract_variant.rationale}"
        ),
        branch_labels=branch_labels,
        outcome=_build_story_outcome_summary(
            pack_manifest,
            scenario_variant,
            contract_variant,
            demo_like,
            workflow_contract,
            comparison_contract,
            branch_labels,
        ),
        exports_preview=_build_story_exports_preview(
            demo_like,
            workflow_contract,
            comparison_contract,
            branch_labels,
        ),
        overview_path=workspace_root / "story_overview.md",
        story_manifest_path=workspace_root / "story_manifest.json",
        exports_preview_path=workspace_root / "exports_preview.json",
        presentation=presentation,
        presentation_manifest_path=workspace_root / "presentation_manifest.json",
        presentation_guide_path=workspace_root / "presentation_guide.md",
        ui_command=demo_like.ui_command,
        workflow_contract_ok=demo_like.baseline_contract_ok,
        comparison_contract_ok=demo_like.comparison_contract_ok,
        kernel_proof={
            "baseline": {
                "events": demo_like.baseline_event_count,
                "graph_actions": demo_like.baseline_graph_action_count,
                "snapshots": demo_like.baseline_snapshot_count,
                "domains": demo_like.baseline_graph_domains,
                "resolved_tools": demo_like.baseline_resolved_tools,
            },
            "comparison": {
                "events": demo_like.comparison_event_count,
                "graph_actions": demo_like.comparison_graph_action_count,
                "snapshots": demo_like.comparison_snapshot_count,
                "domains": demo_like.comparison_graph_domains,
                "resolved_tools": demo_like.comparison_resolved_tools,
            },
        },
    )
    return _ensure_story_presentation(story)


def load_workspace_exports_preview(root: str | Path) -> list[StoryExportPreview]:
    story = load_workspace_story_manifest(root)
    if story is None:
        return []
    return story.exports_preview


def load_workspace_presentation(root: str | Path) -> StoryPresentation | None:
    story = load_workspace_story_manifest(root)
    if story is None:
        return None
    return story.presentation


def _ensure_story_presentation(story: VerticalStoryBundle) -> VerticalStoryBundle:
    if story.presentation.beats:
        if story.presentation_manifest_path is None:
            story.presentation_manifest_path = (
                story.workspace_root / "presentation_manifest.json"
            )
        if story.presentation_guide_path is None:
            story.presentation_guide_path = (
                story.workspace_root / "presentation_guide.md"
            )
        return story
    scenario_variant = get_vertical_scenario_variant(
        story.manifest.name, story.scenario_variant
    )
    contract_variant = get_vertical_contract_variant(
        story.manifest.name, story.contract_variant
    )
    demo_like = VerticalDemoResult(
        manifest=story.manifest,
        workspace_root=story.workspace_root,
        scenario_name=story.scenario_name,
        scenario_variant=story.scenario_variant,
        contract_variant=story.contract_variant,
        workflow_run_id=story.workflow_run_id,
        comparison_run_id=story.comparison_run_id,
        compare_runner=story.compare_runner,
        workflow_manifest_path=story.workspace_root
        / "runs"
        / story.workflow_run_id
        / "run_manifest.json",
        comparison_manifest_path=story.workspace_root
        / "runs"
        / story.comparison_run_id
        / "run_manifest.json",
        contract_path=story.workspace_root
        / "contracts"
        / f"{story.scenario_name}.contract.json",
        overview_path=story.overview_path,
        ui_command=story.ui_command,
        what_if_branches=story.branch_labels,
        baseline_contract_ok=story.workflow_contract_ok,
        comparison_contract_ok=story.comparison_contract_ok,
        baseline_event_count=int(
            story.kernel_proof.get("baseline", {}).get("events", 0)
        ),
        comparison_event_count=int(
            story.kernel_proof.get("comparison", {}).get("events", 0)
        ),
        baseline_graph_action_count=int(
            story.kernel_proof.get("baseline", {}).get("graph_actions", 0)
        ),
        comparison_graph_action_count=int(
            story.kernel_proof.get("comparison", {}).get("graph_actions", 0)
        ),
        baseline_snapshot_count=int(
            story.kernel_proof.get("baseline", {}).get("snapshots", 0)
        ),
        comparison_snapshot_count=int(
            story.kernel_proof.get("comparison", {}).get("snapshots", 0)
        ),
        baseline_graph_domains=list(
            story.kernel_proof.get("baseline", {}).get("domains", [])
        ),
        comparison_graph_domains=list(
            story.kernel_proof.get("comparison", {}).get("domains", [])
        ),
        baseline_resolved_tools=list(
            story.kernel_proof.get("baseline", {}).get("resolved_tools", [])
        ),
        comparison_resolved_tools=list(
            story.kernel_proof.get("comparison", {}).get("resolved_tools", [])
        ),
        kernel_thesis=story.kernel_thesis,
        platform_uses=story.platform_uses,
    )
    story.presentation = _build_story_presentation(
        story.manifest,
        scenario_variant,
        contract_variant,
        demo_like,
        story.branch_labels,
    )
    story.presentation_manifest_path = (
        story.workspace_root / "presentation_manifest.json"
    )
    story.presentation_guide_path = story.workspace_root / "presentation_guide.md"
    return story


def render_vertical_demo_overview(result: VerticalDemoResult) -> str:
    lines = [
        f"# {result.manifest.title}",
        "",
        result.manifest.description,
        "",
        f"- Company: `{result.manifest.company_name}`",
        f"- Scenario: `{result.scenario_name}`",
        f"- Scenario variant: `{result.scenario_variant or 'default'}`",
        f"- Contract variant: `{result.contract_variant or 'default'}`",
        f"- Workflow baseline: `{result.workflow_run_id}`",
        f"- Comparison ({result.compare_runner}): `{result.comparison_run_id}`",
        f"- Contract path: `{result.contract_path}`",
        f"- UI: `{result.ui_command}`",
        "",
        "What this proves:",
    ]
    lines.extend(f"- {bullet}" for bullet in result.manifest.proves)
    lines.extend(
        [
            "",
            "Why the kernel matters:",
            f"- {result.kernel_thesis}",
            (
                f"- Baseline emitted `{result.baseline_event_count}` run events, "
                f"`{result.baseline_graph_action_count}` graph-native actions, and "
                f"`{result.baseline_snapshot_count}` snapshots."
            ),
        ]
    )
    if result.baseline_graph_domains:
        lines.append(
            "- Graph domains touched: "
            + ", ".join(f"`{item}`" for item in result.baseline_graph_domains)
        )
    if result.baseline_resolved_tools:
        lines.append(
            "- Resolved tools used: "
            + ", ".join(f"`{item}`" for item in result.baseline_resolved_tools[:5])
        )
    lines.extend(["", "What this becomes later:"])
    lines.extend(f"- {bullet}" for bullet in result.platform_uses)
    if result.what_if_branches:
        lines.extend(["", "What-if branches:"])
        lines.extend(f"- {branch}" for branch in result.what_if_branches)
    return "\n".join(lines).rstrip() + "\n"


def render_vertical_showcase_overview(result: VerticalShowcaseResult) -> str:
    lines = [
        "# VEI Vertical World Pack Showcase",
        "",
        f"Run ID: `{result.run_id}`",
        f"Comparison runner: `{result.compare_runner}`",
        f"Workspaces: `{len(result.demos)}`",
        "",
        "## One kernel, three companies",
        "",
        result.kernel_thesis,
        "",
        "This is why the same product can later become multiple layers above the kernel:",
        "",
    ]
    lines.extend(f"- {bullet}" for bullet in result.platform_uses)
    lines.extend(
        [
            "",
            "Each world pack below uses the same workspace model, event spine, snapshot/branch system, contract engine, and UI playback surface.",
            "",
        ]
    )
    for demo in result.demos:
        lines.extend(
            [
                f"## {demo.manifest.title}",
                "",
                demo.manifest.description,
                "",
                f"- Workspace: `{demo.workspace_root}`",
                f"- Baseline contract: `{demo.baseline_contract_ok}`",
                f"- Comparison contract: `{demo.comparison_contract_ok}`",
                (
                    f"- Baseline kernel proof: `{demo.baseline_event_count}` events, "
                    f"`{demo.baseline_graph_action_count}` graph actions, "
                    f"`{demo.baseline_snapshot_count}` snapshots"
                ),
                (
                    "- Domains touched: "
                    + ", ".join(f"`{item}`" for item in demo.baseline_graph_domains)
                    if demo.baseline_graph_domains
                    else "- Domains touched: `n/a`"
                ),
                f"- Overview: `{demo.overview_path}`",
                f"- UI: `{demo.ui_command}`",
                "",
            ]
        )
    return "\n".join(lines).rstrip() + "\n"


def render_vertical_variant_matrix_overview(
    result: VerticalVariantMatrixResult,
) -> str:
    lines = [
        "# VEI Variant Matrix Showcase",
        "",
        result.kernel_thesis,
        "",
        "Same world pack, different problem overlays, different contract overlays, same runtime kernel.",
        "",
    ]
    for run in result.runs:
        lines.extend(
            [
                f"## {run.company_name} · {run.combination.title}",
                "",
                f"- Scenario variant: `{run.combination.scenario_variant}`",
                f"- Contract variant: `{run.combination.contract_variant}`",
                f"- Workflow baseline contract: `{run.workflow_contract_ok}`",
                f"- Comparison contract: `{run.comparison_contract_ok}`",
                f"- Baseline events: `{run.workflow_event_count}`",
                f"- Comparison events: `{run.comparison_event_count}`",
                f"- Rationale: {run.combination.rationale}",
                f"- UI: `{run.ui_command}`",
                "",
            ]
        )
    return "\n".join(lines).rstrip() + "\n"


def render_vertical_story_overview(story: VerticalStoryBundle) -> str:
    lines = [
        f"# VEI Story · {story.manifest.company_name}",
        "",
        story.kernel_thesis,
        "",
        "## Company",
        "",
        f"- World: `{story.manifest.title}`",
        f"- Company: `{story.manifest.company_name}`",
        f"- Briefing: {story.company_briefing}",
        f"- Why failure matters: {story.failure_impact}",
        "",
        "## Situation",
        "",
        f"- Scenario: `{story.scenario_name}`",
        f"- Scenario variant: `{story.scenario_variant}`",
        f"- Situation briefing: {story.situation_briefing}",
        "",
        "## Objective",
        "",
        f"- Contract variant: `{story.contract_variant}`",
        f"- Objective briefing: {story.objective_briefing}",
        f"- Presentation manifest: `{story.presentation_manifest_path}`",
        f"- Presentation guide: `{story.presentation_guide_path}`",
        "",
        "## Runs",
        "",
        f"- Workflow baseline: `{story.workflow_run_id}`",
        f"- Comparison ({story.compare_runner}): `{story.comparison_run_id}`",
        f"- Workflow contract ok: `{story.workflow_contract_ok}`",
        f"- Comparison contract ok: `{story.comparison_contract_ok}`",
        f"- UI: `{story.ui_command}`",
        "",
        "## Branch Story",
        "",
        f"- Base world: {story.outcome.base_world}",
        f"- Chosen situation: {story.outcome.chosen_situation}",
        f"- Chosen objective: {story.outcome.chosen_objective}",
        f"- Baseline branch: {story.outcome.baseline_branch}",
        f"- Agent branch: {story.outcome.comparison_branch}",
        "",
        "What changed:",
    ]
    lines.extend(f"- {item}" for item in story.outcome.what_changed)
    lines.extend(["", "Why the outcome matters:"])
    lines.extend(f"- {item}" for item in story.outcome.why_it_matters)
    lines.extend(["", "## Export Preview", ""])
    for item in story.exports_preview:
        lines.extend(
            [
                f"### {item.title}",
                "",
                item.summary,
                "",
            ]
        )
    return "\n".join(lines).rstrip() + "\n"


def render_vertical_story_presentation_guide(story: VerticalStoryBundle) -> str:
    lines = [
        f"# VEI World Briefing Guide · {story.manifest.company_name}",
        "",
        story.presentation.opening_hook,
        "",
        "## Why This World Exists",
        "",
        story.presentation.demo_goal,
        "",
        "## Open The World",
        "",
    ]
    lines.extend(f"- {item}" for item in story.presentation.presenter_setup)
    lines.extend(["", "## World Primitives", ""])
    for primitive in story.presentation.primitives:
        lines.extend(
            [
                f"### {primitive.title}",
                "",
                f"- Current value: `{primitive.current_value}`",
                f"- What it means: {primitive.summary}",
                f"- Under the hood: {primitive.kernel_mapping}",
                "",
            ]
        )
    lines.extend(["## Walkthrough Flow", ""])
    for beat in story.presentation.beats:
        lines.extend(
            [
                f"### Step {beat.step} · {beat.title}",
                "",
                f"- Studio view: `{beat.studio_view}`",
                f"- Operator action: {beat.operator_action}",
                f"- Read it as: {beat.presenter_note}",
                f"- Proof point: {beat.proof_point}",
                f"- Audience takeaway: {beat.audience_takeaway}",
                "",
            ]
        )
    lines.extend(["## Closing Argument", "", story.presentation.closing_argument, ""])
    lines.extend(["## Operator Commands", ""])
    lines.extend(f"- `{item}`" for item in story.presentation.operator_commands)
    return "\n".join(lines).rstrip() + "\n"


def render_vertical_story_showcase_overview(
    result: VerticalStoryShowcaseResult,
) -> str:
    lines = [
        "# VEI Story Showcase",
        "",
        result.kernel_thesis,
        "",
        "This is the clearest path for opening one company world in business language:",
        "- Company",
        "- Situation",
        "- Objective",
        "- Run",
        "- Branch",
        "- Outcome",
        "- Exports",
        "",
    ]
    for story in result.stories:
        lines.extend(
            [
                f"## {story.manifest.company_name}",
                "",
                f"- World: `{story.manifest.title}`",
                f"- Scenario variant: `{story.scenario_variant}`",
                f"- Contract variant: `{story.contract_variant}`",
                f"- Workflow contract ok: `{story.workflow_contract_ok}`",
                f"- Comparison contract ok: `{story.comparison_contract_ok}`",
                f"- Story overview: `{story.overview_path}`",
                f"- Presentation guide: `{story.presentation_guide_path}`",
                f"- UI: `{story.ui_command}`",
                "",
            ]
        )
    return "\n".join(lines).rstrip() + "\n"


def resolve_vertical_names(names: Iterable[str] | None = None) -> list[str]:
    cleaned = [name.strip().lower() for name in (names or []) if name.strip()]
    if cleaned:
        return cleaned
    return [item.name for item in list_vertical_pack_manifests()]


def _extract_what_if_branches(preview: dict[str, object]) -> list[str]:
    scenario = preview.get("scenario")
    if not isinstance(scenario, dict):
        return []
    metadata = scenario.get("metadata")
    if not isinstance(metadata, dict):
        return []
    builder_environment = metadata.get("builder_environment")
    if not isinstance(builder_environment, dict):
        return []
    branches = builder_environment.get("what_if_branches")
    if not isinstance(branches, list):
        return []
    return [str(item) for item in branches if str(item).strip()]


def _kernel_thesis_statement() -> str:
    return (
        "VEI is the shared world kernel underneath every company world we open: the company-specific "
        "part is just the capability graph and contract, while the runtime, event "
        "spine, branching, replay, and inspection surfaces stay the same."
    )


def _platform_uses() -> list[str]:
    return [
        "RL environment: deterministic world state, snapshots, and contract-shaped outcomes become trainable episodes later.",
        "Continuous eval system: the workflow baseline and freer comparison runs already share the same scenario, contract, and event spine.",
        "AI agent management platform: live playback, resolved tools, provenance, and branch diffs make agent behavior inspectable and governable.",
    ]


def _curated_variant_matrix(
    vertical_name: str,
) -> list[VerticalVariantMatrixCombination]:
    curated: dict[str, list[VerticalVariantMatrixCombination]] = {
        "real_estate_management": [
            VerticalVariantMatrixCombination(
                name="opening_readiness",
                title="Opening Readiness",
                scenario_variant="tenant_opening_conflict",
                contract_variant="opening_readiness",
                rationale="Solve the flagship Harbor Point opening conflict cleanly.",
            ),
            VerticalVariantMatrixCombination(
                name="vendor_safety",
                title="Vendor Shock, Safety First",
                scenario_variant="vendor_no_show",
                contract_variant="safety_over_speed",
                rationale="Same property, different future: a vendor drop-out pushes the objective toward safety over schedule.",
            ),
            VerticalVariantMatrixCombination(
                name="tenant_disruption",
                title="Reservation Conflict, Tenant First",
                scenario_variant="double_booked_unit",
                contract_variant="minimize_tenant_disruption",
                rationale="Keep the same world but optimize for tenant continuity and trust.",
            ),
        ],
        "digital_marketing_agency": [
            VerticalVariantMatrixCombination(
                name="launch_safely",
                title="Launch Safely",
                scenario_variant="campaign_launch_guardrail",
                contract_variant="launch_safely",
                rationale="Flagship agency path: approval, pacing, and reporting all become safe together.",
            ),
            VerticalVariantMatrixCombination(
                name="protect_budget",
                title="Protect Budget",
                scenario_variant="budget_runaway",
                contract_variant="protect_budget",
                rationale="Same campaign world, but finance becomes the dominant objective.",
            ),
            VerticalVariantMatrixCombination(
                name="client_comms",
                title="Client Comms First",
                scenario_variant="client_reporting_mismatch",
                contract_variant="client_comms_first",
                rationale="Prioritize truthfulness and artifact integrity when reporting drifts.",
            ),
        ],
        "storage_solutions": [
            VerticalVariantMatrixCombination(
                name="no_overcommit",
                title="No Overcommit",
                scenario_variant="capacity_quote_commitment",
                contract_variant="no_overcommit",
                rationale="Flagship storage path: keep the commitment feasible before it reaches the customer.",
            ),
            VerticalVariantMatrixCombination(
                name="revenue_bias",
                title="Maximize Feasible Revenue",
                scenario_variant="fragmented_capacity",
                contract_variant="maximize_feasible_revenue",
                rationale="Preserve as much value as possible while still restoring feasibility.",
            ),
            VerticalVariantMatrixCombination(
                name="ops_consistency",
                title="Ops Consistency",
                scenario_variant="vendor_dispatch_gap",
                contract_variant="ops_consistency",
                rationale="The company stays the same; the objective changes to downstream execution discipline.",
            ),
        ],
        "b2b_saas": [
            VerticalVariantMatrixCombination(
                name="save_renewal",
                title="Save the Renewal",
                scenario_variant="enterprise_renewal_risk",
                contract_variant="save_the_renewal",
                rationale="Flagship B2B SaaS path: fix the product, rebuild trust, close the renewal.",
            ),
            VerticalVariantMatrixCombination(
                name="protect_revenue",
                title="Protect Revenue",
                scenario_variant="pricing_negotiation_deadlock",
                contract_variant="protect_revenue",
                rationale="Same company, different pressure: hold the line on price while the customer pushes for a discount.",
            ),
            VerticalVariantMatrixCombination(
                name="health_first",
                title="Customer Health First",
                scenario_variant="support_escalation_spiral",
                contract_variant="customer_health_first",
                rationale="Fix the support and product issues before advancing the commercial conversation.",
            ),
        ],
        "service_ops": [
            VerticalVariantMatrixCombination(
                name="protect_sla",
                title="Protect SLA",
                scenario_variant="service_day_collision",
                contract_variant="protect_sla",
                rationale="Flagship service-ops path: recover dispatch fast enough to keep the VIP morning intact.",
            ),
            VerticalVariantMatrixCombination(
                name="protect_revenue",
                title="Protect Revenue",
                scenario_variant="billing_dispute_reopened",
                contract_variant="protect_revenue",
                rationale="Same company, different pressure: contain the reopened billing dispute before revenue and trust erode.",
            ),
            VerticalVariantMatrixCombination(
                name="customer_trust",
                title="Protect Customer Trust",
                scenario_variant="technician_no_show",
                contract_variant="protect_customer_trust",
                rationale="Keep dispatch recovery and customer communication aligned when the technician path breaks.",
            ),
        ],
    }
    if vertical_name not in curated:
        raise KeyError(f"unknown vertical pack: {vertical_name}")
    return curated[vertical_name]


def _build_story_exports_preview(
    demo: VerticalDemoResult,
    workflow_contract: dict[str, object],
    comparison_contract: dict[str, object],
    branch_labels: list[str],
) -> list[StoryExportPreview]:
    workflow_issue_count = _contract_issue_count(
        demo.workflow_manifest_path, workflow_contract
    )
    comparison_issue_count = _contract_issue_count(
        demo.comparison_manifest_path, comparison_contract
    )
    return [
        StoryExportPreview(
            name="rl_episode_export",
            title="RL Episode Export",
            summary=(
                "One world state plus one event spine already yields state transitions, graph-native actions, "
                "contract-shaped rewards, and branch boundaries that can later become trainable RL episodes."
            ),
            payload={
                "scenario_variant": demo.scenario_variant,
                "contract_variant": demo.contract_variant,
                "workflow_run_id": demo.workflow_run_id,
                "event_count": demo.baseline_event_count,
                "graph_action_count": demo.baseline_graph_action_count,
                "snapshot_count": demo.baseline_snapshot_count,
                "branch_labels": branch_labels,
            },
        ),
        StoryExportPreview(
            name="continuous_eval_export",
            title="Continuous Eval Export",
            summary=(
                "The same company world, situation, and objective can be replayed as a baseline/comparison pair, "
                "which makes VEI naturally usable as a continuous eval harness later."
            ),
            payload={
                "workflow_run_id": demo.workflow_run_id,
                "comparison_run_id": demo.comparison_run_id,
                "workflow_contract_ok": demo.baseline_contract_ok,
                "comparison_contract_ok": demo.comparison_contract_ok,
                "workflow_issue_count": workflow_issue_count,
                "comparison_issue_count": comparison_issue_count,
            },
        ),
        StoryExportPreview(
            name="agent_ops_export",
            title="Agent Ops Export",
            summary=(
                "Playback, resolved tools, graph domains, receipts, and contract findings already form an agent-observability bundle "
                "that can later become an operations and governance surface."
            ),
            payload={
                "comparison_runner": demo.compare_runner,
                "resolved_tools": demo.baseline_resolved_tools,
                "comparison_resolved_tools": demo.comparison_resolved_tools,
                "graph_domains": demo.baseline_graph_domains,
                "comparison_graph_domains": demo.comparison_graph_domains,
                "contract_issue_count": comparison_issue_count,
            },
        ),
    ]


def _build_story_outcome_summary(
    manifest: VerticalPackManifest,
    scenario_variant,
    contract_variant,
    demo: VerticalDemoResult,
    workflow_contract: dict[str, object],
    comparison_contract: dict[str, object],
    branch_labels: list[str],
) -> StoryOutcomeSummary:
    workflow_issue_count = _contract_issue_count(
        demo.workflow_manifest_path, workflow_contract
    )
    comparison_issue_count = _contract_issue_count(
        demo.comparison_manifest_path, comparison_contract
    )
    return StoryOutcomeSummary(
        base_world=manifest.company_briefing,
        chosen_situation=scenario_variant.description,
        chosen_objective=contract_variant.objective_summary,
        baseline_branch=branch_labels[0],
        comparison_branch=(
            branch_labels[1] if len(branch_labels) > 1 else "Agent branch"
        ),
        what_changed=[
            (
                f"The same {manifest.company_name} world stayed fixed while the `{scenario_variant.name}` "
                f"situation overlay and `{contract_variant.name}` objective overlay defined this run."
            ),
            (
                f"The workflow baseline resolved the world in `{demo.baseline_event_count}` events and "
                f"`{demo.baseline_graph_action_count}` graph actions; the comparison run took "
                f"`{demo.comparison_event_count}` events and `{demo.comparison_graph_action_count}` graph actions."
            ),
            (
                f"Both runs used the same kernel surfaces, but the alternate path touched different tools and "
                f"ended with `{comparison_issue_count}` contract issue(s) instead of `{workflow_issue_count}`."
            ),
        ],
        why_it_matters=[
            (
                f"The baseline passes because the company world, situation, and objective all stay aligned: "
                f"`{contract_variant.title}` rewards the right business behavior for `{scenario_variant.title}`."
            ),
            (
                "The comparison path fails meaningfully, which is the point of the kernel: same company, "
                "different decisions, different outcome."
            ),
            manifest.failure_impact,
        ],
    )


def _build_story_presentation(
    manifest: VerticalPackManifest,
    scenario_variant,
    contract_variant,
    demo: VerticalDemoResult,
    branch_labels: list[str],
) -> StoryPresentation:
    opening_hook = (
        "VEI is one enterprise world kernel. This world shows that the same runtime can "
        "instantiate different companies, vary the situation, vary the objective, and "
        "still produce inspectable runs, branches, and exportable artifacts."
    )
    demo_goal = (
        f"Start from {manifest.company_name} as a stable company world, then watch how "
        f"`{scenario_variant.title}` changes the situation, how `{contract_variant.title}` "
        "changes the definition of success, and how the same kernel turns both runs into "
        "playback, branching, and future RL/eval/agent-ops outputs."
    )
    presenter_setup = [
        "Open Studio on the Briefing view, then move into Living Company before you touch anything else.",
        "Frame the product as a world studio for enterprises, not a static workflow viewer.",
        "Keep the language anchored in the company world: Company, Situation, and Objective are the stable user-facing primitives.",
        f"Use `{branch_labels[0] if branch_labels else 'baseline path'}` versus "
        f"`{branch_labels[1] if len(branch_labels) > 1 else 'agent path'}` to explain branching.",
    ]
    primitives = [
        StoryPresentationPrimitive(
            name="company",
            title="Company",
            current_value=manifest.company_name,
            summary=(
                "Start with one stable business world. The company stays fixed while the situation and the objective move around it."
            ),
            kernel_mapping="Workspace + blueprint + capability graphs",
        ),
        StoryPresentationPrimitive(
            name="situation",
            title="Situation",
            current_value=scenario_variant.title,
            summary=(
                "Situations are overlays on the base world. They add deadline pressure, faults, and branch-worthy tradeoffs without rebuilding the company."
            ),
            kernel_mapping="Scenario variant overlay on the world state",
        ),
        StoryPresentationPrimitive(
            name="objective",
            title="Objective",
            current_value=contract_variant.title,
            summary=(
                "Objectives change what counts as good behavior. The same company and same situation can be judged under different business preferences."
            ),
            kernel_mapping="Contract variant overlay on the shared contract engine",
        ),
        StoryPresentationPrimitive(
            name="run",
            title="Run",
            current_value=f"{demo.workflow_run_id} vs {demo.comparison_run_id}",
            summary=(
                "Runs are attempts to solve the same world under the same event spine. Workflow, scripted, and LLM agents all land in one runtime model."
            ),
            kernel_mapping="Canonical run/event spine + snapshots + playback",
        ),
        StoryPresentationPrimitive(
            name="branch",
            title="Branch",
            current_value="Baseline branch and agent branch",
            summary=(
                "Branching shows alternate futures from one company world. That is the most intuitive proof that this is an engine, not a static benchmark."
            ),
            kernel_mapping="Snapshots + branch labels + diff and replay surfaces",
        ),
        StoryPresentationPrimitive(
            name="exports",
            title="Exports",
            current_value="RL / eval / agent ops preview",
            summary=(
                "The same run artifacts later become RL episodes, continuous eval cases, and agent observability bundles."
            ),
            kernel_mapping="Derived artifacts from the same run and contract outputs",
        ),
    ]
    beats = [
        StoryPresentationBeat(
            step=1,
            title="Open with the kernel thesis",
            studio_view="presentation",
            operator_action="Start on Briefing, then move into Living Company once the software wall is visible.",
            presenter_note=(
                "Say that VEI is one enterprise world kernel. We are about to show different companies, different situations, and different objectives on top of the same runtime."
            ),
            proof_point="The world opens with the engine, not with a single handcrafted scenario.",
            audience_takeaway="This is a reusable platform layer, not a one-off workflow viewer.",
        ),
        StoryPresentationBeat(
            step=2,
            title="Choose the company world",
            studio_view="worlds",
            operator_action="Click Company and show that this world is one stable business with its own graphs and operating surfaces.",
            presenter_note=(
                f"Introduce {manifest.company_name} in plain language, explain what the company does, and why failure in this world has real business consequences."
            ),
            proof_point="Different companies can live on the same kernel without changing the runtime model.",
            audience_takeaway="The kernel is flexible enough to instantiate very different enterprise environments.",
        ),
        StoryPresentationBeat(
            step=3,
            title="Show the situation overlay",
            studio_view="situations",
            operator_action="Click Situation, highlight the active scenario variant, and call out what changed from the base world.",
            presenter_note=(
                f"Explain that `{scenario_variant.title}` is not a different company. It is one alternate future layered on top of the same company world."
            ),
            proof_point="Problem setup is a first-class overlay, not a rewritten environment.",
            audience_takeaway="The same company can generate many meaningful simulations.",
        ),
        StoryPresentationBeat(
            step=4,
            title="Show the objective overlay",
            studio_view="objectives",
            operator_action="Click Objective and compare the active contract with the other objective variants.",
            presenter_note=(
                f"Explain that `{contract_variant.title}` tells VEI what good looks like in this situation, and that different objectives can produce different preferred behavior on the same world."
            ),
            proof_point="Success criteria are separate from both the world and the situation.",
            audience_takeaway="This same kernel can later support eval, policy testing, and reward shaping.",
        ),
        StoryPresentationBeat(
            step=5,
            title="Run the baseline and the agent path",
            studio_view="runs",
            operator_action="Launch or open the workflow baseline, then the comparison run, and play the timeline for a few events.",
            presenter_note=(
                "Point out that every action lands in one event spine with graph intent, resolved tools, and snapshots. That is what makes the world inspectable instead of magical."
            ),
            proof_point="Same runtime model for deterministic baseline and freer agent behavior.",
            audience_takeaway="This is already a serious observability surface, not just a benchmark harness.",
        ),
        StoryPresentationBeat(
            step=6,
            title="Explain the branch and outcome",
            studio_view="runs",
            operator_action="Scroll to Branch + Outcome and contrast the baseline branch with the agent branch.",
            presenter_note=(
                "Use the branch story to explain that the company world stayed the same, but the decisions changed, so the business result changed."
            ),
            proof_point="Branching makes alternate futures legible on top of one shared world state.",
            audience_takeaway="This is why VEI can later serve as a simulation engine, recovery lab, and decision-testing system.",
        ),
        StoryPresentationBeat(
            step=7,
            title="Close on the platform bridge",
            studio_view="runs",
            operator_action="Finish on Exports and tie the run outputs to RL episodes, continuous eval, and agent operations.",
            presenter_note=(
                "Close by saying that this world already emits the ingredients for the next products: RL transitions, eval comparisons, and agent observability."
            ),
            proof_point="The future-platform story is a direct extension of the current artifacts, not a speculative rewrite.",
            audience_takeaway="The upside is a family of products built on one world kernel.",
        ),
    ]
    return StoryPresentation(
        opening_hook=opening_hook,
        demo_goal=demo_goal,
        presenter_setup=presenter_setup,
        primitives=primitives,
        beats=beats,
        closing_argument=(
            "The core claim is simple: VEI already behaves like a world studio for enterprises. "
            "These worlds are different instantiations of one kernel, and the same kernel is what later becomes an RL environment, a continuous eval harness, and an agent management platform."
        ),
        operator_commands=[
            f"python -m vei.cli.vei project init --vertical {manifest.name} --root {demo.workspace_root}",
            f"python -m vei.cli.vei scenario activate --root {demo.workspace_root} --variant {scenario_variant.name} --bootstrap-contract",
            f"python -m vei.cli.vei contract activate --root {demo.workspace_root} --variant {contract_variant.name}",
            demo.ui_command,
        ],
    )


def _find_story_runs(root: str | Path):
    manifests = list_run_manifests(root)
    workflow_manifest = next(
        (item for item in manifests if item.runner == "workflow"), None
    )
    comparison_manifest = next(
        (item for item in manifests if item.runner in {"scripted", "bc", "llm"}),
        None,
    )
    return workflow_manifest, comparison_manifest


def _empty_run_spine() -> dict[str, object]:
    return {
        "event_count": 0,
        "graph_action_count": 0,
        "graph_domains": [],
        "resolved_tools": [],
    }


def _contract_issue_count(
    manifest_path: Path, contract_payload: dict[str, object]
) -> int:
    contract_issues = (
        len(list(contract_payload.get("issues", [])))
        if isinstance(contract_payload.get("issues"), list)
        else 0
    )
    if manifest_path.exists():
        manifest = load_run_manifest(manifest_path)
        return max(contract_issues, int(manifest.contract.issue_count or 0))
    return contract_issues


def _summarize_run_spine(run_root: Path) -> dict[str, object]:
    events_path = run_root / "events.jsonl"
    if not events_path.exists():
        return {
            "event_count": 0,
            "graph_action_count": 0,
            "graph_domains": [],
            "resolved_tools": [],
        }
    graph_domains: list[str] = []
    resolved_tools: list[str] = []
    event_count = 0
    graph_action_count = 0
    with events_path.open("r", encoding="utf-8") as handle:
        for raw_line in handle:
            line = raw_line.strip()
            if not line:
                continue
            event_count += 1
            payload = json.loads(line)
            graph_domain = payload.get("graph_domain")
            if isinstance(graph_domain, str) and graph_domain:
                graph_action_count += 1
                if graph_domain not in graph_domains:
                    graph_domains.append(graph_domain)
            resolved_tool = payload.get("resolved_tool")
            if (
                isinstance(resolved_tool, str)
                and resolved_tool
                and resolved_tool not in resolved_tools
            ):
                resolved_tools.append(resolved_tool)
    return {
        "event_count": event_count,
        "graph_action_count": graph_action_count,
        "graph_domains": graph_domains,
        "resolved_tools": resolved_tools,
    }


__all__ = [
    "VerticalCompareRunner",
    "VerticalDemoResult",
    "VerticalDemoSpec",
    "StoryPresentation",
    "StoryPresentationBeat",
    "StoryPresentationPrimitive",
    "VerticalStoryBundle",
    "VerticalStoryShowcaseResult",
    "VerticalStoryShowcaseSpec",
    "StoryExportPreview",
    "StoryOutcomeSummary",
    "VerticalShowcaseResult",
    "VerticalShowcaseSpec",
    "VerticalVariantMatrixCombination",
    "VerticalVariantMatrixResult",
    "VerticalVariantMatrixSpec",
    "load_workspace_exports_preview",
    "load_workspace_presentation",
    "load_workspace_story_manifest",
    "prepare_vertical_story",
    "prepare_vertical_demo",
    "render_vertical_story_presentation_guide",
    "render_vertical_story_overview",
    "render_vertical_story_showcase_overview",
    "render_vertical_demo_overview",
    "render_vertical_showcase_overview",
    "render_vertical_variant_matrix_overview",
    "resolve_vertical_names",
    "run_vertical_story_showcase",
    "run_vertical_showcase",
    "run_vertical_variant_matrix",
]
