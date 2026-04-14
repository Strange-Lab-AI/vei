from __future__ import annotations

from pathlib import Path

from vei.context.api import ContextProviderConfig, ContextSnapshot
from vei.contract.models import ContractSpec
from vei.pilot.api import (
    build_pilot_status,
    reset_pilot_gateway,
    start_pilot,
    stop_pilot,
)
from vei.run.api import launch_workspace_run, list_run_manifests
from vei.twin.models import TwinArchetype
from vei.workspace.api import (
    activate_workspace_contract_variant,
    activate_workspace_scenario_variant,
    list_workspace_contract_variants,
    list_workspace_scenario_variants,
    load_workspace,
    load_workspace_contract,
    preview_workspace_scenario,
    resolve_workspace_scenario,
)

from vei.pilot.exercise_models import (
    ExerciseCatalogItem,
    ExerciseComparisonRow,
    ExerciseCompatibilityEndpoint,
    ExerciseCompatibilitySurface,
    ExerciseManifest,
    ExerciseStatus,
)

EXERCISE_MANIFEST_FILE = "exercise_manifest.json"


def start_exercise(
    root: str | Path,
    *,
    snapshot: ContextSnapshot | None = None,
    provider_configs: list[ContextProviderConfig] | None = None,
    organization_name: str | None = None,
    organization_domain: str = "",
    archetype: TwinArchetype = "b2b_saas",
    scenario_variant: str | None = None,
    contract_variant: str | None = None,
    gateway_token: str | None = None,
    host: str = "127.0.0.1",
    gateway_port: int = 3020,
    studio_port: int = 3011,
    rebuild: bool = False,
) -> ExerciseStatus:
    workspace_root = Path(root).expanduser().resolve()
    start_pilot(
        workspace_root,
        snapshot=snapshot,
        provider_configs=provider_configs,
        organization_name=organization_name,
        organization_domain=organization_domain,
        archetype=archetype,
        scenario_variant=scenario_variant,
        contract_variant=contract_variant,
        gateway_token=gateway_token,
        host=host,
        gateway_port=gateway_port,
        studio_port=studio_port,
        rebuild=rebuild,
    )
    _ensure_comparison_runs(workspace_root)
    _write_exercise_manifest(workspace_root)
    return build_exercise_status(workspace_root)


def stop_exercise(root: str | Path) -> ExerciseStatus:
    workspace_root = Path(root).expanduser().resolve()
    stop_pilot(workspace_root)
    if not (workspace_root / EXERCISE_MANIFEST_FILE).exists():
        _write_exercise_manifest(workspace_root)
    return build_exercise_status(workspace_root)


def activate_exercise(
    root: str | Path,
    *,
    scenario_variant: str,
    contract_variant: str | None = None,
) -> ExerciseStatus:
    workspace_root = Path(root).expanduser().resolve()
    activate_workspace_scenario_variant(
        workspace_root,
        scenario_variant,
        bootstrap_contract=True,
    )
    selected_contract = contract_variant or _default_contract_variant(workspace_root)
    activate_workspace_contract_variant(workspace_root, selected_contract)
    _ensure_comparison_runs(workspace_root, force_new=True)
    try:
        reset_pilot_gateway(workspace_root)
    except (FileNotFoundError, RuntimeError):
        pass
    _write_exercise_manifest(workspace_root)
    return build_exercise_status(workspace_root)


def build_exercise_status(root: str | Path) -> ExerciseStatus:
    workspace_root = Path(root).expanduser().resolve()
    manifest = load_exercise_manifest(workspace_root)
    launch_status = build_pilot_status(workspace_root)
    comparison = _build_comparison(workspace_root)
    return ExerciseStatus(
        manifest=manifest,
        launch_status=launch_status,
        comparison=comparison,
    )


def load_exercise_manifest(root: str | Path) -> ExerciseManifest:
    workspace_root = Path(root).expanduser().resolve()
    path = workspace_root / EXERCISE_MANIFEST_FILE
    if not path.exists():
        _write_exercise_manifest(workspace_root)
    return ExerciseManifest.model_validate_json(path.read_text(encoding="utf-8"))


def _write_exercise_manifest(workspace_root: Path) -> ExerciseManifest:
    preview = preview_workspace_scenario(workspace_root)
    workspace = load_workspace(workspace_root)
    contract = load_workspace_contract(workspace_root)
    vertical_name = str(workspace.source_ref or "b2b_saas")
    active_contract = str(
        preview.get("active_contract_variant")
        or _default_contract_variant(workspace_root)
    )
    manifest = ExerciseManifest(
        workspace_root=workspace_root,
        workspace_name=workspace.name,
        company_name=workspace.title or workspace.name,
        archetype=vertical_name,
        crisis_name=str(
            preview.get("scenario_variant_title")
            or preview.get("active_scenario_variant")
            or workspace.active_scenario
        ),
        scenario_variant=str(
            preview.get("active_scenario_variant") or workspace.active_scenario
        ),
        contract_variant=active_contract,
        success_criteria=_success_criteria(contract),
        supported_api_subset=_compatibility_matrix(),
        catalog=_build_catalog(workspace_root, active_contract),
        recommended_first_move=(
            "Read the communication trail first, confirm the open work item, "
            "then take one customer-safe action."
        ),
    )
    (workspace_root / EXERCISE_MANIFEST_FILE).write_text(
        manifest.model_dump_json(indent=2),
        encoding="utf-8",
    )
    return manifest


def _build_catalog(
    workspace_root: Path,
    active_contract: str,
) -> list[ExerciseCatalogItem]:
    preview = preview_workspace_scenario(workspace_root)
    active_scenario_variant = str(preview.get("active_scenario_variant") or "")
    items: list[ExerciseCatalogItem] = []
    for variant in list_workspace_scenario_variants(workspace_root):
        if not isinstance(variant, dict):
            continue
        title = str(variant.get("title") or variant.get("name") or "Scenario")
        summary = str(
            variant.get("description")
            or "Switch to this crisis and watch the company react."
        )
        items.append(
            ExerciseCatalogItem(
                scenario_variant=str(variant.get("name") or ""),
                crisis_name=title,
                summary=summary,
                contract_variant=active_contract,
                objective_summary=str(
                    preview.get("contract_objective_summary")
                    or preview.get("vertical_contract_objective_summary")
                    or "Resolve the crisis without leaving the company in a worse state."
                ),
                active=str(variant.get("name") or "") == active_scenario_variant,
            )
        )
    return items


def _success_criteria(contract: ContractSpec) -> list[str]:
    lines: list[str] = []
    objective = contract.metadata.get("vertical_contract_objective_summary")
    if isinstance(objective, str) and objective.strip():
        lines.append(objective.strip())
    for item in contract.policy_invariants:
        if item.description and item.description not in lines:
            lines.append(item.description)
        if len(lines) >= 3:
            break
    for item in contract.success_predicates:
        if item.description and item.description not in lines:
            lines.append(item.description)
        if len(lines) >= 4:
            break
    if not lines:
        lines.append("Resolve the crisis and leave the company in a healthier state.")
    return lines[:4]


def _compatibility_matrix() -> list[ExerciseCompatibilitySurface]:
    return [
        ExerciseCompatibilitySurface(
            surface="slack",
            title="Slack",
            base_path="/slack/api",
            endpoints=[
                ExerciseCompatibilityEndpoint(
                    method="GET",
                    path="/conversations.list",
                    description="List visible channels",
                ),
                ExerciseCompatibilityEndpoint(
                    method="GET",
                    path="/conversations.history",
                    description="Read channel history",
                ),
                ExerciseCompatibilityEndpoint(
                    method="GET",
                    path="/conversations.replies",
                    description="Read thread replies",
                ),
                ExerciseCompatibilityEndpoint(
                    method="POST",
                    path="/chat.postMessage",
                    description="Post a new channel or thread message",
                ),
            ],
        ),
        ExerciseCompatibilitySurface(
            surface="jira",
            title="Jira",
            base_path="/jira/rest/api/3",
            endpoints=[
                ExerciseCompatibilityEndpoint(
                    method="GET",
                    path="/search",
                    description="Search visible issues",
                ),
                ExerciseCompatibilityEndpoint(
                    method="GET",
                    path="/project",
                    description="List the current project",
                ),
                ExerciseCompatibilityEndpoint(
                    method="GET",
                    path="/issue/{issue_id}",
                    description="Fetch one issue",
                ),
                ExerciseCompatibilityEndpoint(
                    method="POST",
                    path="/issue/{issue_id}/comment",
                    description="Add one issue comment",
                ),
                ExerciseCompatibilityEndpoint(
                    method="POST",
                    path="/issue/{issue_id}/transitions",
                    description="Move an issue to a new status",
                ),
            ],
        ),
        ExerciseCompatibilitySurface(
            surface="graph",
            title="Microsoft Graph",
            base_path="/graph/v1.0",
            endpoints=[
                ExerciseCompatibilityEndpoint(
                    method="GET",
                    path="/me/messages",
                    description="List inbox messages",
                ),
                ExerciseCompatibilityEndpoint(
                    method="GET",
                    path="/me/messages/{message_id}",
                    description="Read one message",
                ),
                ExerciseCompatibilityEndpoint(
                    method="POST",
                    path="/me/sendMail",
                    description="Send one message",
                ),
                ExerciseCompatibilityEndpoint(
                    method="GET",
                    path="/me/events",
                    description="List calendar events",
                ),
                ExerciseCompatibilityEndpoint(
                    method="POST",
                    path="/me/events",
                    description="Create a calendar event",
                ),
            ],
        ),
        ExerciseCompatibilitySurface(
            surface="salesforce",
            title="Salesforce",
            base_path="/salesforce/services/data/v60.0",
            endpoints=[
                ExerciseCompatibilityEndpoint(
                    method="GET",
                    path="/query",
                    description="Query visible accounts, contacts, and opportunities",
                ),
                ExerciseCompatibilityEndpoint(
                    method="GET",
                    path="/sobjects/Opportunity/{record_id}",
                    description="Fetch one opportunity",
                ),
                ExerciseCompatibilityEndpoint(
                    method="POST",
                    path="/sobjects/Opportunity",
                    description="Create one opportunity",
                ),
                ExerciseCompatibilityEndpoint(
                    method="POST",
                    path="/sobjects/Task",
                    description="Log one CRM task",
                ),
            ],
        ),
    ]


def _ensure_comparison_runs(workspace_root: Path, *, force_new: bool = False) -> None:
    current_scenario = resolve_workspace_scenario(workspace_root).name
    manifests = list_run_manifests(workspace_root)
    has_workflow = any(
        item.runner == "workflow" and item.scenario_name == current_scenario
        for item in manifests
    )
    has_scripted = any(
        item.runner == "scripted" and item.scenario_name == current_scenario
        for item in manifests
    )
    if force_new or not has_workflow:
        launch_workspace_run(workspace_root, runner="workflow")
    if force_new or not has_scripted:
        launch_workspace_run(workspace_root, runner="scripted")


def _build_comparison(workspace_root: Path) -> list[ExerciseComparisonRow]:
    manifests = list_run_manifests(workspace_root)
    current_scenario = resolve_workspace_scenario(workspace_root).name
    rows: list[ExerciseComparisonRow] = []
    for runner, label in (
        ("workflow", "Workflow baseline"),
        ("scripted", "Scripted path"),
        ("external", "External agent path"),
    ):
        manifest = next(
            (
                item
                for item in manifests
                if item.runner == runner and item.scenario_name == current_scenario
            ),
            None,
        )
        if manifest is None:
            rows.append(
                ExerciseComparisonRow(
                    runner=runner,
                    label=label,
                    summary="This path has not been run yet.",
                )
            )
            continue
        rows.append(
            ExerciseComparisonRow(
                runner=runner,
                label=label,
                run_id=manifest.run_id,
                status=manifest.status,
                success=manifest.success,
                contract_ok=manifest.contract.ok,
                issue_count=manifest.contract.issue_count,
                action_count=int(manifest.metrics.actions or 0),
                summary=_comparison_summary(
                    manifest.status, manifest.success, manifest.contract.ok
                ),
            )
        )
    return rows


def _comparison_summary(
    status: str,
    success: bool | None,
    contract_ok: bool | None,
) -> str:
    if status == "running":
        return "This path is still underway."
    if success and contract_ok:
        return "This path helped and left the company in a healthier state."
    if contract_ok:
        return "This path completed cleanly, but it still deserves review."
    if status == "error":
        return "This path broke before it resolved the crisis."
    return "This path completed, but the company still carries open risk."


def _default_contract_variant(workspace_root: Path) -> str:
    variants = list_workspace_contract_variants(workspace_root)
    if not variants:
        return ""
    return str(variants[0].get("name") or "")
