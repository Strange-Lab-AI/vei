from __future__ import annotations

import json
import secrets
from pathlib import Path
from typing import Any, Iterable, Sequence, TypeVar

from fastapi import FastAPI
from pydantic import BaseModel

from vei.blueprint import get_facade_plugin, resolve_gateway_surface_bindings
from vei.whatif.filenames import CONTEXT_SNAPSHOT_FILE
from vei.blueprint.api import (
    BlueprintAsset,
    BlueprintCapabilityGraphsAsset,
    BlueprintCommGraphAsset,
    BlueprintEnvironmentAsset,
    BlueprintDocumentAsset,
    BlueprintDocGraphAsset,
    BlueprintIdentityGraphAsset,
    BlueprintIdentityUserAsset,
    BlueprintMailMessageAsset,
    BlueprintMailThreadAsset,
    BlueprintSlackMessageAsset,
    BlueprintTicketAsset,
    BlueprintWorkGraphAsset,
)
from vei.context.api import (
    capture_context,
    hydrate_blueprint,
    write_canonical_history_sidecars,
)
from vei.context.api import ContextProviderConfig, ContextSnapshot
from vei.governor import (
    GovernorWorkspaceConfig,
    default_governor_workspace_config,
    governor_metadata_payload,
)
from vei.verticals import build_vertical_blueprint_asset
from vei.workspace.api import (
    activate_workspace_contract_variant,
    activate_workspace_scenario_variant,
    compile_workspace,
    create_workspace_from_template,
    load_workspace,
    preview_workspace_scenario,
    write_workspace,
)

from ._api_exports import load_api_export
from .models import (
    CompatibilitySurfaceSpec,
    ContextMoldConfig,
    CustomerTwinBundle,
    TwinArchetype,
    TwinCrisisLevel,
    TwinDensityLevel,
    TwinGatewayConfig,
    TwinMatrixBundle,
)

TWIN_MANIFEST_FILE = "twin_manifest.json"
_MODEL_T = TypeVar("_MODEL_T", bound=BaseModel)


def __getattr__(name: str) -> Any:
    return load_api_export(name)


def build_customer_twin(
    root: str | Path,
    *,
    snapshot: ContextSnapshot | None = None,
    provider_configs: list[ContextProviderConfig] | None = None,
    organization_name: str | None = None,
    organization_domain: str = "",
    mold: ContextMoldConfig | None = None,
    mirror_config: GovernorWorkspaceConfig | None = None,
    gateway_token: str | None = None,
    overwrite: bool = True,
) -> CustomerTwinBundle:
    workspace_root = Path(root).expanduser().resolve()
    if snapshot is not None and provider_configs is not None:
        raise ValueError("provide either snapshot or provider_configs, not both")
    if snapshot is None and provider_configs is None:
        raise ValueError("snapshot or provider_configs is required")

    resolved_mold = mold or ContextMoldConfig()
    resolved_mirror = mirror_config or default_governor_workspace_config(
        hero_world=resolved_mold.archetype
    )
    resolved_snapshot = snapshot
    if resolved_snapshot is None:
        if not organization_name:
            raise ValueError(
                "organization_name is required when building from provider configs"
            )
        resolved_snapshot = capture_context(
            provider_configs or [],
            organization_name=organization_name,
            organization_domain=organization_domain,
        )

    resolved_name = organization_name or resolved_snapshot.organization_name
    resolved_domain = organization_domain or resolved_snapshot.organization_domain
    normalized_snapshot = resolved_snapshot.model_copy(
        update={
            "organization_name": resolved_name,
            "organization_domain": resolved_domain,
        }
    )
    twin_asset = build_customer_twin_asset(
        normalized_snapshot,
        mold=resolved_mold,
        organization_name=resolved_name,
        organization_domain=resolved_domain,
    )

    create_workspace_from_template(
        root=workspace_root,
        source_kind="vertical",
        source_ref=resolved_mold.archetype,
        title=resolved_name,
        description=f"Customer twin for {resolved_name}",
        overwrite=overwrite,
    )
    manifest = load_workspace(workspace_root)
    asset_path = workspace_root / manifest.blueprint_asset_path
    asset_path.write_text(twin_asset.model_dump_json(indent=2), encoding="utf-8")
    manifest.title = resolved_name
    manifest.description = f"Customer-shaped {resolved_mold.archetype.replace('_', ' ')} twin for {resolved_name}"
    manifest.metadata = {
        **dict(manifest.metadata),
        "customer_twin": {
            "organization_name": resolved_name,
            "organization_domain": resolved_domain,
            "mold": resolved_mold.model_dump(mode="json"),
        },
        "governor": governor_metadata_payload(resolved_mirror),
    }
    write_workspace(workspace_root, manifest)
    compile_workspace(workspace_root)

    if resolved_mold.scenario_variant:
        activate_workspace_scenario_variant(
            workspace_root,
            resolved_mold.scenario_variant,
            bootstrap_contract=True,
        )
    if resolved_mold.contract_variant:
        activate_workspace_contract_variant(
            workspace_root,
            resolved_mold.contract_variant,
        )

    snapshot_path = workspace_root / CONTEXT_SNAPSHOT_FILE
    snapshot_path.write_text(
        normalized_snapshot.model_dump_json(indent=2),
        encoding="utf-8",
    )
    write_canonical_history_sidecars(normalized_snapshot, snapshot_path)

    bundle = CustomerTwinBundle(
        workspace_root=workspace_root,
        workspace_name=load_workspace(workspace_root).name,
        organization_name=resolved_name,
        organization_domain=resolved_domain,
        mold=resolved_mold,
        context_snapshot_path=str(snapshot_path.relative_to(workspace_root)),
        blueprint_asset_path=str(asset_path.relative_to(workspace_root)),
        gateway=TwinGatewayConfig(
            auth_token=gateway_token or secrets.token_urlsafe(18),
            surfaces=_default_gateway_surfaces(
                resolved_mold.included_surfaces,
                facades=twin_asset.requested_facades,
            ),
            ui_command=(
                "python -m vei.cli.vei ui serve "
                f"--root {workspace_root} --host 127.0.0.1 --port 3011"
            ),
        ),
        summary=_bundle_summary(resolved_name, resolved_mold),
        metadata={
            "preview": preview_workspace_scenario(workspace_root),
            "source_providers": [item.provider for item in resolved_snapshot.sources],
            "normalized_identity": {
                "organization_name": resolved_name,
                "organization_domain": resolved_domain,
            },
            "governor": governor_metadata_payload(resolved_mirror),
        },
    )
    _write_json(workspace_root / TWIN_MANIFEST_FILE, bundle.model_dump(mode="json"))
    return bundle


def build_customer_twin_asset(
    snapshot: ContextSnapshot,
    *,
    mold: ContextMoldConfig | None = None,
    organization_name: str | None = None,
    organization_domain: str = "",
) -> BlueprintAsset:
    resolved_mold = mold or ContextMoldConfig()
    base_asset = build_vertical_blueprint_asset(resolved_mold.archetype).model_copy(
        deep=True
    )
    internal_domains = _internal_placeholder_domains(base_asset)
    captured_asset = hydrate_blueprint(
        snapshot,
        scenario_name=base_asset.scenario_name,
        workflow_name=base_asset.workflow_name or base_asset.scenario_name,
    )
    resolved_name = organization_name or snapshot.organization_name
    resolved_domain = organization_domain or snapshot.organization_domain

    base_asset.name = f"{_slug(resolved_name)}.customer_twin.blueprint"
    base_asset.title = resolved_name
    base_asset.description = f"Customer-shaped twin for {resolved_name}"
    base_asset.requested_facades = sorted(
        set(base_asset.requested_facades) | set(captured_asset.requested_facades)
    )
    base_asset.metadata = {
        **dict(base_asset.metadata),
        "customer_twin": {
            "organization_name": resolved_name,
            "organization_domain": resolved_domain,
            "mold": resolved_mold.model_dump(mode="json"),
            "source_providers": [item.provider for item in snapshot.sources],
        },
    }

    base_asset.environment = _merge_environment(
        base_asset.environment,
        captured_asset.environment,
        organization_name=resolved_name,
        organization_domain=resolved_domain,
    )
    base_asset.capability_graphs = _merge_capability_graphs(
        base_asset.capability_graphs,
        captured_asset.capability_graphs,
        organization_name=resolved_name,
        organization_domain=resolved_domain,
    )
    if _snapshot_uses_mail_archive(snapshot):
        _prefer_archive_capture(base_asset, captured_asset)
    _rewrite_placeholder_domains(
        base_asset,
        resolved_domain,
        internal_domains=internal_domains,
    )
    _apply_mold(
        base_asset,
        organization_name=resolved_name,
        organization_domain=resolved_domain,
        mold=resolved_mold,
    )
    return base_asset


def load_customer_twin(root: str | Path) -> CustomerTwinBundle:
    workspace_root = Path(root).expanduser().resolve()
    return _read_model(workspace_root / TWIN_MANIFEST_FILE, CustomerTwinBundle)


def create_twin_gateway_app(root: str | Path) -> FastAPI:
    from .gateway import create_twin_gateway_app as _create_twin_gateway_app

    return _create_twin_gateway_app(root)


def register_slack_gateway_routes(*args, **kwargs):
    from ._gateway_routes_slack import register_slack_gateway_routes as _register

    return _register(*args, **kwargs)


def register_jira_gateway_routes(*args, **kwargs):
    from ._gateway_routes_jira import register_jira_gateway_routes as _register

    return _register(*args, **kwargs)


def register_graph_gateway_routes(*args, **kwargs):
    from ._gateway_routes_graph import register_graph_gateway_routes as _register

    return _register(*args, **kwargs)


def register_salesforce_gateway_routes(*args, **kwargs):
    from ._gateway_routes_salesforce import (
        register_salesforce_gateway_routes as _register,
    )

    return _register(*args, **kwargs)


def register_notes_gateway_routes(*args, **kwargs):
    from ._gateway_routes_notes import register_notes_gateway_routes as _register

    return _register(*args, **kwargs)


def start_twin(*args, **kwargs):
    from .lifecycle import start_twin as _start_twin

    return _start_twin(*args, **kwargs)


def build_twin_status(*args, **kwargs):
    from .lifecycle import build_twin_status as _build_twin_status

    return _build_twin_status(*args, **kwargs)


def stop_twin(*args, **kwargs):
    from .lifecycle import stop_twin as _stop_twin

    return _stop_twin(*args, **kwargs)


def reset_twin(*args, **kwargs):
    from .lifecycle import reset_twin as _reset_twin

    return _reset_twin(*args, **kwargs)


def finalize_twin(*args, **kwargs):
    from .lifecycle import finalize_twin as _finalize_twin

    return _finalize_twin(*args, **kwargs)


def sync_twin(*args, **kwargs):
    from .lifecycle import sync_twin as _sync_twin

    return _sync_twin(*args, **kwargs)


def pause_twin_orchestrator_agent(*args, **kwargs):
    from .lifecycle import pause_twin_orchestrator_agent as _pause_twin_agent

    return _pause_twin_agent(*args, **kwargs)


def resume_twin_orchestrator_agent(*args, **kwargs):
    from .lifecycle import resume_twin_orchestrator_agent as _resume_twin_agent

    return _resume_twin_agent(*args, **kwargs)


def comment_on_twin_orchestrator_task(*args, **kwargs):
    from .lifecycle import (
        comment_on_twin_orchestrator_task as _comment_on_twin_task,
    )

    return _comment_on_twin_task(*args, **kwargs)


def approve_twin_orchestrator_approval(*args, **kwargs):
    from .lifecycle import (
        approve_twin_orchestrator_approval as _approve_twin_approval,
    )

    return _approve_twin_approval(*args, **kwargs)


def reject_twin_orchestrator_approval(*args, **kwargs):
    from .lifecycle import reject_twin_orchestrator_approval as _reject_twin_approval

    return _reject_twin_approval(*args, **kwargs)


def request_twin_orchestrator_revision(*args, **kwargs):
    from .lifecycle import request_twin_orchestrator_revision as _request_twin_revision

    return _request_twin_revision(*args, **kwargs)


def activate_twin_exercise(*args, **kwargs):
    from .lifecycle import activate_twin_exercise as _activate_twin_exercise

    return _activate_twin_exercise(*args, **kwargs)


def build_workspace_governor_status(*args, **kwargs):
    from .lifecycle import build_workspace_governor_status as _build_workspace_status

    return _build_workspace_status(*args, **kwargs)


def build_twin_matrix(
    output_root: str | Path,
    *,
    snapshot: ContextSnapshot | None = None,
    provider_configs: list[ContextProviderConfig] | None = None,
    organization_name: str | None = None,
    organization_domain: str = "",
    archetypes: Iterable[TwinArchetype] | None = None,
    density_levels: Iterable[TwinDensityLevel] | None = None,
    crisis_levels: Iterable[TwinCrisisLevel] | None = None,
    seeds: Iterable[int] | None = None,
    overwrite: bool = True,
):
    from .matrix import build_twin_matrix as _build_twin_matrix

    return _build_twin_matrix(
        output_root,
        snapshot=snapshot,
        provider_configs=provider_configs,
        organization_name=organization_name,
        organization_domain=organization_domain,
        archetypes=list(archetypes) if archetypes is not None else None,
        density_levels=list(density_levels) if density_levels is not None else None,
        crisis_levels=list(crisis_levels) if crisis_levels is not None else None,
        seeds=list(seeds) if seeds is not None else None,
        overwrite=overwrite,
    )


def load_twin_matrix(output_root: str | Path) -> TwinMatrixBundle:
    from .matrix import load_twin_matrix as _load_twin_matrix

    return _load_twin_matrix(output_root)


def _merge_environment(
    base: Any,
    captured: Any,
    *,
    organization_name: str,
    organization_domain: str,
):
    if base is None and captured is None:
        return None
    if base is None:
        env = captured.model_copy(deep=True)
    else:
        env = base.model_copy(deep=True)
    if captured is None:
        captured = env.__class__(
            organization_name=organization_name,
            organization_domain=organization_domain,
        )

    env.organization_name = organization_name
    env.organization_domain = organization_domain or env.organization_domain
    if captured.scenario_brief:
        env.scenario_brief = captured.scenario_brief
    env.slack_channels = _merge_models(
        env.slack_channels,
        captured.slack_channels,
        key=lambda item: item.channel,
    )
    env.mail_threads = _merge_models(
        env.mail_threads,
        captured.mail_threads,
        key=lambda item: item.thread_id,
    )
    env.documents = _merge_models(
        env.documents,
        captured.documents,
        key=lambda item: item.doc_id,
    )
    env.tickets = _merge_models(
        env.tickets,
        captured.tickets,
        key=lambda item: item.ticket_id,
    )
    env.identity_users = _merge_models(
        env.identity_users,
        captured.identity_users,
        key=lambda item: item.email or item.user_id,
    )
    env.identity_groups = _merge_models(
        env.identity_groups,
        captured.identity_groups,
        key=lambda item: item.group_id,
    )
    env.identity_applications = _merge_models(
        env.identity_applications,
        captured.identity_applications,
        key=lambda item: item.app_id,
    )
    env.crm_companies = _merge_models(
        env.crm_companies,
        captured.crm_companies,
        key=lambda item: item.id,
    )
    env.crm_contacts = _merge_models(
        env.crm_contacts,
        captured.crm_contacts,
        key=lambda item: item.email or item.id,
    )
    env.crm_deals = _merge_models(
        env.crm_deals,
        captured.crm_deals,
        key=lambda item: item.id,
    )
    env.metadata = {
        **dict(env.metadata),
        "customer_twin_capture": True,
        "source_providers": list(
            {
                *list(env.metadata.get("source_providers", [])),
                *list(captured.metadata.get("source_providers", [])),
            }
        ),
    }
    return env


def _environment_from_graphs(
    graphs: BlueprintCapabilityGraphsAsset,
) -> BlueprintEnvironmentAsset:
    comm_graph = graphs.comm_graph
    doc_graph = graphs.doc_graph
    work_graph = graphs.work_graph
    identity_graph = graphs.identity_graph
    revenue_graph = graphs.revenue_graph
    return BlueprintEnvironmentAsset(
        organization_name=graphs.organization_name,
        organization_domain=graphs.organization_domain,
        timezone=graphs.timezone,
        scenario_brief=graphs.scenario_brief,
        slack_initial_message=(
            comm_graph.slack_initial_message if comm_graph is not None else None
        ),
        slack_channels=(
            list(comm_graph.slack_channels) if comm_graph is not None else []
        ),
        mail_threads=(list(comm_graph.mail_threads) if comm_graph is not None else []),
        documents=list(doc_graph.documents) if doc_graph is not None else [],
        tickets=list(work_graph.tickets) if work_graph is not None else [],
        identity_users=list(identity_graph.users) if identity_graph is not None else [],
        identity_groups=(
            list(identity_graph.groups) if identity_graph is not None else []
        ),
        identity_applications=(
            list(identity_graph.applications) if identity_graph is not None else []
        ),
        service_requests=(
            list(work_graph.service_requests) if work_graph is not None else []
        ),
        google_drive_shares=(
            list(doc_graph.drive_shares) if doc_graph is not None else []
        ),
        hris_employees=(
            list(identity_graph.hris_employees) if identity_graph is not None else []
        ),
        crm_companies=(
            list(revenue_graph.companies) if revenue_graph is not None else []
        ),
        crm_contacts=(
            list(revenue_graph.contacts) if revenue_graph is not None else []
        ),
        crm_deals=list(revenue_graph.deals) if revenue_graph is not None else [],
        metadata=dict(graphs.metadata),
    )


def _merge_capability_graphs(
    base: BlueprintCapabilityGraphsAsset | None,
    captured: BlueprintCapabilityGraphsAsset | None,
    *,
    organization_name: str,
    organization_domain: str,
) -> BlueprintCapabilityGraphsAsset | None:
    if base is None and captured is None:
        return None
    if base is None:
        graphs = captured.model_copy(deep=True) if captured is not None else None
    else:
        graphs = base.model_copy(deep=True)
    if graphs is None:
        return None

    graphs.organization_name = organization_name
    graphs.organization_domain = organization_domain or graphs.organization_domain
    if captured is None:
        return graphs

    graphs.comm_graph = _merge_comm_graph(graphs.comm_graph, captured.comm_graph)
    graphs.doc_graph = _merge_doc_graph(graphs.doc_graph, captured.doc_graph)
    graphs.work_graph = _merge_work_graph(graphs.work_graph, captured.work_graph)
    graphs.identity_graph = _merge_identity_graph(
        graphs.identity_graph,
        captured.identity_graph,
    )
    graphs.revenue_graph = _merge_revenue_graph(
        graphs.revenue_graph,
        captured.revenue_graph,
    )
    graphs.metadata = {
        **dict(graphs.metadata),
        "customer_twin_capture": True,
        "source_providers": list(
            {
                *list(graphs.metadata.get("source_providers", [])),
                *list(captured.metadata.get("providers", [])),
            }
        ),
    }
    return graphs


def _merge_comm_graph(
    base: BlueprintCommGraphAsset | None,
    captured: BlueprintCommGraphAsset | None,
) -> BlueprintCommGraphAsset | None:
    if base is None:
        return captured.model_copy(deep=True) if captured is not None else None
    if captured is None:
        return base
    merged = base.model_copy(deep=True)
    merged.slack_channels = _merge_models(
        merged.slack_channels,
        captured.slack_channels,
        key=lambda item: item.channel,
    )
    merged.mail_threads = _merge_models(
        merged.mail_threads,
        captured.mail_threads,
        key=lambda item: item.thread_id,
    )
    return merged


def _merge_doc_graph(
    base: BlueprintDocGraphAsset | None,
    captured: BlueprintDocGraphAsset | None,
) -> BlueprintDocGraphAsset | None:
    if base is None:
        return captured.model_copy(deep=True) if captured is not None else None
    if captured is None:
        return base
    merged = base.model_copy(deep=True)
    merged.documents = _merge_models(
        merged.documents,
        captured.documents,
        key=lambda item: item.doc_id,
    )
    return merged


def _merge_work_graph(
    base: BlueprintWorkGraphAsset | None,
    captured: BlueprintWorkGraphAsset | None,
) -> BlueprintWorkGraphAsset | None:
    if base is None:
        return captured.model_copy(deep=True) if captured is not None else None
    if captured is None:
        return base
    merged = base.model_copy(deep=True)
    merged.tickets = _merge_models(
        merged.tickets,
        captured.tickets,
        key=lambda item: item.ticket_id,
    )
    return merged


def _merge_identity_graph(
    base: BlueprintIdentityGraphAsset | None,
    captured: BlueprintIdentityGraphAsset | None,
) -> BlueprintIdentityGraphAsset | None:
    if base is None:
        return captured.model_copy(deep=True) if captured is not None else None
    if captured is None:
        return base
    merged = base.model_copy(deep=True)
    merged.users = _merge_models(
        merged.users,
        captured.users,
        key=lambda item: item.email or item.user_id,
    )
    merged.groups = _merge_models(
        merged.groups,
        captured.groups,
        key=lambda item: item.group_id,
    )
    merged.applications = _merge_models(
        merged.applications,
        captured.applications,
        key=lambda item: item.app_id,
    )
    return merged


def _merge_revenue_graph(
    base: Any,
    captured: Any,
):
    if base is None:
        return captured.model_copy(deep=True) if captured is not None else None
    if captured is None:
        return base
    merged = base.model_copy(deep=True)
    merged.companies = _merge_models(
        merged.companies,
        captured.companies,
        key=lambda item: item.id,
    )
    merged.contacts = _merge_models(
        merged.contacts,
        captured.contacts,
        key=lambda item: item.email or item.id,
    )
    merged.deals = _merge_models(
        merged.deals,
        captured.deals,
        key=lambda item: item.id,
    )
    return merged


def _merge_models(
    base: Iterable[_MODEL_T],
    captured: Iterable[_MODEL_T],
    *,
    key,
) -> list[_MODEL_T]:
    merged: dict[str, _MODEL_T] = {}
    for item in base:
        merged[str(key(item))] = item
    for item in captured:
        merged[str(key(item))] = item
    return list(merged.values())


def _snapshot_uses_mail_archive(snapshot: ContextSnapshot) -> bool:
    return any(source.provider == "mail_archive" for source in snapshot.sources)


def _prefer_archive_capture(
    asset: BlueprintAsset,
    captured_asset: BlueprintAsset,
) -> None:
    if asset.capability_graphs is None or captured_asset.capability_graphs is None:
        return
    captured_graphs = captured_asset.capability_graphs
    target_graphs = asset.capability_graphs

    if captured_graphs.comm_graph is not None:
        if target_graphs.comm_graph is None:
            target_graphs.comm_graph = captured_graphs.comm_graph.model_copy(deep=True)
        else:
            target_graphs.comm_graph.mail_threads = list(
                captured_graphs.comm_graph.mail_threads
            )
            target_graphs.comm_graph.metadata = {
                **dict(target_graphs.comm_graph.metadata),
                **dict(captured_graphs.comm_graph.metadata),
            }
    if captured_graphs.identity_graph is not None:
        if target_graphs.identity_graph is None:
            target_graphs.identity_graph = captured_graphs.identity_graph.model_copy(
                deep=True
            )
        else:
            target_graphs.identity_graph.users = list(
                captured_graphs.identity_graph.users
            )
            target_graphs.identity_graph.groups = list(
                captured_graphs.identity_graph.groups
            )
            target_graphs.identity_graph.applications = list(
                captured_graphs.identity_graph.applications
            )
            target_graphs.identity_graph.hris_employees = list(
                captured_graphs.identity_graph.hris_employees
            )


def _rewrite_placeholder_domains(
    asset: BlueprintAsset,
    organization_domain: str,
    *,
    internal_domains: set[str],
) -> None:
    if not organization_domain:
        return
    environment = asset.environment
    if environment is not None:
        for thread in environment.mail_threads:
            for message in thread.messages:
                message.from_address = _rewrite_email(
                    message.from_address,
                    organization_domain,
                    internal_domains=internal_domains,
                )
                message.to_address = _rewrite_email(
                    message.to_address,
                    organization_domain,
                    internal_domains=internal_domains,
                )
        for user in environment.identity_users:
            user.email = _rewrite_email(
                user.email,
                organization_domain,
                internal_domains=internal_domains,
            )
            if user.login:
                user.login = _rewrite_email(
                    user.login,
                    organization_domain,
                    internal_domains=internal_domains,
                )
        for contact in environment.crm_contacts:
            contact.email = _rewrite_email(
                contact.email,
                organization_domain,
                internal_domains=internal_domains,
            )
    graphs = asset.capability_graphs
    if graphs is None:
        return
    if graphs.identity_graph is not None:
        for user in graphs.identity_graph.users:
            user.email = _rewrite_email(
                user.email,
                organization_domain,
                internal_domains=internal_domains,
            )
            if user.login:
                user.login = _rewrite_email(
                    user.login,
                    organization_domain,
                    internal_domains=internal_domains,
                )


def _apply_mold(
    asset: BlueprintAsset,
    *,
    organization_name: str,
    organization_domain: str,
    mold: ContextMoldConfig,
) -> None:
    metadata = {
        "density_level": mold.density_level,
        "named_team_expansion": mold.named_team_expansion,
        "crisis_family": mold.crisis_family,
        "redaction_mode": mold.redaction_mode,
        "synthetic_expansion_strength": mold.synthetic_expansion_strength,
        "included_surfaces": list(mold.included_surfaces),
    }
    asset.metadata = {**dict(asset.metadata), "customer_twin_mold": metadata}
    if asset.capability_graphs is not None:
        asset.capability_graphs.metadata = {
            **dict(asset.capability_graphs.metadata),
            "customer_twin_mold": metadata,
        }
    _apply_surface_filter(asset, mold.included_surfaces)
    if mold.redaction_mode == "mask":
        _mask_external_contacts(asset, organization_domain)
    _apply_density(asset, mold.density_level)
    _apply_named_team_expansion(asset, organization_name, organization_domain, mold)
    _apply_synthetic_expansion(asset, organization_name, organization_domain, mold)


def _apply_surface_filter(
    asset: BlueprintAsset,
    included_surfaces: Sequence[str],
) -> None:
    if not included_surfaces:
        return
    if asset.environment is None and asset.capability_graphs is not None:
        asset.environment = _environment_from_graphs(asset.capability_graphs)
    keep = {item.strip().lower() for item in included_surfaces}
    if asset.environment is not None:
        if "slack" not in keep:
            asset.environment.slack_initial_message = None
            asset.environment.slack_channels = []
            asset.environment.metadata = {
                **dict(asset.environment.metadata),
                "strict_empty_slack": True,
            }
        if "mail" not in keep:
            asset.environment.mail_threads = []
            asset.environment.metadata = {
                **dict(asset.environment.metadata),
                "mail_archive_view": False,
            }
        elif asset.environment.mail_threads and _uses_mail_archive(asset):
            mailbox = _primary_mailbox(asset.environment.mail_threads)
            asset.environment.metadata = {
                **dict(asset.environment.metadata),
                "mail_archive_view": True,
                "mail_archive_mailbox": mailbox,
            }
        else:
            asset.environment.metadata = {
                **dict(asset.environment.metadata),
                "mail_archive_view": False,
            }
        if "docs" not in keep:
            asset.environment.documents = []
            asset.environment.google_drive_shares = []
        if "tickets" not in keep and "approvals" not in keep:
            asset.environment.tickets = []
            asset.environment.service_requests = []
        if "identity" not in keep:
            asset.environment.identity_users = []
            asset.environment.identity_groups = []
            asset.environment.identity_applications = []
            asset.environment.hris_employees = []
        if "crm" not in keep:
            asset.environment.crm_companies = []
            asset.environment.crm_contacts = []
            asset.environment.crm_deals = []
    graphs = asset.capability_graphs
    if graphs is not None:
        if "slack" not in keep and graphs.comm_graph is not None:
            graphs.comm_graph.slack_channels = []
        if "mail" not in keep and graphs.comm_graph is not None:
            graphs.comm_graph.mail_threads = []
        if "docs" not in keep:
            graphs.doc_graph = None
        if "tickets" not in keep and "approvals" not in keep:
            graphs.work_graph = None
        if "identity" not in keep:
            graphs.identity_graph = None
        if "crm" not in keep:
            graphs.revenue_graph = None
        if "vertical" not in keep:
            graphs.property_graph = None
            graphs.campaign_graph = None
            graphs.inventory_graph = None
    asset.requested_facades = _filter_requested_facades(
        asset.requested_facades,
        keep,
    )


def _mask_external_contacts(asset: BlueprintAsset, organization_domain: str) -> None:
    if not organization_domain:
        return
    graphs = asset.capability_graphs
    if graphs is not None and graphs.revenue_graph is not None:
        for contact in graphs.revenue_graph.contacts:
            contact.email = _mask_external_email(contact.email, organization_domain)
    environment = asset.environment
    if environment is None:
        return
    for contact in environment.crm_contacts:
        contact.email = _mask_external_email(contact.email, organization_domain)
    for thread in environment.mail_threads:
        for message in thread.messages:
            message.from_address = _mask_external_email(
                message.from_address, organization_domain
            )
            message.to_address = _mask_external_email(
                message.to_address, organization_domain
            )


def _mask_external_email(value: str, organization_domain: str) -> str:
    if "@" not in value:
        return value
    local, domain = value.split("@", 1)
    if domain.strip().lower() == organization_domain.strip().lower():
        return value
    return f"external-{_slug(local)[:12]}@masked.example.com"


def _apply_density(asset: BlueprintAsset, density_level: str) -> None:
    if density_level == "medium":
        return
    graphs = asset.capability_graphs
    if graphs is None:
        return
    if density_level == "small":
        if graphs.comm_graph is not None:
            graphs.comm_graph.slack_channels = graphs.comm_graph.slack_channels[:3]
            for channel in graphs.comm_graph.slack_channels:
                channel.messages = channel.messages[:3]
            graphs.comm_graph.mail_threads = graphs.comm_graph.mail_threads[:2]
            for thread in graphs.comm_graph.mail_threads:
                thread.messages = thread.messages[:1]
        if graphs.doc_graph is not None:
            graphs.doc_graph.documents = graphs.doc_graph.documents[:3]
        if graphs.work_graph is not None:
            graphs.work_graph.tickets = graphs.work_graph.tickets[:4]
        if graphs.identity_graph is not None:
            graphs.identity_graph.users = graphs.identity_graph.users[:5]
        if graphs.revenue_graph is not None:
            graphs.revenue_graph.contacts = graphs.revenue_graph.contacts[:4]
            graphs.revenue_graph.deals = graphs.revenue_graph.deals[:3]


def _apply_named_team_expansion(
    asset: BlueprintAsset,
    organization_name: str,
    organization_domain: str,
    mold: ContextMoldConfig,
) -> None:
    if mold.named_team_expansion == "minimal":
        return
    graphs = asset.capability_graphs
    if graphs is None:
        return
    if graphs.identity_graph is None:
        return
    desired = 8 if mold.named_team_expansion == "standard" else 12
    existing = {user.email.lower() for user in graphs.identity_graph.users}
    roles = _role_names_for_archetype(mold.archetype)
    next_index = 1
    while len(graphs.identity_graph.users) < desired and roles:
        role = roles.pop(0)
        local = f"{_slug(role['first_name'])}.{_slug(role['last_name'])}"
        email = f"{local}@{organization_domain or 'example.com'}"
        if email.lower() in existing:
            continue
        graphs.identity_graph.users.append(
            BlueprintIdentityUserAsset(
                user_id=f"USR-TWIN-{next_index:03d}",
                email=email,
                first_name=role["first_name"],
                last_name=role["last_name"],
                display_name=f"{role['first_name']} {role['last_name']}",
                login=email,
                department=role["department"],
                title=role["title"],
                manager=role.get("manager"),
                groups=[],
                applications=[],
                factors=["password"],
            )
        )
        existing.add(email.lower())
        next_index += 1
    if asset.environment is not None:
        asset.environment.organization_name = organization_name
        asset.environment.organization_domain = (
            organization_domain or asset.environment.organization_domain
        )


def _apply_synthetic_expansion(
    asset: BlueprintAsset,
    organization_name: str,
    organization_domain: str,
    mold: ContextMoldConfig,
) -> None:
    strength = mold.synthetic_expansion_strength
    if mold.density_level != "large" and strength == "light":
        return
    graphs = asset.capability_graphs
    if graphs is None:
        return
    extra_count = 1 if strength == "light" else 2 if strength == "medium" else 3
    if graphs.comm_graph is not None and graphs.comm_graph.slack_channels:
        for channel in graphs.comm_graph.slack_channels[:extra_count]:
            last_message = channel.messages[-1] if channel.messages else None
            channel.messages.append(
                BlueprintSlackMessageAsset(
                    ts=f"{last_message.ts if last_message else '1711111111.000000'}-x{extra_count}",
                    user=last_message.user if last_message else "ops.coordinator",
                    text=(
                        "Keeping the thread current with one more operator note so the "
                        "world stays visibly dense."
                    ),
                    thread_ts=(last_message.thread_ts if last_message else None),
                )
            )
    if graphs.comm_graph is not None and graphs.comm_graph.mail_threads:
        for thread in graphs.comm_graph.mail_threads[:extra_count]:
            last_message = thread.messages[-1] if thread.messages else None
            thread.messages.append(
                BlueprintMailMessageAsset(
                    from_address=(
                        last_message.to_address
                        if last_message is not None
                        else f"ops@{organization_domain or 'example.com'}"
                    ),
                    to_address=(
                        last_message.from_address
                        if last_message is not None
                        else f"team@{organization_domain or 'example.com'}"
                    ),
                    subject=thread.title or "Follow-up",
                    body_text=(
                        f"Additional context for {organization_name}: keep the latest "
                        "decision, owner, and next check-in visible to the team."
                    ),
                    unread=False,
                    time_ms=(
                        last_message.time_ms if last_message is not None else None
                    ),
                )
            )
    if graphs.doc_graph is not None and len(graphs.doc_graph.documents) < 6:
        doc_count = len(graphs.doc_graph.documents)
        for index in range(extra_count):
            graphs.doc_graph.documents.append(
                BlueprintDocumentAsset(
                    doc_id=f"DOC-TWIN-{doc_count + index + 1}",
                    title=f"{organization_name} operator brief {doc_count + index + 1}",
                    body=(
                        "Summary of the latest customer-safe path, current blockers, "
                        "owner handoff, and next decision checkpoint."
                    ),
                    tags=["brief", "synthetic"],
                )
            )
    if graphs.work_graph is not None and len(graphs.work_graph.tickets) < 8:
        ticket_count = len(graphs.work_graph.tickets)
        for index in range(extra_count):
            graphs.work_graph.tickets.append(
                BlueprintTicketAsset(
                    ticket_id=f"TWIN-{ticket_count + index + 1}",
                    title=f"Follow-up action {ticket_count + index + 1}",
                    status="open",
                    assignee="ops.coordinator",
                    description="Synthetic follow-up item to keep the queue realistic.",
                )
            )


def _role_names_for_archetype(archetype: str) -> list[dict[str, str]]:
    shared = [
        {
            "first_name": "Morgan",
            "last_name": "Vale",
            "department": "Operations",
            "title": "Operations Manager",
        },
        {
            "first_name": "Iris",
            "last_name": "Chen",
            "department": "Support",
            "title": "Support Lead",
        },
        {
            "first_name": "Devin",
            "last_name": "Shaw",
            "department": "Revenue",
            "title": "Revenue Lead",
        },
        {
            "first_name": "Lena",
            "last_name": "Morris",
            "department": "Product",
            "title": "Product Manager",
        },
        {
            "first_name": "Omar",
            "last_name": "Price",
            "department": "Success",
            "title": "Customer Success Lead",
        },
    ]
    if archetype == "real_estate_management":
        return shared + [
            {
                "first_name": "Rhea",
                "last_name": "Dalton",
                "department": "Facilities",
                "title": "Facilities Supervisor",
            },
            {
                "first_name": "Evan",
                "last_name": "Hart",
                "department": "Leasing",
                "title": "Leasing Manager",
            },
        ]
    if archetype == "digital_marketing_agency":
        return shared + [
            {
                "first_name": "Paige",
                "last_name": "Ng",
                "department": "Creative",
                "title": "Creative Director",
            },
            {
                "first_name": "Nikhil",
                "last_name": "Rao",
                "department": "Media",
                "title": "Media Strategist",
            },
        ]
    if archetype == "storage_solutions":
        return shared + [
            {
                "first_name": "Marta",
                "last_name": "Lopez",
                "department": "Dispatch",
                "title": "Dispatch Lead",
            },
            {
                "first_name": "Gabe",
                "last_name": "Owens",
                "department": "Capacity",
                "title": "Capacity Planner",
            },
        ]
    return shared + [
        {
            "first_name": "Noah",
            "last_name": "West",
            "department": "Engineering",
            "title": "Engineering Manager",
        },
        {
            "first_name": "Ava",
            "last_name": "Klein",
            "department": "Sales",
            "title": "Account Executive",
        },
    ]


def _rewrite_email(
    value: str,
    domain: str,
    *,
    internal_domains: set[str],
) -> str:
    if "@" not in value:
        return value
    local, current_domain = value.split("@", 1)
    if not _should_rewrite_domain(current_domain, internal_domains):
        return value
    return f"{local}@{domain}"


def _looks_placeholder_domain(value: str) -> bool:
    return value.strip().lower().endswith((".example", ".example.com"))


def _should_rewrite_domain(value: str, internal_domains: set[str]) -> bool:
    lowered = value.strip().lower()
    return _looks_placeholder_domain(lowered) and lowered in internal_domains


def _internal_placeholder_domains(asset: BlueprintAsset) -> set[str]:
    domains = {"example", "example.com"}
    environment = asset.environment
    if environment and environment.organization_domain:
        domains.add(environment.organization_domain.strip().lower())
    graphs = asset.capability_graphs
    if graphs and graphs.organization_domain:
        domains.add(graphs.organization_domain.strip().lower())
    return {value for value in domains if value}


def _default_gateway_surfaces(
    included_surfaces: Sequence[str] | None = None,
    *,
    facades: Sequence[str] | None = None,
) -> list[CompatibilitySurfaceSpec]:
    bindings = resolve_gateway_surface_bindings(
        included_surfaces=included_surfaces or (),
        facade_names=facades,
        default_only=facades is None,
    )
    return [
        CompatibilitySurfaceSpec(
            name=binding.name,
            title=binding.title,
            base_path=binding.base_path,
            auth_style=binding.auth_style,  # type: ignore[arg-type]
        )
        for binding in bindings
    ]


def _filter_requested_facades(
    facades: Sequence[str],
    included_surfaces: set[str],
) -> list[str]:
    if not facades:
        return []
    if "vertical" in included_surfaces:
        return list(facades)
    allowed: list[str] = []
    for facade_name in facades:
        try:
            plugin = get_facade_plugin(facade_name)
        except KeyError:
            continue
        if any(
            plugin.matches_included_surface(surface) for surface in included_surfaces
        ):
            allowed.append(plugin.manifest.name)
    return allowed


def _primary_mailbox(threads: Sequence[BlueprintMailThreadAsset]) -> str:
    counts: dict[str, int] = {}
    for thread in threads:
        for message in thread.messages:
            recipient = message.to_address.strip().lower()
            if not recipient:
                continue
            counts[recipient] = counts.get(recipient, 0) + 1
    if not counts:
        return "me@example"
    return sorted(counts.items(), key=lambda item: (-item[1], item[0]))[0][0]


def _uses_mail_archive(asset: BlueprintAsset) -> bool:
    metadata = dict(asset.metadata or {})
    customer_twin = metadata.get("customer_twin", {})
    if not isinstance(customer_twin, dict):
        return False
    providers = customer_twin.get("source_providers", [])
    if not isinstance(providers, list):
        return False
    return "mail_archive" in providers


def _bundle_summary(name: str, mold: ContextMoldConfig) -> str:
    surfaces = _default_gateway_surfaces(mold.included_surfaces)
    if not surfaces:
        return (
            f"{name} is now packaged as a customer-shaped twin with a "
            f"{mold.archetype.replace('_', ' ')} operating model."
        )
    labels = ", ".join(item.title for item in surfaces)
    return (
        f"{name} is now packaged as a customer-shaped twin with a "
        f"{mold.archetype.replace('_', ' ')} operating model and compatibility "
        f"routes for {labels}."
    )


def _slug(value: str) -> str:
    return "_".join(part for part in value.strip().lower().replace("-", " ").split())


def _read_model(path: Path, model_cls: type[_MODEL_T]) -> _MODEL_T:
    return model_cls.model_validate_json(path.read_text(encoding="utf-8"))


def _write_json(path: Path, payload: Any) -> None:
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
