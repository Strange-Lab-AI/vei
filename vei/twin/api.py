from __future__ import annotations

import json
import secrets
from pathlib import Path
from typing import Any, Iterable, TypeVar

from pydantic import BaseModel

from vei.blueprint.models import (
    BlueprintAsset,
    BlueprintCapabilityGraphsAsset,
    BlueprintCommGraphAsset,
    BlueprintDocGraphAsset,
    BlueprintIdentityGraphAsset,
    BlueprintWorkGraphAsset,
)
from vei.context.api import capture_context, hydrate_blueprint
from vei.context.models import ContextProviderConfig, ContextSnapshot
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

from .models import (
    CompatibilitySurfaceSpec,
    ContextMoldConfig,
    CustomerTwinBundle,
    TwinGatewayConfig,
)


TWIN_MANIFEST_FILE = "twin_manifest.json"
_MODEL_T = TypeVar("_MODEL_T", bound=BaseModel)


def build_customer_twin(
    root: str | Path,
    *,
    snapshot: ContextSnapshot | None = None,
    provider_configs: list[ContextProviderConfig] | None = None,
    organization_name: str | None = None,
    organization_domain: str = "",
    mold: ContextMoldConfig | None = None,
    gateway_token: str | None = None,
    overwrite: bool = True,
) -> CustomerTwinBundle:
    workspace_root = Path(root).expanduser().resolve()
    if snapshot is not None and provider_configs is not None:
        raise ValueError("provide either snapshot or provider_configs, not both")
    if snapshot is None and provider_configs is None:
        raise ValueError("snapshot or provider_configs is required")

    resolved_mold = mold or ContextMoldConfig()
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
    twin_asset = build_customer_twin_asset(
        resolved_snapshot,
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

    snapshot_path = workspace_root / "context_snapshot.json"
    snapshot_path.write_text(
        resolved_snapshot.model_dump_json(indent=2),
        encoding="utf-8",
    )

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
            surfaces=_default_gateway_surfaces(),
            ui_command=(
                "python -m vei.cli.vei ui serve "
                f"--root {workspace_root} --host 127.0.0.1 --port 3011"
            ),
        ),
        summary=(
            f"{resolved_name} is now packaged as a customer-shaped twin with a "
            f"{resolved_mold.archetype.replace('_', ' ')} operating model and "
            "compatibility routes for Slack, Jira, Outlook-style mail/calendar, "
            "and Salesforce-style CRM."
        ),
        metadata={
            "preview": preview_workspace_scenario(workspace_root),
            "source_providers": [item.provider for item in resolved_snapshot.sources],
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
    _rewrite_placeholder_domains(
        base_asset,
        resolved_domain,
        internal_domains=internal_domains,
    )
    return base_asset


def load_customer_twin(root: str | Path) -> CustomerTwinBundle:
    workspace_root = Path(root).expanduser().resolve()
    return _read_model(workspace_root / TWIN_MANIFEST_FILE, CustomerTwinBundle)


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
    lowered = value.strip().lower()
    return lowered.endswith(".example") or lowered.endswith(".example.com")


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


def _default_gateway_surfaces() -> list[CompatibilitySurfaceSpec]:
    return [
        CompatibilitySurfaceSpec(
            name="slack",
            title="Slack",
            base_path="/slack/api",
        ),
        CompatibilitySurfaceSpec(
            name="jira",
            title="Jira",
            base_path="/jira/rest/api/3",
        ),
        CompatibilitySurfaceSpec(
            name="graph",
            title="Microsoft Graph",
            base_path="/graph/v1.0",
        ),
        CompatibilitySurfaceSpec(
            name="salesforce",
            title="Salesforce",
            base_path="/salesforce/services/data/v60.0",
        ),
    ]


def _slug(value: str) -> str:
    return "_".join(part for part in value.strip().lower().replace("-", " ").split())


def _read_model(path: Path, model_cls: type[_MODEL_T]) -> _MODEL_T:
    return model_cls.model_validate_json(path.read_text(encoding="utf-8"))


def _write_json(path: Path, payload: Any) -> None:
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
