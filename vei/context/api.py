from __future__ import annotations

from pathlib import Path
from typing import List, Optional, Union

from vei.blueprint.models import BlueprintAsset

from .models import (
    ContextDiff,
    ContextProviderConfig,
    ContextSnapshot,
    ContextSourceResult,
)
from .providers import get_provider
from .providers.base import iso_now


def capture_context(
    providers: List[ContextProviderConfig],
    *,
    organization_name: str,
    organization_domain: str = "",
) -> ContextSnapshot:
    sources: list[ContextSourceResult] = []
    for config in providers:
        provider = get_provider(config.provider)
        try:
            result = provider.capture(config)
        except Exception as exc:
            result = ContextSourceResult(
                provider=config.provider,
                captured_at=iso_now(),
                status="error",
                error=str(exc),
            )
        sources.append(result)

    return ContextSnapshot(
        organization_name=organization_name,
        organization_domain=organization_domain,
        captured_at=iso_now(),
        sources=sources,
    )


def hydrate_blueprint(
    snapshot: ContextSnapshot,
    *,
    scenario_name: str = "captured_context",
    workflow_name: str = "captured_context",
) -> BlueprintAsset:
    from .hydrate import hydrate_snapshot_to_blueprint

    return hydrate_snapshot_to_blueprint(
        snapshot,
        scenario_name=scenario_name,
        workflow_name=workflow_name,
    )


def ingest_slack_export(
    export_path: Union[str, Path],
    *,
    organization_name: str,
    organization_domain: str = "",
    channel_filter: Optional[List[str]] = None,
    message_limit: int = 200,
) -> ContextSnapshot:
    """Ingest a Slack workspace export directory into a ContextSnapshot."""
    from .providers.slack import capture_from_export

    result = capture_from_export(
        export_path,
        channel_filter=channel_filter,
        message_limit=message_limit,
    )

    return ContextSnapshot(
        organization_name=organization_name,
        organization_domain=organization_domain,
        captured_at=iso_now(),
        sources=[result],
    )


def diff_snapshots(
    before: ContextSnapshot,
    after: ContextSnapshot,
) -> ContextDiff:
    from .diff import compute_diff

    return compute_diff(before, after)
