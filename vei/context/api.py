from __future__ import annotations

from typing import List

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


def diff_snapshots(
    before: ContextSnapshot,
    after: ContextSnapshot,
) -> ContextDiff:
    from .diff import compute_diff

    return compute_diff(before, after)
