from __future__ import annotations

import logging
from pathlib import Path
from typing import Any, List, Optional, Union

from vei.blueprint.api import BlueprintAsset

from ._legacy_archive_snapshot import legacy_threads_payload_to_snapshot
from . import canonical_history as _canonical_history
from .models import (
    ContextDiff,
    ContextProviderStatusSummary,
    ContextSnapshotRole,
    ContextSnapshotStatusSummary,
    ContextStatusFinding,
    snapshot_role,
    with_snapshot_role,
    ContextProviderConfig,
    ContextSnapshot,
    ContextSourceResult,
    CrmSourceData,
    GmailSourceData,
    GoogleSourceData,
    JiraSourceData,
    MailArchiveSourceData,
    SlackSourceData,
    source_payload,
)
from . import public_context as _public_context
from .providers import get_provider
from .providers.base import iso_now

CanonicalHistoryBundle = _canonical_history.CanonicalHistoryBundle
CanonicalHistoryIndex = _canonical_history.CanonicalHistoryIndex
CanonicalHistoryIndexRow = _canonical_history.CanonicalHistoryIndexRow
CanonicalHistoryPaths = _canonical_history.CanonicalHistoryPaths
CanonicalHistoryReadinessReport = _canonical_history.CanonicalHistoryReadinessReport
CanonicalHistoryTimelineResult = _canonical_history.CanonicalHistoryTimelineResult
build_canonical_history_readiness = _canonical_history.build_canonical_history_readiness
build_canonical_history_bundle = _canonical_history.build_canonical_history_bundle
build_canonical_history_bundle_from_rows = (
    _canonical_history.build_canonical_history_bundle_from_rows
)
canonical_history_paths = _canonical_history.canonical_history_paths
canonical_history_sidecars_exist = _canonical_history.canonical_history_sidecars_exist
load_canonical_history_bundle = _canonical_history.load_canonical_history_bundle
query_canonical_history = _canonical_history.query_canonical_history
write_canonical_history_bundle = _canonical_history.write_canonical_history_bundle
write_canonical_history_sidecars = _canonical_history.write_canonical_history_sidecars

_BOUNDARY_EXPORTS = (
    CanonicalHistoryBundle,
    CanonicalHistoryIndex,
    CanonicalHistoryIndexRow,
    CanonicalHistoryPaths,
    CanonicalHistoryReadinessReport,
    CanonicalHistoryTimelineResult,
    ContextProviderConfig,
    ContextSnapshot,
    ContextSourceResult,
    CrmSourceData,
    GmailSourceData,
    GoogleSourceData,
    JiraSourceData,
    MailArchiveSourceData,
    SlackSourceData,
    source_payload,
)

logger = logging.getLogger(__name__)

WhatIfPublicContext = _public_context.WhatIfPublicContext
WhatIfPublicCreditEvent = _public_context.WhatIfPublicCreditEvent
WhatIfPublicFinancialSnapshot = _public_context.WhatIfPublicFinancialSnapshot
WhatIfPublicNewsEvent = _public_context.WhatIfPublicNewsEvent
WhatIfPublicRegulatoryEvent = _public_context.WhatIfPublicRegulatoryEvent
WhatIfPublicStockHistoryRow = _public_context.WhatIfPublicStockHistoryRow
build_public_context = _public_context.build_public_context
discover_public_context_path = _public_context.discover_public_context_path
empty_enron_public_context = _public_context.empty_enron_public_context
empty_public_context = _public_context.empty_public_context
load_enron_public_context = _public_context.load_enron_public_context
load_public_context = _public_context.load_public_context
public_context_has_items = _public_context.public_context_has_items
public_context_prompt_lines = _public_context.public_context_prompt_lines
resolve_world_public_context = _public_context.resolve_world_public_context
slice_public_context_to_branch = _public_context.slice_public_context_to_branch
slice_public_context_to_window = _public_context.slice_public_context_to_window

__all__ = [
    "CanonicalHistoryBundle",
    "CanonicalHistoryIndex",
    "CanonicalHistoryIndexRow",
    "CanonicalHistoryPaths",
    "CanonicalHistoryReadinessReport",
    "CanonicalHistoryTimelineResult",
    "ContextProviderConfig",
    "ContextProviderStatusSummary",
    "ContextSnapshot",
    "WhatIfPublicCreditEvent",
    "WhatIfPublicContext",
    "WhatIfPublicFinancialSnapshot",
    "WhatIfPublicNewsEvent",
    "WhatIfPublicRegulatoryEvent",
    "WhatIfPublicStockHistoryRow",
    "ContextSnapshotRole",
    "ContextSnapshotStatusSummary",
    "ContextStatusFinding",
    "ContextSourceResult",
    "CrmSourceData",
    "GoogleSourceData",
    "build_canonical_history_readiness",
    "build_canonical_history_bundle",
    "build_canonical_history_bundle_from_rows",
    "build_public_context",
    "canonical_history_paths",
    "canonical_history_sidecars_exist",
    "capture_context",
    "diff_snapshots",
    "discover_public_context_path",
    "empty_enron_public_context",
    "empty_public_context",
    "hydrate_blueprint",
    "ingest_gmail_export",
    "ingest_mail_archive_threads",
    "ingest_slack_export",
    "load_canonical_history_bundle",
    "load_enron_public_context",
    "load_public_context",
    "legacy_threads_payload_to_snapshot",
    "public_context_has_items",
    "public_context_prompt_lines",
    "query_canonical_history",
    "resolve_world_public_context",
    "snapshot_role",
    "slice_public_context_to_branch",
    "slice_public_context_to_window",
    "source_payload",
    "with_snapshot_role",
    "write_canonical_history_bundle",
    "write_canonical_history_sidecars",
]


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
            logger.warning(
                "context capture failed for %s (%s)",
                config.provider,
                type(exc).__name__,
                extra={
                    "source": "context_capture",
                    "provider": config.provider,
                    "file_path": str(config.base_url or ""),
                    "exception_type": type(exc).__name__,
                },
                exc_info=True,
            )
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
    ).model_copy(update={"metadata": {"snapshot_role": "company_history_bundle"}})


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
        metadata={"snapshot_role": "company_history_bundle"},
    )


def ingest_gmail_export(
    mbox_path: Union[str, Path],
    *,
    organization_name: str,
    organization_domain: str = "",
    message_limit: int = 200,
) -> ContextSnapshot:
    """Ingest a Gmail Takeout export path into a ContextSnapshot."""
    from .providers.gmail import capture_from_export

    result = capture_from_export(
        mbox_path,
        message_limit=message_limit,
    )

    return ContextSnapshot(
        organization_name=organization_name,
        organization_domain=organization_domain,
        captured_at=iso_now(),
        sources=[result],
        metadata={"snapshot_role": "company_history_bundle"},
    )


def ingest_mail_archive_threads(
    threads: list[dict[str, Any]],
    *,
    organization_name: str,
    organization_domain: str = "",
    actors: list[dict[str, Any]] | None = None,
    captured_at: str | None = None,
    metadata: dict[str, Any] | None = None,
) -> ContextSnapshot:
    """Wrap archive-style threaded mail into a ContextSnapshot."""
    resolved_captured_at = captured_at or iso_now()
    normalized_threads: list[dict[str, Any]] = []
    message_count = 0

    for thread in threads:
        if not isinstance(thread, dict):
            continue
        messages = [
            item for item in (thread.get("messages") or []) if isinstance(item, dict)
        ]
        if not messages:
            continue
        message_count += len(messages)
        normalized_threads.append(
            {
                "thread_id": str(thread.get("thread_id", "")),
                "subject": str(thread.get("subject", thread.get("title", ""))),
                "category": str(thread.get("category", "archive")),
                "messages": messages,
            }
        )

    return ContextSnapshot(
        organization_name=organization_name,
        organization_domain=organization_domain,
        captured_at=resolved_captured_at,
        sources=[
            ContextSourceResult(
                provider="mail_archive",
                captured_at=resolved_captured_at,
                status="ok",
                record_counts={
                    "threads": len(normalized_threads),
                    "messages": message_count,
                    "actors": len(actors or []),
                },
                data={
                    "threads": normalized_threads,
                    "actors": list(actors or []),
                    "profile": {},
                },
            )
        ],
        metadata={
            **dict(metadata or {}),
            "snapshot_role": "company_history_bundle",
        },
    )


def diff_snapshots(
    before: ContextSnapshot,
    after: ContextSnapshot,
) -> ContextDiff:
    from .diff import compute_diff

    return compute_diff(before, after)
