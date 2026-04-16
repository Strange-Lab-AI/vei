from __future__ import annotations

import logging
from pathlib import Path
from typing import Sequence

from vei.twin.api import build_customer_twin
from vei.twin.models import ContextMoldConfig
from vei.whatif.artifact_validation import validate_saved_workspace

from .._branch_context import build_branch_context
from .._constants import EPISODE_MANIFEST_FILE, PUBLIC_CONTEXT_FILE
from ..models import (
    WhatIfCaseContext,
    WhatIfEpisodeManifest,
    WhatIfEpisodeMaterialization,
    WhatIfEvent,
    WhatIfSituationContext,
    WhatIfWorld,
)
from ..public_context import empty_public_context
from ..corpus import (
    CONTENT_NOTICE,
    event_reference,
)

from ._snapshot import (
    _episode_context_snapshot,
)
from ._dataset import _baseline_dataset

logger = logging.getLogger(__name__)


def materialize_episode(
    world: WhatIfWorld,
    *,
    root: str | Path,
    thread_id: str | None = None,
    event_id: str | None = None,
    organization_name: str | None = None,
    organization_domain: str | None = None,
) -> WhatIfEpisodeMaterialization:
    workspace_root = Path(root).expanduser().resolve()
    resolved_organization_name = (
        (organization_name or "").strip()
        or world.summary.organization_name
        or "Historical Archive"
    )
    resolved_organization_domain = (
        (organization_domain or "").strip().lower()
        or world.summary.organization_domain
        or "archive.local"
    )
    branch_context = build_branch_context(
        world,
        thread_id=thread_id,
        event_id=event_id,
        organization_domain=resolved_organization_domain,
    )
    history_preview = [
        event_reference(event) for event in branch_context.past_events[-12:]
    ]
    snapshot = _episode_context_snapshot(
        thread_history=branch_context.thread_history,
        past_events=branch_context.past_events,
        thread_id=branch_context.thread_id,
        thread_subject=branch_context.thread_subject,
        organization_name=resolved_organization_name,
        organization_domain=resolved_organization_domain,
        world=world,
        branch_event=branch_context.branch_event,
        public_context=branch_context.public_context,
        case_context=branch_context.case_context,
        situation_context=branch_context.situation_context,
        historical_business_state=branch_context.historical_business_state,
        source_snapshot=branch_context.source_snapshot,
    )
    included_surfaces = _included_surfaces_for_thread(
        branch_context.thread_history,
        case_context=branch_context.case_context,
        situation_context=branch_context.situation_context,
    )
    bundle = build_customer_twin(
        workspace_root,
        snapshot=snapshot,
        organization_name=resolved_organization_name,
        organization_domain=resolved_organization_domain,
        mold=ContextMoldConfig(
            archetype="b2b_saas",
            density_level="medium",
            named_team_expansion="minimal",
            included_surfaces=included_surfaces,
            synthetic_expansion_strength="light",
        ),
        overwrite=True,
    )
    baseline_dataset = _baseline_dataset(
        thread_subject=branch_context.thread_subject,
        branch_event=branch_context.branch_event,
        future_events=branch_context.future_events,
        organization_domain=resolved_organization_domain,
        source_name=world.source,
    )
    baseline_dataset_path = workspace_root / "whatif_baseline_dataset.json"
    baseline_dataset_path.write_text(
        baseline_dataset.model_dump_json(indent=2),
        encoding="utf-8",
    )
    resolved_public_context = branch_context.public_context or empty_public_context(
        organization_name=resolved_organization_name,
        organization_domain=resolved_organization_domain,
        branch_timestamp=branch_context.branch_event.timestamp,
    )
    public_context_path = workspace_root / PUBLIC_CONTEXT_FILE
    public_context_path.write_text(
        resolved_public_context.model_dump_json(indent=2),
        encoding="utf-8",
    )
    manifest = WhatIfEpisodeManifest(
        source=world.source,
        source_dir=world.source_dir,
        workspace_root=workspace_root,
        organization_name=resolved_organization_name,
        organization_domain=resolved_organization_domain,
        thread_id=branch_context.thread_id,
        thread_subject=branch_context.thread_subject,
        case_id=branch_context.branch_event.case_id,
        surface=branch_context.branch_event.surface,
        branch_event_id=branch_context.branch_event.event_id,
        branch_timestamp=branch_context.branch_event.timestamp,
        branch_event=branch_context.branch_reference,
        history_message_count=len(branch_context.past_events),
        future_event_count=len(branch_context.future_events),
        baseline_dataset_path=str(baseline_dataset_path.relative_to(workspace_root)),
        content_notice=str(world.metadata.get("content_notice", CONTENT_NOTICE)),
        actor_ids=sorted(
            {
                actor_id
                for event in branch_context.thread_history
                for actor_id in {event.actor_id, event.target_id}
                if actor_id
            }
        ),
        history_preview=history_preview,
        baseline_future_preview=[
            event_reference(event) for event in branch_context.future_events[:5]
        ],
        forecast=branch_context.forecast,
        public_context=resolved_public_context,
        case_context=branch_context.case_context,
        situation_context=branch_context.situation_context,
        historical_business_state=branch_context.historical_business_state,
    )
    manifest_path = workspace_root / EPISODE_MANIFEST_FILE
    manifest_path.write_text(manifest.model_dump_json(indent=2), encoding="utf-8")
    issues = validate_saved_workspace(workspace_root)
    if issues:
        issue_text = "; ".join(issues)
        raise ValueError(f"saved workspace validation failed: {issue_text}")
    return WhatIfEpisodeMaterialization(
        manifest_path=manifest_path,
        bundle_path=workspace_root / "twin_manifest.json",
        context_snapshot_path=workspace_root / bundle.context_snapshot_path,
        baseline_dataset_path=baseline_dataset_path,
        workspace_root=workspace_root,
        organization_name=resolved_organization_name,
        organization_domain=resolved_organization_domain,
        thread_id=branch_context.thread_id,
        case_id=branch_context.branch_event.case_id,
        surface=branch_context.branch_event.surface,
        branch_event_id=branch_context.branch_event.event_id,
        branch_event=manifest.branch_event,
        history_message_count=len(branch_context.past_events),
        future_event_count=len(branch_context.future_events),
        history_preview=history_preview,
        baseline_future_preview=list(manifest.baseline_future_preview),
        forecast=branch_context.forecast,
        public_context=resolved_public_context,
        case_context=branch_context.case_context,
        situation_context=branch_context.situation_context,
        historical_business_state=branch_context.historical_business_state,
    )


def _included_surfaces_for_thread(
    events: Sequence[WhatIfEvent],
    *,
    case_context: WhatIfCaseContext | None = None,
    situation_context: WhatIfSituationContext | None = None,
) -> list[str]:
    surfaces = {event.surface or "mail" for event in events}
    if case_context is not None:
        surfaces.update(
            reference.surface
            for reference in case_context.related_history
            if reference.surface
        )
        surfaces.update(
            record.surface for record in case_context.records if record.surface
        )
    if situation_context is not None:
        surfaces.update(
            thread.surface
            for thread in situation_context.related_threads
            if thread.surface
        )
        surfaces.update(
            reference.surface
            for reference in situation_context.related_history
            if reference.surface
        )
    included: list[str] = ["identity"]
    if "mail" in surfaces:
        included.insert(0, "mail")
    if "slack" in surfaces:
        included.insert(0, "slack")
    if "tickets" in surfaces:
        included.insert(0, "tickets")
    if "docs" in surfaces:
        included.insert(0, "docs")
    if "crm" in surfaces:
        included.insert(0, "crm")
    return included
