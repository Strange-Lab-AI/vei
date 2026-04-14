from __future__ import annotations

import logging
from pathlib import Path
from typing import Sequence

from vei.twin.api import build_customer_twin
from vei.twin.models import ContextMoldConfig
from vei.whatif.artifact_validation import validate_saved_workspace

from ..models import (
    WhatIfCaseContext,
    WhatIfEpisodeManifest,
    WhatIfEpisodeMaterialization,
    WhatIfEvent,
    WhatIfSituationContext,
    WhatIfWorld,
)
from ..corpus import (
    CONTENT_NOTICE,
    choose_branch_event,
    event_by_id,
    event_reference,
    hydrate_event_snippets,
    thread_events,
    thread_subject,
)
from ..cases import build_case_context
from ..public_context import slice_public_context_to_branch
from ..business_state import assess_historical_business_state
from ..situations import build_situation_context, recommend_branch_thread

from ._snapshot import (
    _episode_context_snapshot,
    _source_snapshot_for_world,
    _persist_workspace_historical_source,
)
from ._dataset import _baseline_dataset
from ._replay import score_historical_tail

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
    (
        selected_thread_id,
        thread_history,
        branch_event,
        past_events,
        future_events,
        selected_thread_subject,
    ) = resolve_thread_branch(
        world,
        thread_id=thread_id,
        event_id=event_id,
    )
    branch_public_context = slice_public_context_to_branch(
        world.public_context,
        branch_timestamp=branch_event.timestamp,
    )
    source_snapshot = _source_snapshot_for_world(world)
    case_context = build_case_context(
        snapshot=source_snapshot,
        events=world.events,
        case_id=branch_event.case_id,
        branch_thread_id=selected_thread_id,
        branch_timestamp_ms=branch_event.timestamp_ms,
    )
    situation_context = build_situation_context(
        world,
        branch_thread_id=selected_thread_id,
        branch_timestamp_ms=branch_event.timestamp_ms,
    )
    forecast = score_historical_tail(
        future_events,
        organization_domain=resolved_organization_domain,
    )
    historical_business_state = assess_historical_business_state(
        branch_event=event_reference(branch_event),
        forecast=forecast,
        organization_domain=resolved_organization_domain,
        public_context=branch_public_context,
    )
    history_preview = [event_reference(event) for event in past_events[-12:]]
    snapshot = _episode_context_snapshot(
        thread_history=thread_history,
        past_events=past_events,
        thread_id=selected_thread_id,
        thread_subject=selected_thread_subject,
        organization_name=resolved_organization_name,
        organization_domain=resolved_organization_domain,
        world=world,
        branch_event=branch_event,
        public_context=branch_public_context,
        case_context=case_context,
        situation_context=situation_context,
        historical_business_state=historical_business_state,
        source_snapshot=source_snapshot,
    )
    included_surfaces = _included_surfaces_for_thread(
        thread_history,
        case_context=case_context,
        situation_context=situation_context,
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
        thread_subject=selected_thread_subject,
        branch_event=branch_event,
        future_events=future_events,
        organization_domain=resolved_organization_domain,
        source_name=world.source,
    )
    baseline_dataset_path = workspace_root / "whatif_baseline_dataset.json"
    baseline_dataset_path.write_text(
        baseline_dataset.model_dump_json(indent=2),
        encoding="utf-8",
    )
    _persist_workspace_historical_source(world, workspace_root)
    manifest = WhatIfEpisodeManifest(
        source=world.source,
        source_dir=world.source_dir,
        workspace_root=workspace_root,
        organization_name=resolved_organization_name,
        organization_domain=resolved_organization_domain,
        thread_id=selected_thread_id,
        thread_subject=selected_thread_subject,
        case_id=branch_event.case_id,
        surface=branch_event.surface,
        branch_event_id=branch_event.event_id,
        branch_timestamp=branch_event.timestamp,
        branch_event=event_reference(branch_event),
        history_message_count=len(past_events),
        future_event_count=len(future_events),
        baseline_dataset_path=str(baseline_dataset_path.relative_to(workspace_root)),
        content_notice=str(world.metadata.get("content_notice", CONTENT_NOTICE)),
        actor_ids=sorted(
            {
                actor_id
                for event in thread_history
                for actor_id in {event.actor_id, event.target_id}
                if actor_id
            }
        ),
        history_preview=history_preview,
        baseline_future_preview=[event_reference(event) for event in future_events[:5]],
        forecast=forecast,
        public_context=branch_public_context,
        case_context=case_context,
        situation_context=situation_context,
        historical_business_state=historical_business_state,
    )
    manifest_path = workspace_root / "episode_manifest.json"
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
        thread_id=selected_thread_id,
        case_id=branch_event.case_id,
        surface=branch_event.surface,
        branch_event_id=branch_event.event_id,
        branch_event=manifest.branch_event,
        history_message_count=len(past_events),
        future_event_count=len(future_events),
        history_preview=history_preview,
        baseline_future_preview=list(manifest.baseline_future_preview),
        forecast=forecast,
        public_context=branch_public_context,
        case_context=case_context,
        situation_context=situation_context,
        historical_business_state=historical_business_state,
    )


def resolve_thread_branch(
    world: WhatIfWorld,
    *,
    thread_id: str | None = None,
    event_id: str | None = None,
) -> tuple[
    str, list[WhatIfEvent], WhatIfEvent, list[WhatIfEvent], list[WhatIfEvent], str
]:
    selected_thread_id = thread_id
    if selected_thread_id is None:
        if event_id:
            selected_event = event_by_id(world.events, event_id)
            if selected_event is None:
                raise ValueError(f"event not found in world: {event_id}")
            selected_thread_id = selected_event.thread_id
        else:
            selected_thread_id = recommend_branch_thread(world).thread_id

    thread_history = thread_events(world.events, selected_thread_id)
    if not thread_history:
        raise ValueError(f"thread not found in world: {selected_thread_id}")
    if world.source == "enron":
        thread_history = hydrate_event_snippets(
            rosetta_dir=world.source_dir,
            events=thread_history,
        )

    branch_event = choose_branch_event(thread_history, requested_event_id=event_id)
    branch_index = next(
        (
            index
            for index, event in enumerate(thread_history)
            if event.event_id == branch_event.event_id
        ),
        None,
    )
    if branch_index is None:
        raise ValueError(f"branch event not found in thread: {branch_event.event_id}")

    return (
        selected_thread_id,
        thread_history,
        branch_event,
        list(thread_history[:branch_index]),
        list(thread_history[branch_index:]),
        thread_subject(
            world.threads,
            selected_thread_id,
            fallback=branch_event.subject,
        ),
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
