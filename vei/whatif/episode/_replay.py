from __future__ import annotations

import logging
from pathlib import Path
from typing import Any, Sequence

from vei.blueprint.api import create_world_session_from_blueprint
from vei.blueprint.api import BlueprintAsset
from vei.data.models import VEIDataset
from vei.twin import load_customer_twin
from vei.whatif.filenames import EPISODE_MANIFEST_FILE

from ..models import (
    WhatIfEpisodeManifest,
    WhatIfEvent,
    WhatIfHistoricalScore,
    WhatIfReplaySummary,
)
from ..corpus import (
    ENRON_DOMAIN,
    has_external_recipients,
)
from .._helpers import (
    chat_channel_name_from_reference as _chat_channel_name_from_reference,
)
from ..macro_outcomes import attach_macro_outcomes_to_historical_score

logger = logging.getLogger(__name__)


def load_episode_manifest(root: str | Path) -> WhatIfEpisodeManifest:
    workspace_root = Path(root).expanduser().resolve()
    manifest_path = workspace_root / EPISODE_MANIFEST_FILE
    if not manifest_path.exists():
        raise ValueError(f"what-if episode manifest not found: {manifest_path}")
    return WhatIfEpisodeManifest.model_validate_json(
        manifest_path.read_text(encoding="utf-8")
    )


def replay_episode_baseline(
    root: str | Path,
    *,
    tick_ms: int = 0,
    seed: int = 42042,
) -> WhatIfReplaySummary:
    workspace_root = Path(root).expanduser().resolve()
    manifest = load_episode_manifest(workspace_root)
    bundle = load_customer_twin(workspace_root)
    asset_path = workspace_root / bundle.blueprint_asset_path
    dataset_path = workspace_root / manifest.baseline_dataset_path
    asset = BlueprintAsset.model_validate_json(asset_path.read_text(encoding="utf-8"))
    dataset = VEIDataset.model_validate_json(dataset_path.read_text(encoding="utf-8"))
    session = create_world_session_from_blueprint(asset, seed=seed)
    replay_result = session.replay(mode="overlay", dataset_events=dataset.events)

    delivered_event_count = 0
    current_time_ms = session.router.bus.clock_ms
    pending_events = session.pending()
    if tick_ms > 0:
        tick_result = session.router.tick(dt_ms=tick_ms)
        delivered_event_count = sum(tick_result.get("delivered", {}).values())
        current_time_ms = int(tick_result.get("time_ms", current_time_ms))
        pending_events = dict(tick_result.get("pending", {}))

    inbox_count = 0
    top_subjects: list[str] = []
    visible_item_count = 0
    top_items: list[str] = []
    if manifest.surface == "mail":
        inbox = session.call_tool("mail.list", {})
        inbox_count = len(inbox)
        top_subjects = [
            str(item.get("subj", ""))
            for item in inbox[:5]
            if isinstance(item, dict) and item.get("subj")
        ]
        visible_item_count = inbox_count
        top_items = list(top_subjects)
    elif manifest.surface == "slack":
        channel_name = _chat_channel_name_from_reference(manifest.branch_event)
        channel_payload = session.call_tool(
            "slack.open_channel",
            {"channel": channel_name},
        )
        channel_messages = (
            channel_payload.get("messages", [])
            if isinstance(channel_payload, dict)
            else []
        )
        messages = _slack_thread_messages(
            channel_messages,
            conversation_anchor=manifest.branch_event.conversation_anchor,
        )
        visible_item_count = len(messages)
        top_items = [
            str(item.get("text", ""))
            for item in messages[:5]
            if isinstance(item, dict) and item.get("text")
        ]
    elif manifest.surface == "tickets":
        tickets_payload = session.call_tool("tickets.list", {})
        tickets = tickets_payload if isinstance(tickets_payload, list) else []
        visible_item_count = len(tickets)
        top_items = [
            str(item.get("title", ""))
            for item in tickets[:5]
            if isinstance(item, dict) and item.get("title")
        ]
    return WhatIfReplaySummary(
        workspace_root=workspace_root,
        baseline_dataset_path=dataset_path,
        surface=manifest.surface,
        scheduled_event_count=int(replay_result.get("scheduled", 0)),
        delivered_event_count=delivered_event_count,
        current_time_ms=current_time_ms,
        pending_events=pending_events,
        inbox_count=inbox_count,
        top_subjects=top_subjects,
        visible_item_count=visible_item_count,
        top_items=top_items,
        baseline_future_preview=list(manifest.baseline_future_preview),
        forecast=manifest.forecast,
    )


def score_historical_tail(
    events: Sequence[WhatIfEvent],
    *,
    organization_domain: str = ENRON_DOMAIN,
    branch_timestamp: str = "",
    public_context=None,
) -> WhatIfHistoricalScore:
    future_event_count = len(events)
    future_escalation_count = sum(
        1
        for event in events
        if event.flags.is_escalation or event.event_type == "escalation"
    )
    future_assignment_count = sum(
        1 for event in events if event.event_type == "assignment"
    )
    future_approval_count = sum(1 for event in events if event.event_type == "approval")
    future_external_event_count = sum(
        1
        for event in events
        if has_external_recipients(
            event.flags.to_recipients,
            organization_domain=organization_domain,
        )
    )
    risk_score = min(
        1.0,
        (
            (future_escalation_count * 0.25)
            + (future_assignment_count * 0.15)
            + (future_external_event_count * 0.2)
            + max(0, future_event_count - future_approval_count) * 0.02
        ),
    )
    summary = (
        f"{future_event_count} future events remain, including "
        f"{future_escalation_count} escalations and {future_external_event_count} "
        "externally addressed messages."
    )
    score = WhatIfHistoricalScore(
        backend="historical",
        future_event_count=future_event_count,
        future_escalation_count=future_escalation_count,
        future_assignment_count=future_assignment_count,
        future_approval_count=future_approval_count,
        future_external_event_count=future_external_event_count,
        risk_score=round(risk_score, 3),
        summary=summary,
    )
    return attach_macro_outcomes_to_historical_score(
        score,
        organization_domain=organization_domain,
        branch_timestamp=branch_timestamp,
        public_context=public_context,
    )


def _slack_thread_messages(
    messages: Sequence[dict[str, Any]],
    *,
    conversation_anchor: str,
) -> list[dict[str, Any]]:
    if not conversation_anchor:
        return [item for item in messages if isinstance(item, dict)]
    return [
        item
        for item in messages
        if isinstance(item, dict)
        and str(item.get("thread_ts") or item.get("ts") or "").split(".", 1)[0]
        == conversation_anchor
    ] or [item for item in messages if isinstance(item, dict)]
