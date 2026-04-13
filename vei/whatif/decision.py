from __future__ import annotations

from pathlib import Path

from .models import (
    WhatIfDecisionOption,
    WhatIfDecisionScene,
    WhatIfEventReference,
    WhatIfForecast,
    WhatIfWorld,
)
from .corpus import (
    CONTENT_NOTICE,
    display_name,
    event_reference,
    has_external_recipients,
)
from .cases import build_case_context
from .public_context import slice_public_context_to_branch
from .business_state import assess_historical_business_state
from .episode import (
    resolve_thread_branch,
    forecast_episode,
    load_episode_manifest,
    _source_snapshot_for_world,
    _history_preview_from_saved_context,
)


def build_decision_scene(
    world: WhatIfWorld,
    *,
    thread_id: str | None = None,
    event_id: str | None = None,
    history_limit: int = 6,
    future_limit: int = 5,
) -> WhatIfDecisionScene:
    (
        selected_thread_id,
        _thread_history,
        branch_event,
        past_events,
        future_events,
        selected_thread_subject,
    ) = resolve_thread_branch(
        world,
        thread_id=thread_id,
        event_id=event_id,
    )
    organization_name = world.summary.organization_name or "Historical Archive"
    organization_domain = world.summary.organization_domain or "archive.local"
    branch_reference = event_reference(branch_event)
    branch_public_context = slice_public_context_to_branch(
        world.public_context,
        branch_timestamp=branch_event.timestamp,
    )
    case_context = build_case_context(
        snapshot=_source_snapshot_for_world(world),
        events=world.events,
        case_id=branch_event.case_id,
        branch_thread_id=selected_thread_id,
        branch_timestamp_ms=branch_event.timestamp_ms,
    )
    forecast = forecast_episode(
        future_events,
        organization_domain=organization_domain,
    )
    historical_business_state = assess_historical_business_state(
        branch_event=branch_reference,
        forecast=forecast,
        organization_domain=organization_domain,
        public_context=branch_public_context,
    )
    return WhatIfDecisionScene(
        source=world.source,
        organization_name=organization_name,
        organization_domain=organization_domain,
        thread_id=selected_thread_id,
        thread_subject=selected_thread_subject,
        case_id=branch_reference.case_id,
        surface=branch_reference.surface,
        branch_event_id=branch_event.event_id,
        branch_event=branch_reference,
        history_message_count=len(past_events),
        future_event_count=len(future_events),
        content_notice=str(world.metadata.get("content_notice", CONTENT_NOTICE)),
        branch_summary=_decision_branch_summary(
            branch_reference,
            thread_subject=selected_thread_subject,
            organization_domain=organization_domain,
        ),
        historical_action_summary=_historical_action_summary(
            branch_reference,
            thread_subject=selected_thread_subject,
            organization_domain=organization_domain,
        ),
        historical_outcome_summary=_historical_outcome_summary(forecast),
        stakes_summary=_decision_stakes_summary(
            branch_reference,
            forecast,
            organization_domain=organization_domain,
        ),
        decision_question=_decision_question(selected_thread_subject),
        history_preview=[
            event_reference(event) for event in past_events[-max(1, history_limit) :]
        ],
        historical_future_preview=[
            event_reference(event) for event in future_events[: max(1, future_limit)]
        ],
        candidate_options=_decision_options_for_branch(
            branch_reference,
            thread_subject=selected_thread_subject,
            organization_name=organization_name,
            organization_domain=organization_domain,
        ),
        public_context=branch_public_context,
        case_context=case_context,
        historical_business_state=historical_business_state,
    )


def build_saved_decision_scene(
    root: str | Path,
    *,
    history_limit: int = 6,
    future_limit: int = 5,
) -> WhatIfDecisionScene:
    workspace_root = Path(root).expanduser().resolve()
    manifest = load_episode_manifest(workspace_root)
    history_preview = _history_preview_from_saved_context(
        workspace_root,
        manifest=manifest,
        history_limit=history_limit,
    )
    return WhatIfDecisionScene(
        source=manifest.source,
        organization_name=manifest.organization_name,
        organization_domain=manifest.organization_domain,
        thread_id=manifest.thread_id,
        thread_subject=manifest.thread_subject,
        case_id=manifest.case_id,
        surface=manifest.surface,
        branch_event_id=manifest.branch_event_id,
        branch_event=manifest.branch_event,
        history_message_count=manifest.history_message_count,
        future_event_count=manifest.future_event_count,
        content_notice=manifest.content_notice,
        branch_summary=_decision_branch_summary(
            manifest.branch_event,
            thread_subject=manifest.thread_subject,
            organization_domain=manifest.organization_domain,
        ),
        historical_action_summary=_historical_action_summary(
            manifest.branch_event,
            thread_subject=manifest.thread_subject,
            organization_domain=manifest.organization_domain,
        ),
        historical_outcome_summary=_historical_outcome_summary(manifest.forecast),
        stakes_summary=_decision_stakes_summary(
            manifest.branch_event,
            manifest.forecast,
            organization_domain=manifest.organization_domain,
        ),
        decision_question=_decision_question(manifest.thread_subject),
        history_preview=history_preview,
        historical_future_preview=list(manifest.baseline_future_preview[:future_limit]),
        candidate_options=_decision_options_for_branch(
            manifest.branch_event,
            thread_subject=manifest.thread_subject,
            organization_name=manifest.organization_name,
            organization_domain=manifest.organization_domain,
        ),
        public_context=manifest.public_context,
        case_context=manifest.case_context,
        historical_business_state=manifest.historical_business_state,
    )


def _decision_branch_summary(
    branch_event: WhatIfEventReference,
    *,
    thread_subject: str,
    organization_domain: str,
) -> str:
    actor = display_name(branch_event.actor_id)
    verb = _historical_action_verb(branch_event, tense="present")
    subject = (
        thread_subject
        or branch_event.subject
        or branch_event.thread_id
        or "this thread"
    )
    recipient = _branch_recipient_label(
        branch_event,
        organization_domain=organization_domain,
    )
    if branch_event.surface == "slack":
        return f'{actor} is about to {verb} in {recipient} on "{subject}".'
    if branch_event.surface == "tickets":
        return f'{actor} is about to {verb} ticket "{subject}".'
    return f'{actor} is about to {verb} "{subject}" to {recipient}.'


def _historical_action_summary(
    branch_event: WhatIfEventReference,
    *,
    thread_subject: str,
    organization_domain: str,
) -> str:
    actor = display_name(branch_event.actor_id)
    verb = _historical_action_verb(branch_event, tense="past")
    recipient = _branch_recipient_label(
        branch_event,
        organization_domain=organization_domain,
    )
    details: list[str] = []
    if _branch_has_external_sharing(
        branch_event,
        organization_domain=organization_domain,
    ):
        details.append("outside recipient in scope")
    if branch_event.has_attachment_reference:
        details.append("attachment reference present")
    if branch_event.is_forward:
        details.append("forward metadata present")
    if branch_event.is_escalation:
        details.append("escalation signal present")
    suffix = f" ({', '.join(details)})" if details else ""
    subject = (
        thread_subject
        or branch_event.subject
        or branch_event.thread_id
        or "this thread"
    )
    if branch_event.surface == "slack":
        return f'Historically, {actor} {verb} in {recipient} on "{subject}"{suffix}.'
    if branch_event.surface == "tickets":
        return f'Historically, {actor} {verb} ticket "{subject}"{suffix}.'
    return f'Historically, {actor} {verb} "{subject}" to {recipient}{suffix}.'


def _historical_outcome_summary(forecast: WhatIfForecast) -> str:
    return (
        f"The recorded future had {forecast.future_event_count} follow-up events, "
        f"{forecast.future_external_event_count} outside-addressed sends, and "
        f"{forecast.future_escalation_count} escalations."
    )


def _decision_stakes_summary(
    branch_event: WhatIfEventReference,
    forecast: WhatIfForecast,
    *,
    organization_domain: str,
) -> str:
    notes: list[str] = []
    if _branch_has_external_sharing(
        branch_event,
        organization_domain=organization_domain,
    ):
        notes.append("This move reaches outside the company.")
    if branch_event.has_attachment_reference:
        notes.append("The thread carries document-sharing risk.")
    if branch_event.is_escalation or forecast.future_escalation_count > 0:
        notes.append("Leadership or escalation pressure is visible around this thread.")
    if forecast.future_event_count >= 6:
        notes.append(
            "The recorded future stayed active long enough to create coordination load."
        )
    if branch_event.surface == "slack" and not notes:
        notes.append(
            "This moment changes who stays in the channel thread and how much internal coordination follows."
        )
    if branch_event.surface == "tickets" and not notes:
        notes.append(
            "This moment changes ticket ownership, resolution pace, and escalation pressure."
        )
    if not notes:
        notes.append(
            "This moment changes who stays in the loop, how fast the thread moves, and how much follow-up work appears."
        )
    return " ".join(notes[:3])


def _decision_question(thread_subject: str) -> str:
    subject = thread_subject or "this thread"
    return f'What should the company do at this point in "{subject}"?'


def _decision_options_for_branch(
    branch_event: WhatIfEventReference,
    *,
    thread_subject: str,
    organization_name: str,
    organization_domain: str,
) -> list[WhatIfDecisionOption]:
    subject = (
        thread_subject
        or branch_event.subject
        or branch_event.thread_id
        or "this thread"
    )
    counterparty = _branch_recipient_label(
        branch_event,
        organization_domain=organization_domain,
    )
    company_label = organization_name or "the company"
    if branch_event.is_escalation or branch_event.event_type == "escalation":
        return [
            WhatIfDecisionOption(
                option_id="fact_gather",
                label="Pause and gather facts",
                summary="Keep the thread narrow, collect the facts, and name one owner before the next escalation.",
                prompt=(
                    f'Hold the escalation on "{subject}", gather the key facts in one internal note, '
                    "and assign one owner before anyone widens the leadership loop."
                ),
            ),
            WhatIfDecisionOption(
                option_id="single_owner_escalation",
                label="Escalate through one owner",
                summary="Move the thread upward, but through one clear owner and a tighter review path.",
                prompt=(
                    f'Escalate "{subject}" through one named owner, keep distribution narrow, '
                    "and ask for one clear decision instead of a broad leadership blast."
                ),
            ),
            WhatIfDecisionOption(
                option_id="broad_escalation",
                label="Open a broad leadership loop",
                summary="Push for speed by widening the escalation quickly across leaders.",
                prompt=(
                    f'Forward "{subject}" broadly across leadership, ask for rapid views, '
                    "and keep the loop open until a consensus forms."
                ),
            ),
        ]

    if (
        _branch_has_external_sharing(
            branch_event,
            organization_domain=organization_domain,
        )
        or branch_event.has_attachment_reference
        or branch_event.is_forward
    ):
        return [
            WhatIfDecisionOption(
                option_id="internal_review",
                label="Hold for internal review",
                summary="Tightest risk posture. Keep the material inside the company for one more review pass.",
                prompt=(
                    f'Keep "{subject}" inside {company_label}, ask legal or the internal owner for one more review, '
                    "and hold the outside send until one owner clears it."
                ),
            ),
            WhatIfDecisionOption(
                option_id="narrow_status",
                label="Send a narrow status note",
                summary="Keep the relationship warm without sending the full material yet.",
                prompt=(
                    f"Send {counterparty} a short no-attachment status note, promise a clean update soon, "
                    "and keep one internal owner on the next step."
                ),
            ),
            WhatIfDecisionOption(
                option_id="fast_turnaround",
                label="Push for fast turnaround",
                summary="Bias toward speed. Keep the outside loop active and widen circulation for fast comments.",
                prompt=(
                    f'Send "{subject}" now, keep the outside recipient loop active, '
                    "and widen circulation for rapid comments and turnaround."
                ),
            ),
        ]

    if branch_event.event_type == "assignment":
        return [
            WhatIfDecisionOption(
                option_id="single_owner",
                label="Assign one owner",
                summary="Keep the loop tight and make one person responsible for the next step.",
                prompt=(
                    f'Rewrite the next step on "{subject}" into one internal note with one named owner '
                    "and one required action."
                ),
            ),
            WhatIfDecisionOption(
                option_id="focused_review",
                label="Route through focused review",
                summary="Get a stronger answer before the thread broadens.",
                prompt=(
                    f'Route "{subject}" through one focused internal review path before anyone widens the thread, '
                    "then respond with a single consolidated answer."
                ),
            ),
            WhatIfDecisionOption(
                option_id="broad_coordination",
                label="Open a broader coordination loop",
                summary="Trade more coordination for more input and speed.",
                prompt=(
                    f'Forward "{subject}" to a broader cross-functional group, ask for quick comments, '
                    "and keep the thread moving in parallel."
                ),
            ),
        ]

    return [
        WhatIfDecisionOption(
            option_id="tight_loop",
            label="Keep the loop tight",
            summary="Use a narrow internal path and reduce follow-up sprawl.",
            prompt=(
                f'Keep "{subject}" in a tight internal loop, name one owner, '
                "and avoid widening the thread until the next step is clear."
            ),
        ),
        WhatIfDecisionOption(
            option_id="clear_reply",
            label="Reply with a clear next step",
            summary="Balance speed and control with one direct response and one owner.",
            prompt=(
                f'Reply on "{subject}" with one clear next step, one named owner, '
                "and one concrete commitment on timing."
            ),
        ),
        WhatIfDecisionOption(
            option_id="widen_loop",
            label="Widen the loop for speed",
            summary="Invite more people in quickly to accelerate the thread.",
            prompt=(
                f'Widen the participant loop on "{subject}", ask for rapid comments, '
                "and keep the thread moving with parallel follow-up."
            ),
        ),
    ]


def _historical_action_verb(
    branch_event: WhatIfEventReference,
    *,
    tense: str,
) -> str:
    if branch_event.is_escalation or branch_event.event_type == "escalation":
        return "escalated" if tense == "past" else "escalate"
    if branch_event.surface == "slack":
        return "replied" if tense == "past" else "reply"
    if branch_event.surface == "tickets":
        return "updated" if tense == "past" else "update"
    if branch_event.event_type == "assignment" and _branch_looks_like_mail_send(
        branch_event
    ):
        return "sent" if tense == "past" else "send"
    if branch_event.event_type == "assignment":
        return "assigned" if tense == "past" else "assign"
    if branch_event.is_forward:
        return "forwarded" if tense == "past" else "forward"
    if branch_event.is_reply or branch_event.event_type == "reply":
        return "replied on" if tense == "past" else "reply on"
    return "sent" if tense == "past" else "send"


def _branch_looks_like_mail_send(branch_event: WhatIfEventReference) -> bool:
    if branch_event.surface != "mail":
        return False
    recipients = [item for item in branch_event.to_recipients if item]
    if not recipients and branch_event.target_id:
        recipients = [branch_event.target_id]
    return any("@" in recipient for recipient in recipients)


def _branch_recipient_label(
    branch_event: WhatIfEventReference,
    *,
    organization_domain: str,
) -> str:
    if branch_event.surface == "tickets":
        return branch_event.thread_id.split(":", 1)[-1]
    recipients = [item for item in branch_event.to_recipients if item]
    if not recipients and branch_event.target_id:
        recipients = [branch_event.target_id]
    if not recipients:
        if branch_event.surface == "slack":
            return "the current channel"
        if branch_event.surface == "tickets":
            return branch_event.thread_id
        return "the current thread"

    display_recipients = recipients[:2]
    label = ", ".join(
        item if item.startswith("#") else display_name(item)
        for item in display_recipients
    )
    if len(recipients) > 2:
        label = f"{label}, and {len(recipients) - 2} more"
    if has_external_recipients(
        recipients,
        organization_domain=organization_domain,
    ):
        return label
    return label


def _branch_has_external_sharing(
    branch_event: WhatIfEventReference,
    *,
    organization_domain: str,
) -> bool:
    recipients = [item for item in branch_event.to_recipients if item]
    if not recipients and branch_event.target_id:
        recipients = [branch_event.target_id]
    if not recipients:
        return False
    return has_external_recipients(
        recipients,
        organization_domain=organization_domain,
    )
