from __future__ import annotations

import asyncio
import logging
import os
import threading
from pathlib import Path
from typing import Any, Awaitable, Sequence, TypeVar

from vei.blueprint.api import create_world_session_from_blueprint
from vei.blueprint.api import BlueprintAsset
from vei.data.models import BaseEvent, VEIDataset
from vei.llm import providers
from vei.project_settings import default_model_for_provider
from vei.twin import load_customer_twin

try:
    from dotenv import load_dotenv
except ImportError:  # pragma: no cover

    def load_dotenv(*args: object, **kwargs: object) -> None:
        return None


from .models import (
    WhatIfEpisodeManifest,
    WhatIfEventReference,
    WhatIfHistoricalScore,
    WhatIfCounterfactualEstimateDelta,
    WhatIfCounterfactualEstimateResult,
    WhatIfLLMGeneratedMessage,
    WhatIfLLMReplayResult,
    WhatIfLLMUsage,
    WhatIfPublicContext,
)
from .corpus import (
    ENRON_DOMAIN,
    safe_int,
)
from vei.context.api import public_context_prompt_lines
from .cases import case_context_prompt_lines
from .business_state import describe_forecast_business_change
from .macro_outcomes import attach_macro_outcomes_to_forecast_result
from .episode import load_episode_manifest
from ._helpers import intervention_tags
from .situations import situation_context_prompt_lines
from ._helpers import (
    chat_channel_name_from_reference as _chat_channel_name_from_reference,
    historical_archive_address as _historical_archive_address,
    load_episode_snapshot as _load_episode_snapshot,
)

logger = logging.getLogger(__name__)

_RunResultT = TypeVar("_RunResultT")
_COUNTERFACTUAL_FAILURES = (
    asyncio.TimeoutError,
    OSError,
    RuntimeError,
    TypeError,
    ValueError,
)

_DEFAULT_LLM_TIMEOUT_S = 180


def _llm_timeout_seconds() -> int:
    """Resolve the per-call LLM timeout for what-if counterfactuals.

    Override with VEI_WHATIF_LLM_TIMEOUT_S (defaults to 180s, since reasoning
    models like gpt-5-mini routinely exceed 60s on long contexts).
    """
    raw = os.environ.get("VEI_WHATIF_LLM_TIMEOUT_S", "").strip()
    if not raw:
        return _DEFAULT_LLM_TIMEOUT_S
    try:
        value = int(raw)
    except ValueError:
        return _DEFAULT_LLM_TIMEOUT_S
    return max(15, value)


def _run_async(awaitable: Awaitable[_RunResultT]) -> _RunResultT:
    """Run a coroutine from sync code, even when an event loop is already active."""
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        return asyncio.run(awaitable)
    if not loop.is_running():
        return asyncio.run(awaitable)
    return _run_async_in_thread(awaitable)


def _run_async_in_thread(awaitable: Awaitable[_RunResultT]) -> _RunResultT:
    result: dict[str, _RunResultT] = {}
    error: dict[str, BaseException] = {}

    def runner() -> None:
        try:
            result["value"] = asyncio.run(awaitable)
        except BaseException as exc:  # pragma: no cover - surfaced in caller
            error["value"] = exc

    thread = threading.Thread(target=runner, daemon=True)
    thread.start()
    thread.join()
    if "value" in error:
        raise error["value"]
    if "value" not in result:
        raise RuntimeError("counterfactual runner returned no result")
    return result["value"]


# ---------------------------------------------------------------------------
# Private helpers
# ---------------------------------------------------------------------------


def _history_prompt_line(event: WhatIfEventReference) -> str:
    target = ", ".join(event.to_recipients) or event.target_id or event.thread_id
    if event.surface == "slack":
        return (
            f"- Actor: {event.actor_id}\n"
            f"  Channel: {target}\n"
            f"  Type: {event.event_type}\n"
            f"  Thread: {event.subject}\n"
            f"  Text: {event.snippet}"
        )
    if event.surface == "tickets":
        return (
            f"- Actor: {event.actor_id}\n"
            f"  Ticket: {event.thread_id.split(':', 1)[-1]}\n"
            f"  Type: {event.event_type}\n"
            f"  Title: {event.subject}\n"
            f"  Detail: {event.snippet}"
        )
    return (
        f"- From: {event.actor_id}\n"
        f"  To: {target}\n"
        f"  Type: {event.event_type}\n"
        f"  Subject: {event.subject}\n"
        f"  Body: {event.snippet}"
    )


def _llm_surface_instructions(manifest: WhatIfEpisodeManifest) -> str:
    if manifest.surface == "slack":
        channel_name = _chat_channel_name_from_reference(manifest.branch_event)
        return (
            "Use surface='slack'. Set 'to' to the channel name, keep body_text as the chat text, "
            f"and keep conversation_anchor as '{manifest.branch_event.conversation_anchor or ''}' "
            f"for replies in {channel_name}."
        )
    if manifest.surface == "tickets":
        return (
            "Use surface='tickets'. Set 'to' to the ticket id, keep body_text as the ticket comment or update note, "
            "and keep the action on the same ticket."
        )
    return (
        "Use surface='mail'. Set 'to' to one allowed address, keep subject as a realistic email subject, "
        "and keep body_text as the email body."
    )


def _llm_replay_event(
    message: WhatIfLLMGeneratedMessage,
    *,
    manifest: WhatIfEpisodeManifest,
) -> BaseEvent:
    if message.surface == "slack":
        return BaseEvent(
            time_ms=message.delay_ms,
            actor_id=message.actor_id,
            channel="slack",
            type="counterfactual_chat",
            correlation_id=manifest.thread_id,
            payload={
                "channel": message.to,
                "text": message.body_text,
                "thread_ts": message.conversation_anchor or None,
                "user": message.actor_id,
            },
        )
    if message.surface == "tickets":
        return BaseEvent(
            time_ms=message.delay_ms,
            actor_id=message.actor_id,
            channel="tickets",
            type="counterfactual_ticket",
            correlation_id=manifest.thread_id,
            payload={
                "ticket_id": message.to,
                "comment": message.body_text,
                "author": message.actor_id,
            },
        )
    return BaseEvent(
        time_ms=message.delay_ms,
        actor_id=message.actor_id,
        channel="mail",
        type="counterfactual_email",
        correlation_id=manifest.thread_id,
        payload={
            "from": message.actor_id,
            "to": message.to,
            "subj": message.subject,
            "body_text": message.body_text,
            "thread_id": manifest.thread_id,
            "category": "counterfactual",
        },
    )


def _session_for_episode(
    root: Path,
    *,
    seed: int,
):
    bundle = load_customer_twin(root)
    asset_path = root / bundle.blueprint_asset_path
    asset = BlueprintAsset.model_validate_json(asset_path.read_text(encoding="utf-8"))
    return create_world_session_from_blueprint(asset, seed=seed)


def _coerce_episode_snapshot(
    *,
    snapshot: dict[str, Any] | None,
    context: dict[str, Any] | None,
) -> dict[str, Any]:
    if snapshot is not None:
        return snapshot
    if context is not None:
        return context
    raise ValueError("what-if episode is missing a saved context snapshot")


def _allowed_thread_participants(
    *,
    snapshot: dict[str, Any] | None = None,
    context: dict[str, Any] | None = None,
    manifest: WhatIfEpisodeManifest,
) -> tuple[list[str], list[str]]:
    _coerce_episode_snapshot(snapshot=snapshot, context=context)
    actors = sorted(
        {str(actor_id) for actor_id in manifest.actor_ids if str(actor_id).strip()}
    )
    recipients: set[str] = set(actors)
    for event in list(manifest.history_preview) + [manifest.branch_event]:
        if event.actor_id:
            recipients.add(event.actor_id)
        if event.target_id:
            recipients.add(event.target_id)
        for recipient in event.to_recipients:
            if recipient:
                recipients.add(recipient)
    if manifest.surface == "slack":
        recipients.add(_chat_channel_name_from_reference(manifest.branch_event))
    if manifest.surface == "tickets":
        recipients.add(manifest.thread_id.split(":", 1)[-1])
    return actors, sorted(recipients)


def _llm_counterfactual_prompt(
    *,
    snapshot: dict[str, Any] | None = None,
    context: dict[str, Any] | None = None,
    manifest: WhatIfEpisodeManifest,
    prompt: str,
    allowed_actors: Sequence[str],
    allowed_recipients: Sequence[str],
) -> str:
    _coerce_episode_snapshot(snapshot=snapshot, context=context)
    history_lines: list[str] = []
    for event in manifest.history_preview[-8:]:
        history_lines.append(_history_prompt_line(event))
    if not history_lines:
        history_lines.append(
            "- No earlier thread history was saved before this branch point."
        )
    surface_instructions = _llm_surface_instructions(manifest)
    prompt_lines = [
        f"Thread subject: {manifest.thread_subject}",
        f"Surface: {manifest.surface}",
        f"Case id: {manifest.case_id or manifest.thread_id}",
        f"Branch event id: {manifest.branch_event_id}",
        "Historical event being changed:",
        _history_prompt_line(manifest.branch_event),
    ]
    prompt_lines.extend(case_context_prompt_lines(manifest.case_context))
    prompt_lines.extend(situation_context_prompt_lines(manifest.situation_context))
    prompt_lines.extend(public_context_prompt_lines(manifest.public_context))
    prompt_lines.extend(
        [
            "Allowed actors:",
            ", ".join(allowed_actors),
            "Allowed targets:",
            ", ".join(allowed_recipients),
            "Historical thread so far:",
            "\n".join(history_lines[:8]),
            "Surface instructions:",
            surface_instructions,
            "Counterfactual prompt:",
            prompt,
            "Generate only what happens on this thread after the divergence.",
        ]
    )
    return "\n".join(prompt_lines)


def _normalize_llm_messages(
    plan_args: dict[str, Any],
    *,
    manifest: WhatIfEpisodeManifest,
    allowed_actors: Sequence[str],
    allowed_recipients: Sequence[str],
) -> tuple[list[WhatIfLLMGeneratedMessage], list[str]]:
    raw_messages = plan_args.get("messages", plan_args.get("emails", []))
    if not isinstance(raw_messages, list):
        raw_messages = []
    normalized: list[WhatIfLLMGeneratedMessage] = []
    raw_notes = plan_args.get("notes", [])
    notes = (
        [str(item) for item in raw_notes if str(item).strip()]
        if isinstance(raw_notes, list)
        else []
    )
    actor_fallback = (
        allowed_actors[0]
        if allowed_actors
        else _historical_archive_address(
            manifest.organization_domain,
            "counterfactual",
        )
    )
    recipient_fallback = _preferred_recipient_fallback(
        allowed_recipients,
        organization_domain=manifest.organization_domain,
        default=actor_fallback,
    )

    for index, raw in enumerate(raw_messages[:3]):
        if not isinstance(raw, dict):
            continue
        surface = (
            str(raw.get("surface", manifest.surface) or manifest.surface)
            .strip()
            .lower()
        )
        if surface != manifest.surface:
            surface = manifest.surface
            notes.append(
                f"Message {index + 1} used a different surface; it was clamped to {surface}."
            )
        actor_id = str(raw.get("actor_id", actor_fallback)).strip()
        if actor_id not in allowed_actors:
            resolved_actor = _resolve_allowed_identity(actor_id, allowed_actors)
            actor_id = resolved_actor or actor_fallback
            notes.append(
                f"Message {index + 1} used a non-participant actor; it was clamped to {actor_id}."
            )
        recipient = str(raw.get("to", recipient_fallback)).strip()
        if recipient not in allowed_recipients:
            resolved_recipient = _resolve_allowed_identity(
                recipient, allowed_recipients
            )
            recipient = resolved_recipient or recipient_fallback
            notes.append(
                f"Message {index + 1} used a non-thread recipient; it was clamped to {recipient}."
            )
        body_text = str(raw.get("body_text", "")).strip()
        if not body_text:
            continue
        delay_ms = max(1000, safe_int(raw.get("delay_ms", (index + 1) * 1000)))
        conversation_anchor = str(
            raw.get(
                "conversation_anchor",
                raw.get("thread_anchor", manifest.branch_event.conversation_anchor),
            )
            or ""
        ).strip()
        normalized.append(
            WhatIfLLMGeneratedMessage(
                actor_id=actor_id,
                surface=surface,
                to=recipient,
                subject=_message_subject(
                    raw.get("subject"),
                    fallback=manifest.thread_subject,
                ),
                body_text=body_text,
                delay_ms=delay_ms,
                conversation_anchor=conversation_anchor if surface == "slack" else "",
                rationale=str(raw.get("rationale", "")).strip(),
            )
        )
    return normalized, notes


def _message_subject(value: Any, *, fallback: str) -> str:
    subject = str(value or "").strip()
    if subject:
        return subject
    if fallback.lower().startswith("re:"):
        return fallback
    return f"Re: {fallback}"


def _preferred_recipient_fallback(
    recipients: Sequence[str],
    *,
    organization_domain: str,
    default: str,
) -> str:
    for recipient in recipients:
        if (
            recipient
            and organization_domain
            and recipient.lower().endswith(f"@{organization_domain.lower()}")
            and not recipient.lower().startswith("group:")
        ):
            return recipient
    return recipients[0] if recipients else default


def _resolve_allowed_identity(
    raw_value: str,
    allowed_values: Sequence[str],
) -> str | None:
    normalized = raw_value.strip().lower()
    if not normalized:
        return None
    for allowed in allowed_values:
        if normalized == allowed.lower():
            return allowed

    wanted_tokens = _identity_tokens(normalized)
    if not wanted_tokens:
        return None

    best_match: str | None = None
    best_score = 0
    for allowed in allowed_values:
        candidate_tokens = _identity_tokens(allowed.lower())
        overlap = len(wanted_tokens & candidate_tokens)
        if overlap == 0:
            continue
        if normalized in allowed.lower() or allowed.lower() in normalized:
            overlap += 2
        if overlap > best_score:
            best_match = allowed
            best_score = overlap
    return best_match


def _identity_tokens(value: str) -> set[str]:
    cleaned = (
        value.replace("@", " ")
        .replace(".", " ")
        .replace("_", " ")
        .replace("-", " ")
        .replace("<", " ")
        .replace(">", " ")
    )
    return {token for token in cleaned.split() if len(token) >= 2}


def _counterfactual_args(plan: dict[str, Any]) -> dict[str, Any]:
    raw_args = plan.get("args")
    if isinstance(raw_args, dict):
        return raw_args
    return plan


def _counterfactual_notes(plan_args: dict[str, Any]) -> list[str]:
    raw_notes = plan_args.get("notes", [])
    if not isinstance(raw_notes, list):
        return []
    return [str(item) for item in raw_notes if str(item).strip()]


def _apply_recipient_scope(
    recipients: Sequence[str],
    *,
    organization_domain: str,
    tags: set[str],
) -> tuple[list[str], list[str]]:
    result = [str(item).strip() for item in recipients if str(item).strip()]
    internal_recipients = [
        recipient
        for recipient in result
        if organization_domain
        and recipient.lower().endswith(f"@{organization_domain.lower()}")
    ]
    internal_only = bool(
        {
            "hold",
            "pause_forward",
            "external_removed",
            "attachment_removed",
            "legal",
            "compliance",
        }
        & tags
    )
    if not internal_only or not internal_recipients:
        return result, []
    note = "Recipient scope was clamped to internal participants on this archive."
    if organization_domain.strip().lower() == ENRON_DOMAIN:
        note = "Recipient scope was clamped to internal Enron participants."
    return (
        internal_recipients,
        [note],
    )


def _baseline_tick_ms(dataset_path: Path) -> int:
    dataset = VEIDataset.model_validate_json(dataset_path.read_text(encoding="utf-8"))
    if not dataset.events:
        return 0
    return max(event.time_ms for event in dataset.events) + 1000


def _forecast_summary_from_counts(forecast: WhatIfHistoricalScore) -> str:
    return (
        f"{forecast.future_event_count} follow-up events remain, with "
        f"{forecast.future_escalation_count} escalations and "
        f"{forecast.future_external_event_count} external sends."
    )


def _forecast_delta_summary(delta: WhatIfCounterfactualEstimateDelta) -> str:
    direction = (
        "down"
        if delta.risk_score_delta < 0
        else "up" if delta.risk_score_delta > 0 else "flat"
    )
    return (
        f"Predicted risk moves {direction} by {abs(delta.risk_score_delta):.3f}, "
        f"with escalation delta {delta.escalation_delta} and external-send delta "
        f"{delta.external_event_delta}."
    )


# ---------------------------------------------------------------------------
# Public entry points
# ---------------------------------------------------------------------------


def run_llm_counterfactual(
    root: str | Path,
    *,
    prompt: str,
    provider: str = "openai",
    model: str = default_model_for_provider("openai"),
    seed: int = 42042,
) -> WhatIfLLMReplayResult:
    load_dotenv(override=True)
    workspace_root = Path(root).expanduser().resolve()
    manifest = load_episode_manifest(workspace_root)
    snapshot = _load_episode_snapshot(workspace_root)
    session = _session_for_episode(workspace_root, seed=seed)
    allowed_actors, allowed_recipients = _allowed_thread_participants(
        snapshot=snapshot,
        manifest=manifest,
    )
    recipient_scope, recipient_notes = _apply_recipient_scope(
        allowed_recipients,
        organization_domain=manifest.organization_domain,
        tags=intervention_tags(prompt),
    )
    system = (
        "You are simulating a bounded counterfactual continuation on a historical "
        "enterprise thread. Return strict JSON with keys tool and args. "
        "Use tool='emit_counterfactual'. In args, include summary, notes, and "
        "messages. messages must be a list of 1 to 3 objects with actor_id, surface, "
        "to, subject, body_text, delay_ms, rationale, and optional conversation_anchor. "
        "Only use the listed actors and allowed targets. Keep messages plausible, concise, and clearly tied "
        "to the intervention prompt."
    )
    user = _llm_counterfactual_prompt(
        snapshot=snapshot,
        manifest=manifest,
        prompt=prompt,
        allowed_actors=allowed_actors,
        allowed_recipients=recipient_scope,
    )
    timeout_s = _llm_timeout_seconds()
    try:
        response = _run_async(
            providers.plan_once_with_usage(
                provider=provider,
                model=model,
                system=system,
                user=user,
                timeout_s=timeout_s,
            )
        )
        messages, notes = _normalize_llm_messages(
            _counterfactual_args(response.plan),
            manifest=manifest,
            allowed_actors=allowed_actors,
            allowed_recipients=recipient_scope,
        )
        if not messages:
            raise ValueError("LLM returned no usable messages")
    except _COUNTERFACTUAL_FAILURES as exc:
        logger.warning(
            "whatif llm counterfactual generation failed for %s (%s)",
            workspace_root,
            type(exc).__name__,
            extra={
                "source": "counterfactual",
                "provider": provider,
                "file_path": str(workspace_root),
                "exception_type": type(exc).__name__,
            },
            exc_info=True,
        )
        if isinstance(exc, asyncio.TimeoutError):
            summary_text = (
                f"LLM counterfactual generation timed out after {timeout_s}s. "
                f"Increase VEI_WHATIF_LLM_TIMEOUT_S or pick a faster model."
            )
        else:
            summary_text = (
                f"LLM counterfactual generation failed: {type(exc).__name__}."
            )
        return WhatIfLLMReplayResult(
            status="error",
            provider=provider,
            model=model,
            prompt=prompt,
            summary=summary_text,
            error=str(exc) or type(exc).__name__,
            notes=["The forecast path can still be used without live LLM output."],
        )

    max_delay = max(message.delay_ms for message in messages)
    replay_result = session.replay(
        mode="overlay",
        dataset_events=[
            _llm_replay_event(message, manifest=manifest) for message in messages
        ],
    )
    tick_result = session.router.tick(dt_ms=max_delay + 1000)
    inbox_count = 0
    top_subjects: list[str] = []
    if manifest.surface == "mail":
        inbox = session.call_tool("mail.list", {})
        inbox_count = len(inbox)
        top_subjects = [
            str(item.get("subj", ""))
            for item in inbox[:5]
            if isinstance(item, dict) and item.get("subj")
        ]
    plan_args = _counterfactual_args(response.plan)
    summary = str(plan_args.get("summary", "") or "").strip()
    if not summary:
        summary = (
            f"{len(messages)} counterfactual actions were generated across "
            f"{len({message.actor_id for message in messages})} participants."
        )
    return WhatIfLLMReplayResult(
        status="ok",
        provider=provider,
        model=model,
        prompt=prompt,
        summary=summary,
        messages=messages,
        usage=WhatIfLLMUsage(
            provider=response.usage.provider,
            model=response.usage.model,
            prompt_tokens=response.usage.prompt_tokens,
            completion_tokens=response.usage.completion_tokens,
            total_tokens=response.usage.total_tokens,
            estimated_cost_usd=response.usage.estimated_cost_usd,
        ),
        scheduled_event_count=int(replay_result.get("scheduled", 0)),
        delivered_event_count=sum(tick_result.get("delivered", {}).values()),
        inbox_count=inbox_count,
        top_subjects=top_subjects,
        notes=recipient_notes + notes + _counterfactual_notes(plan_args),
    )


def estimate_counterfactual_delta(
    root: str | Path,
    *,
    prompt: str,
) -> WhatIfCounterfactualEstimateResult:
    workspace_root = Path(root).expanduser().resolve()
    manifest = load_episode_manifest(workspace_root)
    baseline = manifest.forecast.model_copy(deep=True)
    predicted = manifest.forecast.model_copy(
        update={"backend": "heuristic_baseline"},
        deep=True,
    )
    tags = intervention_tags(prompt)
    notes: list[str] = []

    event_shift = 0
    escalation_shift = 0
    assignment_shift = 0
    approval_shift = 0
    external_shift = 0
    risk_shift = 0.0

    if {"legal", "compliance"} & tags:
        escalation_shift -= max(1, predicted.future_escalation_count // 2)
        approval_shift += 1
        risk_shift -= 0.18
        notes.append("Compliance involvement reduces uncontrolled escalation.")
    if {"hold", "pause_forward"} & tags:
        external_shift -= max(1, predicted.future_external_event_count)
        event_shift -= max(0, predicted.future_event_count // 3)
        risk_shift -= 0.2
        notes.append("Holding or pausing the thread cuts external exposure.")
    if {"reply_immediately", "clarify_owner"} & tags:
        event_shift -= 1
        assignment_shift -= max(0, predicted.future_assignment_count // 2)
        risk_shift -= 0.12
        notes.append("Fast clarification usually shortens the follow-up tail.")
    if "status_only" in tags:
        external_shift -= max(1, predicted.future_external_event_count // 4)
        event_shift -= max(0, predicted.future_event_count // 8)
        risk_shift -= 0.08
        notes.append(
            "A status-only outside note reduces document exposure while keeping contact warm."
        )
    if {"executive_gate"} & tags:
        escalation_shift -= max(1, predicted.future_escalation_count // 2)
        approval_shift += 1
        risk_shift -= 0.14
        notes.append("Routing through an executive gate lowers escalation spread.")
    if "attachment_removed" in tags and "external_removed" not in tags:
        risk_shift -= 0.16
        notes.append("Keeping the attachment inside lowers sharing risk.")
    if "external_removed" in tags:
        if predicted.future_external_event_count > 0:
            external_shift -= predicted.future_external_event_count
            risk_shift -= 0.24
            notes.append("Removing the outside recipient sharply lowers leak risk.")
        else:
            notes.append(
                "The recorded path already stays internal, so removing outside recipients changes little."
            )
    if {"send_now", "widen_loop"} & tags:
        event_shift += max(1, predicted.future_event_count // 12)
        assignment_shift += max(1, max(predicted.future_assignment_count, 1) // 8)
        external_shift += max(1, max(predicted.future_external_event_count, 1) // 6)
        risk_shift += 0.12
        notes.append(
            "Keeping the outside loop active increases spread and coordination pressure."
        )

    predicted.future_event_count = max(0, predicted.future_event_count + event_shift)
    predicted.future_escalation_count = max(
        0,
        predicted.future_escalation_count + escalation_shift,
    )
    predicted.future_assignment_count = max(
        0,
        predicted.future_assignment_count + assignment_shift,
    )
    predicted.future_approval_count = max(
        0,
        predicted.future_approval_count + approval_shift,
    )
    predicted.future_external_event_count = max(
        0,
        predicted.future_external_event_count + external_shift,
    )
    predicted.risk_score = round(
        max(0.0, min(1.0, predicted.risk_score + risk_shift)),
        3,
    )
    predicted.summary = _forecast_summary_from_counts(predicted)

    delta = WhatIfCounterfactualEstimateDelta(
        risk_score_delta=round(predicted.risk_score - baseline.risk_score, 3),
        future_event_delta=predicted.future_event_count - baseline.future_event_count,
        escalation_delta=(
            predicted.future_escalation_count - baseline.future_escalation_count
        ),
        assignment_delta=(
            predicted.future_assignment_count - baseline.future_assignment_count
        ),
        approval_delta=predicted.future_approval_count - baseline.future_approval_count,
        external_event_delta=(
            predicted.future_external_event_count - baseline.future_external_event_count
        ),
    )
    result = WhatIfCounterfactualEstimateResult(
        status="ok",
        backend="heuristic_baseline",
        prompt=prompt,
        summary=_forecast_delta_summary(delta),
        baseline=baseline,
        predicted=predicted,
        delta=delta,
        notes=notes
        or [
            "No specific intervention tags were detected; forecast remained close to baseline."
        ],
    )
    return _attach_business_state_to_forecast_result(
        result,
        branch_event=manifest.branch_event,
        organization_domain=manifest.organization_domain,
        public_context=manifest.public_context,
    )


def _attach_business_state_to_forecast_result(
    forecast_result: WhatIfCounterfactualEstimateResult,
    *,
    branch_event: WhatIfEventReference | None,
    organization_domain: str,
    public_context: WhatIfPublicContext | None,
) -> WhatIfCounterfactualEstimateResult:
    if branch_event is None or forecast_result.status != "ok":
        return forecast_result
    forecast_result = attach_macro_outcomes_to_forecast_result(
        forecast_result,
        organization_domain=organization_domain,
        branch_timestamp=branch_event.timestamp,
        public_context=public_context,
        capability_note=(
            "This E-JEPA checkpoint does not emit macro outcome heads yet, so "
            "stock, credit, and FERC predictions are left empty for this run."
            if forecast_result.backend == "e_jepa"
            else None
        ),
    )
    forecast_result.business_state_change = describe_forecast_business_change(
        branch_event=branch_event,
        forecast_result=forecast_result,
        organization_domain=organization_domain,
        public_context=public_context,
    )
    return forecast_result
