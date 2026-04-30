"""Canonical event builders for LLM calls and aggregate usage evidence."""

from __future__ import annotations

from typing import Any

from .api import build_event, emit_event
from .models import ActorRef, CanonicalEvent, EventDomain, EventProvenance, TextHandle
from .tool_calls import stable_event_id


def _text_handle(value: str | None, *, store_uri: str = "") -> TextHandle | None:
    if value is None:
        return None
    return TextHandle.from_text(str(value), store_uri=store_uri)


def build_llm_call_event(
    *,
    kind: str,
    provider: str,
    model: str,
    event_id: str | None = None,
    tenant_id: str = "",
    case_id: str | None = None,
    ts_ms: int = 0,
    actor_ref: ActorRef | None = None,
    prompt: str | None = None,
    response: str | None = None,
    status: str = "",
    error: str = "",
    prompt_tokens: int | None = None,
    completion_tokens: int | None = None,
    total_tokens: int | None = None,
    cost_usd: float | None = None,
    latency_ms: int | None = None,
    source_id: str = "",
    source_granularity: str = "per_call",
    provenance_origin: EventProvenance = EventProvenance.SIMULATED,
    link_refs: list[str] | None = None,
) -> CanonicalEvent:
    delta_data: dict[str, Any] = {
        "provider": provider,
        "model": model,
        "status": status,
        "source_granularity": source_granularity,
        "link_refs": list(link_refs or []),
    }
    for key, value in {
        "error": error,
        "prompt_tokens": prompt_tokens,
        "completion_tokens": completion_tokens,
        "total_tokens": total_tokens,
        "cost_usd": cost_usd,
        "latency_ms": latency_ms,
    }.items():
        if value not in {None, ""}:
            delta_data[key] = value
    prompt_handle = _text_handle(prompt, store_uri=f"payload://{source_id}/prompt")
    response_handle = _text_handle(
        response, store_uri=f"payload://{source_id}/response"
    )
    if prompt_handle is not None:
        delta_data["prompt_handle"] = prompt_handle.model_dump(mode="json")
    if response_handle is not None:
        delta_data["response_handle"] = response_handle.model_dump(mode="json")
    return build_event(
        event_id=event_id,
        domain=EventDomain.OBS_GRAPH,
        kind=kind,
        tenant_id=tenant_id,
        case_id=case_id,
        ts_ms=ts_ms,
        actor_ref=actor_ref,
        provenance_origin=provenance_origin,
        provenance_source_id=source_id,
        text_handle=prompt_handle,
        delta_data=delta_data,
    ).with_hash()


def build_llm_usage_observed(
    *,
    provider: str,
    model: str = "",
    event_id: str | None = None,
    tenant_id: str = "",
    ts_ms: int = 0,
    source_id: str = "",
    bucket_start: str = "",
    bucket_end: str = "",
    usage: dict[str, Any] | None = None,
) -> CanonicalEvent:
    return build_event(
        event_id=event_id,
        domain=EventDomain.OBS_GRAPH,
        kind="llm.usage.observed",
        tenant_id=tenant_id,
        ts_ms=ts_ms,
        provenance_origin=EventProvenance.IMPORTED,
        provenance_source_id=source_id,
        delta_data={
            "provider": provider,
            "model": model,
            "bucket_start": bucket_start,
            "bucket_end": bucket_end,
            "usage": usage or {},
            "source_granularity": "aggregate",
            "link_refs": [],
        },
    ).with_hash()


def emit_llm_call_started(**kwargs: Any) -> CanonicalEvent:
    return emit_event(build_llm_call_event(kind="llm.call.started", **kwargs))


def emit_llm_call_completed(**kwargs: Any) -> CanonicalEvent:
    return emit_event(build_llm_call_event(kind="llm.call.completed", **kwargs))


def emit_llm_call_failed(**kwargs: Any) -> CanonicalEvent:
    return emit_event(build_llm_call_event(kind="llm.call.failed", **kwargs))


def emit_llm_usage_observed(**kwargs: Any) -> CanonicalEvent:
    return emit_event(build_llm_usage_observed(**kwargs))


__all__ = [
    "build_llm_call_event",
    "build_llm_usage_observed",
    "emit_llm_call_completed",
    "emit_llm_call_failed",
    "emit_llm_call_started",
    "emit_llm_usage_observed",
    "stable_event_id",
]
