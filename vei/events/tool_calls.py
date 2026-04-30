"""Canonical event builders for agent/tool activity."""

from __future__ import annotations

import json
from hashlib import sha256
from typing import Any, Literal

from pydantic import ValidationError

from .api import build_event, emit_event
from .context import EventContext, merge_event_context
from .links import EventLink, merge_event_links
from .models import (
    ActorRef,
    CanonicalEvent,
    EventDomain,
    EventProvenance,
    InternalExternal,
    ObjectRef,
    TextHandle,
)

ToolCallErrorClass = Literal["timeout", "validation", "permission", "infra", "unknown"]


def classify_tool_call_failure(exc: BaseException) -> ToolCallErrorClass:
    """Map execution failures onto a coarse error class for ``tool.call.failed`` deltas."""

    if isinstance(exc, ValidationError):
        return "validation"
    try:
        import asyncio

        if isinstance(exc, asyncio.TimeoutError):
            return "timeout"
    except Exception:
        pass
    if isinstance(exc, TimeoutError):
        return "timeout"

    exc_type = exc.__class__
    exc_module = getattr(exc_type, "__module__", "") or ""

    if exc_type.__name__ == "ConnectorInvocationError" and exc_module.endswith(
        "connectors.models",
    ):
        status_code = int(getattr(exc, "status_code", 400) or 400)
        if status_code in (401, 403):
            return "permission"
        if status_code >= 500:
            return "infra"
        return "infra"

    if exc_type.__name__ == "MCPError" and exc_module == "vei.router.errors":
        lowered = str(getattr(exc, "code", "") or "").lower()
        if any(
            needle in lowered
            for needle in ("permission", "denied", "blocked", "unauthorized")
        ):
            return "permission"
        if any(
            needle in lowered
            for needle in (
                "unavailable",
                "fault",
                "timeout",
                "rate",
                "quota",
                "retry",
                "upstream",
                "infra",
                "connector",
                "network",
                "temporary",
                "503",
                "502",
            )
        ):
            return "infra"
        return "validation"

    cls_name = exc_type.__name__
    if "Timeout" in cls_name:
        return "timeout"
    if isinstance(exc, (BrokenPipeError, ConnectionError)):
        return "infra"
    if isinstance(exc, (ValueError, TypeError, KeyError)):
        return "validation"

    return "unknown"


def payload_handle(payload: Any, *, store_uri: str = "") -> TextHandle | None:
    if payload is None:
        return None
    text = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)
    return TextHandle.from_text(text, store_uri=store_uri)


def stable_event_id(*parts: str) -> str:
    basis = "|".join(str(part) for part in parts if part is not None)
    return f"evt_{sha256(basis.encode('utf-8')).hexdigest()[:32]}"


def _tool_domain(tool_name: str) -> EventDomain:
    if tool_name.startswith(("slack.", "mail.", "calendar.")):
        return EventDomain.COMM_GRAPH
    if tool_name.startswith(("docs.", "browser.")):
        return EventDomain.DOC_GRAPH
    if tool_name.startswith(("tickets.", "jira.", "service_ops.")):
        return EventDomain.WORK_GRAPH
    if tool_name.startswith(("crm.", "salesforce.")):
        return EventDomain.REVENUE_GRAPH
    if tool_name.startswith(("db.", "database.", "warehouse.")):
        return EventDomain.DATA_GRAPH
    if tool_name.startswith(("okta.", "google_admin.", "hris.")):
        return EventDomain.IDENTITY_GRAPH
    if tool_name.startswith(("siem.", "datadog.", "pagerduty.")):
        return EventDomain.OBS_GRAPH
    return EventDomain.INTERNAL


def build_tool_call_event(
    *,
    kind: str,
    tool_name: str,
    event_id: str | None = None,
    tenant_id: str = "",
    case_id: str | None = None,
    ts_ms: int = 0,
    actor_ref: ActorRef | None = None,
    object_refs: list[ObjectRef] | None = None,
    args: Any = None,
    response: Any = None,
    status: str = "",
    error: str = "",
    latency_ms: int | None = None,
    source_id: str = "",
    source_granularity: str = "per_call",
    provenance_origin: EventProvenance = EventProvenance.SIMULATED,
    link_refs: list[str] | None = None,
    links: list[EventLink | dict[str, Any]] | None = None,
    context: EventContext | dict[str, Any] | None = None,
    inline_payload: bool = False,
    error_class: ToolCallErrorClass | None = None,
) -> CanonicalEvent:
    delta_data: dict[str, Any] = {
        "tool_name": tool_name,
        "status": status,
        "source_granularity": source_granularity,
    }
    if error:
        delta_data["error"] = error
    if latency_ms is not None:
        delta_data["latency_ms"] = latency_ms
    if error_class:
        delta_data["error_class"] = error_class
    if inline_payload:
        delta_data["args"] = args
        delta_data["response"] = response
    else:
        if args is not None:
            handle = payload_handle(args, store_uri=f"payload://{source_id}/args")
            if handle is not None:
                delta_data["args_handle"] = handle.model_dump(mode="json")
        if response is not None:
            handle = payload_handle(
                response, store_uri=f"payload://{source_id}/response"
            )
            if handle is not None:
                delta_data["response_handle"] = handle.model_dump(mode="json")
    delta_data = merge_event_links(delta_data, links=links, link_refs=link_refs)
    delta_data = merge_event_context(delta_data, context)

    return build_event(
        event_id=event_id,
        domain=_tool_domain(tool_name),
        kind=kind,
        tenant_id=tenant_id,
        case_id=case_id,
        ts_ms=ts_ms,
        actor_ref=actor_ref,
        object_refs=object_refs or [],
        internal_external=InternalExternal.INTERNAL,
        provenance_origin=provenance_origin,
        provenance_source_id=source_id,
        text_handle=payload_handle(args, store_uri=f"payload://{source_id}/args"),
        delta_data=delta_data,
    ).with_hash()


def emit_tool_requested(**kwargs: Any) -> CanonicalEvent:
    return emit_event(build_tool_call_event(kind="tool.call.requested", **kwargs))


def emit_tool_completed(**kwargs: Any) -> CanonicalEvent:
    return emit_event(build_tool_call_event(kind="tool.call.completed", **kwargs))


def emit_tool_failed(**kwargs: Any) -> CanonicalEvent:
    return emit_event(build_tool_call_event(kind="tool.call.failed", **kwargs))
