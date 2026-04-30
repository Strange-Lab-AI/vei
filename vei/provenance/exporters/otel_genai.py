"""OpenTelemetry GenAI/MCP-shaped export for VEI provenance events."""

from __future__ import annotations

import hashlib
from typing import Any

from vei.events.api import CanonicalEvent, link_event_ids

from . import register_exporter


def _delta(event: CanonicalEvent) -> dict[str, Any]:
    return event.delta.data if event.delta is not None else {}


def _context(event: CanonicalEvent) -> dict[str, Any]:
    context = _delta(event).get("context", {})
    return context if isinstance(context, dict) else {}


def _attrs(event: CanonicalEvent) -> dict[str, Any]:
    data = _delta(event)
    context = _context(event)
    attrs: dict[str, Any] = {
        "vei.event_id": event.event_id,
        "vei.event_hash": event.hash,
        "vei.case_id": event.case_id or "",
        "vei.source_id": event.provenance.source_id,
        "vei.source_granularity": data.get(
            "source_granularity", context.get("source_granularity", "")
        ),
        "event.name": event.kind,
    }
    for key in (
        "workspace_id",
        "run_id",
        "agent_id",
        "human_user_id",
        "mcp_session_id",
        "mcp_client_id",
        "mcp_server_id",
        "mcp_protocol_version",
        "mcp_transport",
        "jsonrpc_request_id",
    ):
        if context.get(key):
            attrs[f"vei.context.{key}"] = context[key]
    if event.kind.startswith("llm."):
        attrs["gen_ai.operation.name"] = (
            "chat" if event.kind.startswith("llm.call.") else "usage"
        )
        attrs["gen_ai.system"] = data.get("provider", "")
        attrs["gen_ai.request.model"] = data.get("model", "")
        attrs["gen_ai.usage.input_tokens"] = data.get("prompt_tokens", 0)
        attrs["gen_ai.usage.output_tokens"] = data.get("completion_tokens", 0)
    if event.kind.startswith("tool.call."):
        attrs["gen_ai.operation.name"] = "execute_tool"
        attrs["gen_ai.tool.name"] = data.get("tool_name", "")
        attrs["mcp.method.name"] = context.get("mcp_method_name", "tools/call")
        attrs["jsonrpc.request.id"] = context.get("jsonrpc_request_id", "")
        attrs["mcp.session.id"] = context.get("mcp_session_id", "")
        attrs["mcp.protocol.version"] = context.get("mcp_protocol_version", "")
        attrs["network.transport"] = context.get("mcp_transport", "")
        attrs["mcp.tool.status"] = data.get("status", "")
    if event.kind.startswith("governance."):
        attrs["vei.policy.decision"] = data.get("decision", "")
        attrs["vei.policy.reason"] = data.get("reason", "")
    if data.get("error"):
        attrs["error.type"] = data.get("error")
    return attrs


def _otel_value(value: Any) -> dict[str, Any]:
    if isinstance(value, bool):
        return {"boolValue": value}
    if isinstance(value, int) and not isinstance(value, bool):
        return {"intValue": str(value)}
    if isinstance(value, float):
        return {"doubleValue": value}
    return {"stringValue": str(value)}


def _trace_id(event: CanonicalEvent) -> str:
    context = _context(event)
    raw = str(context.get("trace_id") or event.case_id or event.event_id)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:32]


def _span_id(event_id: str) -> str:
    return hashlib.sha256(event_id.encode("utf-8")).hexdigest()[:16]


def _parent_span_id(event: CanonicalEvent) -> str:
    context = _context(event)
    parent_event_id = str(context.get("parent_event_id", ""))
    if not parent_event_id:
        linked_ids = link_event_ids(_delta(event))
        parent_event_id = linked_ids[0] if linked_ids else ""
    return _span_id(parent_event_id) if parent_event_id else ""


def export_otel_genai(events: list[CanonicalEvent]) -> dict[str, Any]:
    spans = []
    first_context = _context(events[0]) if events else {}
    resource_attrs = [
        {"key": "service.name", "value": {"stringValue": "vei"}},
        {"key": "deployment.environment", "value": {"stringValue": "local"}},
    ]
    if first_context.get("workspace_id"):
        resource_attrs.append(
            {
                "key": "vei.workspace_id",
                "value": {"stringValue": str(first_context["workspace_id"])},
            }
        )
    for event in events:
        data = _delta(event)
        end_ms = max(event.ts_ms + int(data.get("latency_ms", 0) or 0), event.ts_ms)
        span: dict[str, Any] = {
            "traceId": _trace_id(event),
            "spanId": _span_id(event.event_id),
            "name": event.kind,
            "kind": 1,
            "startTimeUnixNano": str(max(event.ts_ms, 0) * 1_000_000),
            "endTimeUnixNano": str(max(end_ms, 0) * 1_000_000),
            "attributes": [
                {"key": key, "value": _otel_value(value)}
                for key, value in sorted(_attrs(event).items())
                if value not in {None, ""}
            ],
        }
        parent_span_id = _parent_span_id(event)
        if parent_span_id:
            span["parentSpanId"] = parent_span_id
        if event.kind.endswith(".failed") or data.get("error"):
            span["status"] = {"code": 2, "message": str(data.get("error", ""))}
        else:
            span["status"] = {"code": 1}
        spans.append(span)
    return {
        "resourceSpans": [
            {
                "resource": {"attributes": resource_attrs},
                "scopeSpans": [
                    {
                        "scope": {
                            "name": "vei.provenance.otel_genai",
                            "version": "1",
                        },
                        "spans": spans,
                    }
                ],
            }
        ]
    }


register_exporter("otel", export_otel_genai)
