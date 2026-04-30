"""OpenTelemetry GenAI/MCP-shaped export for VEI provenance events."""

from __future__ import annotations

from typing import Any

from vei.events.api import CanonicalEvent

from . import register_exporter


def _attrs(event: CanonicalEvent) -> dict[str, Any]:
    data = event.delta.data if event.delta is not None else {}
    attrs: dict[str, Any] = {
        "vei.event_id": event.event_id,
        "vei.event_hash": event.hash,
        "vei.case_id": event.case_id or "",
        "vei.source_id": event.provenance.source_id,
        "vei.source_granularity": data.get("source_granularity", ""),
        "event.name": event.kind,
    }
    if event.kind.startswith("llm."):
        attrs["gen_ai.system"] = data.get("provider", "")
        attrs["gen_ai.request.model"] = data.get("model", "")
        attrs["gen_ai.usage.input_tokens"] = data.get("prompt_tokens", 0)
        attrs["gen_ai.usage.output_tokens"] = data.get("completion_tokens", 0)
    if event.kind.startswith("tool.call."):
        attrs["mcp.tool.name"] = data.get("tool_name", "")
        attrs["mcp.tool.status"] = data.get("status", "")
    if event.kind.startswith("governance."):
        attrs["vei.policy.decision"] = data.get("decision", "")
        attrs["vei.policy.reason"] = data.get("reason", "")
    return attrs


def export_otel_genai(events: list[CanonicalEvent]) -> dict[str, Any]:
    spans = []
    for event in events:
        spans.append(
            {
                "name": event.kind,
                "startTimeUnixNano": str(max(event.ts_ms, 0) * 1_000_000),
                "endTimeUnixNano": str(max(event.ts_ms, 0) * 1_000_000),
                "attributes": [
                    {"key": key, "value": {"stringValue": str(value)}}
                    for key, value in sorted(_attrs(event).items())
                ],
            }
        )
    return {"resourceSpans": [{"scopeSpans": [{"spans": spans}]}]}


register_exporter("otel", export_otel_genai)
