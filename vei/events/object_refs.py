"""Object reference extraction helpers for tool provenance."""

from __future__ import annotations

import hashlib
from typing import Any

from .models import ObjectRef


def parse_object_refs(value: Any) -> list[ObjectRef]:
    if not isinstance(value, list):
        return []
    refs: list[ObjectRef] = []
    for item in value:
        if isinstance(item, ObjectRef):
            refs.append(item)
        elif isinstance(item, dict):
            try:
                refs.append(ObjectRef.model_validate(item))
            except ValueError:
                continue
    return refs


def extract_object_refs(
    *,
    tool_name: str,
    args: Any = None,
    response: Any = None,
    explicit_refs: Any = None,
) -> list[ObjectRef]:
    refs = parse_object_refs(explicit_refs)
    payloads = [item for item in (args, response) if isinstance(item, dict)]

    def add(object_id: Any, *, domain: str, kind: str, label: str = "") -> None:
        if object_id in {None, ""}:
            return
        ref = ObjectRef(
            object_id=str(object_id),
            domain=domain,
            kind=kind,
            label=label or str(object_id),
        )
        if all(
            (ref.object_id, ref.domain, ref.kind)
            != (existing.object_id, existing.domain, existing.kind)
            for existing in refs
        ):
            refs.append(ref)

    for payload in payloads:
        add(
            _first(payload, "doc_id", "document_id", "file_id"),
            domain="doc_graph",
            kind="document",
        )
        add(
            _first(payload, "channel", "channel_id"),
            domain="comm_graph",
            kind="channel",
        )
        add(
            _first(payload, "thread_id", "thread_ts"),
            domain="comm_graph",
            kind="thread",
        )
        add(
            _first(payload, "message_id", "email_id"),
            domain="comm_graph",
            kind="message",
        )
        add(
            _first(payload, "ticket_id", "issue_id"), domain="work_graph", kind="ticket"
        )
        add(_first(payload, "account_id"), domain="revenue_graph", kind="account")
        add(_first(payload, "contact_id"), domain="revenue_graph", kind="contact")
        add(_first(payload, "table", "table_name"), domain="data_graph", kind="table")
        add(
            _first(payload, "resource_uri", "uri"), domain="data_graph", kind="resource"
        )
        if "q" in payload and tool_name.startswith(("mail.search", "docs.search")):
            digest = hashlib.sha256(str(payload["q"]).encode("utf-8")).hexdigest()[:16]
            add(f"query:{digest}", domain="comm_graph", kind="search")

    if tool_name.startswith("docs.") and not any(
        ref.domain == "doc_graph" for ref in refs
    ):
        for payload in payloads:
            add(_first(payload, "id"), domain="doc_graph", kind="document")
    return refs


def _first(payload: dict[str, Any], *keys: str) -> Any:
    for key in keys:
        if key in payload and payload[key] not in {None, ""}:
            return payload[key]
    return None
