"""Object reference extraction helpers for tool provenance."""

from __future__ import annotations

import hashlib
from typing import Any, Protocol

from .models import ObjectRef


class ObjectRefExtractor(Protocol):
    def __call__(
        self, *, tool_name: str, payloads: list[dict[str, Any]]
    ) -> list[ObjectRef]: ...


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
        _append_ref(refs, object_id, domain=domain, kind=kind, label=label)

    for extractor in _EXTRACTORS:
        for ref in extractor(tool_name=tool_name, payloads=payloads):
            _append_ref(
                refs,
                ref.object_id,
                domain=ref.domain,
                kind=ref.kind,
                label=ref.label,
            )

    # Generic fallback for logs whose connector family is unknown.
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


def _append_ref(
    refs: list[ObjectRef],
    object_id: Any,
    *,
    domain: str,
    kind: str,
    label: str = "",
) -> None:
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


def _first(payload: dict[str, Any], *keys: str) -> Any:
    for key in keys:
        if key in payload and payload[key] not in {None, ""}:
            return payload[key]
    return None


def _connector_extractor(
    prefixes: tuple[str, ...],
    mappings: tuple[tuple[tuple[str, ...], str, str], ...],
) -> ObjectRefExtractor:
    def extract(*, tool_name: str, payloads: list[dict[str, Any]]) -> list[ObjectRef]:
        if not tool_name.startswith(prefixes):
            return []
        refs: list[ObjectRef] = []
        for payload in payloads:
            for keys, domain, kind in mappings:
                _append_ref(refs, _first(payload, *keys), domain=domain, kind=kind)
        return refs

    return extract


def _mail_search_extractor(
    *, tool_name: str, payloads: list[dict[str, Any]]
) -> list[ObjectRef]:
    if not tool_name.startswith(("mail.search", "gmail.search", "docs.search")):
        return []
    refs: list[ObjectRef] = []
    for payload in payloads:
        query = _first(payload, "q", "query", "search")
        if query:
            digest = hashlib.sha256(str(query).encode("utf-8")).hexdigest()[:16]
            domain = "doc_graph" if tool_name.startswith("docs.") else "comm_graph"
            _append_ref(refs, f"query:{digest}", domain=domain, kind="search")
    return refs


_EXTRACTORS: tuple[ObjectRefExtractor, ...] = (
    _connector_extractor(
        ("docs.", "drive.", "google_drive."),
        (
            (("doc_id", "document_id", "file_id", "id"), "doc_graph", "document"),
            (("folder_id",), "doc_graph", "folder"),
        ),
    ),
    _connector_extractor(
        ("slack.",),
        (
            (("channel", "channel_id"), "comm_graph", "channel"),
            (("thread_id", "thread_ts"), "comm_graph", "thread"),
            (("message_id", "ts"), "comm_graph", "message"),
        ),
    ),
    _connector_extractor(
        ("mail.", "gmail."),
        (
            (("message_id", "email_id", "id"), "comm_graph", "message"),
            (("thread_id",), "comm_graph", "thread"),
            (("mailbox", "folder"), "comm_graph", "mailbox"),
        ),
    ),
    _connector_extractor(
        ("jira.", "tickets.", "linear.", "servicedesk."),
        (
            (("ticket_id", "issue_id", "key", "id"), "work_graph", "ticket"),
            (("project_id", "project_key"), "work_graph", "project"),
        ),
    ),
    _connector_extractor(
        ("salesforce.", "crm."),
        (
            (("account_id",), "revenue_graph", "account"),
            (("contact_id",), "revenue_graph", "contact"),
            (("opportunity_id",), "revenue_graph", "opportunity"),
        ),
    ),
    _connector_extractor(
        ("db.", "database.", "snowflake.", "bigquery.", "postgres."),
        (
            (("table", "table_name"), "data_graph", "table"),
            (("database", "database_name"), "data_graph", "database"),
            (("schema", "schema_name"), "data_graph", "schema"),
        ),
    ),
    _connector_extractor(
        ("mcp.", "resource."),
        ((("resource_uri", "uri"), "data_graph", "resource"),),
    ),
    _mail_search_extractor,
)
