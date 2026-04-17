"""Legacy adapters for reading pre-canonical event formats.

Reads the existing ``events.jsonl`` (StateStore) and saved whatif bundles and
emits ``CanonicalEvent`` v1 envelopes.  ``delta_schema_version = 0`` (opaque
dict) is used for all legacy payloads since the domain-specific delta shapes
were not frozen before v1.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, Iterator, List

from .models import (
    CanonicalEvent,
    EventDomain,
    EventProvenance,
    ProvenanceRecord,
    StateDelta,
)

_KIND_DOMAIN_MAP: Dict[str, EventDomain] = {
    "mail": EventDomain.COMM_GRAPH,
    "slack": EventDomain.COMM_GRAPH,
    "chat": EventDomain.COMM_GRAPH,
    "calendar": EventDomain.COMM_GRAPH,
    "tickets": EventDomain.WORK_GRAPH,
    "servicedesk": EventDomain.WORK_GRAPH,
    "docs": EventDomain.DOC_GRAPH,
    "browser": EventDomain.DOC_GRAPH,
    "identity": EventDomain.IDENTITY_GRAPH,
    "okta": EventDomain.IDENTITY_GRAPH,
    "crm": EventDomain.REVENUE_GRAPH,
    "knowledge": EventDomain.KNOWLEDGE_GRAPH,
    "siem": EventDomain.OBS_GRAPH,
    "datadog": EventDomain.OBS_GRAPH,
    "pagerduty": EventDomain.OBS_GRAPH,
    "erp": EventDomain.OPS_GRAPH,
    "feature_flags": EventDomain.OPS_GRAPH,
    "db": EventDomain.DATA_GRAPH,
    "spreadsheet": EventDomain.DATA_GRAPH,
}


def _infer_domain(kind: str, payload: Dict[str, Any]) -> EventDomain:
    target = payload.get("target", "")
    if target and target in _KIND_DOMAIN_MAP:
        return _KIND_DOMAIN_MAP[target]
    prefix = kind.split(".")[0] if "." in kind else kind
    return _KIND_DOMAIN_MAP.get(prefix, EventDomain.INTERNAL)


def legacy_event_to_canonical(
    raw: Dict[str, Any],
    *,
    tenant_id: str = "",
) -> CanonicalEvent:
    """Convert one legacy StateStore event dict to a CanonicalEvent."""
    kind = str(raw.get("kind", ""))
    payload = dict(raw.get("payload", {}))
    domain = _infer_domain(kind, payload)

    return CanonicalEvent(
        event_id=str(raw.get("event_id", "")),
        tenant_id=tenant_id,
        ts_ms=int(raw.get("clock_ms", 0)),
        domain=domain,
        kind=f"{domain.value}.{kind}" if "." not in kind else kind,
        provenance=ProvenanceRecord(origin=EventProvenance.IMPORTED),
        delta=StateDelta(
            domain=domain,
            delta_schema_version=0,
            data=payload,
        ),
    )


def iter_legacy_events_jsonl(
    path: Path,
    *,
    tenant_id: str = "",
) -> Iterator[CanonicalEvent]:
    """Stream a legacy ``events.jsonl`` file as canonical events."""
    with path.open("r", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            raw = json.loads(line)
            yield legacy_event_to_canonical(raw, tenant_id=tenant_id)


def convert_legacy_events(
    events: List[Dict[str, Any]],
    *,
    tenant_id: str = "",
) -> List[CanonicalEvent]:
    """Batch-convert a list of legacy event dicts."""
    return [legacy_event_to_canonical(e, tenant_id=tenant_id) for e in events]
