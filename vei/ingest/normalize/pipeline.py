"""Streaming normalizer: raw records -> CanonicalEvent.

Lightweight record-at-a-time normalizer that maps raw provider records into
CanonicalEvent objects using ``vei.events.api.infer_domain``.  This does NOT
share cleanup or verification rules from ``vei.context._normalize_extract`` /
``_normalize_cleanup`` / ``_normalize_verify``; those apply only during
full-snapshot bundle normalization (``vei context normalize``).
"""

from __future__ import annotations

from typing import Any, Dict, List

from vei.events.api import (
    CanonicalEvent,
    EventProvenance,
    ProvenanceRecord,
    StateDelta,
    infer_domain,
)


class StreamingNormalizer:
    """Incremental normalizer for raw provider records."""

    def __init__(self, tenant_id: str = "") -> None:
        self._tenant_id = tenant_id

    def normalize(self, raw_record: Dict[str, Any]) -> List[CanonicalEvent]:
        kind = str(raw_record.get("kind", raw_record.get("type", "")))
        payload = dict(raw_record.get("payload", raw_record))
        domain = infer_domain(kind, payload)

        event = CanonicalEvent(
            tenant_id=self._tenant_id,
            ts_ms=int(raw_record.get("clock_ms", raw_record.get("ts_ms", 0))),
            domain=domain,
            kind=f"{domain.value}.{kind}" if kind and "." not in kind else kind,
            provenance=ProvenanceRecord(origin=EventProvenance.IMPORTED),
            delta=StateDelta(
                domain=domain,
                delta_schema_version=0,
                data=payload,
            ),
        )
        return [event]
