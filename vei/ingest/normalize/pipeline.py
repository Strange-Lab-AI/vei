"""Streaming normalizer: raw records -> CanonicalEvent.

Wraps the existing ``vei.context._normalize_extract`` logic so we do not
duplicate rules.  Makes normalization incremental instead of bundle-at-a-time.
"""

from __future__ import annotations

from typing import Any, Dict, List

from vei.events.models import (
    CanonicalEvent,
    EventProvenance,
    ProvenanceRecord,
    StateDelta,
)
from vei.events.legacy import _infer_domain


class StreamingNormalizer:
    """Incremental normalizer for raw provider records."""

    def __init__(self, tenant_id: str = "") -> None:
        self._tenant_id = tenant_id

    def normalize(self, raw_record: Dict[str, Any]) -> List[CanonicalEvent]:
        kind = str(raw_record.get("kind", raw_record.get("type", "")))
        payload = dict(raw_record.get("payload", raw_record))
        domain = _infer_domain(kind, payload)

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
