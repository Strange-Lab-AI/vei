"""Public API and protocols for vei.ingest.

All protocols are implementation-agnostic.  Default implementations ship
under ``vei.ingest.raw``, ``vei.ingest.normalize``, ``vei.ingest.materialize``,
and ``vei.ingest.cases``.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, Iterator, List, Optional, Protocol, runtime_checkable

from pydantic import BaseModel, Field

from vei.events.models import CanonicalEvent

logger = logging.getLogger(__name__)


class SessionSlice(BaseModel):
    """A focused graph slice plus event window for one case."""

    tenant_id: str = ""
    case_id: str = ""
    events: List[CanonicalEvent] = Field(default_factory=list)
    graph_slice: Dict[str, Any] = Field(default_factory=dict)
    metadata: Dict[str, Any] = Field(default_factory=dict)


# ---------------------------------------------------------------------------
# Protocols
# ---------------------------------------------------------------------------


@runtime_checkable
class RawLog(Protocol):
    """Append-only, per-tenant, per-source raw event log."""

    def append(self, raw_record: Dict[str, Any]) -> str: ...

    def iter_since(self, cursor: str) -> Iterator[Dict[str, Any]]: ...


@runtime_checkable
class Normalizer(Protocol):
    """Streaming raw -> CanonicalEvent."""

    def normalize(self, raw_record: Dict[str, Any]) -> List[CanonicalEvent]: ...


@runtime_checkable
class CaseResolver(Protocol):
    """Given a stream of CanonicalEvent, outputs CaseAssignments."""

    def resolve(self, events: List[CanonicalEvent]) -> List["CaseAssignment"]: ...


@runtime_checkable
class Materializer(Protocol):
    """Maintains canonical-event store + per-tenant graph projections."""

    def apply(self, events: List[CanonicalEvent]) -> int: ...

    def query_graph(
        self, tenant_id: str, scope: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]: ...

    def query_events(
        self, tenant_id: str, filters: Optional[Dict[str, Any]] = None
    ) -> List[CanonicalEvent]: ...

    def query_case(self, case_id: str) -> List[CanonicalEvent]: ...


@runtime_checkable
class SessionMaterializer(Protocol):
    """Lazy-hydration boundary: produces a focused slice for WorldSession."""

    def materialize(
        self,
        tenant_id: str,
        case_id: str,
        *,
        window_ms: Optional[int] = None,
    ) -> SessionSlice: ...


# ---------------------------------------------------------------------------
# Pipeline orchestrator
# ---------------------------------------------------------------------------


class IngestPipeline:
    """Orchestrates RawLog -> Normalizer -> CaseResolver -> Materializer."""

    def __init__(
        self,
        *,
        raw_log: RawLog,
        normalizer: Normalizer,
        case_resolver: CaseResolver,
        materializer: Materializer,
    ) -> None:
        self.raw_log = raw_log
        self.normalizer = normalizer
        self.case_resolver = case_resolver
        self.materializer = materializer

    def ingest(self, raw_records: List[Dict[str, Any]]) -> int:
        """Process a batch of raw records end-to-end. Returns events applied."""
        all_events: List[CanonicalEvent] = []
        for raw in raw_records:
            self.raw_log.append(raw)
            events = self.normalizer.normalize(raw)
            all_events.extend(events)
        if all_events:
            assignments = self.case_resolver.resolve(all_events)
            case_by_event_id = {
                event_id: assignment.case_id
                for assignment in assignments
                for event_id in assignment.event_ids
            }
            for event in all_events:
                if event.case_id:
                    continue
                case_id = case_by_event_id.get(event.event_id)
                if case_id:
                    event.case_id = case_id
            return self.materializer.apply(all_events)
        return 0


# ---------------------------------------------------------------------------
# Case assignment model (frozen v1)
# ---------------------------------------------------------------------------


class CaseAssignment(BaseModel):
    """Cross-surface case linkage output from CaseResolver.

    This shape is frozen.  Domain-specific enrichments belong in derived
    views, not here.
    """

    case_id: str
    event_ids: List[str] = Field(default_factory=list)
    participants: List[str] = Field(default_factory=list)
    linked_object_refs: List[str] = Field(default_factory=list)
    surfaces: List[str] = Field(default_factory=list)
    start_ts: int = 0
    end_ts: int = 0


__all__ = [
    "CaseAssignment",
    "CaseResolver",
    "IngestPipeline",
    "Materializer",
    "Normalizer",
    "RawLog",
    "SessionMaterializer",
    "SessionSlice",
]
