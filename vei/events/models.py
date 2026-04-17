"""Canonical event schema for VEI.

CanonicalEvent is the source of truth across VEI.  WorldSession StateStore,
run timelines, and connector receipts are derived views.  Control-plane events
(governance) share the same spine.  Raw provider payloads in the ingest RawLog
are pre-canonical and not authoritative.

Schema stability rules
----------------------
* Fields on ``CanonicalEvent`` are frozen at **v1** (schema_version = 1).
  A bump is a breaking change.
* Per-domain ``StateDelta`` payloads version independently via their own
  ``delta_schema_version``.  ``delta_schema_version = 0`` means opaque dict.
"""

from __future__ import annotations

import hashlib
import json
import uuid
from enum import Enum
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field

# ---------------------------------------------------------------------------
# Stable enums
# ---------------------------------------------------------------------------


class EventDomain(str, Enum):
    COMM_GRAPH = "comm_graph"
    WORK_GRAPH = "work_graph"
    DOC_GRAPH = "doc_graph"
    IDENTITY_GRAPH = "identity_graph"
    REVENUE_GRAPH = "revenue_graph"
    KNOWLEDGE_GRAPH = "knowledge_graph"
    OBS_GRAPH = "obs_graph"
    OPS_GRAPH = "ops_graph"
    DATA_GRAPH = "data_graph"
    GOVERNANCE = "governance"
    VERTICAL = "vertical"
    INTERNAL = "internal"


class EventProvenance(str, Enum):
    IMPORTED = "imported"
    DERIVED = "derived"
    SIMULATED = "simulated"


class InternalExternal(str, Enum):
    INTERNAL = "internal"
    EXTERNAL = "external"
    UNKNOWN = "unknown"


# ---------------------------------------------------------------------------
# Typed references (frozen v1 shapes)
# ---------------------------------------------------------------------------


class ActorRef(BaseModel):
    """Reference to an actor in the enterprise graph."""

    actor_id: str
    display_name: str = ""
    role: str = ""
    tenant_id: str = ""


class ObjectRef(BaseModel):
    """Reference to an enterprise object (thread, ticket, doc, etc.)."""

    object_id: str
    domain: str = ""
    kind: str = ""
    label: str = ""


class CaseRef(BaseModel):
    """Cross-surface case linkage."""

    case_id: str
    surfaces: List[str] = Field(default_factory=list)
    label: str = ""


class TextHandle(BaseModel):
    """Content-addressed lazy reference to text content.

    Bodies are loaded only on demand.  ``content_hash`` is the SHA-256 hex
    digest of the UTF-8 encoded body.  ``store_uri`` is an opaque locator
    that the hosting layer resolves.
    """

    content_hash: str = ""
    store_uri: str = ""
    byte_length: int = 0

    @classmethod
    def from_text(cls, text: str, *, store_uri: str = "") -> "TextHandle":
        encoded = text.encode("utf-8")
        content_hash = hashlib.sha256(encoded).hexdigest()
        return cls(
            content_hash=content_hash,
            store_uri=store_uri,
            byte_length=len(encoded),
        )


class ProvenanceRecord(BaseModel):
    """Extended provenance beyond the simple enum."""

    origin: EventProvenance = EventProvenance.SIMULATED
    source_id: str = ""
    tenant_manifest_decision: str = ""


# ---------------------------------------------------------------------------
# StateDelta (not frozen — versions independently)
# ---------------------------------------------------------------------------


class StateDelta(BaseModel):
    """Per-domain state delta.

    ``delta_schema_version = 0`` means the ``data`` field is an opaque dict.
    Domain-specific typed deltas live in ``vei.events.deltas.<domain>`` and
    carry ``delta_schema_version >= 1``.
    """

    domain: EventDomain = EventDomain.INTERNAL
    delta_schema_version: int = 0
    data: Dict[str, Any] = Field(default_factory=dict)


# ---------------------------------------------------------------------------
# CanonicalEvent — frozen v1 envelope
# ---------------------------------------------------------------------------

_SCHEMA_VERSION = 1


class CanonicalEvent(BaseModel):
    """The single source of truth event on the VEI event spine.

    All fields below are **frozen at v1**.  Adding a required field or
    changing the semantics of an existing one requires bumping
    ``schema_version``.
    """

    schema_version: int = _SCHEMA_VERSION
    event_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    tenant_id: str = ""
    case_id: Optional[str] = None
    ts_ms: int = 0

    domain: EventDomain = EventDomain.INTERNAL
    kind: str = ""

    actor_ref: Optional[ActorRef] = None
    participants: List[ActorRef] = Field(default_factory=list)
    object_refs: List[ObjectRef] = Field(default_factory=list)

    internal_external: InternalExternal = InternalExternal.UNKNOWN

    provenance: ProvenanceRecord = Field(default_factory=ProvenanceRecord)
    text_handle: Optional[TextHandle] = None
    policy_tags: List[str] = Field(default_factory=list)

    delta: Optional[StateDelta] = None

    hash: str = ""

    def compute_hash(self) -> str:
        """Deterministic content hash over the envelope (excludes hash field)."""
        payload = self.model_dump(mode="json", exclude={"hash"})
        canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"))
        return hashlib.sha256(canonical.encode("utf-8")).hexdigest()

    def with_hash(self) -> "CanonicalEvent":
        """Return a copy with the hash field populated."""
        return self.model_copy(update={"hash": self.compute_hash()})
