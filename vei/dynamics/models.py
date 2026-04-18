"""Typed request/response models for the DynamicsBackend protocol."""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field

from vei.events.api import CanonicalEvent

# ---------------------------------------------------------------------------
# Request
# ---------------------------------------------------------------------------


class CompanyGraphSlice(BaseModel):
    """Typed projection of a tenant's enterprise graph at query time."""

    tenant_id: str = ""
    domains: List[str] = Field(default_factory=list)
    actors: List[Dict[str, Any]] = Field(default_factory=list)
    objects: List[Dict[str, Any]] = Field(default_factory=list)
    edges: List[Dict[str, Any]] = Field(default_factory=list)
    metadata: Dict[str, Any] = Field(default_factory=dict)


class CandidateAction(BaseModel):
    """Structured description of the action under evaluation."""

    action_id: str = ""
    label: str = ""
    description: str = ""
    tool: str = ""
    args: Dict[str, Any] = Field(default_factory=dict)
    policy_tags: List[str] = Field(default_factory=list)


class DynamicsRequest(BaseModel):
    """Input contract for DynamicsBackend.forecast()."""

    company_graph_slice: CompanyGraphSlice = Field(
        default_factory=CompanyGraphSlice,
    )
    recent_events: List[CanonicalEvent] = Field(default_factory=list)
    candidate_action: Optional[CandidateAction] = None
    horizon: int = 84
    seed: int = 0


# ---------------------------------------------------------------------------
# Response
# ---------------------------------------------------------------------------


class PredictedEvent(BaseModel):
    """A CanonicalEvent with a calibrated probability attached."""

    event: CanonicalEvent
    probability: float = 0.0


class PointInterval(BaseModel):
    """A point estimate with an optional confidence interval."""

    point: float = 0.0
    lower: Optional[float] = None
    upper: Optional[float] = None


class BusinessHeads(BaseModel):
    """Per-head business outcome predictions."""

    risk: PointInterval = Field(default_factory=PointInterval)
    spread: PointInterval = Field(default_factory=PointInterval)
    escalation: PointInterval = Field(default_factory=PointInterval)
    approval: PointInterval = Field(default_factory=PointInterval)
    load: PointInterval = Field(default_factory=PointInterval)
    drag: PointInterval = Field(default_factory=PointInterval)
    stock_return_5d: PointInterval | None = None
    credit_action_30d: PointInterval | None = None
    ferc_action_180d: PointInterval | None = None


class CalibrationMetrics(BaseModel):
    """Optional calibration info returned by the backend."""

    ece: Optional[float] = None
    interval_coverage: Optional[float] = None
    auroc: Optional[float] = None


class DynamicsResponse(BaseModel):
    """Output contract for DynamicsBackend.forecast()."""

    predicted_events: List[PredictedEvent] = Field(default_factory=list)
    state_delta_summary: Dict[str, Any] = Field(default_factory=dict)
    business_heads: BusinessHeads = Field(default_factory=BusinessHeads)
    backend_id: str = ""
    backend_version: str = ""
    calibration: CalibrationMetrics = Field(default_factory=CalibrationMetrics)


# ---------------------------------------------------------------------------
# Backend info
# ---------------------------------------------------------------------------


class BackendInfo(BaseModel):
    """Descriptor returned by DynamicsBackend.describe()."""

    name: str = ""
    version: str = ""
    backend_type: str = ""
    deterministic: bool = True
    metadata: Dict[str, Any] = Field(default_factory=dict)


class DeterminismManifest(BaseModel):
    """Records exactly what seeds, versions, and hashes were used."""

    backend_id: str = ""
    backend_version: str = ""
    torch_seed: Optional[int] = None
    numpy_seed: Optional[int] = None
    dataset_schema_version: int = 0
    checkpoint_hash: str = ""
    feed_schema_version: int = 0
    notes: List[str] = Field(default_factory=list)
