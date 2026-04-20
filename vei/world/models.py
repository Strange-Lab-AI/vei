from __future__ import annotations

from typing import Any, Dict, List, Literal, Optional

from pydantic import BaseModel, Field


class ScheduledEvent(BaseModel):
    event_id: str
    target: str
    payload: Dict[str, Any] = Field(default_factory=dict)
    due_ms: int
    source: str = "system"
    actor_id: Optional[str] = None
    kind: Literal["scheduled", "injected", "actor_recorded"] = "scheduled"


class InjectedEvent(BaseModel):
    target: str
    payload: Dict[str, Any] = Field(default_factory=dict)
    dt_ms: int = 0
    source: str = "human"
    actor_id: Optional[str] = None
    kind: Literal["injected"] = "injected"


class ActorState(BaseModel):
    actor_id: str
    mode: Literal["scripted", "llm_recorded"] = "scripted"
    status: str = "idle"
    recorded_events: List[ScheduledEvent] = Field(default_factory=list)
    cursor: int = 0
    metadata: Dict[str, Any] = Field(default_factory=dict)


class WorldState(BaseModel):
    branch: str
    clock_ms: int
    rng_state: int
    queue_seq: int
    seed: int
    scenario: Dict[str, Any] = Field(default_factory=dict)
    pending_events: List[ScheduledEvent] = Field(default_factory=list)
    event_log: List[Dict[str, Any]] = Field(default_factory=list)
    components: Dict[str, Dict[str, Any]] = Field(default_factory=dict)
    trace_entries: List[Dict[str, Any]] = Field(default_factory=list)
    receipts: List[Dict[str, Any]] = Field(default_factory=list)
    connector_runtime: Dict[str, Any] = Field(default_factory=dict)
    actor_states: Dict[str, ActorState] = Field(default_factory=dict)
    audit_state: Dict[str, Any] = Field(default_factory=dict)
    replay: Dict[str, Any] = Field(default_factory=dict)


class WorldSnapshot(BaseModel):
    snapshot_id: int
    branch: str
    time_ms: int
    data: WorldState
    label: Optional[str] = None
