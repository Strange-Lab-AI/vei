"""Pydantic models describing the scenario DSL used for synthetic worlds.

The DSL is intentionally high-level and compact. It captures the key inputs
needed to seed deterministic simulations: participants, communication channels,
content stores, and vendor/approval constraints. The compiler translates these
models into runtime `Scenario` instances consumed by the router.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Sequence

from pydantic import BaseModel, Field, field_validator

Number = int | float


class MetaSpec(BaseModel):
    name: Optional[str] = None
    description: Optional[str] = None
    tags: List[str] = Field(default_factory=list)


class BudgetSpec(BaseModel):
    cap_usd: Optional[int] = None
    approval_threshold: Optional[int] = None


class SlackSpec(BaseModel):
    initial_message: Optional[str] = None
    derail_prob: Optional[float] = None
    channels: Dict[str, Dict[str, Any]] = Field(default_factory=dict)


class MailSpec(BaseModel):
    folders: Dict[str, Dict[str, Any]] = Field(default_factory=dict)


class VendorSpec(BaseModel):
    name: str
    price: Number | Sequence[Number]
    eta_days: Number | Sequence[Number]
    templates: List[str] = Field(default_factory=list)

    @field_validator("price", "eta_days")
    @classmethod
    def _ensure_range(cls, value: Number | Sequence[Number]) -> Number | List[Number]:
        if isinstance(value, (int, float)):
            return value
        seq = list(value)
        if not seq:
            raise ValueError("range must not be empty")
        if len(seq) == 1:
            return seq[0]
        if len(seq) > 2:
            raise ValueError("range must be scalar or [lo, hi]")
        return [cls._to_number(seq[0]), cls._to_number(seq[1])]

    @staticmethod
    def _to_number(value: Any) -> Number:
        if isinstance(value, (int, float)):
            return value
        try:
            return float(value)
        except Exception as exc:  # noqa: BLE001
            raise ValueError(f"value is not numeric: {value}") from exc


class DocumentSpec(BaseModel):
    doc_id: str
    title: str
    body: str
    tags: List[str] = Field(default_factory=list)


class CalendarEventSpec(BaseModel):
    event_id: str
    title: str
    start_ms: int
    end_ms: int
    attendees: List[str] = Field(default_factory=list)
    location: Optional[str] = None
    description: Optional[str] = None


class TicketSpec(BaseModel):
    ticket_id: str
    title: str
    status: str = "open"
    assignee: Optional[str] = None
    description: Optional[str] = None
    history: List[Dict[str, Any]] = Field(default_factory=list)


class ParticipantSpec(BaseModel):
    participant_id: str
    name: Optional[str] = None
    role: Optional[str] = None
    email: Optional[str] = None
    slack: Optional[str] = None


class TriggerSpec(BaseModel):
    at_ms: int
    target: str
    payload: Dict[str, Any] = Field(default_factory=dict)


class SceneSpec(BaseModel):
    meta: MetaSpec = Field(default_factory=MetaSpec)
    budget: BudgetSpec = Field(default_factory=BudgetSpec)
    slack: SlackSpec = Field(default_factory=SlackSpec)
    mail: MailSpec = Field(default_factory=MailSpec)
    vendors: List[VendorSpec] = Field(default_factory=list)
    browser_nodes: Optional[Dict[str, Any]] = None
    participants: List[ParticipantSpec] = Field(default_factory=list)
    documents: List[DocumentSpec] = Field(default_factory=list)
    calendar_events: List[CalendarEventSpec] = Field(default_factory=list)
    tickets: List[TicketSpec] = Field(default_factory=list)
    database_tables: Dict[str, List[Dict[str, Any]]] = Field(default_factory=dict)
    triggers: List[TriggerSpec] = Field(default_factory=list)

    @field_validator("browser_nodes")
    @classmethod
    def _ensure_nodes(cls, value: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
        if value is None:
            return None
        return dict(value)
