from __future__ import annotations

from typing import Optional

from pydantic import BaseModel


class FlowStep(BaseModel):
    index: int
    channel: str
    label: str
    tool: Optional[str] = None
    prev_channel: str = "Plan"
    time_ms: Optional[int] = None


class FlowDataset(BaseModel):
    key: str
    label: str
    steps: list[FlowStep]
    source: str
    question: Optional[str] = None
