from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, Literal

MonitorSeverity = Literal["info", "warning", "error"]


@dataclass(slots=True)
class MonitorFinding:
    monitor: str
    code: str
    message: str
    severity: MonitorSeverity
    time_ms: int
    tool: str | None = None
    metadata: Dict[str, Any] = field(default_factory=dict)
