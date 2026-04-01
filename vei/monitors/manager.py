from __future__ import annotations

from typing import Dict, Iterable, List

from .api import ToolRegistryView
from .models import MonitorFinding
from .tool_aware import ToolAwareMonitor

SUPPORTED_MONITORS = {
    "tool_aware": ToolAwareMonitor,
}


class MonitorManager:
    def __init__(self, registry: ToolRegistryView, enabled: Iterable[str]) -> None:
        self.registry = registry
        self._monitors = []
        for name in enabled:
            cls = SUPPORTED_MONITORS.get(name.strip())
            if cls is None:
                continue
            self._monitors.append(cls())
        self._findings: List[MonitorFinding] = []

    def monitors(self) -> List[str]:
        return [getattr(m, "name", m.__class__.__name__) for m in self._monitors]

    def after_tool_call(
        self,
        *,
        tool: str,
        args: Dict[str, object],
        result: object,
        snapshot: Dict[str, object],
    ) -> List[MonitorFinding]:
        findings: List[MonitorFinding] = []
        for monitor in self._monitors:
            try:
                findings.extend(
                    monitor.on_tool_call(
                        tool=tool,
                        args=dict(args),
                        result=result,
                        state_snapshot=snapshot,
                    )
                )
            except Exception as exc:  # noqa: BLE001
                findings.append(
                    MonitorFinding(
                        monitor=getattr(monitor, "name", monitor.__class__.__name__),
                        code="monitor.error",
                        message=str(exc),
                        severity="error",
                        time_ms=int(snapshot.get("time_ms", 0)),
                        tool=tool,
                    )
                )
        self._findings.extend(findings)
        # Keep tail bounded
        if len(self._findings) > 200:
            del self._findings[: len(self._findings) - 200]
        return findings

    def findings_tail(self, n: int = 50) -> List[MonitorFinding]:
        return self._findings[-n:] if n and n > 0 else list(self._findings)
