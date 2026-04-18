from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Callable, Dict, List, Optional, Protocol

from vei.monitors.api import MonitorFinding

if TYPE_CHECKING:
    from .memory import MemoryStore


Status = str


class RouterLike(Protocol):
    """Router-facing subset used by behavior nodes."""

    def call_and_step(self, tool: str, args: Dict[str, object]) -> object: ...

    def observe(self, focus_hint: Optional[str] = None) -> "ObservationLike": ...


class ObservationLike(Protocol):
    def model_dump(self) -> Dict[str, Any]: ...


@dataclass
class BehaviorContext:
    router: RouterLike
    memory: "MemoryStore"
    transcript: List[Dict[str, object]]

    def record(self, entry: Dict[str, object]) -> None:
        self.transcript.append(entry)


class BehaviorNode:
    def tick(self, ctx: BehaviorContext) -> Status:
        raise NotImplementedError


class SequenceNode(BehaviorNode):
    def __init__(self, *children: BehaviorNode) -> None:
        self.children = list(children)

    def tick(self, ctx: BehaviorContext) -> Status:
        for child in self.children:
            status = child.tick(ctx)
            if status != "success":
                return status
        return "success"


class SelectorNode(BehaviorNode):
    def __init__(self, *children: BehaviorNode) -> None:
        self.children = list(children)

    def tick(self, ctx: BehaviorContext) -> Status:
        for child in self.children:
            status = child.tick(ctx)
            if status == "success":
                return "success"
        return "failure"


class ConditionNode(BehaviorNode):
    def __init__(self, predicate: Callable[[BehaviorContext], bool]) -> None:
        self.predicate = predicate

    def tick(self, ctx: BehaviorContext) -> Status:
        return "success" if self.predicate(ctx) else "failure"


class ToolAction(BehaviorNode):
    def __init__(
        self,
        tool: str,
        args: Optional[Dict[str, object]] = None,
        focus: Optional[str] = None,
    ) -> None:
        self.tool = tool
        self.args = args or {}
        self.focus = focus

    def tick(self, ctx: BehaviorContext) -> Status:
        try:
            result = ctx.router.call_and_step(self.tool, dict(self.args))
            ctx.record({"tool": self.tool, "args": self.args, "result": result})
            return "success"
        except Exception as exc:  # noqa: BLE001
            ctx.record({"tool": self.tool, "args": self.args, "error": str(exc)})
            return "failure"


class Observe(BehaviorNode):
    def __init__(self, focus: Optional[str] = None) -> None:
        self.focus = focus

    def tick(self, ctx: BehaviorContext) -> Status:
        obs = ctx.router.observe(focus_hint=self.focus)
        ctx.record({"observation": obs.model_dump()})
        return "success"


class WaitFor(BehaviorNode):
    def __init__(
        self,
        predicate: Callable[[BehaviorContext], bool],
        max_ticks: int = 5,
        focus: Optional[str] = None,
    ) -> None:
        self.predicate = predicate
        self.max_ticks = max_ticks
        self.focus = focus

    def tick(self, ctx: BehaviorContext) -> Status:
        met = False
        for _ in range(max(1, self.max_ticks)):
            obs = ctx.router.observe(focus_hint=self.focus)
            ctx.record({"observation": obs.model_dump(), "wait": True})
            if self.predicate(ctx):
                met = True
                break
        ctx.record({"wait_complete": True, "met": met})
        return "success" if met else "failure"


class MemoriseFinding(BehaviorNode):
    def __init__(
        self, kind: str, extractor: Callable[[BehaviorContext], Optional[str]]
    ) -> None:
        self.kind = kind
        self.extractor = extractor

    def tick(self, ctx: BehaviorContext) -> Status:
        value = self.extractor(ctx)
        if value:
            ctx.memory.remember(kind=self.kind, key="latest", value=value)
            return "success"
        return "failure"


def findings_from_snapshot(snapshot: Dict[str, object]) -> List[MonitorFinding]:
    payload = snapshot.get("monitor_findings")
    if not isinstance(payload, list):
        return []
    findings: List[MonitorFinding] = []
    for item in payload:
        try:
            findings.append(MonitorFinding(**item))
        except Exception:
            continue
    return findings
