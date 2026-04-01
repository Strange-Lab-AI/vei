"""Deterministic event scheduling bus for the simulation router."""

from __future__ import annotations

import heapq
from dataclasses import dataclass
from typing import Any, Dict, List, Optional


@dataclass
class Event:
    t_due_ms: int
    target: str
    payload: Dict[str, Any]
    event_id: str
    source: str = "system"
    actor_id: Optional[str] = None
    kind: str = "scheduled"


class LinearCongruentialGenerator:
    def __init__(self, seed: int):
        self.state = seed & 0xFFFFFFFF

    def next_u32(self) -> int:
        self.state = (1664525 * self.state + 1013904223) & 0xFFFFFFFF
        return self.state

    def next_float(self) -> float:
        return self.next_u32() / 0x100000000

    def randint(self, a: int, b: int) -> int:
        return a + int(self.next_float() * (b - a + 1))


class EventBus:
    def __init__(self, seed: int):
        self.rng = LinearCongruentialGenerator(seed)
        self.clock_ms = 0
        self._heap: list[tuple[int, int, Event]] = []
        self._seq = 0

    def schedule(
        self,
        dt_ms: int,
        target: str,
        payload: Dict[str, Any],
        *,
        event_id: Optional[str] = None,
        source: str = "system",
        actor_id: Optional[str] = None,
        kind: str = "scheduled",
    ) -> str:
        self._seq += 1
        evt = Event(
            self.clock_ms + dt_ms,
            target,
            payload,
            event_id=event_id or f"evt-{self._seq:08d}",
            source=source,
            actor_id=actor_id,
            kind=kind,
        )
        heapq.heappush(self._heap, (evt.t_due_ms, self._seq, evt))
        return evt.event_id

    def next_if_due(self) -> Optional[Event]:
        if self._heap and self._heap[0][0] <= self.clock_ms:
            _, _, evt = heapq.heappop(self._heap)
            return evt
        return None

    def advance(self, dt_ms: int) -> None:
        self.clock_ms += dt_ms

    def peek_due_time(self) -> Optional[int]:
        return self._heap[0][0] if self._heap else None

    def pending_count(self, target: Optional[str] = None) -> int:
        if target is None:
            return len(self._heap)
        return sum(1 for _, _, e in self._heap if e.target == target)

    def cancel(self, event_id: str) -> bool:
        remaining = [
            item
            for item in self._heap
            if getattr(item[2], "event_id", None) != event_id
        ]
        if len(remaining) == len(self._heap):
            return False
        self._heap = remaining
        heapq.heapify(self._heap)
        return True

    def clear(self) -> None:
        self._heap = []

    def list_events(self) -> List[Event]:
        return [event for _, _, event in sorted(self._heap)]
