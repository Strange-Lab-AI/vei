"""State management primitives for the event-sourced router.

State lives purely in memory by default, but providing ``base_dir`` enables
lightweight persistence: each append is written to ``events.jsonl`` and
snapshots land under ``snapshots/<index>.json``.
"""

from __future__ import annotations

import json
import logging
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import (
    Any,
    Callable,
    Dict,
    Iterable,
    Iterator,
    List,
    MutableMapping,
    Optional,
)

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class Event:
    """Append-only record describing a state mutation."""

    index: int
    event_id: str
    kind: str
    payload: Dict[str, object]
    clock_ms: int

    @classmethod
    def create(
        cls,
        index: int,
        kind: str,
        payload: Optional[Dict[str, object]] = None,
        clock_ms: int = 0,
        event_id: Optional[str] = None,
    ) -> "Event":
        return cls(
            index=index,
            event_id=event_id or str(uuid.uuid4()),
            kind=kind,
            payload=dict(payload or {}),
            clock_ms=int(clock_ms),
        )

    def to_dict(self) -> Dict[str, object]:
        return {
            "index": self.index,
            "event_id": self.event_id,
            "kind": self.kind,
            "payload": self.payload,
            "clock_ms": self.clock_ms,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "Event":
        return cls(
            index=int(data["index"]),
            event_id=str(data["event_id"]),
            kind=str(data["kind"]),
            payload=dict(data.get("payload", {})),
            clock_ms=int(data.get("clock_ms", 0)),
        )


@dataclass(frozen=True, slots=True)
class Snapshot:
    """Captured materialised state at a particular event index."""

    index: int
    clock_ms: int
    data: Dict[str, object]

    def to_json(self) -> str:
        return json.dumps(
            {
                "index": self.index,
                "clock_ms": self.clock_ms,
                "data": self.data,
            },
            sort_keys=True,
        )


class StateStore:
    """Lightweight event log with snapshot helpers.

    The store keeps an in-memory log today; providing a file system path simply
    primes future persistence hooks and surfaces in the repr for observability.
    """

    def __init__(self, base_dir: Optional[Path] = None, branch: str = "main") -> None:
        self.base_dir = Path(base_dir) if base_dir else None
        self.branch = branch
        self._events: List[Event] = []
        self._state: Dict[str, object] = {}
        self._snapshots: Dict[int, Snapshot] = {}
        self._reducers: Dict[str, Reducer] = {}
        self._branch_dir: Optional[Path] = None
        self._events_path: Optional[Path] = None
        self._snapshots_dir: Optional[Path] = None
        if self.base_dir:
            self._init_storage()
            self._load_events()

    # -- public API -----------------------------------------------------
    @property
    def head(self) -> int:
        return self._events[-1].index if self._events else -1

    @property
    def events(self) -> Iterable[Event]:
        return tuple(self._events)

    def materialised_state(self) -> Dict[str, object]:
        return json.loads(json.dumps(self._state))  # deep-ish copy

    def append(
        self,
        kind: str,
        payload: Optional[Dict[str, object]] = None,
        *,
        clock_ms: int = 0,
        reducer: Optional[Callable[[MutableMapping[str, object]], None]] = None,
    ) -> Event:
        """Record a mutation and optionally update the in-memory state.

        Reducer receives a mutable mapping view which it may modify in-place.
        """

        next_index = self.head + 1
        event = Event.create(next_index, kind, payload=payload, clock_ms=clock_ms)
        self._events.append(event)

        if reducer is not None:
            view = _MutableStateView(self._state)
            reducer(view)
        registered = self._reducers.get(kind)
        if registered is not None:
            registered(self._state, event)
        self._write_event(event)
        return event

    def take_snapshot(self) -> Snapshot:
        snap = Snapshot(
            index=self.head, clock_ms=self._clock_hint(), data=self.materialised_state()
        )
        self._snapshots[snap.index] = snap
        self._write_snapshot(snap)
        return snap

    def iter_since(self, index: int) -> Iterator[Event]:
        for event in self._events:
            if event.index > index:
                yield event

    def rebuild_state(self, upto: Optional[int] = None) -> Dict[str, object]:
        upto = self.head if upto is None else upto
        base: Dict[str, object] = {}
        for event in self._events:
            if event.index > upto:
                break
            reducer = self._reducers.get(event.kind)
            if reducer:
                reducer(base, event)
        return json.loads(json.dumps(base))

    def register_reducer(self, kind: str, reducer: "Reducer") -> None:
        self._reducers[kind] = reducer
        # Rebuild state from events so newly registered reducers apply retroactively.
        self._replay_state()

    def branch_from(
        self, snapshot: Snapshot, *, branch: Optional[str] = None
    ) -> "StateStore":
        new_store = StateStore(
            base_dir=self.base_dir, branch=branch or f"{self.branch}@{snapshot.index}"
        )
        new_store._events = [evt for evt in self._events if evt.index <= snapshot.index]
        new_store._state = json.loads(json.dumps(snapshot.data))
        new_store._snapshots = {snapshot.index: snapshot}
        new_store._reducers = self._reducers.copy()
        new_store._replay_state()
        return new_store

    # -- internal helpers -----------------------------------------------
    def _clock_hint(self) -> int:
        if self._events:
            return self._events[-1].clock_ms
        return 0

    def _init_storage(self) -> None:
        assert self.base_dir is not None
        branch_dir = self.base_dir / self._sanitize_branch(self.branch)
        branch_dir.mkdir(parents=True, exist_ok=True)
        snapshots_dir = branch_dir / "snapshots"
        snapshots_dir.mkdir(exist_ok=True)
        self._branch_dir = branch_dir
        self._snapshots_dir = snapshots_dir
        self._events_path = branch_dir / "events.jsonl"

    def _load_events(self) -> None:
        if not self._events_path or not self._events_path.exists():
            return
        self._events.clear()
        try:
            with self._events_path.open("r", encoding="utf-8") as fh:
                for line in fh:
                    line = line.strip()
                    if not line:
                        continue
                    data = json.loads(line)
                    evt = Event.from_dict(data)
                    self._events.append(evt)
        except Exception:
            logger.warning(
                "corrupted event log at %s; starting fresh", self._events_path
            )
            self._events = []
        self._replay_state()

    def _write_event(self, event: Event) -> None:
        if not self._events_path:
            return
        try:
            with self._events_path.open("a", encoding="utf-8") as fh:
                fh.write(json.dumps(event.to_dict(), sort_keys=True) + "\n")
        except Exception:
            logger.warning(
                "failed to persist event %d to %s",
                event.index,
                self._events_path,
                exc_info=True,
            )

    def _write_snapshot(self, snapshot: Snapshot) -> None:
        if not self._snapshots_dir:
            return
        path = self._snapshots_dir / f"{snapshot.index:09d}.json"
        try:
            path.write_text(snapshot.to_json(), encoding="utf-8")
        except Exception:
            logger.warning(
                "failed to write snapshot %d to %s", snapshot.index, path, exc_info=True
            )

    def _replay_state(self) -> None:
        base: Dict[str, object] = {}
        for event in self._events:
            reducer = self._reducers.get(event.kind)
            if reducer:
                reducer(base, event)
        self._state = base

    @staticmethod
    def _sanitize_branch(branch: str) -> str:
        safe = branch.replace("/", "_").replace("\\", "_")
        return safe or "main"

    @property
    def storage_dir(self) -> Optional[Path]:
        return self._branch_dir

    def list_snapshot_paths(self) -> List[Path]:
        if not self._snapshots_dir or not self._snapshots_dir.exists():
            return []
        return sorted(self._snapshots_dir.glob("*.json"))


class _MutableStateView(MutableMapping[str, object]):
    """Thin MutableMapping wrapper to highlight in-place mutability."""

    def __init__(self, backing: Dict[str, object]) -> None:
        self._backing = backing

    def __getitem__(self, key: str) -> object:
        return self._backing[key]

    def __setitem__(self, key: str, value: object) -> None:
        self._backing[key] = value

    def __delitem__(self, key: str) -> None:
        del self._backing[key]

    def __iter__(self) -> Iterator[str]:
        return iter(self._backing)

    def __len__(self) -> int:
        return len(self._backing)


Reducer = Callable[[Dict[str, object], Event], None]
