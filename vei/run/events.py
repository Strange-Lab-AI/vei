from __future__ import annotations

import json
from pathlib import Path
from typing import Iterable

from .models import RunTimelineEvent


def load_run_events(path: str | Path) -> list[RunTimelineEvent]:
    events: list[RunTimelineEvent] = []
    for line in Path(path).read_text(encoding="utf-8").splitlines():
        raw = line.strip()
        if not raw:
            continue
        events.append(RunTimelineEvent.model_validate(json.loads(raw)))
    return events


def write_run_events(path: str | Path, events: Iterable[RunTimelineEvent]) -> Path:
    resolved = Path(path)
    resolved.parent.mkdir(parents=True, exist_ok=True)
    with resolved.open("w", encoding="utf-8") as handle:
        for event in events:
            handle.write(json.dumps(event.model_dump(mode="json"), sort_keys=True))
            handle.write("\n")
    return resolved


def append_run_event(path: str | Path, event: RunTimelineEvent) -> RunTimelineEvent:
    resolved = Path(path)
    resolved.parent.mkdir(parents=True, exist_ok=True)
    next_index = 1
    if resolved.exists():
        with resolved.open("r", encoding="utf-8") as handle:
            for line in handle:
                if line.strip():
                    next_index += 1
    event.index = next_index
    with resolved.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(event.model_dump(mode="json"), sort_keys=True))
        handle.write("\n")
    return event


def append_run_events(
    path: str | Path, events: Iterable[RunTimelineEvent]
) -> list[RunTimelineEvent]:
    resolved = Path(path)
    written: list[RunTimelineEvent] = []
    next_index = 1
    if resolved.exists():
        with resolved.open("r", encoding="utf-8") as handle:
            for line in handle:
                if line.strip():
                    next_index += 1
    resolved.parent.mkdir(parents=True, exist_ok=True)
    with resolved.open("a", encoding="utf-8") as handle:
        for event in events:
            event.index = next_index
            handle.write(json.dumps(event.model_dump(mode="json"), sort_keys=True))
            handle.write("\n")
            written.append(event)
            next_index += 1
    return written
