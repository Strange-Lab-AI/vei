from __future__ import annotations

from typing import Any, Dict, List, Optional

from vei.world.api import Scenario

from .errors import MCPError
from .tool_providers import PrefixToolProvider
from .tool_registry import ToolSpec


def _seeded_entries(scenario: Optional[Scenario]) -> Dict[str, Dict[str, Any]]:
    metadata = getattr(scenario, "metadata", None) or {}
    notes = metadata.get("notes")
    if not isinstance(notes, list):
        return {}
    entries: Dict[str, Dict[str, Any]] = {}
    for index, raw in enumerate(notes, start=1):
        if not isinstance(raw, dict):
            continue
        entry_id = str(raw.get("entry_id") or f"NOTE-{index:03d}")
        entries[entry_id] = {
            "entry_id": entry_id,
            "title": str(raw.get("title") or entry_id),
            "body": str(raw.get("body") or ""),
            "tags": list(raw.get("tags") or []),
            "updated_ms": int(raw.get("updated_ms", 1_700_000_000_000 + index)),
        }
    return entries


class NotesSim:
    """Small deterministic note-taking surface used to prove pluggable facades."""

    def __init__(self, bus: Any, scenario: Optional[Scenario] = None):
        self.bus = bus
        self.entries: Dict[str, Dict[str, Any]] = _seeded_entries(scenario)
        self._entry_seq = max(len(self.entries) + 1, 1)

    def list_entries(self, tag: Optional[str] = None) -> List[Dict[str, Any]]:
        rows = list(self.entries.values())
        if tag:
            wanted = tag.strip().lower()
            rows = [
                row
                for row in rows
                if any(str(item).lower() == wanted for item in row.get("tags", []))
            ]
        rows.sort(
            key=lambda row: (
                int(row.get("updated_ms", 0)),
                str(row.get("entry_id", "")),
            ),
            reverse=True,
        )
        return [dict(row) for row in rows]

    def get_entry(self, entry_id: str) -> Dict[str, Any]:
        entry = self.entries.get(entry_id)
        if entry is None:
            raise MCPError("notes.entry_not_found", f"Unknown note entry: {entry_id}")
        return dict(entry)

    def create_entry(
        self,
        title: str,
        body: str,
        tags: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        entry_id = f"NOTE-{self._entry_seq:03d}"
        self._entry_seq += 1
        entry = {
            "entry_id": entry_id,
            "title": title.strip() or entry_id,
            "body": body,
            "tags": [str(item) for item in (tags or []) if str(item).strip()],
            "updated_ms": int(self.bus.clock_ms),
        }
        self.entries[entry_id] = entry
        return dict(entry)

    def update_entry(
        self,
        entry_id: str,
        title: Optional[str] = None,
        body: Optional[str] = None,
        tags: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        entry = self.entries.get(entry_id)
        if entry is None:
            raise MCPError("notes.entry_not_found", f"Unknown note entry: {entry_id}")
        if title is not None:
            entry["title"] = title.strip() or entry["title"]
        if body is not None:
            entry["body"] = body
        if tags is not None:
            entry["tags"] = [str(item) for item in tags if str(item).strip()]
        entry["updated_ms"] = int(self.bus.clock_ms)
        return dict(entry)

    def export_state(self) -> Dict[str, Any]:
        return {
            "entries": {
                entry_id: dict(payload) for entry_id, payload in self.entries.items()
            },
            "entry_seq": int(self._entry_seq),
        }

    def import_state(self, state: Dict[str, Any]) -> None:
        entries = state.get("entries", {})
        self.entries = (
            {str(entry_id): dict(payload) for entry_id, payload in entries.items()}
            if isinstance(entries, dict)
            else {}
        )
        self._entry_seq = int(state.get("entry_seq", max(len(self.entries) + 1, 1)))

    def summary(self) -> str:
        count = len(self.entries)
        return f"Notes has {count} entry{'ies' if count != 1 else ''}."

    def action_menu(self) -> List[Dict[str, Any]]:
        return [
            {
                "tool": "notes.list_entries",
                "label": "Review notes",
                "args": {},
            },
            {
                "tool": "notes.create_entry",
                "label": "Capture note",
                "args": {"title": "Decision", "body": "Summarize the latest decision."},
            },
        ]


class NotesToolProvider(PrefixToolProvider):
    def __init__(self, sim: NotesSim):
        super().__init__("notes", prefixes=("notes.",))
        self.sim = sim
        self._specs = [
            ToolSpec(
                name="notes.list_entries",
                description="List note entries with their latest updates.",
                permissions=("notes:read",),
                default_latency_ms=120,
                latency_jitter_ms=30,
            ),
            ToolSpec(
                name="notes.get_entry",
                description="Read one note entry by id.",
                permissions=("notes:read",),
                default_latency_ms=110,
                latency_jitter_ms=25,
            ),
            ToolSpec(
                name="notes.create_entry",
                description="Create a note entry for decisions, plans, or follow-ups.",
                permissions=("notes:write",),
                side_effects=("notes_mutation",),
                default_latency_ms=160,
                latency_jitter_ms=35,
            ),
            ToolSpec(
                name="notes.update_entry",
                description="Update an existing note entry.",
                permissions=("notes:write",),
                side_effects=("notes_mutation",),
                default_latency_ms=150,
                latency_jitter_ms=35,
            ),
        ]

    def specs(self) -> List[ToolSpec]:
        return list(self._specs)

    def call(self, tool: str, args: Dict[str, Any]) -> Any:
        if tool == "notes.list_entries":
            return self.sim.list_entries(tag=args.get("tag"))
        if tool == "notes.get_entry":
            return self.sim.get_entry(str(args.get("entry_id", "")))
        if tool == "notes.create_entry":
            return self.sim.create_entry(
                title=str(args.get("title", "")),
                body=str(args.get("body", "")),
                tags=args.get("tags"),
            )
        if tool == "notes.update_entry":
            return self.sim.update_entry(
                entry_id=str(args.get("entry_id", "")),
                title=str(args["title"]) if "title" in args else None,
                body=str(args["body"]) if "body" in args else None,
                tags=args.get("tags"),
            )
        raise MCPError("unknown_tool", f"Unknown notes tool: {tool}")
