"""Append-only JSONL raw event log — offline / replay default."""

from __future__ import annotations

import json
import uuid
from pathlib import Path
from typing import Any, Dict, Iterator


class JsonlRawLog:
    """Per-tenant JSONL raw log on disk."""

    def __init__(self, base_dir: Path, tenant_id: str = "default") -> None:
        self._dir = Path(base_dir) / tenant_id
        self._dir.mkdir(parents=True, exist_ok=True)
        self._path = self._dir / "raw_events.jsonl"

    def append(self, raw_record: Dict[str, Any]) -> str:
        record_id = raw_record.get("record_id", str(uuid.uuid4()))
        entry = {"record_id": record_id, **raw_record}
        with self._path.open("a", encoding="utf-8") as fh:
            fh.write(json.dumps(entry, sort_keys=True) + "\n")
        return record_id

    def iter_since(self, cursor: str) -> Iterator[Dict[str, Any]]:
        if not self._path.exists():
            return
        past_cursor = cursor == ""
        with self._path.open("r", encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if not line:
                    continue
                record = json.loads(line)
                if past_cursor:
                    yield record
                elif record.get("record_id") == cursor:
                    past_cursor = True
