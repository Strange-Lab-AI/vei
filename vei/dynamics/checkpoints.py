"""Narrow checkpoint record for trained dynamics backends.

Not a big registry.  Stores metadata next to checkpoint artifacts on disk.
Reference backend writes one record per checkpoint; external subprocess
backends write one when they report back.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List

from pydantic import BaseModel, Field


class CheckpointRecord(BaseModel):
    """Metadata for a trained checkpoint."""

    backend_id: str = ""
    backend_version: str = ""
    feed_schema_version: int = 1
    training_run_id: str = ""
    determinism_manifest: Dict[str, Any] = Field(default_factory=dict)
    provenance: Dict[str, Any] = Field(default_factory=dict)
    content_hash: str = ""
    notes: List[str] = Field(default_factory=list)


def save_checkpoint_record(record: CheckpointRecord, path: Path) -> Path:
    """Write a checkpoint record next to its artifacts."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        record.model_dump_json(indent=2),
        encoding="utf-8",
    )
    return path


def load_checkpoint_record(path: Path) -> CheckpointRecord:
    """Read a checkpoint record from disk."""
    return CheckpointRecord.model_validate_json(
        path.read_text(encoding="utf-8"),
    )
