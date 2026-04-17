from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from vei.context.models import ContextProviderConfig, ContextSourceResult

from .base import iso_now


class GranolaContextProvider:
    name = "granola"

    def capture(self, config: ContextProviderConfig) -> ContextSourceResult:
        root = _resolve_path(config.base_url)
        if root is None:
            raise ValueError(
                "granola provider requires base_url pointing to a local export"
            )
        transcripts = _load_granola_export(root)
        return ContextSourceResult(
            provider="granola",
            captured_at=iso_now(),
            status="ok" if transcripts else "empty",
            record_counts={"transcripts": len(transcripts)},
            data={"transcripts": transcripts},
        )


def capture_from_export(export_path: str | Path) -> ContextSourceResult:
    return GranolaContextProvider().capture(
        ContextProviderConfig(provider="granola", base_url=str(export_path))
    )


def _resolve_path(raw: str | None) -> Path | None:
    if not raw:
        return None
    path = Path(raw).expanduser().resolve()
    if not path.exists():
        return None
    return path


def _load_granola_export(root: Path) -> list[dict[str, Any]]:
    if root.is_file():
        if root.suffix.lower() == ".md":
            return [_markdown_transcript(root)]
        raw = json.loads(root.read_text(encoding="utf-8"))
        if isinstance(raw, dict) and "transcripts" in raw:
            return [item for item in raw["transcripts"] if isinstance(item, dict)]
        if isinstance(raw, list):
            return [item for item in raw if isinstance(item, dict)]
        if isinstance(raw, dict):
            return [raw]
        return []
    transcripts: list[dict[str, Any]] = []
    for path in sorted(root.rglob("*")):
        if path.is_dir():
            continue
        if path.suffix.lower() == ".md":
            transcripts.append(_markdown_transcript(path))
            continue
        if path.suffix.lower() != ".json":
            continue
        raw = json.loads(path.read_text(encoding="utf-8"))
        if isinstance(raw, dict) and "transcripts" in raw:
            transcripts.extend(
                item for item in raw["transcripts"] if isinstance(item, dict)
            )
            continue
        if isinstance(raw, list):
            transcripts.extend(item for item in raw if isinstance(item, dict))
            continue
        if isinstance(raw, dict):
            transcripts.append(raw)
    return transcripts


def _markdown_transcript(path: Path) -> dict[str, Any]:
    return {
        "transcript_id": path.stem,
        "title": path.stem.replace("-", " ").replace("_", " ").title(),
        "body": path.read_text(encoding="utf-8"),
        "source_path": str(path),
    }
