from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from vei.context.models import ContextProviderConfig, ContextSourceResult

from .base import iso_now


class NotionContextProvider:
    name = "notion"

    def capture(self, config: ContextProviderConfig) -> ContextSourceResult:
        root = _resolve_path(config.base_url)
        if root is None:
            raise ValueError(
                "notion provider requires base_url pointing to a local export"
            )
        payload = _load_notion_export(root)
        return ContextSourceResult(
            provider="notion",
            captured_at=iso_now(),
            status="ok" if payload["pages"] or payload["databases"] else "empty",
            record_counts={
                "pages": len(payload["pages"]),
                "databases": len(payload["databases"]),
                "blocks": len(payload["blocks"]),
            },
            data=payload,
        )


def capture_from_export(export_path: str | Path) -> ContextSourceResult:
    return NotionContextProvider().capture(
        ContextProviderConfig(provider="notion", base_url=str(export_path))
    )


def _resolve_path(raw: str | None) -> Path | None:
    if not raw:
        return None
    path = Path(raw).expanduser().resolve()
    if not path.exists():
        return None
    return path


def _load_notion_export(root: Path) -> dict[str, Any]:
    pages: list[dict[str, Any]] = []
    databases: list[dict[str, Any]] = []
    blocks: list[dict[str, Any]] = []

    if root.is_file():
        raw = json.loads(root.read_text(encoding="utf-8"))
        if isinstance(raw, dict):
            pages.extend(_normalize_pages(raw.get("pages") or []))
            databases.extend(_normalize_pages(raw.get("databases") or []))
            blocks.extend(_normalize_pages(raw.get("blocks") or []))
        elif isinstance(raw, list):
            pages.extend(_normalize_pages(raw))
        return {"pages": pages, "databases": databases, "blocks": blocks}

    for path in sorted(root.rglob("*")):
        if path.is_dir():
            continue
        if path.suffix.lower() == ".md":
            pages.append(
                {
                    "page_id": path.stem,
                    "title": path.stem.replace("-", " ").replace("_", " ").title(),
                    "body": path.read_text(encoding="utf-8"),
                    "source_path": str(path),
                }
            )
            continue
        if path.suffix.lower() != ".json":
            continue
        raw = json.loads(path.read_text(encoding="utf-8"))
        bucket = databases if "database" in path.stem.lower() else pages
        if isinstance(raw, list):
            bucket.extend(_normalize_pages(raw, source_path=path))
        elif isinstance(raw, dict):
            if "results" in raw and isinstance(raw["results"], list):
                bucket.extend(_normalize_pages(raw["results"], source_path=path))
            else:
                bucket.extend(_normalize_pages([raw], source_path=path))
    return {"pages": pages, "databases": databases, "blocks": blocks}


def _normalize_pages(
    rows: list[Any],
    *,
    source_path: Path | None = None,
) -> list[dict[str, Any]]:
    normalized: list[dict[str, Any]] = []
    for index, item in enumerate(rows, start=1):
        if not isinstance(item, dict):
            continue
        title = str(
            item.get("title")
            or item.get("name")
            or item.get("page_title")
            or f"Notion Page {index}"
        )
        body = str(
            item.get("body")
            or item.get("content")
            or item.get("text")
            or json.dumps(item, indent=2, sort_keys=True)
        )
        normalized.append(
            {
                "page_id": str(item.get("page_id") or item.get("id") or title),
                "title": title,
                "body": body,
                "tags": [str(tag) for tag in (item.get("tags") or [])],
                "linked_object_refs": [
                    str(ref) for ref in (item.get("linked_object_refs") or [])
                ],
                "source_path": str(source_path) if source_path is not None else "",
            }
        )
    return normalized
