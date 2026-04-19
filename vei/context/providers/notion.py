from __future__ import annotations

import csv
import json
import re
import zipfile
from dataclasses import dataclass
from datetime import UTC, datetime
from io import BytesIO
from pathlib import Path
from typing import Any, Iterator

from vei.context.models import ContextProviderConfig, ContextSourceResult

from .base import iso_now

_UUID_SUFFIX_RE = re.compile(r"\s[0-9a-f]{32}$", re.IGNORECASE)


@dataclass(frozen=True)
class _NotionTextFile:
    source_path: str
    suffix: str
    text: str
    modified_at: str = ""


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
        suffix = root.suffix.lower()
        if suffix == ".zip":
            file_iter = _iter_archive_files(root)
        elif suffix == ".json":
            raw = json.loads(root.read_text(encoding="utf-8"))
            return _normalized_json_payload(raw, source_path=str(root))
        elif suffix in {".md", ".csv"}:
            modified_at = (
                datetime.fromtimestamp(
                    root.stat().st_mtime,
                    UTC,
                )
                .isoformat()
                .replace("+00:00", "Z")
            )
            file_iter = iter(
                [
                    _NotionTextFile(
                        source_path=str(root),
                        suffix=suffix,
                        text=root.read_text(encoding="utf-8-sig", errors="replace"),
                        modified_at=modified_at,
                    )
                ]
            )
        else:
            return {"pages": [], "databases": [], "blocks": []}
    else:
        file_iter = _iter_directory_files(root)

    for file in file_iter:
        if file.suffix == ".md":
            pages.append(_markdown_page(file))
            continue
        if file.suffix == ".csv":
            pages.append(_csv_summary_page(file))
            continue
        if file.suffix != ".json":
            continue
        raw = json.loads(file.text)
        normalized = _normalized_json_payload(raw, source_path=file.source_path)
        pages.extend(normalized["pages"])
        databases.extend(normalized["databases"])
        blocks.extend(normalized["blocks"])
    return {"pages": pages, "databases": databases, "blocks": blocks}


def _normalize_pages(
    rows: list[Any],
    *,
    source_path: str | None = None,
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
                "owner": str(item.get("owner") or item.get("author") or "").strip(),
                "created": str(
                    item.get("created") or item.get("created_at") or ""
                ).strip(),
                "updated": str(
                    item.get("updated")
                    or item.get("updated_at")
                    or item.get("modified_time")
                    or ""
                ).strip(),
                "source_path": source_path or "",
            }
        )
    return normalized


def _normalized_json_payload(
    raw: Any,
    *,
    source_path: str,
) -> dict[str, list[dict[str, Any]]]:
    pages: list[dict[str, Any]] = []
    databases: list[dict[str, Any]] = []
    blocks: list[dict[str, Any]] = []
    if isinstance(raw, dict):
        pages.extend(_normalize_pages(raw.get("pages") or [], source_path=source_path))
        databases.extend(
            _normalize_pages(raw.get("databases") or [], source_path=source_path)
        )
        blocks.extend(
            _normalize_pages(raw.get("blocks") or [], source_path=source_path)
        )
        if not pages and not databases and not blocks:
            bucket = databases if "database" in source_path.lower() else pages
            if "results" in raw and isinstance(raw["results"], list):
                bucket.extend(_normalize_pages(raw["results"], source_path=source_path))
            else:
                bucket.extend(_normalize_pages([raw], source_path=source_path))
        return {"pages": pages, "databases": databases, "blocks": blocks}
    if isinstance(raw, list):
        return {
            "pages": _normalize_pages(raw, source_path=source_path),
            "databases": [],
            "blocks": [],
        }
    return {"pages": [], "databases": [], "blocks": []}


def _iter_directory_files(root: Path) -> Iterator[_NotionTextFile]:
    for path in sorted(root.rglob("*")):
        if path.is_dir():
            continue
        if path.name.startswith("._") or "__MACOSX" in path.parts:
            continue
        suffix = path.suffix.lower()
        if suffix not in {".md", ".json", ".csv"}:
            continue
        modified_at = (
            datetime.fromtimestamp(
                path.stat().st_mtime,
                UTC,
            )
            .isoformat()
            .replace("+00:00", "Z")
        )
        yield _NotionTextFile(
            source_path=str(path),
            suffix=suffix,
            text=path.read_text(encoding="utf-8-sig", errors="replace"),
            modified_at=modified_at,
        )


def _iter_archive_files(path: Path) -> Iterator[_NotionTextFile]:
    yield from _iter_zip_bytes(path.read_bytes(), prefix=str(path))


def _iter_zip_bytes(data: bytes, *, prefix: str) -> Iterator[_NotionTextFile]:
    with zipfile.ZipFile(BytesIO(data)) as archive:
        for info in archive.infolist():
            if info.is_dir():
                continue
            if info.filename.startswith("__MACOSX/") or "/._" in info.filename:
                continue
            member_path = f"{prefix}!/{info.filename}"
            if info.filename.lower().endswith(".zip"):
                yield from _iter_zip_bytes(
                    archive.read(info),
                    prefix=member_path,
                )
                continue
            suffix = Path(info.filename).suffix.lower()
            if suffix not in {".md", ".json", ".csv"}:
                continue
            modified_at = (
                datetime(*info.date_time, tzinfo=UTC)
                .isoformat()
                .replace(
                    "+00:00",
                    "Z",
                )
            )
            yield _NotionTextFile(
                source_path=member_path,
                suffix=suffix,
                text=archive.read(info).decode("utf-8-sig", errors="replace"),
                modified_at=modified_at,
            )


def _markdown_page(file: _NotionTextFile) -> dict[str, Any]:
    title = _clean_title(Path(file.source_path).stem)
    owner = ""
    created = ""
    updated = ""
    for line in file.text.splitlines()[:12]:
        stripped = line.strip()
        if not stripped:
            continue
        if stripped.startswith("# ") and title:
            title = stripped[2:].strip()
            continue
        key, separator, value = stripped.partition(":")
        if separator != ":":
            continue
        normalized_key = key.strip().lower()
        normalized_value = value.strip()
        if normalized_key == "owner":
            owner = normalized_value
            continue
        if normalized_key == "created time":
            created = normalized_value
            continue
        if normalized_key == "last edited time":
            updated = normalized_value

    if not updated:
        updated = file.modified_at
    if not created:
        created = updated
    return {
        "page_id": Path(file.source_path).stem,
        "title": title,
        "body": file.text,
        "owner": owner,
        "created": created,
        "updated": updated,
        "source_path": file.source_path,
    }


def _csv_summary_page(file: _NotionTextFile) -> dict[str, Any]:
    reader = csv.DictReader(file.text.splitlines())
    rows = [
        row for row in reader if any(str(value or "").strip() for value in row.values())
    ]
    lines = [f"{len(rows)} rows"]
    for row in rows[:20]:
        row_parts = [
            f"{key}: {str(value).strip()}"
            for key, value in row.items()
            if str(value or "").strip()
        ]
        if row_parts:
            lines.append(" | ".join(row_parts))
    title = _clean_title(Path(file.source_path).stem)
    return {
        "page_id": Path(file.source_path).stem,
        "title": title,
        "body": "\n".join(lines).strip(),
        "created": file.modified_at,
        "updated": file.modified_at,
        "source_path": file.source_path,
        "tags": ["database"],
    }


def _clean_title(raw: str) -> str:
    cleaned = _UUID_SUFFIX_RE.sub("", raw).strip()
    cleaned = cleaned.replace("_", " ").replace("-", " ").strip()
    return cleaned or raw.strip()
