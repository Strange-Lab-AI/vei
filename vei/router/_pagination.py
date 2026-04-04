"""Shared pagination helpers for router surface modules."""

from __future__ import annotations

from typing import Any, Dict, List

from .errors import MCPError


def normalize_limit(
    limit: int | None, *, default: int = 25, max_limit: int = 200
) -> int:
    if limit is None:
        return default
    if limit < 1:
        return 1
    return min(max_limit, int(limit))


def decode_cursor(
    cursor: str | None, *, prefix: str = "ofs", error_code: str = "invalid_cursor"
) -> int:
    if not cursor:
        return 0
    tag = f"{prefix}:"
    if not cursor.startswith(tag):
        raise MCPError(error_code, f"Cursor must use '{tag}<offset>' format")
    try:
        value = int(cursor.split(":", 1)[1])
    except ValueError as exc:
        raise MCPError(error_code, f"Invalid cursor: {cursor}") from exc
    return max(0, value)


def encode_cursor(offset: int, *, prefix: str = "ofs") -> str:
    return f"{prefix}:{max(0, int(offset))}"


def sortable(value: object, *, lowercase: bool = False) -> object:
    if value is None:
        return ""
    if not lowercase:
        if isinstance(value, (int, float, str)):
            return value
        return str(value)
    return str(value).lower()


def page_rows(
    rows: List[Dict[str, Any]],
    *,
    limit: int | None,
    cursor: str | None,
    key: str,
    default_limit: int = 25,
    max_limit: int = 200,
    cursor_prefix: str = "ofs",
    error_code: str = "invalid_cursor",
) -> Dict[str, Any]:
    page_limit = normalize_limit(limit, default=default_limit, max_limit=max_limit)
    start = decode_cursor(cursor, prefix=cursor_prefix, error_code=error_code)
    sliced = rows[start : start + page_limit]
    next_cursor = (
        encode_cursor(start + page_limit, prefix=cursor_prefix)
        if (start + page_limit) < len(rows)
        else None
    )
    return {
        key: sliced,
        "count": len(sliced),
        "total": len(rows),
        "next_cursor": next_cursor,
        "has_more": next_cursor is not None,
    }
