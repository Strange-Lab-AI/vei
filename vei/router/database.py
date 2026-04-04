from __future__ import annotations

from typing import Any, Dict, List, Optional

from ._pagination import (
    decode_cursor,
    encode_cursor,
    normalize_limit,
    page_rows,
    sortable,
)
from .errors import MCPError
from vei.world.api import Scenario


def _default_tables() -> Dict[str, List[Dict[str, Any]]]:
    return {
        "procurement_orders": [
            {
                "id": "PO-1001",
                "vendor": "MacroCompute",
                "amount_usd": 3199,
                "status": "PENDING_APPROVAL",
                "cost_center": "IT-OPS",
            },
            {
                "id": "PO-1002",
                "vendor": "Dell Business",
                "amount_usd": 2799,
                "status": "APPROVED",
                "cost_center": "ENG-PLATFORM",
            },
        ],
        "crm_pipeline": [
            {
                "id": "OPP-901",
                "account": "MacroCompute",
                "stage": "qualification",
                "amount_usd": 12000,
                "owner": "sam@macrocompute.example",
            }
        ],
        "approval_audit": [
            {
                "id": "APR-1",
                "entity_type": "purchase_order",
                "entity_id": "PO-1001",
                "status": "PENDING",
                "approver": "finance@macrocompute.example",
            }
        ],
    }


class DatabaseSim:
    """Deterministic database-style twin for enterprise query workflows."""

    _DEFAULT_LIMIT = 25
    _MAX_LIMIT = 200

    def __init__(self, scenario: Optional[Scenario] = None):
        seeded = (
            scenario.database_tables
            if scenario and scenario.database_tables is not None
            else _default_tables()
        )
        self.tables: Dict[str, List[Dict[str, Any]]] = {
            str(name): [dict(row) for row in rows]
            for name, rows in dict(seeded).items()
            if isinstance(rows, list)
        }

    def list_tables(
        self,
        query: Optional[str] = None,
        limit: Optional[int] = None,
        cursor: Optional[str] = None,
        sort_by: str = "table",
        sort_dir: str = "asc",
    ) -> List[Dict[str, Any]] | Dict[str, Any]:
        rows = [
            {"table": name, "row_count": len(rows)}
            for name, rows in sorted(self.tables.items())
        ]
        needle = (query or "").strip().lower()
        if needle:
            rows = [row for row in rows if needle in str(row.get("table", "")).lower()]
        sort_field = sort_by if sort_by in {"table", "row_count"} else "table"
        rows.sort(
            key=lambda row: sortable(row.get(sort_field)),
            reverse=sort_dir.lower() != "asc",
        )
        is_legacy = (
            query is None
            and limit is None
            and cursor is None
            and sort_by == "table"
            and sort_dir == "asc"
        )
        if is_legacy:
            return rows
        return page_rows(
            rows,
            key="tables",
            limit=limit,
            cursor=cursor,
            default_limit=self._DEFAULT_LIMIT,
            max_limit=self._MAX_LIMIT,
            error_code="db.invalid_cursor",
        )

    def describe_table(self, table: str) -> Dict[str, Any]:
        rows = self._table(table)
        columns = sorted({str(key) for row in rows for key in row.keys()})
        return {"table": table, "columns": columns, "row_count": len(rows)}

    def query(
        self,
        table: str,
        filters: Optional[Dict[str, Any]] = None,
        columns: Optional[List[str]] = None,
        limit: int = 20,
        offset: int = 0,
        cursor: Optional[str] = None,
        sort_by: Optional[str] = None,
        descending: bool = False,
    ) -> Dict[str, Any]:
        rows = [dict(row) for row in self._table(table)]
        if filters:
            rows = [row for row in rows if _matches_filters(row, filters)]
        if sort_by:
            rows.sort(key=lambda r: sortable(r.get(sort_by)), reverse=bool(descending))
        total = len(rows)
        start = (
            decode_cursor(cursor, error_code="db.invalid_cursor")
            if cursor
            else max(0, int(offset))
        )
        page_limit = normalize_limit(limit, default=20, max_limit=self._MAX_LIMIT)
        end = start + page_limit
        sliced = rows[start:end]
        if columns:
            keep = [str(col) for col in columns]
            sliced = [{k: v for k, v in row.items() if k in keep} for row in sliced]
        next_cursor = encode_cursor(end) if end < total else None
        return {
            "table": table,
            "rows": sliced,
            "count": len(sliced),
            "total": total,
            "offset": start,
            "next_cursor": next_cursor,
            "has_more": next_cursor is not None,
        }

    def upsert(
        self, table: str, row: Dict[str, Any], key: str = "id"
    ) -> Dict[str, Any]:
        if not isinstance(row, dict):
            raise MCPError("db.invalid_row", "db.upsert requires row object")
        table_rows = self.tables.setdefault(str(table), [])
        key_name = str(key or "id")
        row_copy = dict(row)
        if key_name not in row_copy:
            row_copy[key_name] = f"{table.upper()}-{len(table_rows) + 1}"
        row_id = row_copy[key_name]
        updated = False
        for idx, existing in enumerate(table_rows):
            if existing.get(key_name) == row_id:
                merged = dict(existing)
                merged.update(row_copy)
                table_rows[idx] = merged
                updated = True
                break
        if not updated:
            table_rows.append(row_copy)
        return {
            "ok": True,
            "table": table,
            "key": key_name,
            "id": row_id,
            "updated": updated,
        }

    def deliver(self, event: Dict[str, Any]) -> Dict[str, Any]:
        payload = dict(event or {})
        op = str(payload.get("op") or "upsert").lower()
        if op == "upsert":
            table = str(payload.get("table", "events"))
            row = payload.get("row")
            key = str(payload.get("key") or "id")
            if not isinstance(row, dict):
                raise MCPError(
                    "db.invalid_event", "database upsert delivery requires row"
                )
            return self.upsert(table=table, row=row, key=key)
        if op == "query":
            table = str(payload.get("table"))
            filters = payload.get("filters")
            columns = payload.get("columns")
            return self.query(
                table=table,
                filters=filters if isinstance(filters, dict) else None,
                columns=columns if isinstance(columns, list) else None,
                limit=int(payload.get("limit", 20)),
                offset=int(payload.get("offset", 0)),
                cursor=(
                    payload.get("cursor")
                    if isinstance(payload.get("cursor"), str)
                    else None
                ),
                sort_by=(
                    payload.get("sort_by")
                    if isinstance(payload.get("sort_by"), str)
                    else None
                ),
                descending=bool(payload.get("descending", False)),
            )
        raise MCPError("db.invalid_event", f"unsupported database delivery op: {op}")

    def _table(self, table: str) -> List[Dict[str, Any]]:
        if table not in self.tables:
            raise MCPError("db.table_not_found", f"Unknown table: {table}")
        return self.tables[table]


def _matches_filters(row: Dict[str, Any], filters: Dict[str, Any]) -> bool:
    for field, expected in filters.items():
        value = row.get(field)
        if isinstance(expected, dict):
            if "eq" in expected and value != expected.get("eq"):
                return False
            if "neq" in expected and value == expected.get("neq"):
                return False
            if "contains" in expected:
                needle = str(expected.get("contains", "")).lower()
                if needle not in str(value).lower():
                    return False
            if "starts_with" in expected:
                prefix = str(expected.get("starts_with", "")).lower()
                if not str(value).lower().startswith(prefix):
                    return False
            if "gt" in expected and not _compare_numeric(
                value, expected.get("gt"), op="gt"
            ):
                return False
            if "gte" in expected and not _compare_numeric(
                value, expected.get("gte"), op="gte"
            ):
                return False
            if "lt" in expected and not _compare_numeric(
                value, expected.get("lt"), op="lt"
            ):
                return False
            if "lte" in expected and not _compare_numeric(
                value, expected.get("lte"), op="lte"
            ):
                return False
            if "in" in expected:
                items = expected.get("in")
                if isinstance(items, list) and value not in items:
                    return False
            continue
        if value != expected:
            return False
    return True


def _compare_numeric(actual: Any, expected: Any, *, op: str) -> bool:
    try:
        left = float(actual)
        right = float(expected)
    except (TypeError, ValueError):
        return False
    if op == "gt":
        return left > right
    if op == "gte":
        return left >= right
    if op == "lt":
        return left < right
    if op == "lte":
        return left <= right
    return False
