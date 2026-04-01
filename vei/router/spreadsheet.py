from __future__ import annotations

from dataclasses import asdict, is_dataclass
from typing import Any, Callable, Dict, List, Optional

from vei.world import Scenario, SpreadsheetSheet, SpreadsheetWorkbook

from .errors import MCPError
from .tool_providers import PrefixToolProvider
from .tool_registry import ToolSpec


class SpreadsheetSim:
    """Deterministic spreadsheet facade for workbook-centric enterprise analysis."""

    _DEFAULT_LIMIT = 25
    _MAX_LIMIT = 200

    def __init__(self, bus, scenario: Optional[Scenario] = None):  # noqa: ANN001
        self.bus = bus
        seed = (scenario.spreadsheets or {}) if scenario else {}
        self.workbooks: Dict[str, Dict[str, Any]] = {}
        for workbook_id, workbook in seed.items():
            payload = _workbook_to_dict(workbook, workbook_id=workbook_id)
            self.workbooks[payload["workbook_id"]] = payload

    def list_workbooks(
        self,
        query: Optional[str] = None,
        owner: Optional[str] = None,
        limit: Optional[int] = None,
        cursor: Optional[str] = None,
        sort_by: str = "title",
        sort_dir: str = "asc",
    ) -> Dict[str, Any]:
        rows: List[Dict[str, Any]] = []
        needle = (query or "").strip().lower()
        for workbook in self.workbooks.values():
            if owner and workbook.get("owner") != owner:
                continue
            haystack = " ".join(
                [
                    str(workbook.get("title", "")),
                    str(workbook.get("owner", "")),
                    " ".join(str(item) for item in workbook.get("shared_with", [])),
                ]
            ).lower()
            if needle and needle not in haystack:
                continue
            rows.append(
                {
                    "id": workbook["workbook_id"],
                    "title": workbook["title"],
                    "owner": workbook.get("owner"),
                    "sheet_count": len(workbook.get("sheets", {})),
                    "shared_with_count": len(workbook.get("shared_with", [])),
                    "updated_ms": int(workbook.get("updated_ms", 0)),
                }
            )
        return _page(
            rows,
            limit=limit,
            cursor=cursor,
            key="workbooks",
            sort_by=sort_by,
            sort_dir=sort_dir,
        )

    def get_workbook(self, workbook_id: str) -> Dict[str, Any]:
        workbook = self._require_workbook(workbook_id)
        return _copy_workbook(workbook)

    def list_sheets(self, workbook_id: str) -> Dict[str, Any]:
        workbook = self._require_workbook(workbook_id)
        rows = []
        for sheet in workbook.get("sheets", {}).values():
            rows.append(
                {
                    "id": sheet["sheet_id"],
                    "title": sheet["title"],
                    "row_count": len(sheet.get("rows", [])),
                    "cell_count": len(sheet.get("cells", {})),
                    "table_count": len(sheet.get("tables", [])),
                    "filter_count": len(sheet.get("filters", [])),
                    "sort_count": len(sheet.get("sorts", [])),
                }
            )
        rows.sort(key=lambda row: str(row.get("title", "")).lower())
        return {"workbook_id": workbook_id, "sheets": rows, "count": len(rows)}

    def read_sheet(
        self,
        workbook_id: str,
        sheet_id: str,
        include_rows: bool = True,
        include_cells: bool = True,
    ) -> Dict[str, Any]:
        sheet = self._require_sheet(workbook_id, sheet_id)
        payload = {
            "workbook_id": workbook_id,
            "sheet_id": sheet_id,
            "title": sheet["title"],
            "columns": list(sheet.get("columns", [])),
            "formulas": dict(sheet.get("formulas", {})),
            "tables": [dict(item) for item in sheet.get("tables", [])],
            "filters": [dict(item) for item in sheet.get("filters", [])],
            "sorts": [dict(item) for item in sheet.get("sorts", [])],
            "updated_ms": int(sheet.get("updated_ms", 0)),
        }
        if include_rows:
            payload["rows"] = [dict(row) for row in sheet.get("rows", [])]
        if include_cells:
            payload["cells"] = dict(sheet.get("cells", {}))
        return payload

    def update_cell(
        self,
        workbook_id: str,
        sheet_id: str,
        cell: str,
        value: Any,
        note: Optional[str] = None,
    ) -> Dict[str, Any]:
        sheet = self._require_sheet(workbook_id, sheet_id)
        cell_ref = cell.strip().upper()
        if not cell_ref:
            raise MCPError("spreadsheet.invalid_cell", "cell must be non-empty")
        sheet.setdefault("cells", {})[cell_ref] = value
        self._touch_sheet(
            sheet, action="update_cell", detail={"cell": cell_ref, "note": note or ""}
        )
        return {
            "workbook_id": workbook_id,
            "sheet_id": sheet_id,
            "cell": cell_ref,
            "value": value,
        }

    def upsert_row(
        self,
        workbook_id: str,
        sheet_id: str,
        match_field: str,
        match_value: Any,
        row: Dict[str, Any],
        table_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        if not isinstance(row, dict):
            raise MCPError("spreadsheet.invalid_row", "row must be an object")
        sheet = self._require_sheet(workbook_id, sheet_id)
        rows = sheet.setdefault("rows", [])
        updated = False
        for index, existing in enumerate(rows):
            if str(existing.get(match_field)) == str(match_value):
                merged = dict(existing)
                merged.update(row)
                rows[index] = merged
                updated = True
                break
        if not updated:
            rows.append(dict(row))
        if table_id:
            table = self._require_table(sheet, table_id)
            if match_field not in table.get("columns", []):
                table.setdefault("columns", []).append(match_field)
        self._touch_sheet(
            sheet,
            action="upsert_row",
            detail={
                "match_field": match_field,
                "match_value": match_value,
                "table_id": table_id,
            },
        )
        return {
            "workbook_id": workbook_id,
            "sheet_id": sheet_id,
            "updated": updated,
            "row_count": len(rows),
        }

    def set_formula(
        self,
        workbook_id: str,
        sheet_id: str,
        cell: str,
        formula: str,
    ) -> Dict[str, Any]:
        sheet = self._require_sheet(workbook_id, sheet_id)
        cell_ref = cell.strip().upper()
        if not formula.strip():
            raise MCPError("spreadsheet.invalid_formula", "formula must be non-empty")
        sheet.setdefault("formulas", {})[cell_ref] = formula
        self._touch_sheet(sheet, action="set_formula", detail={"cell": cell_ref})
        return {
            "workbook_id": workbook_id,
            "sheet_id": sheet_id,
            "cell": cell_ref,
            "formula": formula,
        }

    def apply_filter(
        self,
        workbook_id: str,
        sheet_id: str,
        column: str,
        equals: Optional[Any] = None,
        contains: Optional[str] = None,
    ) -> Dict[str, Any]:
        if equals is None and contains is None:
            raise MCPError(
                "spreadsheet.invalid_filter",
                "apply_filter requires either equals or contains",
            )
        sheet = self._require_sheet(workbook_id, sheet_id)
        filter_spec = {
            "column": column,
            "equals": equals,
            "contains": contains,
        }
        sheet.setdefault("filters", []).append(filter_spec)
        self._touch_sheet(sheet, action="apply_filter", detail=filter_spec)
        return {
            "workbook_id": workbook_id,
            "sheet_id": sheet_id,
            "filter_count": len(sheet.get("filters", [])),
        }

    def apply_sort(
        self,
        workbook_id: str,
        sheet_id: str,
        column: str,
        direction: str = "asc",
    ) -> Dict[str, Any]:
        sheet = self._require_sheet(workbook_id, sheet_id)
        normalized_direction = direction.strip().lower()
        if normalized_direction not in {"asc", "desc"}:
            raise MCPError(
                "spreadsheet.invalid_sort_direction",
                f"Unsupported sort direction: {direction}",
            )
        sheet.setdefault("sorts", []).append(
            {"column": column, "direction": normalized_direction}
        )
        rows = list(sheet.get("rows", []))
        rows.sort(
            key=lambda row: _sortable(row.get(column)),
            reverse=normalized_direction == "desc",
        )
        sheet["rows"] = rows
        self._touch_sheet(
            sheet,
            action="apply_sort",
            detail={"column": column, "direction": normalized_direction},
        )
        return {
            "workbook_id": workbook_id,
            "sheet_id": sheet_id,
            "sort_count": len(sheet.get("sorts", [])),
        }

    def share_workbook(
        self,
        workbook_id: str,
        principal: str,
        role: str = "viewer",
    ) -> Dict[str, Any]:
        workbook = self._require_workbook(workbook_id)
        shared_with = list(workbook.get("shared_with", []))
        if principal not in shared_with:
            shared_with.append(principal)
        permissions = dict(workbook.get("permissions", {}))
        permissions[principal] = role
        workbook["shared_with"] = shared_with
        workbook["permissions"] = permissions
        workbook["updated_ms"] = self.bus.clock_ms
        return {
            "workbook_id": workbook_id,
            "principal": principal,
            "role": role,
            "shared_with_count": len(shared_with),
        }

    def summary(self) -> str:
        if not self.workbooks:
            return "Spreadsheet: no workbooks"
        latest = max(
            self.workbooks.values(), key=lambda item: int(item.get("updated_ms", 0))
        )
        return (
            "Spreadsheet: "
            f"{len(self.workbooks)} workbooks (latest: {latest.get('title', latest['workbook_id'])})"
        )

    def action_menu(self) -> List[Dict[str, Any]]:
        return [
            {
                "tool": "spreadsheet.list_workbooks",
                "args_schema": {"query": "str?", "owner": "str?", "limit": "int?"},
            },
            {
                "tool": "spreadsheet.read_sheet",
                "args_schema": {
                    "workbook_id": "str",
                    "sheet_id": "str",
                    "include_rows": "bool?",
                    "include_cells": "bool?",
                },
            },
            {
                "tool": "spreadsheet.upsert_row",
                "args_schema": {
                    "workbook_id": "str",
                    "sheet_id": "str",
                    "match_field": "str",
                    "match_value": "str|int|float",
                    "row": "object",
                    "table_id": "str?",
                },
            },
            {
                "tool": "spreadsheet.update_cell",
                "args_schema": {
                    "workbook_id": "str",
                    "sheet_id": "str",
                    "cell": "str",
                    "value": "any",
                    "note": "str?",
                },
            },
            {
                "tool": "spreadsheet.set_formula",
                "args_schema": {
                    "workbook_id": "str",
                    "sheet_id": "str",
                    "cell": "str",
                    "formula": "str",
                },
            },
            {
                "tool": "spreadsheet.apply_filter",
                "args_schema": {
                    "workbook_id": "str",
                    "sheet_id": "str",
                    "column": "str",
                    "equals": "any?",
                    "contains": "str?",
                },
            },
            {
                "tool": "spreadsheet.apply_sort",
                "args_schema": {
                    "workbook_id": "str",
                    "sheet_id": "str",
                    "column": "str",
                    "direction": "asc|desc?",
                },
            },
            {
                "tool": "spreadsheet.share_workbook",
                "args_schema": {
                    "workbook_id": "str",
                    "principal": "str",
                    "role": "str?",
                },
            },
        ]

    def export_state(self) -> Dict[str, Any]:
        return {"workbooks": _copy_workbooks(self.workbooks)}

    def import_state(self, state: Dict[str, Any]) -> None:
        workbooks = state.get("workbooks", {}) if isinstance(state, dict) else {}
        self.workbooks = {
            workbook_id: _workbook_to_dict(payload, workbook_id=workbook_id)
            for workbook_id, payload in workbooks.items()
            if isinstance(payload, dict)
        }

    def _require_workbook(self, workbook_id: str) -> Dict[str, Any]:
        workbook = self.workbooks.get(workbook_id)
        if not workbook:
            raise MCPError(
                "spreadsheet.workbook_not_found", f"Unknown workbook: {workbook_id}"
            )
        return workbook

    def _require_sheet(self, workbook_id: str, sheet_id: str) -> Dict[str, Any]:
        workbook = self._require_workbook(workbook_id)
        sheet = workbook.get("sheets", {}).get(sheet_id)
        if not sheet:
            raise MCPError(
                "spreadsheet.sheet_not_found",
                f"Unknown sheet {sheet_id} in workbook {workbook_id}",
            )
        return sheet

    def _require_table(self, sheet: Dict[str, Any], table_id: str) -> Dict[str, Any]:
        for table in sheet.get("tables", []):
            if table.get("table_id") == table_id:
                return table
        raise MCPError("spreadsheet.table_not_found", f"Unknown table: {table_id}")

    def _touch_sheet(
        self, sheet: Dict[str, Any], *, action: str, detail: Dict[str, Any]
    ) -> None:
        sheet["updated_ms"] = self.bus.clock_ms
        history = list(sheet.get("history", []))
        history.append({"action": action, **detail, "time_ms": self.bus.clock_ms})
        sheet["history"] = history[-50:]


class SpreadsheetToolProvider(PrefixToolProvider):
    def __init__(self, sim: SpreadsheetSim):
        super().__init__("spreadsheet", prefixes=("spreadsheet.",))
        self.sim = sim
        self._specs: List[ToolSpec] = [
            ToolSpec(
                name="spreadsheet.list_workbooks",
                description="List spreadsheet workbooks and ownership metadata.",
                permissions=("spreadsheet:read",),
                default_latency_ms=240,
                latency_jitter_ms=70,
            ),
            ToolSpec(
                name="spreadsheet.get_workbook",
                description="Fetch workbook metadata, sheets, and sharing state.",
                permissions=("spreadsheet:read",),
                default_latency_ms=230,
                latency_jitter_ms=70,
            ),
            ToolSpec(
                name="spreadsheet.list_sheets",
                description="List sheets inside a workbook.",
                permissions=("spreadsheet:read",),
                default_latency_ms=220,
                latency_jitter_ms=60,
            ),
            ToolSpec(
                name="spreadsheet.read_sheet",
                description="Read a sheet with rows, cells, formulas, filters, and sorts.",
                permissions=("spreadsheet:read",),
                default_latency_ms=250,
                latency_jitter_ms=70,
            ),
            ToolSpec(
                name="spreadsheet.update_cell",
                description="Update one workbook cell.",
                permissions=("spreadsheet:write",),
                side_effects=("spreadsheet_mutation",),
                default_latency_ms=320,
                latency_jitter_ms=100,
            ),
            ToolSpec(
                name="spreadsheet.upsert_row",
                description="Insert or update a row in a sheet table.",
                permissions=("spreadsheet:write",),
                side_effects=("spreadsheet_mutation",),
                default_latency_ms=340,
                latency_jitter_ms=100,
            ),
            ToolSpec(
                name="spreadsheet.set_formula",
                description="Attach or update a formula in a cell.",
                permissions=("spreadsheet:write",),
                side_effects=("spreadsheet_mutation",),
                default_latency_ms=310,
                latency_jitter_ms=90,
            ),
            ToolSpec(
                name="spreadsheet.apply_filter",
                description="Apply a filter to a sheet.",
                permissions=("spreadsheet:write",),
                side_effects=("spreadsheet_mutation",),
                default_latency_ms=300,
                latency_jitter_ms=90,
            ),
            ToolSpec(
                name="spreadsheet.apply_sort",
                description="Apply a sort to a sheet.",
                permissions=("spreadsheet:write",),
                side_effects=("spreadsheet_mutation",),
                default_latency_ms=300,
                latency_jitter_ms=90,
            ),
            ToolSpec(
                name="spreadsheet.share_workbook",
                description="Share a workbook with a principal and role.",
                permissions=("spreadsheet:write",),
                side_effects=("spreadsheet_mutation",),
                default_latency_ms=280,
                latency_jitter_ms=80,
            ),
        ]
        self._handlers: Dict[str, Callable[..., Any]] = {
            "spreadsheet.list_workbooks": self.sim.list_workbooks,
            "spreadsheet.get_workbook": self.sim.get_workbook,
            "spreadsheet.list_sheets": self.sim.list_sheets,
            "spreadsheet.read_sheet": self.sim.read_sheet,
            "spreadsheet.update_cell": self.sim.update_cell,
            "spreadsheet.upsert_row": self.sim.upsert_row,
            "spreadsheet.set_formula": self.sim.set_formula,
            "spreadsheet.apply_filter": self.sim.apply_filter,
            "spreadsheet.apply_sort": self.sim.apply_sort,
            "spreadsheet.share_workbook": self.sim.share_workbook,
        }

    def specs(self) -> List[ToolSpec]:
        return list(self._specs)

    def call(self, tool: str, args: Dict[str, Any]) -> Any:
        handler = self._handlers.get(tool)
        if not handler:
            raise MCPError("unknown_tool", f"No such tool: {tool}")
        try:
            return handler(**(args or {}))
        except TypeError as exc:
            raise MCPError("invalid_args", str(exc)) from exc


def _page(
    rows: List[Dict[str, Any]],
    *,
    limit: Optional[int],
    cursor: Optional[str],
    key: str,
    sort_by: str,
    sort_dir: str,
) -> Dict[str, Any]:
    if rows:
        sort_field = sort_by if sort_by in rows[0] else next(iter(rows[0]))
        rows.sort(
            key=lambda row: _sortable(row.get(sort_field)),
            reverse=sort_dir.lower() != "asc",
        )
    start = _decode_cursor(cursor)
    page_limit = _normalize_limit(limit)
    sliced = rows[start : start + page_limit]
    next_cursor = (
        _encode_cursor(start + page_limit) if (start + page_limit) < len(rows) else None
    )
    return {
        key: sliced,
        "count": len(sliced),
        "total": len(rows),
        "next_cursor": next_cursor,
        "has_more": next_cursor is not None,
    }


def _normalize_limit(limit: Optional[int]) -> int:
    if limit is None:
        return SpreadsheetSim._DEFAULT_LIMIT
    return max(1, min(int(limit), SpreadsheetSim._MAX_LIMIT))


def _encode_cursor(index: int) -> str:
    return f"idx:{index}"


def _decode_cursor(cursor: Optional[str]) -> int:
    if not cursor:
        return 0
    if not cursor.startswith("idx:"):
        raise MCPError("spreadsheet.invalid_cursor", f"Invalid cursor: {cursor}")
    try:
        return max(0, int(cursor.split(":", 1)[1]))
    except ValueError as exc:
        raise MCPError(
            "spreadsheet.invalid_cursor", f"Invalid cursor: {cursor}"
        ) from exc


def _workbook_to_dict(value: Any, *, workbook_id: str) -> Dict[str, Any]:
    payload = _jsonable(value)
    if not isinstance(payload, dict):
        raise MCPError(
            "spreadsheet.invalid_workbook",
            f"Workbook seed must be an object: {workbook_id}",
        )
    sheets: Dict[str, Dict[str, Any]] = {}
    raw_sheets = payload.get("sheets", [])
    if isinstance(raw_sheets, dict):
        iterable = raw_sheets.values()
    else:
        iterable = raw_sheets
    for raw_sheet in iterable:
        sheet_payload = _sheet_to_dict(raw_sheet)
        sheets[sheet_payload["sheet_id"]] = sheet_payload
    return {
        "workbook_id": str(payload.get("workbook_id", workbook_id)),
        "title": str(payload.get("title", workbook_id)),
        "owner": payload.get("owner"),
        "shared_with": list(payload.get("shared_with", [])),
        "permissions": dict(payload.get("permissions", {})),
        "sheets": sheets,
        "updated_ms": int(payload.get("updated_ms", 0)),
    }


def _sheet_to_dict(value: Any) -> Dict[str, Any]:
    payload = _jsonable(value)
    if not isinstance(payload, dict):
        raise MCPError("spreadsheet.invalid_sheet", "Sheet seed must be an object")
    sheet_id = str(payload.get("sheet_id", "")).strip()
    if not sheet_id:
        raise MCPError("spreadsheet.invalid_sheet", "Sheet seed must include sheet_id")
    return {
        "sheet_id": sheet_id,
        "title": str(payload.get("title", sheet_id)),
        "columns": list(payload.get("columns", [])),
        "rows": [dict(row) for row in payload.get("rows", [])],
        "cells": dict(payload.get("cells", {})),
        "formulas": dict(payload.get("formulas", {})),
        "tables": [dict(table) for table in payload.get("tables", [])],
        "filters": [dict(item) for item in payload.get("filters", [])],
        "sorts": [dict(item) for item in payload.get("sorts", [])],
        "history": [dict(item) for item in payload.get("history", [])],
        "updated_ms": int(payload.get("updated_ms", 0)),
    }


def _copy_workbook(workbook: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "workbook_id": workbook["workbook_id"],
        "title": workbook["title"],
        "owner": workbook.get("owner"),
        "shared_with": list(workbook.get("shared_with", [])),
        "permissions": dict(workbook.get("permissions", {})),
        "updated_ms": int(workbook.get("updated_ms", 0)),
        "sheets": {
            sheet_id: {
                "sheet_id": sheet["sheet_id"],
                "title": sheet["title"],
                "columns": list(sheet.get("columns", [])),
                "rows": [dict(row) for row in sheet.get("rows", [])],
                "cells": dict(sheet.get("cells", {})),
                "formulas": dict(sheet.get("formulas", {})),
                "tables": [dict(item) for item in sheet.get("tables", [])],
                "filters": [dict(item) for item in sheet.get("filters", [])],
                "sorts": [dict(item) for item in sheet.get("sorts", [])],
                "updated_ms": int(sheet.get("updated_ms", 0)),
            }
            for sheet_id, sheet in workbook.get("sheets", {}).items()
        },
    }


def _copy_workbooks(workbooks: Dict[str, Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    return {
        workbook_id: _copy_workbook(workbook)
        for workbook_id, workbook in workbooks.items()
    }


def _jsonable(value: Any) -> Any:
    if hasattr(value, "model_dump"):
        return value.model_dump()
    if is_dataclass(value):
        return asdict(value)
    if isinstance(value, SpreadsheetWorkbook):
        return asdict(value)
    if isinstance(value, SpreadsheetSheet):
        return asdict(value)
    if isinstance(value, dict):
        return {str(key): _jsonable(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_jsonable(item) for item in value]
    return value


def _sortable(value: Any) -> Any:
    if value is None:
        return ""
    return str(value).lower()
