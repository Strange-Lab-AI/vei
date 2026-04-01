from __future__ import annotations

from typing import Any, Dict, List, Optional

from vei.world.api import Scenario
from .errors import MCPError


class ErpSim:
    """Minimal, deterministic ERP twin exposing MCP-style tools.

    Scope (v0):
    - POs: create/get/list
    - Goods receipts: receive against PO
    - Invoices: submit/get/list
    - Three-way match: PO vs receipt vs invoice
    - Payments: post payment against invoice

    Data is kept in-memory and keyed by simple string IDs for determinism.
    Amount math is integer cents to avoid FP drift.
    """

    _DEFAULT_LIMIT = 25
    _MAX_LIMIT = 200

    def __init__(self, bus: Any, scenario: Optional[Scenario] = None):
        self.bus = bus
        self._po_seq = 1
        self._inv_seq = 1
        self._rcpt_seq = 1
        self.currency_default = "USD"
        # Deterministic error injection (default off). Set VEI_ERP_ERROR_RATE like '0.05' for 5%.
        try:
            import os

            self.error_rate = float(os.environ.get("VEI_ERP_ERROR_RATE", "0"))
        except Exception:
            self.error_rate = 0.0

        # Stores
        self.pos: Dict[str, Dict[str, Any]] = {}
        self.invoices: Dict[str, Dict[str, Any]] = {}
        self.receipts: Dict[str, Dict[str, Any]] = {}

    # Helpers
    def _money_to_cents(self, x: float | int | str) -> int:
        try:
            return int(round(float(x) * 100))
        except Exception:
            return 0

    def _cents_to_money(self, c: int) -> float:
        return round(c / 100.0, 2)

    # Tools
    def create_po(
        self, vendor: str, currency: str, lines: List[Dict[str, Any]]
    ) -> Dict[str, Any]:
        po_id = f"PO-{self._po_seq}"
        self._po_seq += 1
        total_cents = 0
        po_lines: List[Dict[str, Any]] = []
        for i, ln in enumerate(lines, start=1):
            qty = int(ln.get("qty", 0))
            unit_cents = self._money_to_cents(ln.get("unit_price", 0))
            line_total = qty * unit_cents
            total_cents += line_total
            po_lines.append(
                {
                    "line_no": i,
                    "item_id": str(ln.get("item_id", i)),
                    "desc": ln.get("desc", ""),
                    "qty": qty,
                    "unit_price": self._cents_to_money(unit_cents),
                    "amount": self._cents_to_money(line_total),
                }
            )
        po = {
            "id": po_id,
            "vendor": vendor,
            "currency": currency or self.currency_default,
            "status": "OPEN",
            "lines": po_lines,
            "amount": self._cents_to_money(total_cents),
            "created_ms": self.bus.clock_ms,
            "updated_ms": self.bus.clock_ms,
            "received_qty_by_item": {
                str(line_item["item_id"]): 0 for line_item in po_lines
            },
        }
        self.pos[po_id] = po
        return {"id": po_id, "amount": po["amount"], "currency": po["currency"]}

    def get_po(self, id: str) -> Dict[str, Any]:
        po = self.pos.get(id)
        if not po:
            raise MCPError("unknown_po", f"Unknown PO: {id}")
        return po

    def list_pos(
        self,
        vendor: str | None = None,
        status: str | None = None,
        currency: str | None = None,
        limit: int | None = None,
        cursor: str | None = None,
        sort_by: str = "created_ms",
        sort_dir: str = "desc",
    ) -> List[Dict[str, Any]] | Dict[str, Any]:
        rows = list(self.pos.values())
        if vendor:
            needle = vendor.strip().lower()
            rows = [row for row in rows if needle in str(row.get("vendor", "")).lower()]
        if status:
            wanted_status = status.strip().upper()
            rows = [
                row
                for row in rows
                if str(row.get("status", "")).upper() == wanted_status
            ]
        if currency:
            wanted_currency = currency.strip().upper()
            rows = [
                row
                for row in rows
                if str(row.get("currency", "")).strip().upper() == wanted_currency
            ]
        sort_field = (
            sort_by
            if sort_by in {"created_ms", "updated_ms", "amount", "vendor"}
            else "created_ms"
        )
        rows.sort(
            key=lambda row: _sortable(row.get(sort_field)),
            reverse=sort_dir.lower() != "asc",
        )

        is_legacy = (
            vendor is None
            and status is None
            and currency is None
            and limit is None
            and cursor is None
            and sort_by == "created_ms"
            and sort_dir == "desc"
        )
        if is_legacy:
            return rows
        return _page_rows(rows, key="purchase_orders", limit=limit, cursor=cursor)

    def receive_goods(self, po_id: str, lines: List[Dict[str, Any]]) -> Dict[str, Any]:
        po = self.pos.get(po_id)
        if not po:
            raise MCPError("unknown_po", f"Unknown PO: {po_id}")
        rcpt_id = f"RCPT-{self._rcpt_seq}"
        self._rcpt_seq += 1
        item_to_ordered = {
            str(line_item["item_id"]): int(line_item.get("qty", 0))
            for line_item in po.get("lines", [])
        }
        received_qty = dict(po.get("received_qty_by_item", {}))
        rcpt_lines = [
            {
                "item_id": str(ln.get("item_id")),
                "qty": int(ln.get("qty", 0)),
            }
            for ln in lines
        ]
        for line_item in rcpt_lines:
            item_id = str(line_item["item_id"])
            qty = int(line_item["qty"])
            if item_id not in item_to_ordered:
                raise MCPError(
                    "unknown_item", f"Item {item_id} is not present on PO {po_id}"
                )
            new_total = int(received_qty.get(item_id, 0)) + qty
            if new_total > int(item_to_ordered[item_id]):
                raise MCPError(
                    "qty_exceeds_po",
                    f"Received qty for {item_id} exceeds ordered qty on {po_id}",
                )
            received_qty[item_id] = new_total

        rcpt = {
            "id": rcpt_id,
            "po_id": po_id,
            "lines": rcpt_lines,
            "time_ms": self.bus.clock_ms,
        }
        self.receipts[rcpt_id] = rcpt
        all_received = all(
            int(received_qty.get(item_id, 0)) >= int(qty)
            for item_id, qty in item_to_ordered.items()
        )
        po["received_qty_by_item"] = received_qty
        po["status"] = "RECEIVED" if all_received else "PARTIALLY_RECEIVED"
        po["updated_ms"] = self.bus.clock_ms
        return {"id": rcpt_id, "po_status": po["status"]}

    def submit_invoice(
        self, vendor: str, po_id: str, lines: List[Dict[str, Any]]
    ) -> Dict[str, Any]:
        po = self.pos.get(po_id)
        if not po:
            raise MCPError("unknown_po", f"Unknown PO: {po_id}")
        if str(po.get("vendor", "")).strip().lower() != vendor.strip().lower():
            raise MCPError(
                "vendor_mismatch",
                f"Invoice vendor {vendor} does not match PO vendor {po.get('vendor')}",
            )
        # Occasionally simulate validation error
        if self.error_rate > 0 and self.bus.rng.next_float() < self.error_rate:
            raise MCPError(
                "validation_error", "Duplicate invoice number or invalid tax."
            )
        inv_id = f"INV-{self._inv_seq}"
        self._inv_seq += 1
        total_cents = 0
        inv_lines: List[Dict[str, Any]] = []
        for i, ln in enumerate(lines, start=1):
            qty = int(ln.get("qty", 0))
            unit_cents = self._money_to_cents(ln.get("unit_price", 0))
            line_total = qty * unit_cents
            total_cents += line_total
            inv_lines.append(
                {
                    "line_no": i,
                    "item_id": str(ln.get("item_id", i)),
                    "qty": qty,
                    "unit_price": self._cents_to_money(unit_cents),
                    "amount": self._cents_to_money(line_total),
                }
            )
        inv = {
            "id": inv_id,
            "po_id": po_id,
            "vendor": vendor,
            "status": "OPEN",
            "lines": inv_lines,
            "amount": self._cents_to_money(total_cents),
            "paid_amount": 0.0,
            "time_ms": self.bus.clock_ms,
            "updated_ms": self.bus.clock_ms,
        }
        self.invoices[inv_id] = inv
        po["status"] = "INVOICED"
        po["updated_ms"] = self.bus.clock_ms
        return {"id": inv_id, "amount": inv["amount"]}

    def get_invoice(self, id: str) -> Dict[str, Any]:
        inv = self.invoices.get(id)
        if not inv:
            raise MCPError("unknown_invoice", f"Unknown invoice: {id}")
        return inv

    def list_invoices(
        self,
        status: str | None = None,
        vendor: str | None = None,
        po_id: str | None = None,
        limit: int | None = None,
        cursor: str | None = None,
        sort_by: str = "updated_ms",
        sort_dir: str = "desc",
    ) -> List[Dict[str, Any]] | Dict[str, Any]:
        rows = list(self.invoices.values())
        if status:
            wanted_status = status.strip().upper()
            rows = [
                row
                for row in rows
                if str(row.get("status", "")).upper() == wanted_status
            ]
        if vendor:
            needle = vendor.strip().lower()
            rows = [row for row in rows if needle in str(row.get("vendor", "")).lower()]
        if po_id:
            rows = [row for row in rows if str(row.get("po_id")) == po_id]
        sort_field = (
            sort_by
            if sort_by in {"updated_ms", "time_ms", "amount", "vendor"}
            else "updated_ms"
        )
        rows.sort(
            key=lambda row: _sortable(row.get(sort_field)),
            reverse=sort_dir.lower() != "asc",
        )
        is_legacy = (
            status is None
            and vendor is None
            and po_id is None
            and limit is None
            and cursor is None
            and sort_by == "updated_ms"
            and sort_dir == "desc"
        )
        if is_legacy:
            return rows
        return _page_rows(rows, key="invoices", limit=limit, cursor=cursor)

    def match_three_way(
        self, po_id: str, invoice_id: str, receipt_id: Optional[str] = None
    ) -> Dict[str, Any]:
        po = self.pos.get(po_id)
        inv = self.invoices.get(invoice_id)
        rcpt = self.receipts.get(receipt_id) if receipt_id else None
        if not po or not inv:
            raise MCPError("unknown_ref", "PO or Invoice not found")
        # Build item->qty maps
        po_qty = {
            str(line_item["item_id"]): int(line_item["qty"])
            for line_item in po.get("lines", [])
        }
        inv_qty = {
            str(line_item["item_id"]): int(line_item["qty"])
            for line_item in inv.get("lines", [])
        }
        rcpt_qty = {
            str(line_item["item_id"]): int(line_item["qty"])
            for line_item in (rcpt.get("lines", []) if rcpt else [])
        }
        # Compare amounts (within 1 cent)
        po_amount_c = self._money_to_cents(po.get("amount", 0))
        inv_amount_c = self._money_to_cents(inv.get("amount", 0))
        amount_ok = abs(po_amount_c - inv_amount_c) <= 1
        # Quantities
        qty_mismatches: List[Dict[str, Any]] = []
        items = set(po_qty) | set(inv_qty)
        for it in items:
            pq = po_qty.get(it, 0)
            iq = inv_qty.get(it, 0)
            rq = rcpt_qty.get(it, 0)
            if (pq != iq) or (rcpt is not None and iq > rq):
                qty_mismatches.append(
                    {"item_id": it, "po": pq, "invoice": iq, "received": rq}
                )
        status = "MATCH" if (amount_ok and not qty_mismatches) else "MISMATCH"
        po["last_three_way_match"] = {
            "invoice_id": invoice_id,
            "receipt_id": receipt_id,
            "status": status,
            "time_ms": self.bus.clock_ms,
        }
        po["updated_ms"] = self.bus.clock_ms
        return {
            "status": status,
            "amount_ok": amount_ok,
            "qty_mismatches": qty_mismatches,
            "po_id": po_id,
            "invoice_id": invoice_id,
            "receipt_id": receipt_id,
        }

    def post_payment(self, invoice_id: str, amount: float) -> Dict[str, Any]:
        inv = self.invoices.get(invoice_id)
        if not inv:
            raise MCPError("unknown_invoice", f"Unknown invoice: {invoice_id}")
        # Rarely simulate payment gateway rejection
        if self.error_rate > 0 and self.bus.rng.next_float() < (self.error_rate / 2):
            raise MCPError("payment_rejected", "Bank rejected payment.")
        paid_c = self._money_to_cents(
            inv.get("paid_amount", 0.0)
        ) + self._money_to_cents(amount)
        total_c = self._money_to_cents(inv.get("amount", 0.0))
        inv["paid_amount"] = self._cents_to_money(min(paid_c, total_c))
        inv["updated_ms"] = self.bus.clock_ms
        if paid_c >= total_c:
            inv["status"] = "PAID"
        elif paid_c > 0:
            inv["status"] = "PARTIALLY_PAID"
        return {"status": inv["status"], "paid_amount": inv["paid_amount"]}


def _normalize_limit(
    limit: int | None, *, default: int = 25, max_limit: int = 200
) -> int:
    if limit is None:
        return default
    if limit < 1:
        return 1
    return min(max_limit, int(limit))


def _decode_cursor(cursor: str | None) -> int:
    if not cursor:
        return 0
    if not cursor.startswith("ofs:"):
        raise MCPError("invalid_cursor", "Cursor must use 'ofs:<offset>' format")
    try:
        value = int(cursor.split(":", 1)[1])
    except ValueError as exc:
        raise MCPError("invalid_cursor", f"Invalid cursor: {cursor}") from exc
    return max(0, value)


def _encode_cursor(offset: int) -> str:
    return f"ofs:{max(0, int(offset))}"


def _page_rows(
    rows: List[Dict[str, Any]],
    *,
    key: str,
    limit: int | None,
    cursor: str | None,
) -> Dict[str, Any]:
    page_limit = _normalize_limit(
        limit, default=ErpSim._DEFAULT_LIMIT, max_limit=ErpSim._MAX_LIMIT
    )
    start = _decode_cursor(cursor)
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


def _sortable(value: object) -> object:
    if value is None:
        return ""
    if isinstance(value, (int, float, str)):
        return value
    return str(value)
