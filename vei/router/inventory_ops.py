from __future__ import annotations

from typing import Any, Callable, Dict, List, Optional

from vei.world.api import Scenario

from .errors import MCPError
from .tool_providers import PrefixToolProvider
from .tool_registry import ToolSpec


def _normalize_records(value: Any, key_field: str) -> Dict[str, Dict[str, Any]]:
    if isinstance(value, dict):
        return {str(key): dict(payload) for key, payload in value.items()}
    records: Dict[str, Dict[str, Any]] = {}
    for item in value or []:
        payload = dict(item)
        record_id = str(payload.get(key_field) or payload.get("id") or "")
        if record_id:
            records[record_id] = payload
    return records


class InventoryOpsSim:
    """Deterministic inventory/capacity twin for storage-solution demos."""

    def __init__(self, scenario: Optional[Scenario] = None):
        seed = (scenario.inventory_graph or {}) if scenario else {}
        self.sites = _normalize_records(seed.get("sites"), "site_id")
        self.capacity_pools = _normalize_records(seed.get("capacity_pools"), "pool_id")
        self.storage_units = _normalize_records(
            seed.get("storage_units"), "storage_unit_id"
        )
        self.quotes = _normalize_records(seed.get("quotes"), "quote_id")
        self.orders = _normalize_records(seed.get("orders"), "order_id")
        self.allocations = _normalize_records(seed.get("allocations"), "allocation_id")
        self.vendors = _normalize_records(seed.get("vendors"), "vendor_id")

    def export_state(self) -> Dict[str, Any]:
        return {
            "sites": self.sites,
            "capacity_pools": self.capacity_pools,
            "storage_units": self.storage_units,
            "quotes": self.quotes,
            "orders": self.orders,
            "allocations": self.allocations,
            "vendors": self.vendors,
        }

    def import_state(self, state: Dict[str, Any]) -> None:
        self.sites = _normalize_records(state.get("sites"), "site_id")
        self.capacity_pools = _normalize_records(state.get("capacity_pools"), "pool_id")
        self.storage_units = _normalize_records(
            state.get("storage_units"), "storage_unit_id"
        )
        self.quotes = _normalize_records(state.get("quotes"), "quote_id")
        self.orders = _normalize_records(state.get("orders"), "order_id")
        self.allocations = _normalize_records(state.get("allocations"), "allocation_id")
        self.vendors = _normalize_records(state.get("vendors"), "vendor_id")

    def summary(self) -> str:
        return (
            f"{len(self.sites)} sites, {len(self.capacity_pools)} pools, "
            f"{len(self.quotes)} quotes, {len(self.allocations)} allocations"
        )

    def action_menu(self) -> List[Dict[str, Any]]:
        return [
            {"tool": "inventory.allocate_capacity", "label": "Allocate Capacity"},
            {"tool": "inventory.reserve_inventory_block", "label": "Reserve Block"},
            {"tool": "inventory.revise_quote", "label": "Revise Quote"},
            {"tool": "inventory.assign_vendor_action", "label": "Assign Vendor Action"},
        ]

    def list_overview(self) -> Dict[str, Any]:
        return {
            "sites": list(self.sites.values()),
            "capacity_pools": list(self.capacity_pools.values()),
            "quotes": list(self.quotes.values()),
            "orders": list(self.orders.values()),
            "allocations": list(self.allocations.values()),
        }

    def allocate_capacity(
        self, quote_id: str, pool_id: str, units: int
    ) -> Dict[str, Any]:
        quote = self._require(self.quotes, quote_id, "quote")
        pool = self._require(self.capacity_pools, pool_id, "capacity pool")
        requested = int(units)
        available = int(pool.get("total_units", 0)) - int(pool.get("reserved_units", 0))
        if requested > available:
            raise MCPError(
                "inventory.overcommit",
                f"Requested {requested} units but only {available} remain in pool {pool_id}",
            )
        allocation_id = f"ALLOC-{len(self.allocations) + 1:04d}"
        pool["reserved_units"] = int(pool.get("reserved_units", 0)) + requested
        quote["committed_units"] = int(quote.get("committed_units", 0)) + requested
        self.allocations[allocation_id] = {
            "allocation_id": allocation_id,
            "quote_id": quote_id,
            "pool_id": pool_id,
            "units": requested,
            "status": "reserved",
        }
        return {
            "allocation_id": allocation_id,
            "quote_id": quote_id,
            "pool_id": pool_id,
            "units": requested,
        }

    def reserve_inventory_block(
        self, pool_id: str, units: int, note: Optional[str] = None
    ) -> Dict[str, Any]:
        pool = self._require(self.capacity_pools, pool_id, "capacity pool")
        pool["reserved_units"] = int(pool.get("reserved_units", 0)) + int(units)
        if note:
            pool["note"] = note
        return {"pool_id": pool_id, "reserved_units": int(pool["reserved_units"])}

    def revise_quote(
        self, quote_id: str, site_id: str, committed_units: int
    ) -> Dict[str, Any]:
        quote = self._require(self.quotes, quote_id, "quote")
        self._require(self.sites, site_id, "site")
        quote["site_id"] = site_id
        quote["committed_units"] = int(committed_units)
        quote["status"] = "revised"
        return {
            "quote_id": quote_id,
            "site_id": site_id,
            "committed_units": int(committed_units),
            "status": "revised",
        }

    def assign_vendor_action(
        self, order_id: str, vendor_id: str, status: str = "scheduled"
    ) -> Dict[str, Any]:
        order = self._require(self.orders, order_id, "order")
        self._require(self.vendors, vendor_id, "vendor")
        order["vendor_id"] = vendor_id
        order["status"] = status
        return {"order_id": order_id, "vendor_id": vendor_id, "status": status}

    @staticmethod
    def _require(
        store: Dict[str, Dict[str, Any]], key: str, kind: str
    ) -> Dict[str, Any]:
        if key not in store:
            raise MCPError("inventory.not_found", f"Unknown {kind}: {key}")
        return store[key]


class InventoryOpsToolProvider(PrefixToolProvider):
    def __init__(self, sim: InventoryOpsSim):
        super().__init__("inventory", prefixes=("inventory.",))
        self.sim = sim
        self._specs: List[ToolSpec] = [
            ToolSpec(
                name="inventory.list_overview",
                description="List inventory and capacity overview data.",
                permissions=("inventory:read",),
                default_latency_ms=180,
                latency_jitter_ms=40,
            ),
            ToolSpec(
                name="inventory.allocate_capacity",
                description="Allocate capacity from a pool to a quote.",
                permissions=("inventory:write",),
                side_effects=("inventory_mutation",),
                default_latency_ms=260,
                latency_jitter_ms=60,
            ),
            ToolSpec(
                name="inventory.reserve_inventory_block",
                description="Reserve a capacity block before final commitment.",
                permissions=("inventory:write",),
                side_effects=("inventory_mutation",),
                default_latency_ms=220,
                latency_jitter_ms=50,
            ),
            ToolSpec(
                name="inventory.revise_quote",
                description="Revise a storage quote with a feasible site and commitment.",
                permissions=("inventory:write",),
                side_effects=("inventory_mutation",),
                default_latency_ms=230,
                latency_jitter_ms=50,
            ),
            ToolSpec(
                name="inventory.assign_vendor_action",
                description="Assign a downstream fulfillment or vendor action for an order.",
                permissions=("inventory:write",),
                side_effects=("inventory_mutation",),
                default_latency_ms=210,
                latency_jitter_ms=50,
            ),
        ]
        self._handlers: Dict[str, Callable[..., Any]] = {
            "inventory.list_overview": self.sim.list_overview,
            "inventory.allocate_capacity": self.sim.allocate_capacity,
            "inventory.reserve_inventory_block": self.sim.reserve_inventory_block,
            "inventory.revise_quote": self.sim.revise_quote,
            "inventory.assign_vendor_action": self.sim.assign_vendor_action,
        }

    def specs(self) -> List[ToolSpec]:
        return list(self._specs)

    def call(self, tool: str, args: Dict[str, Any]) -> Any:
        handler = self._handlers.get(tool)
        if handler is None:
            raise MCPError("unknown_tool", f"No such tool: {tool}")
        try:
            return handler(**(args or {}))
        except TypeError as exc:
            raise MCPError("invalid_args", str(exc)) from exc
