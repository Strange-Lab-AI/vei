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


class PropertyOpsSim:
    """Deterministic property-operations twin for real-estate demos."""

    def __init__(self, scenario: Optional[Scenario] = None):
        seed = (scenario.property_graph or {}) if scenario else {}
        self.properties = _normalize_records(seed.get("properties"), "property_id")
        self.buildings = _normalize_records(seed.get("buildings"), "building_id")
        self.units = _normalize_records(seed.get("units"), "unit_id")
        self.tenants = _normalize_records(seed.get("tenants"), "tenant_id")
        self.leases = _normalize_records(seed.get("leases"), "lease_id")
        self.vendors = _normalize_records(seed.get("vendors"), "vendor_id")
        self.work_orders = _normalize_records(seed.get("work_orders"), "work_order_id")

    def export_state(self) -> Dict[str, Any]:
        return {
            "properties": self.properties,
            "buildings": self.buildings,
            "units": self.units,
            "tenants": self.tenants,
            "leases": self.leases,
            "vendors": self.vendors,
            "work_orders": self.work_orders,
        }

    def import_state(self, state: Dict[str, Any]) -> None:
        self.properties = _normalize_records(state.get("properties"), "property_id")
        self.buildings = _normalize_records(state.get("buildings"), "building_id")
        self.units = _normalize_records(state.get("units"), "unit_id")
        self.tenants = _normalize_records(state.get("tenants"), "tenant_id")
        self.leases = _normalize_records(state.get("leases"), "lease_id")
        self.vendors = _normalize_records(state.get("vendors"), "vendor_id")
        self.work_orders = _normalize_records(state.get("work_orders"), "work_order_id")

    def summary(self) -> str:
        return (
            f"{len(self.properties)} properties, {len(self.units)} units, "
            f"{len(self.leases)} leases, {len(self.work_orders)} work orders"
        )

    def action_menu(self) -> List[Dict[str, Any]]:
        return [
            {"tool": "property.assign_vendor", "label": "Assign Vendor"},
            {
                "tool": "property.reschedule_work_order",
                "label": "Reschedule Work Order",
            },
            {
                "tool": "property.update_lease_milestone",
                "label": "Update Lease Milestone",
            },
            {"tool": "property.reserve_unit", "label": "Reserve Unit"},
        ]

    def list_overview(self) -> Dict[str, Any]:
        return {
            "properties": list(self.properties.values()),
            "units": list(self.units.values()),
            "leases": list(self.leases.values()),
            "work_orders": list(self.work_orders.values()),
        }

    def assign_vendor(
        self, work_order_id: str, vendor_id: str, note: Optional[str] = None
    ) -> Dict[str, Any]:
        work_order = self._require(self.work_orders, work_order_id, "work_order")
        self._require(self.vendors, vendor_id, "vendor")
        work_order["vendor_id"] = vendor_id
        work_order["status"] = "scheduled"
        if note:
            work_order["note"] = note
        return {
            "work_order_id": work_order_id,
            "vendor_id": vendor_id,
            "status": "scheduled",
        }

    def reschedule_work_order(
        self, work_order_id: str, scheduled_for_ms: int, note: Optional[str] = None
    ) -> Dict[str, Any]:
        work_order = self._require(self.work_orders, work_order_id, "work_order")
        work_order["scheduled_for_ms"] = int(scheduled_for_ms)
        work_order["status"] = "rescheduled"
        if note:
            work_order["note"] = note
        return {
            "work_order_id": work_order_id,
            "scheduled_for_ms": int(scheduled_for_ms),
            "status": "rescheduled",
        }

    def update_lease_milestone(
        self, lease_id: str, milestone: str, status: Optional[str] = None
    ) -> Dict[str, Any]:
        lease = self._require(self.leases, lease_id, "lease")
        lease["milestone"] = milestone
        if status:
            lease["status"] = status
        if milestone.lower() in {"executed", "approved"}:
            lease["amendment_pending"] = False
        return {
            "lease_id": lease_id,
            "milestone": milestone,
            "status": lease.get("status"),
            "amendment_pending": bool(lease.get("amendment_pending", False)),
        }

    def reserve_unit(
        self, unit_id: str, tenant_id: str, status: str = "reserved"
    ) -> Dict[str, Any]:
        unit = self._require(self.units, unit_id, "unit")
        self._require(self.tenants, tenant_id, "tenant")
        unit["reserved_for"] = tenant_id
        unit["status"] = status
        return {"unit_id": unit_id, "tenant_id": tenant_id, "status": status}

    @staticmethod
    def _require(
        store: Dict[str, Dict[str, Any]], key: str, kind: str
    ) -> Dict[str, Any]:
        if key not in store:
            raise MCPError(
                "property.not_found",
                f"Unknown {kind}: {key}",
            )
        return store[key]


class PropertyOpsToolProvider(PrefixToolProvider):
    def __init__(self, sim: PropertyOpsSim):
        super().__init__("property", prefixes=("property.",))
        self.sim = sim
        self._specs: List[ToolSpec] = [
            ToolSpec(
                name="property.list_overview",
                description="List the current property operations overview.",
                permissions=("property:read",),
                default_latency_ms=180,
                latency_jitter_ms=40,
            ),
            ToolSpec(
                name="property.assign_vendor",
                description="Assign a vendor to a property work order.",
                permissions=("property:write",),
                side_effects=("property_mutation",),
                default_latency_ms=240,
                latency_jitter_ms=60,
            ),
            ToolSpec(
                name="property.reschedule_work_order",
                description="Reschedule a property work order.",
                permissions=("property:write",),
                side_effects=("property_mutation",),
                default_latency_ms=240,
                latency_jitter_ms=60,
            ),
            ToolSpec(
                name="property.update_lease_milestone",
                description="Advance a lease or lease amendment milestone.",
                permissions=("property:write",),
                side_effects=("property_mutation",),
                default_latency_ms=220,
                latency_jitter_ms=50,
            ),
            ToolSpec(
                name="property.reserve_unit",
                description="Reserve a unit for a tenant opening.",
                permissions=("property:write",),
                side_effects=("property_mutation",),
                default_latency_ms=220,
                latency_jitter_ms=50,
            ),
        ]
        self._handlers: Dict[str, Callable[..., Any]] = {
            "property.list_overview": self.sim.list_overview,
            "property.assign_vendor": self.sim.assign_vendor,
            "property.reschedule_work_order": self.sim.reschedule_work_order,
            "property.update_lease_milestone": self.sim.update_lease_milestone,
            "property.reserve_unit": self.sim.reserve_unit,
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
