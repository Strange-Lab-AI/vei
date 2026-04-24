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
        if not record_id:
            continue
        records[record_id] = payload
    return records


def _default_policy() -> Dict[str, Any]:
    return {
        "approval_threshold_usd": 1000.0,
        "vip_priority_override": True,
        "billing_hold_on_dispute": True,
        "max_auto_reschedules": 2,
    }


def _append_history(record: Dict[str, Any], event: Dict[str, Any]) -> None:
    history = record.get("history")
    if not isinstance(history, list):
        history = []
        record["history"] = history
    history.append({key: value for key, value in event.items() if value is not None})


class ServiceOpsSim:
    """Deterministic field-service operations twin for dispatch and billing demos."""

    def __init__(self, scenario: Optional[Scenario] = None):
        seed = (scenario.service_ops or {}) if scenario else {}
        self.customers = _normalize_records(seed.get("customers"), "customer_id")
        self.work_orders = _normalize_records(seed.get("work_orders"), "work_order_id")
        self.technicians = _normalize_records(seed.get("technicians"), "technician_id")
        self.appointments = _normalize_records(
            seed.get("appointments"), "appointment_id"
        )
        self.billing_cases = _normalize_records(
            seed.get("billing_cases"), "billing_case_id"
        )
        self.exceptions = _normalize_records(seed.get("exceptions"), "exception_id")
        self.policy = {**_default_policy(), **dict(seed.get("policy") or {})}

    def export_state(self) -> Dict[str, Any]:
        return {
            "customers": self.customers,
            "work_orders": self.work_orders,
            "technicians": self.technicians,
            "appointments": self.appointments,
            "billing_cases": self.billing_cases,
            "exceptions": self.exceptions,
            "policy": dict(self.policy),
        }

    def import_state(self, state: Dict[str, Any]) -> None:
        self.customers = _normalize_records(state.get("customers"), "customer_id")
        self.work_orders = _normalize_records(state.get("work_orders"), "work_order_id")
        self.technicians = _normalize_records(state.get("technicians"), "technician_id")
        self.appointments = _normalize_records(
            state.get("appointments"), "appointment_id"
        )
        self.billing_cases = _normalize_records(
            state.get("billing_cases"), "billing_case_id"
        )
        self.exceptions = _normalize_records(state.get("exceptions"), "exception_id")
        self.policy = {**_default_policy(), **dict(state.get("policy") or {})}

    def summary(self) -> str:
        open_exceptions = sum(
            1
            for payload in self.exceptions.values()
            if str(payload.get("status", "open")).lower() != "resolved"
        )
        billing_holds = sum(
            1 for payload in self.billing_cases.values() if bool(payload.get("hold"))
        )
        assigned_dispatches = sum(
            1
            for payload in self.appointments.values()
            if str(payload.get("dispatch_status", "")).lower() == "assigned"
        )
        return (
            f"{len(self.work_orders)} work orders, {assigned_dispatches} assigned dispatches, "
            f"{billing_holds} billing holds, {open_exceptions} open exceptions"
        )

    def action_menu(self) -> List[Dict[str, Any]]:
        return [
            {"tool": "service_ops.list_overview", "label": "List Overview"},
            {"tool": "service_ops.assign_dispatch", "label": "Assign Dispatch"},
            {"tool": "service_ops.reschedule_dispatch", "label": "Reschedule Dispatch"},
            {
                "tool": "service_ops.update_work_order_status",
                "label": "Update Work Order Status",
            },
            {"tool": "service_ops.set_sla_clock", "label": "Set SLA Clock"},
            {"tool": "service_ops.hold_billing", "label": "Hold Billing"},
            {"tool": "service_ops.clear_exception", "label": "Clear Exception"},
            {"tool": "service_ops.update_policy", "label": "Update Policy"},
        ]

    def list_overview(self) -> Dict[str, Any]:
        return {
            "customers": list(self.customers.values()),
            "work_orders": list(self.work_orders.values()),
            "technicians": list(self.technicians.values()),
            "appointments": list(self.appointments.values()),
            "billing_cases": list(self.billing_cases.values()),
            "exceptions": list(self.exceptions.values()),
            "policy": dict(self.policy),
        }

    def assign_dispatch(
        self,
        work_order_id: str,
        technician_id: str,
        appointment_id: Optional[str] = None,
        scheduled_for_ms: Optional[int] = None,
        note: Optional[str] = None,
    ) -> Dict[str, Any]:
        work_order = self._require(self.work_orders, work_order_id, "work order")
        technician = self._require(self.technicians, technician_id, "technician")
        resolved_appointment_id = str(
            appointment_id or work_order.get("appointment_id") or ""
        )
        if not resolved_appointment_id:
            raise MCPError(
                "service_ops.missing_appointment",
                f"Work order {work_order_id} has no linked appointment",
            )
        appointment = self._require(
            self.appointments, resolved_appointment_id, "appointment"
        )

        estimated = float(work_order.get("estimated_amount_usd") or 0)
        threshold = float(self.policy.get("approval_threshold_usd") or 0)
        vip_override = bool(self.policy.get("vip_priority_override"))
        customer_id = str(work_order.get("customer_id") or "")
        customer = self.customers.get(customer_id, {})
        is_vip = bool(customer.get("vip"))

        if threshold > 0 and estimated > threshold:
            if not (is_vip and vip_override):
                raise MCPError(
                    "service_ops.approval_required",
                    f"Work order {work_order_id} (${estimated:.0f}) exceeds "
                    f"approval threshold (${threshold:.0f})",
                )

        required_skill = str(work_order.get("required_skill") or "").strip().lower()
        if required_skill and required_skill not in {
            str(skill).strip().lower() for skill in technician.get("skills", [])
        }:
            raise MCPError(
                "service_ops.skill_mismatch",
                f"Technician {technician_id} does not cover required skill {required_skill}",
            )

        status = str(technician.get("status", "available")).lower()
        if status not in {"available", "standby"}:
            raise MCPError(
                "service_ops.technician_unavailable",
                f"Technician {technician_id} is not available for dispatch",
            )
        active_appointment_id = self._find_active_appointment_for_technician(
            technician_id,
            exclude_appointment_id=resolved_appointment_id,
        )
        if active_appointment_id is not None:
            raise MCPError(
                "service_ops.technician_busy",
                f"Technician {technician_id} is already assigned to appointment "
                f"{active_appointment_id}",
            )

        previous_technician_id = appointment.get("technician_id")
        if previous_technician_id and previous_technician_id in self.technicians:
            self.technicians[previous_technician_id]["current_appointment_id"] = None

        appointment["technician_id"] = technician_id
        appointment["dispatch_status"] = "assigned"
        appointment["status"] = "scheduled"
        if scheduled_for_ms is not None:
            appointment["scheduled_for_ms"] = int(scheduled_for_ms)

        technician["current_appointment_id"] = resolved_appointment_id
        work_order["technician_id"] = technician_id
        work_order["appointment_id"] = resolved_appointment_id
        work_order["status"] = "dispatched"
        if note:
            work_order["dispatch_note"] = note

        self._resolve_related_exceptions(
            work_order_id=work_order_id,
            resolved_status="mitigated",
        )
        return {
            "work_order_id": work_order_id,
            "appointment_id": resolved_appointment_id,
            "technician_id": technician_id,
            "dispatch_status": appointment["dispatch_status"],
            "status": work_order["status"],
        }

    def reschedule_dispatch(
        self,
        appointment_id: str,
        technician_id: str,
        scheduled_for_ms: int,
        note: Optional[str] = None,
    ) -> Dict[str, Any]:
        appointment = self._require(self.appointments, appointment_id, "appointment")
        work_order_id = str(appointment.get("work_order_id") or "")
        if not work_order_id:
            raise MCPError(
                "service_ops.missing_work_order",
                f"Appointment {appointment_id} has no linked work order",
            )
        reschedule_count = int(appointment.get("reschedule_count", 0) or 0) + 1
        max_auto_reschedules = int(self.policy.get("max_auto_reschedules", 2) or 2)
        if reschedule_count > max_auto_reschedules:
            raise MCPError(
                "service_ops.reschedule_limit",
                f"Appointment {appointment_id} exceeded auto-reschedule limit",
            )
        appointment["reschedule_count"] = reschedule_count
        return self.assign_dispatch(
            work_order_id=work_order_id,
            technician_id=technician_id,
            appointment_id=appointment_id,
            scheduled_for_ms=scheduled_for_ms,
            note=note,
        )

    def hold_billing(
        self,
        billing_case_id: str,
        reason: Optional[str] = None,
        hold: bool = True,
    ) -> Dict[str, Any]:
        billing_case = self._require(
            self.billing_cases, billing_case_id, "billing case"
        )
        dispute = str(billing_case.get("dispute_status") or "").lower()
        is_disputed = dispute in {"open", "reopened", "disputed"}

        if (
            not hold
            and is_disputed
            and bool(self.policy.get("billing_hold_on_dispute"))
        ):
            raise MCPError(
                "service_ops.policy_hold_required",
                f"Policy requires billing hold on disputed case {billing_case_id}",
            )

        billing_case["hold"] = bool(hold)
        if bool(hold):
            billing_case["status"] = "on_hold"
        elif is_disputed:
            billing_case["status"] = "dispute_active"
        if reason:
            billing_case["hold_reason"] = reason
        return {
            "billing_case_id": billing_case_id,
            "hold": bool(billing_case["hold"]),
            "status": str(billing_case.get("status", "")),
        }

    def update_work_order_status(
        self,
        work_order_id: str,
        status: str,
        note: Optional[str] = None,
        appointment_status: Optional[str] = None,
    ) -> Dict[str, Any]:
        work_order = self._require(self.work_orders, work_order_id, "work order")
        normalized_status = str(status or "").strip()
        if not normalized_status:
            raise MCPError(
                "service_ops.invalid_status",
                "Work order status cannot be blank",
            )

        previous_status = str(work_order.get("status") or "")
        work_order["status"] = normalized_status
        if note:
            work_order["status_note"] = note
        _append_history(
            work_order,
            {
                "status": normalized_status,
                "previous_status": previous_status,
                "note": note,
            },
        )

        appointment_id = str(work_order.get("appointment_id") or "")
        resolved_appointment_status = ""
        if appointment_id and appointment_id in self.appointments:
            appointment = self.appointments[appointment_id]
            resolved_appointment_status = (
                str(appointment_status).strip()
                if appointment_status is not None
                else normalized_status
            )
            if not resolved_appointment_status:
                raise MCPError(
                    "service_ops.invalid_status",
                    "Appointment status cannot be blank",
                )
            previous_appointment_status = str(appointment.get("status") or "")
            appointment["status"] = resolved_appointment_status
            if note:
                appointment["status_note"] = note
            _append_history(
                appointment,
                {
                    "status": resolved_appointment_status,
                    "previous_status": previous_appointment_status,
                    "note": note,
                    "work_order_id": work_order_id,
                },
            )

        return {
            "work_order_id": work_order_id,
            "status": str(work_order.get("status") or ""),
            "appointment_id": appointment_id,
            "appointment_status": resolved_appointment_status,
        }

    def set_sla_clock(
        self,
        billing_case_id: str,
        clock_state: str,
        reason: str,
        note: Optional[str] = None,
    ) -> Dict[str, Any]:
        billing_case = self._require(
            self.billing_cases, billing_case_id, "billing case"
        )
        normalized_state = str(clock_state or "").strip().lower()
        if normalized_state not in {"running", "paused"}:
            raise MCPError(
                "service_ops.invalid_clock_state",
                "SLA clock state must be running or paused",
            )
        normalized_reason = str(reason or "").strip()
        if not normalized_reason:
            raise MCPError(
                "service_ops.missing_reason",
                "SLA clock updates require a written reason",
            )

        previous_state = str(billing_case.get("sla_clock_state") or "")
        billing_case["sla_clock_state"] = normalized_state
        billing_case["sla_clock_reason"] = normalized_reason
        if note:
            billing_case["sla_clock_note"] = note
        billing_case["status"] = (
            "active" if normalized_state == "running" else "sla_paused"
        )
        _append_history(
            billing_case,
            {
                "sla_clock_state": normalized_state,
                "previous_sla_clock_state": previous_state,
                "reason": normalized_reason,
                "note": note,
            },
        )

        return {
            "billing_case_id": billing_case_id,
            "sla_clock_state": str(billing_case.get("sla_clock_state") or ""),
            "reason": str(billing_case.get("sla_clock_reason") or ""),
            "status": str(billing_case.get("status") or ""),
        }

    def clear_exception(
        self,
        exception_id: str,
        resolution_note: Optional[str] = None,
        status: str = "resolved",
    ) -> Dict[str, Any]:
        issue = self._require(self.exceptions, exception_id, "exception")
        issue["status"] = status
        if resolution_note:
            issue["resolution_note"] = resolution_note
        return {
            "exception_id": exception_id,
            "status": str(issue["status"]),
        }

    def update_policy(
        self,
        approval_threshold_usd: Optional[float] = None,
        vip_priority_override: Optional[bool] = None,
        billing_hold_on_dispute: Optional[bool] = None,
        max_auto_reschedules: Optional[int] = None,
        reason: Optional[str] = None,
    ) -> Dict[str, Any]:
        if approval_threshold_usd is not None:
            self.policy["approval_threshold_usd"] = float(approval_threshold_usd)
        if vip_priority_override is not None:
            self.policy["vip_priority_override"] = bool(vip_priority_override)
        if billing_hold_on_dispute is not None:
            self.policy["billing_hold_on_dispute"] = bool(billing_hold_on_dispute)
        if max_auto_reschedules is not None:
            self.policy["max_auto_reschedules"] = int(max_auto_reschedules)
        if reason:
            self.policy["last_reason"] = reason
        return dict(self.policy)

    _DISPATCH_EXCEPTION_TYPES = {
        "technician_unavailable",
        "sla_risk",
        "schedule_collision",
    }

    def _find_active_appointment_for_technician(
        self,
        technician_id: str,
        *,
        exclude_appointment_id: str,
    ) -> Optional[str]:
        technician = self.technicians.get(technician_id, {})
        current_appointment_id = str(technician.get("current_appointment_id") or "")
        if current_appointment_id and current_appointment_id != exclude_appointment_id:
            current_appointment = self.appointments.get(current_appointment_id)
            if current_appointment is None or self._appointment_is_active(
                current_appointment
            ):
                return current_appointment_id

        for appointment_id, appointment in self.appointments.items():
            if appointment_id == exclude_appointment_id:
                continue
            if str(appointment.get("technician_id") or "") != technician_id:
                continue
            if self._appointment_is_active(appointment):
                return appointment_id
        return None

    @staticmethod
    def _appointment_is_active(appointment: Dict[str, Any]) -> bool:
        status = str(appointment.get("status", "")).lower()
        if status in {"cancelled", "completed", "closed", "resolved"}:
            return False
        dispatch_status = str(appointment.get("dispatch_status", "")).lower()
        return dispatch_status not in {"cancelled", "completed", "closed", "resolved"}

    def _resolve_related_exceptions(
        self,
        *,
        work_order_id: str,
        resolved_status: str,
    ) -> None:
        for payload in self.exceptions.values():
            if str(payload.get("work_order_id") or "") != work_order_id:
                continue
            if str(payload.get("status", "")).lower() == "resolved":
                continue
            exc_type = str(payload.get("type") or "").lower()
            if exc_type not in self._DISPATCH_EXCEPTION_TYPES:
                continue
            payload["status"] = resolved_status

    @staticmethod
    def _require(
        store: Dict[str, Dict[str, Any]], key: str, kind: str
    ) -> Dict[str, Any]:
        if key not in store:
            raise MCPError("service_ops.not_found", f"Unknown {kind}: {key}")
        return store[key]


class ServiceOpsToolProvider(PrefixToolProvider):
    def __init__(self, sim: ServiceOpsSim):
        super().__init__("service_ops", prefixes=("service_ops.",))
        self.sim = sim
        self._specs: List[ToolSpec] = [
            ToolSpec(
                name="service_ops.list_overview",
                description="List service operations state across dispatch, billing, and exceptions.",
                permissions=("service_ops:read",),
                default_latency_ms=180,
                latency_jitter_ms=40,
            ),
            ToolSpec(
                name="service_ops.assign_dispatch",
                description="Assign a technician to a service work order appointment.",
                permissions=("service_ops:write",),
                side_effects=("service_ops_mutation",),
                default_latency_ms=230,
                latency_jitter_ms=50,
            ),
            ToolSpec(
                name="service_ops.reschedule_dispatch",
                description="Reschedule a field appointment onto a new technician slot.",
                permissions=("service_ops:write",),
                side_effects=("service_ops_mutation",),
                default_latency_ms=240,
                latency_jitter_ms=60,
            ),
            ToolSpec(
                name="service_ops.update_work_order_status",
                description="Update the official service state for a work order and linked appointment.",
                permissions=("service_ops:write",),
                side_effects=("service_ops_mutation",),
                default_latency_ms=190,
                latency_jitter_ms=40,
            ),
            ToolSpec(
                name="service_ops.set_sla_clock",
                description="Set the official SLA clock state and written reason for a billing case.",
                permissions=("service_ops:write",),
                side_effects=("service_ops_mutation",),
                default_latency_ms=190,
                latency_jitter_ms=40,
            ),
            ToolSpec(
                name="service_ops.hold_billing",
                description="Place or release a billing hold for a disputed customer case.",
                permissions=("service_ops:write",),
                side_effects=("service_ops_mutation",),
                default_latency_ms=220,
                latency_jitter_ms=50,
            ),
            ToolSpec(
                name="service_ops.clear_exception",
                description="Mark a service operations exception as resolved or mitigated.",
                permissions=("service_ops:write",),
                side_effects=("service_ops_mutation",),
                default_latency_ms=210,
                latency_jitter_ms=40,
            ),
            ToolSpec(
                name="service_ops.update_policy",
                description="Update the local service-ops policy knobs used in the demo world.",
                permissions=("service_ops:write",),
                side_effects=("service_ops_mutation",),
                default_latency_ms=170,
                latency_jitter_ms=30,
            ),
        ]
        self._handlers: Dict[str, Callable[..., Any]] = {
            "service_ops.list_overview": self.sim.list_overview,
            "service_ops.assign_dispatch": self.sim.assign_dispatch,
            "service_ops.reschedule_dispatch": self.sim.reschedule_dispatch,
            "service_ops.update_work_order_status": self.sim.update_work_order_status,
            "service_ops.set_sla_clock": self.sim.set_sla_clock,
            "service_ops.hold_billing": self.sim.hold_billing,
            "service_ops.clear_exception": self.sim.clear_exception,
            "service_ops.update_policy": self.sim.update_policy,
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
