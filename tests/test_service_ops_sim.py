from __future__ import annotations

import pytest

from vei.router.service_ops import ServiceOpsSim
from vei.router.errors import MCPError
from vei.world.scenario import Scenario


def _seed() -> dict:
    return {
        "customers": [
            {
                "customer_id": "C1",
                "name": "VIP Corp",
                "vip": True,
                "account_status": "active",
            },
            {
                "customer_id": "C2",
                "name": "Standard Inc",
                "vip": False,
                "account_status": "active",
            },
        ],
        "work_orders": [
            {
                "work_order_id": "WO1",
                "customer_id": "C1",
                "title": "Emergency fix",
                "status": "pending_dispatch",
                "required_skill": "controls",
                "appointment_id": "APT1",
                "estimated_amount_usd": 1500.0,
            },
            {
                "work_order_id": "WO2",
                "customer_id": "C2",
                "title": "Routine PM",
                "status": "pending_dispatch",
                "required_skill": "hvac",
                "appointment_id": "APT2",
                "estimated_amount_usd": 200.0,
            },
            {
                "work_order_id": "WO3",
                "customer_id": "C2",
                "title": "Expensive non-VIP job",
                "status": "pending_dispatch",
                "required_skill": "hvac",
                "appointment_id": "APT3",
                "estimated_amount_usd": 2000.0,
            },
        ],
        "technicians": [
            {
                "technician_id": "T1",
                "name": "Alice",
                "status": "available",
                "skills": ["controls", "hvac"],
            },
            {
                "technician_id": "T2",
                "name": "Bob",
                "status": "available",
                "skills": ["plumbing"],
            },
            {
                "technician_id": "T3",
                "name": "Carol",
                "status": "unavailable",
                "skills": ["controls"],
            },
        ],
        "appointments": [
            {
                "appointment_id": "APT1",
                "work_order_id": "WO1",
                "customer_id": "C1",
                "status": "pending",
                "dispatch_status": "pending",
                "reschedule_count": 0,
            },
            {
                "appointment_id": "APT2",
                "work_order_id": "WO2",
                "customer_id": "C2",
                "status": "pending",
                "dispatch_status": "pending",
                "reschedule_count": 0,
            },
            {
                "appointment_id": "APT3",
                "work_order_id": "WO3",
                "customer_id": "C2",
                "status": "pending",
                "dispatch_status": "pending",
                "reschedule_count": 0,
            },
        ],
        "billing_cases": [
            {
                "billing_case_id": "B1",
                "customer_id": "C1",
                "dispute_status": "open",
                "hold": False,
                "amount_usd": 2000.0,
            },
            {
                "billing_case_id": "B2",
                "customer_id": "C2",
                "dispute_status": "clear",
                "hold": False,
                "amount_usd": 300.0,
            },
        ],
        "exceptions": [
            {
                "exception_id": "E1",
                "type": "technician_unavailable",
                "status": "open",
                "work_order_id": "WO1",
            },
            {
                "exception_id": "E2",
                "type": "sla_risk",
                "status": "open",
                "work_order_id": "WO1",
            },
            {
                "exception_id": "E3",
                "type": "billing_dispute_open",
                "status": "open",
                "work_order_id": "WO1",
            },
        ],
        "policy": {
            "approval_threshold_usd": 1000.0,
            "vip_priority_override": True,
            "billing_hold_on_dispute": True,
            "max_auto_reschedules": 2,
        },
    }


def _sim(**overrides: object) -> ServiceOpsSim:
    seed = _seed()
    if overrides:
        seed["policy"] = {**seed["policy"], **overrides}
    scenario = Scenario(service_ops=seed)
    return ServiceOpsSim(scenario=scenario)


def _expect_mcp(code: str, fn, *args, **kwargs):
    """Call fn and assert it raises MCPError with the given code."""
    with pytest.raises(MCPError) as exc_info:
        fn(*args, **kwargs)
    assert exc_info.value.code == code


class TestAssignDispatch:
    def test_basic_dispatch(self):
        sim = _sim()
        result = sim.assign_dispatch("WO1", "T1")
        assert result["status"] == "dispatched"
        assert result["technician_id"] == "T1"
        assert sim.work_orders["WO1"]["status"] == "dispatched"
        assert sim.appointments["APT1"]["dispatch_status"] == "assigned"

    def test_skill_mismatch_rejected(self):
        sim = _sim()
        _expect_mcp("service_ops.skill_mismatch", sim.assign_dispatch, "WO2", "T2")

    def test_unavailable_technician_rejected(self):
        sim = _sim()
        _expect_mcp(
            "service_ops.technician_unavailable", sim.assign_dispatch, "WO1", "T3"
        )

    def test_approval_required_non_vip_over_threshold(self):
        """Non-VIP work order above threshold is blocked."""
        sim = _sim()
        _expect_mcp("service_ops.approval_required", sim.assign_dispatch, "WO3", "T1")

    def test_approval_required_vip_without_override(self):
        """VIP work order above threshold is blocked when vip_priority_override=False."""
        sim = _sim(vip_priority_override=False)
        _expect_mcp("service_ops.approval_required", sim.assign_dispatch, "WO1", "T1")

    def test_vip_override_bypasses_threshold(self):
        """VIP customer + vip_priority_override=True bypasses the approval threshold."""
        sim = _sim(vip_priority_override=True)
        result = sim.assign_dispatch("WO1", "T1")
        assert result["status"] == "dispatched"

    def test_under_threshold_no_approval_needed(self):
        """Work order under threshold dispatches regardless of VIP status."""
        sim = _sim(vip_priority_override=False)
        result = sim.assign_dispatch("WO2", "T1")
        assert result["status"] == "dispatched"

    def test_previous_technician_cleared(self):
        sim = _sim()
        sim.assign_dispatch("WO1", "T1")
        assert sim.technicians["T1"]["current_appointment_id"] == "APT1"
        sim.technicians["T1"]["status"] = "available"
        seed_t4 = {
            "technician_id": "T4",
            "name": "Dave",
            "status": "available",
            "skills": ["controls"],
        }
        sim.technicians["T4"] = seed_t4
        sim.assign_dispatch("WO1", "T4")
        assert sim.technicians["T1"].get("current_appointment_id") is None
        assert sim.technicians["T4"]["current_appointment_id"] == "APT1"

    def test_busy_technician_rejected(self):
        sim = _sim()
        sim.appointments["APT2"]["technician_id"] = "T1"
        sim.appointments["APT2"]["dispatch_status"] = "assigned"
        sim.appointments["APT2"]["status"] = "scheduled"
        sim.technicians["T1"]["current_appointment_id"] = "APT2"

        _expect_mcp("service_ops.technician_busy", sim.assign_dispatch, "WO1", "T1")


class TestRescheduleDispatch:
    def test_reschedule_within_limit(self):
        sim = _sim()
        sim.assign_dispatch("WO1", "T1")
        sim.technicians["T1"]["status"] = "available"
        result = sim.reschedule_dispatch("APT1", "T1", scheduled_for_ms=99999)
        assert result["dispatch_status"] == "assigned"
        assert sim.appointments["APT1"]["reschedule_count"] == 1

    def test_reschedule_exceeds_limit(self):
        sim = _sim(max_auto_reschedules=1)
        sim.assign_dispatch("WO1", "T1")
        sim.technicians["T1"]["status"] = "available"
        sim.reschedule_dispatch("APT1", "T1", scheduled_for_ms=99999)
        sim.technicians["T1"]["status"] = "available"
        _expect_mcp(
            "service_ops.reschedule_limit",
            sim.reschedule_dispatch,
            "APT1",
            "T1",
            scheduled_for_ms=99999,
        )


class TestHoldBilling:
    def test_place_hold(self):
        sim = _sim()
        result = sim.hold_billing("B1", reason="dispute active", hold=True)
        assert result["hold"] is True
        assert result["status"] == "on_hold"

    def test_release_hold_blocked_by_policy(self):
        """billing_hold_on_dispute=True prevents releasing a hold on a disputed case."""
        sim = _sim(billing_hold_on_dispute=True)
        sim.hold_billing("B1", hold=True)
        _expect_mcp(
            "service_ops.policy_hold_required", sim.hold_billing, "B1", hold=False
        )

    def test_release_hold_allowed_when_policy_off(self):
        """billing_hold_on_dispute=False allows releasing holds on disputed cases."""
        sim = _sim(billing_hold_on_dispute=False)
        sim.hold_billing("B1", hold=True)
        result = sim.hold_billing("B1", hold=False)
        assert result["hold"] is False

    def test_release_non_disputed_always_allowed(self):
        """Non-disputed cases can always have holds released regardless of policy."""
        sim = _sim(billing_hold_on_dispute=True)
        sim.hold_billing("B2", hold=True)
        result = sim.hold_billing("B2", hold=False)
        assert result["hold"] is False


class TestOfficialStateUpdates:
    def test_update_work_order_status_updates_linked_appointment(self):
        sim = _sim()
        result = sim.update_work_order_status(
            "WO1", "monitoring", note="Customer service restored pending observation"
        )

        assert result == {
            "work_order_id": "WO1",
            "status": "monitoring",
            "appointment_id": "APT1",
            "appointment_status": "monitoring",
        }
        assert sim.work_orders["WO1"]["status"] == "monitoring"
        assert sim.work_orders["WO1"]["status_note"] == (
            "Customer service restored pending observation"
        )
        assert sim.work_orders["WO1"]["history"][-1]["previous_status"] == (
            "pending_dispatch"
        )
        assert sim.appointments["APT1"]["status"] == "monitoring"
        assert sim.appointments["APT1"]["history"][-1]["work_order_id"] == "WO1"

    def test_update_work_order_status_allows_distinct_appointment_status(self):
        sim = _sim()
        sim.update_work_order_status(
            "WO1",
            "closed",
            appointment_status="completed",
        )

        assert sim.work_orders["WO1"]["status"] == "closed"
        assert sim.appointments["APT1"]["status"] == "completed"

    def test_update_work_order_status_rejects_blank_status(self):
        sim = _sim()
        _expect_mcp(
            "service_ops.invalid_status",
            sim.update_work_order_status,
            "WO1",
            "",
        )

    def test_set_sla_clock_pauses_with_written_reason(self):
        sim = _sim()
        result = sim.set_sla_clock(
            "B1",
            "paused",
            reason="Waiting on customer access confirmation",
            note="Review again at 14:00",
        )

        assert result == {
            "billing_case_id": "B1",
            "sla_clock_state": "paused",
            "reason": "Waiting on customer access confirmation",
            "status": "sla_paused",
        }
        assert sim.billing_cases["B1"]["status"] == "sla_paused"
        assert sim.billing_cases["B1"]["sla_clock_note"] == "Review again at 14:00"
        assert sim.billing_cases["B1"]["history"][-1]["reason"] == (
            "Waiting on customer access confirmation"
        )

    def test_set_sla_clock_resumes_case_as_active(self):
        sim = _sim()
        sim.set_sla_clock("B1", "paused", reason="Customer access pending")
        result = sim.set_sla_clock("B1", "running", reason="Access confirmed")

        assert result["sla_clock_state"] == "running"
        assert result["status"] == "active"
        assert (
            sim.billing_cases["B1"]["history"][-1]["previous_sla_clock_state"]
            == "paused"
        )

    def test_set_sla_clock_rejects_invalid_state(self):
        sim = _sim()
        _expect_mcp(
            "service_ops.invalid_clock_state",
            sim.set_sla_clock,
            "B1",
            "stopped",
            reason="invalid state",
        )

    def test_set_sla_clock_requires_reason(self):
        sim = _sim()
        _expect_mcp(
            "service_ops.missing_reason",
            sim.set_sla_clock,
            "B1",
            "paused",
            reason="",
        )


class TestExceptionResolution:
    def test_dispatch_mitigates_only_dispatch_exceptions(self):
        """assign_dispatch should mitigate technician_unavailable and sla_risk but not billing_dispute_open."""
        sim = _sim()
        sim.assign_dispatch("WO1", "T1")
        assert sim.exceptions["E1"]["status"] == "mitigated"
        assert sim.exceptions["E2"]["status"] == "mitigated"
        assert sim.exceptions["E3"]["status"] == "open"

    def test_clear_exception_manual(self):
        sim = _sim()
        result = sim.clear_exception(
            "E3", resolution_note="dispute resolved", status="resolved"
        )
        assert result["status"] == "resolved"
        assert sim.exceptions["E3"]["resolution_note"] == "dispute resolved"


class TestUpdatePolicy:
    def test_update_changes_dispatch_behavior(self):
        """Updating policy should change enforcement on subsequent calls."""
        sim = _sim(vip_priority_override=False)
        _expect_mcp("service_ops.approval_required", sim.assign_dispatch, "WO1", "T1")
        sim.update_policy(vip_priority_override=True, reason="demo override")
        result = sim.assign_dispatch("WO1", "T1")
        assert result["status"] == "dispatched"
        assert sim.policy["last_reason"] == "demo override"

    def test_update_billing_policy_changes_hold_behavior(self):
        sim = _sim(billing_hold_on_dispute=True)
        sim.hold_billing("B1", hold=True)
        _expect_mcp(
            "service_ops.policy_hold_required", sim.hold_billing, "B1", hold=False
        )
        sim.update_policy(billing_hold_on_dispute=False)
        result = sim.hold_billing("B1", hold=False)
        assert result["hold"] is False


class TestExportImport:
    def test_roundtrip(self):
        sim = _sim()
        sim.assign_dispatch("WO1", "T1")
        state = sim.export_state()
        sim2 = ServiceOpsSim()
        sim2.import_state(state)
        assert sim2.work_orders["WO1"]["status"] == "dispatched"
        assert sim2.policy["vip_priority_override"] is True


class TestActionMenu:
    def test_all_tools_listed(self):
        sim = _sim()
        menu = sim.action_menu()
        tool_names = {item["tool"] for item in menu}
        assert tool_names == {
            "service_ops.list_overview",
            "service_ops.assign_dispatch",
            "service_ops.reschedule_dispatch",
            "service_ops.update_work_order_status",
            "service_ops.set_sla_clock",
            "service_ops.hold_billing",
            "service_ops.clear_exception",
            "service_ops.update_policy",
        }
