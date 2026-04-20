from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import pytest

import vei.world.api as world_api
import vei.world.scenarios as scenario_catalog
from vei.blueprint.api import build_blueprint_asset_for_example
from vei.verticals.faults import (
    FaultOverlaySpec,
    apply_fault_overlays,
    overlay_summaries,
)
from vei.world.scenario import Scenario


def test_fault_overlays_apply_operations_and_validate_inputs() -> None:
    payload = {
        "tickets": [
            {"id": "T1", "status": "open", "notes": ["initial"], "count": 1},
            {"id": "T2", "status": "open", "notes": [], "count": 2},
        ],
        "meta": {"deadline_ms": 1000},
    }
    overlays = [
        FaultOverlaySpec(
            name="set-status",
            path="tickets[id=T1].status",
            operation="set",
            value="resolved",
            label="Resolve ticket",
        ),
        FaultOverlaySpec(
            name="append-note",
            path="tickets[id=T1].notes",
            operation="append",
            value="follow-up",
            label="Add note",
            rationale="Keep the operator trail visible.",
        ),
        FaultOverlaySpec(
            name="remove-note",
            path="tickets[id=T1].notes",
            operation="remove",
            value="initial",
            label="Remove stale note",
        ),
        FaultOverlaySpec(
            name="increment-count",
            path="tickets[id=T2].count",
            operation="increment",
            value=3,
            label="Increment count",
        ),
        FaultOverlaySpec(
            name="shift-deadline",
            path="meta.deadline_ms",
            operation="shift_deadline_ms",
            value=250,
            label="Move deadline",
        ),
    ]

    updated = apply_fault_overlays(payload, overlays)

    assert payload["tickets"][0]["status"] == "open"
    assert updated["tickets"][0]["status"] == "resolved"
    assert updated["tickets"][0]["notes"] == ["follow-up"]
    assert updated["tickets"][1]["count"] == 5
    assert updated["meta"]["deadline_ms"] == 1250
    assert (
        overlay_summaries(overlays)[1] == "Add note: Keep the operator trail visible."
    )

    with pytest.raises(ValueError, match="append target is not a list"):
        apply_fault_overlays(
            payload,
            [
                FaultOverlaySpec(
                    name="bad-append",
                    path="meta.deadline_ms",
                    operation="append",
                    value=1,
                    label="Bad append",
                )
            ],
        )
    with pytest.raises(ValueError, match="remove from list requires value"):
        apply_fault_overlays(
            payload,
            [
                FaultOverlaySpec(
                    name="bad-remove",
                    path="tickets[id=T1].notes",
                    operation="remove",
                    label="Bad remove",
                )
            ],
        )


def test_fault_overlay_helpers_cover_list_paths_and_selector_errors() -> None:
    payload = {
        "numbers": [1, 2],
        "items": [{"id": "A", "count": 1}, {"id": "B", "count": 2}],
        "meta": {"deadline_ms": 1000},
    }

    updated = apply_fault_overlays(
        payload,
        [
            FaultOverlaySpec(
                name="set-list",
                path="numbers.1",
                operation="set",
                value=9,
                label="Set list item",
            ),
            FaultOverlaySpec(
                name="remove-list-item",
                path="numbers.0",
                operation="remove",
                label="Remove list item",
            ),
            FaultOverlaySpec(
                name="increment-list-item",
                path="items[id=B].count",
                operation="increment",
                value=4,
                label="Increment selector",
            ),
        ],
    )

    assert updated["numbers"] == [9]
    assert updated["items"][1]["count"] == 6

    with pytest.raises(ValueError, match="fault overlay path must not be empty"):
        apply_fault_overlays(
            payload,
            [
                FaultOverlaySpec(
                    name="empty-path",
                    path="",
                    operation="set",
                    value=1,
                    label="Empty path",
                )
            ],
        )
    with pytest.raises(ValueError, match="selector target is not a list"):
        apply_fault_overlays(
            payload,
            [
                FaultOverlaySpec(
                    name="bad-selector-target",
                    path="meta[id=A]",
                    operation="set",
                    value=1,
                    label="Bad selector target",
                )
            ],
        )
    with pytest.raises(ValueError, match="selector did not match any item"):
        apply_fault_overlays(
            payload,
            [
                FaultOverlaySpec(
                    name="missing-selector",
                    path="items[id=missing].count",
                    operation="set",
                    value=1,
                    label="Missing selector",
                )
            ],
        )
    with pytest.raises(ValueError, match="unsupported fault operation"):
        apply_fault_overlays(
            payload,
            [
                FaultOverlaySpec.model_construct(
                    name="bad-op",
                    path="meta.deadline_ms",
                    operation="unknown",
                    value=1,
                    label="Bad op",
                )
            ],
        )


def test_world_api_wrappers_delegate_and_attach_sessions(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class DummySession:
        def __init__(self) -> None:
            self.router = "router"

        def observe(self, focus_hint=None):
            return {"focus": focus_hint}

        def call_tool(self, tool, args=None):
            return {"tool": tool, "args": args or {}}

        def act_and_observe(self, tool, args=None):
            return {"tool": tool, "observed": True}

        def pending(self):
            return {"queued": 1}

        def capability_graphs(self):
            return {"graphs": True}

        def graph_plan(self, *, domain=None, limit=12):
            return {"domain": domain, "limit": limit}

        def graph_action(self, action):
            return {"action": action}

        def orientation(self):
            return {"organization_name": "Acme"}

        def structure_view(self):
            return {"cases": True}

        def compare_structure_to_truth(self):
            return {"mirror": True}

        def snapshot(self, label=None):
            return {"snapshot": label}

        def restore(self, snapshot_id):
            return {"restored": snapshot_id}

        def branch(self, snapshot_id, branch_name):
            return {"snapshot": snapshot_id, "branch": branch_name}

        def replay(self, *, mode, dataset_events=None):
            return {"mode": mode, "dataset_events": dataset_events}

        def inject(self, event):
            return {"event": event}

        def list_events(self):
            return [{"event_id": "evt-1"}]

        def cancel_event(self, event_id):
            return {"cancelled": event_id}

    dummy_session = DummySession()
    fake_router = SimpleNamespace(world_session=None)
    monkeypatch.setattr(
        world_api.WorldSession,
        "attach_router",
        staticmethod(lambda router: dummy_session),
    )
    monkeypatch.setattr(world_api, "create_router", lambda **kwargs: fake_router)

    attached = world_api.ensure_world_session(fake_router)
    created = world_api.create_world_session(seed=7, branch="demo")

    assert attached is dummy_session
    assert created is dummy_session
    assert world_api.observe(dummy_session, "slack") == {"focus": "slack"}
    assert world_api.call_tool(dummy_session, "browser.read", {"url": "x"})["tool"] == (
        "browser.read"
    )
    assert world_api.capability_graphs(dummy_session) == {"graphs": True}
    assert world_api.graph_plan(dummy_session, domain="crm", limit=3) == {
        "domain": "crm",
        "limit": 3,
    }
    assert world_api.graph_action(dummy_session, {"kind": "noop"}) == {
        "action": {"kind": "noop"}
    }
    assert world_api.orientation(dummy_session) == {"organization_name": "Acme"}
    assert world_api.structure_view(dummy_session) == {"cases": True}
    assert world_api.compare_structure_to_truth(dummy_session) == {"mirror": True}
    assert world_api.snapshot(dummy_session, "start") == {"snapshot": "start"}
    assert world_api.restore(dummy_session, 1) == {"restored": 1}
    assert world_api.branch(dummy_session, 1, "demo") == {
        "snapshot": 1,
        "branch": "demo",
    }
    assert world_api.replay(dummy_session, mode="dataset", dataset_events=[1]) == {
        "mode": "dataset",
        "dataset_events": [1],
    }
    assert world_api.inject(dummy_session, {"kind": "wake"}) == {
        "event": {"kind": "wake"}
    }
    assert world_api.list_events(dummy_session) == [{"event_id": "evt-1"}]
    assert world_api.cancel_event(dummy_session, "evt-1") == {"cancelled": "evt-1"}


def test_world_scenario_catalog_and_env_loading(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    scenario = scenario_catalog.get_scenario("multi_channel")
    assert scenario.metadata["scenario_name"] == "multi_channel"
    assert "multi_channel" in scenario_catalog.list_scenarios()

    monkeypatch.setenv("VEI_SCENARIO", "multi_channel")
    assert scenario_catalog.load_from_env().metadata["scenario_name"] == "multi_channel"
    monkeypatch.delenv("VEI_SCENARIO", raising=False)

    monkeypatch.setenv("VEI_SCENARIO", "missing")
    assert isinstance(scenario_catalog.load_from_env(), Scenario)
    monkeypatch.delenv("VEI_SCENARIO", raising=False)

    template_path = tmp_path / "scenario.json"
    template_path.write_text(
        json.dumps(
            {
                "budget_cap_usd": 3000,
                "vendors": [
                    {"name": "EnvVendor", "price": [1500, 1600], "eta_days": [3, 4]}
                ],
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setenv("VEI_SCENARIO_CONFIG", str(template_path))
    generated = scenario_catalog.load_from_env(seed=7)
    assert any(
        "EnvVendor" in value for value in (generated.vendor_reply_variants or [])
    )
    monkeypatch.delenv("VEI_SCENARIO_CONFIG", raising=False)

    monkeypatch.setenv("VEI_SCENARIO_CONFIG", "{bad json")
    assert isinstance(scenario_catalog.load_from_env(), Scenario)
    monkeypatch.delenv("VEI_SCENARIO_CONFIG", raising=False)

    asset = build_blueprint_asset_for_example("acquired_user_cutover")
    asset_path = tmp_path / "asset.json"
    asset_path.write_text(asset.model_dump_json(indent=2), encoding="utf-8")
    monkeypatch.setenv("VEI_BLUEPRINT_ASSET", str(asset_path))
    assert isinstance(scenario_catalog.load_from_env(seed=7), Scenario)
    monkeypatch.delenv("VEI_BLUEPRINT_ASSET", raising=False)

    monkeypatch.setenv("VEI_SCENARIO_RANDOM", "1")
    assert isinstance(scenario_catalog.load_from_env(seed=7), Scenario)
    monkeypatch.delenv("VEI_SCENARIO_RANDOM", raising=False)

    assert world_api.get_catalog_scenario("multi_channel").metadata[
        "scenario_name"
    ] == ("multi_channel")
    assert (
        world_api.get_catalog_scenario_manifest("multi_channel").name == "multi_channel"
    )
    assert any(
        item.name == "multi_channel"
        for item in world_api.list_catalog_scenario_manifest()
    )
