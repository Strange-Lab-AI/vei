from __future__ import annotations

from importlib import import_module
from typing import Any

__all__ = [
    "ActorState",
    "CalendarEvent",
    "Document",
    "DriftEngine",
    "Event",
    "IdentityApplicationSeed",
    "IdentityGroupSeed",
    "IdentityUserSeed",
    "InjectedEvent",
    "ReplayAdapter",
    "Scenario",
    "ScenarioManifest",
    "ScheduledEvent",
    "ServiceDeskIncident",
    "ServiceDeskRequest",
    "SpreadsheetSheet",
    "SpreadsheetWorkbook",
    "StateStore",
    "Ticket",
    "WorldSession",
    "WorldSnapshot",
    "WorldState",
    "build_scenario_manifest",
    "compile_scene",
    "generate_scenario",
    "get_scenario",
    "get_scenario_manifest",
    "list_scenario_manifest",
    "list_scenarios",
    "load_from_env",
]


def __getattr__(name: str) -> Any:  # pragma: no cover - thin import facade
    if name in {
        "CalendarEvent",
        "Document",
        "IdentityApplicationSeed",
        "IdentityGroupSeed",
        "IdentityUserSeed",
        "Scenario",
        "ServiceDeskIncident",
        "ServiceDeskRequest",
        "SpreadsheetSheet",
        "SpreadsheetWorkbook",
        "Ticket",
    }:
        module = import_module("vei.world.scenario")
        return getattr(module, name)
    if name in {
        "ScenarioManifest",
        "build_scenario_manifest",
        "get_scenario_manifest",
        "list_scenario_manifest",
    }:
        module = import_module("vei.world.manifest")
        return getattr(module, name)
    if name in {
        "generate_scenario",
        "get_scenario",
        "list_scenarios",
        "load_from_env",
    }:
        module = import_module("vei.world.scenarios")
        return getattr(module, name)
    if name in {
        "ActorState",
        "InjectedEvent",
        "ScheduledEvent",
        "WorldSnapshot",
        "WorldState",
    }:
        module = import_module("vei.world.models")
        return getattr(module, name)
    if name in {"Event", "StateStore"}:
        module = import_module("vei.world.state")
        return getattr(module, name)
    if name == "WorldSession":
        module = import_module("vei.world.session")
        return getattr(module, name)
    if name == "ReplayAdapter":
        module = import_module("vei.world.replay")
        return getattr(module, name)
    if name == "DriftEngine":
        module = import_module("vei.world.drift")
        return getattr(module, name)
    if name == "compile_scene":
        module = import_module("vei.world.compiler")
        return getattr(module, name)
    raise AttributeError(name)


"""World simulation kernel: sessions, scenarios, state, and replay."""
