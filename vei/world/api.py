from __future__ import annotations

from typing import Any, Dict, Optional, Protocol

from vei.router.api import create_router
from vei.capability_graph.models import RuntimeCapabilityGraphs
from vei.orientation.models import WorldOrientation
from vei.world.manifest import (
    ScenarioManifest,
    get_scenario_manifest,
    list_scenario_manifest,
)
from vei.world.models import (
    ActorState,
    InjectedEvent,
    ScheduledEvent,
    WorldSnapshot,
    WorldState,
)
from vei.world.scenario import Scenario
from vei.world.session import WorldSession


class EventBusAPI(Protocol):
    clock_ms: int

    def schedule(self, dt_ms: int, target: str, payload: Dict[str, Any]) -> Any: ...


class WorldSessionAPI(Protocol):
    router: Any

    def observe(self, focus_hint: Optional[str] = None) -> Dict[str, Any]: ...

    def call_tool(
        self, tool: str, args: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]: ...

    def act_and_observe(
        self, tool: str, args: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]: ...

    def pending(self) -> Dict[str, int]: ...

    def capability_graphs(self) -> RuntimeCapabilityGraphs: ...

    def orientation(self) -> WorldOrientation: ...

    def snapshot(self, label: Optional[str] = None) -> WorldSnapshot: ...

    def restore(self, snapshot_id: int) -> WorldSnapshot: ...

    def branch(self, snapshot_id: int, branch_name: str) -> "WorldSessionAPI": ...

    def replay(
        self, *, mode: str, dataset_events: Optional[list[Any]] = None
    ) -> Dict[str, Any]: ...

    def inject(self, event: InjectedEvent | Dict[str, Any]) -> Dict[str, Any]: ...

    def list_events(self) -> list[Dict[str, Any]]: ...

    def cancel_event(self, event_id: str) -> Dict[str, Any]: ...


def create_world_session(
    *,
    seed: int = 42042,
    artifacts_dir: Optional[str] = None,
    scenario: Optional[Scenario] = None,
    connector_mode: Optional[str] = None,
    branch: str = "main",
) -> WorldSession:
    router = create_router(
        seed=seed,
        artifacts_dir=artifacts_dir,
        scenario=scenario,
        connector_mode=connector_mode,
        branch=branch,
    )
    if getattr(router, "world_session", None) is None:
        router.world_session = WorldSession.attach_router(router)  # type: ignore[attr-defined]
    return router.world_session  # type: ignore[attr-defined,return-value]


def observe(
    session: WorldSessionAPI, focus_hint: Optional[str] = None
) -> Dict[str, Any]:
    return session.observe(focus_hint=focus_hint)


def call_tool(
    session: WorldSessionAPI, tool: str, args: Optional[Dict[str, Any]] = None
) -> Dict[str, Any]:
    return session.call_tool(tool, args=args)


def capability_graphs(session: WorldSessionAPI) -> RuntimeCapabilityGraphs:
    return session.capability_graphs()


def orientation(session: WorldSessionAPI) -> WorldOrientation:
    return session.orientation()


def snapshot(session: WorldSessionAPI, label: Optional[str] = None) -> WorldSnapshot:
    return session.snapshot(label=label)


def restore(session: WorldSessionAPI, snapshot_id: int) -> WorldSnapshot:
    return session.restore(snapshot_id)


def branch(
    session: WorldSessionAPI, snapshot_id: int, branch_name: str
) -> WorldSessionAPI:
    return session.branch(snapshot_id, branch_name)


def replay(
    session: WorldSessionAPI, *, mode: str, dataset_events: Optional[list[Any]] = None
) -> Dict[str, Any]:
    return session.replay(mode=mode, dataset_events=dataset_events)


def inject(
    session: WorldSessionAPI, event: InjectedEvent | Dict[str, Any]
) -> Dict[str, Any]:
    return session.inject(event)


def list_events(session: WorldSessionAPI) -> list[Dict[str, Any]]:
    return session.list_events()


def cancel_event(session: WorldSessionAPI, event_id: str) -> Dict[str, Any]:
    return session.cancel_event(event_id)


def get_catalog_scenario(name: str) -> Any:
    from vei.world.scenarios import get_scenario

    return get_scenario(name)


def get_catalog_scenario_manifest(name: str) -> ScenarioManifest:
    return get_scenario_manifest(name)


def list_catalog_scenario_manifest() -> list[ScenarioManifest]:
    return list_scenario_manifest()


__all__ = [
    "ActorState",
    "InjectedEvent",
    "WorldOrientation",
    "ScheduledEvent",
    "RuntimeCapabilityGraphs",
    "WorldSession",
    "WorldSessionAPI",
    "WorldSnapshot",
    "WorldState",
    "branch",
    "capability_graphs",
    "call_tool",
    "cancel_event",
    "create_world_session",
    "get_catalog_scenario",
    "get_catalog_scenario_manifest",
    "inject",
    "list_catalog_scenario_manifest",
    "list_events",
    "observe",
    "orientation",
    "replay",
    "restore",
    "snapshot",
]
