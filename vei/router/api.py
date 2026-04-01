from __future__ import annotations

from typing import Any, Dict, Optional, Protocol, Sequence

from vei.world import Scenario


class ObservationLike(Protocol):
    def model_dump(self) -> Dict[str, Any]: ...


class RouterToolSpecLike(Protocol):
    name: str
    description: str


class RouterToolProvider(Protocol):
    def specs(self) -> Sequence[RouterToolSpecLike]: ...

    def handles(self, tool: str) -> bool: ...

    def call(self, tool: str, args: Dict[str, Any]) -> Any: ...


class RouterAPI(Protocol):
    bus: Any
    trace: Any

    def observe(self, focus_hint: Optional[str] = None) -> ObservationLike: ...

    def call_and_step(self, tool: str, args: Dict[str, Any]) -> Dict[str, Any]: ...

    def act_and_observe(self, tool: str, args: Dict[str, Any]) -> Dict[str, Any]: ...

    def pending(self) -> Dict[str, int]: ...

    def register_tool_provider(self, provider: RouterToolProvider) -> None: ...


class RouterServerAPI(RouterAPI, Protocol):
    registry: Any
    scenario: Optional[Scenario]
    world_session: Any

    def state_snapshot(
        self,
        *,
        include_state: bool = False,
        tool_tail: int = 20,
        include_receipts: bool = True,
    ) -> Dict[str, Any]: ...

    def help_payload(self) -> Dict[str, Any]: ...

    def search_tools(self, query: str, top_k: int = 10) -> Dict[str, Any]: ...

    def tick(self, dt_ms: int) -> Dict[str, Any]: ...


def create_router(
    *,
    seed: int,
    artifacts_dir: Optional[str] = None,
    scenario: Optional[Scenario] = None,
    connector_mode: Optional[str] = None,
    branch: str = "main",
    surface_fidelity: Optional[Dict[str, Any]] = None,
) -> RouterServerAPI:
    """Factory for the router runtime exposed as a typed module API."""
    from .core import Router

    return Router(
        seed=seed,
        artifacts_dir=artifacts_dir,
        scenario=scenario,
        connector_mode=connector_mode,
        branch=branch,
        surface_fidelity=surface_fidelity,
    )
