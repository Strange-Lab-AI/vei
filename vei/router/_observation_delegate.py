from __future__ import annotations

from typing import TYPE_CHECKING, Any, Dict, Optional, Type

from ._observation import build_action_menu, build_focus_summary, resolve_focus_for_tool

if TYPE_CHECKING:
    from .core import Router


class RouterObservation:
    @staticmethod
    def snapshot_observation(
        router: Router,
        observation_cls: Type[Any],
        focus_hint: Optional[str] = None,
    ) -> Any:
        focus = focus_hint or "browser"
        return observation_cls(
            time_ms=router.bus.clock_ms,
            focus=focus,
            summary=RouterObservation.summary(router, focus),
            screenshot_ref=None,
            action_menu=RouterObservation.action_menu(router, focus),
            pending_events=RouterObservation.pending_counts(router),
        )

    @staticmethod
    def step_and_observe(
        router: Router,
        observation_cls: Type[Any],
        tool: str,
        args: Dict[str, Any],
    ) -> Any:
        router.call_and_step(tool, args)
        focus = RouterObservation.focus_for_tool(router, tool)
        return RouterObservation.snapshot_observation(router, observation_cls, focus)

    @staticmethod
    def act_and_observe(
        router: Router,
        observation_cls: Type[Any],
        tool: str,
        args: Dict[str, Any],
    ) -> Dict[str, Any]:
        result = router.call_and_step(tool, args)
        focus = RouterObservation.focus_for_tool(router, tool)
        obs = RouterObservation.snapshot_observation(router, observation_cls, focus)
        return {"result": result, "observation": obs.model_dump()}

    @staticmethod
    def focus_for_tool(router: Router, tool: str) -> str:
        return resolve_focus_for_tool(router, tool)

    @staticmethod
    def pending_counts(router: Router) -> Dict[str, int]:
        counts = {target: 0 for target in router._event_targets()}
        for _, _, event in router.bus._heap:
            counts[event.target] = counts.get(event.target, 0) + 1
        counts["total"] = router.bus.pending_count()
        return counts

    @staticmethod
    def observe(
        router: Router,
        observation_cls: Type[Any],
        focus_hint: Optional[str] = None,
    ) -> Any:
        evt = router.bus.next_if_due()
        if evt:
            router._deliver_due_event(evt)
        router.bus.advance(1000)
        focus = focus_hint or "browser"
        obs = observation_cls(
            time_ms=router.bus.clock_ms,
            focus=focus,
            summary=RouterObservation.summary(router, focus),
            screenshot_ref=None,
            action_menu=RouterObservation.action_menu(router, focus),
            pending_events=RouterObservation.pending_counts(router),
        )
        router.trace.flush()
        return obs

    @staticmethod
    def summary(router: Router, focus: str) -> str:
        return build_focus_summary(router, focus)

    @staticmethod
    def action_menu(router: Router, focus: str) -> list[dict[str, Any]]:
        return build_action_menu(router, focus)
