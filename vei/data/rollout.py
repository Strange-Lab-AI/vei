from __future__ import annotations

from typing import Iterable, List

from vei.behavior.policy import ScriptedProcurementPolicy
from vei.data.models import BaseEvent, DatasetMetadata, VEIDataset
from vei.router.api import RouterAPI
from vei.world import get_scenario
from vei.world.api import create_world_session


def rollout_procurement(
    episodes: int,
    seed: int,
    scenario_name: str | None = None,
) -> VEIDataset:
    events: List[BaseEvent] = []
    for idx in range(max(1, episodes)):
        router_seed = seed + idx
        session = create_world_session(
            seed=router_seed,
            artifacts_dir=None,
            scenario=(get_scenario(scenario_name) if scenario_name else None),
            branch=f"rollout-{idx:03d}",
        )
        session.snapshot("rollout.start")
        runner = ScriptedProcurementPolicy(session.router)
        runner.run()
        session.snapshot("rollout.final")
        events.extend(_events_from_trace(session.router))
    metadata = DatasetMetadata(
        name="procurement_rollout",
        description="Scripted procurement rollout captured from WorldSession",
        tags=["rollout", "scripted", "world_session"],
        source=scenario_name or "default",
    )
    return VEIDataset(
        metadata=metadata,
        events=sorted(events, key=lambda e: e.time_ms),
    )


def _events_from_trace(router: RouterAPI) -> Iterable[BaseEvent]:
    for entry in router.trace.entries:
        time_ms = int(entry.get("time_ms", router.bus.clock_ms))
        if entry.get("type") == "event":
            target = str(entry.get("target", "router"))
            payload = entry.get("payload", {})
            yield BaseEvent(
                time_ms=time_ms,
                actor_id="system",
                channel=target,
                type="event",
                payload={"payload": payload, "emitted": entry.get("emitted")},
            )
        elif entry.get("type") == "call":
            tool = str(entry.get("tool", "tool"))
            yield BaseEvent(
                time_ms=time_ms,
                actor_id="agent",
                channel="tool",
                type=tool,
                payload={
                    "args": entry.get("args", {}),
                    "response": entry.get("response"),
                },
            )
