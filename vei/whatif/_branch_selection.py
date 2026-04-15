from __future__ import annotations

from .corpus import (
    choose_branch_event,
    event_by_id,
    hydrate_event_snippets,
    thread_events,
    thread_subject,
)
from .models import WhatIfEvent, WhatIfWorld
from .situations import recommend_branch_thread


def resolve_thread_branch(
    world: WhatIfWorld,
    *,
    thread_id: str | None = None,
    event_id: str | None = None,
) -> tuple[
    str,
    list[WhatIfEvent],
    WhatIfEvent,
    list[WhatIfEvent],
    list[WhatIfEvent],
    str,
]:
    selected_thread_id = thread_id
    if selected_thread_id is None:
        if event_id:
            selected_event = event_by_id(world.events, event_id)
            if selected_event is None:
                raise ValueError(f"event not found in world: {event_id}")
            selected_thread_id = selected_event.thread_id
        else:
            selected_thread_id = recommend_branch_thread(world).thread_id

    thread_history = thread_events(world.events, selected_thread_id)
    if not thread_history:
        raise ValueError(f"thread not found in world: {selected_thread_id}")
    if world.source == "enron":
        thread_history = hydrate_event_snippets(
            rosetta_dir=world.source_dir,
            events=thread_history,
        )

    branch_event = choose_branch_event(thread_history, requested_event_id=event_id)
    branch_index = next(
        (
            index
            for index, event in enumerate(thread_history)
            if event.event_id == branch_event.event_id
        ),
        None,
    )
    if branch_index is None:
        raise ValueError(f"branch event not found in thread: {branch_event.event_id}")

    return (
        selected_thread_id,
        thread_history,
        branch_event,
        list(thread_history[:branch_index]),
        list(thread_history[branch_index:]),
        thread_subject(
            world.threads,
            selected_thread_id,
            fallback=branch_event.subject,
        ),
    )


__all__ = ["resolve_thread_branch"]
