from __future__ import annotations

from vei.events.api import ActorRef, CanonicalEvent, EventDomain, ObjectRef, StateDelta
from vei.structure.api import (
    build_structure_view_from_canonical_events,
    build_structure_view_from_state_payload,
    build_structure_view_from_world_state,
    compare_structure_to_truth,
    structure_signal_payload,
)
from vei.world.api import create_world_session, get_catalog_scenario


def test_structure_view_reconstructs_cross_surface_case_and_alias_hypothesis() -> None:
    events = [
        CanonicalEvent(
            event_id="evt-1",
            ts_ms=1,
            domain=EventDomain.COMM_GRAPH,
            kind="comm_graph.mail.sent",
            actor_ref=ActorRef(actor_id="alice-1", display_name="Alice Chen"),
            participants=[ActorRef(actor_id="bob-1", display_name="Bob Lee")],
            object_refs=[ObjectRef(object_id="THREAD-1", kind="thread")],
            delta=StateDelta(
                domain=EventDomain.COMM_GRAPH,
                data={
                    "surface": "mail",
                    "conversation_anchor": "renewal-risk",
                    "subject": "Renewal risk",
                },
            ),
        ),
        CanonicalEvent(
            event_id="evt-2",
            ts_ms=2,
            domain=EventDomain.DOC_GRAPH,
            kind="doc_graph.docs.update",
            actor_ref=ActorRef(actor_id="alice-2", display_name="Alice Chen"),
            object_refs=[ObjectRef(object_id="DOC-9", kind="document")],
            delta=StateDelta(
                domain=EventDomain.DOC_GRAPH,
                data={
                    "surface": "docs",
                    "conversation_anchor": "renewal-risk",
                    "subject": "Renewal risk",
                },
            ),
        ),
        CanonicalEvent(
            event_id="evt-3",
            ts_ms=3,
            domain=EventDomain.WORK_GRAPH,
            kind="work_graph.ticket.update",
            actor_ref=ActorRef(actor_id="ops-1", display_name="Ops"),
            object_refs=[ObjectRef(object_id="TICKET-2", kind="ticket")],
            delta=StateDelta(
                domain=EventDomain.WORK_GRAPH,
                data={
                    "surface": "tickets",
                    "conversation_anchor": "different-case",
                    "subject": "Unrelated ticket",
                },
            ),
        ),
    ]

    structure_view = build_structure_view_from_canonical_events(events)

    renewal_case = next(
        item for item in structure_view.cases if "renewal" in item.title.lower()
    )
    assert renewal_case.case_source == "inferred"
    assert {"mail", "docs"} <= set(renewal_case.surfaces)
    assert structure_view.hypotheses
    assert structure_view.hypotheses[0].candidate_entity_ids == [
        "actor:alice-1",
        "actor:alice-2",
    ]
    assert structure_view.suggested_investigations


def test_structure_view_and_comparison_attach_to_world_state() -> None:
    session = create_world_session(
        seed=42042,
        scenario=get_catalog_scenario("oauth_app_containment"),
    )
    session.call_tool(
        "google_admin.suspend_oauth_app",
        {"app_id": "OAUTH-9001", "reason": "containment"},
    )
    state = session.current_state()

    structure_view = build_structure_view_from_world_state(state)
    comparison = compare_structure_to_truth(structure_view, state)

    assert state.event_log
    assert structure_view.total_event_count >= 2
    assert comparison.metrics.entity_link_quality >= 0.0
    assert comparison.metrics.event_ordering >= 0.0


def test_structure_comparison_recovers_payload_object_refs_from_state_events() -> None:
    state = {
        "event_log": [
            {
                "index": 0,
                "event_id": "evt-1",
                "kind": "tool.call",
                "payload": {"tool": "mail.reply", "thread_id": "THREAD-1"},
                "clock_ms": 1,
            }
        ]
    }

    structure_view = build_structure_view_from_state_payload(state)
    comparison = compare_structure_to_truth(structure_view, state)

    assert comparison.truth_entity_refs == ["object:thread:THREAD-1"]
    assert comparison.truth_relation_refs == [
        "involved_in_case:object:thread:THREAD-1->case:THREAD-1"
    ]
    assert structure_signal_payload(comparison) == {
        "entity_link_precision": 1.0,
        "entity_link_recall": 1.0,
        "entity_link_quality": 1.0,
        "relation_precision": 1.0,
        "relation_recall": 1.0,
        "relation_recovery": 1.0,
    }
