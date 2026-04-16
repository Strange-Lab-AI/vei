from __future__ import annotations

from pathlib import Path

from vei.data.models import BaseEvent
from vei.events.api import build_event
from vei.events.models import ActorRef, EventDomain
from vei.ingest.api import SessionSlice
from vei.llm import providers
from vei.world.api import (
    ActorState,
    InjectedEvent,
    ScheduledEvent,
    WorldSession,
    create_world_session,
    get_catalog_scenario,
)


def _session(tmp_path: Path, monkeypatch) -> object:
    return _session_for(tmp_path, monkeypatch, "multi_channel")


def _session_for(tmp_path: Path, monkeypatch, scenario_name: str) -> object:
    monkeypatch.setenv("VEI_STATE_DIR", str(tmp_path / "state"))
    return create_world_session(
        seed=42042,
        scenario=get_catalog_scenario(scenario_name),
    )


def test_world_session_snapshot_restore_round_trip(tmp_path: Path, monkeypatch) -> None:
    session = _session(tmp_path, monkeypatch)
    session.register_actor(
        ActorState(
            actor_id="approver",
            mode="llm_recorded",
            recorded_events=[
                ScheduledEvent(
                    event_id="actor-approval-1",
                    target="slack",
                    payload={
                        "channel": "#procurement",
                        "text": "Recorded approval from actor",
                        "user": "approver",
                    },
                    due_ms=4000,
                    source="actor",
                    actor_id="approver",
                    kind="actor_recorded",
                )
            ],
        )
    )
    injected = session.inject(
        InjectedEvent(
            target="mail",
            payload={
                "from": "human@example.com",
                "subj": "Human follow-up",
                "body_text": "Please confirm price and ETA",
            },
            dt_ms=2000,
            actor_id="human-reviewer",
        )
    )
    baseline = session.snapshot("baseline")
    baseline_event_ids = {item["event_id"] for item in session.list_events()}

    po = session.call_tool(
        "erp.create_po",
        {
            "vendor": "Kernel Co",
            "currency": "USD",
            "lines": [
                {"item_id": "SKU-1", "desc": "Laptop", "qty": 1, "unit_price": 2500}
            ],
        },
    )
    doc = session.call_tool(
        "docs.create",
        {
            "title": "Kernel Notes",
            "body": "Moved into branch-specific state",
            "tags": ["kernel"],
        },
    )
    session.call_tool("okta.suspend_user", {"user_id": "USR-1001"})
    session.call_tool(
        "slack.send_message",
        {"channel": "#procurement", "text": "Temporary branch mutation $2500"},
    )

    session.restore(baseline.snapshot_id)

    docs = session.call_tool("docs.list", {})
    pos = session.call_tool("erp.list_pos", {})
    restored_event_ids = {item["event_id"] for item in session.list_events()}

    assert baseline.snapshot_id >= 0
    assert injected["event_id"] in restored_event_ids
    assert restored_event_ids == baseline_event_ids
    assert all(item["doc_id"] != doc["doc_id"] for item in docs)
    assert all(item["id"] != po["id"] for item in pos)
    assert session.router.okta.get_user("USR-1001")["status"] == "ACTIVE"
    assert session.router.actor_states["approver"].mode == "llm_recorded"


def test_world_session_branch_isolation_covers_mail_crm_and_okta(
    tmp_path: Path, monkeypatch
) -> None:
    session = _session_for(tmp_path, monkeypatch, "macrocompute_default")
    snapshot = session.snapshot("mainline")
    branched = session.branch(snapshot.snapshot_id, "dev")

    branched.call_tool(
        "mail.compose",
        {
            "to": "sales@macrocompute.example",
            "subj": "Branch quote request",
            "body_text": "Please send branch-only pricing.",
        },
    )
    branched.call_tool(
        "crm.create_contact",
        {"email": "branch@example.com", "first_name": "Branch", "last_name": "Only"},
    )
    branched.call_tool("okta.suspend_user", {"user_id": "USR-9001"})

    main_contacts = session.call_tool("crm.list_contacts", {})
    branch_contacts = branched.call_tool("crm.list_contacts", {})

    assert branched.router.state_store.branch == "dev"
    assert len(branched.router.mail.messages) > len(session.router.mail.messages)
    assert all(item["email"] != "branch@example.com" for item in main_contacts)
    assert any(item["email"] == "branch@example.com" for item in branch_contacts)
    assert session.router.okta.get_user("USR-9001")["status"] == "ACTIVE"
    assert branched.router.okta.get_user("USR-9001")["status"] == "SUSPENDED"


def test_world_session_overlay_replay_and_cancel_event(
    tmp_path: Path, monkeypatch
) -> None:
    session = _session_for(tmp_path, monkeypatch, "macrocompute_default")
    result = session.replay(
        mode="overlay",
        dataset_events=[
            BaseEvent(
                time_ms=1500,
                actor_id="vendor",
                channel="mail",
                type="received",
                payload={
                    "from": "vendor@example.com",
                    "subj": "Overlay quote",
                    "body_text": "Price $2400, ETA 4 days",
                },
            )
        ],
    )
    queued = session.list_events()

    assert result["scheduled"] == 1
    assert len(queued) == 1
    cancelled = session.cancel_event(queued[0]["event_id"])
    assert cancelled["ok"] is True
    assert session.list_events() == []


def test_world_session_strict_replay_uses_recorded_actor_events_only(
    tmp_path: Path, monkeypatch
) -> None:
    session = _session(tmp_path, monkeypatch)
    monkeypatch.setattr(
        providers,
        "plan_once",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            AssertionError("LLM should not run")
        ),
    )
    session.register_actor(
        ActorState(
            actor_id="finance-reviewer",
            mode="llm_recorded",
            recorded_events=[
                ScheduledEvent(
                    event_id="rec-1",
                    target="slack",
                    payload={
                        "channel": "#procurement",
                        "text": "Recorded approval with budget $2400",
                        "user": "finance-reviewer",
                    },
                    due_ms=0,
                    source="actor_recording",
                    actor_id="finance-reviewer",
                    kind="actor_recorded",
                )
            ],
        )
    )

    replay = session.replay(mode="strict")
    session.router.tick(dt_ms=0)
    messages = session.router.slack.open_channel("#procurement")["messages"]

    assert replay == {"ok": True, "mode": "strict", "scheduled": 1}
    assert any(
        message["text"] == "Recorded approval with budget $2400" for message in messages
    )


def test_world_session_typed_inject_lists_event_ids(
    tmp_path: Path, monkeypatch
) -> None:
    session = _session_for(tmp_path, monkeypatch, "macrocompute_default")

    result = session.inject(
        {
            "target": "slack",
            "payload": {"channel": "#procurement", "text": "Typed human inject"},
            "dt_ms": 500,
            "actor_id": "operator",
        }
    )
    queued = session.list_events()

    assert result["ok"] is True
    assert result["event_id"]
    assert queued[0]["event_id"] == result["event_id"]
    assert queued[0]["kind"] == "injected"


def test_world_session_from_session_materializer_replays_real_surfaces(
    tmp_path: Path, monkeypatch
) -> None:
    monkeypatch.setenv("VEI_STATE_DIR", str(tmp_path / "state"))

    class StubSessionMaterializer:
        def materialize(
            self,
            tenant_id: str,
            case_id: str,
            *,
            window_ms: int | None = None,
        ) -> SessionSlice:
            del tenant_id, case_id, window_ms
            return SessionSlice(
                tenant_id="acme.example",
                case_id="case-1",
                events=[
                    build_event(
                        domain=EventDomain.COMM_GRAPH,
                        kind="mail.received",
                        case_id="case-1",
                        ts_ms=1_000,
                        actor_ref=ActorRef(actor_id="vendor@example.com"),
                        delta_data={
                            "target": "mail",
                            "from": "vendor@example.com",
                            "to": "me@example",
                            "subject": "Quote",
                            "body_text": "Price is 2400",
                            "thread_id": "thread-quote",
                        },
                    ),
                    build_event(
                        domain=EventDomain.COMM_GRAPH,
                        kind="slack.message",
                        case_id="case-1",
                        ts_ms=2_000,
                        actor_ref=ActorRef(actor_id="finance-reviewer"),
                        delta_data={
                            "target": "slack",
                            "channel": "#approvals",
                            "text": "Need one more approval.",
                        },
                    ),
                ],
            )

    session = WorldSession.from_session_materializer(
        StubSessionMaterializer(),
        "case-1",
        seed=42042,
        tenant_id="acme.example",
    )

    mail_items = session.router.mail.list()
    slack_messages = session.router.slack.open_channel("#approvals")["messages"]

    assert any(item["subj"] == "Quote" for item in mail_items)
    assert any(
        message["text"] == "Need one more approval." for message in slack_messages
    )
    assert session.pending()["total"] == 0
