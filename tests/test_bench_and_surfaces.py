"""Tests for vei bench CLI, fidelity interception, surface fixes, and actor dispatch."""

from __future__ import annotations

import pytest
from typer.testing import CliRunner

from vei.blueprint.models import SurfaceFidelitySpec
from vei.router.core import EventBus, MailSim, SlackSim
from vei.world.api import create_world_session
from vei.world.scenario import Scenario

runner = CliRunner()


# ---------------------------------------------------------------------------
# vei bench CLI
# ---------------------------------------------------------------------------


class TestBenchCLI:
    def test_bench_list_all(self) -> None:
        from vei.cli.vei_bench import app

        result = runner.invoke(app, ["list", "--kind", "all"])
        assert result.exit_code == 0
        assert "Scenarios" in result.output
        assert "Vertical Packs" in result.output
        assert "Benchmark Families" in result.output

    def test_bench_list_scenarios(self) -> None:
        from vei.cli.vei_bench import app

        result = runner.invoke(app, ["list", "--kind", "scenarios"])
        assert result.exit_code == 0
        assert "Scenarios" in result.output
        assert "Vertical Packs" not in result.output

    def test_bench_list_verticals(self) -> None:
        from vei.cli.vei_bench import app

        result = runner.invoke(app, ["list", "--kind", "verticals"])
        assert result.exit_code == 0
        assert "Vertical Packs" in result.output

    def test_bench_list_families(self) -> None:
        from vei.cli.vei_bench import app

        result = runner.invoke(app, ["list", "--kind", "families"])
        assert result.exit_code == 0
        assert "Benchmark Families" in result.output


# ---------------------------------------------------------------------------
# Fidelity interception (L1 / L2 / L3)
# ---------------------------------------------------------------------------


class TestFidelityInterception:
    def test_l1_returns_static_response(self) -> None:
        session = create_world_session(
            seed=1,
            surface_fidelity={"slack": SurfaceFidelitySpec(level="L1")},
        )
        result = session.call_tool("slack.list_channels", {})
        assert result["fidelity"] == "L1"
        assert result["status"] == "ok"

    def test_l1_with_custom_static_response(self) -> None:
        session = create_world_session(
            seed=1,
            surface_fidelity={
                "slack": SurfaceFidelitySpec(
                    level="L1",
                    static_responses={
                        "slack.list_channels": {"channels": ["#general"]}
                    },
                )
            },
        )
        result = session.call_tool("slack.list_channels", {})
        assert result == {"channels": ["#general"]}

    def test_l2_stores_and_retrieves(self) -> None:
        session = create_world_session(
            seed=1,
            surface_fidelity={"slack": SurfaceFidelitySpec(level="L2")},
        )
        send_result = session.call_tool(
            "slack.send_message", {"channel": "general", "text": "hello"}
        )
        assert send_result["fidelity"] == "L2"
        assert send_result["status"] == "ok"

        list_result = session.call_tool("slack.list_channels", {})
        assert list_result["fidelity"] == "L2"
        assert len(list_result["items"]) > 0

    def test_l3_passes_through_to_sim(self) -> None:
        session = create_world_session(seed=1)
        result = session.call_tool("slack.list_channels", {})
        assert isinstance(result, list)
        assert any("#" in ch for ch in result)

    def test_mixed_fidelity_per_surface(self) -> None:
        session = create_world_session(
            seed=1,
            surface_fidelity={
                "slack": SurfaceFidelitySpec(level="L1"),
            },
        )
        slack_result = session.call_tool("slack.list_channels", {})
        assert slack_result["fidelity"] == "L1"

        mail_result = session.call_tool("mail.list", {})
        assert isinstance(mail_result, list)


# ---------------------------------------------------------------------------
# Slack reactions
# ---------------------------------------------------------------------------


class TestSlackReactions:
    @pytest.fixture()
    def slack(self) -> SlackSim:
        bus = EventBus(42)
        scenario = Scenario(
            slack_initial_message="hello",
            slack_channels={"general": {"messages": []}},
        )
        sim = SlackSim(bus, scenario)
        sim.send_message("general", "test message")
        return sim

    def test_react_stores_reaction(self, slack: SlackSim) -> None:
        msgs = slack.open_channel("general")["messages"]
        ts = msgs[0]["ts"]
        result = slack.react("general", ts, "thumbsup")
        assert result["ok"] is True

        msg = slack.open_channel("general")["messages"][0]
        assert "reactions" in msg
        assert msg["reactions"][0]["name"] == "thumbsup"
        assert msg["reactions"][0]["count"] == 1
        assert "agent" in msg["reactions"][0]["users"]

    def test_duplicate_emoji_increments_count(self, slack: SlackSim) -> None:
        msgs = slack.open_channel("general")["messages"]
        ts = msgs[0]["ts"]
        slack.react("general", ts, "thumbsup")
        slack.react("general", ts, "thumbsup")

        msg = slack.open_channel("general")["messages"][0]
        assert len(msg["reactions"]) == 1
        assert msg["reactions"][0]["count"] == 2

    def test_different_emojis_stored_separately(self, slack: SlackSim) -> None:
        msgs = slack.open_channel("general")["messages"]
        ts = msgs[0]["ts"]
        slack.react("general", ts, "thumbsup")
        slack.react("general", ts, "heart")

        msg = slack.open_channel("general")["messages"][0]
        assert len(msg["reactions"]) == 2
        names = {r["name"] for r in msg["reactions"]}
        assert names == {"thumbsup", "heart"}


# ---------------------------------------------------------------------------
# Mail read/unread
# ---------------------------------------------------------------------------


class TestMailReadUnread:
    @pytest.fixture()
    def mail(self) -> MailSim:
        bus = EventBus(42)
        scenario = Scenario(
            slack_initial_message="hello",
            mail_threads=[
                {
                    "thread_id": "t1",
                    "title": "Budget Approval",
                    "messages": [
                        {
                            "from": "boss@vendor.com",
                            "to": "me@example",
                            "subj": "Budget",
                            "body_text": "Please approve.",
                            "unread": True,
                        }
                    ],
                }
            ],
        )
        return MailSim(bus, scenario)

    def test_message_starts_unread(self, mail: MailSim) -> None:
        inbox = mail.list()
        assert len(inbox) == 1
        assert inbox[0]["unread"] is True

    def test_open_marks_as_read(self, mail: MailSim) -> None:
        inbox = mail.list()
        mid = inbox[0]["id"]
        mail.open(mid)
        assert mail.messages[mid]["unread"] is False

    def test_open_returns_content(self, mail: MailSim) -> None:
        inbox = mail.list()
        mid = inbox[0]["id"]
        result = mail.open(mid)
        assert "headers" in result
        assert "body_text" in result
        assert result["body_text"] == "Please approve."


# ---------------------------------------------------------------------------
# Actor dispatch via WorldSession
# ---------------------------------------------------------------------------


class TestActorDispatch:
    def test_attach_and_dispatch(self) -> None:
        from vei.actors.api import ActorRegistry
        from vei.actors.persona import ActorPersona

        session = create_world_session(seed=1)
        registry = ActorRegistry()
        registry.register(
            ActorPersona(
                name="Jane CFO",
                email="jane@example.com",
                role="CFO",
                department="Finance",
                response_bias="cooperative",
                backend="deterministic",
            )
        )
        session.attach_actor_registry(registry)

        session.router.bus.schedule(
            dt_ms=0,
            target="slack",
            payload={
                "text": "Can you approve this budget?",
                "channel": "#procurement",
            },
            actor_id="jane@example.com",
        )
        session.observe()

        log = registry.event_log()
        assert len(log) == 1
        assert log[0]["actor"] == "Jane CFO"
        assert log[0]["backend"] == "deterministic"

    def test_no_registry_no_dispatch(self) -> None:
        session = create_world_session(seed=1)

        session.router.bus.schedule(
            dt_ms=0,
            target="slack",
            payload={"text": "hello", "channel": "#general"},
            actor_id="nobody@example.com",
        )
        session.observe()

    def test_response_scheduled_back(self) -> None:
        from vei.actors.api import ActorRegistry
        from vei.actors.persona import ActorPersona

        session = create_world_session(seed=1)
        registry = ActorRegistry()
        registry.register(
            ActorPersona(
                name="Jane CFO",
                email="jane@example.com",
                role="CFO",
                department="Finance",
                response_bias="cooperative",
                backend="deterministic",
            )
        )
        session.attach_actor_registry(registry)

        session.router.bus.schedule(
            dt_ms=0,
            target="slack",
            payload={
                "text": "Please review this.",
                "channel": "#procurement",
            },
            actor_id="jane@example.com",
        )
        session.observe()

        pending = session.pending()
        assert pending.get("slack", 0) >= 1
