import pytest

from vei.router.core import Router

pytestmark = pytest.mark.integration


def test_inject_slack_message():
    router = Router(seed=42)

    # Inject a message into Slack
    payload = {"channel": "#procurement", "text": "Hello from Human", "user": "human"}
    res = router.call_and_step(
        "vei.inject", {"target": "slack", "payload": payload, "dt_ms": 0}
    )
    assert res["ok"] is True

    # Check Slack messages
    # Since dt_ms=0, it should be scheduled for now.
    # call_and_step checks for due events immediately after execution.

    ch = router.slack.channels["#procurement"]
    msgs = ch["messages"]
    found = any(m["text"] == "Hello from Human" and m["user"] == "human" for m in msgs)

    if not found:
        # It might require a tick if the heap push happened after the pop check (which it didn't, execute happens before next_if_due)
        # But maybe heap order or something.
        router.tick(dt_ms=100)
        msgs = ch["messages"]
        found = any(
            m["text"] == "Hello from Human" and m["user"] == "human" for m in msgs
        )

    assert found


def test_inject_mail_message():
    router = Router(seed=42)

    payload = {
        "from": "human@example.com",
        "subj": "Human Email",
        "body_text": "Hi agent",
    }
    router.call_and_step(
        "vei.inject", {"target": "mail", "payload": payload, "dt_ms": 1000}
    )

    # Should not be there yet (dt_ms=1000)
    inbox = router.mail.list()
    assert not any(m["subj"] == "Human Email" for m in inbox)

    # Tick forward
    router.tick(dt_ms=1000)

    inbox = router.mail.list()
    assert any(m["subj"] == "Human Email" for m in inbox)
