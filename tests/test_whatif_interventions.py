from __future__ import annotations

import pytest

from vei.whatif._helpers import intervention_tags


@pytest.mark.parametrize(
    "prompt, expected_tag",
    [
        ("hold the send", "hold"),
        ("Please pause all outgoing mail.", "hold"),
        ("freeze the forward chain", "hold"),
        ("keep it internal", "external_removed"),
        ("keep the issue internal for now", "external_removed"),
        ("internal only please", "external_removed"),
        ("We need legal review before sending.", "legal"),
        ("Route through the VP for approval", "executive_gate"),
        ("clarify owner before we proceed", "clarify_owner"),
        ("send now", "send_now"),
    ],
)
def test_true_positives(prompt: str, expected_tag: str) -> None:
    assert expected_tag in intervention_tags(prompt)


@pytest.mark.parametrize(
    "prompt, absent_tag",
    [
        ("The holder of the contract was notified.", "hold"),
        ("Please review the placeholder text.", "hold"),
        ("She is an intern at the company.", "external_removed"),
        ("internal review of the document is pending.", "external_removed"),
    ],
)
def test_false_positives_avoided(prompt: str, absent_tag: str) -> None:
    assert absent_tag not in intervention_tags(prompt)
