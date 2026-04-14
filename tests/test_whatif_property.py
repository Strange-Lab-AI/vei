"""Hypothesis property-based tests for heuristic functions in the whatif module."""

from __future__ import annotations

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from vei.whatif.cases import _event_anchor_tokens
from vei.whatif.interventions import intervention_tags
from vei.whatif.models import (
    WhatIfArtifactFlags,
    WhatIfCaseSummary,
    WhatIfEvent,
    WhatIfThreadSummary,
)
from vei.whatif.situations import build_situation_graph

_KNOWN_TAGS: frozenset[str] = frozenset(
    {
        "legal",
        "compliance",
        "hold",
        "pause_forward",
        "status_only",
        "attachment_removed",
        "reply_immediately",
        "clarify_owner",
        "executive_gate",
        "external_removed",
        "send_now",
        "widen_loop",
    }
)

# ---------------------------------------------------------------------------
# Reusable strategies
# ---------------------------------------------------------------------------

_artifact_flags_st = st.builds(
    WhatIfArtifactFlags,
    to_recipients=st.lists(st.text(min_size=1, max_size=30), max_size=3),
    cc_recipients=st.lists(st.text(min_size=1, max_size=30), max_size=3),
    has_attachment_reference=st.booleans(),
    is_forward=st.booleans(),
    is_reply=st.booleans(),
    is_escalation=st.booleans(),
)

_whatif_event_st = st.builds(
    WhatIfEvent,
    event_id=st.text(min_size=1, max_size=20),
    timestamp=st.text(min_size=1, max_size=30),
    timestamp_ms=st.integers(min_value=0, max_value=2**40),
    actor_id=st.text(min_size=1, max_size=30),
    target_id=st.text(max_size=30),
    event_type=st.text(min_size=1, max_size=20),
    thread_id=st.text(min_size=1, max_size=30),
    case_id=st.text(max_size=30),
    surface=st.sampled_from(["mail", "slack", "tickets", "crm", "docs"]),
    conversation_anchor=st.text(max_size=50),
    subject=st.text(max_size=50),
    snippet=st.text(max_size=100),
    flags=_artifact_flags_st,
)

_ACTOR_POOL = ["alice@co.com", "bob@co.com", "charlie@co.com", "dave@co.com"]
_SURFACE_POOL = ["mail", "slack", "tickets", "crm", "docs"]

_thread_ids_st = st.lists(
    st.text(alphabet="abcdefghijklmnop0123456789", min_size=3, max_size=10).map(
        lambda s: f"thread:{s}"
    ),
    min_size=2,
    max_size=8,
    unique=True,
)


def _draw_situation_inputs(
    data: st.DataObject,
    thread_ids: list[str],
) -> tuple[list[WhatIfThreadSummary], list[WhatIfEvent], list[WhatIfCaseSummary]]:
    """Generate threads + events from a fixed thread-id list.

    Uses a small actor/surface pool so shared actors across different
    surfaces are likely, which is how situation links form.
    """
    threads: list[WhatIfThreadSummary] = []
    events: list[WhatIfEvent] = []
    for tid in thread_ids:
        surface = data.draw(st.sampled_from(_SURFACE_POOL))
        actors = data.draw(
            st.lists(st.sampled_from(_ACTOR_POOL), min_size=1, max_size=3)
        )
        threads.append(
            WhatIfThreadSummary(
                thread_id=tid,
                subject=data.draw(st.text(min_size=1, max_size=30)),
                surface=surface,
                actor_ids=actors,
                event_count=data.draw(st.integers(min_value=1, max_value=10)),
                first_timestamp="2001-01-01T00:00:00Z",
                last_timestamp="2001-12-31T23:59:59Z",
            )
        )
        n_events = data.draw(st.integers(min_value=1, max_value=4))
        for i in range(n_events):
            actor = data.draw(st.sampled_from(actors))
            ts_ms = data.draw(st.integers(min_value=1_000_000, max_value=2_000_000))
            events.append(
                WhatIfEvent(
                    event_id=f"{tid}-ev{i}",
                    timestamp="2001-06-15T12:00:00Z",
                    timestamp_ms=ts_ms,
                    actor_id=actor,
                    event_type="message",
                    thread_id=tid,
                    surface=surface,
                    subject=data.draw(st.text(max_size=30)),
                )
            )
    return threads, events, []


# ---------------------------------------------------------------------------
# intervention_tags properties
# ---------------------------------------------------------------------------


@pytest.mark.hypothesis
class TestInterventionTagsProperties:
    @given(prompt=st.text())
    def test_return_type_is_set_of_str(self, prompt: str) -> None:
        result = intervention_tags(prompt)
        assert isinstance(result, set)
        assert all(isinstance(tag, str) for tag in result)

    def test_empty_string_returns_empty_set(self) -> None:
        assert intervention_tags("") == set()

    @given(prompt=st.text())
    def test_tags_from_known_vocabulary(self, prompt: str) -> None:
        result = intervention_tags(prompt)
        assert result <= _KNOWN_TAGS, f"Unknown tags: {result - _KNOWN_TAGS}"

    @given(prompt=st.text())
    def test_never_raises(self, prompt: str) -> None:
        intervention_tags(prompt)


# ---------------------------------------------------------------------------
# _event_anchor_tokens properties
# ---------------------------------------------------------------------------


@pytest.mark.hypothesis
class TestEventAnchorTokensProperties:
    @given(event=_whatif_event_st)
    def test_tokens_are_non_empty_strings(self, event: WhatIfEvent) -> None:
        tokens = _event_anchor_tokens(event)
        assert isinstance(tokens, list)
        for token in tokens:
            assert isinstance(token, str)
            assert len(token) > 0

    @given(event=_whatif_event_st)
    def test_never_raises_on_valid_input(self, event: WhatIfEvent) -> None:
        _event_anchor_tokens(event)


# ---------------------------------------------------------------------------
# Situation graph invariants
# ---------------------------------------------------------------------------


@pytest.mark.hypothesis
class TestSituationGraphProperties:
    @given(thread_ids=_thread_ids_st, data=st.data())
    @settings(max_examples=50)
    def test_every_cluster_has_at_least_two_surfaces(
        self,
        thread_ids: list[str],
        data: st.DataObject,
    ) -> None:
        threads, events, cases = _draw_situation_inputs(data, thread_ids)
        graph = build_situation_graph(threads=threads, cases=cases, events=events)
        for cluster in graph.clusters:
            assert (
                len(cluster.surfaces) >= 2
            ), f"Cluster {cluster.situation_id} has surfaces={cluster.surfaces}"

    @given(thread_ids=_thread_ids_st, data=st.data())
    @settings(max_examples=50)
    def test_link_thread_ids_consistent_with_clusters(
        self,
        thread_ids: list[str],
        data: st.DataObject,
    ) -> None:
        threads, events, cases = _draw_situation_inputs(data, thread_ids)
        graph = build_situation_graph(threads=threads, cases=cases, events=events)

        thread_to_cluster: dict[str, str] = {}
        for cluster in graph.clusters:
            for tid in cluster.thread_ids:
                thread_to_cluster[tid] = cluster.situation_id

        for link in graph.links:
            a_cluster = thread_to_cluster.get(link.thread_id_a)
            b_cluster = thread_to_cluster.get(link.thread_id_b)
            if a_cluster is not None or b_cluster is not None:
                assert a_cluster is not None and b_cluster is not None, (
                    f"Link {link.thread_id_a} <-> {link.thread_id_b}: "
                    f"only one endpoint is in a cluster"
                )
                assert a_cluster == b_cluster, (
                    f"Link endpoints in different clusters: "
                    f"{link.thread_id_a} in {a_cluster}, "
                    f"{link.thread_id_b} in {b_cluster}"
                )
