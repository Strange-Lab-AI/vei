from __future__ import annotations

import re
from collections import Counter, defaultdict
from dataclasses import dataclass
from itertools import combinations
from typing import Iterable, Sequence

from .models import (
    WhatIfCaseSummary,
    WhatIfEvent,
    WhatIfEventReference,
    WhatIfSituationCluster,
    WhatIfSituationContext,
    WhatIfSituationGraph,
    WhatIfSituationLink,
    WhatIfSituationThread,
    WhatIfThreadSummary,
    WhatIfWorld,
)

_PROXIMITY_WINDOW_MS = 7 * 24 * 60 * 60 * 1000
_DOCS_CRM_WINDOW_MS = 3 * 24 * 60 * 60 * 1000
_TERM_PATTERN = re.compile(r"[a-z0-9][a-z0-9._/-]{2,}")
_STOP_TERMS = {
    "about",
    "after",
    "before",
    "comment",
    "company",
    "context",
    "created",
    "customer",
    "deal",
    "document",
    "draft",
    "drive",
    "from",
    "history",
    "internal",
    "mail",
    "message",
    "messages",
    "notes",
    "only",
    "plan",
    "proposal",
    "record",
    "review",
    "shared",
    "stage",
    "team",
    "thread",
    "update",
    "updated",
}


@dataclass(frozen=True)
class _ThreadProfile:
    thread: WhatIfThreadSummary
    first_timestamp_ms: int
    last_timestamp_ms: int
    actor_ids: frozenset[str]
    key_terms: frozenset[str]
    subject_terms: frozenset[str]
    explicit_case_id: str
    case_title: str
    case_anchor_terms: tuple[str, ...]


class _UnionFind:
    def __init__(self, thread_ids: Iterable[str]) -> None:
        self._parent = {thread_id: thread_id for thread_id in thread_ids}

    def find(self, thread_id: str) -> str:
        parent = self._parent[thread_id]
        if parent == thread_id:
            return parent
        resolved = self.find(parent)
        self._parent[thread_id] = resolved
        return resolved

    def union(self, left: str, right: str) -> None:
        left_root = self.find(left)
        right_root = self.find(right)
        if left_root == right_root:
            return
        winner, loser = sorted((left_root, right_root))
        self._parent[loser] = winner


def build_situation_graph(
    *,
    threads: Sequence[WhatIfThreadSummary],
    cases: Sequence[WhatIfCaseSummary],
    events: Sequence[WhatIfEvent],
    proximity_window_ms: int = _PROXIMITY_WINDOW_MS,
    docs_crm_window_ms: int = _DOCS_CRM_WINDOW_MS,
) -> WhatIfSituationGraph:
    profiles = _build_thread_profiles(
        threads=threads,
        cases=cases,
        events=events,
    )
    if len(profiles) < 2:
        return WhatIfSituationGraph()

    links = _build_situation_links(
        profiles=profiles,
        cases=cases,
        proximity_window_ms=proximity_window_ms,
        docs_crm_window_ms=docs_crm_window_ms,
    )
    if not links:
        return WhatIfSituationGraph()

    clusters = _build_situation_clusters(
        profiles=profiles,
        links=links,
    )
    return WhatIfSituationGraph(
        links=sorted(
            links,
            key=lambda item: (item.thread_id_a, item.thread_id_b, item.link_type),
        ),
        clusters=clusters,
    )


def recommend_branch_thread(world: WhatIfWorld) -> WhatIfThreadSummary:
    if not world.threads:
        raise ValueError("cannot recommend a branch thread from an empty world")

    cluster_by_thread = _cluster_by_thread_id(world.situation_graph)
    ranked_threads = sorted(
        world.threads,
        key=lambda thread: (
            -_cluster_surface_count(cluster_by_thread.get(thread.thread_id)),
            -_cluster_thread_count(cluster_by_thread.get(thread.thread_id)),
            -_cluster_actor_count(cluster_by_thread.get(thread.thread_id)),
            -_thread_signal_score(thread),
            -thread.event_count,
            thread.thread_id,
        ),
    )
    return ranked_threads[0]


def build_situation_context(
    world: WhatIfWorld,
    *,
    branch_thread_id: str,
    branch_timestamp_ms: int,
    limit: int = 6,
) -> WhatIfSituationContext | None:
    cluster = _cluster_by_thread_id(world.situation_graph).get(branch_thread_id)
    if cluster is None:
        return None

    thread_lookup = {thread.thread_id: thread for thread in world.threads}
    branch_thread = thread_lookup.get(branch_thread_id)
    if branch_thread is None:
        return None

    first_seen_by_thread: dict[str, int] = {}
    for event in world.events:
        first_seen_by_thread.setdefault(event.thread_id, event.timestamp_ms)

    visible_thread_ids = [
        thread_id
        for thread_id in cluster.thread_ids
        if thread_id != branch_thread_id
        and first_seen_by_thread.get(thread_id, branch_timestamp_ms + 1)
        <= branch_timestamp_ms
    ]
    if not visible_thread_ids:
        return None

    related_threads = [
        _situation_thread_reference(thread_lookup[thread_id])
        for thread_id in visible_thread_ids
        if thread_id in thread_lookup
    ]
    if not related_threads:
        return None

    related_history = [
        _event_reference(event)
        for event in world.events
        if event.thread_id in visible_thread_ids
        and event.timestamp_ms <= branch_timestamp_ms
    ]
    related_history = sorted(
        related_history,
        key=lambda item: (item.timestamp, item.event_id),
    )[-max(1, limit) :]

    visible_surfaces = sorted(
        {
            branch_thread.surface or "mail",
            *[thread.surface for thread in related_threads if thread.surface],
        }
    )
    visible_actor_ids = sorted(
        {
            *branch_thread.actor_ids,
            *[
                actor_id
                for thread in related_threads
                for actor_id in thread.actor_ids
                if actor_id
            ],
        }
    )
    return WhatIfSituationContext(
        situation_id=cluster.situation_id,
        label=cluster.label,
        surfaces=visible_surfaces,
        actor_ids=visible_actor_ids,
        anchor_terms=list(cluster.anchor_terms),
        related_threads=related_threads,
        related_history=related_history,
    )


def situation_context_prompt_lines(
    context: WhatIfSituationContext | None,
    *,
    max_threads: int = 3,
    max_history: int = 4,
) -> list[str]:
    if context is None:
        return []
    if not context.related_threads and not context.related_history:
        return []

    lines = ["Cross-surface situation context known by the branch date:"]
    if context.related_threads:
        lines.append("Related threads:")
        for thread in context.related_threads[: max(1, max_threads)]:
            lines.append(f"- [{thread.surface}] {thread.subject or thread.thread_id}")
    if context.related_history:
        lines.append("Recent linked activity:")
        for event in context.related_history[-max(1, max_history) :]:
            lines.append(
                f"- [{event.surface}] {event.timestamp}: {event.actor_id} -> {event.subject or event.thread_id}"
            )
    return lines


def _build_thread_profiles(
    *,
    threads: Sequence[WhatIfThreadSummary],
    cases: Sequence[WhatIfCaseSummary],
    events: Sequence[WhatIfEvent],
) -> dict[str, _ThreadProfile]:
    case_lookup = {case.case_id: case for case in cases}
    events_by_thread: dict[str, list[WhatIfEvent]] = defaultdict(list)
    for event in events:
        events_by_thread[event.thread_id].append(event)

    profiles: dict[str, _ThreadProfile] = {}
    for thread in threads:
        thread_events = events_by_thread.get(thread.thread_id, [])
        first_timestamp_ms = (
            min(event.timestamp_ms for event in thread_events) if thread_events else 0
        )
        last_timestamp_ms = (
            max(event.timestamp_ms for event in thread_events)
            if thread_events
            else first_timestamp_ms
        )
        case = case_lookup.get(thread.case_id)
        texts: list[str] = [thread.subject, thread.thread_id]
        texts.extend(
            value
            for event in thread_events
            for value in (
                event.subject,
                event.snippet,
                event.conversation_anchor,
            )
            if str(value or "").strip()
        )
        key_terms = frozenset(_key_terms_from_texts(texts))
        subject_terms = frozenset(_key_terms_from_texts([thread.subject], limit=8))
        explicit_case_id = (
            thread.case_id if str(thread.case_id or "").startswith("case:") else ""
        )
        case_anchor_terms = (
            tuple(case.anchor_tokens[:6])
            if case is not None and case.anchor_tokens
            else tuple()
        )
        profiles[thread.thread_id] = _ThreadProfile(
            thread=thread,
            first_timestamp_ms=first_timestamp_ms,
            last_timestamp_ms=last_timestamp_ms,
            actor_ids=frozenset(actor_id for actor_id in thread.actor_ids if actor_id),
            key_terms=key_terms,
            subject_terms=subject_terms,
            explicit_case_id=explicit_case_id,
            case_title=case.title if case is not None else "",
            case_anchor_terms=case_anchor_terms,
        )
    return profiles


def _build_situation_links(
    *,
    profiles: dict[str, _ThreadProfile],
    cases: Sequence[WhatIfCaseSummary],
    proximity_window_ms: int,
    docs_crm_window_ms: int,
) -> list[WhatIfSituationLink]:
    links: list[WhatIfSituationLink] = []
    seen_links: set[tuple[str, str, str]] = set()
    for left_id, right_id in _candidate_thread_pairs(profiles=profiles, cases=cases):
        left = profiles.get(left_id)
        right = profiles.get(right_id)
        if left is None or right is None:
            continue
        if left.thread.surface == right.thread.surface:
            continue

        shared_actor_ids = sorted(left.actor_ids & right.actor_ids)
        shared_terms = sorted(left.key_terms & right.key_terms)
        time_gap_ms = _time_gap_ms(left, right)
        strong_text_overlap = _has_strong_text_overlap(
            left,
            right,
            shared_terms=shared_terms,
        )

        if left.explicit_case_id and left.explicit_case_id == right.explicit_case_id:
            _append_link(
                links,
                seen_links,
                WhatIfSituationLink(
                    thread_id_a=left_id,
                    thread_id_b=right_id,
                    link_type="token",
                    shared_actor_ids=shared_actor_ids,
                    shared_terms=shared_terms[:6],
                    time_gap_ms=time_gap_ms,
                    weight=1.0,
                ),
            )
        if shared_actor_ids and time_gap_ms <= proximity_window_ms:
            _append_link(
                links,
                seen_links,
                WhatIfSituationLink(
                    thread_id_a=left_id,
                    thread_id_b=right_id,
                    link_type="actor_time",
                    shared_actor_ids=shared_actor_ids,
                    shared_terms=shared_terms[:6],
                    time_gap_ms=time_gap_ms,
                    weight=_link_weight(
                        base=0.6,
                        shared_actor_count=len(shared_actor_ids),
                        shared_term_count=len(shared_terms),
                        time_gap_ms=time_gap_ms,
                        window_ms=proximity_window_ms,
                    ),
                ),
            )
        if shared_actor_ids and strong_text_overlap:
            _append_link(
                links,
                seen_links,
                WhatIfSituationLink(
                    thread_id_a=left_id,
                    thread_id_b=right_id,
                    link_type="actor_text",
                    shared_actor_ids=shared_actor_ids,
                    shared_terms=shared_terms[:6],
                    time_gap_ms=time_gap_ms,
                    weight=_link_weight(
                        base=0.65,
                        shared_actor_count=len(shared_actor_ids),
                        shared_term_count=len(shared_terms),
                        time_gap_ms=time_gap_ms,
                        window_ms=proximity_window_ms,
                    ),
                ),
            )
        if (
            not shared_actor_ids
            and strong_text_overlap
            and time_gap_ms <= docs_crm_window_ms
            and _crm_or_docs_pair(left, right)
        ):
            _append_link(
                links,
                seen_links,
                WhatIfSituationLink(
                    thread_id_a=left_id,
                    thread_id_b=right_id,
                    link_type="text_time",
                    shared_terms=shared_terms[:6],
                    time_gap_ms=time_gap_ms,
                    weight=_link_weight(
                        base=0.55,
                        shared_actor_count=0,
                        shared_term_count=len(shared_terms),
                        time_gap_ms=time_gap_ms,
                        window_ms=docs_crm_window_ms,
                    ),
                ),
            )
    return links


def _candidate_thread_pairs(
    *,
    profiles: dict[str, _ThreadProfile],
    cases: Sequence[WhatIfCaseSummary],
) -> set[tuple[str, str]]:
    candidates: set[tuple[str, str]] = set()
    thread_ids = set(profiles)

    for case in cases:
        if not str(case.case_id or "").startswith("case:"):
            continue
        case_thread_ids = [
            thread_id for thread_id in case.thread_ids if thread_id in thread_ids
        ]
        _add_candidate_pairs(candidates, case_thread_ids)

    actor_buckets: dict[str, set[str]] = defaultdict(set)
    term_buckets: dict[str, set[str]] = defaultdict(set)
    for profile in profiles.values():
        for actor_id in profile.actor_ids:
            actor_buckets[actor_id].add(profile.thread.thread_id)
        for term in profile.key_terms:
            term_buckets[term].add(profile.thread.thread_id)

    for thread_ids_for_actor in actor_buckets.values():
        _add_candidate_pairs(candidates, thread_ids_for_actor)
    for thread_ids_for_term in term_buckets.values():
        if len(thread_ids_for_term) < 2 or len(thread_ids_for_term) > 12:
            continue
        _add_candidate_pairs(candidates, thread_ids_for_term)
    return candidates


def _add_candidate_pairs(
    candidates: set[tuple[str, str]],
    thread_ids: Iterable[str],
) -> None:
    normalized = sorted({thread_id for thread_id in thread_ids if thread_id})
    for left_id, right_id in combinations(normalized, 2):
        candidates.add((left_id, right_id))


def _append_link(
    links: list[WhatIfSituationLink],
    seen_links: set[tuple[str, str, str]],
    link: WhatIfSituationLink,
) -> None:
    key = (link.thread_id_a, link.thread_id_b, link.link_type)
    if key in seen_links:
        return
    seen_links.add(key)
    links.append(link)


def _build_situation_clusters(
    *,
    profiles: dict[str, _ThreadProfile],
    links: Sequence[WhatIfSituationLink],
) -> list[WhatIfSituationCluster]:
    union_find = _UnionFind(profiles)
    for link in links:
        union_find.union(link.thread_id_a, link.thread_id_b)

    components: dict[str, set[str]] = defaultdict(set)
    for thread_id in profiles:
        components[union_find.find(thread_id)].add(thread_id)

    candidate_clusters: list[tuple[tuple[int, int, str], WhatIfSituationCluster]] = []
    for component_thread_ids in components.values():
        if len(component_thread_ids) < 2:
            continue
        component_profiles = [profiles[thread_id] for thread_id in component_thread_ids]
        surfaces = sorted(
            {
                profile.thread.surface or "mail"
                for profile in component_profiles
                if profile.thread.surface
            }
        )
        if len(surfaces) < 2:
            continue
        component_links = [
            link
            for link in links
            if link.thread_id_a in component_thread_ids
            and link.thread_id_b in component_thread_ids
        ]
        actor_ids = sorted(
            {
                actor_id
                for profile in component_profiles
                for actor_id in profile.actor_ids
                if actor_id
            }
        )
        case_ids = sorted(
            {
                profile.explicit_case_id
                for profile in component_profiles
                if profile.explicit_case_id
            }
        )
        anchor_terms = _cluster_anchor_terms(component_profiles)
        label = _cluster_label(component_profiles)
        first_profile = min(
            component_profiles,
            key=lambda item: (item.first_timestamp_ms, item.thread.thread_id),
        )
        last_profile = max(
            component_profiles,
            key=lambda item: (item.last_timestamp_ms, item.thread.thread_id),
        )
        cluster = WhatIfSituationCluster(
            situation_id="",
            label=label,
            thread_ids=sorted(component_thread_ids),
            surfaces=surfaces,
            actor_ids=actor_ids,
            case_ids=case_ids,
            first_timestamp=first_profile.thread.first_timestamp,
            last_timestamp=last_profile.thread.last_timestamp,
            link_count=len(component_links),
            anchor_terms=anchor_terms,
        )
        candidate_clusters.append(
            (
                (-len(cluster.surfaces), -len(cluster.thread_ids), cluster.label),
                cluster,
            )
        )

    clusters: list[WhatIfSituationCluster] = []
    for index, (_, cluster) in enumerate(
        sorted(candidate_clusters, key=lambda item: item[0]), start=1
    ):
        clusters.append(
            cluster.model_copy(update={"situation_id": f"situation:{index:03d}"})
        )
    return clusters


def _cluster_anchor_terms(profiles: Sequence[_ThreadProfile]) -> list[str]:
    counts: Counter[str] = Counter()
    for profile in profiles:
        counts.update(profile.case_anchor_terms or profile.key_terms)
    return [term for term, _ in counts.most_common(6)]


def _cluster_label(profiles: Sequence[_ThreadProfile]) -> str:
    titled_profiles = [
        profile
        for profile in profiles
        if profile.explicit_case_id and profile.case_title
    ]
    if titled_profiles:
        chosen = sorted(
            titled_profiles,
            key=lambda item: (-item.thread.event_count, item.case_title),
        )[0]
        return chosen.case_title
    chosen = sorted(
        profiles,
        key=lambda item: (-item.thread.event_count, item.thread.thread_id),
    )[0]
    return chosen.thread.subject or chosen.thread.thread_id


def _cluster_by_thread_id(
    graph: WhatIfSituationGraph | None,
) -> dict[str, WhatIfSituationCluster]:
    if graph is None:
        return {}
    cluster_by_thread: dict[str, WhatIfSituationCluster] = {}
    for cluster in graph.clusters:
        for thread_id in cluster.thread_ids:
            cluster_by_thread[thread_id] = cluster
    return cluster_by_thread


def _cluster_surface_count(cluster: WhatIfSituationCluster | None) -> int:
    if cluster is None:
        return 0
    return len(cluster.surfaces)


def _cluster_thread_count(cluster: WhatIfSituationCluster | None) -> int:
    if cluster is None:
        return 0
    return len(cluster.thread_ids)


def _cluster_actor_count(cluster: WhatIfSituationCluster | None) -> int:
    if cluster is None:
        return 0
    return len(cluster.actor_ids)


def _thread_signal_score(thread: WhatIfThreadSummary) -> int:
    return (
        (thread.escalation_event_count * 4)
        + (thread.external_recipient_event_count * 3)
        + (thread.forward_event_count * 2)
        + (thread.assignment_event_count * 2)
        + thread.approval_event_count
        + thread.attachment_event_count
        + thread.legal_event_count
        + thread.trading_event_count
    )


def _time_gap_ms(left: _ThreadProfile, right: _ThreadProfile) -> int:
    if left.last_timestamp_ms <= right.first_timestamp_ms:
        return right.first_timestamp_ms - left.last_timestamp_ms
    if right.last_timestamp_ms <= left.first_timestamp_ms:
        return left.first_timestamp_ms - right.last_timestamp_ms
    return 0


def _crm_or_docs_pair(left: _ThreadProfile, right: _ThreadProfile) -> bool:
    surfaces = {left.thread.surface, right.thread.surface}
    return "crm" in surfaces or "docs" in surfaces


def _has_strong_text_overlap(
    left: _ThreadProfile,
    right: _ThreadProfile,
    *,
    shared_terms: Sequence[str],
) -> bool:
    if len(shared_terms) >= 2:
        smaller_term_count = min(len(left.key_terms), len(right.key_terms))
        if smaller_term_count <= 3:
            return True
        overlap_ratio = len(shared_terms) / max(1, smaller_term_count)
        if overlap_ratio >= 0.4:
            return True
    shared_subject_terms = left.subject_terms & right.subject_terms
    return any(len(term) >= 8 for term in shared_subject_terms)


def _link_weight(
    *,
    base: float,
    shared_actor_count: int,
    shared_term_count: int,
    time_gap_ms: int,
    window_ms: int,
) -> float:
    time_weight = 0.0
    if window_ms > 0:
        time_weight = max(0.0, 1.0 - (time_gap_ms / window_ms))
    score = (
        base
        + min(shared_actor_count, 3) * 0.08
        + min(shared_term_count, 4) * 0.05
        + time_weight * 0.12
    )
    return round(min(score, 0.99), 3)


def _key_terms_from_texts(
    texts: Iterable[str],
    *,
    limit: int = 12,
) -> list[str]:
    counts: Counter[str] = Counter()
    for text in texts:
        if not str(text or "").strip():
            continue
        counts.update(_normalized_terms(str(text)))
    return [term for term, _ in counts.most_common(limit)]


def _normalized_terms(text: str) -> list[str]:
    normalized_terms: list[str] = []
    for raw_term in _TERM_PATTERN.findall(text.lower()):
        term = raw_term.strip("._/-")
        if len(term) < 3:
            continue
        if term in _STOP_TERMS:
            continue
        if "@" in term:
            continue
        if term.isdigit():
            continue
        normalized_terms.append(term)
    return normalized_terms


def _situation_thread_reference(thread: WhatIfThreadSummary) -> WhatIfSituationThread:
    return WhatIfSituationThread(
        thread_id=thread.thread_id,
        subject=thread.subject,
        surface=thread.surface,
        case_id=thread.case_id,
        actor_ids=list(thread.actor_ids),
        first_timestamp=thread.first_timestamp,
        last_timestamp=thread.last_timestamp,
    )


def _event_reference(event: WhatIfEvent) -> WhatIfEventReference:
    return WhatIfEventReference(
        event_id=event.event_id,
        timestamp=event.timestamp,
        actor_id=event.actor_id,
        target_id=event.target_id,
        event_type=event.event_type,
        thread_id=event.thread_id,
        case_id=event.case_id,
        surface=event.surface,
        conversation_anchor=event.conversation_anchor,
        subject=event.subject,
        snippet=event.snippet,
        to_recipients=list(event.flags.to_recipients),
        cc_recipients=list(event.flags.cc_recipients),
        has_attachment_reference=event.flags.has_attachment_reference,
        is_forward=event.flags.is_forward,
        is_reply=event.flags.is_reply,
        is_escalation=event.flags.is_escalation,
    )


__all__ = [
    "build_situation_context",
    "build_situation_graph",
    "recommend_branch_thread",
    "situation_context_prompt_lines",
]
