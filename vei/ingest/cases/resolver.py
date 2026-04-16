"""CaseResolver — cross-surface case detection and linking.

One class, one rule set, one output shape.  Links mail threads, chat threads,
tickets, docs, CRM records via participant overlap, object_ref graphs, and
time windows.

Runs during normalization, not inside session materialization.
"""

from __future__ import annotations

import uuid
from collections import defaultdict
from typing import Dict, List, Optional, Set

from vei.events.models import CanonicalEvent
from vei.ingest.api import CaseAssignment


class DefaultCaseResolver:
    """Links events into cases by participant overlap and object refs."""

    def __init__(self, time_window_ms: int = 86_400_000) -> None:
        self._time_window_ms = time_window_ms
        self._cases: Dict[str, CaseAssignment] = {}
        self._object_to_case: Dict[str, str] = {}
        self._participant_to_cases: Dict[str, Set[str]] = defaultdict(set)

    def resolve(self, events: List[CanonicalEvent]) -> List[CaseAssignment]:
        new_assignments: List[CaseAssignment] = []

        for event in events:
            matched_case_id = self._find_case(event)

            if matched_case_id is not None:
                event.case_id = matched_case_id
                case = self._cases[matched_case_id]
                case.event_ids.append(event.event_id)
                case.end_ts = max(case.end_ts, event.ts_ms)
                surface = event.domain.value
                if surface not in case.surfaces:
                    case.surfaces.append(surface)
                for p in self._event_participants(event):
                    if p not in case.participants:
                        case.participants.append(p)
                    self._participant_to_cases[p].add(matched_case_id)
                for obj_ref in event.object_refs:
                    ref_key = f"{obj_ref.domain}:{obj_ref.object_id}"
                    if ref_key not in case.linked_object_refs:
                        case.linked_object_refs.append(ref_key)
                    self._object_to_case[ref_key] = matched_case_id
            else:
                case_id = f"case-{uuid.uuid4().hex[:12]}"
                event.case_id = case_id
                participants = self._event_participants(event)
                surface = event.domain.value
                obj_refs = [f"{o.domain}:{o.object_id}" for o in event.object_refs]
                case = CaseAssignment(
                    case_id=case_id,
                    event_ids=[event.event_id],
                    participants=participants,
                    linked_object_refs=obj_refs,
                    surfaces=[surface],
                    start_ts=event.ts_ms,
                    end_ts=event.ts_ms,
                )
                self._cases[case_id] = case
                new_assignments.append(case)
                for p in participants:
                    self._participant_to_cases[p].add(case_id)
                for ref_key in obj_refs:
                    self._object_to_case[ref_key] = case_id

        return new_assignments

    def _find_case(self, event: CanonicalEvent) -> Optional[str]:
        for obj_ref in event.object_refs:
            ref_key = f"{obj_ref.domain}:{obj_ref.object_id}"
            if ref_key in self._object_to_case:
                return self._object_to_case[ref_key]
        if event.case_id and event.case_id in self._cases:
            return event.case_id
        participants = self._event_participants(event)
        for p in participants:
            candidate_cases = self._participant_to_cases.get(p, set())
            for case_id in candidate_cases:
                case = self._cases[case_id]
                if abs(event.ts_ms - case.end_ts) <= self._time_window_ms:
                    return case_id
        return None

    @staticmethod
    def _event_participants(event: CanonicalEvent) -> List[str]:
        participants: List[str] = []
        if event.actor_ref and event.actor_ref.actor_id:
            participants.append(event.actor_ref.actor_id)
        for p in event.participants:
            if p.actor_id and p.actor_id not in participants:
                participants.append(p.actor_id)
        return participants
