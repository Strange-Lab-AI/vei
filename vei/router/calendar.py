from __future__ import annotations

from typing import Any, Dict, List, Optional

from vei.world import CalendarEvent, Scenario

from ._pagination import normalize_limit, decode_cursor, encode_cursor


class CalendarSim:
    """Synthetic calendar twin supporting deterministic enterprise interactions."""

    _DEFAULT_LIMIT = 25
    _MAX_LIMIT = 200
    _VALID_STATUSES = {"CONFIRMED", "TENTATIVE", "CANCELED"}

    def __init__(self, scenario: Optional[Scenario] = None):
        self.events: Dict[str, CalendarEvent] = {}
        self.responses: Dict[str, Dict[str, str]] = {}
        self.metadata: Dict[str, Dict[str, Any]] = {}
        self._clock_ms = 1_700_000_100_000
        if scenario and scenario.calendar_events:
            for idx, event in enumerate(scenario.calendar_events, start=1):
                self.events[event.event_id] = event
                self.responses[event.event_id] = {}
                created_ms = self._clock_ms + idx
                self.metadata[event.event_id] = {
                    "status": "CONFIRMED",
                    "organizer": "system",
                    "version": 1,
                    "created_ms": created_ms,
                    "updated_ms": created_ms,
                    "cancel_reason": None,
                }
        self._event_seq = self._init_seq()

    def list_events(
        self,
        *,
        attendee: Optional[str] = None,
        status: Optional[str] = None,
        starts_after_ms: Optional[int] = None,
        ends_before_ms: Optional[int] = None,
        limit: Optional[int] = None,
        cursor: Optional[str] = None,
        sort_dir: str = "asc",
    ) -> List[Dict[str, object]] | Dict[str, object]:
        rows = [self._event_payload(evt) for evt in self.events.values()]
        if attendee:
            wanted = attendee.strip().lower()
            rows = [
                row
                for row in rows
                if any(str(a).lower() == wanted for a in row.get("attendees", []))
            ]
        if status:
            wanted_status = status.strip().upper()
            rows = [
                row
                for row in rows
                if str(row.get("status", "")).upper() == wanted_status
            ]
        if starts_after_ms is not None:
            rows = [
                row
                for row in rows
                if int(row.get("start_ms", 0)) >= int(starts_after_ms)
            ]
        if ends_before_ms is not None:
            rows = [
                row for row in rows if int(row.get("end_ms", 0)) <= int(ends_before_ms)
            ]

        reverse = sort_dir.lower() == "desc"
        rows.sort(key=lambda row: int(row.get("start_ms", 0)), reverse=reverse)

        is_legacy = (
            attendee is None
            and status is None
            and starts_after_ms is None
            and ends_before_ms is None
            and limit is None
            and cursor is None
            and sort_dir == "asc"
        )
        if is_legacy:
            return rows

        page_limit = normalize_limit(
            limit, default=self._DEFAULT_LIMIT, max_limit=self._MAX_LIMIT
        )
        start = decode_cursor(cursor)
        sliced = rows[start : start + page_limit]
        next_cursor = (
            encode_cursor(start + page_limit)
            if (start + page_limit) < len(rows)
            else None
        )
        return {
            "events": sliced,
            "count": len(sliced),
            "total": len(rows),
            "next_cursor": next_cursor,
            "has_more": next_cursor is not None,
        }

    def create_event(
        self,
        title: str,
        start_ms: int,
        end_ms: int,
        attendees: Optional[List[str]] = None,
        location: Optional[str] = None,
        description: Optional[str] = None,
        organizer: Optional[str] = None,
        status: str = "CONFIRMED",
    ) -> Dict[str, object]:
        event_id = f"EVT-{self._event_seq}"
        self._event_seq += 1
        normalized_status = status.strip().upper() if status else "CONFIRMED"
        if normalized_status not in self._VALID_STATUSES:
            raise ValueError(f"invalid event status: {status}")
        evt = CalendarEvent(
            event_id=event_id,
            title=title,
            start_ms=int(start_ms),
            end_ms=int(end_ms),
            attendees=attendees or None,
            location=location,
            description=description,
        )
        self.events[event_id] = evt
        self.responses[event_id] = {}
        now_ms = self._now_ms()
        self.metadata[event_id] = {
            "status": normalized_status,
            "organizer": organizer or "agent",
            "version": 1,
            "created_ms": now_ms,
            "updated_ms": now_ms,
            "cancel_reason": None,
        }
        return {"event_id": event_id, "status": normalized_status}

    def update_event(
        self,
        event_id: str,
        title: Optional[str] = None,
        start_ms: Optional[int] = None,
        end_ms: Optional[int] = None,
        attendees: Optional[List[str]] = None,
        location: Optional[str] = None,
        description: Optional[str] = None,
        status: Optional[str] = None,
    ) -> Dict[str, object]:
        evt = self.events.get(event_id)
        if not evt:
            raise ValueError(f"unknown event: {event_id}")
        meta = self._meta(event_id)
        if str(meta.get("status")) == "CANCELED":
            raise ValueError(f"cannot update canceled event: {event_id}")
        changed = False
        if title is not None:
            evt.title = title
            changed = True
        if start_ms is not None:
            evt.start_ms = int(start_ms)
            changed = True
        if end_ms is not None:
            evt.end_ms = int(end_ms)
            changed = True
        if attendees is not None:
            evt.attendees = attendees
            changed = True
        if location is not None:
            evt.location = location
            changed = True
        if description is not None:
            evt.description = description
            changed = True
        if status is not None:
            normalized_status = status.strip().upper()
            if normalized_status not in self._VALID_STATUSES:
                raise ValueError(f"invalid event status: {status}")
            if str(meta.get("status")) != normalized_status:
                meta["status"] = normalized_status
                changed = True
        if changed:
            meta["version"] = int(meta.get("version", 1)) + 1
            meta["updated_ms"] = self._now_ms()
        return self._event_payload(evt)

    def cancel_event(
        self, event_id: str, reason: Optional[str] = None
    ) -> Dict[str, object]:
        evt = self.events.get(event_id)
        if not evt:
            raise ValueError(f"unknown event: {event_id}")
        meta = self._meta(event_id)
        if str(meta.get("status")) == "CANCELED":
            return {"event_id": event_id, "status": "CANCELED", "changed": False}
        meta["status"] = "CANCELED"
        meta["cancel_reason"] = reason or "manual_cancel"
        meta["version"] = int(meta.get("version", 1)) + 1
        meta["updated_ms"] = self._now_ms()
        return {"event_id": event_id, "status": "CANCELED", "changed": True}

    def accept(self, event_id: str, attendee: str) -> Dict[str, object]:
        return self._respond(event_id, attendee, "accepted")

    def decline(self, event_id: str, attendee: str) -> Dict[str, object]:
        return self._respond(event_id, attendee, "declined")

    def _respond(self, event_id: str, attendee: str, status: str) -> Dict[str, object]:
        evt = self.events.get(event_id)
        if not evt:
            raise ValueError(f"unknown event: {event_id}")
        meta = self._meta(event_id)
        if str(meta.get("status")) == "CANCELED":
            raise ValueError(f"cannot respond to canceled event: {event_id}")
        if attendee and evt.attendees and attendee not in evt.attendees:
            raise ValueError(f"attendee {attendee} not on event {event_id}")
        self.responses.setdefault(event_id, {})[attendee] = status
        return {"event_id": event_id, "attendee": attendee, "status": status}

    def _event_payload(self, evt: CalendarEvent) -> Dict[str, object]:
        meta = self._meta(evt.event_id)
        return {
            "event_id": evt.event_id,
            "title": evt.title,
            "start_ms": evt.start_ms,
            "end_ms": evt.end_ms,
            "attendees": list(evt.attendees or []),
            "location": evt.location,
            "description": evt.description,
            "status": str(meta.get("status", "CONFIRMED")),
            "organizer": str(meta.get("organizer", "system")),
            "version": int(meta.get("version", 1)),
            "created_ms": int(meta.get("created_ms", 0)),
            "updated_ms": int(meta.get("updated_ms", 0)),
            "cancel_reason": meta.get("cancel_reason"),
            "responses": dict(self.responses.get(evt.event_id, {})),
        }

    def deliver(self, event: Dict[str, object]) -> Dict[str, object]:
        """Apply a scheduled calendar event as an incoming invite."""
        payload = dict(event or {})
        op = str(payload.get("op", "create")).lower()
        if op == "update":
            event_id = payload.get("event_id")
            if not isinstance(event_id, str):
                raise ValueError("calendar update delivery requires event_id")
            return self.update_event(
                event_id=event_id,
                title=(
                    payload.get("title")
                    if isinstance(payload.get("title"), str)
                    else None
                ),
                start_ms=(
                    int(payload["start_ms"])
                    if isinstance(payload.get("start_ms"), int)
                    else None
                ),
                end_ms=(
                    int(payload["end_ms"])
                    if isinstance(payload.get("end_ms"), int)
                    else None
                ),
                attendees=(
                    payload.get("attendees")
                    if isinstance(payload.get("attendees"), list)
                    else None
                ),
                location=(
                    payload.get("location")
                    if isinstance(payload.get("location"), str)
                    else None
                ),
                description=(
                    payload.get("description")
                    if isinstance(payload.get("description"), str)
                    else None
                ),
                status=(
                    payload.get("status")
                    if isinstance(payload.get("status"), str)
                    else None
                ),
            )
        if op == "cancel":
            event_id = payload.get("event_id")
            if not isinstance(event_id, str):
                raise ValueError("calendar cancel delivery requires event_id")
            return self.cancel_event(
                event_id=event_id,
                reason=(
                    payload.get("reason")
                    if isinstance(payload.get("reason"), str)
                    else None
                ),
            )

        title = payload.get("title")
        start_ms = payload.get("start_ms")
        end_ms = payload.get("end_ms")
        if not isinstance(title, str):
            raise ValueError("calendar delivery requires title")
        if not isinstance(start_ms, int) or not isinstance(end_ms, int):
            raise ValueError("calendar delivery requires integer start_ms/end_ms")
        attendees = (
            payload.get("attendees")
            if isinstance(payload.get("attendees"), list)
            else None
        )
        location = (
            payload.get("location")
            if isinstance(payload.get("location"), str)
            else None
        )
        description = (
            payload.get("description")
            if isinstance(payload.get("description"), str)
            else None
        )
        return self.create_event(
            title=title,
            start_ms=start_ms,
            end_ms=end_ms,
            attendees=attendees,
            location=location,
            description=description,
            organizer=(
                payload.get("organizer")
                if isinstance(payload.get("organizer"), str)
                else None
            ),
            status=(
                payload.get("status")
                if isinstance(payload.get("status"), str)
                else "CONFIRMED"
            ),
        )

    def _init_seq(self) -> int:
        seq = 1
        for event_id in self.events.keys():
            try:
                if event_id.startswith("EVT-"):
                    seq = max(seq, int(event_id.split("-", 1)[1]) + 1)
            except ValueError:
                continue
        return seq

    def _meta(self, event_id: str) -> Dict[str, Any]:
        return self.metadata.setdefault(
            event_id,
            {
                "status": "CONFIRMED",
                "organizer": "system",
                "version": 1,
                "created_ms": self._now_ms(),
                "updated_ms": self._now_ms(),
                "cancel_reason": None,
            },
        )

    def _now_ms(self) -> int:
        self._clock_ms += 1
        return self._clock_ms
