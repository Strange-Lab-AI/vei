from __future__ import annotations

from typing import Any, Dict, List, Optional

from vei.world import Scenario, Ticket


class TicketsSim:
    """Deterministic ticketing twin with lifecycle and pagination semantics."""

    _DEFAULT_LIMIT = 25
    _MAX_LIMIT = 200
    _VALID_PRIORITIES = {"P1", "P2", "P3", "P4"}
    _VALID_STATUSES = {"open", "in_progress", "blocked", "resolved", "closed"}
    _ALLOWED_TRANSITIONS = {
        "open": {"in_progress", "blocked", "resolved", "closed"},
        "in_progress": {"blocked", "resolved", "closed"},
        "blocked": {"open", "in_progress", "resolved", "closed"},
        "resolved": {"closed", "open", "in_progress"},
        "closed": {"open"},
    }

    def __init__(self, scenario: Optional[Scenario] = None):
        base = dict(scenario.tickets) if scenario and scenario.tickets else {}
        self.tickets: Dict[str, Ticket] = base
        self.metadata: Dict[str, Dict[str, Any]] = {}
        self._clock_ms = 1_700_000_200_000
        for idx, ticket in enumerate(self.tickets.values(), start=1):
            created_ms = self._clock_ms + idx
            self.metadata[ticket.ticket_id] = {
                "priority": "P3",
                "severity": "medium",
                "labels": [],
                "comments": [],
                "created_ms": created_ms,
                "updated_ms": created_ms,
            }
        self._ticket_seq = self._init_seq()

    def list(
        self,
        *,
        status: Optional[str] = None,
        assignee: Optional[str] = None,
        priority: Optional[str] = None,
        query: Optional[str] = None,
        limit: Optional[int] = None,
        cursor: Optional[str] = None,
        sort_by: str = "updated_ms",
        sort_dir: str = "desc",
    ) -> List[Dict[str, object]] | Dict[str, object]:
        rows = [self._ticket_payload(ticket) for ticket in self.tickets.values()]
        if status:
            wanted_status = status.strip().lower()
            rows = [
                row
                for row in rows
                if str(row.get("status", "")).lower() == wanted_status
            ]
        if assignee:
            wanted_assignee = assignee.strip().lower()
            rows = [
                row
                for row in rows
                if str(row.get("assignee", "")).strip().lower() == wanted_assignee
            ]
        if priority:
            wanted_priority = priority.strip().upper()
            rows = [
                row
                for row in rows
                if str(row.get("priority", "")).strip().upper() == wanted_priority
            ]
        needle = (query or "").strip().lower()
        if needle:
            rows = [
                row
                for row in rows
                if needle in str(row.get("title", "")).lower()
                or needle in str(row.get("description", "")).lower()
            ]

        sort_field = (
            sort_by
            if sort_by in {"updated_ms", "created_ms", "priority", "status", "title"}
            else "updated_ms"
        )
        reverse = sort_dir.lower() != "asc"
        rows.sort(key=lambda row: _sortable(row.get(sort_field)), reverse=reverse)

        is_legacy = (
            status is None
            and assignee is None
            and priority is None
            and query is None
            and limit is None
            and cursor is None
            and sort_by == "updated_ms"
            and sort_dir == "desc"
        )
        if is_legacy:
            return rows

        page_limit = _normalize_limit(
            limit, default=self._DEFAULT_LIMIT, max_limit=self._MAX_LIMIT
        )
        start = _decode_cursor(cursor)
        sliced = rows[start : start + page_limit]
        next_cursor = (
            _encode_cursor(start + page_limit)
            if (start + page_limit) < len(rows)
            else None
        )
        return {
            "tickets": sliced,
            "count": len(sliced),
            "total": len(rows),
            "next_cursor": next_cursor,
            "has_more": next_cursor is not None,
        }

    def get(self, ticket_id: str) -> Dict[str, object]:
        ticket = self.tickets.get(ticket_id)
        if not ticket:
            raise ValueError(f"unknown ticket: {ticket_id}")
        return self._ticket_payload(ticket)

    def create(
        self,
        title: str,
        description: Optional[str] = None,
        assignee: Optional[str] = None,
        priority: str = "P3",
        severity: str = "medium",
        labels: Optional[List[str]] = None,
    ) -> Dict[str, object]:
        ticket_id = f"TCK-{self._ticket_seq}"
        self._ticket_seq += 1
        ticket = Ticket(
            ticket_id=ticket_id,
            title=title,
            status="open",
            assignee=assignee,
            description=description,
            history=[{"status": "open"}],
        )
        self.tickets[ticket_id] = ticket
        normalized_priority = priority.strip().upper() if priority else "P3"
        if normalized_priority not in self._VALID_PRIORITIES:
            raise ValueError(f"invalid ticket priority: {priority}")
        now_ms = self._now_ms()
        self.metadata[ticket_id] = {
            "priority": normalized_priority,
            "severity": severity or "medium",
            "labels": list(labels or []),
            "comments": [],
            "created_ms": now_ms,
            "updated_ms": now_ms,
        }
        return {
            "ticket_id": ticket_id,
            "status": "open",
            "priority": normalized_priority,
        }

    def update(
        self,
        ticket_id: str,
        description: Optional[str] = None,
        assignee: Optional[str] = None,
        priority: Optional[str] = None,
        severity: Optional[str] = None,
        labels: Optional[List[str]] = None,
    ) -> Dict[str, object]:
        ticket = self.tickets.get(ticket_id)
        if not ticket:
            raise ValueError(f"unknown ticket: {ticket_id}")
        meta = self._meta(ticket_id)
        changed = False
        if description is not None:
            ticket.description = description
            changed = True
        if assignee is not None:
            ticket.assignee = assignee
            changed = True
        if priority is not None:
            normalized_priority = priority.strip().upper()
            if normalized_priority not in self._VALID_PRIORITIES:
                raise ValueError(f"invalid ticket priority: {priority}")
            if str(meta.get("priority")) != normalized_priority:
                meta["priority"] = normalized_priority
                changed = True
        if severity is not None:
            if str(meta.get("severity")) != severity:
                meta["severity"] = severity
                changed = True
        if labels is not None:
            meta["labels"] = list(labels)
            changed = True
        ticket.history = list(ticket.history or []) + [
            {"status": ticket.status, "update": "fields"}
        ]
        if changed:
            meta["updated_ms"] = self._now_ms()
        self.tickets[ticket_id] = ticket
        return {
            "ticket_id": ticket_id,
            "status": ticket.status,
            "priority": str(meta.get("priority", "P3")),
        }

    def transition(self, ticket_id: str, status: str) -> Dict[str, object]:
        ticket = self.tickets.get(ticket_id)
        if not ticket:
            raise ValueError(f"unknown ticket: {ticket_id}")
        target = status.strip().lower()
        if target not in self._VALID_STATUSES:
            raise ValueError(f"invalid ticket status: {status}")
        current = ticket.status.strip().lower()
        if target != current and target not in self._ALLOWED_TRANSITIONS.get(
            current, set()
        ):
            raise ValueError(f"invalid transition {current} -> {target}")
        ticket.status = target
        ticket.history = list(ticket.history or []) + [{"status": target}]
        self._meta(ticket_id)["updated_ms"] = self._now_ms()
        self.tickets[ticket_id] = ticket
        return {"ticket_id": ticket_id, "status": target}

    def add_comment(
        self, ticket_id: str, body: str, author: str = "agent"
    ) -> Dict[str, object]:
        ticket = self.tickets.get(ticket_id)
        if not ticket:
            raise ValueError(f"unknown ticket: {ticket_id}")
        if not body.strip():
            raise ValueError("ticket comment body cannot be empty")
        meta = self._meta(ticket_id)
        comments = list(meta.get("comments", []))
        comment_id = f"CMT-{len(comments) + 1:04d}"
        comment = {
            "comment_id": comment_id,
            "author": author,
            "body": body,
            "created_ms": self._now_ms(),
        }
        comments.append(comment)
        meta["comments"] = comments
        meta["updated_ms"] = int(comment["created_ms"])
        ticket.history = list(ticket.history or []) + [
            {"status": ticket.status, "comment": comment_id}
        ]
        self.tickets[ticket_id] = ticket
        return {"ticket_id": ticket_id, "comment_id": comment_id, "author": author}

    def _ticket_payload(self, ticket: Ticket) -> Dict[str, object]:
        meta = self._meta(ticket.ticket_id)
        return {
            "ticket_id": ticket.ticket_id,
            "title": ticket.title,
            "status": ticket.status,
            "assignee": ticket.assignee,
            "description": ticket.description,
            "history": list(ticket.history or []),
            "priority": str(meta.get("priority", "P3")),
            "severity": str(meta.get("severity", "medium")),
            "labels": list(meta.get("labels", [])),
            "comment_count": len(list(meta.get("comments", []))),
            "created_ms": int(meta.get("created_ms", 0)),
            "updated_ms": int(meta.get("updated_ms", 0)),
        }

    def deliver(self, event: Dict[str, object]) -> Dict[str, object]:
        """Apply a scheduled ticket event using tickets tool semantics."""
        payload = dict(event or {})
        ticket_id = payload.get("ticket_id")
        if isinstance(ticket_id, str) and ticket_id in self.tickets:
            if isinstance(payload.get("status"), str):
                return self.transition(ticket_id=ticket_id, status=payload["status"])
            if isinstance(payload.get("comment"), str):
                return self.add_comment(
                    ticket_id=ticket_id,
                    body=payload["comment"],
                    author=(
                        payload.get("author")
                        if isinstance(payload.get("author"), str)
                        else "agent"
                    ),
                )
            return self.update(
                ticket_id=ticket_id,
                description=(
                    payload.get("description")
                    if isinstance(payload.get("description"), str)
                    else None
                ),
                assignee=(
                    payload.get("assignee")
                    if isinstance(payload.get("assignee"), str)
                    else None
                ),
                priority=(
                    payload.get("priority")
                    if isinstance(payload.get("priority"), str)
                    else None
                ),
                severity=(
                    payload.get("severity")
                    if isinstance(payload.get("severity"), str)
                    else None
                ),
                labels=(
                    payload.get("labels")
                    if isinstance(payload.get("labels"), list)
                    else None
                ),
            )

        title = payload.get("title")
        if not isinstance(title, str):
            raise ValueError("tickets delivery requires title for create")
        return self.create(
            title=title,
            description=(
                payload.get("description")
                if isinstance(payload.get("description"), str)
                else None
            ),
            assignee=(
                payload.get("assignee")
                if isinstance(payload.get("assignee"), str)
                else None
            ),
            priority=(
                payload.get("priority")
                if isinstance(payload.get("priority"), str)
                else "P3"
            ),
            severity=(
                payload.get("severity")
                if isinstance(payload.get("severity"), str)
                else "medium"
            ),
            labels=(
                payload.get("labels")
                if isinstance(payload.get("labels"), list)
                else None
            ),
        )

    def _init_seq(self) -> int:
        seq = 1
        for ticket_id in self.tickets.keys():
            try:
                if ticket_id.startswith("TCK-"):
                    seq = max(seq, int(ticket_id.split("-", 1)[1]) + 1)
            except ValueError:
                continue
        return seq

    def _meta(self, ticket_id: str) -> Dict[str, Any]:
        return self.metadata.setdefault(
            ticket_id,
            {
                "priority": "P3",
                "severity": "medium",
                "labels": [],
                "comments": [],
                "created_ms": self._now_ms(),
                "updated_ms": self._now_ms(),
            },
        )

    def _now_ms(self) -> int:
        self._clock_ms += 1
        return self._clock_ms


def _normalize_limit(limit: Optional[int], *, default: int, max_limit: int) -> int:
    if limit is None:
        return default
    if limit < 1:
        return 1
    return min(max_limit, int(limit))


def _decode_cursor(cursor: Optional[str]) -> int:
    if not cursor:
        return 0
    if not cursor.startswith("ofs:"):
        raise ValueError("invalid cursor")
    try:
        value = int(cursor.split(":", 1)[1])
    except ValueError as exc:
        raise ValueError("invalid cursor") from exc
    return max(0, value)


def _encode_cursor(offset: int) -> str:
    return f"ofs:{max(0, int(offset))}"


def _sortable(value: object) -> object:
    if value is None:
        return ""
    if isinstance(value, (int, float, str)):
        return value
    return str(value)
