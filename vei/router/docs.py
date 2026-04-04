from __future__ import annotations

from typing import Any, Dict, List, Optional

from vei.world import Document, Scenario

from ._pagination import normalize_limit, decode_cursor, encode_cursor, sortable


class DocsSim:
    """Deterministic docs twin with enterprise-style metadata and pagination."""

    _DEFAULT_LIMIT = 25
    _MAX_LIMIT = 200
    _VALID_STATUSES = {"DRAFT", "ACTIVE", "ARCHIVED"}

    def __init__(self, scenario: Optional[Scenario] = None):
        base = dict(scenario.documents) if scenario and scenario.documents else {}
        self.docs: Dict[str, Document] = base
        self.metadata: Dict[str, Dict[str, Any]] = {}
        self._clock_ms = 1_700_000_000_000
        for idx, doc_id in enumerate(sorted(self.docs.keys()), start=1):
            created_ms = self._clock_ms + idx
            self.metadata[doc_id] = {
                "owner": "system",
                "status": "ACTIVE",
                "version": 1,
                "created_ms": created_ms,
                "updated_ms": created_ms,
            }
        self._doc_seq = self._init_seq()

    def list(
        self,
        *,
        query: Optional[str] = None,
        tag: Optional[str] = None,
        status: Optional[str] = None,
        owner: Optional[str] = None,
        sort_by: str = "updated_ms",
        sort_dir: str = "desc",
        limit: Optional[int] = None,
        cursor: Optional[str] = None,
        include_body: bool = False,
    ) -> List[Dict[str, object]] | Dict[str, object]:
        rows = [
            self._doc_payload(doc, include_body=include_body)
            for doc in self.docs.values()
        ]

        needle = (query or "").strip().lower()
        if needle:
            rows = [
                row
                for row in rows
                if needle in str(row.get("title", "")).lower()
                or needle in str(row.get("body", "")).lower()
            ]
        if tag:
            wanted = tag.strip().lower()
            rows = [
                row
                for row in rows
                if any(str(value).lower() == wanted for value in row.get("tags", []))
            ]
        if status:
            wanted_status = status.strip().upper()
            rows = [
                row
                for row in rows
                if str(row.get("status", "")).upper() == wanted_status
            ]
        if owner:
            wanted_owner = owner.strip().lower()
            rows = [
                row for row in rows if str(row.get("owner", "")).lower() == wanted_owner
            ]

        sort_field = (
            sort_by
            if sort_by in {"title", "created_ms", "updated_ms"}
            else "updated_ms"
        )
        reverse = sort_dir.lower() != "asc"
        rows.sort(
            key=lambda row: sortable(row.get(sort_field)),
            reverse=reverse,
        )

        is_legacy = (
            query is None
            and tag is None
            and status is None
            and owner is None
            and limit is None
            and cursor is None
            and sort_by == "updated_ms"
            and sort_dir == "desc"
            and not include_body
        )
        if is_legacy:
            return [
                {
                    "doc_id": str(row["doc_id"]),
                    "title": str(row["title"]),
                    "tags": list(row.get("tags", [])),
                }
                for row in rows
            ]

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
            "documents": sliced,
            "count": len(sliced),
            "total": len(rows),
            "next_cursor": next_cursor,
            "has_more": next_cursor is not None,
        }

    def read(self, doc_id: str) -> Dict[str, object]:
        doc = self.docs.get(doc_id)
        if not doc:
            raise ValueError(f"unknown document: {doc_id}")
        return self._doc_payload(doc, include_body=True)

    def create(
        self,
        title: str,
        body: str,
        tags: Optional[List[str]] = None,
        owner: Optional[str] = None,
        status: str = "DRAFT",
    ) -> Dict[str, object]:
        doc_id = f"DOC-{self._doc_seq}"
        self._doc_seq += 1
        doc = Document(doc_id=doc_id, title=title, body=body, tags=tags or None)
        self.docs[doc_id] = doc
        normalized_status = status.strip().upper() if status else "DRAFT"
        if normalized_status not in self._VALID_STATUSES:
            raise ValueError(f"invalid docs status: {status}")
        now_ms = self._now_ms()
        self.metadata[doc_id] = {
            "owner": owner or "agent",
            "status": normalized_status,
            "version": 1,
            "created_ms": now_ms,
            "updated_ms": now_ms,
        }
        return {
            "doc_id": doc_id,
            "title": title,
            "status": normalized_status,
            "version": 1,
        }

    def update(
        self,
        doc_id: str,
        title: Optional[str] = None,
        body: Optional[str] = None,
        tags: Optional[List[str]] = None,
        status: Optional[str] = None,
    ) -> Dict[str, object]:
        doc = self.docs.get(doc_id)
        if not doc:
            raise ValueError(f"unknown document: {doc_id}")
        changed = False
        if title is not None:
            doc.title = title
            changed = True
        if body is not None:
            doc.body = body
            changed = True
        if tags is not None:
            doc.tags = tags or None
            changed = True
        meta = self.metadata.setdefault(
            doc_id,
            {
                "owner": "system",
                "status": "ACTIVE",
                "version": 1,
                "created_ms": self._now_ms(),
                "updated_ms": self._now_ms(),
            },
        )
        if status is not None:
            normalized_status = status.strip().upper()
            if normalized_status not in self._VALID_STATUSES:
                raise ValueError(f"invalid docs status: {status}")
            if str(meta.get("status")) != normalized_status:
                meta["status"] = normalized_status
                changed = True
        if changed:
            meta["version"] = int(meta.get("version", 1)) + 1
            meta["updated_ms"] = self._now_ms()
        self.docs[doc_id] = doc
        return {
            "doc_id": doc_id,
            "title": doc.title,
            "status": str(meta.get("status", "ACTIVE")),
            "version": int(meta.get("version", 1)),
        }

    def search(
        self, query: str, limit: int = 20, cursor: Optional[str] = None
    ) -> List[Dict[str, object]] | Dict[str, object]:
        needle = query.lower().strip()
        hits = []
        if not needle:
            return hits
        for doc in self.docs.values():
            if needle in doc.title.lower() or needle in doc.body.lower():
                payload = self._doc_payload(doc, include_body=False)
                hits.append(
                    {
                        "doc_id": payload["doc_id"],
                        "title": payload["title"],
                        "status": payload["status"],
                        "tags": payload["tags"],
                    }
                )
        hits.sort(key=lambda row: sortable(row.get("title")))

        legacy = limit == 20 and cursor is None
        if legacy:
            return hits[:limit]

        page_limit = normalize_limit(limit, default=20, max_limit=self._MAX_LIMIT)
        start = decode_cursor(cursor)
        sliced = hits[start : start + page_limit]
        next_cursor = (
            encode_cursor(start + page_limit)
            if (start + page_limit) < len(hits)
            else None
        )
        return {
            "documents": sliced,
            "count": len(sliced),
            "total": len(hits),
            "next_cursor": next_cursor,
            "has_more": next_cursor is not None,
        }

    def deliver(self, event: Dict[str, object]) -> Dict[str, object]:
        """Apply a scheduled docs event using the same shape as docs tools."""
        payload = dict(event or {})
        op = str(payload.get("op", "")).lower()
        doc_id = payload.get("doc_id")
        if op == "update" or (isinstance(doc_id, str) and doc_id in self.docs):
            if not isinstance(doc_id, str):
                raise ValueError("docs.update delivery requires doc_id")
            return self.update(
                doc_id=doc_id,
                title=(
                    payload.get("title")
                    if isinstance(payload.get("title"), str)
                    else None
                ),
                body=(
                    payload.get("body")
                    if isinstance(payload.get("body"), str)
                    else None
                ),
                tags=(
                    payload.get("tags")
                    if isinstance(payload.get("tags"), list)
                    else None
                ),
                status=(
                    payload.get("status")
                    if isinstance(payload.get("status"), str)
                    else None
                ),
            )

        title = payload.get("title")
        body = payload.get("body")
        if not isinstance(title, str) or not isinstance(body, str):
            raise ValueError("docs delivery requires title/body for create")
        tags = payload.get("tags") if isinstance(payload.get("tags"), list) else None
        return self.create(
            title=title,
            body=body,
            tags=tags,
            owner=(
                payload.get("owner") if isinstance(payload.get("owner"), str) else None
            ),
            status=(
                payload.get("status")
                if isinstance(payload.get("status"), str)
                else "DRAFT"
            ),
        )

    def _init_seq(self) -> int:
        seq = 1
        for doc_id in self.docs.keys():
            try:
                if doc_id.startswith("DOC-"):
                    seq = max(seq, int(doc_id.split("-", 1)[1]) + 1)
            except ValueError:
                continue
        return seq

    def _doc_payload(self, doc: Document, *, include_body: bool) -> Dict[str, object]:
        meta = self.metadata.setdefault(
            doc.doc_id,
            {
                "owner": "system",
                "status": "ACTIVE",
                "version": 1,
                "created_ms": self._now_ms(),
                "updated_ms": self._now_ms(),
            },
        )
        payload: Dict[str, object] = {
            "doc_id": doc.doc_id,
            "title": doc.title,
            "tags": list(doc.tags or []),
            "owner": str(meta.get("owner", "system")),
            "status": str(meta.get("status", "ACTIVE")),
            "version": int(meta.get("version", 1)),
            "created_ms": int(meta.get("created_ms", 0)),
            "updated_ms": int(meta.get("updated_ms", 0)),
        }
        if include_body:
            payload["body"] = doc.body
        return payload

    def _now_ms(self) -> int:
        self._clock_ms += 1
        return self._clock_ms
