"""Postgres-backed materializer and session materializer — live default.

Requires ``psycopg``.  Import-safe when the dependency is absent.
"""

from __future__ import annotations

import json
import logging
from typing import Any, Dict, List, Optional

from vei.events.models import CanonicalEvent
from vei.ingest.api import SessionSlice

logger = logging.getLogger(__name__)

_PG_AVAILABLE = False
try:
    import psycopg  # noqa: F401

    _PG_AVAILABLE = True
except ImportError:
    pass


class PostgresMaterializer:
    """Canonical-event store + graph projections over Postgres."""

    def __init__(self, dsn: str) -> None:
        self._dsn = dsn
        self._conn: Optional[Any] = None
        if _PG_AVAILABLE:
            self._conn = psycopg.connect(dsn, autocommit=True)
            self._ensure_tables()

    def _ensure_tables(self) -> None:
        if self._conn is None:
            return
        with self._conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS canonical_events (
                    event_id TEXT PRIMARY KEY,
                    tenant_id TEXT NOT NULL,
                    case_id TEXT DEFAULT '',
                    ts_ms BIGINT DEFAULT 0,
                    domain TEXT NOT NULL,
                    kind TEXT NOT NULL,
                    data JSONB NOT NULL
                )
            """)
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_ce_tenant ON canonical_events (tenant_id)
            """)
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_ce_case ON canonical_events (case_id)
            """)

    def apply(self, events: List[CanonicalEvent]) -> int:
        if self._conn is None:
            return 0
        applied = 0
        with self._conn.cursor() as cur:
            for event in events:
                try:
                    cur.execute(
                        """INSERT INTO canonical_events
                           (event_id, tenant_id, case_id, ts_ms, domain, kind, data)
                           VALUES (%s, %s, %s, %s, %s, %s, %s)
                           ON CONFLICT (event_id) DO NOTHING""",
                        [
                            event.event_id,
                            event.tenant_id,
                            event.case_id or "",
                            event.ts_ms,
                            event.domain.value,
                            event.kind,
                            event.model_dump_json(),
                        ],
                    )
                    applied += 1
                except (
                    Exception
                ) as exc:  # noqa: BLE001 - psycopg errors are opaque; log and skip
                    logger.warning(
                        "postgres_apply_failed",
                        extra={"event_id": event.event_id, "error": str(exc)[:200]},
                    )
        return applied

    def query_graph(
        self, tenant_id: str, scope: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        if self._conn is None:
            return {}
        with self._conn.cursor() as cur:
            cur.execute(
                "SELECT DISTINCT domain, kind FROM canonical_events WHERE tenant_id = %s",
                [tenant_id],
            )
            rows = cur.fetchall()
        return {"tenant_id": tenant_id, "kinds": [list(r) for r in rows]}

    def query_events(
        self, tenant_id: str, filters: Optional[Dict[str, Any]] = None
    ) -> List[CanonicalEvent]:
        if self._conn is None:
            return []
        limit = (filters or {}).get("limit", 1000)
        with self._conn.cursor() as cur:
            cur.execute(
                "SELECT data FROM canonical_events WHERE tenant_id = %s ORDER BY ts_ms LIMIT %s",
                [tenant_id, limit],
            )
            rows = cur.fetchall()
        return [
            CanonicalEvent.model_validate(
                r[0] if isinstance(r[0], dict) else json.loads(r[0])
            )
            for r in rows
        ]

    def query_case(self, case_id: str) -> List[CanonicalEvent]:
        if self._conn is None:
            return []
        with self._conn.cursor() as cur:
            cur.execute(
                "SELECT data FROM canonical_events WHERE case_id = %s ORDER BY ts_ms",
                [case_id],
            )
            rows = cur.fetchall()
        return [
            CanonicalEvent.model_validate(
                r[0] if isinstance(r[0], dict) else json.loads(r[0])
            )
            for r in rows
        ]

    def close(self) -> None:
        if self._conn is not None:
            self._conn.close()


class PostgresSessionMaterializer:
    """SessionMaterializer over Postgres."""

    def __init__(self, materializer: PostgresMaterializer) -> None:
        self._mat = materializer

    def materialize(
        self,
        tenant_id: str,
        case_id: str,
        *,
        window_ms: Optional[int] = None,
    ) -> SessionSlice:
        events = self._mat.query_case(case_id)
        graph = self._mat.query_graph(tenant_id)
        return SessionSlice(
            tenant_id=tenant_id,
            case_id=case_id,
            events=events,
            graph_slice=graph,
        )
