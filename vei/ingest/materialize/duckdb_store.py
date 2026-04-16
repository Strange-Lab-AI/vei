"""DuckDB-based materializer — offline / replay only.

Stores canonical events in a DuckDB database with per-tenant tables.
Provides graph projection queries and case event retrieval.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any, Dict, List, Optional

from vei.events.models import CanonicalEvent
from vei.ingest.api import SessionSlice

logger = logging.getLogger(__name__)

_DUCKDB_AVAILABLE = False
try:
    import duckdb

    _DUCKDB_AVAILABLE = True
except ImportError:
    pass


class DuckDBMaterializer:
    """Local canonical-event store + graph projections over DuckDB."""

    def __init__(self, db_path: Path) -> None:
        self._db_path = Path(db_path)
        self._db_path.parent.mkdir(parents=True, exist_ok=True)
        if _DUCKDB_AVAILABLE:
            self._conn = duckdb.connect(str(self._db_path))
            self._init_schema()
        else:
            self._conn = None

    def _init_schema(self) -> None:
        if self._conn is None:
            return
        self._conn.execute("""
            CREATE TABLE IF NOT EXISTS canonical_events (
                event_id VARCHAR PRIMARY KEY,
                tenant_id VARCHAR,
                case_id VARCHAR,
                ts_ms BIGINT,
                domain VARCHAR,
                kind VARCHAR,
                data JSON
            )
        """)
        self._conn.execute("""
            CREATE TABLE IF NOT EXISTS cases (
                case_id VARCHAR PRIMARY KEY,
                tenant_id VARCHAR,
                surfaces JSON,
                start_ts BIGINT,
                end_ts BIGINT
            )
        """)

    def apply(self, events: List[CanonicalEvent]) -> int:
        if self._conn is None:
            return 0
        applied = 0
        for event in events:
            try:
                self._conn.execute(
                    """INSERT OR REPLACE INTO canonical_events
                       (event_id, tenant_id, case_id, ts_ms, domain, kind, data)
                       VALUES (?, ?, ?, ?, ?, ?, ?)""",
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
            ) as exc:  # noqa: BLE001 - duckdb errors are opaque; log and skip
                logger.warning(
                    "duckdb_apply_failed",
                    extra={"event_id": event.event_id, "error": str(exc)[:200]},
                )
        return applied

    def query_graph(
        self, tenant_id: str, scope: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        if self._conn is None:
            return {}
        domain_filter = (scope or {}).get("domain")
        if domain_filter:
            rows = self._conn.execute(
                "SELECT DISTINCT kind FROM canonical_events WHERE tenant_id = ? AND domain = ?",
                [tenant_id, domain_filter],
            ).fetchall()
        else:
            rows = self._conn.execute(
                "SELECT DISTINCT domain, kind FROM canonical_events WHERE tenant_id = ?",
                [tenant_id],
            ).fetchall()
        return {
            "tenant_id": tenant_id,
            "kinds": [r[0] if len(r) == 1 else list(r) for r in rows],
        }

    def query_events(
        self, tenant_id: str, filters: Optional[Dict[str, Any]] = None
    ) -> List[CanonicalEvent]:
        if self._conn is None:
            return []
        limit = (filters or {}).get("limit", 1000)
        rows = self._conn.execute(
            "SELECT data FROM canonical_events WHERE tenant_id = ? ORDER BY ts_ms LIMIT ?",
            [tenant_id, limit],
        ).fetchall()
        return [CanonicalEvent.model_validate_json(r[0]) for r in rows]

    def query_case(self, case_id: str) -> List[CanonicalEvent]:
        if self._conn is None:
            return []
        rows = self._conn.execute(
            "SELECT data FROM canonical_events WHERE case_id = ? ORDER BY ts_ms",
            [case_id],
        ).fetchall()
        return [CanonicalEvent.model_validate_json(r[0]) for r in rows]

    def close(self) -> None:
        if self._conn is not None:
            self._conn.close()


class DuckDBSessionMaterializer:
    """SessionMaterializer over a DuckDB store."""

    def __init__(self, materializer: DuckDBMaterializer) -> None:
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
