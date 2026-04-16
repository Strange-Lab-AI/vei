"""Postgres-backed append-only raw event log — live default.

Requires ``psycopg`` or ``asyncpg``.  This module is import-safe when
the dependency is absent.
"""

from __future__ import annotations

import json
import uuid
from typing import Any, Dict, Iterator, Optional

_PG_AVAILABLE = False
try:
    import psycopg  # noqa: F401

    _PG_AVAILABLE = True
except ImportError:
    pass


class PostgresRawLog:
    """Per-tenant append-only raw event log over Postgres."""

    def __init__(
        self,
        dsn: str,
        tenant_id: str = "default",
        table: str = "raw_events",
    ) -> None:
        self._dsn = dsn
        self._tenant_id = tenant_id
        self._table = table
        self._conn: Optional[Any] = None
        if _PG_AVAILABLE:
            self._conn = psycopg.connect(dsn, autocommit=True)
            self._ensure_table()

    def _ensure_table(self) -> None:
        if self._conn is None:
            return
        with self._conn.cursor() as cur:
            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS {self._table} (
                    record_id TEXT PRIMARY KEY,
                    tenant_id TEXT NOT NULL,
                    source TEXT DEFAULT '',
                    ts_ms BIGINT DEFAULT 0,
                    data JSONB NOT NULL,
                    created_at TIMESTAMPTZ DEFAULT NOW()
                )
            """)

    def append(self, raw_record: Dict[str, Any]) -> str:
        record_id = raw_record.get("record_id", str(uuid.uuid4()))
        if self._conn is None:
            return record_id
        data_json = json.dumps(raw_record)
        with self._conn.cursor() as cur:
            cur.execute(
                f"INSERT INTO {self._table} (record_id, tenant_id, source, ts_ms, data) "
                "VALUES (%s, %s, %s, %s, %s) ON CONFLICT (record_id) DO NOTHING",
                [
                    record_id,
                    self._tenant_id,
                    raw_record.get("source", ""),
                    raw_record.get("ts_ms", 0),
                    data_json,
                ],
            )
        return record_id

    def iter_since(self, cursor: str) -> Iterator[Dict[str, Any]]:
        if self._conn is None:
            return
        with self._conn.cursor() as cur:
            if cursor:
                cur.execute(
                    f"SELECT data FROM {self._table} "
                    f"WHERE tenant_id = %s AND record_id > %s ORDER BY created_at",
                    [self._tenant_id, cursor],
                )
            else:
                cur.execute(
                    f"SELECT data FROM {self._table} "
                    f"WHERE tenant_id = %s ORDER BY created_at",
                    [self._tenant_id],
                )
            for row in cur:
                yield json.loads(row[0]) if isinstance(row[0], str) else row[0]

    def close(self) -> None:
        if self._conn is not None:
            self._conn.close()
