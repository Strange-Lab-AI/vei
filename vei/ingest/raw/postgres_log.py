"""Postgres-backed append-only raw event log — live default.

Requires ``psycopg`` or ``asyncpg``.  This module is import-safe when
the dependency is absent.
"""

from __future__ import annotations

import json
import re
import uuid
from typing import Any, Dict, Iterator, Optional

_PG_AVAILABLE = False
try:
    import psycopg  # noqa: F401

    _PG_AVAILABLE = True
except ImportError:
    pass


_SAFE_IDENT_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _validate_identifier(value: str, *, label: str) -> str:
    """Raise if ``value`` is not a safe SQL identifier."""
    if not _SAFE_IDENT_RE.match(value):
        raise ValueError(
            f"Invalid {label} identifier: {value!r}. "
            "Must match [A-Za-z_][A-Za-z0-9_]*."
        )
    return value


class PostgresRawLog:
    """Per-tenant append-only raw event log over Postgres.

    Table name is validated against ``_SAFE_IDENT_RE`` at construction time,
    which is why f-string interpolation of the identifier is safe.  All
    value positions use parameterised placeholders.
    """

    def __init__(
        self,
        dsn: str,
        tenant_id: str = "default",
        table: str = "raw_events",
    ) -> None:
        self._dsn = dsn
        self._tenant_id = tenant_id
        self._table = _validate_identifier(table, label="table")
        self._conn: Optional[Any] = None
        if _PG_AVAILABLE:
            self._conn = psycopg.connect(dsn, autocommit=True)
            self._ensure_table()

    def _ensure_table(self) -> None:
        if self._conn is None:
            return
        table = self._table
        sql = f"CREATE TABLE IF NOT EXISTS {table} (record_id TEXT PRIMARY KEY, tenant_id TEXT NOT NULL, source TEXT DEFAULT '', ts_ms BIGINT DEFAULT 0, data JSONB NOT NULL, created_at TIMESTAMPTZ DEFAULT NOW())"  # nosec B608 - table identifier validated in _validate_identifier  # noqa: E501
        with self._conn.cursor() as cur:
            cur.execute(sql)

    def append(self, raw_record: Dict[str, Any]) -> str:
        record_id = raw_record.get("record_id", str(uuid.uuid4()))
        if self._conn is None:
            return record_id
        data_json = json.dumps(raw_record)
        table = self._table
        sql = f"INSERT INTO {table} (record_id, tenant_id, source, ts_ms, data) VALUES (%s, %s, %s, %s, %s) ON CONFLICT (record_id) DO NOTHING"  # nosec B608 - table identifier validated in _validate_identifier  # noqa: E501
        with self._conn.cursor() as cur:
            cur.execute(
                sql,
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
        table = self._table
        if cursor:
            sql = f"SELECT data FROM {table} WHERE tenant_id = %s AND record_id > %s ORDER BY created_at"  # nosec B608 - table identifier validated in _validate_identifier  # noqa: E501
            params = [self._tenant_id, cursor]
        else:
            sql = f"SELECT data FROM {table} WHERE tenant_id = %s ORDER BY created_at"  # nosec B608 - table identifier validated in _validate_identifier  # noqa: E501
            params = [self._tenant_id]
        with self._conn.cursor() as cur:
            cur.execute(sql, params)
            for row in cur:
                yield json.loads(row[0]) if isinstance(row[0], str) else row[0]

    def close(self) -> None:
        if self._conn is not None:
            self._conn.close()
