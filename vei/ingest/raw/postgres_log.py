"""Postgres-backed append-only raw event log — live default.

Requires ``psycopg`` or ``asyncpg``.  This module is import-safe when
the dependency is absent.
"""

from __future__ import annotations

import json
import re
import uuid
from datetime import datetime
from typing import Any, Dict, Iterator, Optional

_PG_AVAILABLE = False
try:
    import psycopg  # noqa: F401

    _PG_AVAILABLE = True
except ImportError:
    pass


_SAFE_IDENT_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _encode_cursor(created_at: datetime, record_id: str) -> str:
    return f"{created_at.isoformat()}|{record_id}"


def _decode_cursor(cursor: str) -> tuple[datetime, str] | None:
    if not cursor or "|" not in cursor:
        return None
    raw_created_at, record_id = cursor.split("|", 1)
    if not raw_created_at or not record_id:
        return None
    return datetime.fromisoformat(raw_created_at), record_id


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
        sql = f"INSERT INTO {table} (record_id, tenant_id, source, ts_ms, data) VALUES (%s, %s, %s, %s, %s) ON CONFLICT (record_id) DO UPDATE SET record_id = {table}.record_id RETURNING created_at, record_id"  # nosec B608 - table identifier validated in _validate_identifier  # noqa: E501
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
            row = cur.fetchone()
        if row is None:
            return record_id
        created_at, stored_record_id = row
        if not isinstance(created_at, datetime):
            return str(stored_record_id)
        return _encode_cursor(created_at, str(stored_record_id))

    def iter_since(self, cursor: str) -> Iterator[Dict[str, Any]]:
        if self._conn is None:
            return
        table = self._table
        decoded_cursor = _decode_cursor(cursor)
        if decoded_cursor is None and cursor:
            with self._conn.cursor() as cur:
                cur.execute(
                    f"SELECT created_at, record_id FROM {table} WHERE tenant_id = %s AND record_id = %s",  # nosec B608 - table identifier validated in _validate_identifier  # noqa: E501
                    [self._tenant_id, cursor],
                )
                row = cur.fetchone()
            if row is not None:
                created_at, stored_record_id = row
                if isinstance(created_at, datetime):
                    decoded_cursor = (created_at, str(stored_record_id))
        if decoded_cursor is not None:
            created_at, record_id = decoded_cursor
            sql = f"SELECT data, created_at, record_id FROM {table} WHERE tenant_id = %s AND (created_at > %s OR (created_at = %s AND record_id > %s)) ORDER BY created_at, record_id"  # nosec B608 - table identifier validated in _validate_identifier  # noqa: E501
            params = [self._tenant_id, created_at, created_at, record_id]
        else:
            sql = f"SELECT data, created_at, record_id FROM {table} WHERE tenant_id = %s ORDER BY created_at, record_id"  # nosec B608 - table identifier validated in _validate_identifier  # noqa: E501
            params = [self._tenant_id]
        with self._conn.cursor() as cur:
            cur.execute(sql, params)
            for row in cur:
                payload = (
                    json.loads(row[0]) if isinstance(row[0], str) else dict(row[0])
                )
                created_at = row[1]
                record_id = row[2]
                if isinstance(created_at, datetime):
                    payload["_cursor"] = _encode_cursor(created_at, str(record_id))
                yield payload

    def close(self) -> None:
        if self._conn is not None:
            self._conn.close()
