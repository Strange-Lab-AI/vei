from __future__ import annotations

import sqlite3
import threading
import time
from pathlib import Path
from typing import Iterable, Optional


class MemoryStore:
    """Lightweight episodic memory backed by SQLite for deterministic runs."""

    def __init__(self, path: Optional[str | Path] = None) -> None:
        self._path = Path(path).expanduser() if path else None
        self._lock = threading.Lock()
        self._conn = sqlite3.connect(self._path or ":memory:", check_same_thread=False)
        self._init()

    def _init(self) -> None:
        with self._conn:
            self._conn.execute("""
                CREATE TABLE IF NOT EXISTS facts (
                    kind TEXT NOT NULL,
                    fact_key TEXT NOT NULL,
                    fact_value TEXT NOT NULL,
                    created_ms INTEGER NOT NULL
                )
                """)
            self._conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_facts_kind_key ON facts(kind, fact_key)"
            )

    def remember(self, *, kind: str, key: str, value: str) -> None:
        now_ms = int(time.time() * 1000)
        with self._lock, self._conn:
            self._conn.execute(
                "INSERT INTO facts(kind, fact_key, fact_value, created_ms) VALUES (?, ?, ?, ?)",
                (kind, key, value, now_ms),
            )

    def recall(
        self, *, kind: str, key: Optional[str] = None, limit: int = 5
    ) -> list[str]:
        sql = "SELECT fact_value FROM facts WHERE kind = ?"
        params: list[object] = [kind]
        if key is not None:
            sql += " AND fact_key = ?"
            params.append(key)
        sql += " ORDER BY created_ms DESC LIMIT ?"
        params.append(max(1, int(limit)))
        with self._lock, self._conn:
            rows = self._conn.execute(sql, params).fetchall()
        return [row[0] for row in rows]

    def all(self, kind: Optional[str] = None) -> Iterable[tuple[str, str, str]]:
        sql = "SELECT kind, fact_key, fact_value FROM facts"
        params: list[object] = []
        if kind is not None:
            sql += " WHERE kind = ?"
            params.append(kind)
        sql += " ORDER BY created_ms DESC"
        with self._lock, self._conn:
            rows = self._conn.execute(sql, params).fetchall()
        for row in rows:
            yield row[0], row[1], row[2]

    def close(self) -> None:
        with self._lock:
            self._conn.close()
