"""SQLite-backed key-value storage for dagster-sensor-guard.

Replaces Dagster's daemon_cursor_storage which is not available in all
deployment environments (e.g. Dagster Cloud EKS code servers).
"""

from __future__ import annotations

import logging
import os
import sqlite3
import tempfile
import threading
import time
from typing import Mapping, Optional

logger = logging.getLogger("dagster.sensor_guard")

_DEFAULT_RETENTION_DAYS = 7
_CLEANUP_EVERY_N_WRITES = 10


def _resolve_db_path(db_path: Optional[str] = None) -> str:
    """Resolve the SQLite database file path.

    Priority:
    1. Explicit db_path parameter
    2. $DAGSTER_HOME/sensor_guard.db
    3. <tempdir>/dagster_sensor_guard.db
    """
    if db_path is not None:
        return db_path

    dagster_home = os.environ.get("DAGSTER_HOME")
    if dagster_home:
        return os.path.join(dagster_home, "sensor_guard.db")

    return os.path.join(tempfile.gettempdir(), "dagster_sensor_guard.db")


class SqliteGuardStorage:
    """SQLite-backed implementation of the CursorStorage protocol.

    Stores key-value pairs with automatic TTL-based cleanup of old records.
    """

    def __init__(
        self,
        db_path: Optional[str] = None,
        retention_days: int = _DEFAULT_RETENTION_DAYS,
    ) -> None:
        self._db_path = _resolve_db_path(db_path)
        self._retention_days = retention_days
        self._write_count = 0
        self._local = threading.local()
        self._ensure_table()

    def _get_connection(self) -> sqlite3.Connection:
        conn = getattr(self._local, "conn", None)
        if conn is None:
            conn = sqlite3.connect(self._db_path)
            conn.execute("PRAGMA journal_mode=WAL")
            self._local.conn = conn
        return conn

    def _ensure_table(self) -> None:
        conn = self._get_connection()
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS kv_store (
                key        TEXT PRIMARY KEY,
                value      TEXT NOT NULL,
                updated_at REAL NOT NULL
            )
            """
        )
        conn.commit()

    def get_cursor_values(self, keys: set[str]) -> Mapping[str, str]:
        if not keys:
            return {}
        conn = self._get_connection()
        placeholders = ",".join("?" for _ in keys)
        cursor = conn.execute(
            f"SELECT key, value FROM kv_store WHERE key IN ({placeholders})",
            tuple(keys),
        )
        return dict(cursor.fetchall())

    def set_cursor_values(self, values: Mapping[str, str]) -> None:
        if not values:
            return
        conn = self._get_connection()
        now = time.time()
        conn.executemany(
            """
            INSERT INTO kv_store (key, value, updated_at)
            VALUES (?, ?, ?)
            ON CONFLICT(key) DO UPDATE SET
                value = excluded.value,
                updated_at = excluded.updated_at
            """,
            [(k, v, now) for k, v in values.items()],
        )
        conn.commit()

        self._write_count += 1
        if self._write_count % _CLEANUP_EVERY_N_WRITES == 0:
            self._cleanup()

    def _cleanup(self) -> None:
        """Remove records older than retention_days."""
        cutoff = time.time() - (self._retention_days * 86400)
        conn = self._get_connection()
        deleted = conn.execute(
            "DELETE FROM kv_store WHERE updated_at < ?", (cutoff,)
        ).rowcount
        conn.commit()
        if deleted:
            logger.debug(
                "sensor_guard storage cleanup: removed %d stale records", deleted
            )

    def close(self) -> None:
        """Close the database connection for the current thread."""
        conn = getattr(self._local, "conn", None)
        if conn is not None:
            conn.close()
            self._local.conn = None
