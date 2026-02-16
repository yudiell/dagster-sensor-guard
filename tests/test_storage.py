"""Tests for SqliteGuardStorage."""

import time
from unittest.mock import patch

import pytest

from dagster_sensor_guard.storage import SqliteGuardStorage, _resolve_db_path


class TestResolveDbPath:
    def test_explicit_path_takes_priority(self):
        assert _resolve_db_path("/custom/path.db") == "/custom/path.db"

    def test_dagster_home_fallback(self, monkeypatch):
        monkeypatch.setenv("DAGSTER_HOME", "/opt/dagster")
        assert _resolve_db_path() == "/opt/dagster/sensor_guard.db"

    def test_tempdir_fallback(self, monkeypatch):
        monkeypatch.delenv("DAGSTER_HOME", raising=False)
        result = _resolve_db_path()
        assert "dagster_sensor_guard.db" in result

    def test_memory_path(self):
        assert _resolve_db_path(":memory:") == ":memory:"


class TestSqliteGuardStorage:
    def test_get_empty(self):
        s = SqliteGuardStorage(db_path=":memory:")
        assert s.get_cursor_values({"nonexistent"}) == {}

    def test_set_and_get(self):
        s = SqliteGuardStorage(db_path=":memory:")
        s.set_cursor_values({"k1": "v1", "k2": "v2"})
        result = s.get_cursor_values({"k1", "k2"})
        assert result == {"k1": "v1", "k2": "v2"}

    def test_overwrite(self):
        s = SqliteGuardStorage(db_path=":memory:")
        s.set_cursor_values({"k1": "v1"})
        s.set_cursor_values({"k1": "v2"})
        assert s.get_cursor_values({"k1"}) == {"k1": "v2"}

    def test_partial_keys(self):
        s = SqliteGuardStorage(db_path=":memory:")
        s.set_cursor_values({"k1": "v1", "k2": "v2"})
        assert s.get_cursor_values({"k1"}) == {"k1": "v1"}
        assert s.get_cursor_values({"k3"}) == {}

    def test_empty_keys_returns_empty(self):
        s = SqliteGuardStorage(db_path=":memory:")
        assert s.get_cursor_values(set()) == {}

    def test_empty_values_noop(self):
        s = SqliteGuardStorage(db_path=":memory:")
        s.set_cursor_values({})  # should not error


class TestCleanup:
    def test_old_records_cleaned(self):
        s = SqliteGuardStorage(db_path=":memory:", retention_days=1)
        # Manually insert an old record.
        conn = s._get_connection()
        old_ts = time.time() - 2 * 86400  # 2 days old
        conn.execute(
            "INSERT INTO kv_store (key, value, updated_at) VALUES (?, ?, ?)",
            ("old_key", "old_val", old_ts),
        )
        conn.commit()

        s._cleanup()
        assert s.get_cursor_values({"old_key"}) == {}

    def test_recent_records_kept(self):
        s = SqliteGuardStorage(db_path=":memory:", retention_days=1)
        s.set_cursor_values({"fresh": "value"})
        s._cleanup()
        assert s.get_cursor_values({"fresh"}) == {"fresh": "value"}

    def test_cleanup_runs_periodically(self):
        s = SqliteGuardStorage(db_path=":memory:", retention_days=7)
        with patch.object(s, "_cleanup") as mock_cleanup:
            for i in range(25):
                s.set_cursor_values({f"k{i}": f"v{i}"})
            # Cleanup called on every 10th write: writes 10 and 20.
            assert mock_cleanup.call_count == 2


class TestFilePersistence:
    def test_data_persists_across_instances(self, tmp_path):
        path = str(tmp_path / "test.db")
        s1 = SqliteGuardStorage(db_path=path)
        s1.set_cursor_values({"key": "value"})
        s1.close()

        s2 = SqliteGuardStorage(db_path=path)
        assert s2.get_cursor_values({"key"}) == {"key": "value"}
        s2.close()


class TestProtocolCompliance:
    def test_satisfies_cursor_storage_protocol(self):
        """SqliteGuardStorage satisfies the CursorStorage protocol."""
        s = SqliteGuardStorage(db_path=":memory:")
        assert hasattr(s, "get_cursor_values")
        assert hasattr(s, "set_cursor_values")
        assert callable(s.get_cursor_values)
        assert callable(s.set_cursor_values)
