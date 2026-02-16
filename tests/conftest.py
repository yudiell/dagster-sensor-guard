"""Shared test helpers for dagster-sensor-guard."""

import pytest
from dagster import DagsterInstance, job, op

from dagster_sensor_guard.storage import SqliteGuardStorage


def make_job():
    """Create a no-op job for sensor targets."""

    @op
    def noop():
        pass

    @job
    def noop_job():
        noop()

    return noop_job


@pytest.fixture
def instance():
    with DagsterInstance.ephemeral() as inst:
        yield inst


@pytest.fixture
def storage():
    """In-memory SQLite storage for unit tests."""
    s = SqliteGuardStorage(db_path=":memory:", retention_days=7)
    yield s
    s.close()


@pytest.fixture
def db_path(tmp_path):
    """Temporary SQLite database path for decorator/integration tests."""
    return str(tmp_path / "sensor_guard.db")
