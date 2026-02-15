"""Shared test helpers for dagster-sensor-guard."""

import pytest
from dagster import DagsterInstance, job, op


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
