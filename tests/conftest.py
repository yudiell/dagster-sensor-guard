"""Shared test helpers for dagster-sensor-guard."""

from dagster import job, op


def make_job():
    """Create a no-op job for sensor targets."""

    @op
    def noop():
        pass

    @job
    def noop_job():
        noop()

    return noop_job
