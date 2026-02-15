"""dagster-sensor-guard: Configurable error tolerance for Dagster sensors."""

from dagster_sensor_guard.decorator import resilient_sensor
from dagster_sensor_guard.guard import SensorGuard, SensorGuardKeyError
from dagster_sensor_guard.types import ResetStrategy

__all__ = [
    "resilient_sensor",
    "ResetStrategy",
    "SensorGuard",
    "SensorGuardKeyError",
]
