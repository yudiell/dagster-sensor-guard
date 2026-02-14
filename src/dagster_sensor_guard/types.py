"""Enums and configuration types for dagster-sensor-guard."""

from enum import Enum


class ResetStrategy(str, Enum):
    """How the error count resets after a successful tick."""

    FULL = "full"
    """A single success resets the count to zero."""

    DECAY = "decay"
    """A success decrements the count by a configurable amount."""
