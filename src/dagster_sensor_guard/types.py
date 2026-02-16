"""Enums and configuration types for dagster-sensor-guard."""

from enum import Enum
from typing import Mapping, Protocol


class CursorStorage(Protocol):
    """Protocol for Dagster's daemon cursor storage."""

    def get_cursor_values(self, keys: set[str]) -> Mapping[str, str]: ...
    def set_cursor_values(self, values: Mapping[str, str]) -> None: ...


class ResetStrategy(str, Enum):
    """How the error count resets after a successful tick."""

    FULL = "full"
    """A single success resets the count to zero."""

    DECAY = "decay"
    """A success decrements the count by a configurable amount."""
