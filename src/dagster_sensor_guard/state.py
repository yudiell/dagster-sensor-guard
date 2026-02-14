"""Error state tracking and cursor management for dagster-sensor-guard.

The guard stores its state alongside user cursor data in the sensor's cursor JSON.
The cursor format is:
    {"__dagster_sensor_guard_v1": {...}, "__user_cursor": <user_value>}

When no guard state exists (first tick), defaults are returned.
When the user doesn't use cursors, __user_cursor is None.
"""

from __future__ import annotations

import json
import time
from dataclasses import dataclass
from typing import Optional, Tuple

from dagster_sensor_guard.types import ResetStrategy

_GUARD_KEY = "__dagster_sensor_guard_v1"
_GUARD_KEY_LEGACY = "__sensor_guard"
_USER_KEY = "__user_cursor"


@dataclass(frozen=True)
class GuardState:
    """Tracks consecutive error state across sensor ticks."""

    error_count: int = 0
    first_error_ts: Optional[float] = None
    last_error_ts: Optional[float] = None

    def to_dict(self) -> dict:
        return {
            "error_count": self.error_count,
            "first_error_ts": self.first_error_ts,
            "last_error_ts": self.last_error_ts,
        }

    @classmethod
    def from_dict(cls, data: dict) -> GuardState:
        return cls(
            error_count=data.get("error_count", 0),
            first_error_ts=data.get("first_error_ts"),
            last_error_ts=data.get("last_error_ts"),
        )


def parse_cursor(raw_cursor: Optional[str]) -> Tuple[GuardState, Optional[str]]:
    """Parse a raw cursor string into guard state and user cursor.

    Returns (GuardState, user_cursor). If the cursor has never been set or
    doesn't contain guard data, returns default state and the raw cursor
    as-is (for backwards compatibility with sensors that pre-date the guard).
    """
    if raw_cursor is None:
        return GuardState(), None

    try:
        data = json.loads(raw_cursor)
    except (json.JSONDecodeError, TypeError):
        # Cursor is a plain string from the user, not our JSON wrapper.
        return GuardState(), raw_cursor

    if not isinstance(data, dict):
        # Valid JSON but not a dict — treat as user cursor.
        return GuardState(), raw_cursor

    # Support both the current key and the legacy key for backward compat.
    if _GUARD_KEY in data:
        guard_data = data[_GUARD_KEY]
    elif _GUARD_KEY_LEGACY in data:
        guard_data = data[_GUARD_KEY_LEGACY]
    else:
        # Dict but not our format — treat as user cursor.
        return GuardState(), raw_cursor

    guard_state = GuardState.from_dict(guard_data)
    user_cursor = data.get(_USER_KEY)
    return guard_state, user_cursor


def build_cursor(guard_state: GuardState, user_cursor: Optional[str]) -> str:
    """Serialize guard state and user cursor into a single cursor string."""
    return json.dumps({
        _GUARD_KEY: guard_state.to_dict(),
        _USER_KEY: user_cursor,
    })


def increment_error(
    state: GuardState,
    window_minutes: Optional[int] = None,
) -> GuardState:
    """Record a new consecutive error.

    When window_minutes is set and the error chain has expired (first error
    is older than the window), the state resets and a fresh chain starts.
    """
    now = time.time()

    # Check if the error chain has expired outside the window.
    if (
        window_minutes is not None
        and state.first_error_ts is not None
        and (now - state.first_error_ts) > window_minutes * 60
    ):
        state = GuardState()

    return GuardState(
        error_count=state.error_count + 1,
        first_error_ts=state.first_error_ts if state.first_error_ts is not None else now,
        last_error_ts=now,
    )


def apply_reset(
    state: GuardState,
    strategy: ResetStrategy,
    decay_amount: int,
) -> GuardState:
    """Apply reset strategy after a successful tick."""
    if strategy == ResetStrategy.FULL:
        return GuardState()

    # Decay strategy
    new_count = max(0, state.error_count - decay_amount)
    if new_count == 0:
        return GuardState()
    return GuardState(
        error_count=new_count,
        first_error_ts=state.first_error_ts,
        last_error_ts=state.last_error_ts,
    )


def should_raise(
    state: GuardState,
    threshold: int,
) -> bool:
    """Determine whether the current error should be raised to Dagster.

    The error should raise when the count exceeds the threshold.
    Window expiry is handled by increment_error(), so by the time this
    function is called the state already reflects any window-based reset.
    """
    return state.error_count > threshold
