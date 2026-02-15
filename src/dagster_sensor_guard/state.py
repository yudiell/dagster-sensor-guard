"""Error state tracking and KVS storage for dagster-sensor-guard.

Guard state is stored in Dagster's daemon_cursor_storage (a SQL-backed
key-value store), completely decoupled from the user's sensor cursor.
The user's cursor flows through Dagster natively, untouched.
"""

from __future__ import annotations

import json
import time
from dataclasses import dataclass
from typing import Mapping, Optional, Tuple

from dagster_sensor_guard.types import ResetStrategy

_KVS_KEY_PREFIX = "dagster_sensor_guard"

# Legacy envelope keys — only used for migration detection.
_ENVELOPE_KEY_V1 = "__dagster_sensor_guard_v1"
_ENVELOPE_KEY_LEGACY = "__sensor_guard"
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


def kvs_key(sensor_name: str) -> str:
    """Build the KVS key for a sensor's guard state."""
    return f"{_KVS_KEY_PREFIX}:{sensor_name}"


def load_guard_state(daemon_cursor_storage: object, sensor_name: str) -> GuardState:
    """Load guard state from KVS. Returns default GuardState if not found."""
    key = kvs_key(sensor_name)
    values: Mapping[str, str] = daemon_cursor_storage.get_cursor_values({key})
    raw = values.get(key)
    if raw is None:
        return GuardState()
    try:
        return GuardState.from_dict(json.loads(raw))
    except (json.JSONDecodeError, TypeError, KeyError):
        return GuardState()


def save_guard_state(
    daemon_cursor_storage: object, sensor_name: str, state: GuardState
) -> None:
    """Persist guard state to KVS."""
    key = kvs_key(sensor_name)
    daemon_cursor_storage.set_cursor_values({key: json.dumps(state.to_dict())})


def detect_envelope_cursor(
    raw_cursor: Optional[str],
) -> Optional[Tuple[GuardState, Optional[str]]]:
    """Detect old envelope format in cursor.

    Returns (guard_state, user_cursor) if the cursor contains an old-style
    envelope, or None if it's a plain user cursor.
    """
    if raw_cursor is None:
        return None

    try:
        data = json.loads(raw_cursor)
    except (json.JSONDecodeError, TypeError):
        return None

    if not isinstance(data, dict):
        return None

    if _ENVELOPE_KEY_V1 in data:
        guard_data = data[_ENVELOPE_KEY_V1]
    elif _ENVELOPE_KEY_LEGACY in data:
        guard_data = data[_ENVELOPE_KEY_LEGACY]
    else:
        return None

    guard_state = GuardState.from_dict(guard_data)
    user_cursor = data.get(_USER_KEY)
    return guard_state, user_cursor


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
