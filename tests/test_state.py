"""Tests for state management and cursor serialization."""

import json
import time
from unittest.mock import patch

from dagster_sensor_guard.state import (
    GuardState,
    apply_reset,
    build_cursor,
    increment_error,
    parse_cursor,
    should_raise,
)
from dagster_sensor_guard.types import ResetStrategy


class TestParseCursor:
    def test_none_cursor(self):
        state, user_cursor = parse_cursor(None)
        assert state == GuardState()
        assert user_cursor is None

    def test_plain_string_cursor(self):
        state, user_cursor = parse_cursor("my_offset_123")
        assert state == GuardState()
        assert user_cursor == "my_offset_123"

    def test_json_without_guard_key(self):
        raw = json.dumps({"offset": 42, "batch": "abc"})
        state, user_cursor = parse_cursor(raw)
        assert state == GuardState()
        assert user_cursor == raw

    def test_valid_guard_cursor(self):
        raw = json.dumps({
            "__sensor_guard": {"error_count": 3, "first_error_ts": 1000.0, "last_error_ts": 1010.0},
            "__user_cursor": "user_data",
        })
        state, user_cursor = parse_cursor(raw)
        assert state.error_count == 3
        assert state.first_error_ts == 1000.0
        assert state.last_error_ts == 1010.0
        assert user_cursor == "user_data"

    def test_guard_cursor_with_none_user(self):
        raw = json.dumps({
            "__sensor_guard": {"error_count": 1},
            "__user_cursor": None,
        })
        state, user_cursor = parse_cursor(raw)
        assert state.error_count == 1
        assert user_cursor is None


class TestBuildCursor:
    def test_roundtrip(self):
        state = GuardState(error_count=2, first_error_ts=100.0, last_error_ts=200.0)
        cursor = build_cursor(state, "my_cursor")
        parsed_state, parsed_user = parse_cursor(cursor)
        assert parsed_state == state
        assert parsed_user == "my_cursor"

    def test_roundtrip_none_user(self):
        state = GuardState()
        cursor = build_cursor(state, None)
        parsed_state, parsed_user = parse_cursor(cursor)
        assert parsed_state == state
        assert parsed_user is None


class TestIncrementError:
    def test_first_error(self):
        state = GuardState()
        new_state = increment_error(state)
        assert new_state.error_count == 1
        assert new_state.first_error_ts is not None
        assert new_state.last_error_ts is not None
        assert new_state.first_error_ts == new_state.last_error_ts

    def test_subsequent_error_preserves_first_ts(self):
        first_ts = 1000.0
        state = GuardState(error_count=2, first_error_ts=first_ts, last_error_ts=1005.0)
        new_state = increment_error(state)
        assert new_state.error_count == 3
        assert new_state.first_error_ts == first_ts
        assert new_state.last_error_ts > first_ts


class TestApplyReset:
    def test_full_reset(self):
        state = GuardState(error_count=5, first_error_ts=100.0, last_error_ts=200.0)
        result = apply_reset(state, ResetStrategy.FULL, decay_amount=1)
        assert result == GuardState()

    def test_decay_decrements(self):
        state = GuardState(error_count=4, first_error_ts=100.0, last_error_ts=200.0)
        result = apply_reset(state, ResetStrategy.DECAY, decay_amount=1)
        assert result.error_count == 3
        assert result.first_error_ts == 100.0

    def test_decay_to_zero_resets_fully(self):
        state = GuardState(error_count=1, first_error_ts=100.0, last_error_ts=200.0)
        result = apply_reset(state, ResetStrategy.DECAY, decay_amount=1)
        assert result == GuardState()

    def test_decay_larger_than_count(self):
        state = GuardState(error_count=2, first_error_ts=100.0, last_error_ts=200.0)
        result = apply_reset(state, ResetStrategy.DECAY, decay_amount=5)
        assert result == GuardState()

    def test_decay_on_zero_count(self):
        state = GuardState()
        result = apply_reset(state, ResetStrategy.DECAY, decay_amount=1)
        assert result == GuardState()


class TestShouldRaise:
    def test_count_below_threshold(self):
        state = GuardState(error_count=3)
        assert not should_raise(state, threshold=5, window_minutes=None)

    def test_count_at_threshold(self):
        state = GuardState(error_count=5)
        assert not should_raise(state, threshold=5, window_minutes=None)

    def test_count_above_threshold(self):
        state = GuardState(error_count=6)
        assert should_raise(state, threshold=5, window_minutes=None)

    def test_time_window_within_window_above_threshold(self):
        now = time.time()
        state = GuardState(error_count=6, first_error_ts=now - 60)
        assert should_raise(state, threshold=5, window_minutes=10)

    def test_time_window_outside_window(self):
        now = time.time()
        state = GuardState(error_count=6, first_error_ts=now - 700)
        with patch("dagster_sensor_guard.state.time") as mock_time:
            mock_time.time.return_value = now
            result = should_raise(state, threshold=5, window_minutes=10)
        assert not result
