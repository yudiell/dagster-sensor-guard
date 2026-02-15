"""Tests for state management and KVS storage."""

import time
from unittest.mock import patch

from dagster import DagsterInstance

from dagster_sensor_guard.state import (
    GuardState,
    apply_reset,
    increment_error,
    kvs_key,
    kvs_keys_key,
    load_all_key_states,
    load_guard_state,
    save_all_key_states,
    save_guard_state,
    should_raise,
)
from dagster_sensor_guard.types import ResetStrategy


class TestKvsKey:
    def test_key_format(self):
        assert kvs_key("my_sensor") == "dagster_sensor_guard:my_sensor"

    def test_different_sensors_get_different_keys(self):
        assert kvs_key("sensor_a") != kvs_key("sensor_b")


class TestLoadGuardState:
    def test_returns_default_when_no_state(self, instance):
        state = load_guard_state(instance.daemon_cursor_storage, "nonexistent")
        assert state == GuardState()

    def test_roundtrip_with_save(self, instance):
        original = GuardState(error_count=3, first_error_ts=100.0, last_error_ts=200.0)
        save_guard_state(instance.daemon_cursor_storage, "test_sensor", original)
        loaded = load_guard_state(instance.daemon_cursor_storage, "test_sensor")
        assert loaded == original

    def test_returns_default_on_corrupt_data(self, instance):
        key = kvs_key("corrupt_sensor")
        instance.daemon_cursor_storage.set_cursor_values({key: "not-valid-json"})
        state = load_guard_state(instance.daemon_cursor_storage, "corrupt_sensor")
        assert state == GuardState()

    def test_sensors_are_isolated(self, instance):
        save_guard_state(
            instance.daemon_cursor_storage,
            "sensor_a",
            GuardState(error_count=5),
        )
        state_b = load_guard_state(instance.daemon_cursor_storage, "sensor_b")
        assert state_b == GuardState()


class TestSaveGuardState:
    def test_overwrites_previous_state(self, instance):
        storage = instance.daemon_cursor_storage
        save_guard_state(storage, "test", GuardState(error_count=1))
        save_guard_state(storage, "test", GuardState(error_count=5))
        loaded = load_guard_state(storage, "test")
        assert loaded.error_count == 5



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
        assert not should_raise(state, threshold=5)

    def test_count_at_threshold(self):
        state = GuardState(error_count=5)
        assert not should_raise(state, threshold=5)

    def test_count_above_threshold(self):
        state = GuardState(error_count=6)
        assert should_raise(state, threshold=5)


class TestIncrementErrorWithWindow:
    def test_window_resets_expired_chain(self):
        now = time.time()
        state = GuardState(error_count=5, first_error_ts=now - 700, last_error_ts=now - 60)
        with patch("dagster_sensor_guard.state.time") as mock_time:
            mock_time.time.return_value = now
            new_state = increment_error(state, window_minutes=10)
        # Chain expired — should reset to count 1 (fresh chain).
        assert new_state.error_count == 1

    def test_window_preserves_active_chain(self):
        now = time.time()
        state = GuardState(error_count=5, first_error_ts=now - 60, last_error_ts=now - 10)
        with patch("dagster_sensor_guard.state.time") as mock_time:
            mock_time.time.return_value = now
            new_state = increment_error(state, window_minutes=10)
        # Chain still active — should increment normally.
        assert new_state.error_count == 6


class TestKvsKeysKey:
    def test_key_format(self):
        assert kvs_keys_key("my_sensor") == "dagster_sensor_guard:my_sensor:keys"

    def test_different_from_sensor_key(self):
        assert kvs_keys_key("my_sensor") != kvs_key("my_sensor")


class TestLoadAllKeyStates:
    def test_empty_returns_empty_dict(self, instance):
        result = load_all_key_states(instance.daemon_cursor_storage, "nonexistent")
        assert result == {}

    def test_roundtrip(self, instance):
        original = {
            "orders": GuardState(error_count=2, first_error_ts=100.0, last_error_ts=200.0),
            "customers": GuardState(error_count=1, first_error_ts=150.0, last_error_ts=150.0),
        }
        save_all_key_states(instance.daemon_cursor_storage, "test_sensor", original)
        loaded = load_all_key_states(instance.daemon_cursor_storage, "test_sensor")
        assert loaded == original

    def test_corrupt_data_returns_empty(self, instance):
        key = kvs_keys_key("corrupt_sensor")
        instance.daemon_cursor_storage.set_cursor_values({key: "not-valid-json"})
        result = load_all_key_states(instance.daemon_cursor_storage, "corrupt_sensor")
        assert result == {}

    def test_sensor_isolation(self, instance):
        save_all_key_states(
            instance.daemon_cursor_storage,
            "sensor_a",
            {"key1": GuardState(error_count=5)},
        )
        result = load_all_key_states(instance.daemon_cursor_storage, "sensor_b")
        assert result == {}


class TestSaveAllKeyStates:
    def test_overwrites_previous_state(self, instance):
        storage = instance.daemon_cursor_storage
        save_all_key_states(storage, "test", {"k": GuardState(error_count=1)})
        save_all_key_states(storage, "test", {"k": GuardState(error_count=5)})
        loaded = load_all_key_states(storage, "test")
        assert loaded["k"].error_count == 5

    def test_stores_multiple_keys(self, instance):
        storage = instance.daemon_cursor_storage
        states = {
            "a": GuardState(error_count=1),
            "b": GuardState(error_count=2),
            "c": GuardState(error_count=3),
        }
        save_all_key_states(storage, "test", states)
        loaded = load_all_key_states(storage, "test")
        assert len(loaded) == 3
        assert loaded["a"].error_count == 1
        assert loaded["b"].error_count == 2
        assert loaded["c"].error_count == 3
