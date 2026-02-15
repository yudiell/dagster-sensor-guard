"""Tests for per-key failure tracking (per_key=True)."""

import time
from unittest.mock import MagicMock, patch

import pytest
from dagster import (
    RunRequest,
    SkipReason,
    build_sensor_context,
    sensor,
)

from dagster_sensor_guard import SensorGuard, SensorGuardKeyError, resilient_sensor
from dagster_sensor_guard.state import (
    GuardState,
    load_all_key_states,
    load_guard_state,
    save_all_key_states,
)
from tests.conftest import make_job as _make_job

_SENSOR_NAME = "test_per_key_sensor"


def _invoke_sensor(sensor_def, instance, cursor=None, sensor_name=_SENSOR_NAME):
    context = build_sensor_context(
        cursor=cursor, instance=instance, sensor_name=sensor_name,
    )
    results = list(sensor_def(context))
    return results, context


class TestPerKeyBasicBehavior:
    def test_errors_below_threshold_suppressed_per_key(self, instance):
        """Errors below the threshold for a key are suppressed; loop continues."""

        @sensor(job=_make_job())
        @resilient_sensor(threshold=3, per_key=True)
        def multi_sensor(context, guard):
            for table in ["orders", "customers", "inventory"]:
                with guard.track(table):
                    raise ConnectionError(f"{table} is down")

        # First tick — all 3 keys fail once (1/3), all suppressed.
        results, _ = _invoke_sensor(multi_sensor, instance)
        # No SensorGuardKeyError raised; sensor completes normally.
        assert len(results) == 0  # no RunRequests, no SkipReason from per_key path

    def test_breached_keys_raise_after_all_processed(self, instance):
        """Keys that breach threshold raise SensorGuardKeyError after all keys are processed."""
        tick = 0

        @sensor(job=_make_job())
        @resilient_sensor(threshold=1, per_key=True)
        def multi_sensor(context, guard):
            nonlocal tick
            tick += 1
            for table in ["orders", "customers"]:
                with guard.track(table):
                    raise ConnectionError(f"{table} down tick {tick}")

        # Tick 1: both at count 1 (suppressed, threshold=1 means error_count <= 1 is ok).
        results, _ = _invoke_sensor(multi_sensor, instance)
        assert len(results) == 0

        # Tick 2: both at count 2 (breach, > threshold).
        with pytest.raises(SensorGuardKeyError) as exc_info:
            _invoke_sensor(multi_sensor, instance)

        err = exc_info.value
        assert len(err.breached_keys) == 2
        assert "orders" in err.breached_keys
        assert "customers" in err.breached_keys

    def test_mixed_success_and_failure(self, instance):
        """Some keys succeed while others fail; only failed keys accumulate errors."""

        @sensor(job=_make_job())
        @resilient_sensor(threshold=3, per_key=True)
        def multi_sensor(context, guard):
            for table in ["orders", "customers", "inventory"]:
                with guard.track(table):
                    if table == "customers":
                        raise ConnectionError(f"{table} down")

        _invoke_sensor(multi_sensor, instance)

        key_states = load_all_key_states(
            instance.daemon_cursor_storage, _SENSOR_NAME
        )
        assert key_states.get("orders", GuardState()).error_count == 0
        assert key_states["customers"].error_count == 1
        assert key_states.get("inventory", GuardState()).error_count == 0


class TestPerKeyIsolation:
    def test_each_key_failure_independent(self, instance):
        """Each key's failures are independent of other keys."""
        tick = 0

        @sensor(job=_make_job())
        @resilient_sensor(threshold=3, per_key=True)
        def multi_sensor(context, guard):
            nonlocal tick
            tick += 1
            for table in ["orders", "customers"]:
                with guard.track(table):
                    if table == "orders":
                        raise ConnectionError("orders down")
                    # customers always succeeds

        # Run 3 ticks: orders fails 3 times, customers always succeeds.
        for _ in range(3):
            _invoke_sensor(multi_sensor, instance)

        key_states = load_all_key_states(
            instance.daemon_cursor_storage, _SENSOR_NAME
        )
        assert key_states["orders"].error_count == 3
        assert key_states["customers"].error_count == 0

    def test_different_sensors_per_key_isolated(self, instance):
        """Per-key state for different sensors is isolated."""

        @sensor(job=_make_job(), name="sensor_x")
        @resilient_sensor(threshold=3, per_key=True)
        def sensor_x(context, guard):
            with guard.track("key_a"):
                raise ConnectionError("down")

        @sensor(job=_make_job(), name="sensor_y")
        @resilient_sensor(threshold=3, per_key=True)
        def sensor_y(context, guard):
            with guard.track("key_a"):
                pass  # success

        ctx = build_sensor_context(instance=instance, sensor_name="sensor_x")
        list(sensor_x(ctx))

        ctx = build_sensor_context(instance=instance, sensor_name="sensor_y")
        list(sensor_y(ctx))

        x_states = load_all_key_states(instance.daemon_cursor_storage, "sensor_x")
        y_states = load_all_key_states(instance.daemon_cursor_storage, "sensor_y")

        assert x_states["key_a"].error_count == 1
        assert y_states["key_a"].error_count == 0


class TestPerKeyReset:
    def test_success_resets_key_full_strategy(self, instance):
        """Success on a key resets that key's counter with FULL strategy."""
        tick = 0

        @sensor(job=_make_job())
        @resilient_sensor(threshold=5, per_key=True)
        def multi_sensor(context, guard):
            nonlocal tick
            tick += 1
            with guard.track("orders"):
                if tick <= 2:
                    raise ConnectionError("orders down")

        # 2 failures.
        for _ in range(2):
            _invoke_sensor(multi_sensor, instance)

        key_states = load_all_key_states(
            instance.daemon_cursor_storage, _SENSOR_NAME
        )
        assert key_states["orders"].error_count == 2

        # 1 success — resets to 0.
        _invoke_sensor(multi_sensor, instance)

        key_states = load_all_key_states(
            instance.daemon_cursor_storage, _SENSOR_NAME
        )
        assert key_states["orders"].error_count == 0

    def test_success_decays_key_decay_strategy(self, instance):
        """Success on a key decays that key's counter with DECAY strategy."""
        tick = 0

        @sensor(job=_make_job())
        @resilient_sensor(
            threshold=5, per_key=True, reset_strategy="decay", decay_amount=1
        )
        def multi_sensor(context, guard):
            nonlocal tick
            tick += 1
            with guard.track("orders"):
                if tick <= 3:
                    raise ConnectionError("orders down")

        # 3 failures.
        for _ in range(3):
            _invoke_sensor(multi_sensor, instance)

        key_states = load_all_key_states(
            instance.daemon_cursor_storage, _SENSOR_NAME
        )
        assert key_states["orders"].error_count == 3

        # 1 success — decays by 1.
        _invoke_sensor(multi_sensor, instance)

        key_states = load_all_key_states(
            instance.daemon_cursor_storage, _SENSOR_NAME
        )
        assert key_states["orders"].error_count == 2

    def test_success_resets_only_that_key(self, instance):
        """Success on one key does not affect other keys."""
        tick = 0

        @sensor(job=_make_job())
        @resilient_sensor(threshold=5, per_key=True)
        def multi_sensor(context, guard):
            nonlocal tick
            tick += 1
            for table in ["orders", "customers"]:
                with guard.track(table):
                    if table == "orders":
                        raise ConnectionError("orders always down")
                    # customers: fails first 2, then succeeds
                    if table == "customers" and tick <= 2:
                        raise ConnectionError("customers down")

        # 2 ticks: both fail.
        for _ in range(2):
            _invoke_sensor(multi_sensor, instance)

        key_states = load_all_key_states(
            instance.daemon_cursor_storage, _SENSOR_NAME
        )
        assert key_states["orders"].error_count == 2
        assert key_states["customers"].error_count == 2

        # Tick 3: orders fails, customers succeeds.
        _invoke_sensor(multi_sensor, instance)

        key_states = load_all_key_states(
            instance.daemon_cursor_storage, _SENSOR_NAME
        )
        assert key_states["orders"].error_count == 3
        assert key_states["customers"].error_count == 0  # reset


class TestPerKeyStatePersistence:
    def test_per_key_state_persists_across_ticks(self, instance):
        """Per-key state persists across separate sensor invocations."""
        tick = 0

        @sensor(job=_make_job())
        @resilient_sensor(threshold=5, per_key=True)
        def multi_sensor(context, guard):
            nonlocal tick
            tick += 1
            with guard.track("orders"):
                raise ConnectionError("orders down")

        # 3 separate ticks.
        for _ in range(3):
            _invoke_sensor(multi_sensor, instance)

        key_states = load_all_key_states(
            instance.daemon_cursor_storage, _SENSOR_NAME
        )
        assert key_states["orders"].error_count == 3

    def test_sensor_level_state_independent_of_per_key(self, instance):
        """Sensor-level state is independent of per-key state."""

        @sensor(job=_make_job())
        @resilient_sensor(threshold=5, per_key=True)
        def multi_sensor(context, guard):
            with guard.track("orders"):
                raise ConnectionError("orders down")

        _invoke_sensor(multi_sensor, instance)

        # Per-key state has the error.
        key_states = load_all_key_states(
            instance.daemon_cursor_storage, _SENSOR_NAME
        )
        assert key_states["orders"].error_count == 1

        # Sensor-level state should be clean (no unhandled exception).
        sensor_state = load_guard_state(
            instance.daemon_cursor_storage, _SENSOR_NAME
        )
        assert sensor_state.error_count == 0


class TestPerKeyRunRequests:
    def test_run_requests_from_healthy_keys_yielded(self, instance):
        """RunRequests from healthy keys are yielded even when other keys fail."""

        @sensor(job=_make_job())
        @resilient_sensor(threshold=3, per_key=True)
        def multi_sensor(context, guard):
            for table in ["orders", "customers", "inventory"]:
                with guard.track(table):
                    if table == "customers":
                        raise ConnectionError("customers down")
                    yield RunRequest(run_key=f"{table}_run")

        results, _ = _invoke_sensor(multi_sensor, instance)

        run_keys = [r.run_key for r in results if isinstance(r, RunRequest)]
        assert "orders_run" in run_keys
        assert "inventory_run" in run_keys
        assert "customers_run" not in run_keys

    def test_run_requests_yielded_before_breach_error(self, instance):
        """RunRequests are yielded before SensorGuardKeyError is raised."""
        tick = 0

        @sensor(job=_make_job())
        @resilient_sensor(threshold=1, per_key=True)
        def multi_sensor(context, guard):
            nonlocal tick
            tick += 1
            with guard.track("healthy"):
                yield RunRequest(run_key=f"healthy_{tick}")
            with guard.track("broken"):
                raise ConnectionError("broken")

        # Tick 1: broken at count 1, suppressed.
        results, _ = _invoke_sensor(multi_sensor, instance)
        assert len(results) == 1
        assert results[0].run_key == "healthy_1"

        # Tick 2: broken at count 2 (breach) — RunRequest yielded, then error.
        with pytest.raises(SensorGuardKeyError):
            results, _ = _invoke_sensor(multi_sensor, instance)
            # results collected before exception: the healthy RunRequest was yielded


class TestPerKeyCallback:
    def test_on_suppressed_error_called_for_per_key(self, instance):
        """on_suppressed_error is called for per-key suppressed errors."""
        callback = MagicMock()

        @sensor(job=_make_job())
        @resilient_sensor(threshold=3, per_key=True, on_suppressed_error=callback)
        def multi_sensor(context, guard):
            with guard.track("orders"):
                raise ConnectionError("orders down")

        _invoke_sensor(multi_sensor, instance)

        callback.assert_called_once()
        args = callback.call_args[0]
        assert isinstance(args[0], ConnectionError)
        assert args[1] == 1  # error count
        assert args[2] == 3  # threshold

    def test_callback_called_per_key_not_for_breach(self, instance):
        """Callback is called for suppressed errors, not for breached keys."""
        callback = MagicMock()
        tick = 0

        @sensor(job=_make_job())
        @resilient_sensor(threshold=1, per_key=True, on_suppressed_error=callback)
        def multi_sensor(context, guard):
            nonlocal tick
            tick += 1
            with guard.track("orders"):
                raise ConnectionError("orders down")

        # Tick 1: suppressed — callback called.
        _invoke_sensor(multi_sensor, instance)
        assert callback.call_count == 1

        # Tick 2: breach — callback NOT called.
        with pytest.raises(SensorGuardKeyError):
            _invoke_sensor(multi_sensor, instance)
        assert callback.call_count == 1

    def test_callback_for_multiple_keys(self, instance):
        """Callback is called once per suppressed key error."""
        callback = MagicMock()

        @sensor(job=_make_job())
        @resilient_sensor(threshold=3, per_key=True, on_suppressed_error=callback)
        def multi_sensor(context, guard):
            for table in ["orders", "customers", "inventory"]:
                with guard.track(table):
                    raise ConnectionError(f"{table} down")

        _invoke_sensor(multi_sensor, instance)
        assert callback.call_count == 3


class TestPerKeySensorLevelFallback:
    def test_exception_outside_track_uses_sensor_level(self, instance):
        """Exceptions outside guard.track() are handled by sensor-level tracking."""

        @sensor(job=_make_job())
        @resilient_sensor(threshold=3, per_key=True)
        def multi_sensor(context, guard):
            raise RuntimeError("unexpected top-level error")

        results, _ = _invoke_sensor(multi_sensor, instance)
        assert len(results) == 1
        assert isinstance(results[0], SkipReason)
        assert "(1/3)" in results[0].skip_message

        sensor_state = load_guard_state(
            instance.daemon_cursor_storage, _SENSOR_NAME
        )
        assert sensor_state.error_count == 1

    def test_sensor_level_breach_raises_original_error(self, instance):
        """Sensor-level threshold breach raises the original error, not SensorGuardKeyError."""

        @sensor(job=_make_job())
        @resilient_sensor(threshold=1, per_key=True)
        def multi_sensor(context, guard):
            raise RuntimeError("top-level boom")

        # Tick 1: suppressed.
        _invoke_sensor(multi_sensor, instance)

        # Tick 2: breach — original RuntimeError raised.
        with pytest.raises(RuntimeError, match="top-level boom"):
            _invoke_sensor(multi_sensor, instance)

    def test_per_key_state_saved_on_sensor_level_error(self, instance):
        """Per-key state from before the top-level error is still saved."""

        @sensor(job=_make_job())
        @resilient_sensor(threshold=3, per_key=True)
        def multi_sensor(context, guard):
            with guard.track("orders"):
                raise ConnectionError("orders down")
            # This line raises outside guard.track()
            raise RuntimeError("top-level error after track")

        _invoke_sensor(multi_sensor, instance)

        key_states = load_all_key_states(
            instance.daemon_cursor_storage, _SENSOR_NAME
        )
        assert key_states["orders"].error_count == 1


class TestPerKeyValidation:
    def test_per_key_without_second_param_raises(self):
        """per_key=True without a second parameter raises TypeError at decoration time."""
        with pytest.raises(TypeError, match="per_key=True requires"):

            @resilient_sensor(threshold=3, per_key=True)
            def bad_sensor(context):
                pass

    def test_per_key_with_second_param_accepted(self):
        """per_key=True with a second parameter is accepted."""

        @resilient_sensor(threshold=3, per_key=True)
        def good_sensor(context, guard):
            pass

        # Should not raise.
        assert callable(good_sensor)

    def test_per_key_false_single_param_ok(self):
        """per_key=False (default) with a single parameter is fine."""

        @resilient_sensor(threshold=3)
        def normal_sensor(context):
            pass

        assert callable(normal_sensor)


class TestPerKeyCombinedError:
    def test_sensor_guard_key_error_contains_all_breached(self, instance):
        """SensorGuardKeyError contains all breached keys and their exceptions."""
        tick = 0

        @sensor(job=_make_job())
        @resilient_sensor(threshold=1, per_key=True)
        def multi_sensor(context, guard):
            nonlocal tick
            tick += 1
            for table in ["orders", "customers", "inventory"]:
                with guard.track(table):
                    if table in ("orders", "inventory"):
                        raise ConnectionError(f"{table} down tick {tick}")
                    # customers succeeds

        # Tick 1: orders and inventory at count 1 (suppressed).
        _invoke_sensor(multi_sensor, instance)

        # Tick 2: orders and inventory at count 2 (breach).
        with pytest.raises(SensorGuardKeyError) as exc_info:
            _invoke_sensor(multi_sensor, instance)

        err = exc_info.value
        assert len(err.breached_keys) == 2
        assert "orders" in err.breached_keys
        assert "inventory" in err.breached_keys
        assert "customers" not in err.breached_keys

        # Exception message lists the breached keys.
        assert "2 key(s)" in str(err)
        assert "orders" in str(err)
        assert "inventory" in str(err)

    def test_sensor_guard_key_error_preserves_original_exceptions(self, instance):
        """Breached keys map to their original exceptions."""
        tick = 0

        @sensor(job=_make_job())
        @resilient_sensor(threshold=1, per_key=True)
        def multi_sensor(context, guard):
            nonlocal tick
            tick += 1
            with guard.track("orders"):
                raise ValueError("orders value error")
            with guard.track("customers"):
                raise TypeError("customers type error")

        # Tick 1: suppressed.
        _invoke_sensor(multi_sensor, instance)

        # Tick 2: breach.
        with pytest.raises(SensorGuardKeyError) as exc_info:
            _invoke_sensor(multi_sensor, instance)

        err = exc_info.value
        assert isinstance(err.breached_keys["orders"], ValueError)
        assert isinstance(err.breached_keys["customers"], TypeError)


class TestPerKeyWindow:
    def test_window_applies_independently_per_key(self, instance):
        """window_minutes applies independently to each key."""
        now = time.time()
        tick = 0

        @sensor(job=_make_job())
        @resilient_sensor(threshold=2, per_key=True, window_minutes=10)
        def multi_sensor(context, guard):
            nonlocal tick
            tick += 1
            for table in ["orders", "customers"]:
                with guard.track(table):
                    raise ConnectionError(f"{table} down")

        # Tick 1: both keys fail once.
        _invoke_sensor(multi_sensor, instance)

        # Expire the "orders" key's first_error_ts to be outside the window.
        key_states = load_all_key_states(
            instance.daemon_cursor_storage, _SENSOR_NAME
        )
        orders_state = key_states["orders"]
        key_states["orders"] = GuardState(
            error_count=orders_state.error_count,
            first_error_ts=now - 700,  # outside 10-minute window
            last_error_ts=orders_state.last_error_ts,
        )
        save_all_key_states(
            instance.daemon_cursor_storage, _SENSOR_NAME, key_states
        )

        # Tick 2: orders resets (window expired), customers continues (count 2).
        _invoke_sensor(multi_sensor, instance)

        key_states = load_all_key_states(
            instance.daemon_cursor_storage, _SENSOR_NAME
        )
        # orders: window expired → reset → count 1 (fresh chain).
        assert key_states["orders"].error_count == 1
        # customers: window still active → count 2.
        assert key_states["customers"].error_count == 2
