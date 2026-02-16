"""Tests for the @resilient_sensor decorator."""

import logging
from unittest.mock import MagicMock

import pytest
from dagster import (
    AddDynamicPartitionsRequest,
    AssetKey,
    AssetMaterialization,
    RunRequest,
    SensorResult,
    SkipReason,
    build_sensor_context,
    sensor,
)

from dagster_sensor_guard import resilient_sensor
from dagster_sensor_guard.state import GuardState, load_guard_state, save_guard_state
from tests.conftest import make_job as _make_job

_SENSOR_NAME = "test_sensor"


def _invoke_sensor(sensor_def, instance, cursor=None, sensor_name=_SENSOR_NAME):
    """Invoke a sensor and collect all yielded results.

    Returns (results, context).
    """
    context = build_sensor_context(
        cursor=cursor, instance=instance, sensor_name=sensor_name,
    )
    results = list(sensor_def(context))
    return results, context


class TestCountThreshold:
    def test_errors_below_threshold_are_suppressed(self, instance):
        @sensor(job=_make_job())
        @resilient_sensor(threshold=3)
        def failing_sensor(context):
            raise ConnectionError("timeout")

        cursor = None
        for i in range(3):
            context = build_sensor_context(
                cursor=cursor, instance=instance, sensor_name=_SENSOR_NAME,
            )
            results = list(failing_sensor(context))
            cursor = context.cursor
            assert len(results) == 1
            assert isinstance(results[0], SkipReason)
            assert "Suppressed transient error" in results[0].skip_message
            assert f"({i + 1}/3)" in results[0].skip_message

    def test_error_at_threshold_plus_one_raises(self, instance):
        @sensor(job=_make_job())
        @resilient_sensor(threshold=2)
        def failing_sensor(context):
            raise ConnectionError("timeout")

        cursor = None
        # First 2 are suppressed.
        for _ in range(2):
            context = build_sensor_context(
                cursor=cursor, instance=instance, sensor_name=_SENSOR_NAME,
            )
            list(failing_sensor(context))
            cursor = context.cursor

        # Third should raise.
        context = build_sensor_context(
            cursor=cursor, instance=instance, sensor_name=_SENSOR_NAME,
        )
        with pytest.raises(ConnectionError):
            list(failing_sensor(context))

    def test_error_on_first_tick(self, instance):
        @sensor(job=_make_job())
        @resilient_sensor(threshold=3)
        def failing_sensor(context):
            raise RuntimeError("boom")

        results, _ = _invoke_sensor(failing_sensor, instance)
        assert len(results) == 1
        assert isinstance(results[0], SkipReason)
        assert "(1/3)" in results[0].skip_message


class TestTimeWindowThreshold:
    def test_errors_within_window_accumulate(self, instance):
        @sensor(job=_make_job())
        @resilient_sensor(threshold=2, window_minutes=10)
        def failing_sensor(context):
            raise ConnectionError("timeout")

        cursor = None
        for _ in range(2):
            context = build_sensor_context(
                cursor=cursor, instance=instance, sensor_name=_SENSOR_NAME,
            )
            list(failing_sensor(context))
            cursor = context.cursor

        # Third should raise (within window).
        context = build_sensor_context(
            cursor=cursor, instance=instance, sensor_name=_SENSOR_NAME,
        )
        with pytest.raises(ConnectionError):
            list(failing_sensor(context))

    def test_errors_outside_window_reset_counter(self, instance):
        @sensor(job=_make_job())
        @resilient_sensor(threshold=2, window_minutes=10)
        def failing_sensor(context):
            raise ConnectionError("timeout")

        # Simulate 2 errors.
        cursor = None
        for _ in range(2):
            context = build_sensor_context(
                cursor=cursor, instance=instance, sensor_name=_SENSOR_NAME,
            )
            list(failing_sensor(context))
            cursor = context.cursor

        # Manipulate KVS state to make first_error_ts old (outside window).
        state = load_guard_state(instance.daemon_cursor_storage, _SENSOR_NAME)
        expired_state = GuardState(
            error_count=state.error_count,
            first_error_ts=1000.0,  # very old
            last_error_ts=state.last_error_ts,
        )
        save_guard_state(instance.daemon_cursor_storage, _SENSOR_NAME, expired_state)

        # Next error should be suppressed (counter reset due to window expiry).
        context = build_sensor_context(
            cursor=cursor, instance=instance, sensor_name=_SENSOR_NAME,
        )
        results = list(failing_sensor(context))
        assert len(results) == 1
        assert isinstance(results[0], SkipReason)
        assert "(1/2)" in results[0].skip_message


class TestFullReset:
    def test_success_clears_error_count(self, instance):
        tick = 0

        @sensor(job=_make_job())
        @resilient_sensor(threshold=5)
        def flapping_sensor(context):
            nonlocal tick
            tick += 1
            if tick <= 3:
                raise ConnectionError("timeout")
            yield SkipReason("OK")

        cursor = None
        # 3 errors.
        for _ in range(3):
            context = build_sensor_context(
                cursor=cursor, instance=instance, sensor_name=_SENSOR_NAME,
            )
            list(flapping_sensor(context))
            cursor = context.cursor

        state = load_guard_state(instance.daemon_cursor_storage, _SENSOR_NAME)
        assert state.error_count == 3

        # One success.
        context = build_sensor_context(
            cursor=cursor, instance=instance, sensor_name=_SENSOR_NAME,
        )
        list(flapping_sensor(context))

        state = load_guard_state(instance.daemon_cursor_storage, _SENSOR_NAME)
        assert state.error_count == 0


class TestDecayReset:
    def test_success_decrements_by_decay_amount(self, instance):
        tick = 0

        @sensor(job=_make_job())
        @resilient_sensor(threshold=5, reset_strategy="decay", decay_amount=2)
        def flapping_sensor(context):
            nonlocal tick
            tick += 1
            if tick <= 4:
                raise ConnectionError("timeout")
            yield SkipReason("OK")

        cursor = None
        # 4 errors.
        for _ in range(4):
            context = build_sensor_context(
                cursor=cursor, instance=instance, sensor_name=_SENSOR_NAME,
            )
            list(flapping_sensor(context))
            cursor = context.cursor

        state = load_guard_state(instance.daemon_cursor_storage, _SENSOR_NAME)
        assert state.error_count == 4

        # One success with decay_amount=2.
        context = build_sensor_context(
            cursor=cursor, instance=instance, sensor_name=_SENSOR_NAME,
        )
        list(flapping_sensor(context))

        state = load_guard_state(instance.daemon_cursor_storage, _SENSOR_NAME)
        assert state.error_count == 2

    def test_decay_accumulates_residual_across_rounds(self, instance):
        """Decay only subtracts 1 per success, so residual carries forward."""
        # F, S, F, F, S
        script = [False, True, False, False, True]
        tick = 0

        @sensor(job=_make_job())
        @resilient_sensor(threshold=5, reset_strategy="decay", decay_amount=1)
        def flapping_sensor(context):
            nonlocal tick
            idx = tick
            tick += 1
            if not script[idx]:
                raise ConnectionError("timeout")
            yield SkipReason("OK")

        cursor = None
        counts = []
        for _ in script:
            context = build_sensor_context(
                cursor=cursor, instance=instance, sensor_name=_SENSOR_NAME,
            )
            list(flapping_sensor(context))
            cursor = context.cursor
            state = load_guard_state(instance.daemon_cursor_storage, _SENSOR_NAME)
            counts.append(state.error_count)

        assert counts == [1, 0, 1, 2, 1]

    def test_decay_preserves_count_after_breach(self, instance):
        """With decay strategy, breach preserves the count so subsequent
        failures continue to breach until successes decay it down."""
        # F F F F(breach) F(breach) S F(breach) S S
        script = [False, False, False, False, False, True, False, True, True]
        tick = 0

        @sensor(job=_make_job())
        @resilient_sensor(threshold=3, reset_strategy="decay", decay_amount=1)
        def decay_sensor(context):
            nonlocal tick
            idx = tick
            tick += 1
            if not script[idx]:
                raise ConnectionError("down")
            yield SkipReason("OK")

        cursor = None
        counts = []
        for succeeds in script:
            context = build_sensor_context(
                cursor=cursor, instance=instance, sensor_name=_SENSOR_NAME,
            )
            try:
                list(decay_sensor(context))
            except ConnectionError:
                pass
            cursor = context.cursor
            state = load_guard_state(instance.daemon_cursor_storage, _SENSOR_NAME)
            counts.append(state.error_count)

        # Tick 1: fail → 1 (suppressed)
        # Tick 2: fail → 2 (suppressed)
        # Tick 3: fail → 3 (suppressed)
        # Tick 4: fail → 4 (breach, count preserved)
        # Tick 5: fail → 5 (breach again, count preserved)
        # Tick 6: succeed → decay 5→4
        # Tick 7: fail → 5 (breach, count preserved)
        # Tick 8: succeed → decay 5→4
        # Tick 9: succeed → decay 4→3
        assert counts == [1, 2, 3, 4, 5, 4, 5, 4, 3]


class TestRecoveryAfterBreach:
    def test_success_after_threshold_breach_resets_counter(self, instance):
        """After threshold is breached and error raised, a subsequent success
        should reset the counter back to 0 (full reset strategy)."""
        script = [False, False, False, False, True]
        tick = 0

        @sensor(job=_make_job())
        @resilient_sensor(threshold=3)
        def recovering_sensor(context):
            nonlocal tick
            idx = tick
            tick += 1
            if not script[idx]:
                raise ConnectionError("down")
            yield SkipReason("OK")

        cursor = None

        # First 3 errors are suppressed.
        for i in range(3):
            context = build_sensor_context(
                cursor=cursor, instance=instance, sensor_name=_SENSOR_NAME,
            )
            results = list(recovering_sensor(context))
            cursor = context.cursor
            assert isinstance(results[0], SkipReason)
            assert f"({i + 1}/3)" in results[0].skip_message

        # 4th error breaches threshold.
        context = build_sensor_context(
            cursor=cursor, instance=instance, sensor_name=_SENSOR_NAME,
        )
        with pytest.raises(ConnectionError):
            list(recovering_sensor(context))

        # 5th tick succeeds — counter should reset.
        context = build_sensor_context(
            cursor=cursor, instance=instance, sensor_name=_SENSOR_NAME,
        )
        results = list(recovering_sensor(context))
        cursor = context.cursor

        assert len(results) == 1
        assert isinstance(results[0], SkipReason)
        assert results[0].skip_message == "OK"

        state = load_guard_state(instance.daemon_cursor_storage, _SENSOR_NAME)
        assert state.error_count == 0

    def test_error_after_recovery_starts_fresh_count(self, instance):
        """After recovering from a breach, new errors should start from (1/N)."""
        script = [False, False, False, False, True, False]
        tick = 0

        @sensor(job=_make_job())
        @resilient_sensor(threshold=3)
        def recovering_sensor(context):
            nonlocal tick
            idx = tick
            tick += 1
            if not script[idx]:
                raise ConnectionError("down")
            yield SkipReason("OK")

        cursor = None

        # 3 suppressed + 1 breach.
        for _ in range(3):
            context = build_sensor_context(
                cursor=cursor, instance=instance, sensor_name=_SENSOR_NAME,
            )
            list(recovering_sensor(context))
            cursor = context.cursor

        context = build_sensor_context(
            cursor=cursor, instance=instance, sensor_name=_SENSOR_NAME,
        )
        with pytest.raises(ConnectionError):
            list(recovering_sensor(context))

        # Tick 5: success, resets counter.
        context = build_sensor_context(
            cursor=cursor, instance=instance, sensor_name=_SENSOR_NAME,
        )
        list(recovering_sensor(context))
        cursor = context.cursor

        # Tick 6: fail again — should be (1/3), proving counter reset.
        context = build_sensor_context(
            cursor=cursor, instance=instance, sensor_name=_SENSOR_NAME,
        )
        results = list(recovering_sensor(context))

        assert isinstance(results[0], SkipReason)
        assert "(1/3)" in results[0].skip_message

        state = load_guard_state(instance.daemon_cursor_storage, _SENSOR_NAME)
        assert state.error_count == 1


class TestCursorLeakAfterBreach:
    def test_user_sees_own_cursor_not_json_after_breach(self, instance):
        """After a breach, the user's sensor should see their cursor value,
        not any guard state data."""
        observed_cursors = []
        tick = 0

        @sensor(job=_make_job())
        @resilient_sensor(threshold=2)
        def cursor_sensor(context):
            nonlocal tick
            observed_cursors.append(context.cursor)
            tick += 1
            if tick <= 3:
                context.update_cursor(str(tick * 10))
                raise ConnectionError("down")
            context.update_cursor(str(tick * 10))
            yield SkipReason("OK")

        cursor = None

        # 2 errors suppressed.
        for _ in range(2):
            context = build_sensor_context(
                cursor=cursor, instance=instance, sensor_name=_SENSOR_NAME,
            )
            list(cursor_sensor(context))
            cursor = context.cursor

        # 3rd error breaches.
        context = build_sensor_context(
            cursor=cursor, instance=instance, sensor_name=_SENSOR_NAME,
        )
        with pytest.raises(ConnectionError):
            list(cursor_sensor(context))
        cursor = context.cursor

        # 4th tick succeeds — user must see their cursor, not JSON.
        context = build_sensor_context(
            cursor=cursor, instance=instance, sensor_name=_SENSOR_NAME,
        )
        list(cursor_sensor(context))

        # Tick 1: None (no prior cursor)
        assert observed_cursors[0] is None
        # Tick 2: "10" (set by tick 1)
        assert observed_cursors[1] == "10"
        # Tick 3: "20" (set by tick 2)
        assert observed_cursors[2] == "20"
        # Tick 4 (after breach): "30" (set by tick 3)
        assert observed_cursors[3] == "30"


class TestRootCauseNeverRecovers:
    """With KVS storage, the 'never recovers' bug is eliminated because
    guard state persists independently of Dagster's cursor."""

    def test_kvs_persists_independently_of_tick_outcome(self, instance):
        """Guard state in KVS persists even when Dagster drops the cursor
        on a failed tick. This eliminates the cycling bug."""
        tick = 0

        @sensor(job=_make_job())
        @resilient_sensor(threshold=2)
        def cycling_sensor(context):
            nonlocal tick
            tick += 1
            if tick <= 4:
                raise ConnectionError("down")
            yield SkipReason("recovered")

        cursor = None

        # Ticks 1-2: suppressed errors.
        for _ in range(2):
            ctx = build_sensor_context(
                cursor=cursor, instance=instance, sensor_name=_SENSOR_NAME,
            )
            list(cycling_sensor(ctx))
            cursor = ctx.cursor

        # Tick 3: breach — even if Dagster drops cursor, KVS has reset state.
        ctx = build_sensor_context(
            cursor=cursor, instance=instance, sensor_name=_SENSOR_NAME,
        )
        with pytest.raises(ConnectionError):
            list(cycling_sensor(ctx))

        # KVS has reset state regardless of cursor.
        state = load_guard_state(instance.daemon_cursor_storage, _SENSOR_NAME)
        assert state.error_count == 0

        # Tick 4: still broken — fresh suppression starts.
        ctx = build_sensor_context(
            cursor=cursor, instance=instance, sensor_name=_SENSOR_NAME,
        )
        results = list(cycling_sensor(ctx))
        assert isinstance(results[0], SkipReason)
        assert "(1/2)" in results[0].skip_message

        # Tick 5: issue resolved — recovers.
        ctx = build_sensor_context(
            cursor=cursor, instance=instance, sensor_name=_SENSOR_NAME,
        )
        results = list(cycling_sensor(ctx))
        assert results[0].skip_message == "recovered"

        state = load_guard_state(instance.daemon_cursor_storage, _SENSOR_NAME)
        assert state.error_count == 0

    def test_fresh_retries_after_breach(self, instance):
        """After breach, the reset state in KVS gives fresh retries."""
        tick = 0

        @sensor(job=_make_job())
        @resilient_sensor(threshold=2)
        def retry_sensor(context):
            nonlocal tick
            tick += 1
            if tick <= 5:
                raise ConnectionError("down")
            yield SkipReason("recovered")

        cursor = None

        # Ticks 1-2: suppressed (1/2, 2/2).
        for _ in range(2):
            ctx = build_sensor_context(
                cursor=cursor, instance=instance, sensor_name=_SENSOR_NAME,
            )
            list(retry_sensor(ctx))
            cursor = ctx.cursor

        # Tick 3: breach. KVS resets.
        ctx = build_sensor_context(
            cursor=cursor, instance=instance, sensor_name=_SENSOR_NAME,
        )
        with pytest.raises(ConnectionError):
            list(retry_sensor(ctx))

        state = load_guard_state(instance.daemon_cursor_storage, _SENSOR_NAME)
        assert state.error_count == 0  # Reset, not stuck at 3

        # Ticks 4-5: fresh retries — suppressed as (1/2, 2/2).
        for expected_count in [1, 2]:
            ctx = build_sensor_context(
                cursor=cursor, instance=instance, sensor_name=_SENSOR_NAME,
            )
            results = list(retry_sensor(ctx))

            assert isinstance(results[0], SkipReason)
            assert f"({expected_count}/2)" in results[0].skip_message

        # Tick 6: success — recovery.
        ctx = build_sensor_context(
            cursor=cursor, instance=instance, sensor_name=_SENSOR_NAME,
        )
        results = list(retry_sensor(ctx))
        assert results[0].skip_message == "recovered"


class TestBreachResetsForRecovery:
    def test_breach_persists_reset_state(self, instance):
        """When the threshold is breached, KVS should have error_count=0."""

        @sensor(job=_make_job())
        @resilient_sensor(threshold=2)
        def failing_sensor(context):
            raise ConnectionError("timeout")

        cursor = None

        # 2 suppressed errors.
        for _ in range(2):
            context = build_sensor_context(
                cursor=cursor, instance=instance, sensor_name=_SENSOR_NAME,
            )
            list(failing_sensor(context))
            cursor = context.cursor

        state = load_guard_state(instance.daemon_cursor_storage, _SENSOR_NAME)
        assert state.error_count == 2

        # 3rd error breaches — KVS should have reset state.
        context = build_sensor_context(
            cursor=cursor, instance=instance, sensor_name=_SENSOR_NAME,
        )
        with pytest.raises(ConnectionError):
            list(failing_sensor(context))

        state = load_guard_state(instance.daemon_cursor_storage, _SENSOR_NAME)
        assert state.error_count == 0


class TestCallback:
    def test_on_suppressed_error_called(self, instance):
        callback = MagicMock()

        @sensor(job=_make_job())
        @resilient_sensor(threshold=3, on_suppressed_error=callback)
        def failing_sensor(context):
            raise ValueError("bad value")

        _invoke_sensor(failing_sensor, instance)

        callback.assert_called_once()
        args = callback.call_args[0]
        assert isinstance(args[0], ValueError)
        assert args[1] == 1  # error count
        assert args[2] == 3  # threshold

    def test_callback_not_called_when_threshold_breached(self, instance):
        callback = MagicMock()

        @sensor(job=_make_job())
        @resilient_sensor(threshold=1, on_suppressed_error=callback)
        def failing_sensor(context):
            raise ValueError("bad value")

        # First error is suppressed — callback called.
        context = build_sensor_context(
            instance=instance, sensor_name=_SENSOR_NAME,
        )
        list(failing_sensor(context))
        assert callback.call_count == 1

        # Second error breaches threshold — callback NOT called.
        context = build_sensor_context(
            instance=instance, sensor_name=_SENSOR_NAME,
        )
        with pytest.raises(ValueError):
            list(failing_sensor(context))
        assert callback.call_count == 1  # unchanged


class TestSensorIsolation:
    def test_error_counts_are_independent_across_sensors(self, instance):
        """Each decorated sensor tracks its own error count in KVS."""

        @sensor(job=_make_job(), name="sensor_a")
        @resilient_sensor(threshold=3)
        def sensor_a(context):
            raise ConnectionError("sensor A down")

        @sensor(job=_make_job(), name="sensor_b")
        @resilient_sensor(threshold=3)
        def sensor_b(context):
            raise ConnectionError("sensor B down")

        # Fail sensor_a 3 times.
        for _ in range(3):
            ctx = build_sensor_context(
                instance=instance, sensor_name="sensor_a",
            )
            list(sensor_a(ctx))

        state_a = load_guard_state(instance.daemon_cursor_storage, "sensor_a")
        assert state_a.error_count == 3

        # sensor_b should still be at 0.
        state_b = load_guard_state(instance.daemon_cursor_storage, "sensor_b")
        assert state_b.error_count == 0

        # Fail sensor_b once.
        ctx = build_sensor_context(
            instance=instance, sensor_name="sensor_b",
        )
        list(sensor_b(ctx))

        state_b = load_guard_state(instance.daemon_cursor_storage, "sensor_b")
        assert state_b.error_count == 1

        # sensor_a is still at 3.
        state_a = load_guard_state(instance.daemon_cursor_storage, "sensor_a")
        assert state_a.error_count == 3


class TestMultipleRunRequests:
    def test_run_requests_forwarded_then_error_suppressed_no_skip(self, instance):
        """If RunRequests were yielded before the error, suppress without SkipReason."""

        @sensor(job=_make_job())
        @resilient_sensor(threshold=5)
        def partial_sensor(context):
            yield RunRequest(run_key="a")
            yield RunRequest(run_key="b")
            raise ConnectionError("mid-stream failure")

        results, _ = _invoke_sensor(partial_sensor, instance)

        # Both RunRequests should have been yielded.
        assert len(results) == 2
        assert all(isinstance(r, RunRequest) for r in results)

        state = load_guard_state(instance.daemon_cursor_storage, _SENSOR_NAME)
        assert state.error_count == 1

    def test_multiple_run_requests_success(self, instance):
        @sensor(job=_make_job())
        @resilient_sensor(threshold=5)
        def multi_sensor(context):
            yield RunRequest(run_key="a")
            yield RunRequest(run_key="b")
            yield RunRequest(run_key="c")

        results, _ = _invoke_sensor(multi_sensor, instance)
        assert len(results) == 3
        assert all(isinstance(r, RunRequest) for r in results)


class TestSensorResult:
    def test_sensor_result_yielded_with_contents(self, instance):
        """SensorResult is yielded as-is for Dagster to process."""

        @sensor(job=_make_job())
        @resilient_sensor(threshold=3)
        def result_sensor(context):
            return SensorResult(
                run_requests=[RunRequest(run_key="sr-1"), RunRequest(run_key="sr-2")],
                cursor="new_cursor",
            )

        results, ctx = _invoke_sensor(result_sensor, instance)
        assert len(results) == 1
        assert isinstance(results[0], SensorResult)
        assert len(results[0].run_requests) == 2
        assert results[0].run_requests[0].run_key == "sr-1"
        assert results[0].run_requests[1].run_key == "sr-2"
        # SensorResult passes through natively — cursor is preserved.
        assert results[0].cursor == "new_cursor"

        state = load_guard_state(instance.daemon_cursor_storage, _SENSOR_NAME)
        assert state.error_count == 0

    def test_sensor_result_skip_reason_preserved(self, instance):
        """SensorResult with skip_reason is yielded intact."""

        @sensor(job=_make_job())
        @resilient_sensor(threshold=3)
        def result_sensor(context):
            return SensorResult(skip_reason=SkipReason("nothing to do"))

        results, _ = _invoke_sensor(result_sensor, instance)
        assert len(results) == 1
        assert isinstance(results[0], SensorResult)
        assert results[0].skip_reason.skip_message == "nothing to do"

        state = load_guard_state(instance.daemon_cursor_storage, _SENSOR_NAME)
        assert state.error_count == 0

    def test_sensor_result_error_suppressed(self, instance):
        """Error after a SensorResult tick should be suppressed."""
        tick = 0

        @sensor(job=_make_job())
        @resilient_sensor(threshold=3)
        def result_sensor(context):
            nonlocal tick
            tick += 1
            if tick == 1:
                return SensorResult(run_requests=[RunRequest(run_key="sr-1")])
            raise ConnectionError("timeout")

        # Tick 1: SensorResult success.
        results, ctx = _invoke_sensor(result_sensor, instance)
        assert len(results) == 1
        assert isinstance(results[0], SensorResult)

        # Tick 2: error suppressed.
        context = build_sensor_context(
            cursor=ctx.cursor, instance=instance, sensor_name=_SENSOR_NAME,
        )
        results = list(result_sensor(context))
        assert len(results) == 1
        assert isinstance(results[0], SkipReason)
        assert "(1/3)" in results[0].skip_message

    def test_sensor_result_with_run_requests_then_error_no_skip(self, instance):
        """If a SensorResult with run_requests was yielded before the error,
        suppress without emitting a SkipReason (same as bare RunRequest)."""

        @sensor(job=_make_job())
        @resilient_sensor(threshold=5)
        def partial_sensor(context):
            yield SensorResult(
                run_requests=[RunRequest(run_key="sr-1")],
            )
            raise ConnectionError("mid-stream failure")

        results, _ = _invoke_sensor(partial_sensor, instance)

        # SensorResult was yielded; no SkipReason should follow.
        assert len(results) == 1
        assert isinstance(results[0], SensorResult)

        state = load_guard_state(instance.daemon_cursor_storage, _SENSOR_NAME)
        assert state.error_count == 1


class TestNoneReturn:
    def test_sensor_returning_none(self, instance):
        """A sensor that returns None should succeed cleanly."""

        @sensor(job=_make_job())
        @resilient_sensor(threshold=3)
        def none_sensor(context):
            pass

        results, _ = _invoke_sensor(none_sensor, instance)
        assert len(results) == 0

        state = load_guard_state(instance.daemon_cursor_storage, _SENSOR_NAME)
        assert state.error_count == 0

    def test_none_return_resets_error_count(self, instance):
        """A None-returning success should still reset the error counter."""
        tick = 0

        @sensor(job=_make_job())
        @resilient_sensor(threshold=5)
        def none_sensor(context):
            nonlocal tick
            tick += 1
            if tick <= 2:
                raise ConnectionError("timeout")

        cursor = None
        # 2 errors.
        for _ in range(2):
            context = build_sensor_context(
                cursor=cursor, instance=instance, sensor_name=_SENSOR_NAME,
            )
            list(none_sensor(context))
            cursor = context.cursor

        state = load_guard_state(instance.daemon_cursor_storage, _SENSOR_NAME)
        assert state.error_count == 2

        # Tick 3: success (None return), resets counter.
        context = build_sensor_context(
            cursor=cursor, instance=instance, sensor_name=_SENSOR_NAME,
        )
        list(none_sensor(context))

        state = load_guard_state(instance.daemon_cursor_storage, _SENSOR_NAME)
        assert state.error_count == 0


class TestInputValidation:
    def test_threshold_zero_raises(self):
        with pytest.raises(ValueError, match="threshold must be >= 1"):
            resilient_sensor(threshold=0)

    def test_threshold_negative_raises(self):
        with pytest.raises(ValueError, match="threshold must be >= 1"):
            resilient_sensor(threshold=-1)

    def test_window_minutes_zero_raises(self):
        with pytest.raises(ValueError, match="window_minutes must be > 0"):
            resilient_sensor(window_minutes=0)

    def test_window_minutes_negative_raises(self):
        with pytest.raises(ValueError, match="window_minutes must be > 0"):
            resilient_sensor(window_minutes=-5)

    def test_decay_amount_zero_raises(self):
        with pytest.raises(ValueError, match="decay_amount must be >= 1"):
            resilient_sensor(decay_amount=0)


class TestSensorResultFields:
    def test_dynamic_partitions_requests_preserved(self, instance):
        @sensor(job=_make_job())
        @resilient_sensor(threshold=3)
        def dp_sensor(context):
            return SensorResult(
                run_requests=[RunRequest(run_key="dp-1")],
                dynamic_partitions_requests=[
                    AddDynamicPartitionsRequest(
                        partitions_def_name="my_partitions",
                        partition_keys=["2024-01-01"],
                    ),
                ],
            )

        results, _ = _invoke_sensor(dp_sensor, instance)
        assert len(results) == 1
        sr = results[0]
        assert isinstance(sr, SensorResult)
        assert len(sr.run_requests) == 1
        assert len(sr.dynamic_partitions_requests) == 1
        assert sr.dynamic_partitions_requests[0].partition_keys == ["2024-01-01"]

    def test_asset_events_preserved(self, instance):
        @sensor(job=_make_job())
        @resilient_sensor(threshold=3)
        def asset_sensor(context):
            return SensorResult(
                asset_events=[
                    AssetMaterialization(asset_key=AssetKey("my_asset")),
                ],
            )

        results, _ = _invoke_sensor(asset_sensor, instance)
        assert len(results) == 1
        sr = results[0]
        assert isinstance(sr, SensorResult)
        assert len(sr.asset_events) == 1
        assert sr.asset_events[0].asset_key == AssetKey("my_asset")


class TestCallbackExceptionHandling:
    def test_broken_callback_does_not_crash_sensor(self, instance):
        def bad_callback(error, count, threshold):
            raise RuntimeError("callback exploded")

        @sensor(job=_make_job())
        @resilient_sensor(threshold=3, on_suppressed_error=bad_callback)
        def failing_sensor(context):
            raise ConnectionError("timeout")

        results, _ = _invoke_sensor(failing_sensor, instance)
        assert len(results) == 1
        assert isinstance(results[0], SkipReason)
        state = load_guard_state(instance.daemon_cursor_storage, _SENSOR_NAME)
        assert state.error_count == 1

    def test_broken_callback_logs_warning(self, instance, caplog):
        def bad_callback(error, count, threshold):
            raise RuntimeError("callback exploded")

        @sensor(job=_make_job())
        @resilient_sensor(threshold=3, on_suppressed_error=bad_callback)
        def failing_sensor(context):
            raise ConnectionError("timeout")

        with caplog.at_level(logging.WARNING, logger="dagster.sensor_guard"):
            _invoke_sensor(failing_sensor, instance)

        assert "on_suppressed_error callback raised an exception" in caplog.text


class TestAsyncSensorRejection:
    def test_async_function_raises_type_error(self):
        async def async_sensor(context):
            pass

        with pytest.raises(TypeError, match="does not support async"):
            resilient_sensor(threshold=3)(async_sensor)

    def test_async_generator_raises_type_error(self):
        async def async_gen_sensor(context):
            yield RunRequest(run_key="a")

        with pytest.raises(TypeError, match="does not support async"):
            resilient_sensor(threshold=3)(async_gen_sensor)
