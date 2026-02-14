"""Tests for the @resilient_sensor decorator."""

import json
from unittest.mock import MagicMock

from dagster import RunRequest, SkipReason, build_sensor_context, sensor
from dagster._core.definitions.job_definition import JobDefinition

from dagster_sensor_guard import resilient_sensor
from dagster_sensor_guard.state import parse_cursor


def _make_job() -> JobDefinition:
    """Create a no-op job for sensor targets."""
    from dagster import job, op

    @op
    def noop():
        pass

    @job
    def noop_job():
        noop()

    return noop_job


def _invoke_sensor(sensor_def, cursor=None):
    """Invoke a sensor and collect all yielded results.

    Returns (results, updated_cursor).
    """
    context = build_sensor_context(cursor=cursor)
    results = list(sensor_def(context))
    return results, context._cursor  # noqa: SLF001


class TestCountThreshold:
    def test_errors_below_threshold_are_suppressed(self):
        @resilient_sensor(threshold=3)
        @sensor(job=_make_job())
        def failing_sensor(context):
            raise ConnectionError("timeout")

        cursor = None
        for i in range(3):
            context = build_sensor_context(cursor=cursor)
            results = list(failing_sensor(context))
            cursor = context._cursor  # noqa: SLF001
            assert len(results) == 1
            assert isinstance(results[0], SkipReason)
            assert "Suppressed transient error" in results[0].skip_message
            assert f"({i + 1}/3)" in results[0].skip_message

    def test_error_at_threshold_plus_one_raises(self):
        @resilient_sensor(threshold=2)
        @sensor(job=_make_job())
        def failing_sensor(context):
            raise ConnectionError("timeout")

        cursor = None
        # First 2 are suppressed.
        for _ in range(2):
            context = build_sensor_context(cursor=cursor)
            list(failing_sensor(context))
            cursor = context._cursor  # noqa: SLF001

        # Third should raise.
        context = build_sensor_context(cursor=cursor)
        try:
            list(failing_sensor(context))
            assert False, "Should have raised"
        except ConnectionError:
            pass

    def test_error_on_first_tick(self):
        @resilient_sensor(threshold=3)
        @sensor(job=_make_job())
        def failing_sensor(context):
            raise RuntimeError("boom")

        results, _ = _invoke_sensor(failing_sensor)
        assert len(results) == 1
        assert isinstance(results[0], SkipReason)
        assert "(1/3)" in results[0].skip_message


class TestTimeWindowThreshold:
    def test_errors_within_window_accumulate(self):
        @resilient_sensor(threshold=2, window_minutes=10)
        @sensor(job=_make_job())
        def failing_sensor(context):
            raise ConnectionError("timeout")

        cursor = None
        for _ in range(2):
            context = build_sensor_context(cursor=cursor)
            list(failing_sensor(context))
            cursor = context._cursor  # noqa: SLF001

        # Third should raise (within window).
        context = build_sensor_context(cursor=cursor)
        try:
            list(failing_sensor(context))
            assert False, "Should have raised"
        except ConnectionError:
            pass

    def test_errors_outside_window_reset_counter(self):
        @resilient_sensor(threshold=2, window_minutes=10)
        @sensor(job=_make_job())
        def failing_sensor(context):
            raise ConnectionError("timeout")

        # Simulate 2 errors.
        cursor = None
        for _ in range(2):
            context = build_sensor_context(cursor=cursor)
            list(failing_sensor(context))
            cursor = context._cursor  # noqa: SLF001

        # Manipulate the cursor to make first_error_ts old (outside window).
        data = json.loads(cursor)
        data["__sensor_guard"]["first_error_ts"] = 1000.0  # very old
        cursor = json.dumps(data)

        # Next error should be suppressed (counter reset due to window expiry).
        context = build_sensor_context(cursor=cursor)
        results = list(failing_sensor(context))
        assert len(results) == 1
        assert isinstance(results[0], SkipReason)
        assert "(1/2)" in results[0].skip_message


class TestFullReset:
    def test_success_clears_error_count(self):
        tick = 0

        @resilient_sensor(threshold=5)
        @sensor(job=_make_job())
        def flapping_sensor(context):
            nonlocal tick
            tick += 1
            if tick <= 3:
                raise ConnectionError("timeout")
            yield SkipReason("OK")

        cursor = None
        # 3 errors.
        for _ in range(3):
            context = build_sensor_context(cursor=cursor)
            list(flapping_sensor(context))
            cursor = context._cursor  # noqa: SLF001

        # Verify error count is 3.
        guard_state, _ = parse_cursor(cursor)
        assert guard_state.error_count == 3

        # One success.
        context = build_sensor_context(cursor=cursor)
        list(flapping_sensor(context))
        cursor = context._cursor  # noqa: SLF001

        # Error count should be reset to 0.
        guard_state, _ = parse_cursor(cursor)
        assert guard_state.error_count == 0


class TestDecayReset:
    def test_success_decrements_by_decay_amount(self):
        tick = 0

        @resilient_sensor(threshold=5, reset_strategy="decay", decay_amount=2)
        @sensor(job=_make_job())
        def flapping_sensor(context):
            nonlocal tick
            tick += 1
            if tick <= 4:
                raise ConnectionError("timeout")
            yield SkipReason("OK")

        cursor = None
        # 4 errors.
        for _ in range(4):
            context = build_sensor_context(cursor=cursor)
            list(flapping_sensor(context))
            cursor = context._cursor  # noqa: SLF001

        guard_state, _ = parse_cursor(cursor)
        assert guard_state.error_count == 4

        # One success with decay_amount=2.
        context = build_sensor_context(cursor=cursor)
        list(flapping_sensor(context))
        cursor = context._cursor  # noqa: SLF001

        guard_state, _ = parse_cursor(cursor)
        assert guard_state.error_count == 2


class TestCallback:
    def test_on_suppressed_error_called(self):
        callback = MagicMock()

        @resilient_sensor(threshold=3, on_suppressed_error=callback)
        @sensor(job=_make_job())
        def failing_sensor(context):
            raise ValueError("bad value")

        _invoke_sensor(failing_sensor)

        callback.assert_called_once()
        args = callback.call_args[0]
        assert isinstance(args[0], ValueError)
        assert args[1] == 1  # error count
        assert args[2] == 3  # threshold

    def test_callback_not_called_when_threshold_breached(self):
        callback = MagicMock()

        @resilient_sensor(threshold=1, on_suppressed_error=callback)
        @sensor(job=_make_job())
        def failing_sensor(context):
            raise ValueError("bad value")

        # First error is suppressed — callback called.
        context = build_sensor_context()
        list(failing_sensor(context))
        cursor = context._cursor  # noqa: SLF001
        assert callback.call_count == 1

        # Second error breaches threshold — callback NOT called.
        context = build_sensor_context(cursor=cursor)
        try:
            list(failing_sensor(context))
        except ValueError:
            pass
        assert callback.call_count == 1  # unchanged


class TestSensorIsolation:
    def test_error_counts_are_independent_across_sensors(self):
        """Each decorated sensor tracks its own error count via its own cursor."""

        @resilient_sensor(threshold=3)
        @sensor(job=_make_job(), name="sensor_a")
        def sensor_a(context):
            raise ConnectionError("sensor A down")

        @resilient_sensor(threshold=3)
        @sensor(job=_make_job(), name="sensor_b")
        def sensor_b(context):
            raise ConnectionError("sensor B down")

        cursor_a = None
        cursor_b = None

        # Fail sensor_a 3 times.
        for _ in range(3):
            ctx = build_sensor_context(cursor=cursor_a)
            list(sensor_a(ctx))
            cursor_a = ctx._cursor  # noqa: SLF001

        # sensor_a is at 3 errors.
        guard_a, _ = parse_cursor(cursor_a)
        assert guard_a.error_count == 3

        # sensor_b should still be at 0 — never ticked.
        guard_b, _ = parse_cursor(cursor_b)
        assert guard_b.error_count == 0

        # Fail sensor_b once.
        ctx = build_sensor_context(cursor=cursor_b)
        list(sensor_b(ctx))
        cursor_b = ctx._cursor  # noqa: SLF001

        guard_b, _ = parse_cursor(cursor_b)
        assert guard_b.error_count == 1

        # sensor_a is still at 3 — unaffected by sensor_b.
        guard_a, _ = parse_cursor(cursor_a)
        assert guard_a.error_count == 3


class TestMultipleRunRequests:
    def test_run_requests_forwarded_then_error_suppressed_no_skip(self):
        """If RunRequests were yielded before the error, suppress without SkipReason."""

        @resilient_sensor(threshold=5)
        @sensor(job=_make_job())
        def partial_sensor(context):
            yield RunRequest(run_key="a")
            yield RunRequest(run_key="b")
            raise ConnectionError("mid-stream failure")

        results, cursor = _invoke_sensor(partial_sensor)

        # Both RunRequests should have been yielded.
        assert len(results) == 2
        assert all(isinstance(r, RunRequest) for r in results)

        # Error was suppressed (no SkipReason because RunRequests were yielded).
        guard_state, _ = parse_cursor(cursor)
        assert guard_state.error_count == 1

    def test_multiple_run_requests_success(self):
        @resilient_sensor(threshold=5)
        @sensor(job=_make_job())
        def multi_sensor(context):
            yield RunRequest(run_key="a")
            yield RunRequest(run_key="b")
            yield RunRequest(run_key="c")

        results, _ = _invoke_sensor(multi_sensor)
        assert len(results) == 3
        assert all(isinstance(r, RunRequest) for r in results)
