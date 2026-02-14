"""Tests for the @resilient_sensor decorator."""

import json
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
from dagster_sensor_guard.state import _GUARD_KEY, parse_cursor
from tests.conftest import make_job as _make_job


def _invoke_sensor(sensor_def, cursor=None):
    """Invoke a sensor and collect all yielded results.

    Returns (results, updated_cursor).
    """
    context = build_sensor_context(cursor=cursor)
    results = list(sensor_def(context))
    return results, context._cursor  # noqa: SLF001


class TestCountThreshold:
    def test_errors_below_threshold_are_suppressed(self):
        @sensor(job=_make_job())
        @resilient_sensor(threshold=3)
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
        @sensor(job=_make_job())
        @resilient_sensor(threshold=2)
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
        @sensor(job=_make_job())
        @resilient_sensor(threshold=3)
        def failing_sensor(context):
            raise RuntimeError("boom")

        results, _ = _invoke_sensor(failing_sensor)
        assert len(results) == 1
        assert isinstance(results[0], SkipReason)
        assert "(1/3)" in results[0].skip_message


class TestTimeWindowThreshold:
    def test_errors_within_window_accumulate(self):
        @sensor(job=_make_job())
        @resilient_sensor(threshold=2, window_minutes=10)
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
        @sensor(job=_make_job())
        @resilient_sensor(threshold=2, window_minutes=10)
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
        data[_GUARD_KEY]["first_error_ts"] = 1000.0  # very old
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

    def test_decay_accumulates_residual_across_rounds(self):
        """Decay only subtracts 1 per success, so residual carries forward.

        Pattern: fail, succeed, fail, fail, succeed — after the second succeed
        the count should be 1 (not 0) because the single success only decays
        the 2-error count by 1.
        """
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
            context = build_sensor_context(cursor=cursor)
            list(flapping_sensor(context))
            cursor = context._cursor  # noqa: SLF001
            guard_state, _ = parse_cursor(cursor)
            counts.append(guard_state.error_count)

        # Tick 1: fail → 1
        # Tick 2: succeed → decay 1→0
        # Tick 3: fail → 1
        # Tick 4: fail → 2
        # Tick 5: succeed → decay 2→1 (residual!)
        assert counts == [1, 0, 1, 2, 1]


class TestRecoveryAfterBreach:
    def test_success_after_threshold_breach_resets_counter(self):
        """After threshold is breached and error raised, a subsequent success
        should reset the counter back to 0 (full reset strategy)."""
        # 4 fails then succeed
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
            context = build_sensor_context(cursor=cursor)
            results = list(recovering_sensor(context))
            cursor = context._cursor  # noqa: SLF001
            assert isinstance(results[0], SkipReason)
            assert f"({i + 1}/3)" in results[0].skip_message

        # 4th error breaches threshold.
        context = build_sensor_context(cursor=cursor)
        try:
            list(recovering_sensor(context))
            assert False, "Should have raised"
        except ConnectionError:
            cursor = context._cursor  # noqa: SLF001

        # 5th tick succeeds — counter should reset.
        context = build_sensor_context(cursor=cursor)
        results = list(recovering_sensor(context))
        cursor = context._cursor  # noqa: SLF001

        assert len(results) == 1
        assert isinstance(results[0], SkipReason)
        assert results[0].skip_message == "OK"

        guard_state, _ = parse_cursor(cursor)
        assert guard_state.error_count == 0

    def test_error_after_recovery_starts_fresh_count(self):
        """After recovering from a breach, new errors should start from (1/N)."""
        # 4 fails, succeed, fail
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
            context = build_sensor_context(cursor=cursor)
            list(recovering_sensor(context))
            cursor = context._cursor  # noqa: SLF001

        context = build_sensor_context(cursor=cursor)
        try:
            list(recovering_sensor(context))
        except ConnectionError:
            cursor = context._cursor  # noqa: SLF001

        # Tick 5: success, resets counter.
        context = build_sensor_context(cursor=cursor)
        list(recovering_sensor(context))
        cursor = context._cursor  # noqa: SLF001

        # Tick 6: fail again — should be (1/3), proving counter reset.
        context = build_sensor_context(cursor=cursor)
        results = list(recovering_sensor(context))
        cursor = context._cursor  # noqa: SLF001

        assert isinstance(results[0], SkipReason)
        assert "(1/3)" in results[0].skip_message

        guard_state, _ = parse_cursor(cursor)
        assert guard_state.error_count == 1


class TestCursorLeakAfterBreach:
    def test_user_sees_own_cursor_not_json_after_breach(self):
        """After a breach, the user's sensor should see their cursor value,
        not the raw JSON envelope — even with legacy key format."""
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
            context = build_sensor_context(cursor=cursor)
            list(cursor_sensor(context))
            cursor = context._cursor  # noqa: SLF001

        # 3rd error breaches.
        context = build_sensor_context(cursor=cursor)
        try:
            list(cursor_sensor(context))
        except ConnectionError:
            cursor = context._cursor  # noqa: SLF001

        # 4th tick succeeds — user must see their cursor, not JSON.
        context = build_sensor_context(cursor=cursor)
        list(cursor_sensor(context))

        # Tick 1: None (no prior cursor)
        assert observed_cursors[0] is None
        # Tick 2: "10" (set by tick 1)
        assert observed_cursors[1] == "10"
        # Tick 3: "20" (set by tick 2)
        assert observed_cursors[2] == "20"
        # Tick 4 (after breach): "30" (set by tick 3), NOT the JSON envelope
        assert observed_cursors[3] == "30"

    def test_legacy_cursor_does_not_leak(self):
        """A cursor persisted with the old __sensor_guard key should not
        leak the JSON envelope into context.cursor."""
        observed = []

        @sensor(job=_make_job())
        @resilient_sensor(threshold=3)
        def my_sensor(context):
            observed.append(context.cursor)
            yield SkipReason("OK")

        # Simulate a cursor written by the old code with the legacy key.
        legacy_cursor = json.dumps({
            "__sensor_guard": {
                "error_count": 2,
                "first_error_ts": 1000.0,
                "last_error_ts": 1010.0,
            },
            "__user_cursor": "36",
        })

        context = build_sensor_context(cursor=legacy_cursor)
        list(my_sensor(context))

        assert observed[0] == "36"


class TestRootCauseCursorLeakCascade:
    """Reproduce the exact failure chain: legacy key → leaked JSON cursor →
    ValueError in user code → error count never resets → sensor stuck."""

    def test_int_cursor_valueerror_from_legacy_key(self):
        """A sensor that does int(context.cursor) must not get a ValueError
        because context.cursor is the raw JSON envelope."""
        observed_cursors = []

        @sensor(job=_make_job())
        @resilient_sensor(threshold=3)
        def offset_sensor(context):
            observed_cursors.append(context.cursor)
            offset = int(context.cursor or "0")
            context.update_cursor(str(offset + 1))
            yield SkipReason(f"at {offset}")

        # Simulate a cursor written with the legacy key (pre-rename code).
        legacy_cursor = json.dumps({
            "__sensor_guard": {
                "error_count": 1,
                "first_error_ts": 1000.0,
                "last_error_ts": 1010.0,
            },
            "__user_cursor": "36",
        })

        # Without the fix this would raise ValueError because
        # context.cursor would be the full JSON string.
        context = build_sensor_context(cursor=legacy_cursor)
        results = list(offset_sensor(context))

        assert observed_cursors[0] == "36"
        assert isinstance(results[0], SkipReason)
        assert "at 36" in results[0].skip_message

    def test_never_recovers_when_cursor_leaks(self):
        """End-to-end: legacy cursor → leaked JSON → every tick raises
        ValueError → error count cycles at threshold → sensor stuck forever.

        The fix must break this chain so the sensor can recover."""
        tick = 0

        @sensor(job=_make_job())
        @resilient_sensor(threshold=2)
        def offset_sensor(context):
            nonlocal tick
            tick += 1
            # This is the user's real sensor code: parse cursor as int.
            offset = int(context.cursor or "0")
            context.update_cursor(str(offset + tick))
            yield SkipReason(f"at {offset}")

        # Build a legacy-format cursor as if errors had accumulated.
        legacy_cursor = json.dumps({
            "__sensor_guard": {
                "error_count": 2,
                "first_error_ts": 1000.0,
                "last_error_ts": 1010.0,
            },
            "__user_cursor": "100",
        })

        # With the fix: parse_cursor recognises the legacy key, extracts
        # user_cursor="100", and int("100") succeeds.
        context = build_sensor_context(cursor=legacy_cursor)
        results = list(offset_sensor(context))
        cursor = context._cursor  # noqa: SLF001

        assert isinstance(results[0], SkipReason)
        assert "at 100" in results[0].skip_message

        # Verify the rebuilt cursor uses the new key (migration).
        data = json.loads(cursor)
        assert "__dagster_sensor_guard_v1" in data
        assert "__sensor_guard" not in data


class TestRootCauseNeverRecovers:
    """Reproduce the exact failure chain: Dagster drops cursor on raise →
    count stays at threshold → every subsequent error re-breaches → sensor
    never gets a chance to run apply_reset."""

    def test_error_count_cycles_without_fix_simulation(self):
        """Simulate what happens in real Dagster when cursor is not persisted
        on raise: error count oscillates between threshold and threshold+1,
        re-raising every error tick. Verify the sensor still recovers on
        the first success tick."""
        tick = 0

        @sensor(job=_make_job())
        @resilient_sensor(threshold=2)
        def cycling_sensor(context):
            nonlocal tick
            tick += 1
            # Ticks 1-2: suppressed. Tick 3: breach. Tick 4: another error
            # (still broken). Tick 5: success (issue resolved).
            if tick <= 4:
                raise ConnectionError("down")
            yield SkipReason("recovered")

        cursor = None

        # Ticks 1-2: suppressed errors, cursor persisted normally.
        for _ in range(2):
            ctx = build_sensor_context(cursor=cursor)
            list(cycling_sensor(ctx))
            cursor = ctx._cursor  # noqa: SLF001

        last_good_cursor = cursor  # count=2

        # Tick 3: breach — Dagster drops the cursor update.
        ctx = build_sensor_context(cursor=cursor)
        try:
            list(cycling_sensor(ctx))
        except ConnectionError:
            pass
        # Simulate Dagster: revert to last_good_cursor.
        cursor = last_good_cursor

        # Tick 4: still broken — same cycle. count=2 → 3 → raise.
        ctx = build_sensor_context(cursor=cursor)
        try:
            list(cycling_sensor(ctx))
        except ConnectionError:
            pass
        # Dagster drops cursor again.
        cursor = last_good_cursor

        # Tick 5: issue resolved — MUST recover despite count=2 in cursor.
        ctx = build_sensor_context(cursor=cursor)
        results = list(cycling_sensor(ctx))
        cursor = ctx._cursor  # noqa: SLF001

        assert len(results) == 1
        assert results[0].skip_message == "recovered"

        guard_state, _ = parse_cursor(cursor)
        assert guard_state.error_count == 0

    def test_fresh_retries_after_breach_when_cursor_persisted(self):
        """When Dagster DOES persist cursor on raise (version-dependent),
        the reset state gives the sensor fresh retries instead of
        re-raising immediately on the next error."""
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
            ctx = build_sensor_context(cursor=cursor)
            list(retry_sensor(ctx))
            cursor = ctx._cursor  # noqa: SLF001

        # Tick 3: breach. Cursor IS persisted (reset state).
        ctx = build_sensor_context(cursor=cursor)
        try:
            list(retry_sensor(ctx))
        except ConnectionError:
            cursor = ctx._cursor  # noqa: SLF001

        guard_state, _ = parse_cursor(cursor)
        assert guard_state.error_count == 0  # Reset, not stuck at 3

        # Ticks 4-5: fresh retries — suppressed as (1/2, 2/2).
        for expected_count in [1, 2]:
            ctx = build_sensor_context(cursor=cursor)
            results = list(retry_sensor(ctx))
            cursor = ctx._cursor  # noqa: SLF001

            assert isinstance(results[0], SkipReason)
            assert f"({expected_count}/2)" in results[0].skip_message

        # Tick 6: success — recovery.
        ctx = build_sensor_context(cursor=cursor)
        results = list(retry_sensor(ctx))
        assert results[0].skip_message == "recovered"


class TestBreachResetsForRecovery:
    def test_breach_persists_reset_state(self):
        """When the threshold is breached, the persisted cursor should have
        error_count=0 so the sensor can recover with fresh retries."""

        @sensor(job=_make_job())
        @resilient_sensor(threshold=2)
        def failing_sensor(context):
            raise ConnectionError("timeout")

        cursor = None

        # 2 suppressed errors.
        for _ in range(2):
            context = build_sensor_context(cursor=cursor)
            list(failing_sensor(context))
            cursor = context._cursor  # noqa: SLF001

        guard_state, _ = parse_cursor(cursor)
        assert guard_state.error_count == 2

        # 3rd error breaches — cursor should have reset state.
        context = build_sensor_context(cursor=cursor)
        try:
            list(failing_sensor(context))
        except ConnectionError:
            cursor = context._cursor  # noqa: SLF001

        guard_state, _ = parse_cursor(cursor)
        assert guard_state.error_count == 0

    def test_recovery_when_dagster_drops_cursor_on_raise(self):
        """Simulate real Dagster: cursor is NOT persisted on failed ticks.
        After a breach, the cursor reverts to the last suppression tick.
        The sensor should still recover on the next success."""
        tick = 0

        @sensor(job=_make_job())
        @resilient_sensor(threshold=2)
        def recovering_sensor(context):
            nonlocal tick
            tick += 1
            if tick <= 3:
                raise ConnectionError("down")
            yield SkipReason("OK")

        cursor = None

        # 2 suppressed errors — cursor persisted normally.
        for _ in range(2):
            context = build_sensor_context(cursor=cursor)
            list(recovering_sensor(context))
            cursor = context._cursor  # noqa: SLF001

        last_good_cursor = cursor  # Snapshot before breach

        # 3rd error breaches — in real Dagster, cursor is NOT persisted.
        context = build_sensor_context(cursor=cursor)
        try:
            list(recovering_sensor(context))
        except ConnectionError:
            pass  # Cursor update lost — Dagster doesn't persist on raise

        # Simulate Dagster behavior: use last_good_cursor, NOT context._cursor.
        cursor = last_good_cursor

        # 4th tick: success — should recover even though count=2 persisted.
        context = build_sensor_context(cursor=cursor)
        results = list(recovering_sensor(context))
        cursor = context._cursor  # noqa: SLF001

        assert len(results) == 1
        assert isinstance(results[0], SkipReason)
        assert results[0].skip_message == "OK"

        guard_state, _ = parse_cursor(cursor)
        assert guard_state.error_count == 0


class TestCallback:
    def test_on_suppressed_error_called(self):
        callback = MagicMock()

        @sensor(job=_make_job())
        @resilient_sensor(threshold=3, on_suppressed_error=callback)
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

        @sensor(job=_make_job())
        @resilient_sensor(threshold=1, on_suppressed_error=callback)
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

        @sensor(job=_make_job(), name="sensor_a")
        @resilient_sensor(threshold=3)
        def sensor_a(context):
            raise ConnectionError("sensor A down")

        @sensor(job=_make_job(), name="sensor_b")
        @resilient_sensor(threshold=3)
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

        @sensor(job=_make_job())
        @resilient_sensor(threshold=5)
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
        @sensor(job=_make_job())
        @resilient_sensor(threshold=5)
        def multi_sensor(context):
            yield RunRequest(run_key="a")
            yield RunRequest(run_key="b")
            yield RunRequest(run_key="c")

        results, _ = _invoke_sensor(multi_sensor)
        assert len(results) == 3
        assert all(isinstance(r, RunRequest) for r in results)


class TestSensorResult:
    def test_sensor_result_yielded_with_contents(self):
        """SensorResult is yielded as-is (with cursor stripped) for Dagster to process."""

        @sensor(job=_make_job())
        @resilient_sensor(threshold=3)
        def result_sensor(context):
            return SensorResult(
                run_requests=[RunRequest(run_key="sr-1"), RunRequest(run_key="sr-2")],
                cursor="new_cursor",
            )

        results, cursor = _invoke_sensor(result_sensor)
        assert len(results) == 1
        assert isinstance(results[0], SensorResult)
        assert len(results[0].run_requests) == 2
        assert results[0].run_requests[0].run_key == "sr-1"
        assert results[0].run_requests[1].run_key == "sr-2"
        assert results[0].cursor is None  # cursor stripped, handled via namespacing

        guard_state, user_cursor = parse_cursor(cursor)
        assert guard_state.error_count == 0
        assert user_cursor == "new_cursor"

    def test_sensor_result_skip_reason_preserved(self):
        """SensorResult with skip_reason is yielded intact."""

        @sensor(job=_make_job())
        @resilient_sensor(threshold=3)
        def result_sensor(context):
            return SensorResult(skip_reason=SkipReason("nothing to do"))

        results, cursor = _invoke_sensor(result_sensor)
        assert len(results) == 1
        assert isinstance(results[0], SensorResult)
        assert results[0].skip_reason.skip_message == "nothing to do"

        guard_state, _ = parse_cursor(cursor)
        assert guard_state.error_count == 0

    def test_sensor_result_error_suppressed(self):
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
        results, cursor = _invoke_sensor(result_sensor)
        assert len(results) == 1
        assert isinstance(results[0], SensorResult)

        # Tick 2: error suppressed.
        context = build_sensor_context(cursor=cursor)
        results = list(result_sensor(context))
        assert len(results) == 1
        assert isinstance(results[0], SkipReason)
        assert "(1/3)" in results[0].skip_message


class TestNoneReturn:
    def test_sensor_returning_none(self):
        """A sensor that returns None (no yield, no return) should succeed cleanly."""

        @sensor(job=_make_job())
        @resilient_sensor(threshold=3)
        def none_sensor(context):
            pass  # returns None implicitly

        results, cursor = _invoke_sensor(none_sensor)
        assert len(results) == 0

        guard_state, _ = parse_cursor(cursor)
        assert guard_state.error_count == 0

    def test_none_return_resets_error_count(self):
        """A None-returning success should still reset the error counter."""
        tick = 0

        @sensor(job=_make_job())
        @resilient_sensor(threshold=5)
        def none_sensor(context):
            nonlocal tick
            tick += 1
            if tick <= 2:
                raise ConnectionError("timeout")
            # Implicit None return on tick 3.

        cursor = None
        # 2 errors.
        for _ in range(2):
            context = build_sensor_context(cursor=cursor)
            list(none_sensor(context))
            cursor = context._cursor  # noqa: SLF001

        guard_state, _ = parse_cursor(cursor)
        assert guard_state.error_count == 2

        # Tick 3: success (None return), resets counter.
        context = build_sensor_context(cursor=cursor)
        list(none_sensor(context))
        cursor = context._cursor  # noqa: SLF001

        guard_state, _ = parse_cursor(cursor)
        assert guard_state.error_count == 0


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
    def test_dynamic_partitions_requests_preserved(self):
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

        results, _ = _invoke_sensor(dp_sensor)
        assert len(results) == 1
        sr = results[0]
        assert isinstance(sr, SensorResult)
        assert len(sr.run_requests) == 1
        assert len(sr.dynamic_partitions_requests) == 1
        assert sr.dynamic_partitions_requests[0].partition_keys == ["2024-01-01"]

    def test_asset_events_preserved(self):
        @sensor(job=_make_job())
        @resilient_sensor(threshold=3)
        def asset_sensor(context):
            return SensorResult(
                asset_events=[
                    AssetMaterialization(asset_key=AssetKey("my_asset")),
                ],
            )

        results, _ = _invoke_sensor(asset_sensor)
        assert len(results) == 1
        sr = results[0]
        assert isinstance(sr, SensorResult)
        assert len(sr.asset_events) == 1
        assert sr.asset_events[0].asset_key == AssetKey("my_asset")


class TestCallbackExceptionHandling:
    def test_broken_callback_does_not_crash_sensor(self):
        def bad_callback(error, count, threshold):
            raise RuntimeError("callback exploded")

        @sensor(job=_make_job())
        @resilient_sensor(threshold=3, on_suppressed_error=bad_callback)
        def failing_sensor(context):
            raise ConnectionError("timeout")

        results, cursor = _invoke_sensor(failing_sensor)
        assert len(results) == 1
        assert isinstance(results[0], SkipReason)
        guard_state, _ = parse_cursor(cursor)
        assert guard_state.error_count == 1

    def test_broken_callback_logs_warning(self, caplog):
        def bad_callback(error, count, threshold):
            raise RuntimeError("callback exploded")

        @sensor(job=_make_job())
        @resilient_sensor(threshold=3, on_suppressed_error=bad_callback)
        def failing_sensor(context):
            raise ConnectionError("timeout")

        with caplog.at_level(logging.WARNING, logger="dagster_sensor_guard.decorator"):
            _invoke_sensor(failing_sensor)

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
