"""The @resilient_sensor decorator for Dagster sensors.

Guard state is stored in Dagster's daemon_cursor_storage (KVS), completely
decoupled from the user's sensor cursor. The user's cursor flows through
Dagster natively, untouched.

Migration: On the first tick after upgrade, any old envelope-format cursor
is detected, guard state is moved to KVS, and the user's cursor is restored.
"""

from __future__ import annotations

import inspect
import logging
from functools import update_wrapper
from typing import Callable, Optional, Union

from dagster import RunRequest, SensorDefinition, SensorEvaluationContext, SensorResult, SkipReason

from dagster_sensor_guard.state import (
    GuardState,
    apply_reset,
    detect_envelope_cursor,
    increment_error,
    load_guard_state,
    save_guard_state,
    should_raise,
)
from dagster_sensor_guard.types import ResetStrategy

logger = logging.getLogger(__name__)


def resilient_sensor(
    threshold: int = 3,
    window_minutes: Optional[int] = None,
    reset_strategy: Union[str, ResetStrategy] = ResetStrategy.FULL,
    decay_amount: int = 1,
    on_suppressed_error: Optional[Callable[[Exception, int, int], None]] = None,
) -> Callable:
    """Decorator that adds error tolerance to a Dagster sensor.

    Wraps the raw sensor function with error tracking. Must be stacked
    below @sensor:

        @sensor(job=my_job)
        @resilient_sensor(threshold=3)
        def my_sensor(context):
            ...

    Args:
        threshold: Number of consecutive errors to tolerate before raising.
            Errors 1 through threshold are suppressed; error threshold+1 raises.
        window_minutes: Optional rolling window in minutes. When provided,
            only consecutive errors within this window count toward the
            threshold. If the first error in the chain is older than the
            window, the counter resets. When omitted, errors are counted
            by simple consecutive count with no time constraint.
        reset_strategy: "full" to clear the count on any success, "decay" to
            decrement by decay_amount per success.
        decay_amount: How much to subtract from the error count on success
            (only used with reset_strategy="decay").
        on_suppressed_error: Optional callback invoked each time an error is
            suppressed. Signature: (error, current_count, threshold) -> None.
    """
    if threshold < 1:
        raise ValueError(f"threshold must be >= 1, got {threshold}")
    if window_minutes is not None and window_minutes <= 0:
        raise ValueError(f"window_minutes must be > 0, got {window_minutes}")
    if decay_amount < 1:
        raise ValueError(f"decay_amount must be >= 1, got {decay_amount}")

    reset_enum = ResetStrategy(reset_strategy)

    def decorator(fn: Callable) -> Callable:
        if isinstance(fn, SensorDefinition):
            raise TypeError(
                "@resilient_sensor must be applied below @sensor, not above it.\n"
                "Correct usage:\n"
                "    @sensor(job=my_job)\n"
                "    @resilient_sensor(threshold=3)\n"
                "    def my_sensor(context): ..."
            )

        if inspect.iscoroutinefunction(fn) or inspect.isasyncgenfunction(fn):
            raise TypeError(
                "@resilient_sensor does not support async sensor functions. "
                "Use a synchronous function instead."
            )

        def wrapped_fn(context: SensorEvaluationContext):
            storage = context.instance.daemon_cursor_storage
            sensor_name = context.sensor_name

            # --- One-time migration from envelope cursor ---
            envelope = detect_envelope_cursor(context.cursor)
            if envelope is not None:
                old_guard_state, user_cursor = envelope
                save_guard_state(storage, sensor_name, old_guard_state)
                context.update_cursor(user_cursor)

            # --- Load guard state from KVS ---
            guard_state = load_guard_state(storage, sensor_name)

            has_run_request = False

            try:
                result = fn(context)

                if inspect.isgenerator(result):
                    for item in result:
                        if isinstance(item, RunRequest):
                            has_run_request = True
                        yield item
                elif isinstance(result, SensorResult):
                    if result.run_requests:
                        has_run_request = True
                    yield result
                elif isinstance(result, (list, tuple)):
                    for item in result:
                        if isinstance(item, RunRequest):
                            has_run_request = True
                        yield item
                elif result is not None:
                    if isinstance(result, RunRequest):
                        has_run_request = True
                    yield result

            except Exception as exc:
                # --- Error path ---
                guard_state = increment_error(guard_state, window_minutes)

                if should_raise(guard_state, threshold):
                    if reset_enum == ResetStrategy.FULL:
                        # Full reset: give the sensor fresh retries.
                        save_guard_state(storage, sensor_name, GuardState())
                    else:
                        # Decay: preserve the count so subsequent failures
                        # continue to breach until successes decay it down.
                        save_guard_state(storage, sensor_name, guard_state)
                    raise

                # Persist the incremented error count.
                save_guard_state(storage, sensor_name, guard_state)

                # Suppress the error.
                if on_suppressed_error is not None:
                    try:
                        on_suppressed_error(exc, guard_state.error_count, threshold)
                    except Exception:
                        logger.warning(
                            "on_suppressed_error callback raised an exception",
                            exc_info=True,
                        )

                # Only yield SkipReason if no RunRequests were yielded this tick
                # (Dagster forbids mixing SkipReason with RunRequest).
                if not has_run_request:
                    yield SkipReason(
                        f"Suppressed transient error "
                        f"({guard_state.error_count}/{threshold}): {exc}"
                    )
                return

            # --- Success path ---
            guard_state = apply_reset(guard_state, reset_enum, decay_amount)
            save_guard_state(storage, sensor_name, guard_state)

        # Preserve function metadata for Dagster introspection.
        update_wrapper(wrapped_fn, fn)
        return wrapped_fn

    return decorator
