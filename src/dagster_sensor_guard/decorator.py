"""The @resilient_sensor decorator for Dagster sensors.

Implementation notes (from Dagster source investigation):

- @resilient_sensor wraps the raw sensor function BEFORE @sensor processes it.
  @sensor then receives the wrapped function and builds the SensorDefinition.

- context.cursor is Optional[str] — None when unset.
  context.update_cursor() accepts Optional[str].

- Dagster's wrap_sensor_evaluation iterates generators via next() in a loop.
  Exceptions from the user's generator propagate through next(). Our wrapper
  manually iterates to catch mid-generator exceptions.

- Dagster forbids yielding both SkipReason and RunRequest in the same tick
  (check.failed in evaluate_tick). We track whether RunRequests were yielded
  and skip the SkipReason if so.

- We use context.update_cursor() before/after calling the user's function
  to transparently namespace guard state alongside user cursor data.
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
    build_cursor,
    increment_error,
    parse_cursor,
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
            # --- Cursor setup: extract guard state, expose user cursor ---
            raw_cursor = context.cursor
            guard_state, user_cursor = parse_cursor(raw_cursor)

            # Expose only the user's cursor to the sensor function.
            # Use the public API rather than touching context._cursor directly,
            # which breaks across Dagster versions.
            context.update_cursor(user_cursor)

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
                    if result.cursor is not None:
                        context.update_cursor(result.cursor)
                    # Yield the SensorResult for Dagster to process all fields
                    # (including automation_condition_evaluations and any
                    # future fields). Cursor is stripped since we handle it
                    # via namespacing.
                    yield result._replace(cursor=None)
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

                # Read the user cursor before we overwrite with the envelope.
                updated_user_cursor = context.cursor

                if should_raise(guard_state, threshold):
                    # Persist a *reset* state so recovery is possible on the
                    # next tick.  Dagster may not persist cursor on failed
                    # ticks, but if it does, a fresh counter lets the sensor
                    # suppress transient errors again after the breach.
                    context.update_cursor(
                        build_cursor(GuardState(), updated_user_cursor)
                    )
                    raise

                # Persist the incremented error count for suppressed errors.
                context.update_cursor(build_cursor(guard_state, updated_user_cursor))

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
            context.update_cursor(
                build_cursor(guard_state, context.cursor)
            )

        # Preserve function metadata for Dagster introspection.
        update_wrapper(wrapped_fn, fn)
        return wrapped_fn

    return decorator
