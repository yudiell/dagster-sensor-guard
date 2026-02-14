"""The @resilient_sensor decorator for Dagster sensors.

Implementation notes (from Dagster source investigation):

- @sensor(job=...) returns a SensorDefinition. Our decorator sits above it and
  receives that SensorDefinition. We extract _raw_fn + config, wrap the raw fn,
  and construct a new SensorDefinition.

- context.cursor is Optional[str] — None when unset.
  context.update_cursor() accepts Optional[str].

- Dagster's wrap_sensor_evaluation iterates generators via next() in a loop.
  Exceptions from the user's generator propagate through next(). Our wrapper
  manually iterates to catch mid-generator exceptions.

- Dagster forbids yielding both SkipReason and RunRequest in the same tick
  (check.failed in evaluate_tick). We track whether RunRequests were yielded
  and skip the SkipReason if so.

- We manipulate context._cursor directly before/after calling the user's
  function to transparently namespace guard state alongside user cursor data.
"""

from __future__ import annotations

import inspect
from functools import update_wrapper
from typing import Callable, Optional, Union

from dagster import SensorDefinition, SkipReason
from dagster._core.definitions.run_request import RunRequest, SensorResult
from dagster._core.definitions.sensor_definition import SensorEvaluationContext

from dagster_sensor_guard.state import (
    GuardState,
    apply_reset,
    build_cursor,
    increment_error,
    parse_cursor,
    should_raise,
)
from dagster_sensor_guard.types import ResetStrategy


def resilient_sensor(
    threshold: int = 3,
    window_minutes: Optional[int] = None,
    reset_strategy: Union[str, ResetStrategy] = ResetStrategy.FULL,
    decay_amount: int = 1,
    on_suppressed_error: Optional[Callable[[Exception, int, int], None]] = None,
) -> Callable[[SensorDefinition], SensorDefinition]:
    """Decorator that adds error tolerance to a Dagster sensor.

    Wraps a sensor so that transient errors are suppressed until a consecutive
    failure threshold is breached. Must be stacked above @sensor:

        @resilient_sensor(threshold=3)
        @sensor(job=my_job)
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
    reset_enum = ResetStrategy(reset_strategy)

    def decorator(sensor_def: SensorDefinition) -> SensorDefinition:
        original_fn = sensor_def._raw_fn  # noqa: SLF001

        def wrapped_fn(context: SensorEvaluationContext):
            # --- Cursor setup: extract guard state, expose user cursor ---
            raw_cursor = context._cursor  # noqa: SLF001
            guard_state, user_cursor = parse_cursor(raw_cursor)

            # Expose only the user's cursor to the sensor function.
            context._cursor = user_cursor  # noqa: SLF001
            context._cursor_updated = False  # noqa: SLF001

            has_run_request = False

            try:
                result = original_fn(context)

                if inspect.isgenerator(result):
                    for item in result:
                        if isinstance(item, RunRequest):
                            has_run_request = True
                        yield item
                elif isinstance(result, SensorResult):
                    # SensorResult is a NamedTuple — yielding it raw would
                    # unpack its fields. Extract components instead.
                    if result.run_requests:
                        for rr in result.run_requests:
                            has_run_request = True
                            yield rr
                    if result.skip_reason:
                        yield result.skip_reason
                    if result.cursor is not None:
                        context.update_cursor(result.cursor)
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
                guard_state = increment_error(guard_state)

                # When window_minutes is set, check if the error chain has expired.
                # If so, reset and start a fresh chain with this error.
                if (
                    window_minutes is not None
                    and guard_state.first_error_ts is not None
                ):
                    import time

                    elapsed = time.time() - guard_state.first_error_ts
                    if elapsed > window_minutes * 60:
                        guard_state = increment_error(GuardState())

                # Persist state before deciding to raise or suppress.
                updated_user_cursor = _read_user_cursor(context, user_cursor)
                context.update_cursor(build_cursor(guard_state, updated_user_cursor))

                if should_raise(guard_state, threshold, window_minutes):
                    raise

                # Suppress the error.
                if on_suppressed_error is not None:
                    on_suppressed_error(exc, guard_state.error_count, threshold)

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
            updated_user_cursor = _read_user_cursor(context, user_cursor)
            context.update_cursor(build_cursor(guard_state, updated_user_cursor))

        # Preserve function metadata for Dagster introspection.
        update_wrapper(wrapped_fn, original_fn)

        # Build a new SensorDefinition with the wrapped evaluation function.
        new_sensor = SensorDefinition.dagster_internal_init(
            name=sensor_def.name,
            evaluation_fn=wrapped_fn,
            minimum_interval_seconds=sensor_def.minimum_interval_seconds,
            description=sensor_def.description,
            job_name=None,
            job=None,
            jobs=sensor_def.jobs if sensor_def.has_jobs else None,
            default_status=sensor_def.default_status,
            asset_selection=sensor_def.asset_selection,
            required_resource_keys=sensor_def._raw_required_resource_keys,  # noqa: SLF001
            tags=sensor_def.tags,
            metadata=dict(sensor_def.metadata),
            target=None,
            owners=sensor_def.owners,
        )
        return new_sensor

    return decorator


def _read_user_cursor(
    context: SensorEvaluationContext, original_user_cursor: Optional[str]
) -> Optional[str]:
    """Read back the user's cursor after sensor evaluation.

    If the user called update_cursor(), use whatever they set.
    Otherwise, fall back to what we injected before the call.
    """
    if context._cursor_updated:  # noqa: SLF001
        return context._cursor  # noqa: SLF001
    return original_user_cursor
