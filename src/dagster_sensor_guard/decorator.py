"""The @resilient_sensor decorator for Dagster sensors.

Guard state is stored in Dagster's daemon_cursor_storage (KVS), completely
decoupled from the user's sensor cursor. The user's cursor flows through
Dagster natively, untouched.
"""

from __future__ import annotations

import inspect
import logging
from functools import update_wrapper
from typing import Callable, Optional, Union

from dagster import RunRequest, SensorDefinition, SensorEvaluationContext, SensorResult, SkipReason

from dagster_sensor_guard.guard import SensorGuard, SensorGuardKeyError
from dagster_sensor_guard.state import (
    GuardState,
    apply_reset,
    increment_error,
    load_guard_state,
    save_guard_state,
    should_raise,
)
from dagster_sensor_guard.types import CursorStorage, ResetStrategy

logger = logging.getLogger("dagster.sensor_guard")


def _dispatch_result(result):
    """Yield items from a sensor function's return value."""
    if inspect.isgenerator(result):
        yield from result
    elif isinstance(result, SensorResult):
        yield result
    elif isinstance(result, (list, tuple)):
        yield from result
    elif result is not None:
        yield result


def _handle_sensor_error(
    exc: Exception,
    guard_state: GuardState,
    storage: CursorStorage,
    sensor_name: str,
    window_minutes: Optional[int],
    threshold: int,
    reset_strategy: ResetStrategy,
    on_suppressed_error: Optional[Callable[[Exception, int, int], None]],
    has_run_request: bool,
) -> tuple[GuardState, bool, Optional[SkipReason]]:
    """Process a sensor error: increment count, check threshold, persist state.

    Returns (updated_state, should_reraise, skip_reason_or_none).
    When should_reraise is True, the caller must re-raise the exception
    inside its own except block to preserve the original traceback.
    """
    guard_state = increment_error(guard_state, window_minutes)

    if should_raise(guard_state, threshold):
        if reset_strategy == ResetStrategy.FULL:
            save_guard_state(storage, sensor_name, GuardState())
        else:
            save_guard_state(storage, sensor_name, guard_state)
        return guard_state, True, None

    save_guard_state(storage, sensor_name, guard_state)

    if on_suppressed_error is not None:
        try:
            on_suppressed_error(exc, guard_state.error_count, threshold)
        except Exception:
            logger.warning(
                "on_suppressed_error callback raised an exception",
                exc_info=True,
            )

    skip = None
    if not has_run_request:
        skip = SkipReason(
            f"Suppressed transient error "
            f"({guard_state.error_count}/{threshold}): {exc}"
        )

    return guard_state, False, skip


def resilient_sensor(
    threshold: int = 3,
    window_minutes: Optional[int] = None,
    reset_strategy: Union[str, ResetStrategy] = ResetStrategy.FULL,
    decay_amount: int = 1,
    on_suppressed_error: Optional[Callable[[Exception, int, int], None]] = None,
    per_key: bool = False,
) -> Callable:
    """Decorator that adds error tolerance to a Dagster sensor.

    Wraps the raw sensor function with error tracking. Must be stacked
    below @sensor:

        @sensor(job=my_job)
        @resilient_sensor(threshold=3)
        def my_sensor(context):
            ...

    For sensors that iterate over multiple independent resources, use
    ``per_key=True`` to track failures independently per key:

        @sensor(job=my_job)
        @resilient_sensor(threshold=3, per_key=True)
        def my_sensor(context, guard):
            for table in ["orders", "customers"]:
                with guard.track(table):
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
        per_key: When True, a SensorGuard is injected as the second parameter
            for independent per-key failure tracking. Defaults to False.
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

        if per_key:
            sig = inspect.signature(fn)
            if len(sig.parameters) < 2:
                raise TypeError(
                    "per_key=True requires the sensor function to accept a "
                    "second parameter for the SensorGuard, e.g.:\n"
                    "    def my_sensor(context, guard): ..."
                )

        def wrapped_fn(context: SensorEvaluationContext):
            storage = context.instance.daemon_cursor_storage
            sensor_name = context.sensor_name

            # --- Load guard state from KVS ---
            guard_state = load_guard_state(storage, sensor_name)

            # --- per_key=True: delegate per-key tracking to SensorGuard ---
            if per_key:
                guard = SensorGuard(
                    storage=storage,
                    sensor_name=sensor_name,
                    threshold=threshold,
                    window_minutes=window_minutes,
                    reset_strategy=reset_enum,
                    decay_amount=decay_amount,
                    on_suppressed_error=on_suppressed_error,
                )

                has_run_request = False

                try:
                    result = fn(context, guard)

                    for item in _dispatch_result(result):
                        if isinstance(item, RunRequest):
                            has_run_request = True
                        yield item

                except Exception as exc:
                    # Exception outside guard.track() — sensor-level tracking.
                    guard.save()
                    guard_state, reraise, skip = _handle_sensor_error(
                        exc, guard_state, storage, sensor_name, window_minutes,
                        threshold, reset_enum, on_suppressed_error, has_run_request,
                    )
                    if reraise:
                        raise
                    if skip is not None:
                        yield skip
                    return

                # per_key success path: save per-key state and sensor-level state.
                guard.save()
                guard_state = apply_reset(guard_state, reset_enum, decay_amount)
                save_guard_state(storage, sensor_name, guard_state)

                logger.warning(
                    "[%s] tick summary: %d ok, %d suppressed, %d breached%s",
                    sensor_name,
                    len(guard.succeeded_keys),
                    len(guard.suppressed_keys),
                    len(guard.breached_keys),
                    f" [{', '.join(sorted(guard.breached_keys))}]" if guard.breached_keys else "",
                )

                # Raise after full iteration if any keys breached.
                if guard.breached_keys:
                    raise SensorGuardKeyError(guard.breached_keys)

                return

            # --- per_key=False: original code path (unchanged) ---
            has_run_request = False

            try:
                result = fn(context)

                for item in _dispatch_result(result):
                    if isinstance(item, RunRequest):
                        has_run_request = True
                    yield item

            except Exception as exc:
                guard_state, reraise, skip = _handle_sensor_error(
                    exc, guard_state, storage, sensor_name, window_minutes,
                    threshold, reset_enum, on_suppressed_error, has_run_request,
                )
                if reraise:
                    raise
                if skip is not None:
                    yield skip
                return

            # --- Success path ---
            guard_state = apply_reset(guard_state, reset_enum, decay_amount)
            save_guard_state(storage, sensor_name, guard_state)

        # Preserve function metadata for Dagster introspection.
        update_wrapper(wrapped_fn, fn)
        return wrapped_fn

    return decorator
