"""The @resilient_sensor decorator for Dagster sensors.

Guard state is stored in a local SQLite database (via SqliteGuardStorage),
completely decoupled from the user's sensor cursor. The user's cursor flows
through Dagster natively, untouched.
"""

from __future__ import annotations

import inspect
import logging
import typing
from functools import update_wrapper
from typing import Callable, Optional, Union

from dagster import RunRequest, SensorDefinition, SensorEvaluationContext, SensorResult, SkipReason

from dagster_sensor_guard.guard import SensorGuard, SensorGuardKeyError
from dagster_sensor_guard.storage import SqliteGuardStorage
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
    retention_days: int = 7,
    db_path: Optional[str] = None,
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
        retention_days: Number of days to keep guard state records in the
            SQLite database. Older records are automatically cleaned up.
            Defaults to 7.
        db_path: Explicit path for the SQLite database file. When omitted,
            falls back to $DAGSTER_HOME/sensor_guard.db or a temp directory.
    """
    if threshold < 1:
        raise ValueError(f"threshold must be >= 1, got {threshold}")
    if window_minutes is not None and window_minutes <= 0:
        raise ValueError(f"window_minutes must be > 0, got {window_minutes}")
    if decay_amount < 1:
        raise ValueError(f"decay_amount must be >= 1, got {decay_amount}")
    if retention_days < 1:
        raise ValueError(f"retention_days must be >= 1, got {retention_days}")

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

        # Inspect the original function's parameter layout at decoration time.
        orig_sig = inspect.signature(fn)
        orig_params = list(orig_sig.parameters.values())
        context_param_name = orig_params[0].name if orig_params else "context"

        guard_param_name = None
        if per_key:
            if len(orig_params) < 2:
                raise TypeError(
                    "per_key=True requires the sensor function to accept a "
                    "second parameter for the SensorGuard, e.g.:\n"
                    "    def my_sensor(context, guard): ..."
                )
            guard_param_name = orig_params[1].name

        # Pre-resolve type annotations using the original function's __globals__
        # so Dagster's typing.get_type_hints() doesn't fail due to __globals__
        # mismatch (the wrapper lives in decorator.py, not the user's module).
        try:
            resolved_annotations = typing.get_type_hints(
                fn, include_extras=True,
            )
        except Exception:
            resolved_annotations = dict(getattr(fn, "__annotations__", {}))

        _storage = None

        def wrapped_fn(*args, **kwargs):
            nonlocal _storage
            if _storage is None:
                _storage = SqliteGuardStorage(db_path=db_path, retention_days=retention_days)

            # Extract context from positional or keyword args.
            if args:
                context = args[0]
            else:
                context = kwargs[context_param_name]

            storage = _storage
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

                # Inject guard into the call alongside any resource kwargs.
                fn_kwargs = dict(kwargs)
                if args:
                    fn_kwargs[context_param_name] = args[0]
                fn_kwargs[guard_param_name] = guard

                try:
                    result = fn(**fn_kwargs)

                    for item in _dispatch_result(result):
                        if isinstance(item, RunRequest) or (
                            isinstance(item, SensorResult) and item.run_requests
                        ):
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

            # --- per_key=False: forward all args transparently ---
            has_run_request = False

            try:
                result = fn(*args, **kwargs)

                for item in _dispatch_result(result):
                    if isinstance(item, RunRequest) or (
                        isinstance(item, SensorResult) and item.run_requests
                    ):
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

        # Override annotations with pre-resolved types so Dagster's
        # typing.get_type_hints() works despite the __globals__ mismatch.
        wrapped_fn.__annotations__ = dict(resolved_annotations)

        if per_key:
            # Hide the guard parameter from Dagster's signature inspection.
            # Dagster should only see context + resource params; we inject
            # guard ourselves at call time.
            visible_params = [
                p for p in orig_params if p.name != guard_param_name
            ]
            wrapped_fn.__signature__ = orig_sig.replace(parameters=visible_params)
            wrapped_fn.__annotations__.pop(guard_param_name, None)

        return wrapped_fn

    return decorator
