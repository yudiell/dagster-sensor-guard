"""Per-key failure tracking for multi-resource sensors."""

from __future__ import annotations

import logging
from contextlib import contextmanager
from typing import Callable, Dict, Optional

from dagster_sensor_guard.state import (
    GuardState,
    apply_reset,
    increment_error,
    load_all_key_states,
    save_all_key_states,
    should_raise,
)
from dagster_sensor_guard.types import ResetStrategy

logger = logging.getLogger("dagster.sensor_guard")


class SensorGuardKeyError(Exception):
    """Raised after the sensor loop when one or more keys have breached the threshold."""

    def __init__(self, breached_keys: Dict[str, Exception]) -> None:
        self.breached_keys = breached_keys
        key_names = ", ".join(sorted(breached_keys.keys()))
        super().__init__(
            f"{len(breached_keys)} key(s) exceeded failure threshold: {key_names}"
        )


class SensorGuard:
    """Per-key failure tracker injected into sensors decorated with ``per_key=True``.

    Usage::

        @sensor(job=my_job)
        @resilient_sensor(threshold=3, per_key=True)
        def my_sensor(context, guard):
            for table in ["orders", "customers"]:
                with guard.track(table):
                    ...
    """

    def __init__(
        self,
        *,
        storage: object,
        sensor_name: str,
        threshold: int,
        window_minutes: Optional[int],
        reset_strategy: ResetStrategy,
        decay_amount: int,
        on_suppressed_error: Optional[Callable[[Exception, int, int], None]],
    ) -> None:
        self._storage = storage
        self._sensor_name = sensor_name
        self._threshold = threshold
        self._window_minutes = window_minutes
        self._reset_strategy = reset_strategy
        self._decay_amount = decay_amount
        self._on_suppressed_error = on_suppressed_error

        # One KVS read at construction.
        self._key_states: Dict[str, GuardState] = load_all_key_states(
            storage, sensor_name
        )
        self._breached_keys: Dict[str, Exception] = {}
        self._succeeded_keys: list[str] = []
        self._suppressed_keys: list[str] = []

    @property
    def breached_keys(self) -> Dict[str, Exception]:
        """Keys whose error count exceeded the threshold during this tick."""
        return dict(self._breached_keys)

    @property
    def succeeded_keys(self) -> list[str]:
        """Keys that succeeded during this tick."""
        return list(self._succeeded_keys)

    @property
    def suppressed_keys(self) -> list[str]:
        """Keys whose errors were suppressed during this tick."""
        return list(self._suppressed_keys)

    @contextmanager
    def track(self, key: str):
        """Context manager that tracks success/failure for a single key.

        - Success: resets that key's counter.
        - Error below threshold: suppresses, logs, calls on_suppressed_error.
        - Error at/above threshold: suppresses, collects in breached_keys.
        """
        try:
            yield
        except Exception as exc:
            state = self._key_states.get(key, GuardState())
            state = increment_error(state, self._window_minutes)

            if should_raise(state, self._threshold):
                # Breached — collect but don't raise yet (process remaining keys).
                self._breached_keys[key] = exc
                logger.info(
                    "[%s] key '%s': error exceeded threshold (%d/%d) - %s",
                    self._sensor_name, key, state.error_count, self._threshold, exc,
                )
                if self._reset_strategy == ResetStrategy.FULL:
                    state = GuardState()
                # For DECAY, preserve count so subsequent ticks keep breaching.
                self._key_states[key] = state
            else:
                # Below threshold — suppress.
                self._key_states[key] = state
                self._suppressed_keys.append(key)
                logger.info(
                    "[%s] key '%s': error suppressed (%d/%d) - %s",
                    self._sensor_name, key, state.error_count, self._threshold, exc,
                )
                if self._on_suppressed_error is not None:
                    try:
                        self._on_suppressed_error(
                            exc, state.error_count, self._threshold
                        )
                    except Exception:
                        logger.warning(
                            "on_suppressed_error callback raised an exception",
                            exc_info=True,
                        )
        else:
            # Success — reset this key's counter.
            state = self._key_states.get(key, GuardState())
            state = apply_reset(state, self._reset_strategy, self._decay_amount)
            self._key_states[key] = state
            self._succeeded_keys.append(key)
            logger.info("[%s] key '%s': ok", self._sensor_name, key)

    def save(self) -> None:
        """Batch-write all per-key states to KVS (one write)."""
        save_all_key_states(self._storage, self._sensor_name, self._key_states)
