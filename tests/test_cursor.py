"""Tests for cursor isolation between user data and guard state.

With KVS storage, the user's cursor flows through Dagster natively.
Guard state lives in daemon_cursor_storage, completely separate.
"""

import json

from dagster import SkipReason, build_sensor_context, sensor

from dagster_sensor_guard import resilient_sensor
from dagster_sensor_guard.state import load_guard_state
from tests.conftest import make_job as _make_job

_SENSOR_NAME = "test_cursor_sensor"


class TestCursorIsolation:
    def test_user_cursor_preserved_through_success(self, instance):
        @sensor(job=_make_job())
        @resilient_sensor(threshold=5)
        def cursor_sensor(context):
            current = context.cursor or "0"
            new_val = str(int(current) + 1)
            context.update_cursor(new_val)
            yield SkipReason(f"Processed up to {new_val}")

        cursor = None
        for i in range(3):
            context = build_sensor_context(
                cursor=cursor, instance=instance, sensor_name=_SENSOR_NAME,
            )
            list(cursor_sensor(context))
            cursor = context.cursor

        assert cursor == "3"
        state = load_guard_state(instance.daemon_cursor_storage, _SENSOR_NAME)
        assert state.error_count == 0

    def test_user_cursor_preserved_through_errors(self, instance):
        tick = 0

        @sensor(job=_make_job())
        @resilient_sensor(threshold=5)
        def cursor_sensor(context):
            nonlocal tick
            tick += 1
            if tick == 1:
                context.update_cursor("offset_100")
                yield SkipReason("OK")
            else:
                raise ConnectionError("timeout")

        # Tick 1: success, sets cursor.
        context = build_sensor_context(
            instance=instance, sensor_name=_SENSOR_NAME,
        )
        list(cursor_sensor(context))
        cursor = context.cursor
        assert cursor == "offset_100"

        # Tick 2: failure, user cursor should still be preserved.
        context = build_sensor_context(
            cursor=cursor, instance=instance, sensor_name=_SENSOR_NAME,
        )
        list(cursor_sensor(context))
        # User cursor is untouched — still "offset_100".
        assert context.cursor == "offset_100"

        state = load_guard_state(instance.daemon_cursor_storage, _SENSOR_NAME)
        assert state.error_count == 1

    def test_sensor_with_no_cursor_usage(self, instance):
        @sensor(job=_make_job())
        @resilient_sensor(threshold=3)
        def simple_sensor(context):
            yield SkipReason("Nothing to do")

        context = build_sensor_context(
            instance=instance, sensor_name=_SENSOR_NAME,
        )
        list(simple_sensor(context))

        assert context.cursor is None
        state = load_guard_state(instance.daemon_cursor_storage, _SENSOR_NAME)
        assert state.error_count == 0

    def test_user_reads_own_cursor_not_guard_json(self, instance):
        """User should see their own cursor value, not any guard state."""
        observed_cursors = []

        @sensor(job=_make_job())
        @resilient_sensor(threshold=5)
        def cursor_sensor(context):
            observed_cursors.append(context.cursor)
            context.update_cursor("my_value")
            yield SkipReason("OK")

        # First tick: no cursor set yet.
        context = build_sensor_context(
            instance=instance, sensor_name=_SENSOR_NAME,
        )
        list(cursor_sensor(context))
        cursor = context.cursor

        # Second tick: should see "my_value".
        context = build_sensor_context(
            cursor=cursor, instance=instance, sensor_name=_SENSOR_NAME,
        )
        list(cursor_sensor(context))

        assert observed_cursors[0] is None
        assert observed_cursors[1] == "my_value"

    def test_user_cursor_json_preserved(self, instance):
        """User cursors that are themselves JSON should roundtrip correctly."""

        @sensor(job=_make_job())
        @resilient_sensor(threshold=5)
        def json_cursor_sensor(context):
            if context.cursor:
                data = json.loads(context.cursor)
                data["count"] += 1
            else:
                data = {"count": 1}
            context.update_cursor(json.dumps(data))
            yield SkipReason("OK")

        cursor = None
        for _ in range(3):
            context = build_sensor_context(
                cursor=cursor, instance=instance, sensor_name=_SENSOR_NAME,
            )
            list(json_cursor_sensor(context))
            cursor = context.cursor

        user_data = json.loads(cursor)
        assert user_data == {"count": 3}
