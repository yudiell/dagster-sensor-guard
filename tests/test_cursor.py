"""Tests for cursor isolation between user data and guard state."""

import json

from dagster import SkipReason, build_sensor_context, sensor

from dagster_sensor_guard import resilient_sensor
from dagster_sensor_guard.state import parse_cursor
from tests.conftest import make_job as _make_job


class TestCursorIsolation:
    def test_user_cursor_preserved_through_success(self):
        @sensor(job=_make_job())
        @resilient_sensor(threshold=5)
        def cursor_sensor(context):
            current = context.cursor or "0"
            new_val = str(int(current) + 1)
            context.update_cursor(new_val)
            yield SkipReason(f"Processed up to {new_val}")

        cursor = None
        for i in range(3):
            context = build_sensor_context(cursor=cursor)
            list(cursor_sensor(context))
            cursor = context._cursor  # noqa: SLF001

        guard_state, user_cursor = parse_cursor(cursor)
        assert user_cursor == "3"
        assert guard_state.error_count == 0

    def test_user_cursor_preserved_through_errors(self):
        tick = 0

        @sensor(job=_make_job())
        @resilient_sensor(threshold=5)
        def cursor_sensor(context):
            nonlocal tick
            tick += 1
            # First tick succeeds with cursor update, second tick fails.
            if tick == 1:
                context.update_cursor("offset_100")
                yield SkipReason("OK")
            else:
                raise ConnectionError("timeout")

        # Tick 1: success, sets cursor.
        context = build_sensor_context()
        list(cursor_sensor(context))
        cursor = context._cursor  # noqa: SLF001

        _, user_cursor = parse_cursor(cursor)
        assert user_cursor == "offset_100"

        # Tick 2: failure, user cursor should still be preserved.
        context = build_sensor_context(cursor=cursor)
        list(cursor_sensor(context))
        cursor = context._cursor  # noqa: SLF001

        guard_state, user_cursor = parse_cursor(cursor)
        assert user_cursor == "offset_100"
        assert guard_state.error_count == 1

    def test_sensor_with_no_cursor_usage(self):
        @sensor(job=_make_job())
        @resilient_sensor(threshold=3)
        def simple_sensor(context):
            yield SkipReason("Nothing to do")

        context = build_sensor_context()
        list(simple_sensor(context))
        cursor = context._cursor  # noqa: SLF001

        guard_state, user_cursor = parse_cursor(cursor)
        assert user_cursor is None
        assert guard_state.error_count == 0

    def test_user_reads_own_cursor_not_guard_json(self):
        """User should see their own cursor value, not the raw JSON with guard state."""
        observed_cursors = []

        @sensor(job=_make_job())
        @resilient_sensor(threshold=5)
        def cursor_sensor(context):
            observed_cursors.append(context.cursor)
            context.update_cursor("my_value")
            yield SkipReason("OK")

        # First tick: no cursor set yet.
        context = build_sensor_context()
        list(cursor_sensor(context))
        cursor = context._cursor  # noqa: SLF001

        # Second tick: should see "my_value", not the JSON envelope.
        context = build_sensor_context(cursor=cursor)
        list(cursor_sensor(context))

        assert observed_cursors[0] is None
        assert observed_cursors[1] == "my_value"

    def test_user_cursor_json_preserved(self):
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
            context = build_sensor_context(cursor=cursor)
            list(json_cursor_sensor(context))
            cursor = context._cursor  # noqa: SLF001

        _, user_cursor = parse_cursor(cursor)
        user_data = json.loads(user_cursor)
        assert user_data == {"count": 3}
