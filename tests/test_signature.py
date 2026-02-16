"""Tests for function signature preservation and Dagster compatibility.

Verifies that @resilient_sensor is fully transparent to Dagster's
introspection: resource injection, signature inspection, type hints,
and metadata preservation all work identically to an unwrapped sensor.
"""

import inspect
import typing

import pytest
from dagster import (
    ConfigurableResource,
    RunRequest,
    SensorEvaluationContext,
    SkipReason,
    build_sensor_context,
    sensor,
)
from dagster._core.decorator_utils import get_function_params
from dagster._core.definitions.resource_annotation import get_resource_args

from dagster_sensor_guard import SensorGuard, resilient_sensor
from tests.conftest import make_job as _make_job

_SENSOR_NAME = "test_sig_sensor"


class FakeResource(ConfigurableResource):
    url: str = "http://default"


class AnotherResource(ConfigurableResource):
    token: str = "secret"


def _invoke_sensor(sensor_def, instance, resources=None, sensor_name=_SENSOR_NAME):
    context = build_sensor_context(
        instance=instance,
        sensor_name=sensor_name,
        resources=resources or {},
    )
    results = list(sensor_def(context))
    return results, context


# ---------------------------------------------------------------------------
# Signature preservation
# ---------------------------------------------------------------------------

class TestSignaturePreservation:
    def test_per_key_false_preserves_original_signature(self):
        """Wrapper signature matches the original function exactly."""

        def my_sensor(context: SensorEvaluationContext):
            pass

        wrapped = resilient_sensor(threshold=3)(my_sensor)
        sig = inspect.signature(wrapped)
        assert list(sig.parameters.keys()) == ["context"]

    def test_per_key_false_preserves_resource_params(self):
        """Resource parameters are visible in the wrapper signature."""

        def my_sensor(
            context: SensorEvaluationContext,
            fake: FakeResource,
            another: AnotherResource,
        ):
            pass

        wrapped = resilient_sensor(threshold=3)(my_sensor)
        sig = inspect.signature(wrapped)
        assert list(sig.parameters.keys()) == ["context", "fake", "another"]

    def test_per_key_true_hides_guard_from_signature(self):
        """Guard parameter is invisible to Dagster; only context + resources visible."""

        def my_sensor(
            context: SensorEvaluationContext,
            guard: SensorGuard,
            fake: FakeResource,
        ):
            pass

        wrapped = resilient_sensor(threshold=3, per_key=True)(my_sensor)
        sig = inspect.signature(wrapped)
        param_names = list(sig.parameters.keys())
        assert "guard" not in param_names
        assert param_names == ["context", "fake"]

    def test_per_key_true_guard_only_shows_context(self):
        """With per_key=True and no resources, only context is visible."""

        def my_sensor(context: SensorEvaluationContext, guard: SensorGuard):
            pass

        wrapped = resilient_sensor(threshold=3, per_key=True)(my_sensor)
        sig = inspect.signature(wrapped)
        assert list(sig.parameters.keys()) == ["context"]


# ---------------------------------------------------------------------------
# Dagster introspection compatibility
# ---------------------------------------------------------------------------

class TestDagsterIntrospection:
    def test_dagster_detects_resource_params(self):
        """Dagster's get_resource_args sees resource parameters through the wrapper."""

        def my_sensor(context: SensorEvaluationContext, fake: FakeResource):
            pass

        wrapped = resilient_sensor(threshold=3)(my_sensor)
        resource_params = get_resource_args(wrapped)
        assert len(resource_params) == 1
        assert resource_params[0].name == "fake"

    def test_dagster_detects_multiple_resources(self):
        """Multiple resource parameters are all detected."""

        def my_sensor(
            context: SensorEvaluationContext,
            fake: FakeResource,
            another: AnotherResource,
        ):
            pass

        wrapped = resilient_sensor(threshold=3)(my_sensor)
        resource_params = get_resource_args(wrapped)
        names = {p.name for p in resource_params}
        assert names == {"fake", "another"}

    def test_dagster_detects_resources_with_per_key(self):
        """Resources are detected even with per_key=True (guard is hidden)."""

        def my_sensor(
            context: SensorEvaluationContext,
            guard: SensorGuard,
            fake: FakeResource,
        ):
            pass

        wrapped = resilient_sensor(threshold=3, per_key=True)(my_sensor)
        resource_params = get_resource_args(wrapped)
        assert len(resource_params) == 1
        assert resource_params[0].name == "fake"

    def test_dagster_identifies_context_param(self):
        """Dagster correctly identifies the context parameter by name."""

        def my_sensor(ctx: SensorEvaluationContext, fake: FakeResource):
            pass

        wrapped = resilient_sensor(threshold=3)(my_sensor)
        params = get_function_params(wrapped)
        resource_names = {p.name for p in get_resource_args(wrapped)}
        context_name = next(
            p.name for p in params if p.name not in resource_names
        )
        assert context_name == "ctx"


# ---------------------------------------------------------------------------
# Annotations and type hints
# ---------------------------------------------------------------------------

class TestAnnotationPreservation:
    def test_annotations_contain_resolved_types(self):
        """Annotations are pre-resolved types, not strings."""

        def my_sensor(context: SensorEvaluationContext, fake: FakeResource):
            pass

        wrapped = resilient_sensor(threshold=3)(my_sensor)
        annotations = wrapped.__annotations__
        assert annotations["context"] is SensorEvaluationContext
        assert annotations["fake"] is FakeResource

    def test_per_key_annotations_exclude_guard(self):
        """Guard parameter is removed from annotations."""

        def my_sensor(
            context: SensorEvaluationContext,
            guard: SensorGuard,
            fake: FakeResource,
        ):
            pass

        wrapped = resilient_sensor(threshold=3, per_key=True)(my_sensor)
        assert "guard" not in wrapped.__annotations__
        assert "fake" in wrapped.__annotations__
        assert "context" in wrapped.__annotations__

    def test_return_annotation_preserved(self):
        """Return type annotation is preserved."""
        from dagster import SensorResult

        def my_sensor(context: SensorEvaluationContext) -> SensorResult:
            pass

        wrapped = resilient_sensor(threshold=3)(my_sensor)
        hints = typing.get_type_hints(wrapped)
        assert hints.get("return") is SensorResult


# ---------------------------------------------------------------------------
# Metadata preservation
# ---------------------------------------------------------------------------

class TestMetadataPreservation:
    def test_name_preserved(self):
        def my_special_sensor(context: SensorEvaluationContext):
            pass

        wrapped = resilient_sensor(threshold=3)(my_special_sensor)
        assert wrapped.__name__ == "my_special_sensor"

    def test_qualname_preserved(self):
        def my_special_sensor(context: SensorEvaluationContext):
            pass

        wrapped = resilient_sensor(threshold=3)(my_special_sensor)
        assert "my_special_sensor" in wrapped.__qualname__

    def test_module_preserved(self):
        def my_sensor(context: SensorEvaluationContext):
            pass

        wrapped = resilient_sensor(threshold=3)(my_sensor)
        assert wrapped.__module__ == my_sensor.__module__

    def test_docstring_preserved(self):
        def my_sensor(context: SensorEvaluationContext):
            """Monitor the flux capacitor."""
            pass

        wrapped = resilient_sensor(threshold=3)(my_sensor)
        assert wrapped.__doc__ == "Monitor the flux capacitor."

    def test_wrapped_points_to_original(self):
        def my_sensor(context: SensorEvaluationContext):
            pass

        wrapped = resilient_sensor(threshold=3)(my_sensor)
        assert wrapped.__wrapped__ is my_sensor


# ---------------------------------------------------------------------------
# Resource injection end-to-end
# ---------------------------------------------------------------------------

class TestResourceInjection:
    def test_resource_injected_per_key_false(self, instance):
        """Resources are forwarded to the original function (per_key=False)."""
        received = {}

        @sensor(job=_make_job())
        @resilient_sensor(threshold=3)
        def resource_sensor(context: SensorEvaluationContext, fake: FakeResource):
            received["fake"] = fake
            yield SkipReason("ok")

        ctx = build_sensor_context(
            instance=instance,
            sensor_name=_SENSOR_NAME,
            resources={"fake": FakeResource(url="http://injected")},
        )
        results = list(resource_sensor(ctx))
        assert len(results) == 1
        assert isinstance(results[0], SkipReason)
        assert received["fake"].url == "http://injected"

    def test_resource_injected_per_key_true(self, instance):
        """Resources are forwarded alongside the guard (per_key=True)."""
        received = {}

        @sensor(job=_make_job())
        @resilient_sensor(threshold=3, per_key=True)
        def resource_sensor(
            context: SensorEvaluationContext,
            guard: SensorGuard,
            fake: FakeResource,
        ):
            received["guard"] = guard
            received["fake"] = fake
            with guard.track("key1"):
                pass

        ctx = build_sensor_context(
            instance=instance,
            sensor_name=_SENSOR_NAME,
            resources={"fake": FakeResource(url="http://pk-injected")},
        )
        results = list(resource_sensor(ctx))
        assert isinstance(received["guard"], SensorGuard)
        assert received["fake"].url == "http://pk-injected"

    def test_multiple_resources_injected(self, instance):
        """Multiple resources are all forwarded correctly."""
        received = {}

        @sensor(job=_make_job())
        @resilient_sensor(threshold=3)
        def multi_resource_sensor(
            context: SensorEvaluationContext,
            fake: FakeResource,
            another: AnotherResource,
        ):
            received["fake"] = fake
            received["another"] = another
            yield SkipReason("ok")

        ctx = build_sensor_context(
            instance=instance,
            sensor_name=_SENSOR_NAME,
            resources={
                "fake": FakeResource(url="http://multi"),
                "another": AnotherResource(token="abc"),
            },
        )
        results = list(multi_resource_sensor(ctx))
        assert len(results) == 1
        assert received["fake"].url == "http://multi"
        assert received["another"].token == "abc"

    def test_resource_with_error_suppression(self, instance):
        """Resource injection works alongside error suppression."""
        received_urls = []

        @sensor(job=_make_job())
        @resilient_sensor(threshold=3)
        def failing_resource_sensor(
            context: SensorEvaluationContext, fake: FakeResource,
        ):
            received_urls.append(fake.url)
            raise ConnectionError("timeout")

        for _ in range(2):
            ctx = build_sensor_context(
                instance=instance,
                sensor_name=_SENSOR_NAME,
                resources={"fake": FakeResource(url="http://retry")},
            )
            results = list(failing_resource_sensor(ctx))
            assert isinstance(results[0], SkipReason)

        assert received_urls == ["http://retry", "http://retry"]

    def test_resource_with_run_requests(self, instance):
        """Resources work with sensors that yield RunRequests."""

        @sensor(job=_make_job())
        @resilient_sensor(threshold=3)
        def run_sensor(
            context: SensorEvaluationContext, fake: FakeResource,
        ):
            yield RunRequest(run_key=f"run-{fake.url}")

        ctx = build_sensor_context(
            instance=instance,
            sensor_name=_SENSOR_NAME,
            resources={"fake": FakeResource(url="http://run")},
        )
        results = list(run_sensor(ctx))
        assert len(results) == 1
        assert results[0].run_key == "run-http://run"

    def test_resource_with_per_key_error_tracking(self, instance):
        """Resources + per_key error tracking work together end-to-end."""
        received = {}

        @sensor(job=_make_job())
        @resilient_sensor(threshold=3, per_key=True)
        def per_key_resource_sensor(
            context: SensorEvaluationContext,
            guard: SensorGuard,
            fake: FakeResource,
        ):
            received["url"] = fake.url
            for table in ["orders", "customers"]:
                with guard.track(table):
                    if table == "orders":
                        raise ConnectionError(f"{table} down")
                    yield RunRequest(run_key=f"{table}-{fake.url}")

        ctx = build_sensor_context(
            instance=instance,
            sensor_name=_SENSOR_NAME,
            resources={"fake": FakeResource(url="http://pk")},
        )
        results = list(per_key_resource_sensor(ctx))
        assert received["url"] == "http://pk"
        run_keys = [r.run_key for r in results if isinstance(r, RunRequest)]
        assert "customers-http://pk" in run_keys
        assert not any("orders" in k for k in run_keys)


# ---------------------------------------------------------------------------
# Full @sensor stack integration
# ---------------------------------------------------------------------------

class TestSensorStackIntegration:
    def test_sensor_definition_has_correct_name(self):
        """@sensor sees the correct function name through the wrapper."""

        @sensor(job=_make_job())
        @resilient_sensor(threshold=3)
        def my_named_sensor(context: SensorEvaluationContext, fake: FakeResource):
            pass

        assert my_named_sensor.name == "my_named_sensor"

    def test_sensor_definition_with_per_key_has_correct_name(self):
        """@sensor sees correct name even with per_key guard injection."""

        @sensor(job=_make_job())
        @resilient_sensor(threshold=3, per_key=True)
        def my_pk_sensor(
            context: SensorEvaluationContext,
            guard: SensorGuard,
            fake: FakeResource,
        ):
            pass

        assert my_pk_sensor.name == "my_pk_sensor"

    def test_custom_context_param_name(self, instance):
        """Sensors with non-standard context param names work correctly."""

        @sensor(job=_make_job())
        @resilient_sensor(threshold=3)
        def custom_sensor(ctx: SensorEvaluationContext, fake: FakeResource):
            yield SkipReason(f"got {fake.url}")

        context = build_sensor_context(
            instance=instance,
            sensor_name=_SENSOR_NAME,
            resources={"fake": FakeResource(url="http://custom")},
        )
        results = list(custom_sensor(context))
        assert results[0].skip_message == "got http://custom"
