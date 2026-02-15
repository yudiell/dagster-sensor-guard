# dagster-sensor-guard

> **Disclaimer:** This is a community project and is not affiliated with, endorsed by, or maintained by Dagster Labs.

A decorator that adds configurable error tolerance to Dagster sensors, suppressing transient failures and only raising errors after consecutive failure thresholds are breached.

## Installation

```bash
pip install dagster-sensor-guard
```

## Quick Start

> **`@resilient_sensor` must be placed below `@sensor`, not above it.** It wraps the raw function before `@sensor` processes it. Reversing the order will raise a `TypeError`.

All parameters are optional. The simplest usage with sensible defaults:

```python
from dagster import sensor, RunRequest, SkipReason
from dagster_sensor_guard import resilient_sensor

@sensor(job=my_job, minimum_interval_seconds=60)
@resilient_sensor()
def my_sensor(context):
    new_files = check_for_new_files()
    if new_files:
        for f in new_files:
            yield RunRequest(run_key=f)
    else:
        yield SkipReason("No new files")
```

Errors 1 through 3 are suppressed with a `SkipReason`, e.g. `Suppressed transient error (2/3): Connection timed out`. Error 4 raises to Dagster normally. A single successful tick resets the counter.

## Parameters

All parameters are **optional** and have defaults. There are no required parameters.

### Core parameters

| Parameter | Type | Default | Description |
| --- | --- | --- | --- |
| `threshold` | `int` | `3` | Consecutive errors to tolerate. Errors 1 through `threshold` are suppressed; error `threshold + 1` raises to Dagster. |
| `window_minutes` | `int` | `None` | Optional rolling time window in minutes. When set, only consecutive errors within this window count toward the threshold. When omitted, consecutive errors are counted with no time constraint. |
| `reset_strategy` | `str` | `"full"` | `"full"` or `"decay"`. Determines how success resets the error count. See below. |
| `decay_amount` | `int` | `1` | How much to subtract from the error count per successful tick. **Only used when `reset_strategy="decay"`**. |
| `on_suppressed_error` | `Callable` | `None` | Optional callback invoked each time an error is suppressed. Signature: `(error: Exception, count: int, threshold: int) -> None`. |
| `per_key` | `bool` | `False` | When `True`, a `SensorGuard` is injected as the second parameter for independent per-key failure tracking. See [Per-Key Failure Tracking](#per-key-failure-tracking). |

## Consecutive count vs. time window

### Consecutive count (default)

When `window_minutes` is not set, consecutive errors accumulate regardless of how spread out they are.

```python
# Sensor runs every 5 minutes.
# After 3 consecutive failures (could span 15 minutes), the 4th raises.
@sensor(job=my_job, minimum_interval_seconds=300)
@resilient_sensor(threshold=3)
def my_sensor(context):
    ...
```

### Adding a time window

When `window_minutes` is set, only errors clustered within that window count. Failures spread over a long time aren't concerning.

**Important**: `window_minutes` must be large enough to fit `threshold + 1` sensor ticks. Otherwise the threshold can never be reached within the window.

Formula: `window_minutes > (threshold + 1) * (minimum_interval_seconds / 60)`

```python
# Sensor runs every 2 minutes, threshold=3, window=10 minutes.
# 10 min / 2 min = 5 ticks fit in the window — enough to hit threshold+1 (4).
# If 3 consecutive errors happen within 10 minutes, the 4th raises.
# If errors are spread over more than 10 minutes, the counter resets.
@sensor(job=my_job, minimum_interval_seconds=120)
@resilient_sensor(
    threshold=3,
    window_minutes=10,
)
def my_sensor(context):
    ...
```

```python
# BAD: sensor runs every 5 minutes, threshold=3, window=10 minutes.
# 10 min / 5 min = only 2 ticks fit in the window.
# You need 4 ticks (threshold+1) to raise, but only 2 fit.
# The threshold can NEVER be reached. Don't do this.
@sensor(job=my_job, minimum_interval_seconds=300)
@resilient_sensor(
    threshold=3,
    window_minutes=10,  # too small!
)
```

## Choosing the right reset strategy

### Full reset (default)

One successful tick clears the error count entirely. Simple and predictable.

```python
@sensor(job=my_job)
@resilient_sensor(threshold=3)
def my_sensor(context):
    ...
```

### Decay reset

Each success decrements the count by `decay_amount` instead of clearing it. Useful when a service is flapping — briefly recovering before failing again. The service must sustain multiple successes to fully recover trust.

```python
# Error count is 3, one success brings it to 1 (not 0).
# The service must succeed 2 more times to fully clear the count.
@sensor(job=my_job)
@resilient_sensor(
    threshold=5,
    reset_strategy="decay",
    decay_amount=2,
)
def my_sensor(context):
    ...
```

## Per-Key Failure Tracking

For sensors that iterate over multiple independent resources (tables, buckets, APIs), a single failure normally kills the entire tick. With `per_key=True`, each resource is tracked independently — one failing key doesn't block the others.

```python
from dagster import sensor, RunRequest
from dagster_sensor_guard import resilient_sensor

@sensor(job=my_job)
@resilient_sensor(threshold=3, per_key=True)
def multi_table_sensor(context, guard):
    for table in ["orders", "customers", "inventory"]:
        with guard.track(table):
            max_ts = query_table(table)
            if has_new_data(table, max_ts):
                yield RunRequest(run_key=f"{table}_{max_ts}")
```

When `per_key=True`, a `SensorGuard` is injected as the second parameter. Wrap each independent unit of work with `guard.track(key)`:

- **Success** resets that key's error counter (respecting `reset_strategy`)
- **Error below threshold** is suppressed for that key; the loop continues to the next key
- **Error at threshold** collects the key; after all keys are processed, a `SensorGuardKeyError` is raised containing all breached keys

RunRequests from healthy keys are yielded even when other keys fail. All keys are always processed before any breach surfaces.

### Handling breached keys

```python
from dagster_sensor_guard import SensorGuardKeyError

try:
    list(my_sensor(context))
except SensorGuardKeyError as e:
    print(e.breached_keys)  # {"orders": ConnectionError(...), "inventory": TimeoutError(...)}
```

### Errors outside `guard.track()`

Exceptions raised outside a `guard.track()` block fall back to the sensor-level tracking (the same behavior as `per_key=False`).

### All parameters work with per-key

`window_minutes`, `reset_strategy`, `decay_amount`, and `on_suppressed_error` all apply independently per key:

```python
@sensor(job=my_job)
@resilient_sensor(
    threshold=5,
    per_key=True,
    reset_strategy="decay",
    decay_amount=1,
    window_minutes=30,
    on_suppressed_error=lambda err, count, threshold: logger.warning(
        f"Key error suppressed ({count}/{threshold}): {err}"
    ),
)
def my_sensor(context, guard):
    for table in tables:
        with guard.track(table):
            ...
```

## Suppressed error callback

```python
def log_suppressed(error, count, threshold):
    logger.warning(f"Sensor error suppressed ({count}/{threshold}): {error}")

@sensor(job=my_job)
@resilient_sensor(threshold=3, on_suppressed_error=log_suppressed)
def my_sensor(context):
    ...
```

The callback is invoked each time an error is suppressed (not when the threshold is breached and the error raises).

## Cursor Transparency

Guard state is stored in Dagster's `daemon_cursor_storage` (a SQL-backed key-value store), completely separate from your sensor cursor. Your cursor flows through Dagster natively, untouched:

```python
@sensor(job=my_job)
@resilient_sensor()
def my_sensor(context):
    offset = int(context.cursor or "0")
    # ... process from offset ...
    context.update_cursor(str(new_offset))
```

`context.cursor` is always your value. `context.update_cursor()` works as expected. The Dagster UI shows your cursor, not guard internals.

## Logging

When using `per_key=True`, the decorator logs a tick summary at **WARNING** level after each tick so it's always visible in the `dagster dev` console:

```
dagster.sensor_guard - WARNING - [multi_table_sensor] tick summary: 2 ok, 1 suppressed, 0 breached
dagster.sensor_guard - WARNING - [multi_table_sensor] tick summary: 2 ok, 0 suppressed, 1 breached [inventory]
```

Per-key outcomes (ok, suppressed, breached) are logged at **INFO** level. To see them in `dagster dev`, raise the code server log level:

```bash
dagster dev --code-server-log-level info
```

```
dagster.sensor_guard - INFO - [multi_table_sensor] key 'orders': ok
dagster.sensor_guard - INFO - [multi_table_sensor] key 'customers': error suppressed (1/3) - Connection timed out
dagster.sensor_guard - INFO - [multi_table_sensor] key 'inventory': error exceeded threshold (4/3) - Connection timed out
```

## License

MIT
