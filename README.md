# dagster-sensor-guard

A decorator that adds configurable error tolerance to Dagster sensors, suppressing transient failures and only raising errors after consecutive failure thresholds are breached.

## Installation

```bash
pip install dagster-sensor-guard
```

## Quick Start

All parameters are optional. The simplest usage with sensible defaults:

```python
from dagster import sensor, RunRequest, SkipReason
from dagster_sensor_guard import resilient_sensor

@resilient_sensor()
@sensor(job=my_job, minimum_interval_seconds=60)
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

## Consecutive count vs. time window

### Consecutive count (default)

When `window_minutes` is not set, consecutive errors accumulate regardless of how spread out they are.

```python
# Sensor runs every 5 minutes.
# After 3 consecutive failures (could span 15 minutes), the 4th raises.
@resilient_sensor(threshold=3)
@sensor(job=my_job, minimum_interval_seconds=300)
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
@resilient_sensor(
    threshold=3,
    window_minutes=10,
)
@sensor(job=my_job, minimum_interval_seconds=120)
def my_sensor(context):
    ...
```

```python
# BAD: sensor runs every 5 minutes, threshold=3, window=10 minutes.
# 10 min / 5 min = only 2 ticks fit in the window.
# You need 4 ticks (threshold+1) to raise, but only 2 fit.
# The threshold can NEVER be reached. Don't do this.
@resilient_sensor(
    threshold=3,
    window_minutes=10,  # too small!
)
@sensor(job=my_job, minimum_interval_seconds=300)
```

## Choosing the right reset strategy

### Full reset (default)

One successful tick clears the error count entirely. Simple and predictable.

```python
@resilient_sensor(threshold=3)
@sensor(job=my_job)
def my_sensor(context):
    ...
```

### Decay reset

Each success decrements the count by `decay_amount` instead of clearing it. Useful when a service is flapping — briefly recovering before failing again. The service must sustain multiple successes to fully recover trust.

```python
# Error count is 3, one success brings it to 1 (not 0).
# The service must succeed 2 more times to fully clear the count.
@resilient_sensor(
    threshold=5,
    reset_strategy="decay",
    decay_amount=2,
)
@sensor(job=my_job)
def my_sensor(context):
    ...
```

## Suppressed error callback

```python
def log_suppressed(error, count, threshold):
    logger.warning(f"Sensor error suppressed ({count}/{threshold}): {error}")

@resilient_sensor(threshold=3, on_suppressed_error=log_suppressed)
@sensor(job=my_job)
def my_sensor(context):
    ...
```

The callback is invoked each time an error is suppressed (not when the threshold is breached and the error raises).

## Cursor Transparency

The decorator transparently namespaces its state in the sensor cursor. Your existing cursor logic works without modification:

```python
@resilient_sensor()
@sensor(job=my_job)
def my_sensor(context):
    offset = int(context.cursor or "0")
    # ... process from offset ...
    context.update_cursor(str(new_offset))
```

`context.cursor` returns your value, not the internal guard state. `context.update_cursor()` works as expected.

## License

MIT
