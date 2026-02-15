# Feature Request: Per-Key Failure Tracking Within a Single Sensor

> **Status: Implemented** — Shipped in the `per-key-failure-tracking` branch.
> See the [Per-Key Failure Tracking section in README.md](README.md#per-key-failure-tracking) for usage documentation.

## Summary

Add support for tracking failure thresholds independently per item/key within a single sensor evaluation, rather than only at the sensor level. This enables sensors that process multiple independent data sources to continue operating when one source experiences transient failures, without affecting the others.

---

## Current Behavior

`dagster-sensor-guard` tracks a single failure counter per sensor. When any exception occurs during sensor evaluation, the counter increments. After reaching the threshold, the next failure raises to Dagster. A successful tick resets the counter.

This works well for sensors that check a single resource, but falls short for sensors that iterate over multiple independent resources.

---

## The Problem

Consider a sensor that monitors multiple database tables, S3 buckets, API endpoints, or message queues:

```python
@sensor(job=my_job)
@resilient_sensor(threshold=3)
def multi_table_sensor(context):
    for table in ["orders", "customers", "inventory"]:
        max_timestamp = query_table(table)
        if has_new_data(table, max_timestamp):
            yield RunRequest(run_key=f"{table}_{max_timestamp}")
```

### Problem 1: One failure stops all processing

If `query_table("orders")` raises an exception, the sensor stops immediately. `customers` and `inventory` are never checked during that tick, even though they may be perfectly healthy.

### Problem 2: Failure counter conflates unrelated sources

- Tick 1: `orders` fails → counter = 1
- Tick 2: `customers` fails → counter = 2
- Tick 3: `inventory` fails → counter = 3, error raised

The sensor now reports a persistent failure, but no single table actually failed 3 times. Each table only failed once. This is a false positive that masks the true health of each individual source.

### Problem 3: One flaky source poisons the entire sensor

If `orders` is intermittently slow and times out occasionally, it can consume the entire failure budget. Meanwhile, `customers` and `inventory` may be completely stable, but their processing becomes unreliable because `orders` keeps pushing the sensor toward its threshold.

---

## Implemented Solution

The implemented API uses `per_key=True` on the decorator, injecting a `SensorGuard` as the second parameter:

```python
@sensor(job=my_job)
@resilient_sensor(threshold=3, per_key=True)
def multi_table_sensor(context, guard):
    for table in ["orders", "customers", "inventory"]:
        with guard.track(table):
            max_timestamp = query_table(table)
            if has_new_data(table, max_timestamp):
                yield RunRequest(run_key=f"{table}_{max_timestamp}")
```

### Behavior

1. Each `key` maintains its own independent failure counter
2. When an exception occurs inside `guard.track(key)`:
   - If below threshold: exception is suppressed, `on_suppressed_error` callback invoked, loop continues to next item
   - If at/above threshold: error is collected in `breached_keys` (not raised immediately), loop continues
3. After the loop completes, if any keys exceeded their threshold, a `SensorGuardKeyError` is raised containing all breached keys
4. Successful execution of a key resets only that key's counter (respecting `reset_strategy`)
5. All per-key state is persisted in `daemon_cursor_storage` in a single batch write per tick, under the key `dagster_sensor_guard:{sensor_name}:keys` (separate from the sensor-level key)
6. Exceptions raised outside `guard.track()` fall back to sensor-level tracking (identical to `per_key=False` behavior)

### Design decisions

- **`per_key=False` by default** — existing sensors are completely unaffected, zero code path changes
- **Guard injected as second parameter** — not attached to Dagster's context object, avoiding fragility on Dagster upgrades
- **Batch KVS operations** — one load at construction + one save after the loop, regardless of key count
- **Raise after full iteration** — all keys are processed before any breach surfaces, maximizing useful work
- **Validation at decoration time** — `per_key=True` with a single-parameter function raises `TypeError` immediately

---

## Alternative API Options Considered

### Option A: Implicit guard injection (chosen approach, modified)

The implemented design combines the best of Options A and B: `per_key=True` enables the feature, and the `SensorGuard` is injected as a second parameter.

### Option B: Attach to context object

```python
@sensor(job=my_job)
@resilient_sensor(threshold=3, per_key=True)
def my_sensor(context):
    with context.resilient_guard.track(table):
        ...
```

Rejected — modifying Dagster's context object is fragile and could break on Dagster upgrades.

### Option C: Callback-based

```python
yield from guard.map(tables, process_table, key=lambda t: t)
```

Rejected — less flexible, doesn't support `yield RunRequest` inside the callback naturally.

---

## Future Configuration Options

These were deferred from V1 and can be added later without breaking changes:

| Parameter           | Description                                                                             |
| ------------------- | --------------------------------------------------------------------------------------- |
| `per_key_threshold` | Override threshold for per-key tracking (if different from sensor-level)                |
| `fail_fast`         | If `True`, raise immediately when first key exceeds threshold instead of collecting all |
| `max_keys`          | Limit number of tracked keys to prevent unbounded state growth                          |
| `key_ttl_minutes`   | Automatically expire keys not seen within this window                                   |

---

## State Storage

Per-key state is stored in a separate KVS key from the sensor-level state:

- Sensor-level: `dagster_sensor_guard:{sensor_name}` (unchanged)
- Per-key: `dagster_sensor_guard:{sensor_name}:keys`

The per-key value is a JSON object mapping key names to `GuardState` dicts:

```json
{
  "orders": { "error_count": 2, "first_error_ts": 1739600000.0, "last_error_ts": 1739601000.0 },
  "customers": { "error_count": 0, "first_error_ts": null, "last_error_ts": null },
  "inventory": { "error_count": 1, "first_error_ts": 1739599500.0, "last_error_ts": 1739599500.0 }
}
```

`window_minutes` and `reset_strategy` apply independently to each key.

---

## Real-World Use Cases

1. **Multi-table CDC sensors**: Monitor multiple landing tables for new data arrival
2. **Multi-bucket file sensors**: Watch several S3 buckets/prefixes for new files
3. **Multi-tenant API polling**: Check multiple customer accounts via API
4. **Multi-region health checks**: Verify services across regions, isolate regional outages
5. **Multi-queue consumers**: Process messages from multiple SQS/Kafka topics

---

## Workaround Without This Feature

Currently, users must either:

1. **Create separate sensors per resource** — increases boilerplate, clutters the Dagster UI, and doesn't scale when resources are dynamic
2. **Implement custom per-key tracking** — requires manual cursor management, duplicates the logic that `dagster-sensor-guard` already provides at the sensor level
