# Retry Policy

This document defines the semantics of task retries and backoff within the distributed task queue.

## Retry Count Semantics
- `retries` represents the **number of failed attempts** a task has undergone.
- A new task starts with `retries = 0`.
- If a task fails, `retries` is incremented by 1.
- If `retries` reaches `max_retries` (default 3), the task transitions to the `DEAD_LETTER` state.

## Exponential Backoff Formula
When a task fails but has remaining retries, it is not executed immediately. Instead, it enters a `RETRYING` state and is scheduled for future execution using an exponential backoff formula.

The precise formula used is:
```python
delay_ms = min(BASE_DELAY_MS * (2 ** (retries - 1)), MAX_DELAY_MS) + jitter
```

- **Base Delay (`BASE_DELAY_MS`)**: 2000 ms (2 seconds).
- **Max Delay (`MAX_DELAY_MS`)**: 60000 ms (60 seconds).
- **Jitter**: A random uniform value between 0 and 1000 ms added to the total delay to prevent thundering herds.

## State Transitions
1. **Initial**: `RUNNING` -> task crashes/throws.
2. Worker sets status to `RETRYING`, increments `retries`, and calculates `next_retry_at`.
3. The task is added to a Redis Sorted Set (`queue:scheduled`) with its score set to the `next_retry_at` timestamp.
4. A background `sweep_scheduled_tasks` loop polls the scheduled queue. When the time arrives, it moves the task to `queue:pending` with its **original priority preserved**.
5. The task status is updated to `QUEUED`.
6. A worker claims it and it becomes `RUNNING` again.

## Priority Preservation
A retried task retains its exact original priority. For example, a `CRITICAL` task that fails will remain a `CRITICAL` task upon its next retry execution.
