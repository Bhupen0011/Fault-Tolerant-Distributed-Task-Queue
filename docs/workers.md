# Worker Architecture

The Worker is an independently scalable Python container that claims tasks from Redis and executes them. It ensures tasks are processed safely and concurrently.

## Worker Registration
Upon startup, each worker generates a unique `worker_id` (UUID) and registers itself in the PostgreSQL `worker` table with a `running` status and its hostname.

## Heartbeats
Workers run a background asyncio loop that periodically publishes heartbeats:
1. **Redis**: Sets a key `worker:heartbeat:{worker_id}` with a short TTL (e.g., 10 seconds). This allows for near-instant liveness detection.
2. **PostgreSQL**: Updates the `last_heartbeat` timestamp in the `worker` table.

## Task Execution Flow
1. Worker executes the Lua `claim_task` script, atomically moving the highest priority task from `queue:pending` to `queue:processing` in Redis.
2. Worker conditionally updates the task in PostgreSQL from `QUEUED` to `RUNNING`, establishing authoritative ownership with its `worker_id` and a newly generated `lease_id`.
3. Worker creates a `TaskExecution` record to track this specific attempt.
4. Worker executes the task (e.g. `sleep`, `fail`, `done`).
5. On completion, the worker conditionally updates PostgreSQL to `COMPLETED` or `FAILED` (only if the lease hasn't expired/changed), and removes the task from Redis `queue:processing`.

## Concurrency Strategy
Multiple worker containers can run simultaneously (`worker-1`, `worker-2`, etc.). The Redis Lua script guarantees they never receive the same task payload. PostgreSQL conditional updates ensure that even in race conditions, only one worker can gain authoritative database ownership.

## Known Limitations
- Exactly-once execution is not guaranteed. A worker could finish execution but crash before updating PostgreSQL to `COMPLETED`, leading to the task being recovered and executed again by another worker (at-least-once).
# Updates for Phase 5
Workers now handle cooperative cancellation polling and scheduling delayed retries via queue:scheduled.
