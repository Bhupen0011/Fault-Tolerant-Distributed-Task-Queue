# Redis Priority Queue Architecture

## Redis Sorted Sets
The queue system is built on two Redis Sorted Sets:
- `queue:pending`: Tasks waiting to be processed.
- `queue:processing`: Tasks currently claimed by a worker under a processing lease.

## Priority Score Algorithm
The score for `queue:pending` is calculated as:
`(priority * 10,000,000,000,000) + timestamp_in_ms`
Where:
- Priority ranges from 0 (CRITICAL) to 3 (LOW).
- The offset `10^13` guarantees that a CRITICAL task will ALWAYS have a lower score than a HIGH task, preventing priority collisions far into the future.
- The `timestamp_in_ms` ensures FIFO behavior within the same priority band.

## Lua Atomicity
Task claiming is performed atomically via a Redis Lua script. The script:
1. Performs `ZRANGE queue:pending 0 0` to identify the highest priority task.
2. Removes it from `queue:pending` using `ZREM`.
3. Adds it to `queue:processing` using `ZADD` with a score equal to the lease expiration timestamp.
4. Returns the task payload.

This ensures that concurrent workers will never receive the exact same task payload.

## Lease Representation
The score in `queue:processing` represents the expiration time of the task's lease (`current_time_ms + lease_duration_ms`). When a lease expires, the task can be safely recovered (implemented in future phases).

## Redis/PostgreSQL Boundary
Redis is solely an ephemeral coordination layer. The processing leases in `queue:processing` are temporary state. PostgreSQL remains the permanent source of truth for task history, final results, and current states.

## Phase 2 → Phase 3 Migration
Any tasks published to the Phase 2 simple list (`tasks:published`) are automatically migrated on startup into the `queue:pending` Sorted Set with a default Priority 2 score, preventing silent loss of previously published tasks.

## Known Limitations
- The system guarantees at-least-once delivery, not exactly-once delivery. Duplicate processing can occur if a worker completes a task but crashes before acknowledging it, leading to lease expiration and recovery.
- Re-queueing logic and DLQ processing are not yet implemented.
