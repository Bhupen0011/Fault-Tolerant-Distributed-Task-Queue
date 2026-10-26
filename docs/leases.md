# Lease Management

Tasks claimed by a worker are granted a processing lease to prevent them from being permanently stuck if the worker crashes.

## Lease Design
- **Redis**: The task is placed in the `queue:processing` Sorted Set. The score is the Unix timestamp of when the lease expires (`now + lease_duration`).
- **PostgreSQL**: The `task` table is updated with `lease_id` (a unique UUID for this execution attempt) and `lease_expires_at`. 

## PostgreSQL Ownership Mechanism
PostgreSQL is the authoritative source of ownership. Even if Redis grants a task, the worker must successfully update PostgreSQL (using conditional updates) to claim actual ownership.

## Lease Renewal
While a task is executing, the worker runs a background loop to periodically extend the lease in both Redis and PostgreSQL. The worker checks that it still holds the current `lease_id` before extending.

## Failure Recovery Flow
If Worker A crashes:
1. Its heartbeat stops and its Redis heartbeat key expires.
2. Its active task leases naturally expire (time > `lease_expires_at`).
3. A background sweeper detects the expired score in `queue:processing`.
4. The sweeper verifies the task is indeed expired in PostgreSQL.
5. The task is transitioned back to `QUEUED` and re-inserted into `queue:pending`.
6. Worker B claims the task, generating a brand new `lease_id`.

## Stale-Worker Protection
If Worker A wakes up from a long garbage-collection pause or network partition *after* Worker B has reclaimed the task, Worker A will attempt to update the task state or renew the lease. However, Worker A's conditional update (`WHERE task.lease_id = A`) will fail, because the database now holds `lease_id = B`. Worker A's stale updates are safely rejected.
# Updates for Phase 5
A new lease is explicitly required for every retry attempt. Leases are not carried over after failure.
