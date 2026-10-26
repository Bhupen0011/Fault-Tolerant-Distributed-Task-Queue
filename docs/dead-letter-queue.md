# Dead-Letter Queue (DLQ)

When a task fails persistently and exhausts all configured retry attempts, it is transitioned to a Dead-Letter Queue state.

## Semantics
- When a task execution fails, the worker compares the incremented `retries` count against `max_retries`.
- If `retries >= max_retries`, the task is marked as `DEAD_LETTER` in PostgreSQL.
- The task is **not** silently deleted. It remains persistently visible in the database.
- It is **not** requeued automatically.
- PostgreSQL remains the authoritative source of truth. Redis does not maintain a DLQ structure; the task is simply dropped from the ephemeral processing/scheduled queues.

## Preserved Metadata
The task record in PostgreSQL preserves:
- `error_message`: The final exception/error that caused the failure.
- `retries`: The total number of failed attempts.
- `max_retries`: The maximum limit that was breached.
- `worker_id`: The ID of the last worker that attempted execution.
- `lease_id`: The final lease granted before failure.
- `updated_at`: The exact timestamp when the task was moved to the DLQ.

Execution history also remains intact via the `TaskExecution` table, allowing operators to inspect the logs of all historical attempts that led up to the dead-letter state.
