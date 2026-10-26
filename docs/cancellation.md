# Cooperative Cancellation

The system supports cancelling tasks both before they start and while they are currently executing.

## Cancellation Flow
Clients can issue a POST request to `/api/tasks/{task_id}/cancel`.
1. **Queued Tasks (`PENDING`, `QUEUED`, `RETRYING`)**:
   - The task is immediately marked as `CANCELLED` in PostgreSQL.
   - When a worker subsequently claims the task from Redis, it validates the DB state. Seeing it `CANCELLED`, it aborts execution immediately.
2. **Running Tasks (`RUNNING`)**:
   - The task is marked as `CANCEL_REQUESTED`.
   - The worker executing the task is responsible for observing this request and stopping itself.

## Cooperative Design
Task cancellation for `RUNNING` tasks is entirely **cooperative**. The system does not force-kill the underlying OS thread or process. 
- A long-running task handler must proactively poll the database to determine if `CANCEL_REQUESTED` has been set.
- If it detects a cancellation, the handler should safely unwind its state and raise an `asyncio.CancelledError`.
- **Limitation**: Non-cooperative or poorly-written handlers that block the event loop or fail to yield/poll will not be terminated immediately and will continue executing until they finish naturally or crash.

## Race Conditions
Races are resolved deterministically using PostgreSQL as the source of truth:
- **Cancel vs Completion**: If a worker completes a task just as a user issues a cancel request, the cancel endpoint will return a HTTP 400 error because the task has already reached a terminal state (`COMPLETED`).
- **Cancel vs Retry**: If a task is cancelled while sitting in the backoff (`RETRYING`) queue, the scheduled background sweeper will observe the `CANCELLED` state and permanently discard the task from the scheduled queue without executing it.
