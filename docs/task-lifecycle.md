# Task Lifecycle

Tasks move through the following explicit states. 

*   **PENDING**: (Phase 2) Task created and saved to DB (along with OutboxEvent).
*   **QUEUED**: (Phase 2) Task successfully published to Redis via Outbox Dispatcher.
*   **RUNNING**: (Phase 4) A worker holds a valid lease and is processing the task.
*   **COMPLETED**: (Phase 4) Task finished successfully. Result is in the DB.
*   **FAILED**: (Phase 4) Task encountered an error but hasn't exceeded its retry limit.
*   **RETRYING**: (Phase 5) Task scheduled to re-run (exponential backoff).
*   **CANCEL_REQUESTED**: (Phase 5) Client requested cancellation while the task was `RUNNING`.
*   **CANCELLED**: (Phase 5) Task cancelled from `QUEUED`, or worker observed `CANCEL_REQUESTED` and aborted.
*   **DEAD_LETTER**: (Phase 5) Task failed repeatedly and exhausted its max retries. Requires manual intervention.

## Phase 4 Implementation Status
- PENDING, QUEUED, RUNNING, COMPLETED, and FAILED transitions are fully implemented.
- RETRYING, CANCELLED, and DEAD_LETTER states are not yet implemented.
# Updates for Phase 5
See retry-policy.md, cancellation.md, and dead-letter-queue.md for new state transitions.
