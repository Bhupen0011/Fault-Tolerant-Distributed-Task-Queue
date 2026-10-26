# Failure Recovery & Reliability Design

Designed to provide at-least-once delivery/processing semantics under the documented failure model.

## Transactional Outbox (Phase 2)
To prevent the API from crashing after a DB commit but before enqueuing a task in Redis, we use the Transactional Outbox Pattern.

## Lease Tokens and Ownership (Phase 4)
Visibility timeouts are strengthened with explicit ownership (lease_id) to prevent stale workers from corrupting state.

## Cooperative Cancellation (Phase 5)
Workers periodically check for `CANCEL_REQUESTED` during execution.

## Phase 4 Implementation Status
- Transactional Outbox pattern is fully implemented.
- Redis queues and expired lease detection are implemented.
- Worker recovery logic is fully implemented via lease tracking and sweeper loops.
- Cooperative cancellation logic is scheduled for future phases.
# Updates for Phase 5
Retries and Dead-Letter tracking are now implemented via worker execution handlers. See retry-policy.md.
