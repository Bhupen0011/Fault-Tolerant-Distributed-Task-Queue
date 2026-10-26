# Transactional Outbox Pattern

## Why the Outbox Pattern is Needed
When creating a task, the API must save the task to PostgreSQL and enqueue it in Redis. If the API crashes after committing to PostgreSQL but before enqueuing in Redis, the task is permanently lost to the queue. The Transactional Outbox Pattern prevents this by making the database write and the queue message intent atomic.

## Transaction Boundaries
1. The API creates a `Task` and an `OutboxEvent` in a single PostgreSQL transaction.
2. The Outbox Dispatcher polls the database for unpublished `OutboxEvent`s.

## Dispatcher Behavior
The dispatcher continuously polls for `published_at IS NULL` events. It selects them using `SKIP LOCKED` to prevent multiple dispatcher instances from processing the same event concurrently. It publishes the event to Redis, then marks the event as published and updates the task status to `QUEUED` in the database.

## Retry Behavior
If Redis publication fails, the dispatcher increments the `attempts` counter and logs the `last_error` in the `OutboxEvent`. It will retry publishing on the next polling cycle.

## Duplicate Publication and At-Least-Once Semantics
The dispatcher provides **At-Least-Once** semantics. If the dispatcher successfully publishes to Redis but crashes before updating the `OutboxEvent.published_at` field, the event will be published again upon restart. The stable `event_id` ensures that duplicate messages can be recognized downstream. Exactly-once is not guaranteed.

## PostgreSQL vs Redis Responsibilities
- **PostgreSQL**: Authoritative source of truth for the task state and whether it has been enqueued (OutboxEvent).
- **Redis**: Ephemeral queue that receives published messages.

## Limitations
Polling the database can become a bottleneck at extremely high scale, requiring potential migration to log-based CDC (like Debezium) in the future.
