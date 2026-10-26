# API Design

## Phase 1 Implementation Status
The following health endpoints are implemented:
- `GET /health`
- `GET /health/db`
- `GET /health/redis`

## Phase 2 Implementation Status
The following Task endpoints are implemented:
- `POST /api/tasks`: Creates a task and outbox event transactionally. Returns `PENDING` status.
- `GET /api/tasks/{task_id}`: Retrieves task details.
- `GET /api/tasks`: Lists tasks.

## Phase 6 Updates
The API now supports full REST resources with pagination and filtering for tasks, worker listings, queue statistics, and WebSocket connections.

## Security & Validation
Pydantic validates request structure, field types, and configured constraints. Application-level authorization, rate limiting, payload limits, and other security controls are handled separately.
