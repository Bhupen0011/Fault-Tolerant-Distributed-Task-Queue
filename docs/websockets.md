# WebSockets

The system provides a real-time event stream via WebSockets.

## Connection Strategy
Clients connect to the `ws://<host>/ws/metrics` endpoint. 
The WebSocket connection manager accepts the connection and retains it in memory. A background loop inside FastAPI subscribes to a Redis Pub/Sub channel named `system_events`. When a task state transition occurs across the cluster, a worker (or the API itself) pushes a JSON event payload to Redis. The connection manager broadcasts this payload to all active WebSocket connections. 

## Fault Tolerance
If a single client disconnects unexpectedly, the connection manager detects the error, cleans up the stale connection, and continues broadcasting to other healthy clients without interrupting the main application event loop. If zero clients are connected, the background task safely idles.

## Event Schema
Events follow a uniform schema:
```json
{
  "event": "task.created",
  "timestamp": "2026-09-25T21:00:00Z",
  "task_id": "c3f0c...",
  "status": "PENDING",
  "priority": 1
}
```

Supported event types include:
- `task.created`
- `task.queued`
- `task.started`
- `task.retrying`
- `task.completed`
- `task.failed`
- `task.cancel_requested`
- `task.cancelled`
- `task.dead_letter`
- `worker.online`
- `worker.offline`

## Limitations
- **Delivery Guarantees**: Delivery over WebSockets is best-effort. Reconnecting clients will miss events that occurred while disconnected. For authoritative task state, always rely on the REST API (backed by PostgreSQL).
- **Ordering**: Due to concurrent processing, network latency, and the distributed nature of the cluster, WebSocket events might not arrive in perfectly strictly monotonically ascending temporal order relative to their actual creation time. Always rely on the `timestamp` field for chronological truth.
