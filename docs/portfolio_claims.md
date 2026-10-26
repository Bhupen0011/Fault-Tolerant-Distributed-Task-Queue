# Portfolio Claims

This document explicitly separates verified engineering capabilities of this project from theoretical or aspirational claims that should not be made.

## SAFE TO CLAIM
These claims are fully supported by the codebase, architecture, and automated test suite:

- **Transactional Outbox Implementation:** Task creation and publication events are atomically committed to PostgreSQL, preventing data loss during message broker outages.
- **Redis Priority Queueing:** Implemented a robust `ZPOPMIN` based strict-priority queue using Redis Sorted Sets, ensuring `CRITICAL` tasks preempt `NORMAL` tasks.
- **Multiple Worker Processing:** Designed for concurrent execution; multiple independently scalable worker containers safely pull from the same queue without race conditions.
- **Lease-Based Recovery Mechanism:** Workers acquire expiring leases on tasks. If a worker process abruptly dies, healthy workers detect the expired lease and recover the task, preserving its original priority.
- **Worker Heartbeat Monitoring:** Workers periodically report their health status to PostgreSQL, making cluster health visible.
- **At-Least-Once Semantics:** The system provides at-least-once processing guarantees across the entire lifecycle (submission to completion).
- **Retry & Dead-Letter Queue (DLQ):** Includes exponential backoff with jitter and a definitive `DEAD_LETTER` state for permanently failed tasks.
- **Cooperative Cancellation:** Tasks can be intercepted and cancelled while `QUEUED`, and cancellation signals propagate cooperatively to `RUNNING` tasks.
- **FastAPI REST API:** Fully functional asynchronous REST interface backed by SQLAlchemy.
- **WebSocket Monitoring:** Real-time metrics and state transitions broadcasted seamlessly to clients using Redis Pub/Sub and FastAPI WebSockets.
- **Docker Deployment:** Fully containerized cluster environment configurable via standard `docker-compose.yml`.
- **Local Benchmark Measurement:** Demonstrated local throughput of ~53 tasks/sec utilizing 2 local worker containers.

## DO NOT CLAIM
These claims are intentionally excluded because they misrepresent the system's current architecture, lack validation, or are theoretically impossible in this design:

- **Exactly-Once Processing:** The system *does not* guarantee exactly-once processing. Network partitions, hard worker crashes during the completion phase, or temporary Outbox polling overlap can result in duplicate executions. Tasks must be idempotent.
- **Zero Duplicate Execution:** Same as above. Do not claim duplicates are mathematically impossible.
- **Unlimited Scalability:** The system depends on PostgreSQL row-level locking for the Outbox Dispatcher and lease management. While scalable, it is not "unlimited" and will eventually bottleneck on PostgreSQL IOPS.
- **Production-Grade Availability:** This is a portfolio demonstration project. True production-grade availability would require multi-AZ Redis Clusters, PgBouncer, CI/CD pipelines, and robust observability (Prometheus/Grafana), which are not present here.
- **Guaranteed 53.43 tasks/sec in Production:** The 53.43 tasks/sec benchmark is strictly a local measurement against a simulated `sleep 0` workload. Production throughput will vary wildly based on task complexity, database tuning, and actual network latency.
- **Strict FIFO across all states:** While `QUEUED` tasks exhibit FIFO order within priority bands, retried tasks or recovered dead-worker tasks will be injected based on their recovery timestamp, which may interleave with newer tasks.
