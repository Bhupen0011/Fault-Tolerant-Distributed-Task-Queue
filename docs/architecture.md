# Architecture

## System Boundaries

- **PostgreSQL = Persistent source of truth**: Owns task metadata, task ownership, task state transitions, retry counts, execution history, results, worker metadata, and outbox events. If Redis and PostgreSQL temporarily disagree, PostgreSQL is the authoritative persistent state.
- **Redis = Ephemeral coordination layer**: Responsible for queueing, lease coordination, heartbeats, and transient scheduling state.

## Services
- **Backend (FastAPI)**
- **Outbox Dispatcher (Standalone Process)**
- **Workers**
- **PostgreSQL**
- **Redis**
- **Frontend (React)**

## Phase 4 Implementation Status
- Foundation models and databases are initialized.
- FastAPI backend structure is created.
- Outbox Dispatcher is implemented as a standalone container polling PostgreSQL.
- Transactional Outbox pattern implemented for reliable task creation.
- Redis Priority Queue Logic (ZSETs `queue:pending`, `queue:processing`) and atomic Lua Scripts are implemented.
- Worker containers implemented with registration, heartbeats, task claiming, DB ownership (leases), and failure recovery sweeps.
- (Not Implemented Yet): Dead-Letter Queues, Exponential Backoff, Cancellation, React Dashboard.
