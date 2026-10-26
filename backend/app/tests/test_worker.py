import pytest
import asyncio
import uuid
import time
from datetime import datetime, timezone, timedelta
from sqlalchemy import select
from app.db.session import AsyncSessionLocal
from app.models.task import Task, TaskExecution, OutboxEvent
from app.models.worker import Worker
from app.worker.main import TaskWorker, LEASE_DURATION_MS
from app.core.redis import get_redis_client
import json

@pytest.fixture
async def worker():
    redis = await get_redis_client()
    w = TaskWorker(redis)
    await w.register()
    yield w
    await w.unregister()
@pytest.fixture(autouse=True)
async def flush_redis():
    redis = await get_redis_client()
    await redis.flushall()
    await redis.aclose()

@pytest.mark.asyncio
async def test_worker_registration(worker):
    async with AsyncSessionLocal() as db:
        w = (await db.execute(select(Worker).where(Worker.id == worker.worker_id))).scalars().first()
        assert w is not None
        assert w.status == "running"

@pytest.mark.asyncio
async def test_heartbeat(worker):
    task = asyncio.create_task(worker.heartbeat_loop())
    await asyncio.sleep(0.5)
    
    # Check redis
    val = await worker.redis.get(f"worker:heartbeat:{worker.worker_id}")
    assert val == "1"
    
    worker.is_shutting_down = True
    await task

@pytest.mark.asyncio
async def test_task_claim_and_completion(worker, async_client):
    # clear db to avoid claiming tasks from other tests
    async with AsyncSessionLocal() as db:
        async with db.begin():
            from sqlalchemy import delete
            await db.execute(delete(OutboxEvent))
            await db.execute(delete(TaskExecution))
            await db.execute(delete(Task))
            
    # submit task
    resp = await async_client.post("/api/tasks/", json={"priority": 1, "payload": {"type": "sleep", "duration": 0.1}})
    task_id = resp.json()["id"]
    
    # Manually trigger outbox dispatcher to move it to redis pending
    from app.outbox_dispatcher.main import process_outbox_batch
    async with AsyncSessionLocal() as db:
        async with db.begin():
            await process_outbox_batch(db, worker.redis)
            
    # Now claim it using the raw logic
    msg_str = await worker.broker.claim_task_raw(LEASE_DURATION_MS)
    assert msg_str is not None
    
    # execute
    await worker.run_task(msg_str)
    
    # Verify complete
    async with AsyncSessionLocal() as db:
        t = (await db.execute(select(Task).where(Task.id == uuid.UUID(task_id)))).scalars().first()
        assert t.status == "COMPLETED"
        assert t.worker_id == worker.worker_id
        assert t.result == {"result": "slept"}
        
        # Verify execution record
        execs = (await db.execute(select(TaskExecution).where(TaskExecution.task_id == t.id))).scalars().all()
        assert len(execs) == 1
        assert execs[0].status == "COMPLETED"

@pytest.mark.asyncio
async def test_concurrent_claim_race(async_client):
    # clear db to avoid claiming tasks from other tests
    async with AsyncSessionLocal() as db:
        async with db.begin():
            from sqlalchemy import delete
            await db.execute(delete(OutboxEvent))
            await db.execute(delete(TaskExecution))
            await db.execute(delete(Task))
            
    redis = await get_redis_client()
    w1 = TaskWorker(redis)
    w2 = TaskWorker(redis)
    await w1.register()
    await w2.register()
    
    resp = await async_client.post("/api/tasks/", json={"priority": 1, "payload": {"type": "sleep", "duration": 1}})
    task_id = resp.json()["id"]
    
    from app.outbox_dispatcher.main import process_outbox_batch
    async with AsyncSessionLocal() as db:
        async with db.begin():
            await process_outbox_batch(db, redis)
            
    # both try to claim
    m1, m2 = await asyncio.gather(
        w1.broker.claim_task_raw(LEASE_DURATION_MS),
        w2.broker.claim_task_raw(LEASE_DURATION_MS)
    )
    
    # Exactly one gets it
    assert (m1 is not None and m2 is None) or (m1 is None and m2 is not None)
    
    msg = m1 or m2
    worker = w1 if m1 else w2
    
    await worker.run_task(msg)
    
    async with AsyncSessionLocal() as db:
        t = (await db.execute(select(Task).where(Task.id == uuid.UUID(task_id)))).scalars().first()
        assert t.status == "COMPLETED"
        assert t.worker_id == worker.worker_id

    await w1.unregister()
    await w2.unregister()
    await redis.aclose()
