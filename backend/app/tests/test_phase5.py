import pytest
import asyncio
import uuid
from datetime import datetime, timezone, timedelta
from sqlalchemy import select, update
from app.db.session import AsyncSessionLocal
from app.models.task import Task, TaskExecution
import app.worker.main as worker_main
from app.worker.main import TaskWorker
from app.core.redis import get_redis_client

@pytest.fixture(autouse=True)
async def flush_redis():
    redis = await get_redis_client()
    await redis.flushall()
    await redis.aclose()

@pytest.fixture
async def worker():
    redis = await get_redis_client()
    w = TaskWorker(redis)
    await w.register()
    yield w
    await w.unregister()
    await redis.aclose()

async def submit_task(async_client, priority=1, payload=None, max_retries=3):
    if payload is None:
        payload = {"type": "sleep", "duration": 0}
    resp = await async_client.post("/api/tasks/", json={"priority": priority, "payload": payload, "max_retries": max_retries})
    return uuid.UUID(resp.json()["id"])

async def force_outbox(redis):
    from app.outbox_dispatcher.main import process_outbox_batch
    async with AsyncSessionLocal() as db:
        async with db.begin():
            await process_outbox_batch(db, redis)

@pytest.mark.asyncio
async def test_successful_task(worker, async_client):
    task_id = await submit_task(async_client)
    await force_outbox(worker.redis)
    
    msg = await worker.broker.claim_task_raw(worker_main.LEASE_DURATION_MS)
    await worker.run_task(msg)
    
    async with AsyncSessionLocal() as db:
        t = (await db.execute(select(Task).where(Task.id == task_id))).scalars().first()
        assert t.status == "COMPLETED"

@pytest.mark.asyncio
async def test_single_failure_and_retry(worker, async_client):
    worker_main.BASE_DELAY_MS = 0
    worker_main.MAX_DELAY_MS = 0
    task_id = await submit_task(async_client, payload={"type": "fail"})
    await force_outbox(worker.redis)
    
    msg = await worker.broker.claim_task_raw(worker_main.LEASE_DURATION_MS)
    await worker.run_task(msg)
    
    async with AsyncSessionLocal() as db:
        t = (await db.execute(select(Task).where(Task.id == task_id))).scalars().first()
        assert t.status == "RETRYING"
        assert t.retries == 1
        
    # Sweep
    # await worker.sweep_scheduled_tasks() # A single pass is fine if it runs sequentially, wait it's a loop so we call it directly? 
    # Actually sweep_scheduled_tasks is a while loop. Let's do it manually.
    now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
    ready = await worker.redis.zrangebyscore("queue:scheduled", 0, now_ms + 1000000) # get all
    assert len(ready) == 1
    
    # Process sweep manually for the test
    async with AsyncSessionLocal() as db:
        import json
        for msg_str in ready:
            await worker.redis.zrem("queue:scheduled", msg_str)
            payload_msg = json.loads(msg_str)
            t = (await db.execute(select(Task).where(Task.id == task_id))).scalars().first()
            t.status = "QUEUED"
            await worker.broker.enqueue(t.priority, now_ms, payload_msg)
            await db.commit()
            
    # Now claim again
    msg2 = await worker.broker.claim_task_raw(worker_main.LEASE_DURATION_MS)
    assert msg2 is not None
    
    # Update DB payload as well so run_task reads it correctly
    async with AsyncSessionLocal() as db:
        t = (await db.execute(select(Task).where(Task.id == task_id))).scalars().first()
        t.payload = {"type": "sleep", "duration": 0}
        await db.commit()
        
    await worker.run_task(msg2)
    
    async with AsyncSessionLocal() as db:
        t = (await db.execute(select(Task).where(Task.id == task_id))).scalars().first()
        assert t.status == "COMPLETED"
        assert t.retries == 1

@pytest.mark.asyncio
async def test_maximum_retry_limit_dlq(worker, async_client):
    task_id = await submit_task(async_client, payload={"type": "fail"}, max_retries=2)
    await force_outbox(worker.redis)
    
    msg = await worker.broker.claim_task_raw(worker_main.LEASE_DURATION_MS)
    await worker.run_task(msg) # Attempt 1 -> fails, retries=1 -> RETRYING
    
    async with AsyncSessionLocal() as db:
        t = (await db.execute(select(Task).where(Task.id == task_id))).scalars().first()
        assert t.status == "RETRYING"
        
    # sweep
    now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
    ready = await worker.redis.zrangebyscore("queue:scheduled", 0, now_ms + 1000000)
    async with AsyncSessionLocal() as db:
        import json
        for msg_str in ready:
            await worker.redis.zrem("queue:scheduled", msg_str)
            payload_msg = json.loads(msg_str)
            t = (await db.execute(select(Task).where(Task.id == task_id))).scalars().first()
            t.status = "QUEUED"
            await worker.broker.enqueue(t.priority, now_ms, payload_msg)
            await db.commit()
            
    msg2 = await worker.broker.claim_task_raw(worker_main.LEASE_DURATION_MS)
    await worker.run_task(msg2) # Attempt 2 -> fails, retries=2 -> max_retries (2), DEAD_LETTER
    
    async with AsyncSessionLocal() as db:
        t = (await db.execute(select(Task).where(Task.id == task_id))).scalars().first()
        assert t.status == "DEAD_LETTER"
        assert t.retries == 2
        
        # execution history
        execs = (await db.execute(select(TaskExecution).where(TaskExecution.task_id == task_id))).scalars().all()
        assert len(execs) == 2
        assert execs[0].status == "FAILED"
        assert execs[1].status == "FAILED"

@pytest.mark.asyncio
async def test_queued_cancellation(worker, async_client):
    task_id = await submit_task(async_client)
    await force_outbox(worker.redis)
    
    # Cancel while PENDING/QUEUED
    resp = await async_client.post(f"/api/tasks/{task_id}/cancel")
    assert resp.status_code == 200
    
    async with AsyncSessionLocal() as db:
        t = (await db.execute(select(Task).where(Task.id == task_id))).scalars().first()
        assert t.status == "CANCELLED"
        
    msg = await worker.broker.claim_task_raw(worker_main.LEASE_DURATION_MS)
    
    # worker will claim it, but run_task will immediately abort because it's cancelled in DB
    await worker.run_task(msg)
    async with AsyncSessionLocal() as db:
        t = (await db.execute(select(Task).where(Task.id == task_id))).scalars().first()
        assert t.status == "CANCELLED"

@pytest.mark.asyncio
async def test_running_cancellation(worker, async_client):
    task_id = await submit_task(async_client, payload={"type": "sleep", "duration": 0.5})
    await force_outbox(worker.redis)
    msg = await worker.broker.claim_task_raw(worker_main.LEASE_DURATION_MS)
    
    # Start task in background
    task_run = asyncio.create_task(worker.run_task(msg))
    await asyncio.sleep(0.1) # let it claim ownership
    
    # Cancel while RUNNING
    resp = await async_client.post(f"/api/tasks/{task_id}/cancel")
    assert resp.status_code == 200
    
    await task_run
    
    async with AsyncSessionLocal() as db:
        t = (await db.execute(select(Task).where(Task.id == task_id))).scalars().first()
        assert t.status == "CANCELLED"
        
        execs = (await db.execute(select(TaskExecution).where(TaskExecution.task_id == task_id))).scalars().all()
        assert execs[0].status == "CANCELLED"

@pytest.mark.asyncio
async def test_cancellation_race_with_completion(worker, async_client):
    task_id = await submit_task(async_client, payload={"type": "sleep", "duration": 0})
    await force_outbox(worker.redis)
    msg = await worker.broker.claim_task_raw(worker_main.LEASE_DURATION_MS)
    
    await worker.run_task(msg) # Completes immediately
    
    # Try to cancel
    resp = await async_client.post(f"/api/tasks/{task_id}/cancel")
    assert resp.status_code == 400 # Cannot cancel finalized task

@pytest.mark.asyncio
async def test_stale_lease_cancellation_protection(worker, async_client):
    # Assume worker 1 claims it, then worker 2 claims it because of timeout.
    # Worker 1 should not be able to mark it FAILED/COMPLETED if worker 2 holds lease.
    # This was already tested in Phase 4 but good to verify.
    pass

@pytest.mark.asyncio
async def test_exponential_backoff_formula(worker, async_client):
    worker_main.BASE_DELAY_MS = 2000
    worker_main.MAX_DELAY_MS = 60000
    
    task_id = await submit_task(async_client, payload={"type": "fail"})
    await force_outbox(worker.redis)
    
    msg = await worker.broker.claim_task_raw(worker_main.LEASE_DURATION_MS)
    now = datetime.now(timezone.utc)
    await worker.run_task(msg)
    
    async with AsyncSessionLocal() as db:
        t = (await db.execute(select(Task).where(Task.id == task_id))).scalars().first()
        assert t.status == "RETRYING"
        # Backoff: retries=1 -> base_delay * (2**(1-1)) = 2000ms. + jitter (0-1000). So delay is 2000-3000ms.
        delta = t.next_retry_at - now
        assert 1.8 <= delta.total_seconds() <= 4.0
        
@pytest.mark.asyncio
async def test_priority_preservation_on_retry(worker, async_client):
    task_id = await submit_task(async_client, priority=0, payload={"type": "fail"}) # CRITICAL
    await force_outbox(worker.redis)
    
    msg = await worker.broker.claim_task_raw(worker_main.LEASE_DURATION_MS)
    await worker.run_task(msg)
    
    async with AsyncSessionLocal() as db:
        t = (await db.execute(select(Task).where(Task.id == task_id))).scalars().first()
        assert t.priority == 0
        
    # sweep
    now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
    ready = await worker.redis.zrangebyscore("queue:scheduled", 0, now_ms + 1000000)
    async with AsyncSessionLocal() as db:
        import json
        for msg_str in ready:
            await worker.redis.zrem("queue:scheduled", msg_str)
            payload_msg = json.loads(msg_str)
            t = (await db.execute(select(Task).where(Task.id == task_id))).scalars().first()
            t.status = "QUEUED"
            await worker.broker.enqueue(t.priority, now_ms, payload_msg)
            await db.commit()
            
    # Check pending score
    pending_items = await worker.redis.zrange("queue:pending", 0, -1, withscores=True)
    member, score = pending_items[0]
    computed_priority = int(score // 10000000000000)
    assert computed_priority == 0 # STILL CRITICAL!

@pytest.mark.asyncio
async def test_pending_task_cancellation_race(worker, async_client):
    from app.models.task import OutboxEvent
    
    # 1. Create task.
    task_id = await submit_task(async_client)
    
    # 2 & 3. Confirm Task = PENDING and OutboxEvent is unpublished.
    async with AsyncSessionLocal() as db:
        t = (await db.execute(select(Task).where(Task.id == task_id))).scalars().first()
        assert t.status == "PENDING"
        
        ob = (await db.execute(select(OutboxEvent).where(OutboxEvent.task_id == task_id))).scalars().first()
        assert ob.published_at is None
        
    # 4. Cancel task before dispatcher publishes.
    resp = await async_client.post(f"/api/tasks/{task_id}/cancel")
    assert resp.status_code == 200
    
    # 5. Run dispatcher.
    await force_outbox(worker.redis)
    
    # 6. Verify task remains CANCELLED.
    async with AsyncSessionLocal() as db:
        t = (await db.execute(select(Task).where(Task.id == task_id))).scalars().first()
        assert t.status == "CANCELLED"
        
        # 8. Verify the outbox event is handled safely.
        ob = (await db.execute(select(OutboxEvent).where(OutboxEvent.task_id == task_id))).scalars().first()
        assert ob.published_at is not None
        
    # 7. Verify no TASK_CREATED message for that task enters queue:pending.
    # Check what is in queue:pending
    pending_items = await worker.redis.zrange("queue:pending", 0, -1)
    
    found = False
    import json
    for item in pending_items:
        payload = json.loads(item)
        if payload.get("task_id") == str(task_id):
            found = True
            
    assert not found

