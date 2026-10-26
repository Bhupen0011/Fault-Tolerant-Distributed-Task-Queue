import pytest
import asyncio
import uuid
from datetime import datetime, timezone, timedelta
from sqlalchemy import select, update
from app.db.session import AsyncSessionLocal
from app.models.task import Task, TaskExecution
from app.worker.main import TaskWorker, LEASE_DURATION_MS
from app.core.redis import get_redis_client

@pytest.fixture(autouse=True)
async def flush_redis():
    redis = await get_redis_client()
    await redis.flushall()
    await redis.aclose()

@pytest.mark.asyncio
async def test_worker_crash_recovery(async_client):
    redis = await get_redis_client()
    # Force a very short lease for testing
    import app.worker.main as worker_main
    worker_main.LEASE_DURATION_MS = 1000  # 1 second
    
    w1 = TaskWorker(redis)
    await w1.register()
    
    # submit task
    resp = await async_client.post("/api/tasks/", json={"priority": 1, "payload": {"type": "sleep", "duration": 0}})
    task_id = uuid.UUID(resp.json()["id"])
    
    from app.outbox_dispatcher.main import process_outbox_batch
    async with AsyncSessionLocal() as db:
        async with db.begin():
            await process_outbox_batch(db, redis)
            
    # W1 claims it but crashes before finishing
    msg_str = await w1.broker.claim_task_raw(worker_main.LEASE_DURATION_MS)
    
    # Partially simulate what run_task does (claim ownership)
    lease_id_1 = uuid.uuid4()
    await w1.claim_ownership(task_id, lease_id_1)
    
    # Record execution 1
    exec_id_1 = uuid.uuid4()
    async with AsyncSessionLocal() as db:
        exec_record = TaskExecution(
            id=exec_id_1,
            task_id=task_id,
            worker_id=w1.worker_id,
            lease_id=lease_id_1,
            started_at=datetime.now(timezone.utc),
            status="RUNNING"
        )
        db.add(exec_record)
        await db.commit()
        
    # W1 CRASHES (no completion, no lease renewal)
    await w1.unregister()
    
    # Wait for lease to expire
    await asyncio.sleep(1.5)
    
    # Start W2
    w2 = TaskWorker(redis)
    await w2.register()
    
    # Trigger sweep manually to recover task
    async with AsyncSessionLocal() as db:
        now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
        expired = await w2.broker.get_expired_leases_raw(now_ms)
        # Should find it
        assert len(expired) >= 1
        
        # Reclaim it
        for e_str in expired:
            await redis.zrem("queue:processing", e_str)
            import json
            payload = json.loads(e_str)
            await w2.broker.enqueue(2, now_ms, payload)
            
            # Reset task state in postgres
            t = (await db.execute(select(Task).where(Task.id == uuid.UUID(payload["task_id"])))).scalars().first()
            t.status = "QUEUED"
            await db.commit()
            
    # W2 claims it
    msg_str_2 = await w2.broker.claim_task_raw(worker_main.LEASE_DURATION_MS)
    assert msg_str_2 is not None
    
    # W2 executes to completion
    await w2.run_task(msg_str_2)
    
    # Verify final state
    async with AsyncSessionLocal() as db:
        t = (await db.execute(select(Task).where(Task.id == task_id))).scalars().first()
        assert t.status == "COMPLETED"
        assert t.worker_id == w2.worker_id
        
        # Verify 2 executions exist
        execs = (await db.execute(select(TaskExecution).where(TaskExecution.task_id == task_id).order_by(TaskExecution.started_at))).scalars().all()
        assert len(execs) == 2
        assert execs[0].worker_id == w1.worker_id
        assert execs[0].status == "RUNNING"  # Never finished
        
        assert execs[1].worker_id == w2.worker_id
        assert execs[1].status == "COMPLETED"
        
    # Stale Worker A completion check
    # Try to complete using W1's old lease
    res = await w1.claim_ownership(task_id, lease_id_1)
    assert res is False # Should be rejected
    
    await w2.unregister()

@pytest.mark.asyncio
@pytest.mark.parametrize("priority_val", [0, 1, 2, 3])
async def test_priority_preserved_on_recovery(async_client, priority_val):
    redis = await get_redis_client()
    import app.worker.main as worker_main
    worker_main.LEASE_DURATION_MS = 1000
    
    w1 = TaskWorker(redis)
    await w1.register()
    
    # 1. Create a task with the given priority
    resp = await async_client.post("/api/tasks/", json={"priority": priority_val, "payload": {"type": "sleep", "duration": 0}})
    task_id = uuid.UUID(resp.json()["id"])
    
    from app.outbox_dispatcher.main import process_outbox_batch
    async with AsyncSessionLocal() as db:
        async with db.begin():
            await process_outbox_batch(db, redis)
            
    # 2. Worker claims it
    msg_str = await w1.broker.claim_task_raw(worker_main.LEASE_DURATION_MS)
    lease_id = uuid.uuid4()
    await w1.claim_ownership(task_id, lease_id)
    
    # 3. Simulate worker failure
    await w1.unregister()
    
    # 4. Allow lease to expire
    await asyncio.sleep(1.5)
    
    # 5. Reclaim/requeue the task (using the actual sweep function or logic)
    # To run a single sweep iteration instead of the infinite loop:
    now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
    expired = await w1.broker.get_expired_leases_raw(now_ms)
    assert len(expired) >= 1
    
    async with AsyncSessionLocal() as db:
        for e_str in expired:
            import json
            payload = json.loads(e_str)
            task_obj = (await db.execute(select(Task).where(Task.id == uuid.UUID(payload["task_id"])))).scalars().first()
            if task_obj and task_obj.status == "RUNNING":
                task_obj.status = "QUEUED"
                await redis.zrem("queue:processing", e_str)
                await w1.broker.enqueue(task_obj.priority, now_ms, payload)
                await db.commit()
                
    # 6. Verify its priority remains intact by checking the score in queue:pending
    # The score should be roughly (priority_val * 10^13) + timestamp
    # We can fetch it from redis directly or check what claim_task gives us.
    import json
    # Because zrange withscores returns (member, score)
    pending_items = await redis.zrange("queue:pending", 0, -1, withscores=True)
    assert len(pending_items) == 1
    member, score = pending_items[0]
    
    # The score formula: priority * 10^13 + timestamp
    # Since priority is 0, 1, 2, or 3 and timestamp is < 10^13
    computed_priority = int(score // 10000000000000)
    assert computed_priority == priority_val
