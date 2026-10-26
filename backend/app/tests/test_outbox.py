import pytest
import asyncio
from sqlalchemy import select
from app.models.task import Task, OutboxEvent
from app.db.session import AsyncSessionLocal
from app.outbox_dispatcher.main import process_outbox_batch
from app.core.redis import get_redis_client
import json

@pytest.mark.asyncio
async def test_transaction_rollback():
    # Test 2: Force transaction rollback
    # We simulate this by checking that without commit, nothing is saved.
    # Actually, if an exception occurs in create_task, DB is rolled back.
    async with AsyncSessionLocal() as db:
        async with db.begin():
            task = Task(status="PENDING", priority=2)
            db.add(task)
            await db.flush()
            outbox = OutboxEvent(task_id=task.id, event_type="TEST")
            db.add(outbox)
            # deliberately rolling back
            await db.rollback()
        
        # Verify neither exists
        res = await db.execute(select(Task).where(Task.status == "PENDING"))
        assert len(res.scalars().all()) == 0

@pytest.mark.asyncio
async def test_dispatcher_success(async_client):
    # Test 3: Dispatcher publishes event successfully
    # Create task
    response = await async_client.post("/api/tasks/", json={"priority": 1})
    task_id = response.json()["id"]
    
    # Process outbox
    redis_client = await get_redis_client()
    # clean redis
    await redis_client.delete("queue:pending")
    
    async with AsyncSessionLocal() as db:
        async with db.begin():
            processed = await process_outbox_batch(db, redis_client)
            assert processed >= 1
            
    # Verify Redis received message
    messages = await redis_client.zrange("queue:pending", 0, -1)
    assert len(messages) >= 1
    msg_data = json.loads(messages[-1])
    assert msg_data["task_id"] == task_id
    
    # Verify DB state
    async with AsyncSessionLocal() as db:
        task = (await db.execute(select(Task).where(Task.id == task_id))).scalars().first()
        assert task.status == "QUEUED"
        
        outbox = (await db.execute(select(OutboxEvent).where(OutboxEvent.task_id == task_id))).scalars().first()
        assert outbox.published_at is not None

@pytest.mark.asyncio
async def test_redis_unavailable(async_client, monkeypatch):
    # Test 4: Redis unavailable
    response = await async_client.post("/api/tasks/", json={"priority": 1})
    task_id = response.json()["id"]
    
    # Mock redis client to raise exception
    class MockRedis:
        def register_script(self, script):
            return self

        async def __call__(self, *args, **kwargs):
            raise ConnectionError("Redis is down")
            
        async def zadd(self, *args, **kwargs):
            raise ConnectionError("Redis is down")
            
        async def publish(self, *args, **kwargs):
            raise ConnectionError("Redis is down")
            
    async with AsyncSessionLocal() as db:
        async with db.begin():
            # fetch the specific outbox event to check attempts later
            outbox_before = (await db.execute(select(OutboxEvent).where(OutboxEvent.task_id == task_id))).scalars().first()
            attempts_before = outbox_before.attempts
            
            await process_outbox_batch(db, MockRedis())
            
    async with AsyncSessionLocal() as db:
        task = (await db.execute(select(Task).where(Task.id == task_id))).scalars().first()
        assert task.status == "PENDING"
        
        outbox = (await db.execute(select(OutboxEvent).where(OutboxEvent.task_id == task_id))).scalars().first()
        assert outbox.published_at is None
        assert outbox.attempts == attempts_before + 1
        assert "Redis is down" in outbox.last_error

@pytest.mark.asyncio
async def test_duplicate_publication(async_client):
    # Test 5: Dispatcher restart after redis publish but before DB commit
    response = await async_client.post("/api/tasks/", json={"priority": 1})
    task_id = response.json()["id"]
    
    redis_client = await get_redis_client()
    
    # We will simulate processing, but roll back the DB transaction intentionally
    class CrashAfterPublish(Exception): pass
    
    # Mock broker.enqueue to actually enqueue, then crash
    original_enqueue = redis_client.zadd
    async def fake_zadd(*args, **kwargs):
        await original_enqueue(*args, **kwargs)
        raise CrashAfterPublish("Crash after publish")
        
    redis_client.zadd = fake_zadd
    
    async with AsyncSessionLocal() as db:
        try:
            async with db.begin():
                processed = await process_outbox_batch(db, redis_client)
        except CrashAfterPublish:
            pass
            
    redis_client.zadd = original_enqueue
            
    # Now db still has unpublished event, but Redis has the message
    async with AsyncSessionLocal() as db:
        outbox = (await db.execute(select(OutboxEvent).where(OutboxEvent.task_id == task_id))).scalars().first()
        assert outbox.published_at is None
        
    # Process again correctly
    async with AsyncSessionLocal() as db:
        async with db.begin():
            processed = await process_outbox_batch(db, redis_client)
            assert processed >= 1
            
    # Verify redis has a message
    messages = await redis_client.zrange("queue:pending", 0, -1)
    # The queue is a sorted set so it overwrites duplicates with the same score.
    # Therefore, length will be exactly 1 for identical payloads, not 2!
    assert len([m for m in messages if json.loads(m)["task_id"] == task_id]) == 1

@pytest.mark.asyncio
async def test_concurrent_dispatchers(async_client):
    # Test 6: Two dispatcher instances process concurrently
    # We create multiple tasks
    for _ in range(5):
        await async_client.post("/api/tasks/", json={"priority": 1})
        
    redis_client = await get_redis_client()
    
    async def run_dispatcher():
        async with AsyncSessionLocal() as db:
            async with db.begin():
                return await process_outbox_batch(db, redis_client)
                
    # Run 3 dispatchers concurrently
    results = await asyncio.gather(run_dispatcher(), run_dispatcher(), run_dispatcher())
    
    # Sum of processed items across dispatchers should equal total unpublished (at least 5)
    # Because of SKIP LOCKED, they won't process the same rows or block each other
    total_processed = sum(results)
    assert total_processed >= 5
