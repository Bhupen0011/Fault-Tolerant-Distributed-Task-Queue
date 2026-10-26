import pytest
import asyncio
import time
from app.queue.broker import RedisQueueBroker
from app.core.redis import get_redis_client
import uuid

@pytest.mark.asyncio
async def test_priority_ordering():
    # Test Priority ordering (CRITICAL=0 before HIGH=1 before NORMAL=2 before LOW=3)
    redis = await get_redis_client()
    await redis.flushdb()
    
    broker = RedisQueueBroker(redis)
    ts = int(time.time() * 1000)
    
    await broker.enqueue(priority=3, timestamp_ms=ts, message={"id": "low"})
    await broker.enqueue(priority=0, timestamp_ms=ts, message={"id": "critical"})
    await broker.enqueue(priority=1, timestamp_ms=ts, message={"id": "high"})
    await broker.enqueue(priority=2, timestamp_ms=ts, message={"id": "normal"})
    
    t1 = await broker.claim_task(lease_duration_ms=10000)
    t2 = await broker.claim_task(lease_duration_ms=10000)
    t3 = await broker.claim_task(lease_duration_ms=10000)
    t4 = await broker.claim_task(lease_duration_ms=10000)
    
    assert t1["id"] == "critical"
    assert t2["id"] == "high"
    assert t3["id"] == "normal"
    assert t4["id"] == "low"

@pytest.mark.asyncio
async def test_fifo_ordering():
    # Test FIFO ordering within the same priority
    redis = await get_redis_client()
    await redis.flushdb()
    broker = RedisQueueBroker(redis)
    
    ts1 = int(time.time() * 1000)
    ts2 = ts1 + 1000
    ts3 = ts2 + 1000
    
    await broker.enqueue(priority=1, timestamp_ms=ts2, message={"id": "second"})
    await broker.enqueue(priority=1, timestamp_ms=ts3, message={"id": "third"})
    await broker.enqueue(priority=1, timestamp_ms=ts1, message={"id": "first"})
    
    t1 = await broker.claim_task(lease_duration_ms=10000)
    t2 = await broker.claim_task(lease_duration_ms=10000)
    t3 = await broker.claim_task(lease_duration_ms=10000)
    
    assert t1["id"] == "first"
    assert t2["id"] == "second"
    assert t3["id"] == "third"

@pytest.mark.asyncio
async def test_atomic_claim_and_lease():
    # Test atomic claim and processing lease creation
    redis = await get_redis_client()
    await redis.flushdb()
    broker = RedisQueueBroker(redis)
    
    await broker.enqueue(priority=0, timestamp_ms=0, message={"id": "test_lease"})
    
    t1 = await broker.claim_task(lease_duration_ms=60000)
    assert t1["id"] == "test_lease"
    
    # Pending should be empty
    pending = await redis.zrange("queue:pending", 0, -1)
    assert len(pending) == 0
    
    # Processing should have 1 item
    processing = await redis.zrange("queue:processing", 0, -1, withscores=True)
    assert len(processing) == 1
    score = processing[0][1]
    
    # Score should be roughly current_time + 60000
    now = time.time() * 1000
    assert score > now

@pytest.mark.asyncio
async def test_expired_lease_detection():
    # Test expired lease detection
    redis = await get_redis_client()
    await redis.flushdb()
    broker = RedisQueueBroker(redis)
    
    await broker.enqueue(priority=0, timestamp_ms=0, message={"id": "expiring"})
    # Lease for 100ms
    t1 = await broker.claim_task(lease_duration_ms=100)
    
    now = int(time.time() * 1000)
    expired = await broker.get_expired_leases(now)
    assert len(expired) == 0
    
    # wait for lease to expire
    await asyncio.sleep(0.15)
    
    now = int(time.time() * 1000)
    expired = await broker.get_expired_leases(now)
    assert len(expired) == 1
    assert expired[0]["id"] == "expiring"

@pytest.mark.asyncio
async def test_concurrent_claims():
    # Test concurrent claims via Lua scripts
    redis = await get_redis_client()
    await redis.flushdb()
    broker = RedisQueueBroker(redis)
    
    # Enqueue 5 tasks
    for i in range(5):
        await broker.enqueue(priority=0, timestamp_ms=i, message={"id": f"task_{i}"})
        
    async def claim_worker():
        br = RedisQueueBroker(redis)
        tasks = []
        # try claiming up to 5 times
        for _ in range(5):
            t = await br.claim_task(lease_duration_ms=10000)
            if t:
                tasks.append(t["id"])
        return tasks

    results = await asyncio.gather(
        claim_worker(), claim_worker(), claim_worker(), claim_worker()
    )
    
    claimed = []
    for r in results:
        claimed.extend(r)
        
    # Total claimed should be exactly 5
    assert len(claimed) == 5
    # All 5 should be distinct
    assert len(set(claimed)) == 5

@pytest.mark.asyncio
async def test_empty_queue():
    # Test empty queue
    redis = await get_redis_client()
    await redis.flushdb()
    broker = RedisQueueBroker(redis)
    
    res = await broker.claim_task(10000)
    assert res is None

@pytest.mark.asyncio
async def test_phase2_migration():
    # Test phase 2 migration
    redis = await get_redis_client()
    await redis.flushdb()
    
    await redis.rpush("tasks:published", '{"id":"phase2_1"}')
    await redis.rpush("tasks:published", '{"id":"phase2_2"}')
    
    broker = RedisQueueBroker(redis)
    await broker.migrate_phase2_queue()
    
    # list should be empty
    rem = await redis.llen("tasks:published")
    assert rem == 0
    
    # pending should have 2
    pending = await redis.zrange("queue:pending", 0, -1)
    assert len(pending) == 2

@pytest.mark.asyncio
async def test_malformed_message():
    redis = await get_redis_client()
    await redis.flushdb()
    
    # manually insert garbage
    await redis.zadd("queue:pending", {"NOT_JSON": 0})
    broker = RedisQueueBroker(redis)
    
    with pytest.raises(Exception):
        await broker.claim_task(10000)

@pytest.mark.asyncio
async def test_redis_failure():
    class BrokenRedis:
        def register_script(self, *args, **kwargs):
            return self.script_call
        async def script_call(self, *args, **kwargs):
            raise ConnectionError("Redis is dead")
    
    broker = RedisQueueBroker(BrokenRedis())
    with pytest.raises(ConnectionError):
        await broker.claim_task(10000)
