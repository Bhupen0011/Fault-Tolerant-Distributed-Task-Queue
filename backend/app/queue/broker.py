import json
import time
import logging
from typing import Optional, Dict, Any

logger = logging.getLogger(__name__)

CLAIM_SCRIPT = """
local item = redis.call('zrange', KEYS[1], 0, 0)
if item and #item > 0 then
    local payload = item[1]
    redis.call('zrem', KEYS[1], payload)
    redis.call('zadd', KEYS[2], ARGV[1], payload)
    return payload
end
return nil
"""

class RedisQueueBroker:
    def __init__(self, redis_client):
        self.redis = redis_client
        self._claim_script = self.redis.register_script(CLAIM_SCRIPT)

    @staticmethod
    def calculate_score(priority: int, timestamp_ms: int) -> float:
        """
        Calculates deterministic queue score.
        Priority 0 (CRITICAL) to 3 (LOW).
        Offset by 10^13 to guarantee Priority 0 is always < Priority 1, 
        even far into the future.
        """
        return float((priority * 10_000_000_000_000) + timestamp_ms)

    async def enqueue(self, priority: int, timestamp_ms: int, message: Dict[str, Any]):
        score = self.calculate_score(priority, timestamp_ms)
        await self.redis.zadd("queue:pending", {json.dumps(message): score})

    async def claim_task(self, lease_duration_ms: int) -> Optional[Dict[str, Any]]:
        result = await self.claim_task_raw(lease_duration_ms)
        if result:
            return json.loads(result)
        return None

    async def claim_task_raw(self, lease_duration_ms: int) -> Optional[str]:
        expiration_ms = int(time.time() * 1000) + lease_duration_ms
        result = await self._claim_script(
            keys=["queue:pending", "queue:processing"],
            args=[expiration_ms]
        )
        if result:
            return result
        return None

    async def get_expired_leases(self, current_time_ms: int):
        """
        Identifies expired leases in queue:processing.
        """
        results = await self.get_expired_leases_raw(current_time_ms)
        return [json.loads(res) for res in results]

    async def get_expired_leases_raw(self, current_time_ms: int):
        return await self.redis.zrangebyscore("queue:processing", 0, current_time_ms)

    async def migrate_phase2_queue(self):
        """
        Migrate existing tasks from Phase 2 List 'tasks:published' 
        to Phase 3 Sorted Set 'queue:pending'.
        """
        migrated = 0
        while True:
            item = await self.redis.lpop("tasks:published")
            if not item:
                break
            score = self.calculate_score(priority=2, timestamp_ms=int(time.time() * 1000))
            await self.redis.zadd("queue:pending", {item: score})
            migrated += 1
        if migrated > 0:
            logger.info(f"Migrated {migrated} items from Phase 2 queue to Phase 3 priority queue.")
