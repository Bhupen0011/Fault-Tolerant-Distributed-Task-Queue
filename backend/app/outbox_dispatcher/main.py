import asyncio
import json
import logging
import time
from datetime import datetime, timezone
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from app.db.session import AsyncSessionLocal
from app.models.task import Task, OutboxEvent
from app.core.redis import get_redis_client
from app.queue.broker import RedisQueueBroker

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

async def process_outbox_batch(db: AsyncSession, redis_client):
    stmt = select(OutboxEvent).where(
        OutboxEvent.published_at.is_(None)
    ).with_for_update(skip_locked=True).limit(50)
    
    result = await db.execute(stmt)
    events = result.scalars().all()
    
    if not events:
        return 0
        
    broker = RedisQueueBroker(redis_client)
    processed_count = 0
    
    for event in events:
        try:
            # Get priority from payload or default to 2
            payload_data = event.payload or {}
            priority = payload_data.get("priority", 2)
            timestamp_ms = int(datetime.utcnow().timestamp() * 1000)
            
            task_stmt = select(Task).where(Task.id == event.task_id)
            task_res = await db.execute(task_stmt)
            task = task_res.scalars().first()
            
            if task and task.status == "CANCELLED":
                # Do not enqueue, just mark event as handled
                event.published_at = datetime.now(timezone.utc)
                processed_count += 1
                continue

            message = {
                "event_id": str(event.id),
                "task_id": str(event.task_id),
                "event_type": event.event_type
            }
            
            await broker.enqueue(priority, timestamp_ms, message)
            event.published_at = datetime.now(timezone.utc)
            
            if task and task.status == "PENDING":
                task.status = "QUEUED"
                from app.core.events import broadcast_task_event
                await broadcast_task_event("task.queued", task)
            
            processed_count += 1
        except Exception as e:
            logger.error(f"Failed to publish event {event.id}: {e}")
            event.attempts += 1
            event.last_error = str(e)
            
    # We let the caller handle commit now
    return processed_count

async def main():
    logger.info("Starting Outbox Dispatcher (Phase 3)...")
    
    # Run once at startup
    try:
        redis_client = await get_redis_client()
        broker = RedisQueueBroker(redis_client)
        await broker.migrate_phase2_queue()
        await redis_client.aclose()
    except Exception as e:
        logger.error(f"Migration error: {e}")

    while True:
        try:
            redis_client = await get_redis_client()
            async with AsyncSessionLocal() as db:
                async with db.begin():
                    count = await process_outbox_batch(db, redis_client)
                    if count > 0:
                        logger.info(f"Processed {count} outbox events")
            await redis_client.aclose()
        except Exception as e:
            logger.error(f"Dispatcher error: {e}")
            
        await asyncio.sleep(2)

if __name__ == "__main__":
    asyncio.run(main())
