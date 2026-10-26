from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func
from typing import List, Dict, Any
import datetime

from app.db.session import AsyncSessionLocal
from app.models.task import Task
from app.models.worker import Worker
from app.core.redis import get_redis_client

router = APIRouter()

async def get_db():
    async with AsyncSessionLocal() as session:
        yield session

@router.get("/workers")
async def get_workers(db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(Worker))
    workers = result.scalars().all()
    
    # We could define a threshold for "healthy", e.g., heartbeat in last 30 seconds
    now = datetime.datetime.now(datetime.timezone.utc)
    threshold = now - datetime.timedelta(seconds=30)
    
    response = []
    for w in workers:
        is_healthy = w.last_heartbeat is not None and w.last_heartbeat >= threshold
        status = w.status if is_healthy else "OFFLINE"
        response.append({
            "worker_id": str(w.id),
            "hostname": w.hostname,
            "status": status,
            "last_heartbeat": w.last_heartbeat
        })
    return response

@router.get("/queue/stats")
async def get_queue_stats(db: AsyncSession = Depends(get_db)):
    redis = await get_redis_client()
    
    # Redis stats
    pending_count = await redis.zcard("queue:pending")
    processing_count = await redis.zcard("queue:processing")
    scheduled_count = await redis.zcard("queue:scheduled")
    await redis.aclose()
    
    # Postgres stats for DEAD_LETTER
    res_dlq = await db.execute(select(func.count(Task.id)).where(Task.status == "DEAD_LETTER"))
    dead_letter_count = res_dlq.scalar()
    
    # Active workers count
    now = datetime.datetime.now(datetime.timezone.utc)
    threshold = now - datetime.timedelta(seconds=30)
    
    res_total_workers = await db.execute(select(func.count(Worker.id)))
    total_workers = res_total_workers.scalar()
    
    res_healthy_workers = await db.execute(select(func.count(Worker.id)).where(Worker.last_heartbeat >= threshold))
    healthy_workers = res_healthy_workers.scalar()
    unhealthy_workers = total_workers - healthy_workers
    
    return {
        "tasks": {
            "pending": pending_count,
            "processing": processing_count,
            "scheduled": scheduled_count,
            "dead_letter": dead_letter_count
        },
        "workers": {
            "total": total_workers,
            "healthy": healthy_workers,
            "unhealthy": unhealthy_workers
        }
    }
