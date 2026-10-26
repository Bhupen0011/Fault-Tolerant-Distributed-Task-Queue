from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from typing import List
import uuid

from app.db.session import AsyncSessionLocal
from app.models.task import Task, OutboxEvent
from app.schemas.task import TaskCreate, TaskResponse, TaskListResponse
from app.core.events import broadcast_task_event
from sqlalchemy import func
from typing import Optional

router = APIRouter()

async def get_db():
    async with AsyncSessionLocal() as session:
        yield session

@router.post("/", response_model=TaskResponse)
async def create_task(task_in: TaskCreate, db: AsyncSession = Depends(get_db)):
    # Task creation transaction
    task = Task(
        status="PENDING",
        priority=task_in.priority,
        payload=task_in.payload,
        max_retries=task_in.max_retries
    )
    db.add(task)
    await db.flush()  # To get task.id
    
    outbox_event = OutboxEvent(
        task_id=task.id,
        event_type="TASK_CREATED",
        payload={
            "priority": task.priority,
            "max_retries": task.max_retries
        }
    )
    db.add(outbox_event)
    await db.commit()
    await db.refresh(task)
    
    # Broadcast event asynchronously via Redis
    await broadcast_task_event("task.created", task)
    
    return task

@router.get("/{task_id}", response_model=TaskResponse)
async def get_task(task_id: uuid.UUID, db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(Task).where(Task.id == task_id))
    task = result.scalars().first()
    if not task:
        raise HTTPException(status_code=404, detail="Task not found")
    return task

@router.get("/", response_model=TaskListResponse)
async def list_tasks(
    page: int = 1, 
    page_size: int = 20, 
    status: Optional[str] = None, 
    priority: Optional[int] = None, 
    db: AsyncSession = Depends(get_db)
):
    if page < 1: page = 1
    if page_size < 1: page_size = 1
    if page_size > 100: page_size = 100
    
    skip = (page - 1) * page_size
    query = select(Task)
    
    if status:
        query = query.where(Task.status == status)
    if priority is not None:
        query = query.where(Task.priority == priority)
        
    query = query.order_by(Task.created_at.desc())
    
    # Count total
    count_query = select(func.count(Task.id))
    if status:
        count_query = count_query.where(Task.status == status)
    if priority is not None:
        count_query = count_query.where(Task.priority == priority)
        
    total_res = await db.execute(count_query)
    total = total_res.scalar()
    
    result = await db.execute(query.offset(skip).limit(page_size))
    tasks = result.scalars().all()
    
    return {
        "total": total,
        "page": page,
        "page_size": page_size,
        "tasks": tasks
    }

from datetime import datetime, timezone
from app.core.redis import get_redis_client

@router.post("/{task_id}/cancel", response_model=TaskResponse)
async def cancel_task(task_id: uuid.UUID, db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(Task).where(Task.id == task_id))
    task = result.scalars().first()
    if not task:
        raise HTTPException(status_code=404, detail="Task not found")
        
    now = datetime.now(timezone.utc)
    if task.status in ("COMPLETED", "FAILED", "DEAD_LETTER", "CANCELLED"):
        raise HTTPException(status_code=400, detail="Cannot cancel a finalized task")
        
    if task.status == "RUNNING":
        task.status = "CANCEL_REQUESTED"
        event_type = "task.cancel_requested"
        task.cancellation_requested_at = now
        task.updated_at = now
    else: # PENDING, QUEUED, RETRYING
        task.status = "CANCELLED"
        event_type = "task.cancelled"
        task.cancellation_requested_at = now
        task.updated_at = now
        
    await db.commit()
    await db.refresh(task)
    
    await broadcast_task_event(event_type, task)
    
    return task
