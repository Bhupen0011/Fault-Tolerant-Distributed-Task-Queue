import pytest
import asyncio
from httpx import AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from app.db.session import AsyncSessionLocal
from app.models.task import Task
from app.worker.main import TaskWorker
import json
import uuid

@pytest.fixture
async def async_client():
    from app.main import app
    from httpx import ASGITransport
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        yield client

@pytest.fixture
async def worker():
    from app.core.redis import get_redis_client
    redis = await get_redis_client()
    w = TaskWorker(redis)
    await w.register()
    yield w
    await w.unregister()
    await redis.aclose()

@pytest.mark.asyncio
async def test_rest_endpoints(async_client):
    # 1. Create task
    resp = await async_client.post("/api/tasks/", json={"priority": 1, "payload": {"type": "sleep"}})
    assert resp.status_code == 200
    task_id = resp.json()["id"]
    
    # 2. Retrieve task
    resp2 = await async_client.get(f"/api/tasks/{task_id}")
    assert resp2.status_code == 200
    assert resp2.json()["id"] == task_id
    assert resp2.json()["status"] == "PENDING"
    
    # 3 & 4. List tasks & pagination
    resp3 = await async_client.get("/api/tasks/?page=1&page_size=10")
    assert resp3.status_code == 200
    assert resp3.json()["total"] >= 1
    assert len(resp3.json()["tasks"]) >= 1
    
    # 5. Status filtering
    resp4 = await async_client.get("/api/tasks/?status=PENDING")
    assert resp4.status_code == 200
    assert any(t["id"] == task_id for t in resp4.json()["tasks"])
    
    # 6. Priority filtering
    resp5 = await async_client.get("/api/tasks/?priority=1")
    assert resp5.status_code == 200
    assert any(t["id"] == task_id for t in resp5.json()["tasks"])
    
    # 7. Invalid task ID
    resp6 = await async_client.get(f"/api/tasks/{uuid.uuid4()}")
    assert resp6.status_code == 404
    
    # 8 & 9. Invalid priority / max_retries validation
    resp7 = await async_client.post("/api/tasks/", json={"priority": 5, "max_retries": -1})
    assert resp7.status_code == 422 # Pydantic validation error

@pytest.mark.asyncio
async def test_queue_and_worker_stats(async_client, worker):
    # 14. Queue statistics
    resp = await async_client.get("/api/queue/stats")
    assert resp.status_code == 200
    data = resp.json()
    assert "tasks" in data
    assert "workers" in data
    assert data["workers"]["total"] >= 1
    
    # 15. Worker listing
    resp2 = await async_client.get("/api/workers")
    assert resp2.status_code == 200
    workers = resp2.json()
    assert len(workers) >= 1
    assert "worker_id" in workers[0]
    

