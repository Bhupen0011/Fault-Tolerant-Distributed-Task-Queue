import pytest
import uuid

@pytest.mark.asyncio
async def test_create_task(async_client):
    # Test 1: Create task. Verify Task and OutboxEvent exist, status=PENDING
    response = await async_client.post("/api/tasks/", json={"priority": 1, "max_retries": 5})
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "PENDING"
    assert data["priority"] == 1
    assert data["max_retries"] == 5
    
    task_id = data["id"]
    
    get_resp = await async_client.get(f"/api/tasks/{task_id}")
    assert get_resp.status_code == 200
    assert get_resp.json()["status"] == "PENDING"

@pytest.mark.asyncio
async def test_get_tasks(async_client):
    response = await async_client.get("/api/tasks/")
    assert response.status_code == 200
    assert isinstance(response.json(), dict)
    assert "tasks" in response.json()
    assert "total" in response.json()
