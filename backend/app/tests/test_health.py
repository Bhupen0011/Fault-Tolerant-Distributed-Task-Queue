import pytest

@pytest.mark.asyncio
async def test_health_check(async_client):
    response = await async_client.get("/health/")
    assert response.status_code == 200
    assert response.json() == {"status": "ok"}

@pytest.mark.asyncio
async def test_db_health(async_client):
    response = await async_client.get("/health/db")
    assert response.status_code == 200
    assert response.json()["status"] == "ok"
    assert response.json()["db"] == "connected"

@pytest.mark.asyncio
async def test_redis_health(async_client):
    response = await async_client.get("/health/redis")
    assert response.status_code == 200
    assert response.json()["status"] == "ok"
    assert response.json()["redis"] == "connected"
