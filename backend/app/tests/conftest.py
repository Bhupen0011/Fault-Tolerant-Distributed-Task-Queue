import os

# Override environment variables for test isolation before importing app
os.environ["DATABASE_URL"] = "postgresql+asyncpg://postgres:postgres@postgres:5432/taskqueue_test"
os.environ["REDIS_URL"] = "redis://redis:6379/1"

import pytest
import pytest_asyncio
from httpx import AsyncClient
import asyncio
from app.main import app
from app.db.session import engine, AsyncSessionLocal
from app.db.base_class import Base


@pytest_asyncio.fixture(scope="session", autouse=True)
async def setup_db():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
        await conn.run_sync(Base.metadata.create_all)
    yield
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)

@pytest_asyncio.fixture
async def async_client():
    async with AsyncClient(app=app, base_url="http://test") as client:
        yield client
