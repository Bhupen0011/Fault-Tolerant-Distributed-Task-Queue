from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text
from app.db.session import AsyncSessionLocal
from app.core.redis import get_redis_client
import logging

logger = logging.getLogger(__name__)

router = APIRouter()

async def get_db():
    async with AsyncSessionLocal() as session:
        yield session

@router.get("/")
async def health_check():
    return {"status": "ok"}

@router.get("/db")
async def db_health(db: AsyncSession = Depends(get_db)):
    try:
        await db.execute(text("SELECT 1"))
        return {"status": "ok", "db": "connected"}
    except Exception as e:
        logger.error(f"DB Health check failed: {e}")
        return {"status": "error", "db": "disconnected", "detail": str(e)}

@router.get("/redis")
async def redis_health():
    try:
        client = await get_redis_client()
        await client.ping()
        await client.aclose()
        return {"status": "ok", "redis": "connected"}
    except Exception as e:
        logger.error(f"Redis Health check failed: {e}")
        return {"status": "error", "redis": "disconnected", "detail": str(e)}
