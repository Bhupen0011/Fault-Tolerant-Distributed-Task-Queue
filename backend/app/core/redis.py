from redis.asyncio import Redis
from app.core.config import settings
import logging

logger = logging.getLogger(__name__)

async def get_redis_client() -> Redis:
    client = Redis.from_url(settings.REDIS_URL, decode_responses=True)
    return client
