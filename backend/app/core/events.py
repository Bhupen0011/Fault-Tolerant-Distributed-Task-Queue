import json
import logging
from typing import Any, Dict, Optional
from datetime import datetime, timezone
from app.core.redis import get_redis_client

logger = logging.getLogger(__name__)

async def publish_event(event_type: str, data: Dict[str, Any]):
    """
    Publish an event to the Redis Pub/Sub channel 'system_events'.
    """
    try:
        redis = await get_redis_client()
        event_payload = {
            "event": event_type,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            **data
        }
        await redis.publish("system_events", json.dumps(event_payload))
        await redis.aclose()
    except Exception as e:
        logger.error(f"Failed to publish event {event_type}: {e}")

async def broadcast_task_event(event_type: str, task: Any):
    """
    Convenience method for broadcasting task state changes.
    """
    data = {
        "task_id": str(task.id),
        "status": task.status,
        "priority": task.priority,
    }
    await publish_event(event_type, data)
