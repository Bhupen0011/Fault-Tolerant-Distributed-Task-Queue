import asyncio
import json
import logging
from typing import List
from fastapi import APIRouter, WebSocket, WebSocketDisconnect
from app.core.redis import get_redis_client

logger = logging.getLogger(__name__)
router = APIRouter()

class ConnectionManager:
    def __init__(self):
        self.active_connections: List[WebSocket] = []
        self.redis_subscriber_task = None

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)

    def disconnect(self, websocket: WebSocket):
        if websocket in self.active_connections:
            self.active_connections.remove(websocket)

    async def broadcast(self, message: str):
        stale_connections = []
        for connection in self.active_connections:
            try:
                await connection.send_text(message)
            except Exception as e:
                logger.error(f"WebSocket broadcast error: {e}")
                stale_connections.append(connection)
                
        for connection in stale_connections:
            self.disconnect(connection)

    async def listen_to_redis(self):
        try:
            redis = await get_redis_client()
            pubsub = redis.pubsub()
            await pubsub.subscribe("system_events")
            
            async for message in pubsub.listen():
                if message["type"] == "message":
                    data = message["data"].decode("utf-8")
                    if self.active_connections:
                        await self.broadcast(data)
                    else:
                        # Stop listening if no one is connected to save resources
                        # Or just keep listening. Let's break to save resources.
                        # Wait, we might miss the window between 0 and 1 connection?
                        # Keep it simple: keep listening.
                        pass
            # Don't try to cancel from within TestClient as it might crash
            pass
        except Exception as e:
            logger.error(f"Redis PubSub listener error: {e}")
        finally:
            if 'pubsub' in locals():
                await pubsub.unsubscribe("system_events")
                await pubsub.close()
            if 'redis' in locals():
                await redis.aclose()
            self.redis_subscriber_task = None

manager = ConnectionManager()

@router.websocket("/ws/metrics")
async def websocket_endpoint(websocket: WebSocket):
    await manager.connect(websocket)
    try:
        while True:
            # We don't expect client messages, but we need to keep connection alive
            # and detect disconnects.
            data = await websocket.receive_text()
    except WebSocketDisconnect:
        manager.disconnect(websocket)
    except Exception as e:
        manager.disconnect(websocket)
