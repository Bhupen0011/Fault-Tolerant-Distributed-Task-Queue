import asyncio
import websockets
import json
import httpx
import sys
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("smoke_test")

async def listen_for_events(client_id, ws_url, task_id, events_to_collect):
    collected = set()
    try:
        async with websockets.connect(ws_url) as websocket:
            logger.info(f"Client {client_id} connected")
            while True:
                message = await asyncio.wait_for(websocket.recv(), timeout=20.0)
                event = json.loads(message)
                if event.get("task_id") == task_id:
                    event_type = event.get("event")
                    logger.info(f"Client {client_id} received: {event_type}")
                    collected.add(event_type)
                    
                    if len(collected) >= events_to_collect:
                        break
    except Exception as e:
        logger.error(f"Client {client_id} error: {e}")
    return collected

async def main():
    api_url = "http://localhost:8000"
    ws_url = "ws://localhost:8000/ws/metrics"
    
    logger.info("1. Starting WS clients...")
    
    # We will submit a task that fails once and then completes (if possible) or just a simple sleep task.
    # We will submit a fail task.
    
    # We need to wait for events, but we don't know the task ID yet.
    # Websocket clients will connect, wait for message, and filter by task_id.
    
    client1_queue = asyncio.Queue()
    client2_queue = asyncio.Queue()
    
    async def run_client(client_id, queue, disconnect_early=False):
        try:
            async with websockets.connect(ws_url) as ws:
                logger.info(f"Client {client_id} connected")
                while True:
                    if disconnect_early and queue.qsize() >= 2:
                        logger.info(f"Client {client_id} disconnecting early")
                        break
                    
                    msg = await asyncio.wait_for(ws.recv(), timeout=30.0)
                    await queue.put(json.loads(msg))
        except Exception as e:
            logger.info(f"Client {client_id} finished: {e}")

    c1_task = asyncio.create_task(run_client(1, client1_queue))
    c2_task = asyncio.create_task(run_client(2, client2_queue, disconnect_early=True))
    
    await asyncio.sleep(2) # let them connect
    
    logger.info("2. Submitting task...")
    async with httpx.AsyncClient() as client:
        # submit a task that will sleep for 2s
        resp = await client.post(f"{api_url}/api/tasks/", json={"priority": 1, "payload": {"type": "sleep", "duration": 2}})
        task_id = resp.json()["id"]
        logger.info(f"Task submitted: {task_id}")
        
    logger.info("3. Waiting for events...")
    c1_events = set()
    
    # Collect events from client 1 for this task
    while True:
        try:
            event = await asyncio.wait_for(client1_queue.get(), timeout=20.0)
            if event.get("task_id") == task_id:
                logger.info(f"C1 received event: {event.get('event')}")
                c1_events.add(event.get("event"))
                if event.get("event") in ("task.completed", "task.failed", "task.dead_letter"):
                    break
        except asyncio.TimeoutError:
            break
            
    c1_task.cancel()
    
    # Verify C2 got at least some events but disconnected early
    c2_events = set()
    while not client2_queue.empty():
        event = client2_queue.get_nowait()
        if event.get("task_id") == task_id:
            c2_events.add(event.get("event"))

    logger.info(f"C1 collected events: {c1_events}")
    logger.info(f"C2 collected events: {c2_events}")
    
    assert "task.created" in c1_events
    assert "task.queued" in c1_events
    assert "task.started" in c1_events
    assert "task.completed" in c1_events
    
    assert len(c2_events) > 0 # At least created
    
    logger.info("4. Testing Cancellation...")
    c3_queue = asyncio.Queue()
    c3_task = asyncio.create_task(run_client(3, c3_queue))
    await asyncio.sleep(1)
    
    async with httpx.AsyncClient() as client:
        resp = await client.post(f"{api_url}/api/tasks/", json={"priority": 1, "payload": {"type": "sleep", "duration": 10}})
        cancel_task_id = resp.json()["id"]
        logger.info(f"Task for cancellation submitted: {cancel_task_id}")
        
        # immediately cancel
        cancel_resp = await client.post(f"{api_url}/api/tasks/{cancel_task_id}/cancel")
        logger.info(f"Cancel response: {cancel_resp.status_code}")
        
    c3_events = set()
    while True:
        try:
            event = await asyncio.wait_for(c3_queue.get(), timeout=15.0)
            if event.get("task_id") == cancel_task_id:
                logger.info(f"C3 received event: {event.get('event')}")
                c3_events.add(event.get("event"))
                if event.get("event") in ("task.cancelled", "task.cancel_requested"):
                    break
        except asyncio.TimeoutError:
            break
            
    c3_task.cancel()
    logger.info(f"C3 collected events: {c3_events}")
    assert "task.cancelled" in c3_events or "task.cancel_requested" in c3_events
    
    logger.info("SMOKE TEST PASSED.")

if __name__ == "__main__":
    asyncio.run(main())
