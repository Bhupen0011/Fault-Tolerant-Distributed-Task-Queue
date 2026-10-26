import asyncio
import json
import logging
import uuid
import socket
import signal
from datetime import datetime, timezone, timedelta
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, update
from app.db.session import AsyncSessionLocal
from app.models.task import Task, TaskExecution
from app.models.worker import Worker
from app.core.redis import get_redis_client
from app.queue.broker import RedisQueueBroker
from app.core.events import broadcast_task_event, publish_event

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

LEASE_DURATION_MS = 10000 # 10s
HEARTBEAT_INTERVAL = 3 # 3s
HEARTBEAT_TTL = 10 # 10s
BASE_DELAY_MS = 2000
MAX_DELAY_MS = 60000

class TaskWorker:
    def __init__(self, redis_client):
        self.worker_id = uuid.uuid4()
        self.hostname = socket.gethostname()
        self.redis = redis_client
        self.broker = RedisQueueBroker(redis_client)
        self.is_shutting_down = False
        self.active_tasks = {} # task_id -> {lease_id, task_id, message_str}

    async def register(self):
        async with AsyncSessionLocal() as db:
            worker_record = Worker(id=self.worker_id, hostname=self.hostname, status="running")
            db.add(worker_record)
            await db.commit()
        logger.info(f"Worker {self.worker_id} registered")
        await publish_event("worker.online", {"worker_id": str(self.worker_id), "hostname": self.hostname})

    async def unregister(self):
        async with AsyncSessionLocal() as db:
            stmt = update(Worker).where(Worker.id == self.worker_id).values(status="offline")
            await db.execute(stmt)
            await db.commit()
        key = f"worker:heartbeat:{self.worker_id}"
        await self.redis.delete(key)
        logger.info(f"Worker {self.worker_id} unregistered")
        await publish_event("worker.offline", {"worker_id": str(self.worker_id)})

    async def heartbeat_loop(self):
        while not self.is_shutting_down:
            try:
                key = f"worker:heartbeat:{self.worker_id}"
                await self.redis.setex(key, HEARTBEAT_TTL, "1")
                async with AsyncSessionLocal() as db:
                    stmt = update(Worker).where(Worker.id == self.worker_id).values(
                        last_heartbeat=datetime.now(timezone.utc),
                        status="running"
                    )
                    await db.execute(stmt)
                    await db.commit()
            except Exception as e:
                logger.error(f"Heartbeat error: {e}")
            await asyncio.sleep(HEARTBEAT_INTERVAL)
            
    async def renew_leases_loop(self):
        while not self.is_shutting_down:
            try:
                now = datetime.now(timezone.utc)
                new_expires = now + timedelta(milliseconds=LEASE_DURATION_MS)
                new_score = int(now.timestamp() * 1000) + LEASE_DURATION_MS
                
                for task_id, info in list(self.active_tasks.items()):
                    # Renew Postgres
                    async with AsyncSessionLocal() as db:
                        stmt = update(Task).where(
                            Task.id == task_id,
                            Task.lease_id == info["lease_id"],
                            Task.worker_id == self.worker_id
                        ).values(lease_expires_at=new_expires).returning(Task.id)
                        res = await db.execute(stmt)
                        if res.scalar():
                            # Renew Redis
                            await self.redis.zadd("queue:processing", {info["message_str"]: new_score})
                            await db.commit()
                        else:
                            # We lost the lease! DB rejected renewal
                            logger.warning(f"Lost lease for task {task_id}")
                            self.active_tasks.pop(task_id, None)
            except Exception as e:
                logger.error(f"Lease renewal error: {e}")
            await asyncio.sleep(LEASE_DURATION_MS / 3000.0)

    async def claim_ownership(self, task_id: uuid.UUID, lease_id: uuid.UUID) -> bool:
        async with AsyncSessionLocal() as db:
            now = datetime.now(timezone.utc)
            expires = now + timedelta(milliseconds=LEASE_DURATION_MS)
            
            stmt = update(Task).where(
                Task.id == task_id,
                (Task.status == "QUEUED") | 
                ((Task.status == "RUNNING") & (Task.lease_expires_at < now))
            ).values(
                status="RUNNING",
                worker_id=self.worker_id,
                lease_id=lease_id,
                lease_expires_at=expires,
                updated_at=now
            ).returning(Task.id)
            
            res = await db.execute(stmt)
            await db.commit()
            return bool(res.scalar())

    async def run_task(self, message_str: str):
        print(f"RUN_TASK started with {message_str}")
        try:
            msg_data = json.loads(message_str)
            task_id_str = msg_data.get("task_id")
            if not task_id_str:
                print("NO TASK ID")
                return
            task_id = uuid.UUID(task_id_str)
        except Exception as e:
            print(f"EXCEPTION IN PARSE: {e}")
            return
            
        print(f"Parsed task_id {task_id}")
        lease_id = uuid.uuid4()
        
        # 1. Claim ownership in DB
        res = await self.claim_ownership(task_id, lease_id)
        print(f"CLAIM OWNERSHIP RETURNED {res}")
        if not res:
            logger.warning(f"Failed to establish DB ownership for task {task_id}")
            # we should remove it from processing since we couldn't claim it in db
            await self.redis.zrem("queue:processing", message_str)
            return

        self.active_tasks[task_id] = {"lease_id": lease_id, "message_str": message_str}
        
        # 2. Record Execution
        execution_id = uuid.uuid4()
        async with AsyncSessionLocal() as db:
            now = datetime.now(timezone.utc)
            exec_record = TaskExecution(
                id=execution_id,
                task_id=task_id,
                worker_id=self.worker_id,
                lease_id=lease_id,
                started_at=now,
                status="RUNNING"
            )
            db.add(exec_record)
            
            # Fetch task for payload
            task_obj = (await db.execute(select(Task).where(Task.id == task_id))).scalars().first()
            task_payload = task_obj.payload or {}
            
            # extract fields for broadcast before commit
            task_info = {
                "id": task_obj.id,
                "status": task_obj.status,
                "priority": task_obj.priority
            }
            await db.commit()
            
        class _Dummy:
            pass
        t_dummy = _Dummy()
        t_dummy.id = task_info["id"]
        t_dummy.status = task_info["status"]
        t_dummy.priority = task_info["priority"]
        await broadcast_task_event("task.started", t_dummy)

        # 3. Execute with cooperative cancellation check
        error = None
        result_data = None
        task_status_final = "COMPLETED"
        try:
            if task_obj.status == "CANCEL_REQUESTED":
                raise asyncio.CancelledError("Cancellation requested before execution")
                
            task_type = task_payload.get("type", "sleep")
            if task_type == "sleep":
                duration = task_payload.get("duration", 1)
                # Cooperative sleep loop
                for _ in range(int(duration * 10)):
                    await asyncio.sleep(0.1)
                    # Poll for cancellation
                    async with AsyncSessionLocal() as db_poll:
                        current_status = (await db_poll.execute(select(Task.status).where(Task.id == task_id))).scalar()
                        if current_status == "CANCEL_REQUESTED":
                            raise asyncio.CancelledError("Cancellation requested during execution")
                result_data = {"result": "slept"}
            elif task_type == "fail":
                raise Exception("Intentional failure")
            else:
                result_data = {"result": "done"}
        except asyncio.CancelledError as e:
            error = str(e)
            task_status_final = "CANCELLED"
        except Exception as e:
            error = str(e)
            task_status_final = "FAILED"
            
        # 4. Finalize
        if task_id not in self.active_tasks:
            # We lost the lease during execution
            return

        async with AsyncSessionLocal() as db:
            now = datetime.now(timezone.utc)
            t = (await db.execute(select(Task).where(Task.id == task_id, Task.lease_id == lease_id))).scalars().first()
            if not t:
                return # Lost lease or reclaimed
                
            if error:
                if task_status_final == "CANCELLED":
                    t.status = "CANCELLED"
                    t.error_message = error
                    t.updated_at = now
                else:
                    t.retries += 1
                    if t.retries >= t.max_retries:
                        t.status = "DEAD_LETTER"
                        t.error_message = error
                        t.updated_at = now
                    else:
                        t.status = "RETRYING"
                        t.error_message = error
                        t.updated_at = now
                        import random
                        jitter = random.randint(0, 1000)
                        delay_ms = min(BASE_DELAY_MS * (2 ** (t.retries - 1)), MAX_DELAY_MS) + jitter
                        t.next_retry_at = now + timedelta(milliseconds=delay_ms)
                        
                        # Add to queue:scheduled
                        score = t.next_retry_at.timestamp() * 1000
                        message_dict = json.loads(message_str)
                        await self.redis.zadd("queue:scheduled", {json.dumps(message_dict): score})
            else:
                t.status = "COMPLETED"
                t.result = result_data
                t.completed_at = now
                t.updated_at = now
                
            exec_stmt = update(TaskExecution).where(TaskExecution.id == execution_id).values(
                finished_at=now,
                status=task_status_final,
                logs=error
            )
            await db.execute(exec_stmt)
            
            # Broadcast the terminal/retry event
            class _Dummy2:
                pass
            t_dummy2 = _Dummy2()
            t_dummy2.id = t.id
            t_dummy2.status = t.status
            t_dummy2.priority = t.priority
            
            await db.commit()
            
            # Remove from redis queue
            await self.redis.zrem("queue:processing", message_str)
            
            if t_dummy2.status == "COMPLETED":
                await broadcast_task_event("task.completed", t_dummy2)
            elif t_dummy2.status == "CANCELLED":
                await broadcast_task_event("task.cancelled", t_dummy2)
            elif t_dummy2.status == "DEAD_LETTER":
                await broadcast_task_event("task.dead_letter", t_dummy2)
            elif t_dummy2.status == "RETRYING":
                await broadcast_task_event("task.retrying", t_dummy2)
                
        self.active_tasks.pop(task_id, None)

    async def claim_loop(self):
        while not self.is_shutting_down:
            try:
                # We fetch a raw string from Lua claim
                message_str = await self.broker.claim_task_raw(LEASE_DURATION_MS)
                if message_str:
                    # Spawn task
                    asyncio.create_task(self.run_task(message_str))
                else:
                    await asyncio.sleep(1)
            except Exception as e:
                logger.error(f"Claim loop error: {e}")
                await asyncio.sleep(1)

    async def sweep_expired_leases(self):
        # A separate loop that checks queue:processing for expired items and reclaims them
        while not self.is_shutting_down:
            try:
                now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
                expired = await self.broker.get_expired_leases_raw(now_ms)
                for msg_str in expired:
                    msg = json.loads(msg_str)
                    task_id = uuid.UUID(msg["task_id"])
                    
                    # Verify in DB if the task actually expired
                    async with AsyncSessionLocal() as db:
                        now = datetime.now(timezone.utc)
                        task_obj = (await db.execute(select(Task).where(Task.id == task_id))).scalars().first()
                        if task_obj and task_obj.status == "RUNNING" and task_obj.lease_expires_at and task_obj.lease_expires_at < now:
                            # Reclaim it back to queue:pending
                            logger.info(f"Reclaiming expired task {task_id}")
                            # update DB
                            task_obj.status = "QUEUED"
                            # move in redis
                            await self.redis.zrem("queue:processing", msg_str)
                            priority = task_obj.priority
                            await self.broker.enqueue(priority, now_ms, msg)
                            await db.commit()
                            await broadcast_task_event("task.queued", task_obj)
                        elif task_obj and task_obj.status in ("COMPLETED", "FAILED", "QUEUED"):
                            # Cleanup orphan in processing queue
                            await self.redis.zrem("queue:processing", msg_str)
                            await db.commit()
            except Exception as e:
                pass
            await asyncio.sleep(5)

    async def sweep_scheduled_tasks(self):
        while not self.is_shutting_down:
            try:
                now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
                ready = await self.redis.zrangebyscore("queue:scheduled", 0, now_ms)
                for msg_str in ready:
                    msg = json.loads(msg_str)
                    task_id = uuid.UUID(msg["task_id"])
                    
                    async with AsyncSessionLocal() as db:
                        t = (await db.execute(select(Task).where(Task.id == task_id))).scalars().first()
                        if t and t.status == "RETRYING":
                            t.status = "QUEUED"
                            await self.redis.zrem("queue:scheduled", msg_str)
                            await self.broker.enqueue(t.priority, now_ms, msg)
                            await db.commit()
                            await broadcast_task_event("task.queued", t)
                        elif t and t.status in ("CANCELLED", "CANCEL_REQUESTED"):
                            await self.redis.zrem("queue:scheduled", msg_str)
                            await db.commit()
            except Exception as e:
                logger.error(f"Scheduled sweep error: {e}")
            await asyncio.sleep(2)

async def run_worker():
    redis = await get_redis_client()
    worker = TaskWorker(redis)
    await worker.register()
    
    # Setup graceful shutdown
    loop = asyncio.get_running_loop()
    def stop():
        worker.is_shutting_down = True
    loop.add_signal_handler(signal.SIGINT, stop)
    loop.add_signal_handler(signal.SIGTERM, stop)
    
    tasks = [
        asyncio.create_task(worker.heartbeat_loop()),
        asyncio.create_task(worker.renew_leases_loop()),
        asyncio.create_task(worker.claim_loop()),
        asyncio.create_task(worker.sweep_expired_leases()),
        asyncio.create_task(worker.sweep_scheduled_tasks())
    ]
    
    while not worker.is_shutting_down:
        await asyncio.sleep(1)
        
    await worker.unregister()
    for t in tasks:
        t.cancel()
    await redis.aclose()

if __name__ == "__main__":
    asyncio.run(run_worker())
