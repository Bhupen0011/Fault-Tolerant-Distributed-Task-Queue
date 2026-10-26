from fastapi import FastAPI
from app.api.health import router as health_router
from app.api.tasks import router as tasks_router
from app.api.system import router as system_router
from app.api.websockets import router as websockets_router
from fastapi.middleware.cors import CORSMiddleware
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

app = FastAPI(title="Fault-Tolerant Distributed Task Queue", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], # TODO: restrict in production based on env vars
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(health_router, prefix="/health", tags=["health"])
app.include_router(tasks_router, prefix="/api/tasks", tags=["tasks"])
app.include_router(system_router, prefix="/api", tags=["system"])
app.include_router(websockets_router, tags=["websockets"])

@app.on_event("startup")
async def startup_event():
    logging.info("Starting up API...")
    from app.api.websockets import manager
    import asyncio
    manager.redis_subscriber_task = asyncio.create_task(manager.listen_to_redis())

@app.on_event("shutdown")
async def shutdown_event():
    logging.info("Shutting down API...")
