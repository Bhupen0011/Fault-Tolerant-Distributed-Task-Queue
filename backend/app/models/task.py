from sqlalchemy import Column, String, Integer, DateTime, ForeignKey, Index
from sqlalchemy.dialects.postgresql import UUID, JSONB
from sqlalchemy.orm import relationship
import uuid
import datetime
from app.db.base_class import Base

class Task(Base):
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    status = Column(String, nullable=False, index=True, default="PENDING")
    priority = Column(Integer, nullable=False, index=True, default=2)  # 0=CRITICAL, 1=HIGH, 2=NORMAL, 3=LOW
    payload = Column(JSONB, nullable=True)
    result = Column(JSONB, nullable=True)
    error_message = Column(String, nullable=True)
    retries = Column(Integer, nullable=False, default=0)
    max_retries = Column(Integer, nullable=False, default=3)
    
    lease_id = Column(UUID(as_uuid=True), nullable=True)
    worker_id = Column(UUID(as_uuid=True), nullable=True, index=True)
    lease_expires_at = Column(DateTime(timezone=True), nullable=True, index=True)
    
    created_at = Column(DateTime(timezone=True), default=lambda: datetime.datetime.now(datetime.timezone.utc), index=True)
    updated_at = Column(DateTime(timezone=True), default=lambda: datetime.datetime.now(datetime.timezone.utc), onupdate=lambda: datetime.datetime.now(datetime.timezone.utc))
    completed_at = Column(DateTime(timezone=True), nullable=True)
    cancellation_requested_at = Column(DateTime(timezone=True), nullable=True)
    next_retry_at = Column(DateTime(timezone=True), nullable=True, index=True)

    outbox_events = relationship("OutboxEvent", back_populates="task")
    executions = relationship("TaskExecution", back_populates="task")

class OutboxEvent(Base):
    __tablename__ = "outboxevent"
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    task_id = Column(UUID(as_uuid=True), ForeignKey("task.id"), nullable=False)
    event_type = Column(String, nullable=False)
    payload = Column(JSONB, nullable=True)
    created_at = Column(DateTime(timezone=True), default=lambda: datetime.datetime.now(datetime.timezone.utc))
    published_at = Column(DateTime(timezone=True), nullable=True, index=True)
    attempts = Column(Integer, nullable=False, default=0)
    last_error = Column(String, nullable=True)

    task = relationship("Task", back_populates="outbox_events")

class TaskExecution(Base):
    __tablename__ = "taskexecution"
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    task_id = Column(UUID(as_uuid=True), ForeignKey("task.id"), nullable=False)
    worker_id = Column(UUID(as_uuid=True), ForeignKey("worker.id"), nullable=False)
    lease_id = Column(UUID(as_uuid=True), nullable=False)
    started_at = Column(DateTime(timezone=True), default=lambda: datetime.datetime.now(datetime.timezone.utc))
    finished_at = Column(DateTime(timezone=True), nullable=True)
    status = Column(String, nullable=False)
    logs = Column(String, nullable=True)

    task = relationship("Task", back_populates="executions")
