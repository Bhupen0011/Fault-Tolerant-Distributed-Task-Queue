from sqlalchemy import Column, String, DateTime
from sqlalchemy.dialects.postgresql import UUID
import uuid
import datetime
from app.db.base_class import Base

class Worker(Base):
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    hostname = Column(String, nullable=False)
    last_heartbeat = Column(DateTime(timezone=True), default=lambda: datetime.datetime.now(datetime.timezone.utc))
    status = Column(String, nullable=False, default="offline")
