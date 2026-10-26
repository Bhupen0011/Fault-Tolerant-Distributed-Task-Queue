from pydantic import BaseModel, UUID4, Field
from typing import Optional, Any, List
from datetime import datetime

class TaskCreate(BaseModel):
    priority: int = Field(default=2, ge=0, le=3, description="0=CRITICAL, 1=HIGH, 2=NORMAL, 3=LOW")
    payload: Optional[dict[str, Any]] = None
    max_retries: int = Field(default=3, ge=0)

class TaskResponse(BaseModel):
    id: UUID4
    status: str
    priority: int
    payload: Optional[dict[str, Any]]
    result: Optional[dict[str, Any]]
    error_message: Optional[str]
    retries: int
    max_retries: int
    created_at: datetime
    updated_at: datetime
    
    class Config:
        from_attributes = True

class TaskListResponse(BaseModel):
    total: int
    page: int
    page_size: int
    tasks: List[TaskResponse]
