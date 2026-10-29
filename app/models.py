"""HTTP and database models used by the job processing system."""
from datetime import datetime, timezone
from enum import Enum
from typing import Any
from uuid import UUID, uuid4

from pydantic import BaseModel, Field


class JobStatus(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    RETRYING = "retrying"
    COMPLETED = "completed"
    FAILED = "failed"


class JobRequest(BaseModel):
    payload: dict[str, Any]
    # Number of total executions, including the first attempt.
    max_attempts: int = Field(default=3, ge=1, le=10)


class Job(BaseModel):
    id: UUID = Field(default_factory=uuid4)
    payload: dict[str, Any]
    status: JobStatus = JobStatus.PENDING
    result: Any | None = None
    error: str | None = None
    attempts: int = 0
    max_attempts: int = Field(default=3, ge=1, le=10)
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    available_at: datetime | None = None
    lease_until: datetime | None = None
