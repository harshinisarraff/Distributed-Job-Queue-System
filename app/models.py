
from datetime import datetime, timezone
from enum import Enum
from typing import Any
from uuid import UUID, uuid4

from pydantic import BaseModel, Field


# Defines the possible states of a job during execution.
class JobStatus(str, Enum):
    PENDING = "pending"       # Waiting in the queue
    RUNNING = "running"       # Currently being processed
    COMPLETED = "completed"   # Successfully finished
    FAILED = "failed"         # Processing failed


# Defines the data a client sends when submitting a job.
# Pydantic validates that the payload is a dictionary.
class JobRequest(BaseModel):
    payload: dict[str, Any]


# Represents a job and stores its execution state.
class Job(BaseModel):
    # Generate a unique ID for every new job.
    id: UUID = Field(default_factory=uuid4)

    # Store the task information provided by the client.
    payload: dict[str, Any]

    # New jobs start in the pending state.
    status: JobStatus = JobStatus.PENDING

    # Store the output after successful execution.
    result: Any | None = None

    # Store an error message if processing fails.
    error: str | None = None

    # Track how many times the job has been attempted.
    attempts: int = 0

    # Record when the job was created, using UTC time.
    created_at: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc)
    )
