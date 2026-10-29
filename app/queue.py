
import asyncio
from typing import Any
from uuid import UUID

from app.models import Job


class JobQueue:
    # Initialize a queue with a maximum capacity.
    def __init__(self, maxsize: int = 1000):
        # Stores jobs waiting to be processed.
        self._queue: asyncio.Queue[Job] = asyncio.Queue(
            maxsize=maxsize
        )

        # Stores jobs by ID so their status can be retrieved.
        self._jobs: dict[UUID, Job] = {}

    # Create a new job and add it to the queue.
    async def submit(self, payload: dict[str, Any]) -> Job:
        job = Job(payload=payload)

        # Wait for available space if the queue is full.
        await self._queue.put(job)

        # Save the job for future status lookups.
        self._jobs[job.id] = job

        return job

    # Retrieve the next available job.
    async def get(self) -> Job:
        # Wait if no jobs are available.
        return await self._queue.get()

    # Notify the queue that a retrieved job is finished.
    def task_done(self) -> None:
        self._queue.task_done()

    # Wait until all queued jobs have been acknowledged.
    async def join(self) -> None:
        await self._queue.join()

    # Look up a job using its unique ID.
    def get_job(self, job_id: UUID) -> Job | None:
        return self._jobs.get(job_id)

    # Return the number of jobs currently waiting.
    def size(self) -> int:
        return self._queue.qsize()
