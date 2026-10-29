"""Async facade around the SQLite broker, safe for FastAPI's event loop."""
import asyncio
import time
from typing import Any
from uuid import UUID

from app.models import Job
from app.store import JobStore, QueueFull


class JobQueue:
    def __init__(self, db_path: str = "data/jobs.sqlite3", maxsize: int = 1000):
        self.store = JobStore(db_path, max_pending=maxsize)

    async def submit(self, payload: dict[str, Any], max_attempts: int = 3,
                     timeout: float = 0.5) -> Job:
        # Return capacity to the caller rather than blocking HTTP indefinitely.
        deadline = time.monotonic() + timeout
        while True:
            try:
                return await asyncio.to_thread(self.store.submit, payload, max_attempts)
            except QueueFull:
                if time.monotonic() >= deadline:
                    raise
                await asyncio.sleep(min(0.02, max(0, deadline - time.monotonic())))

    async def claim(self, owner: str, lease_seconds: float = 30) -> Job | None:
        return await asyncio.to_thread(self.store.claim, owner, lease_seconds)

    async def complete(self, job_id: UUID, owner: str, result: Any) -> bool:
        return await asyncio.to_thread(self.store.complete, str(job_id), owner, result)

    async def fail(self, job_id: UUID, owner: str, error: str, retryable: bool,
                   base_delay: float = 0.1, max_delay: float = 2.0) -> bool:
        return await asyncio.to_thread(self.store.fail, str(job_id), owner, error,
                                       retryable, base_delay, max_delay)

    async def get_job(self, job_id: UUID) -> Job | None:
        return await asyncio.to_thread(self.store.get_job, str(job_id))

    async def stats(self) -> dict[str, int]:
        return await asyncio.to_thread(self.store.stats)

    async def recover_expired(self) -> int:
        return await asyncio.to_thread(self.store.recover_expired)

    async def cleanup(self, older_than_seconds: float) -> int:
        return await asyncio.to_thread(self.store.prune_completed, older_than_seconds)
