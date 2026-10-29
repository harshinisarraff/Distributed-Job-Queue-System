"""Async workers: retries, exponential backoff, lease-based crash recovery."""
import asyncio
import logging
import uuid
from typing import Any

from app.queue import JobQueue

logger = logging.getLogger(__name__)


class TransientJobError(Exception):
    """An error that can reasonably succeed on a later attempt."""


async def execute_job(payload: dict[str, Any], attempt: int = 1) -> Any:
    task = payload.get("task")

    if task in ("add", "multiply"):
        a, b = payload.get("a"), payload.get("b")
        if any(isinstance(x, bool) or not isinstance(x, (int, float)) for x in (a, b)):
            raise ValueError("a and b must be numbers")
        return a + b if task == "add" else a * b

    if task == "sleep":
        seconds = payload.get("seconds")
        if (isinstance(seconds, bool) or not isinstance(seconds, (int, float))
                or not 0 <= seconds <= 5):
            raise ValueError("seconds must be a number between 0 and 5")
        await asyncio.sleep(seconds)
        return f"Waited {seconds} seconds"

    # Deterministic test/demo of a temporary outage, no real network dependency.
    if task == "flaky":
        failures = payload.get("failures_before_success", 2)
        if not isinstance(failures, int) or isinstance(failures, bool) or not 0 <= failures <= 9:
            raise ValueError("failures_before_success must be an integer from 0 to 9")
        if attempt <= failures:
            raise TransientJobError("Simulated temporary service failure")
        return {"message": "Recovered", "attempt": attempt}

    raise ValueError(f"Unknown task: {task}")


class Worker:
    def __init__(self, queue: JobQueue, worker_id: str | None = None,
                 retry_base_delay: float = 0.1, retry_max_delay: float = 2.0,
                 lease_seconds: float = 30, poll_interval: float = 0.1):
        if retry_base_delay < 0 or retry_max_delay < 0:
            raise ValueError("Retry delays must be non-negative")
        self.queue = queue
        self.worker_id = worker_id or str(uuid.uuid4())
        self.retry_base_delay = retry_base_delay
        self.retry_max_delay = retry_max_delay
        self.lease_seconds = lease_seconds
        self.poll_interval = poll_interval

    async def run_once(self) -> bool:
        """Try one available job. Returns False when no job is ready."""
        job = await self.queue.claim(self.worker_id, self.lease_seconds)
        if job is None:
            return False
        try:
            result = await execute_job(job.payload, attempt=job.attempts)
        except asyncio.CancelledError:
            # Job stays leased; another worker can recover it when lease expires.
            raise
        except (TransientJobError, ConnectionError, TimeoutError) as exc:
            await self.queue.fail(job.id, self.worker_id, str(exc), True,
                                  self.retry_base_delay, self.retry_max_delay)
        except Exception as exc:
            await self.queue.fail(job.id, self.worker_id, str(exc), False)
        else:
            accepted = await self.queue.complete(job.id, self.worker_id, result)
            if not accepted:
                logger.warning("Discarded stale result for job %s", job.id)
        return True

    async def run_forever(self, stop_event: asyncio.Event | None = None) -> None:
        stop_event = stop_event or asyncio.Event()
        while not stop_event.is_set():
            try:
                processed = await self.run_once()
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("Worker %s encountered an unexpected error", self.worker_id)
                processed = False
            if not processed:
                try:
                    await asyncio.wait_for(stop_event.wait(), timeout=self.poll_interval)
                except TimeoutError:
                    pass
