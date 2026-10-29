
import asyncio
from typing import Any

from app.models import JobStatus
from app.queue import JobQueue


# Executes a task based on the job's payload.
async def execute_job(payload: dict[str, Any]) -> Any:
    task = payload.get("task")

    # Add two numbers.
    if task == "add":
        return payload["a"] + payload["b"]

    # Multiply two numbers.
    elif task == "multiply":
        return payload["a"] * payload["b"]

    # Simulate an asynchronous I/O operation.
    elif task == "sleep":
        seconds = payload["seconds"]

        if not isinstance(seconds, (int, float)) or not 0 <= seconds <= 5:
            raise ValueError("seconds must be between 0 and 5")

        await asyncio.sleep(seconds)
        return f"Waited {seconds} seconds"

    # Reject tasks that are not supported.
    else:
        raise ValueError(f"Unknown task: {task}")


class Worker:
    # Give the worker access to the shared job queue.
    def __init__(self, queue: JobQueue):
        self.queue = queue

    # Retrieve and process one job from the queue.
    async def run_once(self) -> None:

        # Wait until a job becomes available.
        job = await self.queue.get()

        try:
            # Mark the job as running.
            job.status = JobStatus.RUNNING
            job.attempts += 1

            # Execute the job and store the result.
            job.result = await execute_job(job.payload)

            # Mark the job as successfully completed.
            job.status = JobStatus.COMPLETED

        except Exception as exc:
            # Record failures without crashing the worker.
            job.status = JobStatus.FAILED
            job.error = str(exc)

        finally:
            # Acknowledge that processing has finished.
            self.queue.task_done()
    
        # Continuously process jobs until the worker is cancelled.
    async def run_forever(self) -> None:
        while True:
            # Retrieve and process the next available job.
            await self.run_once()
