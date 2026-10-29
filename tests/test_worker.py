
import asyncio

from app.models import JobStatus
from app.queue import JobQueue
from app.worker import Worker


def test_worker_success():
    async def run_test():
        queue = JobQueue()
        worker = Worker(queue)

        # Submit an addition job.
        job = await queue.submit({
            "task": "add",
            "a": 10,
            "b": 20
        })

        # Process the job.
        await worker.run_once()

        # Verify the execution result.
        assert job.status == JobStatus.COMPLETED
        assert job.result == 30
        assert job.attempts == 1

        await queue.join()

    asyncio.run(run_test())


def test_worker_failure():
    async def run_test():
        queue = JobQueue()
        worker = Worker(queue)

        # Submit an unsupported task.
        job = await queue.submit({
            "task": "unknown"
        })

        await worker.run_once()

        # Verify that the failure was recorded.
        assert job.status == JobStatus.FAILED
        assert "Unknown task" in job.error
        assert job.attempts == 1

        await queue.join()

    asyncio.run(run_test())


def test_multiple_workers():
    async def run_test():
        queue = JobQueue()

        worker1 = Worker(queue)
        worker2 = Worker(queue)

        # Add two jobs that simulate I/O.
        job1 = await queue.submit({
            "task": "sleep",
            "seconds": 0.1
        })

        job2 = await queue.submit({
            "task": "sleep",
            "seconds": 0.1
        })

        # Both workers process jobs concurrently.
        await asyncio.gather(
            worker1.run_once(),
            worker2.run_once()
        )

        assert job1.status == JobStatus.COMPLETED
        assert job2.status == JobStatus.COMPLETED

        await queue.join()

    asyncio.run(run_test())
