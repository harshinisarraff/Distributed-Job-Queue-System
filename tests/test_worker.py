import asyncio

from app.models import JobStatus
from app.worker import Worker


def test_worker_success(queue):
    async def run():
        job = await queue.submit({"task": "add", "a": 10, "b": 20})
        assert await Worker(queue).run_once()
        finished = await queue.get_job(job.id)
        assert finished.status == JobStatus.COMPLETED
        assert finished.result == 30
        assert finished.attempts == 1
    asyncio.run(run())


def test_worker_failure(queue):
    async def run():
        job = await queue.submit({"task": "unknown"})
        assert await Worker(queue).run_once()
        finished = await queue.get_job(job.id)
        assert finished.status == JobStatus.FAILED
        assert "Unknown task" in finished.error
        assert finished.attempts == 1
    asyncio.run(run())


def test_multiple_workers(queue):
    async def run():
        jobs = [await queue.submit({"task": "sleep", "seconds": 0.02}) for _ in range(2)]
        await asyncio.gather(Worker(queue).run_once(), Worker(queue).run_once())
        statuses = [(await queue.get_job(j.id)).status for j in jobs]
        assert all(status == JobStatus.COMPLETED for status in statuses)
    asyncio.run(run())
