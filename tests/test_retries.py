import asyncio
from app.models import JobStatus
from app.worker import Worker


def test_flaky_job_succeeds_after_two_retries(queue):
    async def run():
        job = await queue.submit({"task": "flaky", "failures_before_success": 2}, max_attempts=3)
        worker = Worker(queue, retry_base_delay=0.01)
        for attempt in range(1, 4):
            await worker.run_once()
            current = await queue.get_job(job.id)
            if attempt < 3:
                assert current.status == JobStatus.RETRYING
                assert current.attempts == attempt
                await asyncio.sleep(0.025)
        current = await queue.get_job(job.id)
        assert current.status == JobStatus.COMPLETED
        assert current.result == {"message": "Recovered", "attempt": 3}
        assert current.error is None
    asyncio.run(run())


def test_exhausted_retries(queue):
    async def run():
        job = await queue.submit({"task": "flaky", "failures_before_success": 5}, max_attempts=2)
        worker = Worker(queue, retry_base_delay=0.001)
        await worker.run_once()
        await asyncio.sleep(0.01)
        await worker.run_once()
        current = await queue.get_job(job.id)
        assert current.status == JobStatus.FAILED
        assert current.attempts == 2
        assert "temporary" in current.error
    asyncio.run(run())


def test_permanent_failure_does_not_retry(queue):
    async def run():
        job = await queue.submit({"task": "unknown"}, max_attempts=8)
        await Worker(queue).run_once()
        current = await queue.get_job(job.id)
        assert current.status == JobStatus.FAILED
        assert current.attempts == 1
    asyncio.run(run())


def test_backoff_increases_and_caps(queue):
    async def run():
        job = await queue.submit({"task": "flaky", "failures_before_success": 9}, max_attempts=4)
        worker = Worker(queue, retry_base_delay=0.1, retry_max_delay=0.15)
        await worker.run_once()
        first = await queue.get_job(job.id)
        assert first.status == JobStatus.RETRYING
        first_delay = (first.available_at - first.updated_at).total_seconds()
        assert 0.08 < first_delay < 0.12
        await asyncio.sleep(0.11)
        await worker.run_once()
        second = await queue.get_job(job.id)
        second_delay = (second.available_at - second.updated_at).total_seconds()
        assert 0.13 < second_delay < 0.17
    asyncio.run(run())
