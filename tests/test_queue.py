
import asyncio

from app.queue import JobQueue


def test_job_queue():
    async def run_test():
        queue = JobQueue(maxsize=10)

        # Submit two jobs to the queue.
        job1 = await queue.submit({"task": "add", "a": 5, "b": 10})
        job2 = await queue.submit({"task": "add", "a": 20, "b": 30})

        # Verify both jobs are waiting.
        assert queue.size() == 2

        # Verify we can retrieve a job by its ID.
        assert queue.get_job(job1.id) == job1

        # Verify FIFO ordering.
        first = await queue.get()
        assert first.id == job1.id
        queue.task_done()

        second = await queue.get()
        assert second.id == job2.id
        queue.task_done()

        # Verify all jobs are acknowledged.
        await queue.join()
        assert queue.size() == 0

    asyncio.run(run_test())
