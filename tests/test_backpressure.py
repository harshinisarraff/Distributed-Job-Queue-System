import asyncio
import pytest

from app.queue import JobQueue
from app.store import QueueFull


def test_full_queue_timeout(tmp_path):
    async def run():
        queue = JobQueue(str(tmp_path / "tiny.sqlite3"), maxsize=1)
        await queue.submit({"task": "add", "a": 1, "b": 2})
        with pytest.raises(QueueFull):
            await queue.submit({"task": "add", "a": 3, "b": 4}, timeout=0.03)
        assert (await queue.stats())["active"] == 1
    asyncio.run(run())


def test_capacity_frees_when_job_completes(tmp_path):
    async def run():
        queue = JobQueue(str(tmp_path / "tiny.sqlite3"), maxsize=1)
        first = await queue.submit({"task": "add", "a": 1, "b": 2})
        task = asyncio.create_task(queue.submit({"task": "add", "a": 3, "b": 4}, timeout=0.5))
        await asyncio.sleep(0.02)
        assert not task.done()
        assert (await queue.claim("worker")).id == first.id
        await queue.complete(first.id, "worker", 3)
        second = await asyncio.wait_for(task, 1)
        assert second.id != first.id
    asyncio.run(run())
