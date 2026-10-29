import asyncio

from app.models import JobStatus


def test_submit_claim_and_complete(queue):
    async def run():
        first = await queue.submit({"task": "add", "a": 2, "b": 3})
        second = await queue.submit({"task": "multiply", "a": 2, "b": 4})
        assert first.id != second.id
        claimed = await queue.claim("worker-1")
        assert claimed.id == first.id
        assert claimed.status == JobStatus.RUNNING
        assert claimed.attempts == 1
        assert await queue.complete(first.id, "worker-1", 5)
        assert (await queue.get_job(first.id)).result == 5
        assert (await queue.claim("worker-2")).id == second.id
    asyncio.run(run())


def test_claim_once_across_process_style_connections(queue):
    async def run():
        from app.queue import JobQueue
        other = JobQueue(db_path=queue.store.path, maxsize=20)
        job = await queue.submit({"task": "add", "a": 1, "b": 2})
        claims = await asyncio.gather(queue.claim("worker-1"), other.claim("worker-2"))
        assert sum(c is not None for c in claims) == 1
        assert next(c for c in claims if c is not None).id == job.id
    asyncio.run(run())


def test_lease_recovery(queue):
    async def run():
        import sqlite3
        job = await queue.submit({"task": "add", "a": 2, "b": 3})
        claimed = await queue.claim("crashed-worker")
        assert claimed.id == job.id
        # Force lease expiry, simulating a dead worker without sleeping 30s.
        with sqlite3.connect(queue.store.path) as conn:
            conn.execute("UPDATE jobs SET lease_until=0 WHERE id=?", (str(job.id),))
        recovered = await queue.claim("recovery-worker")
        assert recovered.id == job.id
        assert recovered.attempts == 2
        # Late results from the old worker must not overwrite recovery.
        assert not await queue.complete(job.id, "crashed-worker", 99)
        assert await queue.complete(job.id, "recovery-worker", 5)
        assert (await queue.get_job(job.id)).result == 5
    asyncio.run(run())


def test_expired_final_attempt_becomes_failed(queue):
    async def run():
        import sqlite3
        job = await queue.submit({"task": "sleep", "seconds": 0}, max_attempts=1)
        await queue.claim("crashed-worker")
        with sqlite3.connect(queue.store.path) as conn:
            conn.execute("UPDATE jobs SET lease_until=0 WHERE id=?", (str(job.id),))
        assert await queue.claim("another-worker") is None
        updated = await queue.get_job(job.id)
        assert updated.status == JobStatus.FAILED
        assert updated.attempts == 1
    asyncio.run(run())
