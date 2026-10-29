"""Start standalone workers sharing the same SQLite broker as the API process."""
import asyncio
import logging
import os
import signal

from app.queue import JobQueue
from app.worker import Worker


async def main():
    logging.basicConfig(level=logging.INFO)
    queue = JobQueue(
        db_path=os.getenv("DB_PATH", "data/jobs.sqlite3"),
        maxsize=int(os.getenv("QUEUE_CAPACITY", "1000")),
    )
    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop_event.set)
        except NotImplementedError:
            pass

    count = int(os.getenv("WORKER_COUNT", "2"))
    workers = [Worker(queue, worker_id=f"worker-{os.getpid()}-{i}") for i in range(count)]
    tasks = [asyncio.create_task(w.run_forever(stop_event)) for w in workers]
    logging.info("Started %s worker(s)", count)
    await stop_event.wait()
    # Stop claiming new work; give running tasks a short window to finish.
    try:
        await asyncio.wait_for(asyncio.gather(*tasks), timeout=8)
    except TimeoutError:
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)


if __name__ == "__main__":
    asyncio.run(main())
