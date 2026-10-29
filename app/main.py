"""HTTP API for job submission, status lookup, and queue metrics."""
import asyncio
import os
from contextlib import asynccontextmanager
from uuid import UUID

from fastapi import FastAPI, HTTPException, Request

from app.models import Job, JobRequest
from app.queue import JobQueue
from app.store import QueueFull
from app.worker import Worker


def create_app(db_path: str | None = None, embedded_workers: bool | None = None,
               max_pending: int | None = None) -> FastAPI:
    """App factory supports isolated tests and separate API/worker containers."""
    if db_path is None:
        db_path = os.getenv("DB_PATH", "data/jobs.sqlite3")
    if max_pending is None:
        max_pending = int(os.getenv("QUEUE_CAPACITY", "1000"))
    if embedded_workers is None:
        embedded_workers = os.getenv("EMBEDDED_WORKERS", "1") == "1"

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        queue = JobQueue(db_path=db_path, maxsize=max_pending)
        app.state.queue = queue
        stop_event = asyncio.Event()
        worker_tasks = []
        if embedded_workers:
            # Embedded workers are convenient for local development.
            worker_tasks = [
                asyncio.create_task(Worker(queue).run_forever(stop_event))
                for _ in range(int(os.getenv("WORKER_COUNT", "2")))
            ]
        try:
            yield
        finally:
            # Stop accepting HTTP requests before this lifespan cleanup runs.
            # Give in-progress jobs limited time to finish before exiting.
            if worker_tasks:
                deadline = asyncio.get_running_loop().time() + 5.0
                while asyncio.get_running_loop().time() < deadline:
                    if (await queue.stats())["active"] == 0:
                        break
                    await asyncio.sleep(0.05)
                stop_event.set()
                try:
                    await asyncio.wait_for(asyncio.gather(*worker_tasks), timeout=2)
                except TimeoutError:
                    for task in worker_tasks:
                        task.cancel()
                    await asyncio.gather(*worker_tasks, return_exceptions=True)

    app = FastAPI(
        title="Distributed Job Queue System",
        description="Durable multi-process asynchronous job queue with retries and backpressure",
        version="1.0.0",
        lifespan=lifespan,
    )

    @app.get("/")
    async def root():
        return {"message": "Distributed Job Queue API is running"}

    @app.get("/health")
    async def health(request: Request):
        # A database query verifies that the queue storage is reachable.
        await request.app.state.queue.stats()
        return {"status": "healthy", "service": "distributed-job-queue"}

    @app.get("/metrics")
    async def metrics(request: Request):
        return await request.app.state.queue.stats()

    @app.post("/jobs", response_model=Job, status_code=202)
    async def submit_job(job_request: JobRequest, request: Request):
        try:
            return await request.app.state.queue.submit(
                job_request.payload, max_attempts=job_request.max_attempts,
                timeout=0.5,
            )
        except QueueFull as exc:
            raise HTTPException(status_code=503, detail=str(exc),
                                headers={"Retry-After": "1"}) from exc
        except (TypeError, ValueError) as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc

    @app.get("/jobs/{job_id}", response_model=Job)
    async def get_job(job_id: UUID, request: Request):
        job = await request.app.state.queue.get_job(job_id)
        if job is None:
            raise HTTPException(status_code=404, detail="Job not found")
        return job

    return app


app = create_app()
