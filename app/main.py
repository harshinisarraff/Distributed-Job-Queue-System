
import asyncio
from contextlib import asynccontextmanager
from uuid import UUID

from fastapi import FastAPI, HTTPException, Request

from app.models import Job, JobRequest
from app.queue import JobQueue
from app.worker import Worker


# Manage what happens when the API starts and stops.
@asynccontextmanager
async def lifespan(app: FastAPI):

    # Create one shared queue with capacity for 1000 jobs.
    queue = JobQueue(maxsize=1000)

    # Store the queue so API endpoints can access it.
    app.state.queue = queue

    # Create two workers that share the same queue.
    workers = [
        Worker(queue),
        Worker(queue)
    ]

    # Start both workers as asynchronous background tasks.
    worker_tasks = [
        asyncio.create_task(worker.run_forever())
        for worker in workers
    ]

    try:
        # Allow FastAPI to start accepting HTTP requests.
        yield

    finally:
        # Wait for all submitted jobs to be acknowledged.
        await queue.join()

        # Stop workers after processing is finished.
        for task in worker_tasks:
            task.cancel()

        # Wait until both worker tasks have exited.
        await asyncio.gather(
            *worker_tasks,
            return_exceptions=True
        )


# Initialize FastAPI and attach the startup/shutdown logic.
app = FastAPI(
    title="Distributed Job Queue",
    description="A high-throughput asynchronous job processing system",
    version="1.0.0",
    lifespan=lifespan
)


# Check whether the API is responding.
@app.get("/health")
async def health_check():
    return {
        "status": "healthy",
        "service": "distributed-job-queue"
    }


# Return a welcome message.
@app.get("/")
async def root():
    return {
        "message": "Distributed Job Queue API is running"
    }


# Accept a new job from a client.
@app.post("/jobs", response_model=Job, status_code=202)
async def submit_job(job_request: JobRequest, request: Request):

    # Access the shared job queue.
    queue: JobQueue = request.app.state.queue

    # Create and enqueue a new job.
    job = await queue.submit(job_request.payload)

    # Return the job information to the client.
    return job


# Retrieve a job by its unique ID.
@app.get("/jobs/{job_id}", response_model=Job)
async def get_job(job_id: UUID, request: Request):

    # Access the shared queue.
    queue: JobQueue = request.app.state.queue

    # Look up the job using its ID.
    job = queue.get_job(job_id)

    # Return an HTTP 404 if the job doesn't exist.
    if job is None:
        raise HTTPException(
            status_code=404,
            detail="Job not found"
        )

    # Return its current status and result.
    return job
