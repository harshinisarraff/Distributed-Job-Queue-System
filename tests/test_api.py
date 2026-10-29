
import time

from fastapi.testclient import TestClient

from app.main import app


# Test submitting a job and retrieving its result.
def test_submit_and_process_job():

    # Start FastAPI with its lifespan and background workers.
    with TestClient(app) as client:

        # Submit an addition job through the HTTP API.
        response = client.post(
            "/jobs",
            json={
                "payload": {
                    "task": "add",
                    "a": 10,
                    "b": 20
                }
            }
        )

        # Verify the API accepted the job.
        assert response.status_code == 202

        # Get the unique ID assigned to the job.
        job_id = response.json()["id"]

        # Allow up to 2 seconds for job processing.
        deadline = time.monotonic() + 2

        while time.monotonic() < deadline:

            # Retrieve the latest job status.
            result = client.get(f"/jobs/{job_id}")

            assert result.status_code == 200

            job = result.json()

            # Verify the worker processed the job correctly.
            if job["status"] == "completed":
                assert job["result"] == 30
                assert job["attempts"] == 1
                return

            # Fail immediately if job execution failed.
            if job["status"] == "failed":
                raise AssertionError(job["error"])

            # Briefly wait before checking again.
            time.sleep(0.01)

        raise AssertionError("Job did not complete in time")


# Verify that requesting an unknown job returns HTTP 404.
def test_job_not_found():
    with TestClient(app) as client:

        response = client.get(
            "/jobs/00000000-0000-0000-0000-000000000000"
        )

        assert response.status_code == 404
        assert response.json()["detail"] == "Job not found"
