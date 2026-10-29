import time

from fastapi.testclient import TestClient

from app.main import create_app


def test_submit_and_process_job(tmp_path):
    app = create_app(db_path=str(tmp_path / "jobs.sqlite3"), embedded_workers=True)
    with TestClient(app) as client:
        response = client.post("/jobs", json={
            "payload": {"task": "add", "a": 10, "b": 20},
            "max_attempts": 2,
        })
        assert response.status_code == 202
        assert response.json()["max_attempts"] == 2
        job_id = response.json()["id"]
        deadline = time.monotonic() + 2
        while time.monotonic() < deadline:
            result = client.get(f"/jobs/{job_id}")
            if result.json()["status"] == "completed":
                assert result.json()["result"] == 30
                return
            time.sleep(0.01)
        raise AssertionError("Job did not complete in time")


def test_job_not_found_and_validation(tmp_path):
    app = create_app(db_path=str(tmp_path / "jobs.sqlite3"))
    with TestClient(app) as client:
        assert client.get("/jobs/00000000-0000-0000-0000-000000000000").status_code == 404
        assert client.post("/jobs", json={"payload": {}, "max_attempts": 0}).status_code == 422
        assert client.post("/jobs", json={"payload": {}, "max_attempts": 11}).status_code == 422
        assert client.get("/health").json()["status"] == "healthy"


def test_api_returns_503_on_capacity(tmp_path):
    app = create_app(db_path=str(tmp_path / "jobs.sqlite3"), embedded_workers=False, max_pending=1)
    with TestClient(app) as client:
        body = {"payload": {"task": "sleep", "seconds": 1}}
        assert client.post("/jobs", json=body).status_code == 202
        full = client.post("/jobs", json=body)
        assert full.status_code == 503
        assert full.headers.get("retry-after") == "1"
        assert client.get("/metrics").json()["active"] == 1
