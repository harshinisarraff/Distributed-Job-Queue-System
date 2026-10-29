import pytest

from app.queue import JobQueue


@pytest.fixture
def queue(tmp_path):
    # Each test has a separate database; tests cannot affect each other.
    return JobQueue(db_path=str(tmp_path / "jobs.sqlite3"), maxsize=20)
