"""Durable, transaction-safe SQLite broker for workers on one shared host.

All changes to job state are atomic SQLite transactions. Workers communicate
through the database rather than sharing Python memory. SQLite WAL supports
multiple local processes; it is NOT a multi-machine broker.
"""
from __future__ import annotations

import json
from contextlib import contextmanager
import sqlite3
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from uuid import uuid4

from app.models import Job, JobStatus


class QueueFull(Exception):
    """Job capacity has been reached."""


def _datetime(timestamp: float | None) -> datetime | None:
    return datetime.fromtimestamp(timestamp, tz=timezone.utc) if timestamp is not None else None


class JobStore:
    def __init__(self, path: str, max_pending: int = 1000):
        if max_pending < 1:
            raise ValueError("max_pending must be at least 1")
        self.path = str(Path(path).expanduser().resolve())
        self.max_pending = max_pending
        Path(self.path).parent.mkdir(parents=True, exist_ok=True)
        self.initialize()

    @contextmanager
    def _connect(self):
        conn = sqlite3.connect(self.path, timeout=10, isolation_level=None)
        try:
            conn.row_factory = sqlite3.Row
            conn.execute("PRAGMA busy_timeout=10000")
            yield conn
        finally:
            conn.close()

    def initialize(self) -> None:
        with self._connect() as conn:
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("""
                CREATE TABLE IF NOT EXISTS jobs (
                    id TEXT PRIMARY KEY,
                    payload TEXT NOT NULL,
                    status TEXT NOT NULL,
                    result TEXT,
                    error TEXT,
                    attempts INTEGER NOT NULL DEFAULT 0,
                    max_attempts INTEGER NOT NULL DEFAULT 3,
                    created_at REAL NOT NULL,
                    updated_at REAL NOT NULL,
                    available_at REAL NOT NULL,
                    lease_until REAL,
                    lease_owner TEXT
                )
            """)
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_jobs_ready
                ON jobs(status, available_at, created_at)
            """)
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_jobs_lease
                ON jobs(status, lease_until)
            """)

    @staticmethod
    def _as_job(row: sqlite3.Row) -> Job:
        return Job(
            id=row["id"],
            payload=json.loads(row["payload"]),
            status=JobStatus(row["status"]),
            result=json.loads(row["result"]) if row["result"] is not None else None,
            error=row["error"],
            attempts=row["attempts"],
            max_attempts=row["max_attempts"],
            created_at=_datetime(row["created_at"]),
            updated_at=_datetime(row["updated_at"]),
            available_at=_datetime(row["available_at"]),
            lease_until=_datetime(row["lease_until"]),
        )

    def submit(self, payload: dict[str, Any], max_attempts: int = 3) -> Job:
        if not 1 <= max_attempts <= 10:
            raise ValueError("max_attempts must be from 1 to 10")
        payload_json = json.dumps(payload, allow_nan=False)
        now = time.time()
        job_id = str(uuid4())
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            # Count ALL unfinished jobs, including active and retry-delayed jobs.
            active = conn.execute("""
                SELECT COUNT(*) FROM jobs
                WHERE status IN ('pending','retrying','running')
            """).fetchone()[0]
            if active >= self.max_pending:
                conn.rollback()
                raise QueueFull("The job queue is currently full")
            conn.execute("""
                INSERT INTO jobs (id, payload, status, attempts, max_attempts,
                                  created_at, updated_at, available_at)
                VALUES (?, ?, 'pending', 0, ?, ?, ?, ?)
            """, (job_id, payload_json, max_attempts, now, now, now))
            row = conn.execute("SELECT * FROM jobs WHERE id=?", (job_id,)).fetchone()
            conn.commit()
        return self._as_job(row)

    def get_job(self, job_id: str) -> Job | None:
        with self._connect() as conn:
            row = conn.execute("SELECT * FROM jobs WHERE id=?", (str(job_id),)).fetchone()
        return self._as_job(row) if row else None

    @staticmethod
    def _recover_locked(conn: sqlite3.Connection, now: float) -> int:
        """Recover jobs whose owner lost its lease (at-least-once processing)."""
        expired = conn.execute("""
            SELECT id, attempts, max_attempts FROM jobs
            WHERE status='running' AND lease_until <= ?
        """, (now,)).fetchall()
        for row in expired:
            if row["attempts"] >= row["max_attempts"]:
                conn.execute("""
                    UPDATE jobs SET status='failed',
                        error='Worker lease expired; attempts exhausted',
                        lease_owner=NULL, lease_until=NULL, updated_at=?
                    WHERE id=?
                """, (now, row["id"]))
            else:
                conn.execute("""
                    UPDATE jobs SET status='retrying', available_at=?,
                        error='Worker lease expired; retry scheduled',
                        lease_owner=NULL, lease_until=NULL, updated_at=?
                    WHERE id=?
                """, (now, now, row["id"]))
        return len(expired)

    def recover_expired(self) -> int:
        now = time.time()
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            recovered = self._recover_locked(conn, now)
            conn.commit()
        return recovered

    def claim(self, owner: str, lease_seconds: float = 30) -> Job | None:
        if lease_seconds <= 0:
            raise ValueError("lease_seconds must be positive")
        now = time.time()
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            self._recover_locked(conn, now)
            row = conn.execute("""
                SELECT id FROM jobs
                WHERE status IN ('pending','retrying') AND available_at <= ?
                ORDER BY available_at ASC, created_at ASC LIMIT 1
            """, (now,)).fetchone()
            if row is None:
                conn.commit()
                return None
            conn.execute("""
                UPDATE jobs SET status='running', attempts=attempts+1,
                    lease_owner=?, lease_until=?, updated_at=? WHERE id=?
            """, (owner, now + lease_seconds, now, row["id"]))
            claimed = conn.execute("SELECT * FROM jobs WHERE id=?", (row["id"],)).fetchone()
            conn.commit()
        return self._as_job(claimed)

    def complete(self, job_id: str, owner: str, result: Any) -> bool:
        now = time.time()
        result_json = json.dumps(result, allow_nan=False)
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            cursor = conn.execute("""
                UPDATE jobs SET status='completed', result=?, error=NULL,
                    lease_owner=NULL, lease_until=NULL, updated_at=?
                WHERE id=? AND status='running' AND lease_owner=?
                  AND lease_until > ?
            """, (result_json, now, str(job_id), owner, now))
            conn.commit()
        return cursor.rowcount == 1

    def fail(self, job_id: str, owner: str, error: str, retryable: bool,
             base_delay: float = 0.1, max_delay: float = 2.0) -> bool:
        now = time.time()
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute("""
                SELECT attempts, max_attempts FROM jobs
                WHERE id=? AND status='running' AND lease_owner=? AND lease_until > ?
            """, (str(job_id), owner, now)).fetchone()
            if row is None:
                conn.commit()
                return False
            can_retry = retryable and row["attempts"] < row["max_attempts"]
            if can_retry:
                delay = min(base_delay * (2 ** (row["attempts"] - 1)), max_delay)
                conn.execute("""
                    UPDATE jobs SET status='retrying', error=?, available_at=?,
                        lease_owner=NULL, lease_until=NULL, updated_at=?
                    WHERE id=?
                """, (error, now + delay, now, str(job_id)))
            else:
                conn.execute("""
                    UPDATE jobs SET status='failed', error=?,
                        lease_owner=NULL, lease_until=NULL, updated_at=?
                    WHERE id=?
                """, (error, now, str(job_id)))
            conn.commit()
        return True

    def stats(self) -> dict[str, int]:
        counts = {status.value: 0 for status in JobStatus}
        with self._connect() as conn:
            for row in conn.execute("SELECT status, COUNT(*) AS n FROM jobs GROUP BY status"):
                counts[row["status"]] = row["n"]
        counts["active"] = counts["pending"] + counts["retrying"] + counts["running"]
        counts["capacity"] = self.max_pending
        return counts

    def prune_completed(self, older_than_seconds: float) -> int:
        """Delete terminal job history older than the retention period."""
        threshold = time.time() - older_than_seconds
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            cur = conn.execute("""
                DELETE FROM jobs WHERE status IN ('completed','failed') AND updated_at < ?
            """, (threshold,))
            conn.commit()
        return cur.rowcount
