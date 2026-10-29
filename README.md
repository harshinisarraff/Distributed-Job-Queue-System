# Distributed Job Queue System

A Python job-processing service with a FastAPI HTTP interface, asynchronous workers, a **durable SQLite broker**, configurable backpressure, retry scheduling with exponential backoff, worker leases for crash recovery, and Docker Compose deployment with **separate API and worker processes**.

> **Scope:** This version runs multiple worker processes/containers on **one host sharing a local SQLite database volume**. It is *not* a multi-host Redis/Kafka system. SQLite is chosen to make it runnable without a separate broker. For multi-machine deployment, replace the broker with Redis Streams or another network service. Do not claim an 8K–10K jobs/s result without measuring it.

## Architecture

```text
HTTP client --POST /jobs--> FastAPI API process
                                |
                                v
                       SQLite job store (WAL)
                          /            \
                         /              \
             worker process A      worker process B
                   |                      |
           claim -> execute         claim -> execute
                   \                      /
                    \--- job status ------/
                                |
HTTP client --GET /jobs/{id}------+
```

Each process accesses the same database. Job claims and status changes happen in SQLite transactions (`BEGIN IMMEDIATE`), so two workers cannot claim the same pending job simultaneously. A claimed job receives a lease. When a worker crashes, another worker recovers an expired lease and retries the job **at least once**. Exactly-once execution is *not guaranteed*; external side effects should be idempotent.

## Quick start (macOS, Linux)

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements.txt
python -m uvicorn app.main:app --reload
```

Open http://127.0.0.1:8000/docs to try the API. By default the development API also starts two workers inside the API process.

### Submit a job

```bash
curl -s -X POST http://127.0.0.1:8000/jobs \
  -H 'Content-Type: application/json' \
  -d '{"payload":{"task":"add","a":10,"b":20},"max_attempts":3}'
```

The response includes `id`. Check the state with `curl http://127.0.0.1:8000/jobs/<id>`. The eventual result is `30`.

### Try automatic retries

```bash
curl -s -X POST http://127.0.0.1:8000/jobs \
  -H 'Content-Type: application/json' \
  -d '{"payload":{"task":"flaky","failures_before_success":2},"max_attempts":3}'
```

The demo task deliberately fails twice, then succeeds on attempt 3. Status transitions are `pending -> running -> retrying -> running -> retrying -> running -> completed`.

### Supported demo tasks

- `{"task":"add","a":10,"b":20}`
- `{"task":"multiply","a":7,"b":8}`
- `{"task":"sleep","seconds":0.25}` (0–5 seconds)
- `{"task":"flaky","failures_before_success":2}` (controlled transient failure demonstration)

## API

| Method | Route | Purpose |
|---|---|---|
| GET | `/` | Welcome message |
| GET | `/health` | API and DB storage health |
| POST | `/jobs` | Submit job (202) or reject full queue (503) |
| GET | `/jobs/{job_id}` | Job status, attempts, result, and errors |
| GET | `/metrics` | Counts by job status and active capacity |

`POST /jobs` validates `max_attempts` between 1 and 10. An unknown ID returns 404.

## Separate processes without Docker

Terminal A (only API, does not embed workers):

```bash
EMBEDDED_WORKERS=0 python -m uvicorn app.main:app --reload
```

Terminal B (independent worker pool sharing `DB_PATH`):

```bash
WORKER_COUNT=2 python -m app.worker_service
```

Leave `DB_PATH` the same for all processes. Both use `data/jobs.sqlite3` by default.

## Docker Compose

```bash
docker compose up --build
```

The `api` and `worker` services share a named local Docker volume at `/app/data`. **Do not** replace this with an NFS-mounted SQLite database; WAL requires compatible local locking and is not appropriate for generic distributed filesystems.

Stop with `docker compose down` (keep volume) or `docker compose down -v` (delete stored jobs).

## Reliability details

- **Backpressure:** Caps all active (`pending`, `retrying`, `running`) jobs. A submission waits up to 0.5 seconds for capacity, then receives HTTP 503 + `Retry-After: 1`.
- **Retry policy:** Retry only `TransientJobError`, `ConnectionError`, and `TimeoutError`. Nonretryable exceptions (e.g. invalid payload) fail immediately. Delay is `min(0.1 * 2**(attempt-1), 2.0)` seconds.
- **Lease recovery:** Running jobs have a 30-second lease. If a worker disappears, another worker can reclaim the expired job when polling. This provides *at-least-once*, not exactly-once, behavior. A worker still running after its lease expires may duplicate work; use idempotency keys for real side effects.
- **Shutdown:** Embedded API workers wait briefly for work to finish and stop claiming more jobs; in-flight jobs beyond the grace period may be reclaimed after lease expiry. The standalone worker service handles SIGINT and SIGTERM.
- **History retention:** `JobQueue.cleanup(seconds)` deletes terminal jobs older than a chosen age. It is a management utility, not an automated cron job; set up a periodic cleanup in production.
- **SQLite is the bottleneck:** Writes serialize, and this design is for one host with moderate load. Move to Redis Streams/PostgreSQL for stronger scale and multi-host use.
- **Security:** No authentication, rate limiting, tenant isolation, or worker sandbox; this is a local portfolio demo, not a public production service. Avoid exposing it directly to the internet.

## Tests

```bash
python -m pytest -v
```

Tests cover HTTP endpoints, cross-connection claims, basic workers, backpressure, exponential retries, lease expiry, and recovery.

## Performance benchmarks

Start the API (and workers) then run:

```bash
python benchmarks/load_test.py --count 200 --concurrency 20
```

Reports accepted/rejected requests, completed jobs per second, and **approximate** observed p50/p95 latency. Measurements include HTTP/polling overhead. The benchmark does **not** establish an 8K–10K jobs/sec throughput claim or 40% performance gain; test under controlled workloads before adding results to your resume.

## Source layout

```text
app/
  main.py             # FastAPI entry point and app lifecycle
  models.py           # Pydantic job/request models
  queue.py            # Async API to durable job store
  store.py            # SQLite transactional broker / lease handling
  worker.py           # Task execution and retries
  worker_service.py   # Standalone worker process entry point
benchmarks/load_test.py
tests/
Dockerfile
compose.yaml
requirements.txt
```

## Next improvements

Redis Streams with consumer groups for multi-host deployment; heartbeat-based lease renewal; authentication and per-client quotas; automated cleanup; structured metrics and tracing; real external task adapters.
