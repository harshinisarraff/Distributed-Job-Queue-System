"""Simple end-to-end HTTP load test; measures actual observed throughput.

Run: python benchmarks/load_test.py --count 200 --concurrency 20
This is a development benchmark, not a scientifically controlled benchmark.
"""
import argparse
import asyncio
import statistics
import time

import httpx


async def benchmark(url: str, count: int, concurrency: int, timeout: float):
    limit = asyncio.Semaphore(concurrency)
    submitted_at = {}
    accepted = []
    rejected = 0
    started = time.perf_counter()
    async with httpx.AsyncClient(base_url=url, timeout=15.0) as client:
        async def submit(n):
            nonlocal rejected
            async with limit:
                start = time.perf_counter()
                response = await client.post("/jobs", json={
                    "payload": {"task": "add", "a": n, "b": 1},
                    "max_attempts": 2,
                })
                if response.status_code == 202:
                    job_id = response.json()["id"]
                    submitted_at[job_id] = start
                    accepted.append(job_id)
                else:
                    rejected += 1

        await asyncio.gather(*(submit(n) for n in range(count)))
        submission_seconds = time.perf_counter() - started
        completed = set()
        failed = set()
        latencies_ms = []
        deadline = time.monotonic() + timeout

        # Poll in batches. Latencies include polling delay and are approximate.
        async def check(job_id):
            async with limit:
                response = await client.get(f"/jobs/{job_id}")
                response.raise_for_status()
                state = response.json()["status"]
                if state == "completed":
                    completed.add(job_id)
                    latencies_ms.append((time.perf_counter() - submitted_at[job_id]) * 1000)
                elif state == "failed":
                    failed.add(job_id)

        while len(completed) + len(failed) < len(accepted) and time.monotonic() < deadline:
            remaining = [j for j in accepted if j not in completed and j not in failed]
            await asyncio.gather(*(check(j) for j in remaining))
            if len(completed) + len(failed) < len(accepted):
                await asyncio.sleep(0.05)
        elapsed = time.perf_counter() - started

    def percentile(values, pct):
        ordered = sorted(values)
        if not ordered:
            return None
        return ordered[min(len(ordered) - 1, int((len(ordered)-1) * pct))]

    print(f"Submitted: {count}, Accepted: {len(accepted)}, Rejected: {rejected}")
    print(f"Completed: {len(completed)}, Failed: {len(failed)}, Unfinished: {len(accepted)-len(completed)-len(failed)}")
    print(f"Submission rate: {count/submission_seconds:.1f} requests/s")
    print(f"Completed throughput: {len(completed)/elapsed:.1f} jobs/s")
    if latencies_ms:
        print(f"Observed latency p50: {percentile(latencies_ms, .50):.1f} ms")
        print(f"Observed latency p95: {percentile(latencies_ms, .95):.1f} ms")
        print(f"Observed latency mean: {statistics.mean(latencies_ms):.1f} ms")
    print("Note: this includes HTTP overhead and polling delay; report measured results only.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", default="http://127.0.0.1:8000")
    parser.add_argument("--count", type=int, default=200)
    parser.add_argument("--concurrency", type=int, default=20)
    parser.add_argument("--timeout", type=float, default=30)
    args = parser.parse_args()
    if args.count < 1 or args.concurrency < 1 or args.timeout <= 0:
        parser.error("count, concurrency, and timeout must be positive")
    asyncio.run(benchmark(args.url, args.count, args.concurrency, args.timeout))
