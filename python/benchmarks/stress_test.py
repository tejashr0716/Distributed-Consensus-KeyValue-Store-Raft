import asyncio
import random
import time
import statistics
from httpx import AsyncClient, ASGITransport
from python.api.main import app
from python.database.connection import close_db_pool
from python.services.redis_service import close_redis

TOTAL_OPS = 1000
CONCURRENCY = 15

async def worker(client: AsyncClient, queue: asyncio.Queue, latencies: list):
    while True:
        try:
            op_type, payload = queue.get_nowait()
        except asyncio.QueueEmpty:
            break

        t0 = time.perf_counter()
        if op_type == "SET":
            resp = await client.post("/api/v1/kv/set", json=payload)
        else:
            resp = await client.post("/api/v1/locks/acquire", json=payload)
        t1 = time.perf_counter()
        if resp.status_code in [200, 201, 409]:
            latencies.append((t1 - t0) * 1000.0)

async def run_stress_test():
    print("=================================================================")
    print("  ChandyStore Raft Consensus & Distributed Lock Stress Test       ")
    print(f"  Target: {TOTAL_OPS} operations | Concurrency: {CONCURRENCY} workers")
    print("=================================================================\n")

    queue = asyncio.Queue()
    for i in range(TOTAL_OPS):
        if i % 2 == 0:
            queue.put_nowait(("SET", {
                "key": f"stress:counter:{random.randint(1, 100)}",
                "value": f"val_{i}_{random.random()}",
                "client_id": f"CLIENT_{random.randint(1, 5)}"
            }))
        else:
            queue.put_nowait(("LOCK", {
                "lock_name": f"resource:lock_{random.randint(1, 20)}",
                "client_id": f"CLIENT_{random.randint(1, 10)}",
                "lease_ms": 1000
            }))

    latencies = []
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        start_time = time.perf_counter()
        tasks = [asyncio.create_task(worker(client, queue, latencies)) for _ in range(CONCURRENCY)]
        await asyncio.gather(*tasks)
        total_time = time.perf_counter() - start_time

    await close_db_pool()
    await close_redis()

    latencies.sort()
    throughput = len(latencies) / total_time
    p50 = statistics.median(latencies)
    p95 = latencies[int(len(latencies) * 0.95)]
    p99 = latencies[int(len(latencies) * 0.99)]

    print(f">> Completed:            {len(latencies)} / {TOTAL_OPS} operations")
    print(f">> Total Wall Time:      {total_time:.2f} seconds")
    print(f">> Sustained Throughput: {throughput:,.1f} ops/sec")
    print(f">> Latency p50:          {p50:.2f} ms")
    print(f">> Latency p95:          {p95:.2f} ms")
    print(f">> Latency p99:          {p99:.2f} ms")
    print("=================================================================")

if __name__ == "__main__":
    asyncio.run(run_stress_test())
