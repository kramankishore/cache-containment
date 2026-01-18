import asyncio
import random
import time

from db.pool import ConnectionPool, PoolTimeout


POOL_SIZE = 5
QUERY_LATENCY = 0.5      # seconds per "DB query"
TOTAL_REQUESTS = 30
ACQUIRE_TIMEOUT = 1.0   # seconds


async def simulated_query(pool: ConnectionPool, request_id: int):
    start = time.monotonic()
    try:
        async with await pool.acquire(timeout=ACQUIRE_TIMEOUT):
            # Simulate DB work
            await asyncio.sleep(QUERY_LATENCY)

            elapsed = time.monotonic() - start
            print(
                f"[OK] request={request_id:02d} "
                f"elapsed={elapsed:.2f}s "
                f"active={pool.active} "
                f"waiting={pool.waiting}"
            )

    except PoolTimeout:
        elapsed = time.monotonic() - start
        print(
            f"[TIMEOUT] request={request_id:02d} "
            f"elapsed={elapsed:.2f}s "
            f"active={pool.active} "
            f"waiting={pool.waiting}"
        )


async def monitor(pool: ConnectionPool):
    """
    Periodically print pool state.
    This makes queueing and saturation visible over time.
    """
    while True:
        await asyncio.sleep(0.2)
        print(
            f"[POOL] active={pool.active} "
            f"waiting={pool.waiting} "
            f"timeouts={pool.timeout_count}"
        )


async def main():
    pool = ConnectionPool(max_connections=POOL_SIZE)

    # Start monitoring task
    monitor_task = asyncio.create_task(monitor(pool))

    # Fire a burst of concurrent requests
    tasks = []
    for i in range(TOTAL_REQUESTS):
        tasks.append(asyncio.create_task(simulated_query(pool, i)))

        # Small jitter to avoid perfect simultaneity
        await asyncio.sleep(random.uniform(0.01, 0.05))

    await asyncio.gather(*tasks)

    monitor_task.cancel()

    print("\n--- Summary ---")
    print(f"Max connections     : {pool.max_connections}")
    print(f"Total requests      : {TOTAL_REQUESTS}")
    print(f"Timeouts            : {pool.timeout_count}")


if __name__ == "__main__":
    asyncio.run(main())
