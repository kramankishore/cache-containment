import asyncio
import random
import time
import httpx


API_URL = "http://localhost:8000/item"
RPS = 40                # requests per second
DURATION_SECONDS = 200
KEYSPACE = 500


async def worker(client: httpx.AsyncClient, request_id: int):
    item_id = random.randint(1, KEYSPACE)
    start = time.monotonic()

    try:
        resp = await client.get(f"{API_URL}/{item_id}", timeout=5.0)
        elapsed = time.monotonic() - start

        if resp.status_code == 200:
            data = resp.json()
            print(
                f"[OK] req={request_id:04d} "
                f"latency={elapsed:.3f}s "
            )
        else:
            print(
                f"[ERR] req={request_id:04d} "
                f"status={resp.status_code} "
                f"latency={elapsed:.3f}s"
            )

    except Exception as e:
        elapsed = time.monotonic() - start
        print(
            f"[FAIL] req={request_id:04d} "
            f"latency={elapsed:.3f}s "
            f"error={e}"
        )


async def main():
    async with httpx.AsyncClient() as client:
        start = time.monotonic()
        request_id = 0

        while time.monotonic() - start < DURATION_SECONDS:
            asyncio.create_task(worker(client, request_id))
            request_id += 1
            await asyncio.sleep(1 / RPS)

        # Let in-flight requests finish
        await asyncio.sleep(5)


if __name__ == "__main__":
    asyncio.run(main())
