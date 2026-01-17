import asyncio
import time
from fastapi import FastAPI, HTTPException

from db.pool import ConnectionPool, PoolTimeout


# ----------------------------
# Configuration (inline for now)
# ----------------------------

POOL_SIZE = 5
QUERY_LATENCY_SECONDS = 0.2
ACQUIRE_TIMEOUT_SECONDS = 0.5


# ----------------------------
# Application setup
# ----------------------------

app = FastAPI(title="Database Service Simulator")

pool = ConnectionPool(max_connections=POOL_SIZE)


# ----------------------------
# Endpoints
# ----------------------------

@app.get("/health")
async def health():
    return {
        "status": "ok",
        "pool_active": pool.active,
        "pool_waiting": pool.waiting,
    }


@app.get("/db_query/{item_id}")
async def db_query(item_id: int):
    """
    Simulates a database query guarded by an application-side
    connection pool.

    This endpoint is intentionally simple:
    - acquire connection
    - do work
    - release connection
    """
    start = time.monotonic()

    try:
        async with await pool.acquire(timeout=ACQUIRE_TIMEOUT_SECONDS):
            # Simulate database work
            await asyncio.sleep(QUERY_LATENCY_SECONDS)

    except PoolTimeout:
        print(
                f"[POOL] max={pool.max_connections} "
                f"active={pool.active} "
                f"waiting={pool.waiting}"
                f"timeout_count={pool.timeout_count}"
            )
        raise HTTPException(
            status_code=503,
            detail="Database overloaded (connection pool timeout)"
        )

    elapsed = time.monotonic() - start

    print(
        f"[POOL] max={pool.max_connections} "
        f"active={pool.active} "
        f"waiting={pool.waiting} "
        f"timeout_count={pool.timeout_count} "
    )

    return {
        "item_id": item_id,
        "latency_seconds": round(elapsed, 3),
        "pool_active": pool.active,
        "pool_waiting": pool.waiting,
    }
