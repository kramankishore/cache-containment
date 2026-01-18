import os
import asyncio
import time

from fastapi import FastAPI, HTTPException, Response
from prometheus_client import (
    Histogram,
    generate_latest,
    CONTENT_TYPE_LATEST,
)

from db.pool import ConnectionPool, PoolTimeout

# ----------------------------
# Configuration (env-owned)
# ----------------------------

DB_MAX_CONNECTIONS = int(
    os.getenv("DB_MAX_CONNECTIONS", "5")
)

DB_ACQUIRE_TIMEOUT_SECONDS = float(
    os.getenv("DB_ACQUIRE_TIMEOUT_SECONDS", "1.0")
)

QUERY_LATENCY_SECONDS = float(
    os.getenv("DB_QUERY_LATENCY_SECONDS", "0.2")
)

# ------------------------------
# Prometheus metrics
# ------------------------------

DB_QUERY_LATENCY = Histogram(
    "db_query_latency_seconds",
    "DB query latency",
    buckets=(0.05, 0.1, 0.2, 0.5, 1, 2)
)

# ----------------------------
# Application setup
# ----------------------------

app = FastAPI(title="DB Service")

pool = ConnectionPool(max_connections=DB_MAX_CONNECTIONS)

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
    start = time.monotonic()

    try:
        async with await pool.acquire(timeout=DB_ACQUIRE_TIMEOUT_SECONDS):
            await asyncio.sleep(QUERY_LATENCY_SECONDS)

    except PoolTimeout:
        elapsed = time.monotonic() - start

        print(
            f"[DB_TIMEOUT] "
            f"elapsed={elapsed:.3f}s "
            f"active={pool.active} "
            f"waiting={pool.waiting}"
        )

        raise HTTPException(
            status_code=503,
            detail="Database overloaded (connection pool timeout)"
        )

    finally:
        DB_QUERY_LATENCY.observe(time.monotonic() - start)

    elapsed = time.monotonic() - start

    print(
        f"[DB_OK] "
        f"elapsed={elapsed:.3f}s "
        f"active={pool.active} "
        f"waiting={pool.waiting}"
    )

    return {
        "item_id": item_id,
        "latency_seconds": round(elapsed, 3),
        "pool_active": pool.active,
        "pool_waiting": pool.waiting,
    }


@app.get("/metrics")
def metrics():
    return Response(
        generate_latest(),
        media_type=CONTENT_TYPE_LATEST
    )
