import asyncio
import time
import os
import httpx
from fastapi import FastAPI, HTTPException, Response

from cache.cache import Cache

from prometheus_client import (
    Counter,
    Histogram,
    generate_latest,
    CONTENT_TYPE_LATEST,
)

# ----------------------------
# Configuration
# ----------------------------

DB_URL = os.getenv(
    "DB_URL",
    "http://localhost:8001/db_query"
)

DB_TIMEOUT_SECONDS = float(
    os.getenv("DB_TIMEOUT_SECONDS", "2.0")
)


# ------------------------------
# Prometheus metrics
# ------------------------------

REQUEST_COUNT = Counter(
    "api_requests_total",
    "Total API requests",
    ["method", "path", "status"]
)

REQUEST_LATENCY = Histogram(
    "api_request_latency_seconds",
    "API request latency",
    buckets=(0.05, 0.1, 0.2, 0.5, 1, 2, 5, 10, 15, 20, 25, 30, 35)
)


# ----------------------------
# Application setup
# ----------------------------

app = FastAPI(title="API Service")

cache = Cache()


# ----------------------------
# Endpoints
# ----------------------------

@app.get("/health")
async def health():
    return {
        "status": "ok",
        "cache_hits": cache.hits,
        "cache_misses": cache.misses,
    }

@app.get("/item/{item_id}")
async def get_item(item_id: int):
    key = f"item:{item_id}"
    start = time.monotonic()

    status = "200"

    async def load_from_db():
        async with httpx.AsyncClient() as client:
            try:
                resp = await client.get(
                    f"{DB_URL}/{item_id}",
                    timeout=DB_TIMEOUT_SECONDS,
                )
            except httpx.RequestError:
                raise HTTPException(
                    status_code=503,
                    detail="Database unreachable"
                )

        if resp.status_code != 200:
            raise HTTPException(
                status_code=503,
                detail="Database overloaded"
            )

        return resp.json()

    try:
        value, hit, miss = await cache.get(key, load_from_db)

        return {
            "item_id": item_id,
            "value": value,
        }

    except HTTPException:
        status = "503"
        raise

    finally:
        elapsed = time.monotonic() - start

        # ---- Prometheus ----
        REQUEST_LATENCY.observe(elapsed)
        REQUEST_COUNT.labels(
            method="GET",
            path="/item/{id}",
            status=status
        ).inc()

        # ---- Logs (human-first) ----
        print(
            f"[API] "
            f"item={item_id} "
            f"status={status} "
            f"elapsed={elapsed:.3f}s "
            f"hit={hit if status == '200' else '-'} "
            f"miss={miss if status == '200' else '-'}"
        )


@app.post("/_control/clear_cache")
async def clear_cache():
    evicted = cache.clear()
    return {
        "status": "cache_cleared",
        "evicted_keys": evicted,
        "cache_hits": cache.hits,
        "cache_misses": cache.misses,
    }

# Prometheus instrumented metrics exposed for collection by prometheus server
@app.get("/metrics")
def metrics():
    return Response(
        generate_latest(),
        media_type=CONTENT_TYPE_LATEST
    )
