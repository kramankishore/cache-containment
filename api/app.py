import asyncio
import time
import os
import httpx
from fastapi import FastAPI, HTTPException, Response, Request

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

# Requests that ENTER the system
REQUESTS_STARTED = Counter(
    "api_requests_started_total",
    "Total API requests started",
    ["method", "path"]
)

# Requests that COMPLETE (success or failure)
REQUESTS_COMPLETED = Counter(
    "api_requests_total",
    "Total API requests completed",
    ["method", "path", "status"]
)

# Explicit failures (guaranteed)
REQUESTS_FAILED = Counter(
    "api_requests_failed_total",
    "Total API requests failed",
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
async def get_item(item_id: int, request: Request):
    path = "/item/{id}"
    method = request.method

    # ---- request ARRIVAL ----
    REQUESTS_STARTED.labels(method=method, path=path).inc()

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

    except asyncio.CancelledError:
        status = "499"

        REQUESTS_FAILED.labels(
            method=method,
            path=path,
            status=status
        ).inc()

        raise

    except HTTPException as e:
        status = str(e.status_code)

        REQUESTS_FAILED.labels(
            method=method,
            path=path,
            status=status
        ).inc()

        raise

    except Exception:
        status = "500"

        REQUESTS_FAILED.labels(
            method=method,
            path=path,
            status=status
        ).inc()

        raise

    finally:
        elapsed = time.monotonic() - start

        REQUEST_LATENCY.observe(elapsed)

        REQUESTS_COMPLETED.labels(
            method=method,
            path=path,
            status=status
        ).inc()

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

@app.get("/metrics")
def metrics():
    return Response(
        generate_latest(),
        media_type=CONTENT_TYPE_LATEST
    )
