import asyncio
import time

import httpx
from fastapi import FastAPI, HTTPException

from cache.cache import Cache


# ----------------------------
# Configuration
# ----------------------------

DB_URL = "http://localhost:8001/db_query"
DB_TIMEOUT_SECONDS = 2.0


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

    [value, hit, miss] = await cache.get(key, load_from_db)

    elapsed = time.monotonic() - start

    print(
        f"[CACHE] hit={hit} "
        f"miss={miss} "
        f"cache_hits={cache.hits} "
        f"cache_misses={cache.misses} "
    )
    return {
        "item_id": item_id,
        "latency_seconds": round(elapsed, 3),
        "value": value,
    }

@app.post("/_control/clear_cache")
async def clear_cache():
    evicted = cache.clear()
    return {
        "status": "cache_cleared",
        "evicted_keys": evicted,
        "cache_hits": cache.hits,
        "cache_misses": cache.misses,
    }
