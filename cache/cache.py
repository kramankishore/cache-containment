import asyncio
import os
from typing import Any, Callable, Awaitable
from prometheus_client import Counter


CONTAINED_CACHE = os.getenv("CONTAINED_CACHE", "0") == "1"
MAX_CONCURRENT_LOADS = int(os.getenv("CACHE_MAX_LOADS", "5"))

# ------------------------------
# Prometheus metrics
# ------------------------------

CACHE_HITS = Counter(
    "cache_hits_total",
    "Total cache hits"
)

CACHE_MISSES = Counter(
    "cache_misses_total",
    "Total cache misses"
)


class Cache:
    """
    Speed-first in-memory cache.

    Behavior:
    - Cache hit: return immediately
    - Cache miss: call loader immediately (no gating)
    - Store result and return

    This cache intentionally does NOT:
    - limit concurrency
    - coalesce requests
    - apply backpressure
    """

    def __init__(self):
        self._store: dict[str, Any] = {}
        self._lock = asyncio.Lock()

        # Simple metrics
        self.hits = 0
        self.misses = 0

        self._contained = CONTAINED_CACHE
        self._load_semaphore = (
            asyncio.Semaphore(MAX_CONCURRENT_LOADS)
            if self._contained
            else None
        )

        print(
            f"[CACHE] mode={'CONTAINED' if self._contained else 'SPEED_FIRST'} "
            f"max_loads={MAX_CONCURRENT_LOADS if self._contained else '∞'}"
        )


    async def get(
        self,
        key: str,
        loader: Callable[[], Awaitable[Any]],
    ) -> Any:
        # Fast path: check cache without lock
        if key in self._store:
            self.hits += 1
            CACHE_HITS.inc()

            return [self._store[key], 1, 0]  # value, hit, miss

        # ---- cache miss ----
        self.misses += 1
        CACHE_MISSES.inc()

        print(
            f"[CACHE_MISS] "
            f"key={key} "
            f"mode={'CONTAINED' if self._contained else 'SPEED_FIRST'}"
        )

        # ---- CONTAINMENT TOGGLE ----
        if self._contained:
            # Waiting happens HERE instead of DB pool
            print(
                f"[CACHE_WAIT] "
                f"key={key} "
                f"in_flight={self._load_semaphore._value}"
            )

            async with self._load_semaphore:
                value = await loader()

        else:
            # Waiting happens downstream (DB pool)
            value = await loader()

        # Store result (protected to avoid race corruption)
        async with self._lock:
            if key not in self._store:
                self._store[key] = value

        print(
            f"[CACHE_FILL] "
            f"key={key}"
        )

        return [value, 0, 1]  # value, hit, miss


    def clear(self):
        size = len(self._store)
        self._store.clear()
        return size
