import asyncio
import os
from typing import Any, Callable, Awaitable

CONTAINED_CACHE = os.getenv("CONTAINED_CACHE", "0") == "1"
MAX_CONCURRENT_LOADS = int(os.getenv("CACHE_MAX_LOADS", "5"))

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
            return [self._store[key], 1, 0] # value, hit, miss

        # Miss path
        self.misses += 1

        # ---- CONTAINMENT TOGGLE ----
        if self._contained:
            # Database calls are gated by the semaphore
            async with self._load_semaphore:
                value = await loader()
        else:
            # Call database immediately (speed-first)
            value = await loader()

        # Store result (protected to avoid race corruption)
        async with self._lock:
            # Double-check not required for correctness here,
            # but avoids overwriting if another request filled it.
            if key not in self._store:
                self._store[key] = value

        return [value, 0, 1] # value, hit, miss

    def clear(self):
        size = len(self._store)
        self._store.clear()
        return size
