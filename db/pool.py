import asyncio
import time
from typing import Optional
from prometheus_client import Gauge, Counter

# ------------------------------
# Prometheus metrics
# ------------------------------

DB_POOL_ACTIVE = Gauge(
    "db_pool_active_connections",
    "Active DB connections"
)

DB_POOL_WAITING = Gauge(
    "db_pool_waiting_requests",
    "Requests waiting for DB connection"
)

DB_POOL_TIMEOUTS = Counter(
    "db_pool_timeouts_total",
    "DB connection acquire timeouts"
)


class PoolTimeout(Exception):
    """Raised when a connection cannot be acquired within the timeout."""
    pass


class ConnectionPool:
    """
    Application-side connection pool.

    Models finite concurrency and explicit queueing.
    No environment awareness.
    """

    def __init__(self, max_connections: int):
        self._max_connections = max_connections
        self._semaphore = asyncio.Semaphore(max_connections)

        self._active = 0
        self._waiting = 0

        self._timeout_count = 0

        self._lock = asyncio.Lock()

    @property
    def max_connections(self) -> int:
        return self._max_connections

    @property
    def active(self) -> int:
        return self._active

    @property
    def waiting(self) -> int:
        return self._waiting

    @property
    def timeout_count(self) -> int:
        return self._timeout_count

    async def acquire(self, timeout: Optional[float] = None):
        """
        Acquire a connection from the pool.

        Waiting and timeout behavior is observable via metrics.
        """
        start = time.monotonic()

        async with self._lock:
            self._waiting += 1
            DB_POOL_WAITING.set(self._waiting)

        print(
            f"[POOL_WAIT] "
            f"active={self._active} "
            f"waiting={self._waiting}"
        )

        try:
            if timeout is None:
                await self._semaphore.acquire()
            else:
                try:
                    await asyncio.wait_for(self._semaphore.acquire(), timeout)
                except asyncio.TimeoutError:
                    async with self._lock:
                        self._timeout_count += 1
                        DB_POOL_TIMEOUTS.inc()

                    print(
                        f"[POOL_TIMEOUT] "
                        f"waited={time.monotonic() - start:.3f}s "
                        f"max={self._max_connections} "
                        f"active={self._active} "
                        f"waiting={self._waiting}"
                    )

                    raise PoolTimeout(
                        f"Timed out acquiring connection after {timeout}s"
                    )

        finally:
            async with self._lock:
                self._waiting -= 1
                DB_POOL_WAITING.set(self._waiting)

        async with self._lock:
            self._active += 1
            DB_POOL_ACTIVE.set(self._active)

        print(
            f"[POOL_ACQUIRE] "
            f"waited={time.monotonic() - start:.3f}s "
            f"active={self._active} "
            f"waiting={self._waiting}"
        )

        return _PoolConnection(self)

    async def _release(self):
        async with self._lock:
            self._active -= 1
            DB_POOL_ACTIVE.set(self._active)

        self._semaphore.release()

        print(
            f"[POOL_RELEASE] "
            f"active={self._active} "
            f"waiting={self._waiting}"
        )


class _PoolConnection:
    """
    Represents a checked-out connection.
    """

    def __init__(self, pool: ConnectionPool):
        self._pool = pool
        self._released = False

    async def release(self):
        if not self._released:
            self._released = True
            await self._pool._release()

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        await self.release()
