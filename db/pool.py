import asyncio
import time
from typing import Optional


class PoolTimeout(Exception):
    """Raised when a connection cannot be acquired within the timeout."""
    pass


class ConnectionPool:
    """
    Application-side connection pool.

    This models the finite willingness of the application to issue concurrent
    database requests. It is intentionally simple and explicit.

    Key behaviors:
    - Fixed maximum concurrency
    - Blocking acquisition
    - Optional timeout while waiting
    - Explicit visibility into waiting vs active work
    """

    def __init__(self, max_connections: int):
        self._max_connections = max_connections
        self._semaphore = asyncio.Semaphore(max_connections)

        # Metrics / state (intentionally simple, no framework bindings)
        self._active = 0
        self._waiting = 0

        # Timing metrics (seconds)
        self._total_wait_time = 0.0
        self._acquire_count = 0
        self._timeout_count = 0

        # Protect counters
        self._lock = asyncio.Lock()

    @property
    def max_connections(self) -> int:
        return self._max_connections

    @property
    def active(self) -> int:
        """Number of connections currently in use."""
        return self._active

    @property
    def waiting(self) -> int:
        """Number of requests currently waiting to acquire a connection."""
        return self._waiting

    @property
    def timeout_count(self) -> int:
        return self._timeout_count

    @property
    def average_wait_time(self) -> float:
        if self._acquire_count == 0:
            return 0.0
        return self._total_wait_time / self._acquire_count

    async def acquire(self, timeout: Optional[float] = None):
        """
        Acquire a connection from the pool.

        If all connections are in use:
        - the caller waits
        - waiting is explicitly tracked
        - optional timeout may raise PoolTimeout
        """
        start = time.monotonic()

        async with self._lock:
            self._waiting += 1

        try:
            if timeout is None:
                await self._semaphore.acquire()
            else:
                try:
                    await asyncio.wait_for(self._semaphore.acquire(), timeout)
                except asyncio.TimeoutError:
                    async with self._lock:
                        self._timeout_count += 1
                    raise PoolTimeout(f"Timed out acquiring connection after {timeout}s")
        finally:
            wait_time = time.monotonic() - start
            async with self._lock:
                self._waiting -= 1
                self._total_wait_time += wait_time
                self._acquire_count += 1

        async with self._lock:
            self._active += 1

        return _PoolConnection(self)

    async def _release(self):
        async with self._lock:
            self._active -= 1
        self._semaphore.release()


class _PoolConnection:
    """
    Represents a checked-out connection.

    Intended to be used as an async context manager.
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
