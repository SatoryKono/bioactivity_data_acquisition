"""Rate limiting primitives used by HTTP clients.

This module implements a thread-safe token bucket rate limiter that can be used
in synchronous and asynchronous contexts. A global limiter applies to all
clients, while per-client limiters can be configured individually. The
``RateLimiterSet`` helper exposes a single interface for acquiring both global
and client specific permits.
"""

from __future__ import annotations

import asyncio
import threading
import time
from collections.abc import Iterator
from contextlib import AbstractContextManager, asynccontextmanager
from dataclasses import dataclass


class RateLimitError(ValueError):
    """Raised when an invalid rate limit configuration is supplied."""


class RateLimiter(AbstractContextManager["RateLimiter"]):
    """Token bucket rate limiter.

    Parameters
    ----------
    rate:
        Target requests-per-second.
    burst:
        Optional bucket size. Defaults to ``max(1, int(rate))`` which means the
        limiter allows a small burst equal to approximately one second worth of
        throughput.
    """

    def __init__(self, rate: float, *, burst: int | None = None) -> None:
        if rate <= 0:
            raise RateLimitError("Rate must be a positive value")

        self._rate = float(rate)
        self._capacity = float(burst if burst is not None else max(1, int(rate)))
        self._tokens = self._capacity
        self._lock = threading.Lock()
        self._last_refill = time.monotonic()

    @property
    def rate(self) -> float:
        return self._rate

    @property
    def capacity(self) -> float:
        return self._capacity

    def _refill(self) -> None:
        now = time.monotonic()
        elapsed = now - self._last_refill
        if elapsed > 0:
            refill = elapsed * self._rate
            self._tokens = min(self._capacity, self._tokens + refill)
            self._last_refill = now

    def _reserve(self) -> float:
        while True:
            with self._lock:
                self._refill()
                if self._tokens >= 1:
                    self._tokens -= 1
                    return 0.0

                missing = 1 - self._tokens
                wait = missing / self._rate

            if wait > 0:
                time.sleep(wait)
            else:
                # Defensive guard against floating point inaccuracies.
                time.sleep(0)

    def acquire(self) -> None:
        """Acquire a single permit, blocking until it becomes available."""

        self._reserve()

    async def acquire_async(self) -> None:
        """Asynchronous variant of :meth:`acquire`."""

        while True:
            with self._lock:
                self._refill()
                if self._tokens >= 1:
                    self._tokens -= 1
                    return

                missing = 1 - self._tokens
                wait = missing / self._rate

            if wait > 0:
                await asyncio.sleep(wait)
            else:
                await asyncio.sleep(0)

    # ``AbstractContextManager`` requires ``__exit__`` but provides default.
    def __enter__(self) -> RateLimiter:  # pragma: no cover - trivial
        self.acquire()
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:  # pragma: no cover - trivial
        return False

    async def __aenter__(self) -> RateLimiter:
        await self.acquire_async()
        return self

    async def __aexit__(self, *exc_info) -> None:  # pragma: no cover - trivial
        return None


@dataclass(frozen=True)
class RateLimitParams:
    """Configuration for a rate limiter."""

    rps: float
    burst: int | None = None

    def create_limiter(self) -> RateLimiter:
        return RateLimiter(self.rps, burst=self.burst)


class RateLimiterSet:
    """Composite limiter that enforces global and per-client quotas."""

    def __init__(self, *limiters: RateLimiter | None) -> None:
        self._limiters = tuple(limiter for limiter in limiters if limiter is not None)

    def acquire(self) -> None:
        for limiter in self._limiters:
            limiter.acquire()

    async def acquire_async(self) -> None:
        for limiter in self._limiters:
            await limiter.acquire_async()

    def __enter__(self) -> RateLimiterSet:  # pragma: no cover - trivial
        self.acquire()
        return self

    async def __aenter__(self) -> RateLimiterSet:  # pragma: no cover - trivial
        await self.acquire_async()
        return self

    async def __aexit__(self, *exc_info) -> None:  # pragma: no cover - trivial
        return None


_GLOBAL_LIMITER: RateLimiter | None = None
_CLIENT_LIMITERS: dict[str, RateLimiter] = {}
_CONFIG_LOCK = threading.Lock()


def configure_rate_limits(
    *,
    global_limit: RateLimitParams | None = None,
    client_limits: dict[str, RateLimitParams] | None = None,
) -> None:
    """Configure process-wide rate limiters.

    Parameters
    ----------
    global_limit:
        Global limit that is shared by all clients.
    client_limits:
        Mapping of client name to limiter configuration.
    """

    global _GLOBAL_LIMITER, _CLIENT_LIMITERS

    with _CONFIG_LOCK:
        _GLOBAL_LIMITER = global_limit.create_limiter() if global_limit else None
        _CLIENT_LIMITERS = (
            {name: params.create_limiter() for name, params in client_limits.items()}
            if client_limits
            else {}
        )


def get_rate_limiter(client_name: str, default: RateLimitParams | None = None) -> RateLimiterSet:
    """Return the composite limiter for the provided client name."""

    with _CONFIG_LOCK:
        client_limiter = _CLIENT_LIMITERS.get(client_name)
        if client_limiter is None and default is not None:
            client_limiter = default.create_limiter()
            _CLIENT_LIMITERS[client_name] = client_limiter

        return RateLimiterSet(_GLOBAL_LIMITER, client_limiter)


def set_client_limit(client_name: str, params: RateLimitParams) -> None:
    """Update (or create) the limiter for a given client."""

    with _CONFIG_LOCK:
        _CLIENT_LIMITERS[client_name] = params.create_limiter()


def reset_rate_limits() -> None:
    """Clear all configured limiters. Primarily intended for tests."""

    global _GLOBAL_LIMITER, _CLIENT_LIMITERS
    with _CONFIG_LOCK:
        _GLOBAL_LIMITER = None
        _CLIENT_LIMITERS = {}


@asynccontextmanager
async def limit_async(client_name: str) -> Iterator[None]:
    """Async context manager that acquires permits for ``client_name``."""

    limiter = get_rate_limiter(client_name)
    await limiter.acquire_async()
    yield
