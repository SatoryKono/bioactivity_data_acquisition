"""Unified rate limiting utilities.

This module provides a token-bucket :class:`RateLimiter` and convenience
helpers used across the project to throttle HTTP requests.  Network-facing
code should use :func:`get_limiter` to retrieve a shared limiter instance and
call :meth:`RateLimiter.acquire` before performing a request.  All sleeping is
routed through :func:`sleep` so tests can monkeypatch it easily.

Supports both synchronous and asynchronous usage patterns.
"""

from __future__ import annotations

import asyncio
import math
import threading
import time
from contextlib import AbstractContextManager, asynccontextmanager
from dataclasses import dataclass
from typing import Any

from cachetools import TTLCache

GLOBAL_LIMITER_NAME = "system_global"


_MIN_WAIT_SECONDS = 1e-3
_MAX_WAIT_SECONDS = 5.0


class RateLimitError(ValueError):
    """Raised when an invalid rate limit configuration is supplied."""


class RateLimiter(AbstractContextManager["RateLimiter"]):
    """Token bucket rate limiter with async support.

    Parameters
    ----------
    rps:
        Allowed requests per second.  A value ``<= 0`` disables throttling.
    burst:
        Maximum burst size.  Defaults to ``ceil(rps)``.
    """

    def __init__(self, rps: float, burst: int | None = None) -> None:
        if rps <= 0:
            raise RateLimitError("Rate must be a positive value")

        self.rps = rps
        self.burst = burst if burst is not None else max(1, int(rps))
        self._tokens = float(self.burst)
        self._updated = time.monotonic()
        self._lock = threading.Lock()

    @property
    def rate(self) -> float:
        """Get the current rate limit."""
        return self.rps

    @property
    def capacity(self) -> float:
        """Get the current burst capacity."""
        return float(self.burst)

    def acquire(self) -> None:
        """Block until a token is available.

        The wait strategy uses an adaptive back-off that doubles the minimum
        sleep duration on every retry until a reasonable upper bound is
        reached.  This approach keeps the loop responsive for highly
        contended call sites (including concurrent threads) while guaranteeing
        forward progress even when the system scheduler undersleeps.
        """
        if self.rps <= 0:
            return

        base_wait = max(1.0 / self.rps, _MIN_WAIT_SECONDS)
        max_wait = max(base_wait, _MAX_WAIT_SECONDS)
        adaptive_wait = base_wait
        wait = 0.0

        while True:
            if wait > 0:
                sleep(wait)

            with self._lock:
                now = time.monotonic()
                elapsed = now - self._updated
                self._tokens = min(float(self.burst), self._tokens + elapsed * self.rps)
                if self._tokens >= 1 or math.isclose(self._tokens, 1.0, rel_tol=0.0, abs_tol=1e-9):
                    self._tokens = max(0.0, self._tokens - 1)
                    self._updated = now
                    return

                required = max(0.0, (1 - self._tokens) / self.rps)
                wait = min(max(required, adaptive_wait), max_wait)

            adaptive_wait = min(adaptive_wait * 2, max_wait)

    async def acquire_async(self) -> None:
        """Asynchronous variant of :meth:`acquire`."""
        if self.rps <= 0:
            return

        base_wait = max(1.0 / self.rps, _MIN_WAIT_SECONDS)
        max_wait = max(base_wait, _MAX_WAIT_SECONDS)
        adaptive_wait = base_wait
        wait = 0.0

        while True:
            if wait > 0:
                await asyncio.sleep(wait)

            with self._lock:
                now = time.monotonic()
                elapsed = now - self._updated
                self._tokens = min(float(self.burst), self._tokens + elapsed * self.rps)
                if self._tokens >= 1 or math.isclose(self._tokens, 1.0, rel_tol=0.0, abs_tol=1e-9):
                    self._tokens = max(0.0, self._tokens - 1)
                    self._updated = now
                    return

                required = max(0.0, (1 - self._tokens) / self.rps)
                wait = min(max(required, adaptive_wait), max_wait)

            adaptive_wait = min(adaptive_wait * 2, max_wait)

    def __enter__(self) -> RateLimiter:
        """Context manager entry."""
        self.acquire()
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Context manager exit."""
        return None

    async def __aenter__(self) -> RateLimiter:
        """Async context manager entry."""
        await self.acquire_async()
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Async context manager exit."""
        return None


_limiters: TTLCache[str, RateLimiter] = TTLCache(maxsize=128, ttl=600)

_limiters_lock = threading.Lock()


def configure_limiter_cache(maxsize: int, ttl: int) -> None:
    """Configure the cache storing :class:`RateLimiter` instances.

    Parameters
    ----------
    maxsize:
        Maximum number of cached limiters.
    ttl:
        Time-to-live for cache entries in seconds.
    """

    global _limiters
    with _limiters_lock:
        _limiters = TTLCache(maxsize=maxsize, ttl=ttl)


def get_limiter(name: str, rps: float, burst: int | None = None) -> RateLimiter:
    """Return a shared :class:`RateLimiter` identified by ``name``.

    Parameters
    ----------
    name:
        Identifier for the limiter.  Subsequent calls with the same name return
        the same instance.
    rps:
        Allowed requests per second.  A value ``<= 0`` disables throttling.
    burst:
        Maximum burst size.  Defaults to ``ceil(rps)``.
    """
    with _limiters_lock:
        limiter: RateLimiter | None = _limiters.get(name)

        if limiter is None or limiter.rps != rps or (burst is not None and limiter.burst != burst):
            limiter = RateLimiter(rps, burst)
            _limiters[name] = limiter
        return limiter


def get_global_limiter(rps: float | None, burst: int | None = None) -> RateLimiter | None:
    """Return the shared system-wide :class:`RateLimiter` if enabled.

    Parameters
    ----------
    rps:
        Allowed requests per second for the entire pipeline.  A value ``<= 0``
        disables the limiter.
    burst:
        Maximum burst size for the global limiter.  Non-positive values are
        treated as ``None``.
    """

    if rps is None or rps <= 0:
        return None
    burst_value = burst if burst is not None and burst > 0 else None
    return get_limiter(GLOBAL_LIMITER_NAME, rps, burst_value)


@dataclass(frozen=True)
class RateLimitParams:
    """Configuration for a rate limiter."""

    rps: float
    burst: int | None = None

    def create_limiter(self) -> RateLimiter:
        """Create a RateLimiter instance from these parameters."""
        return RateLimiter(self.rps, burst=self.burst)


class RateLimiterSet:
    """Composite limiter that enforces global and per-client quotas."""

    def __init__(self, *limiters: RateLimiter | None) -> None:
        self._limiters = tuple(limiter for limiter in limiters if limiter is not None)

    def acquire(self) -> None:
        """Acquire permits from all limiters."""
        for limiter in self._limiters:
            limiter.acquire()

    async def acquire_async(self) -> None:
        """Acquire permits from all limiters asynchronously."""
        for limiter in self._limiters:
            await limiter.acquire_async()

    def __enter__(self) -> RateLimiterSet:
        """Context manager entry."""
        self.acquire()
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Context manager exit."""
        return None

    async def __aenter__(self) -> RateLimiterSet:
        """Async context manager entry."""
        await self.acquire_async()
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Async context manager exit."""
        return None


# Global limiter management (from utils/rate_limit.py)
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
        _CLIENT_LIMITERS = {name: params.create_limiter() for name, params in client_limits.items()} if client_limits else {}


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
async def limit_async(client_name: str):
    """Async context manager that acquires permits for ``client_name``."""
    limiter = get_rate_limiter(client_name)
    await limiter.acquire_async()
    yield


def sleep(delay: float) -> None:
    """Sleep for ``delay`` seconds.

    Parameters
    ----------
    delay:
        Number of seconds to pause execution.
    """
    time.sleep(delay)
