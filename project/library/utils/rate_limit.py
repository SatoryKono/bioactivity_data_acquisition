"""Rate limiting utilities for controlling HTTP throughput."""
from __future__ import annotations

import threading
import time
from dataclasses import dataclass


@dataclass
class RateLimitConfig:
    """Configuration for rate limiting in requests per second."""

    rps: float


class RateLimiter:
    """A simple thread-safe rate limiter enforcing a minimum interval between calls."""

    def __init__(self, rate_limit: RateLimitConfig | None = None) -> None:
        self._rate_limit = rate_limit
        self._lock = threading.Lock()
        self._next_allowed_time = 0.0

    @property
    def enabled(self) -> bool:
        return bool(self._rate_limit and self._rate_limit.rps > 0)

    def wait(self) -> None:
        if not self.enabled:
            return
        interval = 1.0 / self._rate_limit.rps
        with self._lock:
            now = time.monotonic()
            if now < self._next_allowed_time:
                sleep_for = self._next_allowed_time - now
            else:
                sleep_for = 0.0
            self._next_allowed_time = max(self._next_allowed_time, now) + interval
        if sleep_for > 0:
            time.sleep(sleep_for)


class CompositeRateLimiter:
    """Combines a global and per-client limiter."""

    def __init__(self, global_limiter: RateLimiter | None, client_limiter: RateLimiter | None) -> None:
        self._global_limiter = global_limiter
        self._client_limiter = client_limiter

    def wait(self) -> None:
        if self._global_limiter:
            self._global_limiter.wait()
        if self._client_limiter:
            self._client_limiter.wait()


def build_rate_limiter(rps: float | None) -> RateLimiter | None:
    """Create a :class:`RateLimiter` from a requested RPS value."""

    if rps is None:
        return None
    return RateLimiter(RateLimitConfig(rps=rps))

