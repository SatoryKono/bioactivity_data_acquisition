"""Rate limiting primitives used by HTTP clients."""

from __future__ import annotations

import threading
import time
from collections.abc import Iterable
from dataclasses import dataclass, field


@dataclass
class RateLimiter:
    """A cooperative token bucket limiter enforcing requests-per-second budgets."""

    rate_per_second: float | None = None
    _lock: threading.Lock = field(default_factory=threading.Lock, init=False, repr=False)
    _last_invocation: float = field(default=0.0, init=False, repr=False)

    def throttle(self) -> None:
        """Sleep just enough to respect the configured rate limit."""

        if not self.rate_per_second or self.rate_per_second <= 0:
            return

        minimum_interval = 1.0 / float(self.rate_per_second)
        with self._lock:
            now = time.monotonic()
            elapsed = now - self._last_invocation
            if self._last_invocation and elapsed < minimum_interval:
                time.sleep(minimum_interval - elapsed)
                now = time.monotonic()
            self._last_invocation = now


class CompositeRateLimiter:
    """Combine multiple rate limiters into a single interface."""

    def __init__(self, limiters: Iterable[RateLimiter | None]):
        self._limiters = [limiter for limiter in limiters if limiter is not None]

    def throttle(self) -> None:
        for limiter in self._limiters:
            limiter.throttle()
