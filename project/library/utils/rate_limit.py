"""Simple rate limiting helpers for HTTP clients."""

from __future__ import annotations

import time
from dataclasses import dataclass, field


@dataclass
class RateLimiter:
    """Token bucket style limiter based on a fixed per-minute allowance."""

    rate_per_minute: int | None = None
    _last_invocation: float = field(default=0.0, init=False, repr=False)

    def throttle(self) -> None:
        """Sleep just enough to respect the configured rate limit."""

        if not self.rate_per_minute:
            return

        interval = 60.0 / float(self.rate_per_minute)
        now = time.monotonic()
        elapsed = now - self._last_invocation
        if self._last_invocation and elapsed < interval:
            time.sleep(interval - elapsed)
        self._last_invocation = time.monotonic()
