"""Token-bucket rate limiter with optional jitter."""
from __future__ import annotations

import random
import threading
import time
from dataclasses import dataclass
from typing import Callable


@dataclass
class RateLimiterConfig:
    max_calls: int
    period: float
    jitter: bool = False


class TokenBucketRateLimiter:
    """Thread-safe token bucket limiter."""

    def __init__(
        self,
        config: RateLimiterConfig,
        *,
        monotonic: Callable[[], float] | None = None,
        sleeper: Callable[[float], None] | None = None,
    ) -> None:
        if config.max_calls <= 0:
            msg = "max_calls must be positive"
            raise ValueError(msg)
        if config.period <= 0:
            msg = "period must be positive"
            raise ValueError(msg)
        self.config = config
        self._monotonic = monotonic or time.monotonic
        self._sleep = sleeper or time.sleep
        self._lock = threading.Lock()
        self._tokens = float(config.max_calls)
        self._last_refill = self._monotonic()
        self._refill_rate = config.max_calls / config.period

    def _refill(self) -> None:
        now = self._monotonic()
        elapsed = now - self._last_refill
        if elapsed <= 0:
            return
        self._tokens = min(
            float(self.config.max_calls), self._tokens + elapsed * self._refill_rate
        )
        self._last_refill = now

    def acquire(self) -> float:
        """Acquire a token, sleeping if necessary. Returns waited seconds."""
        waited = 0.0
        while True:
            with self._lock:
                self._refill()
                if self._tokens >= 1.0:
                    self._tokens -= 1.0
                    return waited
                deficit = 1.0 - self._tokens
                wait_for = deficit / self._refill_rate
            if self.config.jitter:
                wait_for *= random.uniform(0.9, 1.1)
            if wait_for > 0:
                self._sleep(wait_for)
                waited += wait_for
            else:  # pragma: no cover
                self._sleep(0)
