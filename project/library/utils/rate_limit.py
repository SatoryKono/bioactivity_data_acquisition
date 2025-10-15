"""Simple rate limiter utilities."""
from __future__ import annotations

import threading
import time
from collections.abc import Generator
from contextlib import contextmanager
from dataclasses import dataclass


@dataclass
class RateLimiter:
    """Token bucket rate limiter with thread safety."""

    rate_per_sec: float
    capacity: float | None = None

    def __post_init__(self) -> None:
        if self.rate_per_sec <= 0:
            raise ValueError("rate_per_sec must be positive")
        self.capacity = self.capacity or self.rate_per_sec
        self._tokens = float(self.capacity)
        self._lock = threading.Lock()
        self._updated_at = time.monotonic()

    def _add_new_tokens(self) -> None:
        now = time.monotonic()
        elapsed = now - self._updated_at
        self._updated_at = now
        self._tokens = min(self.capacity or self.rate_per_sec, self._tokens + elapsed * self.rate_per_sec)

    def acquire(self) -> None:
        while True:
            with self._lock:
                self._add_new_tokens()
                if self._tokens >= 1:
                    self._tokens -= 1
                    return
                sleep_time = max(0.0, (1 - self._tokens) / self.rate_per_sec)
            time.sleep(sleep_time)

    @contextmanager
    def limit(self) -> Generator[None, None, None]:
        self.acquire()
        yield
