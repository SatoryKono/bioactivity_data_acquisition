from __future__ import annotations

import random
import threading
import time
from dataclasses import dataclass

from bioetl.core.logging import LogEvents, UnifiedLogger


@dataclass
class RateLimiterConfig:
    max_calls: int
    period: float
    jitter: bool


class TokenBucketRateLimiter:
    """Потокобезопасный токен-бакет с опциональным джиттером."""

    def __init__(self, config: RateLimiterConfig) -> None:
        if config.max_calls <= 0:
            msg = "max_calls must be > 0"
            raise ValueError(msg)
        if config.period <= 0:
            msg = "period must be > 0"
            raise ValueError(msg)
        self._config = config
        self._tokens = float(config.max_calls)
        self._last_refill = time.monotonic()
        self._lock = threading.Lock()
        self._logger = UnifiedLogger.get(__name__).bind(component="rate_limiter")

    def _refill(self) -> None:
        now = time.monotonic()
        elapsed = now - self._last_refill
        rate = self._config.max_calls / self._config.period
        added = elapsed * rate
        if added > 0:
            self._tokens = min(self._config.max_calls, self._tokens + added)
            self._last_refill = now

    def acquire(self) -> float:
        """Блокирует до появления токена, возвращает время ожидания (сек)."""

        waited = 0.0
        while True:
            with self._lock:
                self._refill()
                if self._tokens >= 1:
                    self._tokens -= 1
                    if waited > 0:
                        self._logger.info(LogEvents.RATE_LIMIT, waited=waited)
                    return waited
                refill_interval = self._config.period / self._config.max_calls
                sleep_for = (1 - self._tokens) / (self._config.max_calls / self._config.period)
            if self._config.jitter:
                jitter_delta = random.uniform(0, refill_interval)
                sleep_for += jitter_delta
            time.sleep(max(0.0, sleep_for))
            waited += sleep_for
