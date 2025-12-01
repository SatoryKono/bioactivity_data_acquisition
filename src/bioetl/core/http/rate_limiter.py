from __future__ import annotations

import threading
import time
from dataclasses import dataclass
from typing import Optional, Protocol, runtime_checkable

import structlog


@runtime_checkable
class RateLimiter(Protocol):
    """Контракт для ограничителей скорости."""

    def try_acquire(self) -> bool:
        ...

    def acquire(self, *, timeout: Optional[float] = None) -> bool:
        ...


@dataclass(frozen=True)
class TokenBucketConfig:
    max_tokens: int
    refill_period_sec: float


class TokenBucketRateLimiterImpl(RateLimiter):
    """Простой и потокобезопасный token-bucket лимитер.

    Поддерживает блокирующее ``acquire`` с опциональным таймаутом и
    неблокирующий ``try_acquire``. Все операции защищены одним lock, что
    позволяет безопасно использовать лимитер в многопоточной среде.
    """

    def __init__(self, config: TokenBucketConfig) -> None:
        if config.max_tokens <= 0:
            msg = "max_tokens must be positive"
            raise ValueError(msg)
        if config.refill_period_sec <= 0:
            msg = "refill_period_sec must be positive"
            raise ValueError(msg)
        self._config = config
        self._tokens = float(config.max_tokens)
        self._last_refill = time.monotonic()
        self._lock = threading.Lock()
        self._logger = structlog.get_logger(__name__).bind(component="rate_limiter")

    def _refill(self) -> None:
        now = time.monotonic()
        elapsed = now - self._last_refill
        if elapsed <= 0:
            return
        rate = self._config.max_tokens / self._config.refill_period_sec
        self._tokens = min(self._config.max_tokens, self._tokens + elapsed * rate)
        self._last_refill = now

    def try_acquire(self) -> bool:
        """Неблокирующая попытка забрать токен."""

        with self._lock:
            self._refill()
            if self._tokens >= 1:
                self._tokens -= 1
                return True
            return False

    def acquire(self, *, timeout: Optional[float] = None) -> bool:
        """Блокирует до появления токена или истечения ``timeout``.

        Возвращает ``True`` если токен получен, иначе ``False``. Таймаут 0
        делает вызов неблокирующим.
        """

        deadline = None if timeout is None else time.monotonic() + timeout
        waited_total = 0.0
        while True:
            with self._lock:
                self._refill()
                if self._tokens >= 1:
                    self._tokens -= 1
                    if waited_total:
                        self._logger.info("rate_limit_wait", waited_sec=waited_total)
                    return True
                now = time.monotonic()
                if deadline is not None and now >= deadline:
                    return False
                # время до появления следующего токена
                tokens_needed = 1 - self._tokens
                rate = self._config.max_tokens / self._config.refill_period_sec
                sleep_for = max(0.0, tokens_needed / rate)
            if deadline is not None:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    return False
                sleep_for = min(sleep_for, remaining)
            time.sleep(sleep_for)
            waited_total += sleep_for


__all__ = ["RateLimiter", "TokenBucketConfig", "TokenBucketRateLimiterImpl"]
