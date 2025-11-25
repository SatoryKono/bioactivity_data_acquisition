from __future__ import annotations

from typing import Any, Callable, Mapping, Protocol, runtime_checkable

from requests import Response


@runtime_checkable
class CacheStrategy(Protocol):
    """Контракт для кэшей HTTP-ответов."""

    @staticmethod
    def make_key(
        method: str,
        url: str,
        params: Mapping[str, Any] | None,
        headers: Mapping[str, str] | None,
    ) -> str:
        ...

    def get(self, key: str) -> bytes | None:
        ...

    def set(self, key: str, value: bytes) -> None:
        ...


@runtime_checkable
class RateLimiter(Protocol):
    """Контракт для ограничителей скорости."""

    def try_acquire(self) -> bool:
        ...

    def acquire(self, *, timeout: float | None = None) -> bool:
        ...


@runtime_checkable
class RetryStrategy(Protocol):
    """Контракт для вычисления задержек между повторными попытками."""

    @property
    def max_retries(self) -> int:
        ...

    def compute_backoff(self, attempt: int, retry_after: float | None = None) -> float:
        ...


@runtime_checkable
class CircuitBreakerStrategy(Protocol):
    """Контракт для circuit breaker."""

    def before_call(self) -> None:
        ...

    def record_success(self) -> None:
        ...

    def record_failure(self) -> None:
        ...

    def call(self, func: Callable[[], Response]) -> Response:
        ...


__all__ = [
    "CacheStrategy",
    "RateLimiter",
    "RetryStrategy",
    "CircuitBreakerStrategy",
]
