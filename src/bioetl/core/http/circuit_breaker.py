from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Callable, Protocol, runtime_checkable

import structlog
from requests import Response


class CircuitBreakerOpenError(RuntimeError):
    """Исключение при открытом circuit breaker."""


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


@dataclass
class CircuitBreakerConfig:
    failure_threshold: int
    reset_timeout_sec: float


class CircuitBreakerImpl(CircuitBreakerStrategy):
    def __init__(self, config: CircuitBreakerConfig) -> None:
        if config.failure_threshold <= 0:
            raise ValueError("failure_threshold must be positive")
        if config.reset_timeout_sec <= 0:
            raise ValueError("reset_timeout_sec must be positive")
        self._config = config
        self._state = "closed"
        self._failures = 0
        self._opened_at: float | None = None
        self._logger = structlog.get_logger(__name__).bind(component="circuit_breaker")

    @property
    def state(self) -> str:
        return self._state

    def _transition(self, new_state: str, reason: str) -> None:
        self._state = new_state
        self._logger.warning("circuit_breaker_transition", state=new_state, reason=reason)

    def before_call(self) -> None:
        if self._state != "open":
            return
        assert self._opened_at is not None
        elapsed = time.monotonic() - self._opened_at
        if elapsed >= self._config.reset_timeout_sec:
            self._state = "half-open"
        else:
            raise CircuitBreakerOpenError("Circuit breaker is open")

    def record_success(self) -> None:
        self._failures = 0
        if self._state in {"open", "half-open"}:
            self._transition("closed", "success")
            self._opened_at = None

    def record_failure(self) -> None:
        self._failures += 1
        if self._failures >= self._config.failure_threshold:
            self._opened_at = time.monotonic()
            self._transition("open", "failure_threshold")

    def call(self, func: Callable[[], Response]) -> Response:
        self.before_call()
        try:
            response = func()
        except Exception:
            self.record_failure()
            raise
        return response

    def time_until_half_open(self) -> float | None:
        if self._state != "open" or self._opened_at is None:
            return None
        elapsed = time.monotonic() - self._opened_at
        return max(0.0, self._config.reset_timeout_sec - elapsed)


__all__ = [
    "CircuitBreakerStrategy",
    "CircuitBreakerImpl",
    "CircuitBreakerConfig",
    "CircuitBreakerOpenError",
]
