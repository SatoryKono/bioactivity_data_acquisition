from __future__ import annotations

import random
from dataclasses import dataclass
from typing import Protocol, runtime_checkable


@runtime_checkable
class RetryStrategy(Protocol):
    """Контракт для вычисления задержек между повторными попытками."""

    @property
    def max_retries(self) -> int:
        ...

    def compute_backoff(self, attempt: int, retry_after: float | None = None) -> float:
        ...


@dataclass
class ExponentialBackoffRetryImpl:
    max_retries: int
    backoff_factor: float
    max_backoff_sec: float
    jitter: bool = True

    def compute_backoff(self, attempt: int, retry_after: float | None = None) -> float:
        base = self.backoff_factor * (2 ** max(0, attempt - 1))
        if self.jitter:
            base *= random.uniform(0.8, 1.2)
        backoff = min(base, self.max_backoff_sec)
        if retry_after is not None:
            backoff = max(backoff, retry_after)
        return max(0.0, backoff)


__all__ = ["RetryStrategy", "ExponentialBackoffRetryImpl"]
