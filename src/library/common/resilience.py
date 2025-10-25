"""Unified resilience strategies for API clients.

This module provides a comprehensive set of resilience patterns including
circuit breaker, fallback strategies, and graceful degradation for handling
API failures and rate limiting.
"""

from __future__ import annotations

import asyncio
import logging
import random
import threading
import time
from abc import ABC, abstractmethod
from collections.abc import Callable
from dataclasses import dataclass
from enum import Enum
from typing import Any, TypeVar

from library.clients.exceptions import ApiClientError

logger = logging.getLogger(__name__)

T = TypeVar("T")


class CircuitState(Enum):
    """Circuit breaker states."""

    CLOSED = "closed"  # Normal operation
    OPEN = "open"  # Circuit is open, failing fast
    HALF_OPEN = "half_open"  # Testing if service is back


@dataclass
class CircuitBreakerConfig:
    """Configuration for circuit breaker."""

    failure_threshold: int = 5  # Number of failures before opening circuit
    recovery_timeout: float = 60.0  # Time to wait before trying half-open
    success_threshold: int = 3  # Number of successes needed to close circuit
    timeout: float = 30.0  # Request timeout
    expected_exception: type[Exception] = ApiClientError  # Exception type to track


class CircuitBreaker:
    """Circuit breaker implementation for API reliability."""

    def __init__(self, config: CircuitBreakerConfig):
        self.config = config
        self.logger = logging.getLogger(f"{__name__}.CircuitBreaker")

        # State management
        self._state = CircuitState.CLOSED
        self._failure_count = 0
        self._success_count = 0
        self._last_failure_time: float | None = None
        self._lock = threading.Lock()

    @property
    def state(self) -> CircuitState:
        """Get current circuit state."""
        return self._state

    def call(self, func: Callable[..., T], *args, **kwargs) -> T:
        """Execute function with circuit breaker protection."""
        with self._lock:
            if self._state == CircuitState.OPEN:
                if self._should_attempt_reset():
                    self._state = CircuitState.HALF_OPEN
                    self._success_count = 0
                    self.logger.info("Circuit breaker transitioning to HALF_OPEN")
                else:
                    raise ApiClientError(f"Circuit breaker is OPEN. Service unavailable. Last failure: {self._last_failure_time}")

        try:
            result = func(*args, **kwargs)
            self._on_success()
            return result
        except self.config.expected_exception:
            self._on_failure()
            raise
        except Exception as e:
            # Handle DNS errors as critical
            error_str = str(e).lower()
            if "name resolution" in error_str or "getaddrinfo failed" in error_str:
                self.logger.error(f"DNS resolution failed: {e}")
                self._on_failure()
                raise ApiClientError(f"DNS resolution failed: {e}") from e
            # For other exceptions use standard handling
            if isinstance(e, self.config.expected_exception):
                self._on_failure()
                raise
            else:
                # Unexpected exceptions don't affect circuit breaker
                raise

    def _should_attempt_reset(self) -> bool:
        """Check if we should attempt to reset the circuit."""
        if self._last_failure_time is None:
            return True

        return time.time() - self._last_failure_time >= self.config.recovery_timeout

    def _on_success(self):
        """Handle successful call."""
        with self._lock:
            if self._state == CircuitState.HALF_OPEN:
                self._success_count += 1
                if self._success_count >= self.config.success_threshold:
                    self._state = CircuitState.CLOSED
                    self._failure_count = 0
                    self.logger.info("Circuit breaker transitioning to CLOSED")
            elif self._state == CircuitState.CLOSED:
                # Reset failure count on success
                self._failure_count = 0

    def _on_failure(self):
        """Handle failed call."""
        with self._lock:
            self._failure_count += 1
            self._last_failure_time = time.time()

            if self._failure_count >= self.config.failure_threshold:
                self._state = CircuitState.OPEN
                self.logger.warning(f"Circuit breaker transitioning to OPEN after {self._failure_count} failures")


@dataclass
class FallbackConfig:
    """Configuration for fallback strategies."""

    max_retries: int = 3
    base_delay: float = 1.0
    max_delay: float = 60.0
    backoff_multiplier: float = 2.0
    jitter: bool = True


class FallbackStrategy(ABC):
    """Abstract base class for fallback strategies."""

    def __init__(self, config: FallbackConfig):
        self.config = config
        self.logger = logging.getLogger(f"{__name__}.FallbackStrategy")

    @abstractmethod
    def should_retry(self, error: ApiClientError, attempt: int) -> bool:
        """Determine if we should retry after this error."""
        pass

    @abstractmethod
    def get_delay(self, error: ApiClientError, attempt: int) -> float:
        """Calculate delay before next retry."""
        pass

    @abstractmethod
    def get_fallback_data(self, error: ApiClientError) -> dict[str, Any]:
        """Get fallback data when all retries are exhausted."""
        pass


class RateLimitFallbackStrategy(FallbackStrategy):
    """Fallback strategy specifically for rate limiting errors (429)."""

    def should_retry(self, error: ApiClientError, attempt: int) -> bool:
        """Retry for rate limiting errors up to max_retries."""
        if getattr(error, "status_code", None) == 429:
            return attempt < self.config.max_retries
        return False

    def get_delay(self, error: ApiClientError, attempt: int) -> float:
        """Calculate exponential backoff delay with jitter."""
        if getattr(error, "status_code", None) == 429:
            # Exponential backoff with jitter
            delay = min(self.config.base_delay * (self.config.backoff_multiplier**attempt), self.config.max_delay)

            if self.config.jitter:
                # Add random jitter (±25%) for load distribution
                jitter_factor = random.uniform(0.75, 1.25)  # noqa: S311
                delay *= jitter_factor

            return delay

        return self.config.base_delay

    def get_fallback_data(self, error: ApiClientError) -> dict[str, Any]:
        """Return empty record with error information."""
        return {"source": "fallback", "error": f"Rate limited after {self.config.max_retries} attempts: {str(error)}", "rate_limited": True, "retry_count": self.config.max_retries}


class SemanticScholarFallbackStrategy(FallbackStrategy):
    """Specialized fallback strategy for Semantic Scholar API with very strict rate limits."""

    def should_retry(self, error: ApiClientError, attempt: int) -> bool:
        """Very conservative retry logic for Semantic Scholar."""
        if getattr(error, "status_code", None) == 429:
            # For rate limiting, don't retry - let the main rate limiter handle it
            return False
        elif getattr(error, "status_code", None) in [500, 502, 503, 504]:
            # For server errors, retry once
            return attempt < 1
        else:
            # For other errors, don't retry
            return False

    def get_delay(self, error: ApiClientError, attempt: int) -> float:
        """Optimized delays for Semantic Scholar rate limiting."""
        if getattr(error, "status_code", None) == 429:
            # Very long delay for rate limiting
            return 300.0  # 5 minutes
        elif getattr(error, "status_code", None) in [500, 502, 503, 504]:
            # Moderate delay for server errors
            return 30.0
        else:
            return self.config.base_delay

    def get_fallback_data(self, error: ApiClientError) -> dict[str, Any]:
        """Return empty record with Semantic Scholar specific error information."""
        return {
            "source": "semantic_scholar_fallback",
            "error": f"Semantic Scholar API failed: {str(error)}",
            "fallback_reason": "api_unavailable",
            "retry_count": self.config.max_retries,
        }


class GenericFallbackStrategy(FallbackStrategy):
    """Generic fallback strategy for most APIs."""

    def should_retry(self, error: ApiClientError, attempt: int) -> bool:
        """Retry for transient errors."""
        if getattr(error, "status_code", None) in [429, 500, 502, 503, 504]:
            return attempt < self.config.max_retries
        return False

    def get_delay(self, error: ApiClientError, attempt: int) -> float:
        """Calculate exponential backoff delay."""
        delay = min(self.config.base_delay * (self.config.backoff_multiplier**attempt), self.config.max_delay)

        if self.config.jitter:
            jitter_factor = random.uniform(0.75, 1.25)  # noqa: S311
            delay *= jitter_factor

        return delay

    def get_fallback_data(self, error: ApiClientError) -> dict[str, Any]:
        """Return empty record with error information."""
        return {"source": "fallback", "error": f"API failed after {self.config.max_retries} attempts: {str(error)}", "retry_count": self.config.max_retries}


class FallbackManager:
    """Manages fallback strategies for API clients."""

    def __init__(self, strategy: FallbackStrategy):
        self.strategy = strategy
        self.logger = logging.getLogger(f"{__name__}.FallbackManager")

    def execute_with_fallback(self, func: Callable[..., T], *args, **kwargs) -> T | dict[str, Any]:
        """Execute function with fallback strategy."""
        last_error = None

        for attempt in range(self.strategy.config.max_retries + 1):
            try:
                return func(*args, **kwargs)
            except ApiClientError as e:
                last_error = e

                if not self.strategy.should_retry(e, attempt):
                    self.logger.warning(f"Not retrying after error: {e} (attempt {attempt + 1})")
                    break

                delay = self.strategy.get_delay(e, attempt)
                self.logger.warning(f"Retrying after error: {e} (attempt {attempt + 1}, delay {delay:.2f}s)")

                if delay > 0:
                    time.sleep(delay)

        # All retries exhausted, return fallback data
        self.logger.error(f"All retries exhausted, using fallback data: {last_error}")
        if last_error is not None:
            return self.strategy.get_fallback_data(last_error)
        else:
            return {"error": "Unknown error occurred", "source": "fallback"}

    async def execute_with_fallback_async(self, func: Callable[..., Any], *args, **kwargs) -> Any:
        """Execute async function with fallback strategy."""
        last_error = None

        for attempt in range(self.strategy.config.max_retries + 1):
            try:
                if asyncio.iscoroutinefunction(func):
                    return await func(*args, **kwargs)
                else:
                    return func(*args, **kwargs)
            except ApiClientError as e:
                last_error = e

                if not self.strategy.should_retry(e, attempt):
                    self.logger.warning(f"Not retrying after error: {e} (attempt {attempt + 1})")
                    break

                delay = self.strategy.get_delay(e, attempt)
                self.logger.warning(f"Retrying after error: {e} (attempt {attempt + 1}, delay {delay:.2f}s)")

                if delay > 0:
                    await asyncio.sleep(delay)

        # All retries exhausted, return fallback data
        self.logger.error(f"All retries exhausted, using fallback data: {last_error}")
        if last_error is not None:
            return self.strategy.get_fallback_data(last_error)
        else:
            return {"error": "Unknown error occurred", "source": "fallback"}


class GracefulDegradationManager:
    """Manages graceful degradation for API failures."""

    def __init__(self, fallback_data: dict[str, Any] | None = None):
        self.fallback_data = fallback_data or {}
        self.logger = logging.getLogger(f"{__name__}.GracefulDegradationManager")

    def should_degrade(self, service_name: str, error: ApiClientError) -> bool:
        """Determine if we should degrade gracefully."""
        # Degrade for critical errors
        if getattr(error, "status_code", None) in [500, 502, 503, 504]:
            return True

        # Degrade for rate limiting if we have fallback data
        if getattr(error, "status_code", None) == 429 and self.fallback_data:
            return True

        return False

    def get_fallback_data(self, service_name: str, request_params: dict[str, Any], error: ApiClientError) -> dict[str, Any]:
        """Get fallback data for graceful degradation."""
        fallback = dict(self.fallback_data)
        fallback.update({"source": f"{service_name}_degraded", "error": str(error), "degraded": True, "request_params": request_params})
        return fallback


# Factory functions for common configurations
def create_rate_limit_fallback(max_retries: int = 3) -> FallbackManager:
    """Create a fallback manager for rate limiting scenarios."""
    config = FallbackConfig(max_retries=max_retries, base_delay=5.0, max_delay=60.0, backoff_multiplier=2.0, jitter=True)
    strategy = RateLimitFallbackStrategy(config)
    return FallbackManager(strategy)


def create_semantic_scholar_fallback() -> FallbackManager:
    """Create a fallback manager specifically for Semantic Scholar API."""
    config = FallbackConfig(max_retries=2, base_delay=15.0, max_delay=300.0, backoff_multiplier=2.0, jitter=True)
    strategy = SemanticScholarFallbackStrategy(config)
    return FallbackManager(strategy)


def create_generic_fallback(max_retries: int = 3) -> FallbackManager:
    """Create a generic fallback manager for most APIs."""
    config = FallbackConfig(max_retries=max_retries, base_delay=1.0, max_delay=30.0, backoff_multiplier=2.0, jitter=True)
    strategy = GenericFallbackStrategy(config)
    return FallbackManager(strategy)


def create_circuit_breaker(failure_threshold: int = 5, recovery_timeout: float = 60.0) -> CircuitBreaker:
    """Create a circuit breaker with default configuration."""
    config = CircuitBreakerConfig(failure_threshold=failure_threshold, recovery_timeout=recovery_timeout, success_threshold=3, timeout=30.0)
    return CircuitBreaker(config)
