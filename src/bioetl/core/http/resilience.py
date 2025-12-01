"""Resilience factory and components for the UnifiedAPIClient."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Mapping

import requests
import structlog
from structlog.typing import FilteringBoundLogger

from bioetl.core.http.cache import (
    CacheStrategy,
    TTLCache,
    TTLCacheConfig,
)
from bioetl.core.http.circuit_breaker import (
    CircuitBreaker,
    CircuitBreakerConfig,
    CircuitBreakerStrategy,
)
from bioetl.core.http.pagination import (
    DefaultPaginationStrategy,
    PaginationStrategy,
)
from bioetl.core.http.rate_limiter import (
    RateLimiter,
    TokenBucketConfig,
    TokenBucketRateLimiter,
)
from bioetl.core.http.request_builder import RequestBuilder
from bioetl.core.http.request_executor import (
    RequestExecutorProtocol,
    ResilientRequestExecutorImpl,
)
from bioetl.core.http.retry import RetryPolicy, RetryStrategy

if TYPE_CHECKING:
    from bioetl.core.http.config import APIConfig


@dataclass
class ResilienceComponents:
    """
    Container for all strategies and components used by the UnifiedAPIClient.

    Holds the configured request executor and individual strategies (retry,
    rate limiting, caching, etc.) so they can be passed to the client.
    """

    request_builder: RequestBuilder
    executor: RequestExecutorProtocol
    pagination_strategy: PaginationStrategy
    retry_strategy: RetryStrategy
    rate_limiter: RateLimiter
    cache: CacheStrategy | None
    circuit_breaker: CircuitBreakerStrategy


class DefaultRetryFactory:
    """Фабрика стратегий повторов по умолчанию."""

    def __init__(self, config: APIConfig) -> None:
        self._config = config

    def create(self, *, strategy: RetryStrategy | None = None) -> RetryStrategy:
        if strategy is not None:
            return strategy
        return RetryPolicy(
            max_retries=self._config.max_retries,
            backoff_factor=self._config.backoff_factor,
            max_backoff_sec=self._config.max_backoff_sec,
        )


class DefaultRateLimiterFactory:
    """Фабрика лимитеров скорости по умолчанию."""

    def __init__(self, config: APIConfig) -> None:
        self._config = config

    def create(self, *, rate_limiter: RateLimiter | None = None) -> RateLimiter:
        if rate_limiter is not None:
            return rate_limiter
        return TokenBucketRateLimiter(
            TokenBucketConfig(
                max_tokens=self._config.rate_limit_calls,
                refill_period_sec=float(self._config.rate_limit_period_sec),
            )
        )


class DefaultCacheFactory:
    """Фабрика кэша по умолчанию."""

    def __init__(self, config: APIConfig) -> None:
        self._config = config

    def create(self, *, cache: CacheStrategy | None = None) -> CacheStrategy | None:
        if cache is not None:
            return cache
        if not self._config.cache_enabled:
            return None
        return TTLCache(TTLCacheConfig(ttl_seconds=self._config.cache_ttl_sec))


class DefaultCircuitBreakerFactory:
    """Фабрика circuit breaker по умолчанию."""

    def __init__(self, config: APIConfig) -> None:
        self._config = config

    def create(
        self, *, circuit_breaker: CircuitBreakerStrategy | None = None
    ) -> CircuitBreakerStrategy:
        if circuit_breaker is not None:
            return circuit_breaker
        return CircuitBreaker(
            CircuitBreakerConfig(
                failure_threshold=self._config.circuit_breaker_fail_max,
                reset_timeout_sec=self._config.circuit_breaker_reset_sec,
            )
        )


class DefaultResilienceFactory:
    """Фабрика сборки устойчивых HTTP-компонентов по умолчанию."""

    def __init__(
        self,
        config: APIConfig,
        *,
        logger: FilteringBoundLogger | None = None,
        retry_factory: DefaultRetryFactory | None = None,
        rate_limiter_factory: DefaultRateLimiterFactory | None = None,
        cache_factory: DefaultCacheFactory | None = None,
        circuit_breaker_factory: DefaultCircuitBreakerFactory | None = None,
    ) -> None:
        self._config = config
        self._logger = logger or structlog.get_logger(__name__).bind(
            api_base=config.base_url
        )
        self._retry_factory = retry_factory or DefaultRetryFactory(config)
        self._rate_limiter_factory = rate_limiter_factory or DefaultRateLimiterFactory(
            config
        )
        self._cache_factory = cache_factory or DefaultCacheFactory(config)
        self._circuit_breaker_factory = (
            circuit_breaker_factory or DefaultCircuitBreakerFactory(config)
        )

    def create(
        self,
        *,
        request_builder: RequestBuilder | None = None,
        session: requests.Session | None = None,
        retry_strategy: RetryStrategy | None = None,
        rate_limiter: RateLimiter | None = None,
        cache: CacheStrategy | None = None,
        circuit_breaker: CircuitBreakerStrategy | None = None,
        pagination_strategy: PaginationStrategy | None = None,
        verify_ssl: bool = True,
        default_headers: Mapping[str, str] | None = None,
    ) -> ResilienceComponents:
        builder = request_builder or RequestBuilder(
            self._config,
            session=session,
            verify_ssl=verify_ssl,
            default_headers=default_headers,
        )
        prepared_retry = self._retry_factory.create(strategy=retry_strategy)
        prepared_rate_limiter = self._rate_limiter_factory.create(
            rate_limiter=rate_limiter
        )
        prepared_cache = self._cache_factory.create(cache=cache)
        prepared_breaker = self._circuit_breaker_factory.create(
            circuit_breaker=circuit_breaker
        )

        executor = ResilientRequestExecutorImpl(
            session=builder.session,
            logger=self._logger,
            retry_strategy=prepared_retry,
            rate_limiter=prepared_rate_limiter,
            cache=prepared_cache,
            circuit_breaker=prepared_breaker,
            timeout_sec=self._config.timeout_sec,
        )
        pagination = pagination_strategy or DefaultPaginationStrategy()

        return ResilienceComponents(
            request_builder=builder,
            executor=executor,
            pagination_strategy=pagination,
            retry_strategy=prepared_retry,
            rate_limiter=prepared_rate_limiter,
            cache=prepared_cache,
            circuit_breaker=prepared_breaker,
        )


class ResilientRequestExecutorFactory(DefaultResilienceFactory):
    """Обратная совместимость для старых импортов."""


__all__ = [
    "DefaultCacheFactory",
    "DefaultCircuitBreakerFactory",
    "DefaultRateLimiterFactory",
    "DefaultResilienceFactory",
    "DefaultRetryFactory",
    "ResilientRequestExecutorFactory",
    "ResilienceComponents",
]
