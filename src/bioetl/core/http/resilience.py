from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Mapping

import structlog
import requests
from structlog.typing import FilteringBoundLogger

if TYPE_CHECKING:
    from bioetl.core.http.config import APIConfig
from bioetl.core.http.request_builder import RequestBuilder
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
from bioetl.core.http.request_executor import _ResilientRequestExecutor
from bioetl.core.http.retry import RetryPolicy, RetryStrategy


@dataclass
class ResilienceComponents:
    request_builder: RequestBuilder
    executor: _ResilientRequestExecutor
    pagination_strategy: PaginationStrategy
    retry_strategy: RetryStrategy
    rate_limiter: RateLimiter
    cache: CacheStrategy | None
    circuit_breaker: CircuitBreakerStrategy


class ResilientRequestExecutorFactory:
    """
    Строит набор зависимостей для ``UnifiedAPIClient`` на основе конфигурации.
    """

    def __init__(
        self, config: APIConfig, *, logger: FilteringBoundLogger | None = None
    ) -> None:
        self._config = config
        self._logger = logger or structlog.get_logger(__name__).bind(
            api_base=config.base_url
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
        prepared_retry = retry_strategy or RetryPolicy(
            max_retries=self._config.max_retries,
            backoff_factor=self._config.backoff_factor,
            max_backoff_sec=self._config.max_backoff_sec,
        )
        prepared_rate_limiter = rate_limiter or TokenBucketRateLimiter(
            TokenBucketConfig(
                max_tokens=self._config.rate_limit_calls,
                refill_period_sec=float(self._config.rate_limit_period_sec),
            )
        )
        prepared_cache = cache
        if prepared_cache is None and self._config.cache_enabled:
            prepared_cache = TTLCache(
                TTLCacheConfig(ttl_seconds=self._config.cache_ttl_sec)
            )
        prepared_breaker = circuit_breaker or CircuitBreaker(
            CircuitBreakerConfig(
                failure_threshold=self._config.circuit_breaker_fail_max,
                reset_timeout_sec=self._config.circuit_breaker_reset_sec,
            )
        )
        executor = _ResilientRequestExecutor(
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


__all__ = ["ResilientRequestExecutorFactory", "ResilienceComponents"]
