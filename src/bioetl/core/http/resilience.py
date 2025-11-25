from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Mapping

import requests
import structlog
from structlog.typing import FilteringBoundLogger

if TYPE_CHECKING:
    from bioetl.core.http.api_client import APIConfig
from bioetl.core.http.cache import CacheStrategy, TTLCache, TTLCacheConfig
from bioetl.core.http.circuit_breaker import CircuitBreaker, CircuitBreakerConfig, CircuitBreakerStrategy
from bioetl.core.http.pagination import DefaultPaginationStrategy, PaginationStrategy
from bioetl.core.http.rate_limiter import RateLimiter, TokenBucketConfig, TokenBucketRateLimiter
from bioetl.core.http.request_executor import _ResilientRequestExecutor
from bioetl.core.http.retry import RetryPolicy, RetryStrategy


@dataclass
class ResilienceComponents:
    session: requests.Session
    executor: _ResilientRequestExecutor
    pagination_strategy: PaginationStrategy
    retry_strategy: RetryStrategy
    rate_limiter: RateLimiter
    cache: CacheStrategy | None
    circuit_breaker: CircuitBreakerStrategy


class ResilientRequestExecutorFactory:
    """Строит набор зависимостей для ``UnifiedAPIClient`` на основе конфигурации."""

    def __init__(self, config: APIConfig, *, logger: FilteringBoundLogger | None = None) -> None:
        self._config = config
        self._logger = logger or structlog.get_logger(__name__).bind(api_base=config.base_url)

    def create(
        self,
        *,
        session: requests.Session | None = None,
        retry_strategy: RetryStrategy | None = None,
        rate_limiter: RateLimiter | None = None,
        cache: CacheStrategy | None = None,
        circuit_breaker: CircuitBreakerStrategy | None = None,
        pagination_strategy: PaginationStrategy | None = None,
        verify_ssl: bool = True,
        default_headers: Mapping[str, str] | None = None,
    ) -> ResilienceComponents:
        prepared_session = self._prepare_session(session=session, verify_ssl=verify_ssl, default_headers=default_headers)
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
            prepared_cache = TTLCache(TTLCacheConfig(ttl_seconds=self._config.cache_ttl_sec))
        prepared_breaker = circuit_breaker or CircuitBreaker(
            CircuitBreakerConfig(
                failure_threshold=self._config.circuit_breaker_fail_max,
                reset_timeout_sec=self._config.circuit_breaker_reset_sec,
            )
        )
        executor = _ResilientRequestExecutor(
            session=prepared_session,
            logger=self._logger,
            retry_strategy=prepared_retry,
            rate_limiter=prepared_rate_limiter,
            cache=prepared_cache,
            circuit_breaker=prepared_breaker,
            timeout_sec=self._config.timeout_sec,
        )
        pagination = pagination_strategy or DefaultPaginationStrategy()

        return ResilienceComponents(
            session=prepared_session,
            executor=executor,
            pagination_strategy=pagination,
            retry_strategy=prepared_retry,
            rate_limiter=prepared_rate_limiter,
            cache=prepared_cache,
            circuit_breaker=prepared_breaker,
        )

    def _prepare_session(
        self,
        *,
        session: requests.Session | None,
        verify_ssl: bool,
        default_headers: Mapping[str, str] | None,
    ) -> requests.Session:
        prepared_session = session or requests.Session()
        headers = {"User-Agent": self._config.user_agent, **self._config.default_headers}
        if default_headers:
            headers.update(default_headers)
        prepared_session.headers.update(headers)
        prepared_session.verify = verify_ssl
        return prepared_session


__all__ = ["ResilientRequestExecutorFactory", "ResilienceComponents"]
