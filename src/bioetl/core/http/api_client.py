from __future__ import annotations
from dataclasses import dataclass, field
from typing import Any, Dict, Iterator, Mapping

import requests
import structlog

from bioetl.core.http.cache import CacheStrategy
from bioetl.core.http.circuit_breaker import CircuitBreakerStrategy
from bioetl.core.http.pagination import DefaultPaginationStrategy, PaginationStrategy
from bioetl.core.http.rate_limiter import RateLimiter
from bioetl.core.http.request_builder import RequestBuilder
from bioetl.core.http.request_executor import HTTPClientError, _ResilientRequestExecutor
from bioetl.core.http.resilience import ResilientRequestExecutorFactory
from bioetl.core.http.retry import RetryStrategy


@dataclass
class APIConfig:
    base_url: str
    timeout_sec: float
    max_retries: int
    backoff_factor: float
    max_backoff_sec: float
    rate_limit_calls: int
    rate_limit_period_sec: float
    cache_enabled: bool
    cache_ttl_sec: int
    circuit_breaker_fail_max: int
    circuit_breaker_reset_sec: int
    default_headers: dict[str, str] = field(default_factory=dict)
    user_agent: str = "bioetl-http-client"


class UnifiedAPIClient:
    def __init__(
        self,
        config: APIConfig,
        *,
        request_executor: _ResilientRequestExecutor,
        request_builder: RequestBuilder,
        pagination_strategy: PaginationStrategy | None = None,
    ) -> None:
        self.config = config
        self._logger = structlog.get_logger(__name__).bind(api_base=config.base_url)
        self._request_builder = request_builder
        self._pagination = pagination_strategy or DefaultPaginationStrategy()
        self._request_executor = request_executor

    @classmethod
    def from_config(
        cls,
        config: APIConfig,
        *,
        session: requests.Session | None = None,
        retry_strategy: RetryStrategy | None = None,
        rate_limiter: RateLimiter | None = None,
        cache: CacheStrategy | None = None,
        circuit_breaker: CircuitBreakerStrategy | None = None,
        pagination_strategy: PaginationStrategy | None = None,
        request_builder: RequestBuilder | None = None,
        verify_ssl: bool = True,
        default_headers: Mapping[str, str] | None = None,
    ) -> "UnifiedAPIClient":
        builder = request_builder or RequestBuilder(
            config,
            session=session,
            verify_ssl=verify_ssl,
            default_headers=default_headers,
        )
        factory = ResilientRequestExecutorFactory(config)
        components = factory.create(
            request_builder=builder,
            retry_strategy=retry_strategy,
            rate_limiter=rate_limiter,
            cache=cache,
            circuit_breaker=circuit_breaker,
            pagination_strategy=pagination_strategy,
        )
        return cls(
            config,
            request_executor=components.executor,
            request_builder=components.request_builder,
            pagination_strategy=components.pagination_strategy,
        )

    # ---------------------------- public API ---------------------------
    def get_json(
        self,
        path: str,
        params: Mapping[str, Any] | None = None,
        headers: Mapping[str, str] | None = None,
        *,
        paginate: bool = False,
    ) -> Dict[str, Any] | list[Dict[str, Any]]:
        if paginate:
            return list(self.iterate_paginated(path, params=params or {}))
        return self.request("GET", path, headers=headers, params=params)

    def post_json(
        self,
        path: str,
        json: Any,
        headers: Mapping[str, str] | None = None,
    ) -> Dict[str, Any]:
        return self.request("POST", path, headers=headers, json=json)

    def paginate_json(
        self,
        path: str,
        params: Mapping[str, Any] | None = None,
        *,
        page_key: str = "items",
        next_key: str = "next",
        page_param: str | None = "page",
    ) -> Iterator[Dict[str, Any]]:
        yield from self.iterate_paginated(
            path,
            params=params or {},
            page_key=page_key,
            next_key=next_key,
            page_param=page_param,
        )

    def iterate_paginated(
        self,
        path: str,
        params: Mapping[str, Any],
        *,
        page_key: str = "results",
        next_key: str = "next",
        page_param: str | None = "page",
    ) -> Iterator[Dict[str, Any]]:
        first_payload = self.request("GET", path, params=params)
        yield from self._pagination.iter_pages(
            first_payload,
            self,
            endpoint=path,
            params=params,
            page_key=page_key,
            next_key=next_key,
            page_param=page_param,
        )

    def close(self) -> None:
        self._request_builder.close()

    # --------------------------- internals -----------------------------
    def request(
        self,
        method: str,
        path: str,
        *,
        headers: Mapping[str, str] | None = None,
        params: Mapping[str, Any] | None = None,
        json: Any | None = None,
    ) -> Dict[str, Any]:
        url = self._request_builder.build_url(path)
        merged_headers = self._request_builder.merge_headers(headers)
        return self._request_executor.request(
            method,
            url,
            headers=merged_headers,
            params=params,
            json=json,
        )

    __all__ = [
        "APIConfig",
        "UnifiedAPIClient",
        "HTTPClientError",
        "ResilientRequestExecutorFactory",
    ]
