from __future__ import annotations

from __future__ import annotations
from dataclasses import dataclass, field
from typing import Any, Dict, Iterator, Mapping
from urllib.parse import urljoin

import requests
import structlog

from bioetl.core.http.cache import CacheStrategy
from bioetl.core.http.circuit_breaker import CircuitBreakerStrategy
from bioetl.core.http.pagination import DefaultPaginationStrategy, PaginationStrategy
from bioetl.core.http.rate_limiter import RateLimiter
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
        session: requests.Session,
        pagination_strategy: PaginationStrategy | None = None,
    ) -> None:
        self.config = config
        self._logger = structlog.get_logger(__name__).bind(api_base=config.base_url)
        self._session = session
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
        verify_ssl: bool = True,
        default_headers: Mapping[str, str] | None = None,
    ) -> "UnifiedAPIClient":
        factory = ResilientRequestExecutorFactory(config)
        components = factory.create(
            session=session,
            retry_strategy=retry_strategy,
            rate_limiter=rate_limiter,
            cache=cache,
            circuit_breaker=circuit_breaker,
            pagination_strategy=pagination_strategy,
            verify_ssl=verify_ssl,
            default_headers=default_headers,
        )
        return cls(
            config,
            request_executor=components.executor,
            session=components.session,
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
        page_key: str = "results",
        next_key: str = "next",
        page_param: str | None = "page",
    ) -> Iterator[Dict[str, Any]]:
        yield from self.iterate_paginated(
            path, params=params or {}, page_key=page_key, next_key=next_key, page_param=page_param
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
        yield from self._pagination.paginate(
            path,
            params,
            lambda next_path, page_params: self.request("GET", next_path, params=page_params),
            page_key=page_key,
            next_key=next_key,
            page_param=page_param,
        )

    def close(self) -> None:
        self._session.close()

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
        url = self._resolve_url(path)
        merged_headers = {**self._session.headers, **(headers or {})}
        return self._request_executor.request(
            method,
            url,
            headers=merged_headers,
            params=params,
            json=json,
        )

    def _resolve_url(self, path: str) -> str:
        if path.startswith("http://") or path.startswith("https://"):
            return path
        return urljoin(self.config.base_url.rstrip("/") + "/", path.lstrip("/"))

__all__ = [
    "APIConfig",
    "UnifiedAPIClient",
    "HTTPClientError",
    "ResilientRequestExecutorFactory",
]
