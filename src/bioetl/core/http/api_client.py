from __future__ import annotations

import json
import time

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, Iterator, Mapping
from urllib.parse import urljoin

import requests
from requests import Response
from requests.exceptions import RequestException
import structlog
from structlog.typing import FilteringBoundLogger

from bioetl.core.http.cache import CacheStrategy, TTLCache, TTLCacheConfig
from bioetl.core.http.circuit_breaker import (
    CircuitBreaker,
    CircuitBreakerConfig,
    CircuitBreakerOpenError,
    CircuitBreakerStrategy,
)
from bioetl.core.http.pagination import DefaultPaginationStrategy, PaginationStrategy
from bioetl.core.http.rate_limiter import RateLimiter, TokenBucketConfig, TokenBucketRateLimiter
from bioetl.core.http.retry import RetryPolicy, RetryStrategy

_RETRYABLE_STATUS_CODES = {429, 500, 502, 503, 504}


class HTTPClientError(RuntimeError):
    """Базовое исключение клиента."""


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
        session: requests.Session | None = None,
        rate_limiter: RateLimiter | None = None,
        cache: CacheStrategy | None = None,
        retry_strategy: RetryStrategy | None = None,
        circuit_breaker: CircuitBreakerStrategy | None = None,
        pagination_strategy: PaginationStrategy | None = None,
        verify_ssl: bool = True,
    ) -> None:
        self.config = config
        self._logger = structlog.get_logger(__name__).bind(api_base=config.base_url)
        self._session = session or requests.Session()
        default_headers = {"User-Agent": config.user_agent, **config.default_headers}
        self._session.headers.update(default_headers)
        self._session.verify = verify_ssl
        self._retry_strategy = retry_strategy or RetryPolicy(
            max_retries=config.max_retries,
            backoff_factor=config.backoff_factor,
            max_backoff_sec=config.max_backoff_sec,
        )
        self._rate_limiter = rate_limiter or TokenBucketRateLimiter(
            TokenBucketConfig(
                max_tokens=config.rate_limit_calls,
                refill_period_sec=float(config.rate_limit_period_sec),
            )
        )
        self._cache = cache if cache is not None else (
            TTLCache(TTLCacheConfig(ttl_seconds=config.cache_ttl_sec)) if config.cache_enabled else None
        )
        self._breaker = circuit_breaker or CircuitBreaker(
            CircuitBreakerConfig(
                failure_threshold=config.circuit_breaker_fail_max,
                reset_timeout_sec=config.circuit_breaker_reset_sec,
            )
        )
        self._pagination = pagination_strategy or DefaultPaginationStrategy()
        self._request_executor = _ResilientRequestExecutor(
            session=self._session,
            logger=self._logger,
            retry_strategy=self._retry_strategy,
            rate_limiter=self._rate_limiter,
            cache=self._cache,
            circuit_breaker=self._breaker,
            timeout_sec=self.config.timeout_sec,
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


class _ResilientRequestExecutor:
    def __init__(
        self,
        *,
        session: requests.Session,
        logger: FilteringBoundLogger,
        retry_strategy: RetryStrategy,
        rate_limiter: RateLimiter,
        cache: CacheStrategy | None,
        circuit_breaker: CircuitBreakerStrategy,
        timeout_sec: float,
    ) -> None:
        self._session = session
        self._logger = logger
        self._retry_strategy = retry_strategy
        self._rate_limiter = rate_limiter
        self._cache = cache
        self._breaker = circuit_breaker
        self._timeout_sec = timeout_sec

    def request(
        self,
        method: str,
        url: str,
        *,
        headers: Mapping[str, str] | None = None,
        params: Mapping[str, Any] | None = None,
        json: Any | None = None,
    ) -> Dict[str, Any]:
        cache_key = None
        if method.upper() == "GET" and self._cache:
            cache_key = self._cache.make_key(method, url, params, headers)
            cached = self._cache.get(cache_key)
            if cached is not None:
                return self._deserialize(cached)

        attempt = 0
        last_error: Exception | None = None
        while attempt <= self._retry_strategy.max_retries:
            attempt += 1
            try:
                if not self._rate_limiter.acquire():
                    raise HTTPClientError("Rate limiter timeout")
                start = time.perf_counter()
                response = self._breaker.call(
                    lambda: self._session.request(
                        method,
                        url,
                        params=params,
                        json=json,
                        headers=headers,
                        timeout=self._timeout_sec,
                    )
                )
                latency_ms = (time.perf_counter() - start) * 1000
                if response.status_code in _RETRYABLE_STATUS_CODES:
                    retry_after = self._parse_retry_after(response)
                    if attempt > self._retry_strategy.max_retries:
                        response.raise_for_status()
                    wait_for = self._retry_strategy.compute_backoff(attempt, retry_after=retry_after)
                    self._logger.warning(
                        "api_retry",
                        attempt=attempt,
                        url=url,
                        status=response.status_code,
                        retry_after_sec=retry_after,
                        wait_sec=wait_for,
                    )
                    time.sleep(wait_for)
                    self._breaker.record_failure()
                    continue
                response.raise_for_status()
                payload = response.json()
                self._breaker.record_success()
                self._logger.info(
                    "api_call",
                    status=response.status_code,
                    latency_ms=latency_ms,
                    attempts=attempt,
                    cache_hit=False,
                )
                if cache_key and self._cache:
                    self._cache.set(cache_key, self._serialize(payload))
                return payload
            except CircuitBreakerOpenError:
                raise
            except (RequestException, ValueError) as exc:
                last_error = exc
                self._breaker.record_failure()
                if attempt > self._retry_strategy.max_retries:
                    raise HTTPClientError(str(exc)) from exc
                wait_for = self._retry_strategy.compute_backoff(attempt)
                self._logger.warning("api_retry", attempt=attempt, url=url, error=str(exc), wait_sec=wait_for)
                time.sleep(wait_for)

        raise HTTPClientError("Request failed") from last_error

    def _parse_retry_after(self, response: Response) -> float | None:
        raw = response.headers.get("Retry-After")
        if not raw:
            return None
        if raw.isdigit():
            return float(raw)
        try:
            parsed = datetime.strptime(raw, "%a, %d %b %Y %H:%M:%S GMT").replace(tzinfo=timezone.utc)
        except Exception:
            return None
        return max(0.0, parsed.timestamp() - datetime.now(tz=timezone.utc).timestamp())

    def _serialize(self, payload: Dict[str, Any]) -> bytes:
        return json.dumps(payload, sort_keys=True).encode("utf-8")

    def _deserialize(self, payload: bytes) -> Dict[str, Any]:
        data = json.loads(payload.decode("utf-8"))
        self._logger.info("api_call", cache_hit=True)
        return data

__all__ = [
    "APIConfig",
    "RetryPolicy",
    "CircuitBreaker",
    "CircuitBreakerConfig",
    "CircuitBreakerOpenError",
    "UnifiedAPIClient",
    "HTTPClientError",
]
