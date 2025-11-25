"""Resilient HTTP client with retries, rate-limiting, cache, and circuit breaker."""
from __future__ import annotations

import logging
import random
import tempfile
import time
from dataclasses import dataclass, field
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import Any, Callable, Mapping
from urllib.parse import urljoin

import requests
from requests import Response
from requests.exceptions import RequestException, Timeout

from ._cache import TTLCache
from ._rate_limiter import RateLimiterConfig, TokenBucketRateLimiter

RETRYABLE_STATUS_CODES = {429, 500, 502, 503, 504}
logger = logging.getLogger(__name__)


@dataclass
class APIConfig:
    name: str
    base_url: str
    timeout: float = 30.0
    connect_timeout: float | None = None
    retry_max_attempts: int = 3
    retry_backoff_factor: float = 0.5
    retry_jitter: bool = True
    rate_limit_max_calls: int = 10
    rate_limit_period: int = 1
    rate_limit_jitter: bool = False
    cache_enabled: bool = False
    cache_ttl: int = 300
    headers: dict[str, str] = field(default_factory=dict)


@dataclass
class RetryPolicy:
    max_attempts: int
    backoff_factor: float
    jitter: bool

    def backoff(self, attempt: int) -> float:
        base = self.backoff_factor * (2 ** (attempt - 1))
        if self.jitter:
            delta = base * 0.1
            return random.uniform(base - delta, base + delta)
        return base


@dataclass
class CircuitBreakerConfig:
    failure_threshold: int = 5
    success_threshold: int = 1
    timeout_seconds: float = 60.0


class CircuitBreakerOpenError(RequestException):
    """Raised when circuit breaker is open."""


class _CircuitBreaker:
    def __init__(
        self,
        config: CircuitBreakerConfig,
        *,
        monotonic: Callable[[], float] | None = None,
    ) -> None:
        self.config = config
        self._monotonic = monotonic or time.monotonic
        self._state: str = "closed"
        self._failure_count = 0
        self._success_count = 0
        self._opened_at: float | None = None

    def _now(self) -> float:
        return self._monotonic()

    def before_request(self) -> None:
        if self._state != "open":
            return
        assert self._opened_at is not None
        if self._now() - self._opened_at >= self.config.timeout_seconds:
            self._state = "half-open"
            self._success_count = 0
        else:
            raise CircuitBreakerOpenError("Circuit breaker is open")

    def record_success(self) -> None:
        if self._state == "half-open":
            self._success_count += 1
            if self._success_count >= self.config.success_threshold:
                self._state = "closed"
                self._failure_count = 0
        else:
            self._failure_count = 0

    def record_failure(self) -> None:
        if self._state == "half-open":
            self._open()
            return
        self._failure_count += 1
        if self._failure_count >= self.config.failure_threshold:
            self._open()

    def _open(self) -> None:
        self._state = "open"
        self._opened_at = self._now()
        self._success_count = 0


class UnifiedAPIClient:
    def __init__(
        self,
        config: APIConfig,
        *,
        session: requests.Session | None = None,
        circuit_breaker_config: CircuitBreakerConfig | None = None,
        cache_path: str | None = None,
    ) -> None:
        self.config = config
        self.retry_policy = RetryPolicy(
            config.retry_max_attempts,
            config.retry_backoff_factor,
            config.retry_jitter,
        )
        self.session = session or requests.Session()
        self.rate_limiter = TokenBucketRateLimiter(
            RateLimiterConfig(
                max_calls=config.rate_limit_max_calls,
                period=float(config.rate_limit_period),
                jitter=config.rate_limit_jitter,
            )
        )
        default_cache_path = Path(tempfile.gettempdir()) / f"{config.name}_http_cache.db"
        self.cache = (
            TTLCache(cache_path or default_cache_path, ttl=config.cache_ttl)
            if config.cache_enabled
            else None
        )
        cb_config = circuit_breaker_config or CircuitBreakerConfig()
        self.circuit_breaker = _CircuitBreaker(cb_config)

    def _build_url(self, path: str) -> str:
        return urljoin(self.config.base_url.rstrip("/") + "/", path.lstrip("/"))

    def _merge_headers(self, headers: Mapping[str, str] | None) -> dict[str, str]:
        merged = dict(self.config.headers)
        if headers:
            merged.update(headers)
        return merged

    def _timeout(self, override: float | None) -> float | tuple[float, float]:
        effective = override if override is not None else self.config.timeout
        if self.config.connect_timeout is not None:
            return (self.config.connect_timeout, effective)
        return effective

    def _should_retry(self, response: Response | None, error: Exception | None) -> bool:
        if error is not None:
            return isinstance(error, (Timeout, RequestException))
        if response is None:
            return False
        return response.status_code in RETRYABLE_STATUS_CODES

    def _retry_after(self, response: Response) -> float | None:
        value = response.headers.get("Retry-After")
        if not value:
            return None
        if value.isdigit():
            return float(value)
        try:
            dt = parsedate_to_datetime(value)
        except (TypeError, ValueError):
            return None
        if dt is None:
            return None
        return max(0.0, (dt - dt.now(dt.tzinfo)).total_seconds())

    def request(
        self,
        method: str,
        path: str,
        *,
        params: Mapping[str, Any] | None = None,
        data: Any | None = None,
        json: Any | None = None,
        headers: Mapping[str, str] | None = None,
        timeout: float | None = None,
    ) -> Response:
        url = self._build_url(path)
        merged_headers = self._merge_headers(headers)

        if self.cache and method.upper() == "GET":
            cache_key = self.cache.make_key(method, url, params, merged_headers)
            cached = self.cache.get(cache_key)
            if cached:
                logger.info("cache_hit", api=self.config.name, url=url)
                return cached
        else:
            cache_key = None

        self.circuit_breaker.before_request()

        last_error: Exception | None = None
        response: Response | None = None
        for attempt in range(1, self.retry_policy.max_attempts + 1):
            self.rate_limiter.acquire()
            try:
                response = self.session.request(
                    method,
                    url,
                    params=params,
                    data=data,
                    json=json,
                    headers=merged_headers,
                    timeout=self._timeout(timeout),
                )
                last_error = None
            except Exception as exc:  # noqa: BLE001
                last_error = exc

            should_retry = self._should_retry(response, last_error)
            if not should_retry:
                break

            if attempt >= self.retry_policy.max_attempts:
                break

            wait_for = self.retry_policy.backoff(attempt)
            if response is not None and response.status_code == 429:
                retry_after = self._retry_after(response)
                if retry_after is not None:
                    wait_for = max(wait_for, retry_after)
            logger.info(
                "retrying_request",
                attempt=attempt,
                wait=wait_for,
                url=url,
                status=response.status_code if response else None,
                error=str(last_error) if last_error else None,
            )
            time.sleep(wait_for)

        if last_error is not None:
            self.circuit_breaker.record_failure()
            raise last_error

        assert response is not None
        if response.status_code in RETRYABLE_STATUS_CODES:
            self.circuit_breaker.record_failure()
        else:
            self.circuit_breaker.record_success()

        if (
            self.cache
            and cache_key
            and response.status_code == 200
            and method.upper() == "GET"
        ):
            self.cache.set(cache_key, response)

        return response

    def get(
        self,
        path: str,
        *,
        params: Mapping[str, Any] | None = None,
        headers: Mapping[str, str] | None = None,
        timeout: float | None = None,
    ) -> Response:
        return self.request(
            "GET", path, params=params, headers=headers, timeout=timeout
        )

    def post(
        self,
        path: str,
        *,
        data: Any | None = None,
        json: Any | None = None,
        headers: Mapping[str, str] | None = None,
        timeout: float | None = None,
    ) -> Response:
        return self.request(
            "POST", path, data=data, json=json, headers=headers, timeout=timeout
        )

    def fetch_all_pages(
        self,
        path: str,
        *,
        params: Mapping[str, Any] | None = None,
        timeout: float | None = None,
    ) -> list[Response]:
        pages: list[Response] = []
        current_path = path
        current_params = dict(params or {})
        while True:
            resp = self.get(current_path, params=current_params, timeout=timeout)
            pages.append(resp)
            try:
                data = resp.json()
            except ValueError:
                break
            next_link = None
            if isinstance(data, Mapping):
                next_link = data.get("next")
            if not next_link:
                break
            if next_link.startswith("http"):
                current_path = next_link
                current_params = {}
            else:
                current_path = next_link
                current_params = {}
        return pages
