from __future__ import annotations

import json
import random
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, Iterator, Mapping, MutableMapping
from urllib.parse import urljoin

import requests
from requests import Response
from requests.exceptions import RequestException
import structlog

from bioetl.core.http._cache import TTLCache, TTLCacheConfig
from bioetl.core.http._rate_limiter import TokenBucketConfig, TokenBucketRateLimiter

_RETRYABLE_STATUS_CODES = {429, 500, 502, 503, 504}


class CircuitBreakerOpenError(RuntimeError):
    """Исключение при открытом circuit breaker."""


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


@dataclass
class RetryPolicy:
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


@dataclass
class CircuitBreakerConfig:
    failure_threshold: int
    reset_timeout_sec: float


class CircuitBreaker:
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

    def call(self, func: callable[[], Response]) -> Response:
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


class UnifiedAPIClient:
    def __init__(
        self,
        config: APIConfig,
        *,
        session: requests.Session | None = None,
        verify_ssl: bool = True,
    ) -> None:
        self.config = config
        self._logger = structlog.get_logger(__name__).bind(api_base=config.base_url)
        self._session = session or requests.Session()
        default_headers = {"User-Agent": config.user_agent, **config.default_headers}
        self._session.headers.update(default_headers)
        self._session.verify = verify_ssl
        self._retry_policy = RetryPolicy(
            max_retries=config.max_retries,
            backoff_factor=config.backoff_factor,
            max_backoff_sec=config.max_backoff_sec,
        )
        self._rate_limiter = TokenBucketRateLimiter(
            TokenBucketConfig(
                max_tokens=config.rate_limit_calls,
                refill_period_sec=float(config.rate_limit_period_sec),
            )
        )
        self._cache = TTLCache(TTLCacheConfig(ttl_seconds=config.cache_ttl_sec)) if config.cache_enabled else None
        self._breaker = CircuitBreaker(
            CircuitBreakerConfig(
                failure_threshold=config.circuit_breaker_fail_max,
                reset_timeout_sec=config.circuit_breaker_reset_sec,
            )
        )

    # ---------------------------- public API ---------------------------
    def get_json(
        self,
        path: str,
        params: Mapping[str, Any] | None = None,
        headers: Mapping[str, str] | None = None,
        *,
        paginate: bool = False,
    ) -> Dict[str, Any] | Iterator[Dict[str, Any]]:
        if paginate:
            return self.iterate_paginated(path, params=params or {})
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
        page_param: str = "page",
    ) -> Iterator[Dict[str, Any]]:
        yield from self.iterate_paginated(path, params=params or {}, page_key=page_key, page_param=page_param)

    def iterate_paginated(
        self,
        path: str,
        params: Mapping[str, Any],
        *,
        page_key: str = "items",
        page_param: str = "page",
    ) -> Iterator[Dict[str, Any]]:
        page_params: MutableMapping[str, Any] = dict(params)
        page_params.setdefault(page_param, 1)
        while True:
            payload = self.request("GET", path, params=page_params)
            yield payload
            items = payload.get(page_key) if isinstance(payload, Mapping) else None
            if not items:
                break
            page_params[page_param] = page_params.get(page_param, 1) + 1

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
        cache_key = None
        if method.upper() == "GET" and self._cache:
            cache_key = self._cache.make_key(method, url, params, merged_headers)
            cached = self._cache.get(cache_key)
            if cached is not None:
                return self._deserialize(cached)

        attempt = 0
        last_error: Exception | None = None
        while attempt <= self._retry_policy.max_retries:
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
                        headers=merged_headers,
                        timeout=self.config.timeout_sec,
                    )
                )
                latency_ms = (time.perf_counter() - start) * 1000
                if response.status_code in _RETRYABLE_STATUS_CODES:
                    retry_after = self._parse_retry_after(response)
                    if attempt > self._retry_policy.max_retries:
                        response.raise_for_status()
                    wait_for = self._retry_policy.compute_backoff(attempt, retry_after=retry_after)
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
                if attempt > self._retry_policy.max_retries:
                    raise HTTPClientError(str(exc)) from exc
                wait_for = self._retry_policy.compute_backoff(attempt)
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

    def _resolve_url(self, path: str) -> str:
        if path.startswith("http://") or path.startswith("https://"):
            return path
        return urljoin(self.config.base_url.rstrip("/") + "/", path.lstrip("/"))

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
