from __future__ import annotations

import json
import random
import time
from dataclasses import dataclass
from typing import Any, Mapping, MutableMapping
from urllib.parse import urljoin

import requests
from requests import Response
from requests.exceptions import RequestException, Timeout

from bioetl.core.http._cache import TTLCache
from bioetl.core.http._rate_limiter import RateLimiterConfig, TokenBucketRateLimiter
from bioetl.core.logging import LogEvents, UnifiedLogger

_RETRYABLE_STATUS_CODES = {429, 500, 502, 503, 504}


@dataclass
class APIConfig:
    name: str
    base_url: str
    timeout: float
    connect_timeout: float | None
    retry_max_attempts: int
    retry_backoff_factor: float
    retry_jitter: bool
    rate_limit_max_calls: int
    rate_limit_period: int
    rate_limit_jitter: bool
    cache_enabled: bool
    cache_ttl: int
    headers: dict[str, str]


@dataclass
class RetryPolicy:
    max_attempts: int
    backoff_factor: float
    jitter: bool


@dataclass
class CircuitBreakerConfig:
    failure_threshold: int
    success_threshold: int
    timeout_seconds: float


class CircuitBreakerOpenError(RequestException):
    """Исключение при открытом circuit breaker."""


class CircuitBreaker:
    def __init__(self, config: CircuitBreakerConfig) -> None:
        if config.failure_threshold <= 0:
            msg = "failure_threshold must be positive"
            raise ValueError(msg)
        if config.success_threshold <= 0:
            msg = "success_threshold must be positive"
            raise ValueError(msg)
        if config.timeout_seconds <= 0:
            msg = "timeout_seconds must be positive"
            raise ValueError(msg)
        self._config = config
        self._state: str = "closed"
        self._failure_count = 0
        self._success_count = 0
        self._opened_at: float | None = None
        self._logger = UnifiedLogger.get(__name__).bind(component="circuit_breaker")

    def _transition(self, state: str, reason: str) -> None:
        self._state = state
        self._logger.info(
            LogEvents.CIRCUIT_BREAKER,
            state=state,
            reason=reason,
            failures=self._failure_count,
            successes=self._success_count,
        )

    def _before_call(self) -> None:
        if self._state == "open":
            assert self._opened_at is not None
            elapsed = time.monotonic() - self._opened_at
            if elapsed >= self._config.timeout_seconds:
                self._state = "half-open"
                self._success_count = 0
            else:
                raise CircuitBreakerOpenError(
                    "Circuit breaker is open; retry after cooldown"
                )

    def record_success(self) -> None:
        if self._state in {"half-open", "open"}:
            self._success_count += 1
            if self._success_count >= self._config.success_threshold:
                self._state = "closed"
                self._failure_count = 0
                self._success_count = 0
                self._opened_at = None
                self._transition("closed", "success_threshold_met")
        elif self._state == "closed":
            self._failure_count = 0

    def record_failure(self) -> None:
        self._failure_count += 1
        if self._state == "half-open":
            self._state = "open"
            self._opened_at = time.monotonic()
            self._transition("open", "failure_in_half_open")
            return
        if self._state == "closed" and self._failure_count >= self._config.failure_threshold:
            self._state = "open"
            self._opened_at = time.monotonic()
            self._transition("open", "failure_threshold_exceeded")

    def call(self, func: callable[[], Response]) -> Response:
        self._before_call()
        try:
            result = func()
        except Exception:
            self.record_failure()
            raise
        return result

    @property
    def state(self) -> str:
        return self._state

    def time_until_half_open(self) -> float | None:
        if self._state != "open" or self._opened_at is None:
            return None
        elapsed = time.monotonic() - self._opened_at
        remaining = self._config.timeout_seconds - elapsed
        return max(0.0, remaining)


class UnifiedAPIClient:
    def __init__(
        self,
        config: APIConfig,
        *,
        session: requests.Session | None = None,
        circuit_breaker: CircuitBreakerConfig | None = None,
    ) -> None:
        self.config = config
        self._session = session or requests.Session()
        self._session.headers.update(config.headers)
        self._retry_policy = RetryPolicy(
            max_attempts=config.retry_max_attempts,
            backoff_factor=config.retry_backoff_factor,
            jitter=config.retry_jitter,
        )
        self._rate_limiter = TokenBucketRateLimiter(
            RateLimiterConfig(
                max_calls=config.rate_limit_max_calls,
                period=float(config.rate_limit_period),
                jitter=config.rate_limit_jitter,
            )
        )
        self._cache = TTLCache() if config.cache_enabled else None
        breaker_config = circuit_breaker or CircuitBreakerConfig(
            failure_threshold=max(1, config.retry_max_attempts),
            success_threshold=1,
            timeout_seconds=max(config.timeout, 1.0),
        )
        self._circuit_breaker = CircuitBreaker(breaker_config)
        self._logger = UnifiedLogger.get(__name__).bind(api=config.name)

    # ------------------------------------------------------------------
    # Публичные методы
    # ------------------------------------------------------------------
    def get(
        self,
        path: str,
        *,
        params: Mapping[str, Any] | None = None,
        timeout: float | None = None,
    ) -> Response:
        return self.request("GET", path, params=params, timeout=timeout)

    def post(
        self,
        path: str,
        *,
        data: Any | None = None,
        json_data: Any | None = None,
        timeout: float | None = None,
    ) -> Response:
        return self.request("POST", path, data=data, json=json_data, timeout=timeout)

    def fetch_all_pages(
        self,
        path: str,
        *,
        params: Mapping[str, Any] | None = None,
        timeout: float | None = None,
    ) -> list[Response]:
        responses: list[Response] = []
        next_path: str | None = path
        current_params: MutableMapping[str, Any] = dict(params or {})
        while next_path:
            response = self.get(next_path, params=current_params, timeout=timeout)
            responses.append(response)
            try:
                payload = response.json()
            except Exception:
                break
            next_path = payload.get("next") if isinstance(payload, Mapping) else None
            if next_path:
                current_params = {}
        return responses

    # ------------------------------------------------------------------
    # Внутренние помощники
    # ------------------------------------------------------------------
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
        url = self._resolve_url(path)
        merged_headers = self._merge_headers(headers)
        cache_key = (
            self._cache.make_key(method, url, params, merged_headers)
            if self._cache
            else None
        )

        if method.upper() == "GET" and cache_key and self._cache:
            cached = self._cache.get(cache_key, self.config.cache_ttl)
            if cached is not None:
                response = self._deserialize_response(cached)
                self._logger.info(LogEvents.API_CALL, url=url, cached=True)
                return response

        attempt = 1
        last_error: Exception | None = None
        while attempt <= self._retry_policy.max_attempts:
            try:
                waited = self._rate_limiter.acquire()
                if waited > 0:
                    self._logger.info(LogEvents.RATE_LIMIT, waited=waited)
                response = self._circuit_breaker.call(
                    lambda: self._send_request(
                        method,
                        url,
                        params=params,
                        data=data,
                        json=json,
                        headers=merged_headers,
                        timeout=timeout,
                    )
                )
            except CircuitBreakerOpenError:
                raise
            except (Timeout, RequestException) as exc:
                last_error = exc
                if attempt >= self._retry_policy.max_attempts:
                    raise
                sleep_for = self._compute_backoff(attempt)
                self._logger.warning(LogEvents.RETRY, attempt=attempt, url=url)
                time.sleep(sleep_for)
                attempt += 1
                continue

            if response.status_code in _RETRYABLE_STATUS_CODES:
                if attempt >= self._retry_policy.max_attempts:
                    response.raise_for_status()
                retry_after = self._parse_retry_after(response)
                sleep_for = max(self._compute_backoff(attempt), retry_after or 0)
                self._logger.warning(
                    LogEvents.RETRY,
                    attempt=attempt,
                    url=url,
                    status=response.status_code,
                    retry_after=retry_after,
                )
                time.sleep(sleep_for)
                attempt += 1
                self._circuit_breaker.record_failure()
                continue

            if response.status_code >= 400:
                self._circuit_breaker.record_failure()
                response.raise_for_status()

            self._circuit_breaker.record_success()
            if method.upper() == "GET" and cache_key and self._cache:
                self._cache.set(cache_key, self._serialize_response(response))
            self._logger.info(
                LogEvents.API_CALL,
                url=url,
                status=response.status_code,
                attempt=attempt,
                cached=False,
            )
            return response

        raise last_error or Timeout("Request failed after retries")

    def _send_request(
        self,
        method: str,
        url: str,
        *,
        params: Mapping[str, Any] | None,
        data: Any | None,
        json: Any | None,
        headers: Mapping[str, str],
        timeout: float | None,
    ) -> Response:
        effective_timeout: float | tuple[float, float]
        total_timeout = timeout or self.config.timeout
        if self.config.connect_timeout is not None:
            effective_timeout = (self.config.connect_timeout, total_timeout)
        else:
            effective_timeout = total_timeout
        return self._session.request(
            method,
            url,
            params=params,
            data=data,
            json=json,
            headers=headers,
            timeout=effective_timeout,
        )

    def _compute_backoff(self, attempt: int) -> float:
        base = self._retry_policy.backoff_factor * (2 ** (attempt - 1))
        if not self._retry_policy.jitter:
            return base
        jitter = random.uniform(-0.1 * base, 0.1 * base)
        return max(0.0, base + jitter)

    def _parse_retry_after(self, response: Response) -> float | None:
        header = response.headers.get("Retry-After")
        if not header:
            return None
        if header.isdigit():
            return float(header)
        try:
            retry_after = requests.utils.parse_date(header)
        except Exception:
            return None
        if retry_after is None:
            return None
        seconds = retry_after - time.time()
        return max(0.0, seconds)

    def _resolve_url(self, path: str) -> str:
        if path.startswith("http://") or path.startswith("https://"):
            return path
        return urljoin(self.config.base_url.rstrip("/") + "/", path.lstrip("/"))

    def _merge_headers(self, headers: Mapping[str, str] | None) -> Mapping[str, str]:
        merged: dict[str, str] = dict(self._session.headers)
        if headers:
            merged.update(headers)
        return merged

    def _serialize_response(self, response: Response) -> bytes:
        payload = {
            "status": response.status_code,
            "headers": dict(response.headers),
            "url": response.url,
            "content": response.content.decode(response.encoding or "utf-8", errors="ignore"),
            "encoding": response.encoding,
        }
        return json.dumps(payload).encode("utf-8")

    def _deserialize_response(self, payload: bytes) -> Response:
        raw = json.loads(payload.decode("utf-8"))
        resp = Response()
        resp.status_code = int(raw["status"])
        resp._content = (raw.get("content") or "").encode(raw.get("encoding") or "utf-8")
        resp.headers = raw.get("headers") or {}
        resp.url = raw.get("url")
        resp.encoding = raw.get("encoding")
        return resp


__all__ = [
    "APIConfig",
    "RetryPolicy",
    "CircuitBreakerConfig",
    "CircuitBreakerOpenError",
    "UnifiedAPIClient",
]
