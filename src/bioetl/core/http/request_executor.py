from __future__ import annotations

from __future__ import annotations

import json
import time
from datetime import datetime, timezone
from typing import Any, Mapping

from requests import Response
from requests import Session
from requests.exceptions import RequestException
import structlog
from structlog.typing import FilteringBoundLogger

from bioetl.core.http.cache import CacheStrategy
from bioetl.core.http.circuit_breaker import (
    CircuitBreakerOpenError,
    CircuitBreakerStrategy,
)
from bioetl.core.http.rate_limiter import RateLimiter
from bioetl.core.http.retry import RetryStrategy

_RETRYABLE_STATUS_CODES = {429, 500, 502, 503, 504}


class HTTPClientError(RuntimeError):
    """Базовое исключение клиента."""


class _ResilientRequestExecutor:
    def __init__(
        self,
        *,
        session: Session,
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

    def _normalize_cache_params(
        self,
        params: Mapping[str, Any] | None,
    ) -> Mapping[str, Any] | None:
        if not params:
            return None
        normalized: dict[str, Any] = dict(params)
        page_value = normalized.get("page")
        if page_value is not None and str(page_value) == "1":
            normalized.pop("page")
        return normalized or None

    def request(
        self,
        method: str,
        url: str,
        *,
        headers: Mapping[str, str] | None = None,
        params: Mapping[str, Any] | None = None,
        json: Any | None = None,
    ) -> dict[str, Any]:
        cache_key = None
        if method.upper() == "GET" and self._cache:
            norm_params = self._normalize_cache_params(params)
            cache_key = self._cache.make_key(
                method,
                url,
                norm_params,
                headers,
            )
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

    def _serialize(self, payload: dict[str, Any]) -> bytes:
        return json.dumps(payload, sort_keys=True).encode("utf-8")

    def _deserialize(self, payload: bytes) -> dict[str, Any]:
        data = json.loads(payload.decode("utf-8"))
        self._logger.info("api_call", cache_hit=True)
        return data


__all__ = ["HTTPClientError", "_ResilientRequestExecutor"]
