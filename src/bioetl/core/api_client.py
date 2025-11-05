"""Unified HTTP client with retries, timeouts, and rate limiting."""

from __future__ import annotations

import random
import threading
import time
from collections import deque
from collections.abc import Mapping, MutableMapping
from dataclasses import dataclass
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from typing import Any
from urllib.parse import urljoin
from uuid import uuid4

import requests
from requests import Response
from requests.exceptions import HTTPError, RequestException, Timeout

from bioetl.config.models import HTTPClientConfig
from bioetl.core.logger import UnifiedLogger

__all__ = [
    "TokenBucketLimiter",
    "CircuitBreakerOpenError",
    "UnifiedAPIClient",
    "merge_http_configs",
]


class CircuitBreakerOpenError(RequestException):
    """Raised when the circuit breaker blocks outbound requests."""

    pass


@dataclass(slots=True, frozen=True)
class _RetryState:
    attempt: int
    response: Response | None = None
    error: RequestException | None = None
    retry_after: float | None = None


class TokenBucketLimiter:
    """Simple token bucket limiter enforcing max calls per period."""

    def __init__(self, max_calls: int, period: float, *, jitter: bool = True) -> None:
        if max_calls <= 0:
            msg = "max_calls must be > 0"
            raise ValueError(msg)
        if period <= 0:
            msg = "period must be > 0"
            raise ValueError(msg)
        self.max_calls = max_calls
        self.period = period
        self.jitter = jitter
        self._timestamps: deque[float] = deque()
        self._lock = threading.Lock()
        self._jitter_max = period / max_calls

    def acquire(self) -> float:
        """Block until a token is available and return wait seconds."""

        waited = 0.0
        while True:
            with self._lock:
                now = time.monotonic()
                while self._timestamps and now - self._timestamps[0] >= self.period:
                    self._timestamps.popleft()
                if len(self._timestamps) < self.max_calls:
                    self._timestamps.append(now)
                    return waited
                sleep_for = self.period - (now - self._timestamps[0])
            if self.jitter:
                sleep_for += random.uniform(0.0, self._jitter_max)
            if sleep_for > 0:
                time.sleep(sleep_for)
                waited += sleep_for
            else:  # pragma: no cover - defensive, should rarely happen
                time.sleep(0)


def _parse_retry_after(value: str | None) -> float | None:
    if not value:
        return None
    value = value.strip()
    if not value:
        return None
    if value.isdigit():
        try:
            return float(value)
        except ValueError:
            return None
    try:
        parsed = parsedate_to_datetime(value)
    except (TypeError, ValueError):
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    now = datetime.now(timezone.utc)
    delta = (parsed - now).total_seconds()
    return max(delta, 0.0)


def _deep_merge(base: MutableMapping[str, Any], override: Mapping[str, Any]) -> MutableMapping[str, Any]:
    for key, value in override.items():
        if key in base and isinstance(base[key], MutableMapping) and isinstance(value, Mapping):
            _deep_merge(base[key], value)  # type: ignore[arg-type]
        else:
            base[key] = value  # type: ignore[index]
    return base


class UnifiedAPIClient:
    """HTTP client providing retries, timeouts, and rate limiting."""

    def __init__(
        self,
        config: HTTPClientConfig,
        *,
        base_url: str | None = None,
        name: str | None = None,
        session: requests.Session | None = None,
    ) -> None:
        self.config = config
        self.base_url = base_url.rstrip("/") if base_url else ""
        self.name = name or "default"
        self._session = session or requests.Session()
        self._session.headers.update(dict(config.headers))
        self._timeout = self._derive_timeout(config)
        self._retry_total = int(config.retries.total)
        self._retry_statuses = set(config.retries.statuses)
        self._backoff_multiplier = float(config.retries.backoff_multiplier)
        self._backoff_max = float(config.retries.backoff_max)
        self._max_url_length = int(config.max_url_length)
        self._rate_limiter = TokenBucketLimiter(
            config.rate_limit.max_calls,
            config.rate_limit.period,
            jitter=config.rate_limit_jitter,
        )
        self._logger = UnifiedLogger.get(__name__).bind(
            component="http_client",
            http_client=self.name,
        )

    @staticmethod
    def _derive_timeout(config: HTTPClientConfig) -> tuple[float, float]:
        connect = min(config.connect_timeout_sec, config.timeout_sec)
        remaining = max(config.timeout_sec - connect, 0.0)
        read = config.read_timeout_sec
        if remaining > 0:
            read = min(read, remaining)
        if read <= 0:
            read = config.read_timeout_sec
        return (connect, read)

    def close(self) -> None:
        self._session.close()

    # ------------------------------------------------------------------
    # Request execution
    # ------------------------------------------------------------------

    def get(
        self,
        endpoint: str,
        *,
        params: Mapping[str, Any] | None = None,
        headers: Mapping[str, str] | None = None,
    ) -> Response:
        params_dict: dict[str, Any] = dict(params or {})
        full_url: str | None = None
        if params_dict and (self.base_url or endpoint.startswith(("http://", "https://"))):
            full_url = self._prepare_full_url(endpoint, params_dict)
            if self._max_url_length and len(full_url) > self._max_url_length:
                override_headers = dict(headers or {})
                override_headers.setdefault("X-HTTP-Method-Override", "GET")
                self._logger.info(
                    "http.request.method_override",
                    endpoint=full_url,
                    url_length=len(full_url),
                    max_length=self._max_url_length,
                    client=self.name,
                )
                return self.request(
                    "POST",
                    endpoint,
                    data=params_dict,
                    headers=override_headers,
                )
        return self.request("GET", endpoint, params=params_dict or None, headers=headers)

    def request(
        self,
        method: str,
        endpoint: str,
        *,
        params: Mapping[str, Any] | None = None,
        json: Any | None = None,
        data: Any | None = None,
        headers: Mapping[str, str] | None = None,
    ) -> Response:
        url = self._resolve_url(endpoint)
        attempt = 0
        last_error: RequestException | None = None
        response: Response | None = None
        max_attempts = self._retry_total + 1
        request_id = str(uuid4())
        while attempt < max_attempts:
            attempt += 1
            wait_seconds = self._rate_limiter.acquire()
            if wait_seconds:
                self._logger.debug(
                    "http.rate_limiter.wait",
                    wait_seconds=wait_seconds,
                    endpoint=url,
                    attempt=attempt,
                    request_id=request_id,
                )
            start = time.perf_counter()
            try:
                response = self._session.request(
                    method,
                    url,
                    params=params,
                    json=json,
                    data=data,
                    headers=self._apply_headers(headers),
                    timeout=self._timeout,
                )
            except RequestException as exc:
                duration_ms = (time.perf_counter() - start) * 1000
                last_error = exc
                self._logger.warning(
                    "http.request.exception",
                    endpoint=url,
                    attempt=attempt,
                    duration_ms=duration_ms,
                    request_id=request_id,
                    error=str(exc),
                )
                if attempt >= max_attempts:
                    break
                sleep_for = self._compute_backoff(_RetryState(attempt=attempt, error=exc))
                self._sleep(sleep_for)
                continue

            duration_ms = (time.perf_counter() - start) * 1000
            status_code = response.status_code
            retry_after = _parse_retry_after(response.headers.get("Retry-After"))
            if self._should_retry(status_code):
                self._logger.warning(
                    "http.request.retry",
                    endpoint=url,
                    attempt=attempt,
                    duration_ms=duration_ms,
                    status_code=status_code,
                    retry_after=retry_after,
                    request_id=request_id,
                )
                if attempt >= max_attempts:
                    break
                sleep_for = self._compute_backoff(
                    _RetryState(attempt=attempt, response=response, retry_after=retry_after)
                )
                self._sleep(sleep_for)
                continue

            if 400 <= status_code:
                self._logger.error(
                    "http.request.failed",
                    endpoint=url,
                    attempt=attempt,
                    duration_ms=duration_ms,
                    status_code=status_code,
                    request_id=request_id,
                )
                response.raise_for_status()
            else:
                self._logger.info(
                    "http.request.completed",
                    endpoint=url,
                    attempt=attempt,
                    duration_ms=duration_ms,
                    status_code=status_code,
                    request_id=request_id,
                )
            return response

        if response is not None and response.status_code >= 400:
            try:
                response.raise_for_status()
            except HTTPError:
                raise
        if last_error:
            raise last_error
        raise Timeout(f"Request to {url} failed after {max_attempts} attempts")

    def request_json(
        self,
        method: str,
        endpoint: str,
        *,
        params: Mapping[str, Any] | None = None,
        json: Any | None = None,
        data: Any | None = None,
        headers: Mapping[str, str] | None = None,
    ) -> Any:
        response = self.request(
            method,
            endpoint,
            params=params,
            json=json,
            data=data,
            headers=headers,
        )
        return response.json()

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _apply_headers(self, extra: Mapping[str, str] | None) -> Mapping[str, str]:
        # Convert headers to str-only dict, handling both str and bytes values
        session_headers: dict[str, str] = {}
        for k, v in self._session.headers.items():
            if isinstance(v, str):
                session_headers[k] = v
            else:
                # Handle bytes (requests headers can be bytes) or other types
                try:
                    session_headers[k] = v.decode("utf-8")  # type: ignore[union-attr]
                except (AttributeError, UnicodeDecodeError):
                    session_headers[k] = str(v)

        if not extra:
            return session_headers
        merged: dict[str, str] = dict(session_headers)
        merged.update(extra)
        return merged

    def _resolve_url(self, endpoint: str) -> str:
        if endpoint.startswith("http://") or endpoint.startswith("https://"):
            return endpoint
        if not self.base_url:
            return endpoint
        resolved = urljoin(self.base_url + "/", endpoint.lstrip("/"))
        self._logger.debug(
            "http.resolve_url",
            endpoint=endpoint,
            base_url=self.base_url,
            resolved=resolved,
        )
        return resolved

    def _prepare_full_url(self, endpoint: str, params: Mapping[str, Any]) -> str:
        url = self._resolve_url(endpoint)
        prepared = requests.Request("GET", url, params=params).prepare()
        return prepared.url or url

    def _should_retry(self, status_code: int) -> bool:
        if status_code in self._retry_statuses:
            return True
        return 500 <= status_code <= 599

    def _compute_backoff(self, state: _RetryState) -> float:
        if state.retry_after is not None:
            return state.retry_after
        attempt_index = max(state.attempt - 1, 0)
        delay = self._backoff_multiplier ** attempt_index
        delay = min(delay, self._backoff_max)
        if self.config.rate_limit_jitter and delay > 0:
            delay += random.uniform(0.0, min(delay, 1.0))
        return delay

    @staticmethod
    def _sleep(duration: float) -> None:
        if duration <= 0:
            return
        time.sleep(duration)


def merge_http_configs(base: HTTPClientConfig, *overrides: HTTPClientConfig | None) -> HTTPClientConfig:
    """Return a new ``HTTPClientConfig`` merging overrides on top of ``base``."""

    payload: MutableMapping[str, Any] = base.model_dump()
    for override in overrides:
        if override is None:
            continue
        override_payload = override.model_dump(exclude_unset=True)
        payload = _deep_merge(payload, override_payload)
    return HTTPClientConfig.model_validate(payload)
