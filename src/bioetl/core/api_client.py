"""Unified HTTP API client with retry, rate limiting, and caching.

This module provides a centralized HTTP client for interacting with external
APIs, with built-in support for retry policies, rate limiting, circuit breaking,
and caching.
"""

from __future__ import annotations

import hashlib
import json
import random
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Any
from urllib.parse import urlparse, urlunparse

import requests
from requests.adapters import HTTPAdapter
from requests.exceptions import RequestException, Timeout

from bioetl.configs.models import CacheConfig, HTTPClientConfig
from bioetl.core.logger import bind_global_context, get_logger


class CircuitState(str, Enum):
    """Circuit breaker states."""

    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


@dataclass
class RetryPolicy:
    """Retry policy with exponential backoff and jitter."""

    total: int
    backoff_multiplier: float
    backoff_max: float
    statuses: tuple[int, ...]
    jitter: bool = True

    def should_retry(self, attempt: int, status_code: int | None = None, exception: Exception | None = None) -> bool:
        """Determine if a retry should be attempted.

        Args:
            attempt: Current attempt number (0-indexed).
            status_code: HTTP status code from response, if any.
            exception: Exception raised, if any.

        Returns:
            True if retry should be attempted, False otherwise.
        """
        if attempt >= self.total:
            return False

        # Retry on 5xx errors
        if status_code and 500 <= status_code < 600:
            return True

        # Retry on configured status codes
        if status_code and status_code in self.statuses:
            return True

        # Retry on network errors (RequestException)
        if exception and isinstance(exception, RequestException):
            # Don't retry on 4xx client errors unless explicitly configured
            if isinstance(exception, requests.exceptions.HTTPError):
                response = getattr(exception, "response", None)
                if response and 400 <= response.status_code < 500:
                    return response.status_code in self.statuses
            return True

        return False

    def calculate_backoff(self, attempt: int, retry_after: float | None = None) -> float:
        """Calculate backoff delay for retry attempt.

        Args:
            attempt: Current attempt number (0-indexed).
            retry_after: Retry-After header value in seconds, if present.

        Returns:
            Delay in seconds before next retry.
        """
        # Retry-After takes precedence
        if retry_after is not None:
            return float(retry_after)

        # Exponential backoff
        delay = self.backoff_multiplier ** attempt

        # Apply jitter if enabled
        if self.jitter:
            jitter_amount = delay * 0.1  # 10% jitter
            delay = delay + random.uniform(-jitter_amount, jitter_amount)

        # Cap at max
        delay = min(delay, self.backoff_max)

        return max(0.0, delay)


@dataclass
class TokenBucketLimiter:
    """Token bucket rate limiter."""

    max_calls: int
    period: float
    jitter: bool = True

    def __post_init__(self) -> None:
        """Initialize token bucket state."""
        self._tokens: float = float(self.max_calls)
        self._last_refill: float = time.time()

    def acquire(self) -> float:
        """Acquire a token, waiting if necessary.

        Returns:
            Wait time in seconds (0 if no wait needed).
        """
        now = time.time()
        elapsed = now - self._last_refill

        # Refill tokens based on elapsed time
        if elapsed > 0:
            tokens_to_add = (elapsed / self.period) * self.max_calls
            self._tokens = min(self.max_calls, self._tokens + tokens_to_add)
            self._last_refill = now

        # Check if we have a token
        if self._tokens >= 1.0:
            self._tokens -= 1.0
            return 0.0

        # Calculate wait time
        wait_time = (1.0 - self._tokens) * (self.period / self.max_calls)

        # Apply jitter if enabled
        if self.jitter:
            jitter_amount = wait_time * 0.1
            wait_time = wait_time + random.uniform(-jitter_amount, jitter_amount)

        # Refill after wait
        time.sleep(max(0.0, wait_time))
        self._tokens = 0.0
        self._last_refill = time.time()

        return wait_time


@dataclass
class CircuitBreaker:
    """Simple circuit breaker for protecting against cascading failures."""

    failure_threshold: int = 5
    timeout: float = 60.0

    def __post_init__(self) -> None:
        """Initialize circuit breaker state."""
        self._state: CircuitState = CircuitState.CLOSED
        self._failure_count: int = 0
        self._last_failure_time: float | None = None
        self._success_count: int = 0

    def call(self) -> bool:
        """Check if call is allowed.

        Returns:
            True if call is allowed, False if circuit is open.
        """
        now = time.time()

        # Check if we should transition from OPEN to HALF_OPEN
        if self._state == CircuitState.OPEN:
            if self._last_failure_time and (now - self._last_failure_time) >= self.timeout:
                self._state = CircuitState.HALF_OPEN
                self._success_count = 0
                return True
            return False

        return True

    def record_success(self) -> None:
        """Record a successful call."""
        if self._state == CircuitState.HALF_OPEN:
            self._success_count += 1
            if self._success_count >= 2:  # Require 2 successes to close
                self._state = CircuitState.CLOSED
                self._failure_count = 0
                self._success_count = 0
        elif self._state == CircuitState.CLOSED:
            self._failure_count = 0

    def record_failure(self) -> None:
        """Record a failed call."""
        self._failure_count += 1
        self._last_failure_time = time.time()

        if self._failure_count >= self.failure_threshold:
            self._state = CircuitState.OPEN
            self._success_count = 0

    @property
    def state(self) -> CircuitState:
        """Get current circuit state."""
        return self._state


@dataclass
class CacheEntry:
    """Cache entry for HTTP responses."""

    url: str
    method: str
    params: dict[str, Any]
    response_body: bytes
    headers: dict[str, str]
    status_code: int
    etag: str | None
    expires_at: float

    def is_expired(self) -> bool:
        """Check if cache entry is expired."""
        return time.time() > self.expires_at


@dataclass
class HTTPCache:
    """HTTP response cache with TTL and E-Tag support."""

    enabled: bool
    directory: Path
    ttl: int

    def __post_init__(self) -> None:
        """Initialize cache directory."""
        if self.enabled:
            self.directory.mkdir(parents=True, exist_ok=True)

    def _cache_key(self, url: str, method: str, params: dict[str, Any]) -> str:
        """Generate cache key for request."""
        key_data = f"{method}:{url}:{json.dumps(params, sort_keys=True)}"
        return hashlib.sha256(key_data.encode("utf-8")).hexdigest()

    def _cache_path(self, cache_key: str) -> Path:
        """Get cache file path."""
        return self.directory / f"{cache_key}.json"

    def get(self, url: str, method: str, params: dict[str, Any]) -> CacheEntry | None:
        """Get cached response if available and not expired.

        Args:
            url: Request URL.
            method: HTTP method.
            params: Request parameters.

        Returns:
            CacheEntry if found and valid, None otherwise.
        """
        if not self.enabled:
            return None

        cache_key = self._cache_key(url, method, params)
        cache_path = self._cache_path(cache_key)

        if not cache_path.exists():
            return None

        try:
            with cache_path.open("rb") as f:
                data = json.loads(f.read())
                entry = CacheEntry(
                    url=data["url"],
                    method=data["method"],
                    params=data["params"],
                    response_body=data["response_body"].encode("utf-8"),
                    headers=data["headers"],
                    status_code=data["status_code"],
                    etag=data.get("etag"),
                    expires_at=data["expires_at"],
                )

                if entry.is_expired():
                    cache_path.unlink()
                    return None

                return entry
        except Exception:
            # If cache file is corrupted, remove it
            cache_path.unlink(missing_ok=True)
            return None

    def set(
        self,
        url: str,
        method: str,
        params: dict[str, Any],
        response_body: bytes,
        headers: dict[str, str],
        status_code: int,
        etag: str | None = None,
    ) -> None:
        """Store response in cache.

        Args:
            url: Request URL.
            method: HTTP method.
            params: Request parameters.
            response_body: Response body.
            headers: Response headers.
            status_code: HTTP status code.
            etag: E-Tag header value, if present.
        """
        if not self.enabled:
            return

        cache_key = self._cache_key(url, method, params)
        cache_path = self._cache_path(cache_key)

        expires_at = time.time() + self.ttl

        try:
            data = {
                "url": url,
                "method": method,
                "params": params,
                "response_body": response_body.decode("utf-8", errors="replace"),
                "headers": headers,
                "status_code": status_code,
                "etag": etag,
                "expires_at": expires_at,
            }

            tmp_path = cache_path.with_suffix(".tmp")
            with tmp_path.open("w", encoding="utf-8") as f:
                json.dump(data, f)
            tmp_path.replace(cache_path)
        except Exception:
            # If cache write fails, silently continue
            pass

    def clear(self) -> None:
        """Clear all cached entries."""
        if not self.enabled:
            return

        for cache_file in self.directory.glob("*.json"):
            cache_file.unlink(missing_ok=True)


@dataclass
class UnifiedAPIClient:
    """Unified HTTP API client with retry, rate limiting, and caching."""

    name: str
    base_url: str
    config: HTTPClientConfig
    cache_config: CacheConfig | None = None
    logger: Any = field(default_factory=lambda: get_logger(__name__))

    def __post_init__(self) -> None:
        """Initialize client components."""
        # Create session with connection pooling
        self.session = requests.Session()

        # Configure adapter with timeouts
        adapter = HTTPAdapter()
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)

        # Initialize components
        self.retry_policy = RetryPolicy(
            total=self.config.retries.total,
            backoff_multiplier=self.config.retries.backoff_multiplier,
            backoff_max=self.config.retries.backoff_max,
            statuses=self.config.retries.statuses,
            jitter=True,
        )

        self.rate_limiter = TokenBucketLimiter(
            max_calls=self.config.rate_limit.max_calls,
            period=self.config.rate_limit.period,
            jitter=self.config.rate_limit_jitter,
        )

        self.circuit_breaker = CircuitBreaker(failure_threshold=5, timeout=60.0)

        # Initialize cache
        if self.cache_config:
            cache_dir = Path(self.cache_config.directory)
            self.cache = HTTPCache(
                enabled=self.cache_config.enabled,
                directory=cache_dir,
                ttl=self.cache_config.ttl,
            )
        else:
            self.cache = HTTPCache(enabled=False, directory=Path(), ttl=0)

    def close(self) -> None:
        """Close the client and release resources."""
        self.session.close()

    def __enter__(self) -> UnifiedAPIClient:
        """Context manager entry."""
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Context manager exit."""
        self.close()

    def _parse_retry_after(self, retry_after: str | None) -> float | None:
        """Parse Retry-After header value.

        Args:
            retry_after: Retry-After header value (seconds or HTTP-date).

        Returns:
            Delay in seconds, or None if invalid.
        """
        if not retry_after:
            return None

        # Try integer (seconds)
        try:
            return float(retry_after)
        except ValueError:
            pass

        # Try HTTP-date format
        try:
            from email.utils import parsedate_to_datetime

            dt = parsedate_to_datetime(retry_after)
            if dt:
                delta = dt - datetime.now(timezone.utc)
                return max(0.0, delta.total_seconds())
        except Exception:
            pass

        return None

    def _build_url(self, endpoint: str) -> str:
        """Build full URL from endpoint.

        Args:
            endpoint: Endpoint path (relative or absolute).

        Returns:
            Full URL.
        """
        if endpoint.startswith("http://") or endpoint.startswith("https://"):
            return endpoint

        # Ensure base_url ends with / and endpoint doesn't start with /
        base = self.base_url.rstrip("/")
        endpoint = endpoint.lstrip("/")

        return f"{base}/{endpoint}"

    def _execute(
        self,
        method: str,
        url: str,
        params: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
        **kwargs: Any,
    ) -> requests.Response:
        """Execute HTTP request with retry, rate limiting, and circuit breaking.

        Args:
            method: HTTP method.
            url: Request URL.
            params: Query parameters.
            headers: Request headers.
            **kwargs: Additional arguments passed to requests.

        Returns:
            Response object.

        Raises:
            requests.exceptions.RequestException: If all retries are exhausted.
        """
        full_url = self._build_url(url)
        params = params or {}
        headers = headers or {}
        headers = {**self.config.headers, **headers}

        request_id = str(uuid.uuid4())

        # Generate request trace context
        http_context = {
            "request_id": request_id,
            "endpoint": full_url,
            "method": method,
            "params": params,
        }

        # Check cache for GET requests
        if method.upper() == "GET" and self.cache.enabled:
            cache_entry = self.cache.get(full_url, method, params)
            if cache_entry:
                # Check if we can use If-None-Match
                if cache_entry.etag:
                    headers["If-None-Match"] = cache_entry.etag

        # Bind HTTP context for logging
        bind_global_context(http=http_context)
        self.logger.debug("http_request_started", **http_context)

        # Check circuit breaker
        if not self.circuit_breaker.call():
            self.logger.warning(
                "circuit_breaker_open",
                **http_context,
                circuit_state=self.circuit_breaker.state.value,
            )
            raise requests.exceptions.RequestException("Circuit breaker is open")

        # Rate limiting
        wait_time = self.rate_limiter.acquire()
        if wait_time > 0:
            http_context["rate_limit_wait_seconds"] = wait_time
            self.logger.debug("rate_limit_wait", **http_context)

        attempt = 0
        last_exception: Exception | None = None

        while True:
            attempt += 1
            http_context["attempt"] = attempt

            request_start = time.time()

            try:
                # Prepare request
                timeout = (self.config.connect_timeout_sec, self.config.read_timeout_sec)

                # Execute request
                response = self.session.request(
                    method=method,
                    url=full_url,
                    params=params,
                    headers=headers,
                    timeout=timeout,
                    **kwargs,
                )

                duration_ms = (time.time() - request_start) * 1000.0
                http_context["duration_ms"] = duration_ms
                http_context["status_code"] = response.status_code

                # Handle 304 Not Modified (cache hit)
                if response.status_code == 304 and cache_entry:
                    self.logger.debug("cache_hit", **http_context)
                    # Return cached response
                    cached_response = requests.Response()
                    cached_response.status_code = cache_entry.status_code
                    cached_response.headers = cache_entry.headers
                    cached_response._content = cache_entry.response_body
                    cached_response.url = full_url
                    self.circuit_breaker.record_success()
                    return cached_response

                # Handle successful responses
                if 200 <= response.status_code < 300:
                    # Cache successful GET responses
                    if method.upper() == "GET" and self.cache.enabled:
                        etag = response.headers.get("ETag")
                        self.cache.set(
                            url=full_url,
                            method=method,
                            params=params,
                            response_body=response.content,
                            headers=dict(response.headers),
                            status_code=response.status_code,
                            etag=etag,
                        )

                    self.circuit_breaker.record_success()
                    self.logger.info("http_request_success", **http_context)
                    return response

                # Check if we should retry
                should_retry = self.retry_policy.should_retry(
                    attempt=attempt - 1,
                    status_code=response.status_code,
                )

                if not should_retry:
                    response.raise_for_status()

                # Calculate backoff
                retry_after = self._parse_retry_after(response.headers.get("Retry-After"))
                backoff = self.retry_policy.calculate_backoff(attempt - 1, retry_after)

                http_context["retry_after"] = retry_after
                http_context["backoff_seconds"] = backoff

                self.logger.warning("http_request_retry", **http_context, status_code=response.status_code)

                if backoff > 0:
                    time.sleep(backoff)

                # Continue to retry
                continue

            except (Timeout, requests.exceptions.ConnectionError) as e:
                duration_ms = (time.time() - request_start) * 1000.0
                http_context["duration_ms"] = duration_ms
                last_exception = e

                should_retry = self.retry_policy.should_retry(attempt=attempt - 1, exception=e)

                if not should_retry:
                    self.circuit_breaker.record_failure()
                    self.logger.error("http_request_failed", **http_context, error=str(e), exc_info=True)
                    raise

                backoff = self.retry_policy.calculate_backoff(attempt - 1)
                http_context["backoff_seconds"] = backoff

                self.logger.warning("http_request_retry", **http_context, error=str(e))

                if backoff > 0:
                    time.sleep(backoff)

                # Continue to retry
                continue

            except requests.exceptions.RequestException as e:
                duration_ms = (time.time() - request_start) * 1000.0
                http_context["duration_ms"] = duration_ms
                last_exception = e

                # Don't retry on 4xx errors unless explicitly configured
                if isinstance(e, requests.exceptions.HTTPError):
                    response = getattr(e, "response", None)
                    if response and 400 <= response.status_code < 500:
                        should_retry = self.retry_policy.should_retry(
                            attempt=attempt - 1,
                            status_code=response.status_code,
                        )
                        if not should_retry:
                            self.circuit_breaker.record_failure()
                            self.logger.error("http_request_failed", **http_context, error=str(e), exc_info=True)
                            raise

                should_retry = self.retry_policy.should_retry(attempt=attempt - 1, exception=e)

                if not should_retry:
                    self.circuit_breaker.record_failure()
                    self.logger.error("http_request_failed", **http_context, error=str(e), exc_info=True)
                    raise

                backoff = self.retry_policy.calculate_backoff(attempt - 1)
                http_context["backoff_seconds"] = backoff

                self.logger.warning("http_request_retry", **http_context, error=str(e))

                if backoff > 0:
                    time.sleep(backoff)

                # Continue to retry
                continue

        # Should never reach here, but just in case
        if last_exception:
            self.circuit_breaker.record_failure()
            raise last_exception

        raise requests.exceptions.RequestException("Unexpected error in request execution")

    def request_json(
        self,
        endpoint: str,
        params: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
        **kwargs: Any,
    ) -> dict[str, Any] | list[Any]:
        """Make HTTP request and return JSON response.

        Args:
            endpoint: Endpoint path (relative or absolute).
            params: Query parameters.
            headers: Request headers.
            **kwargs: Additional arguments passed to requests.

        Returns:
            Parsed JSON response.

        Raises:
            requests.exceptions.RequestException: If request fails.
            ValueError: If response is not valid JSON.
        """
        response = self._execute("GET", endpoint, params=params, headers=headers, **kwargs)
        try:
            return response.json()
        except ValueError as e:
            self.logger.error(
                "json_decode_error",
                endpoint=endpoint,
                status_code=response.status_code,
                error=str(e),
            )
            raise ValueError(f"Failed to decode JSON response: {e}") from e

    def request_text(
        self,
        endpoint: str,
        params: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
        method: str = "GET",
        **kwargs: Any,
    ) -> str:
        """Make HTTP request and return text response.

        Args:
            endpoint: Endpoint path (relative or absolute).
            params: Query parameters.
            headers: Request headers.
            method: HTTP method (default: GET).
            **kwargs: Additional arguments passed to requests.

        Returns:
            Response text.

        Raises:
            requests.exceptions.RequestException: If request fails.
        """
        response = self._execute(method, endpoint, params=params, headers=headers, **kwargs)
        return response.text

