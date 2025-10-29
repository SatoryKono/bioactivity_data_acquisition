"""UnifiedAPIClient: resilient HTTP client with circuit breaker, rate limiting, retry policies."""

import hashlib
import json
import random
import threading
import time
from collections.abc import Callable, Iterable
from dataclasses import dataclass, field
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from typing import Any

import requests
from cachetools import TTLCache  # type: ignore

from bioetl.core.logger import UnifiedLogger

logger = UnifiedLogger.get(__name__)


class CircuitBreakerOpenError(Exception):
    """Circuit breaker is open."""


class RateLimitExceeded(Exception):
    """Rate limit exceeded."""


class PartialFailure(Exception):
    """Partial failure in batch request."""


@dataclass
class APIConfig:
    """API client configuration."""

    name: str
    base_url: str
    headers: dict[str, str] = field(default_factory=dict)
    cache_enabled: bool = False
    cache_ttl: int = 3600
    cache_maxsize: int = 1024
    rate_limit_max_calls: int = 1
    rate_limit_period: float = 1.0
    rate_limit_jitter: bool = True
    retry_total: int = 3
    retry_backoff_factor: float = 2.0
    retry_backoff_max: float | None = None
    retry_giveup_on: list[type[Exception]] = field(default_factory=list)
    retry_status_codes: list[int] = field(default_factory=list)
    partial_retry_max: int = 3
    timeout_connect: float = 10.0
    timeout_read: float = 30.0
    cb_failure_threshold: int = 5
    cb_timeout: float = 60.0
    fallback_enabled: bool = True
    fallback_strategies: list[str] = field(default_factory=lambda: ["network", "timeout"])


class CircuitBreaker:
    """Circuit breaker для защиты API."""

    def __init__(self, name: str, failure_threshold: int = 5, timeout: float = 60.0):
        self.name = name
        self.failure_threshold = failure_threshold
        self.timeout = timeout
        self.failure_count = 0
        self.last_failure_time: float | None = None
        self.state = "closed"  # closed, open, half-open
        self.lock = threading.Lock()

    def call(self, func: Callable[[], Any]) -> Any:
        """Выполняет func с circuit breaker."""
        with self.lock:
            if self.state == "open":
                if self.last_failure_time and time.time() - self.last_failure_time > self.timeout:
                    self.state = "half-open"
                    logger.warning("circuit_breaker_half_open", circuit=self.name)
                else:
                    raise CircuitBreakerOpenError(f"Circuit breaker for {self.name} is open")

            if self.state == "half-open":
                logger.info("circuit_breaker_test", circuit=self.name)

        try:
            result = func()
            with self.lock:
                if self.state == "half-open":
                    self.state = "closed"
                    self.failure_count = 0
                    logger.info("circuit_breaker_closed", circuit=self.name)
            return result
        except Exception as e:
            with self.lock:
                self.failure_count += 1
                self.last_failure_time = time.time()

                if self.failure_count >= self.failure_threshold:
                    self.state = "open"
                    logger.error(
                        "circuit_breaker_opened",
                        circuit=self.name,
                        failures=self.failure_count,
                        error=str(e),
                    )
            raise

    def reset(self) -> None:
        """Сбрасывает circuit breaker."""
        with self.lock:
            self.state = "closed"
            self.failure_count = 0
            self.last_failure_time = None


class TokenBucketLimiter:
    """Token bucket rate limiter с jitter."""

    def __init__(self, max_calls: int, period: float, jitter: bool = True):
        self.max_calls = max_calls
        self.period = period
        self.jitter = jitter
        self.tokens = max_calls
        self.last_refill = time.monotonic()
        self.lock = threading.Lock()

    def acquire(self) -> None:
        """Ожидает и получает token."""
        with self.lock:
            self._refill()

            if self.tokens >= 1:
                self.tokens -= 1
                if self.jitter:
                    jitter = random.uniform(0, self.period * 0.1)
                    time.sleep(jitter)
            else:
                wait_time = self.period - (time.monotonic() - self.last_refill)
                if wait_time > 0:
                    logger.debug("rate_limit_wait", wait_seconds=wait_time)
                    time.sleep(wait_time)
                    self._refill()
                    self.tokens -= 1

    def _refill(self) -> None:
        """Пополняет bucket."""
        now = time.monotonic()
        elapsed = now - self.last_refill

        if elapsed >= self.period:
            self.tokens = self.max_calls
            self.last_refill = now


class RetryPolicy:
    """Политика повторов с giveup условиями."""

    DEFAULT_RETRY_STATUS_CODES: set[int] = {429}

    def __init__(
        self,
        total: int = 3,
        backoff_factor: float = 2.0,
        giveup_on: list[type[Exception]] | None = None,
        backoff_max: float | None = None,
        status_codes: Iterable[int] | None = None,
    ):
        self.total = total
        self.backoff_factor = backoff_factor
        self.giveup_on = giveup_on or []
        self.backoff_max = backoff_max
        self.retry_status_codes: set[int] = (
            {int(code) for code in status_codes}
            if status_codes is not None
            else set()
        )

    def should_giveup(self, exc: Exception, attempt: int) -> bool:
        """Определяет, нужно ли прекратить попытки."""
        if attempt >= self.total:
            return True

        if type(exc) in self.giveup_on:
            return True

        if isinstance(exc, requests.exceptions.HTTPError):
            response = getattr(exc, "response", None)
            if response is not None:
                status_code = response.status_code
                retryable_statuses = (
                    set(self.retry_status_codes)
                    if self.retry_status_codes
                    else set(self.DEFAULT_RETRY_STATUS_CODES)
                )

                if 500 <= status_code < 600:
                    return False

                if status_code in retryable_statuses:
                    return False

                if 400 <= status_code < 500:
                    logger.error(
                        "client_error_giving_up",
                        code=status_code,
                        attempt=attempt,
                        error=str(exc),
                    )
                    return True

        return False

    def get_wait_time(self, attempt: int, retry_after: float | None = None) -> float:
        """Вычисляет время ожидания для attempt."""
        if retry_after is not None:
            return max(0.0, float(retry_after))

        effective_attempt = max(attempt, 0)
        wait_time = float(self.backoff_factor) ** effective_attempt

        if self.backoff_max is not None:
            wait_time = min(wait_time, self.backoff_max)

        return wait_time


class UnifiedAPIClient:
    """Unified API client with circuit breaker, rate limiting, and retries."""

    def __init__(self, config: APIConfig):
        self.config = config
        self.session = requests.Session()
        self.session.headers.update(config.headers)

        # Initialize circuit breaker
        self.circuit_breaker = CircuitBreaker(
            name=config.name,
            failure_threshold=config.cb_failure_threshold,
            timeout=config.cb_timeout,
        )

        # Initialize rate limiter
        self.rate_limiter = TokenBucketLimiter(
            max_calls=config.rate_limit_max_calls,
            period=config.rate_limit_period,
            jitter=config.rate_limit_jitter,
        )

        # Initialize retry policy
        self.retry_policy = RetryPolicy(
            total=config.retry_total,
            backoff_factor=config.retry_backoff_factor,
            giveup_on=config.retry_giveup_on,
            backoff_max=config.retry_backoff_max,
            status_codes=config.retry_status_codes,
        )

        # Initialize cache if enabled
        self.cache: TTLCache[str, Any] | None = None
        if config.cache_enabled:
            self.cache = TTLCache(
                maxsize=config.cache_maxsize,
                ttl=config.cache_ttl,
            )

    def request_json(
        self,
        url: str,
        params: dict[str, Any] | None = None,
        method: str = "GET",
        data: dict[str, Any] | None = None,
        json: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """
        Make JSON request with all protections.

        Args:
            url: Request URL
            params: Query parameters
            method: HTTP method

        Returns:
            JSON response as dict
        """
        # Build full URL
        if not url.startswith("http"):
            url = f"{self.config.base_url}{url}"

        # Check cache for GET requests
        cache_key: str | None = None
        if self.cache and method == "GET" and not data and not json:
            cache_key = self._cache_key(url, params)
            if cache_key in self.cache:
                logger.debug("cache_hit", url=url)
                cached_value: dict[str, Any] = self.cache[cache_key]
                return cached_value

        # Execute with circuit breaker
        def _execute() -> requests.Response:
            # Rate limit
            self.rate_limiter.acquire()

            # Make request
            response = self.session.request(
                method=method,
                url=url,
                params=params,
                data=data,
                json=json,
                timeout=(self.config.timeout_connect, self.config.timeout_read),
            )

            # Check for retry-after header
            if response.status_code == 429:
                retry_after = response.headers.get("Retry-After")
                if retry_after:
                    wait_time = float(retry_after)
                    logger.warning("retry_after_header", wait_seconds=wait_time)
                    time.sleep(wait_time)
                    # Retry once after waiting
                    response = self.session.request(
                        method=method,
                        url=url,
                        params=params,
                        data=data,
                        json=json,
                        timeout=(self.config.timeout_connect, self.config.timeout_read),
                    )

            return response

        # Retry logic
        last_exc: Exception | None = None
        last_response: requests.Response | None = None
        last_attempt_timestamp: float | None = None
        last_retry_after_header: str | None = None
        last_error_text: str | None = None
        last_attempt = 0

        for attempt in range(1, self.retry_policy.total + 1):
            try:
                response = self.circuit_breaker.call(_execute)
                response.raise_for_status()

                # Parse JSON
                data: dict[str, Any] = response.json()

                # Cache result
                if self.cache and cache_key and method == "GET" and not data and not json:
                    self.cache[cache_key] = data

                return data

            except requests.exceptions.HTTPError as e:
                last_exc = e
                last_attempt = attempt
                last_attempt_timestamp = time.time()
                last_response = getattr(e, "response", None)
                last_retry_after_header = (
                    last_response.headers.get("Retry-After")
                    if last_response is not None and last_response.headers
                    else None
                )
                last_error_text = (
                    last_response.text if last_response is not None else str(e)
                )

                if self.retry_policy.should_giveup(e, attempt):
                    break

                retry_after_seconds = self._retry_after_seconds(last_response)
                wait_time = self.retry_policy.get_wait_time(
                    attempt,
                    retry_after=retry_after_seconds,
                )
                logger.warning(
                    "retrying_request",
                    attempt=attempt,
                    wait_seconds=wait_time,
                    error=str(e),
                    status_code=(
                        last_response.status_code if last_response is not None else None
                    ),
                    retry_after=last_retry_after_header,
                )
                time.sleep(wait_time)

            except Exception as e:
                last_exc = e
                logger.error("request_error", attempt=attempt, error=str(e))
                raise

        if last_exc:
            if not hasattr(last_exc, "retry_metadata"):
                metadata = {
                    "attempt": last_attempt,
                    "timestamp": last_attempt_timestamp or time.time(),
                    "status_code": (
                        last_response.status_code if last_response is not None else None
                    ),
                    "error_text": last_error_text or str(last_exc),
                    "retry_after": last_retry_after_header,
                }
                setattr(last_exc, "retry_metadata", metadata)
            raise last_exc
        raise RuntimeError("Request failed with no exception captured")

    @staticmethod
    def _retry_after_seconds(response: requests.Response | None) -> float | None:
        """Parse Retry-After header into seconds if possible."""

        if response is None or not response.headers:
            return None

        retry_after = response.headers.get("Retry-After")
        if not retry_after:
            return None

        try:
            return float(retry_after)
        except (TypeError, ValueError):
            try:
                parsed = parsedate_to_datetime(retry_after)
            except (TypeError, ValueError):
                return None

            if parsed is None:
                return None

            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=timezone.utc)

            delta = parsed - datetime.now(timezone.utc)
            return max(delta.total_seconds(), 0.0)

    def _cache_key(self, url: str, params: dict[str, Any] | None) -> str:
        """Generate cache key for request."""
        key_parts = [url]
        if params:
            sorted_params = sorted(params.items())
            params_str = json.dumps(sorted_params, sort_keys=True)
            key_parts.append(params_str)
        return hashlib.sha256("|".join(key_parts).encode()).hexdigest()

    def close(self) -> None:
        """Close session and cleanup resources."""
        self.session.close()

