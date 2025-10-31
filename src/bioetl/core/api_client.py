"""UnifiedAPIClient: resilient HTTP client with circuit breaker, rate limiting, retry policies."""

import copy
import hashlib
import json
import random
import threading
import time
from collections.abc import Callable, Iterable, Iterator, Sequence
from dataclasses import dataclass, field
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from typing import Any, TypeVar, cast

import backoff
import requests
from cachetools import TTLCache  # type: ignore
from requests.exceptions import HTTPError, RequestException

from bioetl.core.fallback_manager import FallbackManager
from bioetl.core.logger import UnifiedLogger

logger = UnifiedLogger.get(__name__)

PayloadT = TypeVar("PayloadT")

FALLBACK_MANAGER_SUPPORTED_STRATEGIES: frozenset[str] = frozenset({
    "network",
    "timeout",
    "5xx",
})


def _current_utc_time() -> datetime:
    """Return the current UTC time."""

    return datetime.now(timezone.utc)


def parse_retry_after(value: float | int | str | None) -> float | None:
    """Parse Retry-After header value to seconds.

    Supports both numeric seconds and HTTP-date formats.
    """

    if value is None:
        return None

    if isinstance(value, (int, float)):
        try:
            seconds = float(value)
        except (TypeError, ValueError):
            return None
        return max(seconds, 0.0)

    if isinstance(value, str):
        retry_after = value.strip()
        if not retry_after:
            return None

        try:
            seconds = float(retry_after)
        except (TypeError, ValueError):
            try:
                parsed = parsedate_to_datetime(retry_after)
            except (TypeError, ValueError):
                return None

            if parsed is None:
                return None

            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=timezone.utc)

            seconds = (parsed - _current_utc_time()).total_seconds()

        return max(seconds, 0.0)

    return None


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
    fallback_strategies: list[str] = field(
        default_factory=lambda: [
            "cache",
            "partial_retry",
            "network",
            "timeout",
            "5xx",
        ]
    )


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

    LONG_WAIT_THRESHOLD_SECONDS = 1.0

    def __init__(self, max_calls: int, period: float, jitter: bool = True):
        self.max_calls = max_calls
        self.period = period
        self.jitter = jitter
        self.tokens = max_calls
        self.last_refill = time.monotonic()
        self.lock = threading.Lock()

    def acquire(self) -> None:
        """Ожидает и получает token."""
        while True:
            jitter_sleep = 0.0
            wait_time = 0.0

            with self.lock:
                self._refill()

                if self.tokens >= 1:
                    self.tokens -= 1
                    if self.jitter:
                        jitter_sleep = random.uniform(0, self.period * 0.1)
                else:
                    wait_time = max(0.0, self.period - (time.monotonic() - self.last_refill))

            if wait_time > 0:
                if wait_time >= self.LONG_WAIT_THRESHOLD_SECONDS:
                    logger.warning(
                        "rate_limit_wait_long",
                        wait_seconds=wait_time,
                        max_calls=self.max_calls,
                        period=self.period,
                    )
                else:
                    logger.debug(
                        "rate_limit_wait",
                        wait_seconds=wait_time,
                        max_calls=self.max_calls,
                        period=self.period,
                    )
                time.sleep(wait_time)
                continue

            if jitter_sleep > 0:
                time.sleep(jitter_sleep)

            break

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

    def get_wait_time(
        self, attempt: int, retry_after: float | int | str | None = None
    ) -> float:
        """Вычисляет время ожидания для attempt."""
        if retry_after is not None:
            wait_override = parse_retry_after(retry_after)
            if wait_override is not None:
                logger.debug(
                    "retry_policy_retry_after_override",
                    attempt=attempt,
                    wait_seconds=wait_override,
                    retry_after_raw=retry_after,
                )
                return wait_override

        effective_attempt = max(attempt, 0)
        wait_time = float(self.backoff_factor) ** effective_attempt

        if self.backoff_max is not None:
            wait_time = min(wait_time, self.backoff_max)

        return wait_time


@dataclass
class _RequestRetryContext:
    client: "UnifiedAPIClient"
    url: str
    method: str
    params: dict[str, Any] | None
    data_present: bool
    json_present: bool
    attempt: int = 0
    last_exc: RequestException | None = None
    last_response: requests.Response | None = None
    last_retry_after_header: str | None = None
    last_retry_after_seconds: float | None = None
    wait_time: float = 0.0
    sleep_wait: float = 0.0
    last_error_text: str | None = None
    last_attempt_timestamp: float | None = None

    def start_attempt(self) -> None:
        """Record that a new attempt is starting."""

        self.attempt += 1
        self.wait_time = 0.0
        self.sleep_wait = 0.0

    def record_failure(self, exc: RequestException) -> None:
        """Capture context about a failure before raising for backoff handling."""

        self.last_exc = exc
        self.last_attempt_timestamp = time.time()
        self.last_response = getattr(exc, "response", None)
        self.last_retry_after_header = (
            self.last_response.headers.get("Retry-After")
            if self.last_response is not None and self.last_response.headers
            else None
        )
        self.last_retry_after_seconds = self.client._retry_after_seconds(
            self.last_response
        )
        self.last_error_text = (
            self.last_response.text if self.last_response is not None else str(exc)
        )
        self.wait_time = self.client.retry_policy.get_wait_time(
            self.attempt,
            retry_after=self.last_retry_after_seconds,
        )
        if self.wait_time > 0:
            self.sleep_wait = float(self.wait_time)
        else:
            self.sleep_wait = 0.0

    def record_unhandled_exception(self, exc: Exception) -> None:
        """Log unexpected errors that should not trigger retry."""

        logger.error(
            "request_error",
            attempt=self.attempt,
            url=self.url,
            method=self.method,
            params=self.params,
            error=str(exc),
        )

    def should_giveup(self, exc: Exception) -> bool:
        """Delegate giveup decision to retry policy with side effects for logging."""

        should_stop = self.client.retry_policy.should_giveup(exc, self.attempt)
        if (
            should_stop
            and isinstance(exc, RequestException)
            and not isinstance(exc, HTTPError)
        ):
            status_code = (
                self.last_response.status_code if self.last_response is not None else None
            )
            logger.error(
                "request_exception_giveup",
                attempt=self.attempt,
                url=self.url,
                method=self.method,
                params=self.params,
                error=str(exc),
                exception_type=type(exc).__name__,
                status_code=status_code,
            )
        return should_stop

    def on_backoff(self, details: dict[str, Any]) -> None:
        """Log retry attempts and manage waiting via backoff callback."""

        tries = details.get("tries")
        if isinstance(tries, int):
            self.attempt = max(self.attempt, tries)

        exc = details.get("exception")
        if exc is None and self.last_exc is not None:
            exc = self.last_exc

        status_code = (
            self.last_response.status_code if self.last_response is not None else None
        )

        if isinstance(exc, HTTPError):
            logger.warning(
                "retrying_request",
                attempt=self.attempt,
                wait_seconds=self.wait_time,
                sleep_seconds=self.sleep_wait,
                error=str(exc),
                status_code=status_code,
                retry_after=self.last_retry_after_header,
            )
        else:
            logger.warning(
                "retrying_request_exception",
                attempt=self.attempt,
                wait_seconds=self.wait_time,
                sleep_seconds=self.sleep_wait,
                error=str(exc) if exc is not None else None,
                exception_type=type(exc).__name__ if exc is not None else None,
                status_code=status_code,
                retry_after=self.last_retry_after_header,
            )

        if self.wait_time > 0:
            actual_sleep = float(self.wait_time)
            logger.debug(
                "retry_wait_sleep",
                attempt=self.attempt,
                sleep_seconds=actual_sleep,
                status_code=status_code,
                retry_after=self.last_retry_after_header,
            )
            time.sleep(actual_sleep)

    def on_giveup(self, details: dict[str, Any]) -> None:
        """Attach retry metadata and log final failure when retries exhausted."""

        tries = details.get("tries")
        if isinstance(tries, int):
            self.attempt = max(self.attempt, tries)

        exc = details.get("exception")
        if exc is None and self.last_exc is not None:
            exc = self.last_exc

        if isinstance(exc, Exception) and not hasattr(exc, "retry_metadata"):
            metadata = {
                "attempt": self.attempt,
                "timestamp": self.last_attempt_timestamp or time.time(),
                "status_code": (
                    self.last_response.status_code
                    if self.last_response is not None
                    else None
                ),
                "error_text": self.last_error_text
                if self.last_error_text is not None
                else str(exc),
                "retry_after": self.last_retry_after_header,
            }
            cast(Any, exc).retry_metadata = metadata

        logger.error(
            "request_failed_after_retries",
            url=self.url,
            method=self.method,
            params=self.params,
            data_present=self.data_present,
            json_present=self.json_present,
            attempt=self.attempt,
            status_code=(
                self.last_response.status_code if self.last_response is not None else None
            ),
            retry_after=self.last_retry_after_header,
            error=str(exc) if exc is not None else None,
            exception_type=type(exc).__name__ if exc is not None else None,
            timestamp=self.last_attempt_timestamp or time.time(),
        )


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

        manager_strategies = [
            strategy
            for strategy in config.fallback_strategies
            if strategy in FALLBACK_MANAGER_SUPPORTED_STRATEGIES
        ]
        self.fallback_manager: FallbackManager | None = None
        if manager_strategies:
            self.fallback_manager = FallbackManager(strategies=manager_strategies)

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
        """Выполняет JSON-запрос с защитами: CB, rate-limit, retry, Retry-After и fallback."""

        def _parse_json(response: requests.Response) -> dict[str, Any]:
            payload = response.json()
            if isinstance(payload, dict):
                return payload
            return cast(dict[str, Any], payload)

        return self._request(
            url=url,
            params=params,
            method=method,
            data=data,
            json_payload=json,
            cacheable=True,
            response_parser=_parse_json,
            stream=False,
        )

    def request_text(
        self,
        url: str,
        params: dict[str, Any] | None = None,
        method: str = "GET",
        data: dict[str, Any] | None = None,
        json: dict[str, Any] | None = None,
        *,
        stream: bool = False,
        encoding: str | None = None,
        chunk_size: int = 8192,
    ) -> str | Iterator[str]:
        """Выполняет запрос и возвращает текстовый ответ либо поток строк."""

        def _parse_text(response: requests.Response) -> str | Iterator[str]:
            if encoding:
                response.encoding = encoding
            elif response.encoding is None:
                try:
                    response.encoding = response.apparent_encoding
                except Exception:
                    response.encoding = "utf-8"

            if stream:
                line_iterator = response.iter_lines(
                    decode_unicode=True, chunk_size=chunk_size
                )
                return iter(line_iterator)

            return response.text

        return self._request(
            url=url,
            params=params,
            method=method,
            data=data,
            json_payload=json,
            cacheable=not stream,
            response_parser=_parse_text,
            stream=stream,
        )

    def _request(
        self,
        *,
        url: str,
        params: dict[str, Any] | None,
        method: str,
        data: dict[str, Any] | None,
        json_payload: dict[str, Any] | None,
        cacheable: bool,
        response_parser: Callable[[requests.Response], PayloadT],
        stream: bool,
    ) -> PayloadT:
        """Generic request helper with shared retry, fallback and caching logic."""

        if not url.startswith("http"):
            base_url = self.config.base_url.rstrip("/")
            relative_path = url.lstrip("/")

            # ``urllib.parse.urljoin`` normalizes the path which breaks endpoints that
            # intentionally embed another scheme, e.g. ``/works/https://doi.org/...``
            # used by the OpenAlex API.  Rely on deterministic string concatenation
            # to preserve the relative path exactly as provided by the adapter.
            url = f"{base_url}/{relative_path}"

        query_params = copy.deepcopy(params) if params is not None else None
        data_payload = data
        request_has_body = bool(data_payload or json_payload)
        request_has_no_body = not request_has_body

        cache = self.cache
        use_cache = (
            cacheable
            and cache is not None
            and method == "GET"
            and request_has_no_body
            and not stream
        )

        cache_key: str | None = None
        if use_cache:
            assert cache is not None
            cache_key = self._cache_key(url, query_params)
            if cache_key in cache:
                logger.debug("cache_hit", url=url)
                cached_value: PayloadT = cast(PayloadT, cache[cache_key])
                return self._clone_payload(cached_value)

        context = _RequestRetryContext(
            client=self,
            url=url,
            method=method,
            params=query_params,
            data_present=data_payload is not None,
            json_present=json_payload is not None,
        )

        def _perform_request() -> requests.Response:
            return self._execute(
                method=method,
                url=url,
                params=query_params,
                data=data_payload,
                json=json_payload,
                stream=stream,
            )

        def _request_operation() -> PayloadT:
            context.start_attempt()
            try:
                try:
                    response = self.circuit_breaker.call(_perform_request)
                except RequestException as exc:
                    context.record_failure(exc)
                    raise

                try:
                    response.raise_for_status()
                except HTTPError as exc:
                    context.record_failure(exc)
                    raise

                try:
                    payload = response_parser(response)
                except Exception as exc:
                    context.record_unhandled_exception(exc)
                    raise

                return payload
            except Exception:
                raise

        max_tries = max(1, self.retry_policy.total)

        backoff_decorated = backoff.on_exception(
            wait_gen=backoff.constant,
            interval=0,
            exception=(RequestException,),
            max_tries=max_tries,
            giveup=context.should_giveup,
            on_backoff=context.on_backoff,
            on_giveup=context.on_giveup,
            logger=None,
        )(_request_operation)

        try:
            payload = backoff_decorated()
        except RequestException as exc:
            if not self.config.fallback_enabled:
                raise

            payload = self._apply_fallback_strategies(
                context=context,
                cache_key=cache_key,
                request_has_no_body=request_has_no_body,
                request_operation=_request_operation,
                last_exception=exc,
            )

        if use_cache and cache_key is not None and cache is not None:
            cache[cache_key] = self._clone_payload(payload)

        return payload

    def _apply_fallback_strategies(
        self,
        *,
        context: _RequestRetryContext,
        cache_key: str | None,
        request_has_no_body: bool,
        request_operation: Callable[[], PayloadT],
        last_exception: RequestException,
    ) -> PayloadT:
        """Execute configured fallback strategies in order."""

        strategies: Sequence[str] = self.config.fallback_strategies
        cache: TTLCache[str, Any] | None = self.cache
        last_error: RequestException = last_exception

        for strategy in strategies:
            if strategy == "cache":
                if cache is not None and cache_key and request_has_no_body and cache_key in cache:
                    logger.warning(
                        "fallback_cache_hit",
                        url=context.url,
                        method=context.method,
                    )
                    cached_value: PayloadT = cast(PayloadT, cache[cache_key])
                    return self._clone_payload(cached_value)

                logger.debug(
                    "fallback_cache_miss",
                    url=context.url,
                    method=context.method,
                    cache_key_present=cache_key is not None,
                    cache_configured=cache is not None,
                )
                continue

            if strategy == "partial_retry":
                try:
                    return self._fallback_partial_retry(
                        context=context,
                        request_operation=request_operation,
                        max_attempts=self.config.partial_retry_max,
                        last_exception=last_error,
                    )
                except RequestException as exc:
                    last_error = exc
                    continue

            if strategy in FALLBACK_MANAGER_SUPPORTED_STRATEGIES:
                payload = self._fallback_via_manager(
                    strategy=strategy,
                    context=context,
                    last_exception=last_error,
                )
                if payload is not None:
                    return payload
                continue

            logger.warning(
                "fallback_strategy_unknown",
                strategy=strategy,
                url=context.url,
                method=context.method,
            )

        raise last_error

    def _fallback_partial_retry(
        self,
        *,
        context: _RequestRetryContext,
        request_operation: Callable[[], PayloadT],
        max_attempts: int,
        last_exception: RequestException,
    ) -> PayloadT:
        """Perform partial retry attempts after primary retries are exhausted."""

        if max_attempts <= 0:
            logger.debug(
                "fallback_partial_retry_disabled",
                url=context.url,
                method=context.method,
                max_attempts=max_attempts,
            )
            if context.last_exc is not None:
                raise context.last_exc
            raise last_exception

        logger.warning(
            "fallback_partial_retry_start",
            url=context.url,
            method=context.method,
            max_attempts=max_attempts,
            attempt=context.attempt,
        )

        last_error: RequestException | None = context.last_exc

        attempts_remaining = max(max_attempts, 0)

        while attempts_remaining > 0:
            wait_time = context.wait_time
            if wait_time and wait_time > 0:
                logger.debug(
                    "fallback_partial_retry_sleep",
                    wait_seconds=wait_time,
                    attempt=context.attempt,
                )
                time.sleep(wait_time)

            try:
                payload = request_operation()
            except RequestException as exc:
                last_error = exc
                attempts_remaining -= 1
                if attempts_remaining == 0:
                    break
                continue

            logger.info(
                "fallback_partial_retry_success",
                url=context.url,
                method=context.method,
                attempt=context.attempt,
            )
            return payload

        logger.error(
            "fallback_partial_retry_failed",
            url=context.url,
            method=context.method,
            max_attempts=max_attempts,
            attempt=context.attempt,
            error=str(last_error) if last_error is not None else None,
        )

        if last_error is not None:
            raise last_error

        raise RequestException("partial retry failed without exception")

    def _fallback_via_manager(
        self,
        *,
        strategy: str,
        context: _RequestRetryContext,
        last_exception: RequestException,
    ) -> PayloadT | None:
        """Delegate fallback creation to :class:`FallbackManager` when configured."""

        manager = self.fallback_manager
        if manager is None:
            logger.debug(
                "fallback_manager_not_configured",
                strategy=strategy,
                url=context.url,
                method=context.method,
            )
            return None

        error = context.last_exc or last_exception
        resolved_strategy = manager.get_strategy_for_error(error)
        if resolved_strategy is None:
            logger.debug(
                "fallback_manager_strategy_not_applicable",
                configured=list(manager.strategies),
                strategy=strategy,
                url=context.url,
                method=context.method,
            )
            return None

        if resolved_strategy != strategy:
            logger.debug(
                "fallback_manager_strategy_mismatch",
                resolved_strategy=resolved_strategy,
                requested_strategy=strategy,
                url=context.url,
                method=context.method,
            )
            return None

        def _raise_error() -> PayloadT:
            raise error

        payload = manager.execute_with_fallback(
            _raise_error,
            fallback_data=lambda: self._build_manager_fallback_payload(
                strategy=strategy,
                context=context,
                error=error,
            ),
        )

        return cast(PayloadT, payload)

    def _build_manager_fallback_payload(
        self,
        *,
        strategy: str,
        context: _RequestRetryContext,
        error: RequestException,
    ) -> dict[str, Any]:
        """Construct deterministic fallback payload for manager-handled errors."""

        response = getattr(error, "response", None)
        status_code: int | None = None
        if response is not None:
            try:
                status_code = int(getattr(response, "status_code", None))
            except (TypeError, ValueError):  # pragma: no cover - defensive
                status_code = None

        retry_after_seconds = context.last_retry_after_seconds
        if retry_after_seconds is None:
            retry_after_source: float | int | str | None = context.last_retry_after_header
            if retry_after_source is None and response is not None and response.headers:
                retry_after_source = response.headers.get("Retry-After")
            retry_after_seconds = parse_retry_after(retry_after_source)

        attempt = context.attempt if context.attempt > 0 else None
        error_text = context.last_error_text or str(error)
        message = (error_text or str(error) or "Fallback triggered").strip()
        fallback_label = f"{self.config.name.upper()}_FALLBACK" if self.config.name else "FALLBACK"

        payload: dict[str, Any] = {
            "source_system": fallback_label,
            "fallback_reason": strategy,
            "fallback_error_type": type(error).__name__,
            "fallback_error_code": strategy,
            "fallback_error_message": message,
            "fallback_http_status": status_code,
            "fallback_retry_after_sec": retry_after_seconds,
            "fallback_attempt": attempt,
            "fallback_timestamp": _current_utc_time().isoformat(),
        }

        return payload

    @staticmethod
    def _retry_after_seconds(response: requests.Response | None) -> float | None:
        """Parse Retry-After header into seconds if possible."""

        if response is None or not response.headers:
            return None

        retry_after = response.headers.get("Retry-After")
        if not retry_after:
            return None

        parsed_seconds = parse_retry_after(retry_after)
        if parsed_seconds is None:
            logger.debug(
                "retry_after_parse_failed",
                retry_after_raw=retry_after,
            )
            return None

        logger.debug(
            "retry_after_parsed",
            retry_after_raw=retry_after,
            wait_seconds=parsed_seconds,
        )
        return parsed_seconds

    def _execute(
        self,
        *,
        method: str,
        url: str,
        params: dict[str, Any] | None = None,
        data: dict[str, Any] | None = None,
        json: dict[str, Any] | None = None,
        stream: bool = False,
    ) -> requests.Response:
        """Execute a single HTTP request respecting Retry-After semantics."""

        self.rate_limiter.acquire()

        request_kwargs: dict[str, Any] = {
            "method": method,
            "url": url,
            "params": params,
            "data": data,
            "json": json,
            "timeout": (self.config.timeout_connect, self.config.timeout_read),
            "stream": stream,
        }

        response = self.session.request(**request_kwargs)

        if response.status_code != 429:
            return response

        retry_after_seconds = self._retry_after_seconds(response)
        retry_after_raw = response.headers.get("Retry-After") if response.headers else None

        if retry_after_seconds is None:
            if retry_after_raw:
                logger.warning(
                    "retry_after_header_invalid",
                    retry_after_raw=retry_after_raw,
                )
            return response

        logger.warning(
            "retry_after_header",
            wait_seconds=retry_after_seconds,
            retry_after_raw=retry_after_raw,
        )

        time.sleep(retry_after_seconds)

        # Re-acquire the limiter token before the follow-up request.
        self.rate_limiter.acquire()

        retry_request_kwargs = request_kwargs.copy()
        response = self.session.request(**retry_request_kwargs)
        return response

    @staticmethod
    def _clone_payload(payload: PayloadT) -> PayloadT:
        """Return a deep copy of payload, avoiding shared mutable state."""

        if isinstance(payload, (str, bytes, int, float, bool)) or payload is None:
            return cast(PayloadT, payload)

        try:
            return copy.deepcopy(payload)
        except Exception:
            try:
                cloned = json.loads(json.dumps(payload))
            except Exception:
                return payload

            return cast(PayloadT, cloned)

    def _cache_key(self, url: str, params: dict[str, Any] | None) -> str:
        """Generate cache key for request."""

        def _stringify(value: Any) -> Any:
            try:
                if isinstance(value, datetime):
                    return value.isoformat()

                if isinstance(value, set):
                    try:
                        sorted_items = sorted(value, key=lambda item: repr(item))
                    except Exception:
                        return repr(value)
                    return [_stringify(item) for item in sorted_items]

                if isinstance(value, (list, tuple)):
                    return [_stringify(item) for item in value]

                if isinstance(value, dict):
                    def _sort_key_dict_item(item: tuple[Any, Any]) -> tuple[str, str]:
                        key_obj = item[0]
                        # Детерминированная сортировка: тип + repr
                        return (type(key_obj).__name__, repr(key_obj))

                    # Представляем словарь как список пар [key, value] с сохранением типов ключей
                    items = [
                        [_stringify(k), _stringify(v)] for k, v in value.items()
                    ]
                    items.sort(key=lambda pair: (type(pair[0]).__name__, repr(pair[0])))
                    return items

                return value
            except Exception:
                return repr(value)

        key_parts = [url]
        if params:
            def _sort_key_top(item: tuple[Any, Any]) -> tuple[str, str]:
                k = item[0]
                return (type(k).__name__, repr(k))

            # Нормализуем ключи и значения с сохранением типов ключей
            normalized_params = [
                (_stringify(key), _stringify(value)) for key, value in params.items()
            ]
            normalized_params.sort(key=lambda item: (type(item[0]).__name__, repr(item[0])))
            params_str = json.dumps(normalized_params, sort_keys=True, separators=(",", ":"))
            key_parts.append(params_str)
        return hashlib.sha256("|".join(key_parts).encode()).hexdigest()

    def close(self) -> None:
        """Close session and cleanup resources."""
        self.session.close()

