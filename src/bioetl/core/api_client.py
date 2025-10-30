"""UnifiedAPIClient: resilient HTTP client with circuit breaker, rate limiting, retry policies."""

import copy
import hashlib
import json
import random
import threading
import time
from collections.abc import Callable, Iterable
from dataclasses import dataclass, field
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from typing import Any, cast
from urllib.parse import urljoin

import requests
from cachetools import TTLCache  # type: ignore

from bioetl.core.logger import UnifiedLogger

logger = UnifiedLogger.get(__name__)


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
        """Выполняет JSON-запрос с защитами: CB, rate-limit, retry, Retry-After и fallback.

        Порядок:
        1) Построение URL, проверка кэша для GET без тела.
        2) Запрос с учётом circuit breaker и rate limiter.
        3) Повторы по политике `RetryPolicy` (429: учитывается Retry-After).
        4) Если повторы исчерпаны и `config.fallback_enabled=True`, запускаются стратегии
           по порядку из `config.fallback_strategies`:
           - "cache": вернуть ответ из кэша (только для GET без тела), если доступен.
           - "partial_retry": выполнить до `config.partial_retry_max` дополнительных попыток
             с той же семантикой ожиданий (Retry-After, backoff). При успехе — кэшировать.
        5) При неудаче после всех стратегий пробрасывается исходное исключение.

        Возвращает:
            dict: разобранный JSON-ответ.
        """
        # Build full URL
        if not url.startswith("http"):
            base_url = self.config.base_url.rstrip("/") + "/"
            relative_path = url.lstrip("/")
            url = urljoin(base_url, relative_path)

        query_params = copy.deepcopy(params) if params is not None else None
        data_payload = data
        json_payload = json
        request_has_body = bool(data_payload or json_payload)
        request_has_no_body = not request_has_body

        # Check cache for GET requests
        cache_key: str | None = None
        if self.cache and method == "GET" and request_has_no_body:
            cache_key = self._cache_key(url, query_params)
            if cache_key in self.cache:
                logger.debug("cache_hit", url=url)
                cached_value: dict[str, Any] = self.cache[cache_key]
                return self._clone_payload(cached_value)

        # Execute with circuit breaker
        def _perform_request() -> requests.Response:
            return self._execute(
                method=method,
                url=url,
                params=query_params,
                data=data_payload,
                json=json_payload,
            )

        # Retry logic
        last_exc: Exception | None = None
        last_response: requests.Response | None = None
        last_attempt_timestamp: float | None = None
        last_retry_after_header: str | None = None
        last_error_text: str | None = None
        last_attempt = 0

        for attempt in range(1, self.retry_policy.total + 1):
            try:
                response = self.circuit_breaker.call(_perform_request)
                response.raise_for_status()

                # Parse JSON
                payload: dict[str, Any] = response.json()

                # Cache result
                if (
                    self.cache
                    and cache_key
                    and method == "GET"
                    and request_has_no_body
                ):
                    self.cache[cache_key] = self._clone_payload(payload)

                return payload

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
                if retry_after_seconds is not None and wait_time > 0:
                    time.sleep(wait_time)

            except requests.exceptions.RequestException as e:
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
                    logger.error(
                        "request_exception_giveup",
                        attempt=attempt,
                        url=url,
                        method=method,
                        params=query_params,
                        error=str(e),
                        exception_type=type(e).__name__,
                        status_code=(
                            last_response.status_code
                            if last_response is not None
                            else None
                        ),
                    )
                    break

                retry_after_seconds = self._retry_after_seconds(last_response)
                wait_time = self.retry_policy.get_wait_time(
                    attempt,
                    retry_after=retry_after_seconds,
                )
                logger.warning(
                    "retrying_request_exception",
                    attempt=attempt,
                    wait_seconds=wait_time,
                    error=str(e),
                    exception_type=type(e).__name__,
                    status_code=(
                        last_response.status_code if last_response is not None else None
                    ),
                    retry_after=last_retry_after_header,
                )
                if retry_after_seconds is not None and wait_time > 0:
                    time.sleep(wait_time)

            except Exception as e:
                last_exc = e
                logger.error(
                    "request_error",
                    attempt=attempt,
                    url=url,
                    method=method,
                    params=query_params,
                    error=str(e),
                )
                raise

        if last_exc:
            # Попытка fallback-стратегий после исчерпания основных ретраев
            if self.config.fallback_enabled:
                strategies = list(self.config.fallback_strategies or [])
                for strategy in strategies:
                    # Стратегия: cache
                    if strategy == "cache":
                        if (
                            self.cache
                            and cache_key
                            and method == "GET"
                            and request_has_no_body
                            and cache_key in self.cache
                        ):
                            logger.info(
                                "fallback_strategy_selected",
                                strategy="cache",
                                url=url,
                                method=method,
                                attempt=last_attempt,
                                status_code=(
                                    last_response.status_code if last_response is not None else None
                                ),
                                reason="cache_hit",
                            )
                            cached_value_fb: dict[str, Any] = self.cache[cache_key]
                            return self._clone_payload(cached_value_fb)
                        else:
                            logger.info(
                                "fallback_strategy_skipped",
                                strategy="cache",
                                url=url,
                                method=method,
                                attempt=last_attempt,
                                status_code=(
                                    last_response.status_code if last_response is not None else None
                                ),
                                reason="cache_miss_or_not_applicable",
                            )

                    # Стратегия: partial_retry
                    elif strategy == "partial_retry":
                        max_extra = int(max(0, self.config.partial_retry_max))
                        if max_extra <= 0:
                            logger.info(
                                "fallback_strategy_skipped",
                                strategy="partial_retry",
                                url=url,
                                method=method,
                                attempt=last_attempt,
                                status_code=(
                                    last_response.status_code if last_response is not None else None
                                ),
                                reason="partial_retry_max=0",
                            )
                        else:
                            logger.info(
                                "fallback_strategy_selected",
                                strategy="partial_retry",
                                url=url,
                                method=method,
                                attempt=last_attempt,
                                status_code=(
                                    last_response.status_code if last_response is not None else None
                                ),
                                reason="extra_attempts",
                                extra_attempts=max_extra,
                            )

                            # Выполняем дополнительные попытки
                            for extra_idx in range(1, max_extra + 1):
                                try:
                                    response = self.circuit_breaker.call(_perform_request)
                                    response.raise_for_status()
                                    payload = response.json()
                                    if (
                                        self.cache
                                        and cache_key
                                        and method == "GET"
                                        and request_has_no_body
                                    ):
                                        self.cache[cache_key] = self._clone_payload(payload)
                                    return payload
                                except requests.exceptions.HTTPError as e:
                                    last_exc = e
                                    last_response = getattr(e, "response", None)
                                    retry_after_seconds = self._retry_after_seconds(last_response)
                                    wait_time = self.retry_policy.get_wait_time(
                                        extra_idx,
                                        retry_after=retry_after_seconds,
                                    )
                                    logger.warning(
                                        "partial_retry_http_error",
                                        extra_attempt=extra_idx,
                                        wait_seconds=wait_time,
                                        error=str(e),
                                        status_code=(
                                            last_response.status_code if last_response is not None else None
                                        ),
                                        retry_after=(
                                            last_response.headers.get("Retry-After")
                                            if last_response is not None and last_response.headers
                                            else None
                                        ),
                                    )
                                    if retry_after_seconds is not None and wait_time > 0:
                                        time.sleep(wait_time)
                                    continue
                                except requests.exceptions.RequestException as e:
                                    last_exc = e
                                    retry_after_seconds = self._retry_after_seconds(
                                        getattr(e, "response", None)
                                    )
                                    wait_time = self.retry_policy.get_wait_time(
                                        extra_idx,
                                        retry_after=retry_after_seconds,
                                    )
                                    logger.warning(
                                        "partial_retry_request_exception",
                                        extra_attempt=extra_idx,
                                        wait_seconds=wait_time,
                                        error=str(e),
                                        exception_type=type(e).__name__,
                                    )
                                    if retry_after_seconds is not None and wait_time > 0:
                                        time.sleep(wait_time)
                                    continue
                                except Exception as e:
                                    last_exc = e
                                    logger.error(
                                        "partial_retry_unexpected_error",
                                        extra_attempt=extra_idx,
                                        error=str(e),
                                    )
                                    break

                    else:
                        # Неизвестная стратегия — пропускаем с логом
                        logger.info(
                            "fallback_strategy_unknown",
                            strategy=strategy,
                            url=url,
                            method=method,
                            attempt=last_attempt,
                        )

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
                last_exc.retry_metadata = metadata  # type: ignore[attr-defined]
            logger.error(
                "request_failed_after_retries",
                url=url,
                method=method,
                params=query_params,
                data_present=data_payload is not None,
                json_present=json_payload is not None,
                attempt=last_attempt,
                status_code=(
                    last_response.status_code if last_response is not None else None
                ),
                retry_after=last_retry_after_header,
                error=str(last_exc),
                exception_type=type(last_exc).__name__,
                timestamp=last_attempt_timestamp or time.time(),
            )
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
    def _clone_payload(payload: dict[str, Any]) -> dict[str, Any]:
        """Return a deep copy of payload, avoiding shared mutable state."""

        try:
            return copy.deepcopy(payload)
        except Exception:
            # ``requests`` can sometimes return objects that are not fully deepcopyable.
            # Fall back to JSON round-trip cloning as a safe guard.
            return cast(dict[str, Any], json.loads(json.dumps(payload)))

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

