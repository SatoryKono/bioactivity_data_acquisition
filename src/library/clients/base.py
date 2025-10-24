"""Base utilities shared by all HTTP clients."""
from __future__ import annotations

import json
import random
import threading
import time
from collections import deque
from collections.abc import MutableMapping
from dataclasses import dataclass
from functools import partial
from typing import Any
from urllib.parse import urljoin

import backoff
import requests
from requests import Response

from library.clients.circuit_breaker import APICircuitBreaker
from library.clients.exceptions import ApiClientError, RateLimitError
from library.clients.fallback import FallbackConfig, FallbackManager, NetworkErrorFallbackStrategy
from library.clients.graceful_degradation import get_degradation_manager
from library.clients.session import get_shared_session
from library.config import APIClientConfig
from library.logging_setup import get_logger
from library.telemetry import add_span_attribute


@dataclass(frozen=True)
class RateLimitConfig:
    """Rate limit settings."""

    max_calls: int
    period: float


class RateLimiter:
    """A simple thread-safe rate limiter.

    The limiter keeps timestamps of recent calls and raises a ``RateLimitError``
    when the number of calls within ``period`` exceeds ``max_calls``.
    """

    def __init__(self, config: RateLimitConfig) -> None:
        self._config = config
        self._timestamps: deque[float] = deque()
        self._lock = threading.Lock()

    def acquire(self) -> None:
        now = time.monotonic()
        with self._lock:
            while self._timestamps and now - self._timestamps[0] >= self._config.period:
                self._timestamps.popleft()
            if len(self._timestamps) >= self._config.max_calls:
                raise RateLimitError(
                    f"rate limit exceeded: {self._config.max_calls} calls per "
                    f"{self._config.period}s"
                )
            self._timestamps.append(now)


class BaseApiClient:
    """Shared functionality for the service-specific clients."""

    def __init__(
        self,
        config: APIClientConfig,
        *,
        session: requests.Session | None = None,
        rate_limiter: RateLimiter | None = None,
        timeout: float = 10.0,
        max_retries: int = 3,
        default_headers: MutableMapping[str, str] | None = None,
        fallback_manager: FallbackManager | None = None,
        circuit_breaker: APICircuitBreaker | None = None,
    ) -> None:
        self.config = config
        self.base_url = config.resolved_base_url.rstrip("/")
        self.session = session or get_shared_session()
        limiter = config.rate_limit
        if limiter is not None and rate_limiter is None:
            rate_limiter = RateLimiter(RateLimitConfig(limiter.max_calls, limiter.period))
        self.rate_limiter = rate_limiter
        # Use config timeout, with API-specific defaults for slow APIs
        if config.timeout is not None:
            self.timeout = config.timeout
        else:
            # Set longer timeouts for slow APIs
            if 'semanticscholar' in self.base_url.lower():
                self.timeout = 60.0  # 60 seconds for Semantic Scholar
            elif 'pubmed' in self.base_url.lower():
                self.timeout = 45.0  # 45 seconds for PubMed
            else:
                self.timeout = 30.0  # 30 seconds default
        self.max_retries = (
            max_retries if max_retries is not None else max(1, config.retries.total)
        )
        self.default_headers = {**config.headers}
        if default_headers:
            self.default_headers.update(default_headers)
        self.logger = get_logger(self.__class__.__name__, base_url=self.base_url)
        
        # Initialize fallback manager if not provided
        if fallback_manager is None:
            fallback_config = FallbackConfig(
                max_retries=max(3, self.max_retries),
                base_delay=1.0,
                max_delay=60.0,
                backoff_multiplier=2.0,
                jitter=True
            )
            # Используем NetworkErrorFallbackStrategy для лучшей обработки DNS ошибок
            fallback_strategy = NetworkErrorFallbackStrategy(fallback_config)
            self.fallback_manager = FallbackManager(fallback_strategy)
        else:
            self.fallback_manager = fallback_manager
        
        # Initialize circuit breaker if not provided
        if circuit_breaker is None:
            self.circuit_breaker = APICircuitBreaker(self.config.name)
        else:
            self.circuit_breaker = circuit_breaker
        
        # Initialize degradation manager
        self.degradation_manager = get_degradation_manager()

    def _make_url(self, path: str) -> str:
        if not path:
            return str(self.base_url)
        if path.startswith(("http://", "https://")):
            return path
        normalized = path.lstrip("/")
        return str(urljoin(self.base_url + "/", f"./{normalized}"))

    def _send_with_backoff(self, method: str, url: str, **kwargs: Any) -> Response:
        def _call() -> Response:
            # Use config timeout if specified, otherwise rely on session default timeout
            request_timeout = self.timeout if self.timeout is not None else None
            return self.session.request(method, url, timeout=request_timeout, **kwargs)

        def _giveup(exc: Exception) -> bool:
            """Определяет, когда прекратить повторные попытки."""
            if isinstance(exc, requests.exceptions.HTTPError):
                # Для ошибок 429 (rate limiting) продолжаем повторные попытки
                if hasattr(exc, 'response') and exc.response is not None:
                    status_code = exc.response.status_code
                    if status_code == 429:
                        return False  # Не прекращаем попытки для 429
                    # Для 4xx ошибок (кроме 429) прекращаем попытки
                    elif 400 <= status_code < 500:
                        return True
                    # Для 5xx ошибок продолжаем попытки
                    elif 500 <= status_code < 600:
                        return False
                    # Для других HTTP ошибок прекращаем попытки
                    return True
                return True
            elif isinstance(exc, requests.exceptions.ConnectionError):
                # Проверяем тип ошибки соединения
                error_str = str(exc).lower()
                if "name resolution" in error_str or "getaddrinfo failed" in error_str:
                    # DNS ошибки - прекращаем попытки после нескольких попыток
                    return True
                elif "timeout" in error_str:
                    # Таймауты соединения - продолжаем попытки
                    return False
                else:
                    # Другие ошибки соединения - продолжаем попытки
                    return False
            elif isinstance(exc, requests.exceptions.Timeout):
                # Для таймаутов продолжаем попытки
                return False
            elif isinstance(exc, requests.exceptions.RequestException):
                # Для других HTTP исключений прекращаем попытки
                return True
            # Для других исключений продолжаем попытки
            return False

        def _call_with_rate_limit() -> Response:
            # Apply rate limiting before making request
            if self.rate_limiter is not None:
                try:
                    self.rate_limiter.acquire()
                except RateLimitError as e:
                    # If rate limited, wait and retry
                    # Экранируем символы % в сообщениях об ошибках для безопасного логирования
                    error_msg = str(e).replace("%", "%%")
                    self.logger.warning("Rate limit hit: {}", error_msg)
                    # Use config-based delay instead of hardcoded delay
                    if hasattr(self.config, 'rate_limit') and self.config.rate_limit:
                        delay = self.config.rate_limit.period + random.uniform(0, 1)  # noqa: S311
                    else:
                        delay = 5.0 + random.uniform(0, 1)  # Default 5-6 seconds with jitter  # noqa: S311
                    time.sleep(delay)
                    self.rate_limiter.acquire()
            
            return _call()

        wait_gen = partial(backoff.expo, factor=self.config.retries.backoff_multiplier)
        
        # Создаем безопасный обработчик для backoff
        def _safe_backoff_handler(details):
            """Безопасный обработчик для backoff без проблем с форматированием."""
            try:
                # Экранируем символы % в сообщениях
                safe_msg = str(details.get('message', '')).replace('%', '%%')
                
                # Логируем без использования форматирования
                self.logger.warning(
                    "Backing off %s(...) for %.1fs (%s)",
                    details.get('target', 'unknown'),
                    details.get('wait', 0),
                    safe_msg
                )
            except Exception as e:
                # Если что-то пошло не так с логированием, просто пропускаем
                self.logger.debug("Backoff handler error: %s", str(e))
        
        sender = backoff.on_exception(
            wait_gen,
            requests.exceptions.RequestException,
            max_tries=self.max_retries,
            giveup=_giveup,
            on_backoff=_safe_backoff_handler,
        )(_call_with_rate_limit)
        
        # Use circuit breaker to protect the request
        return self.circuit_breaker.call(sender)  # type: ignore

    def _request_with_fallback(
        self,
        method: str,
        path: str = "",
        *,
        expected_status: int = 200,
        headers: MutableMapping[str, str] | None = None,
        **kwargs: Any,
    ) -> dict[str, Any]:
        """Make a request with fallback strategy for handling errors."""
        def _make_request():
            return self._request(method, path, expected_status=expected_status, headers=headers, **kwargs)
        
        return self.fallback_manager.execute_with_fallback(_make_request)
    
    def _request_with_graceful_degradation(
        self,
        method: str,
        path: str = "",
        *,
        expected_status: int = 200,
        headers: MutableMapping[str, str] | None = None,
        original_request_data: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> dict[str, Any]:
        """Make a request with graceful degradation fallback."""
        try:
            return self._request_with_fallback(
                method, path, expected_status=expected_status, headers=headers, **kwargs
            )
        except ApiClientError as e:
            # Check if we should degrade gracefully
            if self.degradation_manager.should_degrade(self.config.name, e):
                self.logger.warning(
                    f"API {self.config.name} failed, using graceful degradation: {e}"
                )
                return self.degradation_manager.get_fallback_data(
                    self.config.name, 
                    original_request_data or {},
                    e
                )
            else:
                # Re-raise the error if degradation is not appropriate
                raise

    def _request(
        self,
        method: str,
        path: str = "",
        *,
        expected_status: int = 200,
        headers: MutableMapping[str, str] | None = None,
        **kwargs: Any,
    ) -> dict[str, Any]:
        url = self._make_url(path)
        request_headers = dict(self.default_headers)
        if headers:
            request_headers.update(headers)

        # Add tracing attributes
        add_span_attribute("http.method", method)
        add_span_attribute("http.url", url)
        add_span_attribute("api.client", self.config.name)
        add_span_attribute("http.expected_status", expected_status)

        # Экранируем символы % в параметрах для безопасного логирования
        safe_params = str(kwargs.get('params', '')).replace('%', '%%')
        safe_headers = str(request_headers or '').replace('%', '%%')
        safe_url = str(url).replace('%', '%%')
        
        self.logger.info(
            "request method=%s url=%s params=%s headers=%s base_url=%s", 
            method, safe_url, safe_params, safe_headers, self.base_url
        )

        try:
            response = self._send_with_backoff(method, url, headers=request_headers, **kwargs)
        except requests.exceptions.RequestException as exc:  # pragma: no cover - defensive
            self.logger.error("transport_error error={}", str(exc))
            add_span_attribute("error", True)
            add_span_attribute("error.type", "transport_error")
            raise ApiClientError(str(exc)) from exc

        # Add response attributes to span
        add_span_attribute("http.status_code", response.status_code)
        add_span_attribute("http.response_size", len(response.content))

        if response.status_code != expected_status:
            # Ограничиваем вывод текста ответа для HTML ошибок
            response_text = response.text
            if response_text.startswith('<!DOCTYPE html>') or response_text.startswith('<html'):
                response_text = f"HTML error page ({len(response_text)} characters)"
            
            self.logger.warning(
                f"unexpected_status status_code={response.status_code} text={response_text} expected_status={expected_status}"
            )
            add_span_attribute("error", True)
            add_span_attribute("error.type", "unexpected_status")
            
            # Специальная обработка для ошибок rate limiting (429)
            if response.status_code == 429:
                retry_after = response.headers.get('Retry-After')
                if retry_after:
                    try:
                        wait_time = int(retry_after)
                        # Используем время из заголовка Retry-After, но ограничиваем максимум
                        max_wait = 60  # Максимум 1 минута
                        wait_time = min(wait_time, max_wait)
                        self.logger.info("Rate limited. Waiting %d seconds before retry.", wait_time)
                        time.sleep(wait_time)
                    except (ValueError, TypeError):
                        # Экранируем символы % в сообщениях об ошибках для безопасного логирования
                        retry_after_msg = str(retry_after).replace("%", "%%")
                        self.logger.warning("Invalid Retry-After header: {}", retry_after_msg)
                        # Используем консервативную задержку 30 секунд
                        self.logger.info("Using conservative 30-second delay")
                        time.sleep(30)
                
                raise ApiClientError(
                    f"Rate limited by API (status {response.status_code}). "
                    f"Message: {response.text}",
                    status_code=response.status_code,
                )
            
            raise ApiClientError(
                f"unexpected status code {response.status_code}",
                status_code=response.status_code,
            )

        try:
            payload = response.json()
            self.logger.debug("JSON response received: {}", type(payload))
        except json.JSONDecodeError as exc:
            # Проверяем, не является ли ответ XML (например, от ChEMBL API)
            content_type = response.headers.get('content-type', '').lower()
            if 'xml' in content_type:
                self.logger.info(
                    f"xml_response_received content_type={content_type} url={url} message=API returned XML, attempting to parse"
                )
                # Пытаемся парсить XML
                try:
                    # Use defusedxml for safe XML parsing
                    from defusedxml.ElementTree import fromstring as safe_fromstring
                    root = safe_fromstring(response.text)
                    payload = self._xml_to_dict(root)
                except Exception as xml_exc:
                    self.logger.error("xml_parse_error error={}", str(xml_exc))
                    raise ApiClientError(f"Failed to parse XML response: {xml_exc}") from xml_exc
            else:
                self.logger.error("invalid_json error={} content_type={}", str(exc), content_type)
                raise ApiClientError("response was not valid JSON") from exc

        self.logger.info("response status_code=%d", response.status_code)
        if not isinstance(payload, dict):
            raise ApiClientError("expected JSON object from API")
        return payload

    def _xml_to_dict(self, element) -> dict[str, Any]:
        """Преобразует XML элемент в словарь."""
        result = {}
        
        # Обрабатываем атрибуты
        if element.attrib:
            result.update(element.attrib)
        
        # Обрабатываем дочерние элементы
        for child in element:
            child_dict = self._xml_to_dict(child)
            if child.tag in result:
                # Если ключ уже существует, создаем список
                if not isinstance(result[child.tag], list):
                    result[child.tag] = [result[child.tag]]
                result[child.tag].append(child_dict)
            else:
                result[child.tag] = child_dict
        
        # Если нет дочерних элементов и атрибутов, возвращаем текст
        if not result and element.text:
            return element.text.strip()
        
        return result
