"""Base utilities shared by all HTTP clients."""
from __future__ import annotations

import json
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

from library.clients.session import get_shared_session
from library.clients.fallback import FallbackManager, FallbackConfig, AdaptiveFallbackStrategy
from library.clients.exceptions import ApiClientError, RateLimitError
from library.config import APIClientConfig
from library.logger import get_logger
from library.telemetry import traced_operation, add_span_attribute, add_span_event


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
    ) -> None:
        self.config = config
        self.base_url = config.resolved_base_url.rstrip("/")
        self.session = session or get_shared_session()
        limiter = config.rate_limit
        if limiter is not None and rate_limiter is None:
            rate_limiter = RateLimiter(RateLimitConfig(limiter.max_calls, limiter.period))
        self.rate_limiter = rate_limiter
        # Use config timeout, session will use its default if not overridden
        self.timeout = config.timeout
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
            fallback_strategy = AdaptiveFallbackStrategy(fallback_config)
            self.fallback_manager = FallbackManager(fallback_strategy)
        else:
            self.fallback_manager = fallback_manager

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
                    if exc.response.status_code == 429:
                        return False  # Не прекращаем попытки для 429
                    # Для других HTTP ошибок прекращаем попытки
                    return True
                return True
            # Для других исключений продолжаем попытки
            return False

        wait_gen = partial(backoff.expo, factor=self.config.retries.backoff_multiplier)
        sender = backoff.on_exception(
            wait_gen,
            requests.exceptions.RequestException,
            max_tries=self.max_retries,
            giveup=_giveup,
        )(_call)
        return sender()  # type: ignore

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

    def _request(
        self,
        method: str,
        path: str = "",
        *,
        expected_status: int = 200,
        headers: MutableMapping[str, str] | None = None,
        **kwargs: Any,
    ) -> dict[str, Any]:
        if self.rate_limiter is not None:
            self.rate_limiter.acquire()
        url = self._make_url(path)
        request_headers = dict(self.default_headers)
        if headers:
            request_headers.update(headers)

        # Add tracing attributes
        add_span_attribute("http.method", method)
        add_span_attribute("http.url", url)
        add_span_attribute("api.client", self.config.name)
        add_span_attribute("http.expected_status", expected_status)

        self.logger.info(
            "request",
            method=method,
            url=url,
            params=kwargs.get("params"),
            headers=request_headers or None,
        )

        try:
            response = self._send_with_backoff(method, url, headers=request_headers, **kwargs)
        except requests.exceptions.RequestException as exc:  # pragma: no cover - defensive
            self.logger.error("transport_error", error=str(exc))
            add_span_attribute("error", True)
            add_span_attribute("error.type", "transport_error")
            raise ApiClientError(str(exc)) from exc

        # Add response attributes to span
        add_span_attribute("http.status_code", response.status_code)
        add_span_attribute("http.response_size", len(response.content))

        if response.status_code != expected_status:
            self.logger.warning(
                "unexpected_status",
                status_code=response.status_code,
                text=response.text,
                expected_status=expected_status,
            )
            add_span_attribute("error", True)
            add_span_attribute("error.type", "unexpected_status")
            
            # Специальная обработка для ошибок rate limiting (429)
            if response.status_code == 429:
                retry_after = response.headers.get('Retry-After')
                if retry_after:
                    try:
                        wait_time = int(retry_after)
                        # Увеличиваем время ожидания для Semantic Scholar
                        if 'semanticscholar' in self.base_url.lower():
                            wait_time = max(wait_time, 300)  # Минимум 5 минут для Semantic Scholar
                        self.logger.info(f"Rate limited. Waiting {wait_time} seconds before retry.")
                        time.sleep(wait_time)
                    except (ValueError, TypeError):
                        self.logger.warning(f"Invalid Retry-After header: {retry_after}")
                        # Для Semantic Scholar используем консервативную задержку
                        if 'semanticscholar' in self.base_url.lower():
                            self.logger.info("Using conservative 5-minute delay for Semantic Scholar")
                            time.sleep(300)
                
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
        except json.JSONDecodeError as exc:
            # Проверяем, не является ли ответ XML (например, от ChEMBL API)
            content_type = response.headers.get('content-type', '').lower()
            if 'xml' in content_type:
                self.logger.info(
                    "xml_response_received",
                    content_type=content_type,
                    url=url,
                    message="API returned XML, attempting to parse"
                )
                # Пытаемся парсить XML
                try:
                    import xml.etree.ElementTree as ET
                    root = ET.fromstring(response.text)
                    payload = self._xml_to_dict(root)
                except Exception as xml_exc:
                    self.logger.error("xml_parse_error", error=str(xml_exc))
                    raise ApiClientError(f"Failed to parse XML response: {xml_exc}") from xml_exc
            else:
                self.logger.error("invalid_json", error=str(exc), content_type=content_type)
                raise ApiClientError("response was not valid JSON") from exc

        self.logger.info("response", status_code=response.status_code)
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
