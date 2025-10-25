"""Simplified base utilities for HTTP clients."""

from __future__ import annotations

import logging
from collections.abc import MutableMapping
from typing import TYPE_CHECKING, Any
from urllib.parse import urljoin

import requests
from requests import Response

<<<<<<< Updated upstream
from library.clients.circuit_breaker import APICircuitBreaker
from library.clients.exceptions import ApiClientError, RateLimitError
from library.clients.fallback import AdaptiveFallbackStrategy, FallbackConfig, FallbackManager
from library.clients.graceful_degradation import get_degradation_manager
from library.clients.session import get_shared_session
from library.config import APIClientConfig
from library.logging_setup import get_logger
from library.telemetry import add_span_attribute
=======
from library.settings import APIClientConfig
>>>>>>> Stashed changes

from ..clients.session import get_shared_session

if TYPE_CHECKING:
    from library.common.rate_limiter import RateLimiter

logger = logging.getLogger(__name__)


class BaseApiClient:
    """Simplified base functionality for HTTP clients."""

    def __init__(
        self,
        config: APIClientConfig,
        *,
        session: requests.Session | None = None,
        rate_limiter: RateLimiter | None = None,
        timeout: float | tuple[float, float] | None = None,
        max_retries: int = 3,
        default_headers: MutableMapping[str, str] | None = None,
    ) -> None:
        self.config = config
        self.base_url = config.base_url.rstrip("/")
        self.session = session or get_shared_session()
<<<<<<< Updated upstream
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
            fallback_strategy = AdaptiveFallbackStrategy(fallback_config)
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
=======

        # Rate limiting
        if rate_limiter is None:
            # Use default rate limiting based on service
            from ..common.rate_limiter import get_limiter

            if "pubmed" in self.base_url.lower():
                self.rate_limiter = get_limiter("pubmed", 3.0, 5)
            elif "crossref" in self.base_url.lower():
                self.rate_limiter = get_limiter("crossref", 5.0, 10)
            elif "semanticscholar" in self.base_url.lower():
                self.rate_limiter = get_limiter("semanticscholar", 2.0, 3)
            else:
                self.rate_limiter = get_limiter("default", 5.0, 10)
        else:
            self.rate_limiter = rate_limiter

        # Timeout configuration
        if timeout is not None:
            self.timeout = timeout
        else:
            # Use config timeouts
            self.timeout = (config.timeout_connect, config.timeout_read)

        self.max_retries = max_retries

        # Headers
        self.default_headers = {**config.headers} if hasattr(config, "headers") else {}
        if default_headers:
            self.default_headers.update(default_headers)
>>>>>>> Stashed changes

    def _make_url(self, path: str) -> str:
        """Build full URL from path."""
        if not path:
            return str(self.base_url)
        if path.startswith(("http://", "https://")):
            return path
        normalized = path.lstrip("/")
        return str(urljoin(self.base_url + "/", f"./{normalized}"))

<<<<<<< Updated upstream
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
                # Для ошибок соединения продолжаем попытки
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
                    self.logger.warning(f"Rate limit hit: {error_msg}")
                    # Use config-based delay instead of hardcoded delay
                    if hasattr(self.config, 'rate_limit') and self.config.rate_limit:
                        delay = self.config.rate_limit.period + random.uniform(0, 1)  # noqa: S311
                    else:
                        delay = 5.0 + random.uniform(0, 1)  # Default 5-6 seconds with jitter  # noqa: S311
                    time.sleep(delay)
                    self.rate_limiter.acquire()
            
            return _call()

        wait_gen = partial(backoff.expo, factor=self.config.retries.backoff_multiplier)
        sender = backoff.on_exception(
            wait_gen,
            requests.exceptions.RequestException,
            max_tries=self.max_retries,
            giveup=_giveup,
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

=======
>>>>>>> Stashed changes
    def _request(
        self,
        method: str,
        path: str,
        **kwargs: Any,
    ) -> Response:
        """Make HTTP request with rate limiting and retry logic."""
        url = self._make_url(path)

        # Apply rate limiting
        if self.rate_limiter:
            self.rate_limiter.acquire()

<<<<<<< Updated upstream
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
=======
        # Set timeout
        if "timeout" not in kwargs:
            kwargs["timeout"] = self.timeout

        # Set headers
        headers = kwargs.get("headers", {})
        headers.update(self.default_headers)
        kwargs["headers"] = headers
>>>>>>> Stashed changes

        # Make request with retry
        last_exception = None
        for attempt in range(self.max_retries + 1):
            try:
                response = self.session.request(method, url, **kwargs)
                response.raise_for_status()
                return response
            except requests.exceptions.RequestException as e:
                last_exception = e
                if attempt == self.max_retries:
                    logger.error(f"Request failed after {self.max_retries} retries: {e}")
                    raise
                logger.warning(f"Request attempt {attempt + 1} failed: {e}, retrying...")
                continue

<<<<<<< Updated upstream
        if response.status_code != expected_status:
            # Ограничиваем вывод текста ответа для HTML ошибок
            response_text = response.text
            if response_text.startswith('<!DOCTYPE html>') or response_text.startswith('<html'):
                response_text = f"HTML error page ({len(response_text)} characters)"
            
            self.logger.warning(
                "unexpected_status",
                status_code=response.status_code,
                text=response_text,
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
                        # Используем время из заголовка Retry-After, но ограничиваем максимум
                        max_wait = 60  # Максимум 1 минута
                        wait_time = min(wait_time, max_wait)
                        self.logger.info(f"Rate limited. Waiting {wait_time} seconds before retry.")
                        time.sleep(wait_time)
                    except (ValueError, TypeError):
                        # Экранируем символы % в сообщениях об ошибках для безопасного логирования
                        retry_after_msg = str(retry_after).replace("%", "%%")
                        self.logger.warning(f"Invalid Retry-After header: {retry_after_msg}")
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
            self.logger.debug(f"JSON response received: {type(payload)}")
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
                    # Use defusedxml for safe XML parsing
                    from defusedxml.ElementTree import fromstring as safe_fromstring
                    root = safe_fromstring(response.text)
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
=======
        # This should never be reached, but mypy needs it
        if last_exception:
            raise last_exception
        raise RuntimeError("Unexpected error in request retry loop")

    def get(self, path: str, **kwargs: Any) -> Response:
        """Make GET request."""
        return self._request("GET", path, **kwargs)

    def post(self, path: str, **kwargs: Any) -> Response:
        """Make POST request."""
        return self._request("POST", path, **kwargs)
>>>>>>> Stashed changes

    def put(self, path: str, **kwargs: Any) -> Response:
        """Make PUT request."""
        return self._request("PUT", path, **kwargs)

    def delete(self, path: str, **kwargs: Any) -> Response:
        """Make DELETE request."""
        return self._request("DELETE", path, **kwargs)

    def close(self) -> None:
        """Close the session."""
        if hasattr(self.session, "close"):
            self.session.close()

    def __enter__(self) -> BaseApiClient:
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self.close()
