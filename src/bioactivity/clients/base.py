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

from bioactivity.clients.session import get_shared_session
from bioactivity.config import APIClientConfig
from bioactivity.logging import get_logger


class ApiClientError(RuntimeError):
    """Generic client error."""

    def __init__(self, message: str, *, status_code: int | None = None) -> None:
        super().__init__(message)
        self.status_code = status_code


class RateLimitError(ApiClientError):
    """Raised when the rate limiter rejects the request."""


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
    ) -> None:
        self.config = config
        self.base_url = config.resolved_base_url.rstrip("/")
        self.session = session or get_shared_session()
        limiter = config.rate_limit
        if limiter is not None and rate_limiter is None:
            rate_limiter = RateLimiter(RateLimitConfig(limiter.max_calls, limiter.period))
        self.rate_limiter = rate_limiter
        self.timeout = (
            timeout if hasattr(config, 'timeout') and config.timeout is not None 
            else config.timeout
        )
        self.max_retries = (
            max_retries if max_retries is not None 
            else max(1, config.retries.max_tries)
        )
        self.default_headers = {**config.headers}
        if default_headers:
            self.default_headers.update(default_headers)
        self.logger = get_logger(self.__class__.__name__, base_url=self.base_url)

    def _make_url(self, path: str) -> str:
        if not path:
            return self.base_url
        if path.startswith(("http://", "https://")):
            return path
        normalized = path.lstrip("/")
        return urljoin(self.base_url + "/", f"./{normalized}")

    def _send_with_backoff(self, method: str, url: str, **kwargs: Any) -> Response:
        def _call() -> Response:
            return self.session.request(method, url, timeout=self.timeout, **kwargs)

        wait_gen = partial(backoff.expo, factor=self.config.retries.backoff_multiplier)
        sender = backoff.on_exception(
            wait_gen,
            requests.exceptions.RequestException,
            max_tries=self.max_retries,
            giveup=lambda exc: isinstance(exc, requests.exceptions.HTTPError),
        )(_call)
        return sender()

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
            raise ApiClientError(str(exc)) from exc

        if response.status_code != expected_status:
            self.logger.warning(
                "unexpected_status",
                status_code=response.status_code,
                text=response.text,
                expected_status=expected_status,
            )
            raise ApiClientError(
                f"unexpected status code {response.status_code}",
                status_code=response.status_code,
            )

        try:
            payload = response.json()
        except json.JSONDecodeError as exc:
            self.logger.error("invalid_json", error=str(exc))
            raise ApiClientError("response was not valid JSON") from exc

        self.logger.info("response", status_code=response.status_code)
        if not isinstance(payload, dict):
            raise ApiClientError("expected JSON object from API")
        return payload
