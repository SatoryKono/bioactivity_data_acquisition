"""Base HTTP client configuration."""
from __future__ import annotations

import random
import time
from dataclasses import dataclass
from typing import Any

import requests
from library import DEFAULT_CONTACT_EMAIL, __version__
from library.utils.errors import SourceRequestError
from library.utils.rate_limit import RateLimiter
from requests import Response
from requests_cache import CachedSession

DEFAULT_TIMEOUT = 30
RETRYABLE_STATUS = {429, 500, 502, 503, 504}


@dataclass(slots=True)
class SessionConfig:
    cache_name: str = ".http_cache"
    expire_after: int = 24 * 60 * 60
    allowable_methods: tuple[str, ...] = ("GET", "POST")
    timeout: int = DEFAULT_TIMEOUT
    global_rps: float = 5.0


class SessionManager:
    """Factory for cached HTTP sessions with shared rate limiter."""

    def __init__(self, config: SessionConfig | None = None, *, email: str | None = None) -> None:
        self.config = config or SessionConfig()
        self.session = CachedSession(
            cache_name=self.config.cache_name,
            backend="sqlite",
            allowable_methods=self.config.allowable_methods,
            expire_after=self.config.expire_after,
            stale_if_error=True,
        )
        user_agent = f"project-publications-etl/{__version__} (+mailto={email or DEFAULT_CONTACT_EMAIL})"
        self.session.headers.update({"User-Agent": user_agent})
        self.global_limiter = RateLimiter(self.config.global_rps)

    def request(self, method: str, url: str, **kwargs: Any) -> Response:
        with self.global_limiter.limit():
            return self.session.request(method, url, timeout=self.config.timeout, **kwargs)


class BaseClient:
    """Base class with retry, limiter and parsing helpers."""

    base_url: str = ""

    def __init__(
        self,
        session_manager: SessionManager,
        *,
        name: str,
        rate_per_sec: float,
    ) -> None:
        self.session_manager = session_manager
        self.name = name
        self.rate_limiter = RateLimiter(rate_per_sec)

    def _build_url(self, path: str) -> str:
        return f"{self.base_url}{path}"

    def _request(self, method: str, url: str, **kwargs: Any) -> Response:
        backoff = 1.0
        while True:
            with self.rate_limiter.limit():
                response = self.session_manager.request(method, url, **kwargs)
            if response.from_cache:  # type: ignore[attr-defined]
                return response
            if response.status_code in RETRYABLE_STATUS:
                retry_after = _parse_retry_after(response)
                wait = retry_after if retry_after is not None else backoff + random.uniform(0, backoff)
                time.sleep(wait)
                backoff = min(backoff * 2, 60)
                continue
            try:
                response.raise_for_status()
            except requests.HTTPError as exc:  # noqa: PERF203 - specific handling
                raise SourceRequestError(f"{self.name} request failed: {exc}") from exc
            return response

    def get_json(self, path: str, *, params: dict[str, Any] | None = None) -> dict[str, Any]:
        url = self._build_url(path)
        response = self._request("GET", url, params=params)
        try:
            return response.json()
        except ValueError as exc:  # noqa: PERF203
            raise SourceRequestError(f"{self.name} returned invalid JSON") from exc


def _parse_retry_after(response: Response) -> float | None:
    header = response.headers.get("Retry-After")
    if not header:
        return None
    try:
        return float(header)
    except ValueError:
        return None
