"""HTTP clients for publication sources."""

from __future__ import annotations

import os
import random
import time
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from importlib import metadata
from typing import Any
from urllib.parse import urljoin

import requests
import requests_cache

from library.utils.errors import ExtractionError
from library.utils.logging import bind_context, get_logger, log_error_to_file
from library.utils.rate_limit import CompositeRateLimiter, RateLimiter

_DEFAULT_CACHE_NAME = ".http_cache"
_DEFAULT_CACHE_EXPIRY = 24 * 3600
_DEFAULT_TIMEOUT = 30.0
_DEFAULT_MAX_ATTEMPTS = 5
_DEFAULT_BACKOFF_MIN = 1.0
_DEFAULT_BACKOFF_MAX = 60.0
_RETRYABLE_STATUS = {429, 500, 502, 503, 504}
_SESSION: requests.Session | None = None


def _resolve_user_agent() -> str:
    email = os.getenv("PUBLICATIONS_CONTACT_EMAIL", "data@example.com")
    try:
        version = metadata.version("bioactivity-data-pipeline")
    except metadata.PackageNotFoundError:
        version = "0.0.0"
    return f"project-publications-etl/{version} (+mailto:{email})"


def get_http_session(
    *, cache_name: str = _DEFAULT_CACHE_NAME, expire_after: int | None = _DEFAULT_CACHE_EXPIRY
) -> requests.Session:
    """Return a shared cached HTTP session with the correct headers."""

    global _SESSION
    if _SESSION is None:
        session = requests_cache.CachedSession(
            cache_name=cache_name,
            backend="sqlite",
            expire_after=expire_after,
            allowable_codes=(200, 203, 206),
            allowable_methods=("GET", "POST"),
            stale_if_error=True,
        )
        session.headers.update({"User-Agent": _resolve_user_agent()})
        _SESSION = session
    else:
        if expire_after is not None:
            _SESSION.cache.expire_after = expire_after
    return _SESSION


@dataclass(slots=True)
class ClientConfig:
    """Configuration for a publication client."""

    name: str
    base_url: str
    api_key: str | None = None
    rate_limit_per_second: float | None = None
    rate_limit_per_minute: int | None = None
    timeout: float = _DEFAULT_TIMEOUT
    cache_expiry: int | None = _DEFAULT_CACHE_EXPIRY
    max_attempts: int = _DEFAULT_MAX_ATTEMPTS
    backoff_min: float = _DEFAULT_BACKOFF_MIN
    backoff_max: float = _DEFAULT_BACKOFF_MAX
    extra_headers: Mapping[str, str] | None = None

    def derive_rate_per_second(self) -> float | None:
        if self.rate_limit_per_second:
            return float(self.rate_limit_per_second)
        if self.rate_limit_per_minute:
            return float(self.rate_limit_per_minute) / 60.0
        return None


class BasePublicationsClient:
    """Base class encapsulating resilient HTTP access."""

    def __init__(
        self,
        config: ClientConfig,
        *,
        session: requests.Session | None = None,
        global_limiter: RateLimiter | None = None,
    ) -> None:
        self.config = config
        self.session = session or get_http_session(expire_after=config.cache_expiry)
        self.logger = get_logger(config.name).bind(stage="extract", source=config.name)
        rate_per_second = config.derive_rate_per_second()
        self.rate_limiter = RateLimiter(rate_per_second=rate_per_second) if rate_per_second else None
        self._limiter = CompositeRateLimiter([global_limiter, self.rate_limiter])
        self.timeout = config.timeout
        self.max_attempts = config.max_attempts
        self.backoff_min = config.backoff_min
        self.backoff_max = config.backoff_max
        self.extra_headers = dict(config.extra_headers or {})

    def _request(
        self,
        endpoint: str,
        *,
        params: Mapping[str, Any] | None = None,
        json_payload: Any | None = None,
        method: str = "GET",
        context: Mapping[str, Any] | None = None,
    ) -> Any:
        """Perform an HTTP request with retries, caching, and logging."""

        url = urljoin(self.config.base_url.rstrip("/") + "/", endpoint.lstrip("/"))
        headers = dict(self.extra_headers)
        if self.config.api_key and "authorization" not in {key.lower() for key in headers}:
            headers["Authorization"] = f"Bearer {self.config.api_key}"

        attempt = 0
        backoff_interval = self.backoff_min
        bound_logger = bind_context(self.logger, **(context or {}))

        while attempt < self.max_attempts:
            attempt += 1
            self._limiter.throttle()
            try:
                response = self.session.request(
                    method,
                    url,
                    params=params,
                    json=json_payload,
                    headers=headers,
                    timeout=self.timeout,
                )
            except requests.RequestException as exc:  # pragma: no cover - network errors
                wait_time = min(self.backoff_max, backoff_interval)
                bound_logger.warning(
                    "request_error",
                    attempt=attempt,
                    wait=wait_time,
                    error=str(exc),
                    url=url,
                )
                if attempt >= self.max_attempts:
                    log_error_to_file(self.config.name, {
                        "url": url,
                        "error": str(exc),
                        "attempt": attempt,
                        **(context or {}),
                    })
                    raise ExtractionError(f"{self.config.name} request failed: {exc}") from exc
                self._sleep_with_jitter(wait_time)
                backoff_interval = min(self.backoff_max, backoff_interval * 2)
                continue

            if response.status_code in _RETRYABLE_STATUS:
                retry_after = self._retry_after(response)
                wait_time = max(backoff_interval, retry_after or 0.0)
                bound_logger.warning(
                    "request_retry",
                    attempt=attempt,
                    wait=wait_time,
                    status=response.status_code,
                    url=url,
                )
                if attempt >= self.max_attempts:
                    payload = {
                        "status": response.status_code,
                        "body": response.text[:500],
                        "url": url,
                        "attempt": attempt,
                        **(context or {}),
                    }
                    log_error_to_file(self.config.name, payload)
                    raise ExtractionError(
                        f"{self.config.name} retry limit reached with status {response.status_code}"
                    )
                self._sleep_with_jitter(wait_time)
                backoff_interval = min(self.backoff_max, backoff_interval * 2)
                continue

            if not response.ok:
                payload = {
                    "status": response.status_code,
                    "body": response.text[:500],
                    "url": url,
                    **(context or {}),
                }
                log_error_to_file(self.config.name, payload)
                raise ExtractionError(
                    f"{self.config.name} responded with {response.status_code}: {response.text[:200]}"
                )

            if response.status_code == 204:
                return None

            try:
                return response.json()
            except ValueError as exc:
                payload = {"url": url, "body": response.text[:500], **(context or {})}
                log_error_to_file(self.config.name, payload)
                raise ExtractionError(f"{self.config.name} returned invalid JSON") from exc

        raise ExtractionError(f"{self.config.name} request failed after {self.max_attempts} attempts")

    def _retry_after(self, response: requests.Response) -> float | None:
        value = response.headers.get("Retry-After")
        if not value:
            return None
        try:
            return float(value)
        except ValueError:
            try:
                dt = parsedate_to_datetime(value)
            except (TypeError, ValueError):  # pragma: no cover - defensive
                return None
            if dt is None:
                return None
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            now = datetime.now(timezone.utc)
            return max(0.0, (dt - now).total_seconds())
        return None

    def _sleep_with_jitter(self, base_interval: float) -> None:
        base = max(base_interval, 0.0)
        jitter = random.uniform(0, max(base * 0.5, 0.1)) if base > 0 else 0.0  # noqa: S311
        time.sleep(base + jitter)

    def fetch_publications(self, query: str) -> list[dict[str, Any]]:  # pragma: no cover - abstract
        """Fetch publications for the provided query."""

        raise NotImplementedError
