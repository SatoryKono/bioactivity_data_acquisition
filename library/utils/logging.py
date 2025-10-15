"""Logging helpers and resilient HTTP session configuration."""

from __future__ import annotations

import logging
import os
import threading
import time
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from typing import Callable, Iterator, Optional

import backoff
import requests
import requests_cache

from library import __version__

LOGGER = logging.getLogger(__name__)

USER_AGENT_TEMPLATE = "project-publications-etl/{version} (+mailto={email})"
DEFAULT_CONTACT_EMAIL = os.getenv("PROJECT_PUBLICATIONS_CONTACT_EMAIL", "publications@example.com")
DEFAULT_CACHE_NAME = ".http_cache"
DEFAULT_CACHE_EXPIRE = 24 * 60 * 60  # 24 hours

_RETRYABLE_STATUS_CODES = {429, 500, 502, 503, 504}
_MAX_TRIES = 5
_SHARED_SESSION: Optional[requests.Session] = None
_SESSION_LOCK = threading.Lock()


class TransientHTTPError(requests.HTTPError):
    """HTTP error that signals the request may succeed after a retry."""

    def __init__(self, response: requests.Response, *, retry_after: Optional[float] = None) -> None:
        super().__init__(f"HTTP {response.status_code} for {response.url}", response=response)
        self.retry_after = retry_after


def _parse_retry_after(value: Optional[str]) -> Optional[float]:
    if not value:
        return None

    try:
        seconds = float(value)
        if seconds < 0:
            return None
        return seconds
    except ValueError:
        try:
            retry_dt = parsedate_to_datetime(value)
        except (TypeError, ValueError):
            return None
        if retry_dt.tzinfo is None:
            retry_dt = retry_dt.replace(tzinfo=timezone.utc)
        now = datetime.now(timezone.utc)
        delta = (retry_dt - now).total_seconds()
        return max(0.0, delta)


def _wrap_with_backoff(func: Callable[..., requests.Response]) -> Callable[..., requests.Response]:
    def next_delay(wait_gen: Iterator[float | None]) -> float:
        value = next(wait_gen)
        if value is None:
            return 0.0
        return float(value)

    def wrapper(*args, **kwargs):
        http_waits = backoff.expo()
        error_waits = backoff.expo()
        attempts = 0

        while True:
            try:
                response = func(*args, **kwargs)
            except requests.exceptions.RequestException:
                attempts += 1
                if attempts >= _MAX_TRIES:
                    raise
                delay = backoff.full_jitter(next_delay(http_waits))
                if delay > 0:
                    time.sleep(delay)
                continue

            status = response.status_code
            if status in _RETRYABLE_STATUS_CODES:
                retry_after = _parse_retry_after(response.headers.get("Retry-After"))
                attempts += 1
                if attempts >= _MAX_TRIES:
                    response.close()
                    raise TransientHTTPError(response, retry_after=retry_after)

                base_delay = next_delay(error_waits)
                jittered = backoff.full_jitter(base_delay)
                wait_time = max(jittered, retry_after or 0.0)
                if wait_time > 0:
                    time.sleep(wait_time)
                response.close()
                continue

            if 400 <= status:
                response.raise_for_status()
            return response

    return wrapper


def _build_shared_session(
    *,
    cache_name: str,
    cache_expire: int,
    version: str,
    email: str,
) -> requests.Session:
    if not email:
        raise ValueError("Contact email must be provided for the User-Agent header")

    ua = USER_AGENT_TEMPLATE.format(version=version, email=email)

    session = requests_cache.CachedSession(cache_name=cache_name, expire_after=cache_expire)
    session.headers.setdefault("User-Agent", ua)
    session.request = _wrap_with_backoff(session.request)  # type: ignore[assignment]
    LOGGER.debug("Configured cached HTTP session", cache=cache_name, expire_after=cache_expire)

    return session


def create_shared_session(
    *,
    cache_name: str = DEFAULT_CACHE_NAME,
    cache_expire: int = DEFAULT_CACHE_EXPIRE,
    version: str = __version__,
    email: str = DEFAULT_CONTACT_EMAIL,
) -> requests.Session:
    """Create (or reuse) the configured shared :class:`requests.Session`."""

    global _SHARED_SESSION

    session = _build_shared_session(
        cache_name=cache_name,
        cache_expire=cache_expire,
        version=version,
        email=email,
    )

    with _SESSION_LOCK:
        _SHARED_SESSION = session
    return session


def get_shared_session() -> requests.Session:
    """Return the shared HTTP session, creating it lazily if required."""

    global _SHARED_SESSION
    session = _SHARED_SESSION
    if session is not None:
        return session

    with _SESSION_LOCK:
        session = _SHARED_SESSION
        if session is None:
            session = _build_shared_session(
                cache_name=DEFAULT_CACHE_NAME,
                cache_expire=DEFAULT_CACHE_EXPIRE,
                version=__version__,
                email=DEFAULT_CONTACT_EMAIL,
            )
            _SHARED_SESSION = session
        return session


def reset_shared_session() -> None:
    """Dispose the cached shared session (useful in tests)."""

    global _SHARED_SESSION
    with _SESSION_LOCK:
        _SHARED_SESSION = None
