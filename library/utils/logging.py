"""Structured logging configuration and resilient HTTP session management for the ETL pipeline."""
from __future__ import annotations

import json
import logging
import os
import threading
import time
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import Any, Callable, Iterator, Optional

import backoff
import requests
import requests_cache
import structlog

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


def create_shared_session(
    *,
    cache_name: str = DEFAULT_CACHE_NAME,
    cache_expire: int = DEFAULT_CACHE_EXPIRE,
    version: str = __version__,
    email: str = DEFAULT_CONTACT_EMAIL,
) -> requests.Session:
    """Create (or reuse) the configured shared :class:`requests.Session`."""

    global _SHARED_SESSION

    if not email:
        raise ValueError("Contact email must be provided for the User-Agent header")

    ua = USER_AGENT_TEMPLATE.format(version=version, email=email)

    session = requests_cache.CachedSession(cache_name=cache_name, expire_after=cache_expire)
    session.headers.setdefault("User-Agent", ua)
    session.request = _wrap_with_backoff(session.request)  # type: ignore[assignment]
    LOGGER.debug("Configured cached HTTP session", cache=cache_name, expire_after=cache_expire)

    with _SESSION_LOCK:
        _SHARED_SESSION = session
    return session


def get_shared_session() -> requests.Session:
    """Return the shared HTTP session, creating it lazily if required."""

    global _SHARED_SESSION
    with _SESSION_LOCK:
        if _SHARED_SESSION is None:
            return create_shared_session()
        return _SHARED_SESSION


def reset_shared_session() -> None:
    """Dispose the cached shared session (useful in tests)."""

    global _SHARED_SESSION
    with _SESSION_LOCK:
        _SHARED_SESSION = None


def _error_file_writer(log_dir: Path) -> Callable[[Any, str, dict[str, Any]], dict[str, Any]]:
    """Create a structlog processor that mirrors error events into ``*.error`` files."""

    log_dir.mkdir(parents=True, exist_ok=True)

    def processor(_: Any, method_name: str, event_dict: dict[str, Any]) -> dict[str, Any]:
        if method_name in {"error", "exception", "critical"}:
            source = event_dict.get("source") or "pipeline"
            path = log_dir / f"{source}.error"
            with path.open("a", encoding="utf-8") as handle:
                handle.write(json.dumps(event_dict, ensure_ascii=False) + "\n")
        return event_dict

    return processor


def setup_logging(
    level: int | str = "INFO", log_dir: str | Path | None = None
) -> structlog.stdlib.BoundLogger:
    """Initialise structlog with JSON output and error file mirroring."""

    level_value = logging.getLevelName(level) if isinstance(level, str) else level
    if isinstance(level_value, str):
        # getLevelName returns the textual form if level not found; fall back to INFO
        level_value = logging.INFO

    target_dir = Path(log_dir or "logs")
    target_dir.mkdir(parents=True, exist_ok=True)

    logging.basicConfig(
        level=level_value,
        format="%(message)s",
        handlers=[logging.StreamHandler()],
        force=True,
    )

    structlog.reset_defaults()
    structlog.configure(
        processors=[
            structlog.processors.TimeStamper(fmt="iso", key="ts"),
            structlog.stdlib.add_log_level,
            structlog.processors.EventRenamer("msg"),
            _error_file_writer(target_dir),
            structlog.processors.JSONRenderer(),
        ],
        wrapper_class=structlog.stdlib.BoundLogger,
        logger_factory=structlog.stdlib.LoggerFactory(),
        cache_logger_on_first_use=True,
    )

    return structlog.get_logger()


def get_logger(**initial_context: Any) -> structlog.stdlib.BoundLogger:
    """Return a logger bound with the provided context."""

    logger = structlog.get_logger()
    if initial_context:
        logger = logger.bind(**initial_context)
    return logger
