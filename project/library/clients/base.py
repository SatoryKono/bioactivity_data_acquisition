"""Base client implementation providing resilient HTTP access."""
from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any, Dict, Optional
from urllib.parse import quote

import backoff
import requests
import requests_cache
from requests import Response
from requests.exceptions import RequestException

from ..io.normalize import coerce_text
from ..utils.errors import ClientError, RetryableHTTPError
from ..utils.logging import ContextLogger, get_error_logger, get_logger
from ..utils.rate_limit import CompositeRateLimiter, RateLimiter, build_rate_limiter

CACHE_DIR = Path(".cache")
CACHE_DIR.mkdir(parents=True, exist_ok=True)
CACHE_PATH = CACHE_DIR / "http_cache"
USER_AGENT_EMAIL = "data@example.com"
PACKAGE_VERSION = "0.1.0"
USER_AGENT = f"project-publications-etl/{PACKAGE_VERSION} (+mailto={USER_AGENT_EMAIL})"
DEFAULT_TIMEOUT = 30

_RETRYABLE_STATUS = {429, 500, 502, 503, 504}


def handle_backoff(details: Dict[str, Any]) -> None:  # pragma: no cover - used indirectly by backoff
    target = details.get("target")
    client = getattr(target, "__self__", None)
    log_method = getattr(client, "_log_backoff", None)
    if callable(log_method):
        log_method(details)


def handle_giveup(details: Dict[str, Any]) -> None:  # pragma: no cover - used indirectly by backoff
    target = details.get("target")
    client = getattr(target, "__self__", None)
    log_method = getattr(client, "_log_giveup", None)
    if callable(log_method):
        log_method(details)


def details_target_giveup(exc: Exception) -> bool:  # pragma: no cover - used indirectly by backoff
    if isinstance(exc, RetryableHTTPError):
        response = exc.response
        if response is not None and response.status_code in _RETRYABLE_STATUS:
            return False
    if isinstance(exc, RequestException):
        return False
    return True


def create_session(expire_after: int = 60 * 60 * 12) -> requests.Session:
    session = requests_cache.CachedSession(
        cache_name=str(CACHE_PATH),
        backend="sqlite",
        allowable_methods=("GET", "POST"),
        expire_after=expire_after,
    )
    session.headers.setdefault("User-Agent", USER_AGENT)
    return session


def _parse_retry_after(response: Response) -> Optional[float]:
    header = response.headers.get("Retry-After")
    if not header:
        return None
    try:
        return float(header)
    except ValueError:
        return None


class BaseClient:
    """Base class encapsulating shared client behaviour."""

    def __init__(
        self,
        *,
        source: str,
        base_url: str,
        session: Optional[requests.Session] = None,
        per_client_rps: Optional[float] = None,
        global_limiter: Optional[RateLimiter] = None,
        run_id: str,
    ) -> None:
        self.source = source
        self.base_url = base_url.rstrip("/")
        self.session = session or create_session()
        self.logger: ContextLogger = get_logger(run_id, stage="extract", source=source)
        self.error_logger = get_error_logger(source)
        self.limiter = CompositeRateLimiter(global_limiter, build_rate_limiter(per_client_rps))

    def _prepare_url(self, path: str) -> str:
        if path.startswith("http"):
            return path
        return f"{self.base_url}/{path.lstrip('/')}"

    def _request(self, method: str, path: str, **kwargs: Any) -> Response:
        url = self._prepare_url(path)
        kwargs.setdefault("timeout", DEFAULT_TIMEOUT)
        self.limiter.wait()
        return self._perform_request(method, url, **kwargs)

    def _log_backoff(self, details: Dict[str, Any]) -> None:
        exc = details.get("exception")
        wait = details.get("wait")
        self.logger.warning(
            "retrying request",
            extra={
                "extra_fields": {
                    "wait": wait,
                    "tries": details.get("tries"),
                    "target": details.get("target"),
                    "exception": repr(exc),
                }
            },
        )
        if isinstance(exc, RetryableHTTPError) and exc.wait_time:
            time.sleep(exc.wait_time)

    def _log_giveup(self, details: Dict[str, Any]) -> None:
        exc = details.get("exception")
        self.error_logger.error(
            "request failed",
            extra={
                "source": self.source,
                "extra_fields": {
                    "target": getattr(details.get("target"), "__name__", None),
                    "exception": repr(exc),
                },
            },
        )

    @backoff.on_exception(
        backoff.expo,
        (RetryableHTTPError, RequestException),
        max_time=120,
        max_tries=6,
        jitter=backoff.full_jitter,
        on_backoff=handle_backoff,
        on_giveup=handle_giveup,
        giveup=details_target_giveup,
    )
    def _perform_request(self, method: str, url: str, **kwargs: Any) -> Response:
        response = self.session.request(method, url, **kwargs)
        if response.status_code in _RETRYABLE_STATUS:
            wait_time = _parse_retry_after(response)
            raise RetryableHTTPError(
                f"{self.source} returned {response.status_code}",
                response=response,
                wait_time=wait_time,
            )
        if response.status_code >= 400:
            self.error_logger.error(
                "non-retryable status",
                extra={
                    "source": self.source,
                    "extra_fields": {
                        "url": url,
                        "status": response.status_code,
                        "text": response.text[:500],
                    },
                },
            )
            raise ClientError(f"{self.source} request failed with status {response.status_code}")
        return response

    def get_json(self, path: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        response = self._request("GET", path, params=params)
        try:
            return response.json()
        except json.JSONDecodeError as exc:
            self.error_logger.error(
                "failed to decode json",
                extra={
                    "source": self.source,
                    "extra_fields": {"error": str(exc), "body": response.text[:200]},
                },
            )
            raise ClientError(f"{self.source} returned invalid JSON") from exc

    def post_json(self, path: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        response = self._request("POST", path, json=payload)
        try:
            return response.json()
        except json.JSONDecodeError as exc:
            self.error_logger.error(
                "failed to decode json",
                extra={
                    "source": self.source,
                    "extra_fields": {"error": str(exc), "body": response.text[:200]},
                },
            )
            raise ClientError(f"{self.source} returned invalid JSON") from exc

    @staticmethod
    def encode_identifier(identifier: str) -> str:
        safe_identifier = coerce_text(identifier)
        return quote(safe_identifier, safe="") if safe_identifier else ""


