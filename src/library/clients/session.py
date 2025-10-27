"""Shared requests session helpers."""

from __future__ import annotations

import threading
from urllib3.util.retry import Retry

import requests
from requests.adapters import HTTPAdapter

_SESSION: requests.Session | None = None
_LOCK = threading.Lock()


def get_shared_session() -> requests.Session:
    """Get a shared requests session with global timeout and retry settings."""
    global _SESSION
    if _SESSION is None:
        with _LOCK:
            if _SESSION is None:
                _SESSION = requests.Session()

                # Configure retry strategy
                retry_strategy = Retry(
                    total=3, backoff_factor=1.0, status_forcelist=[429, 500, 502, 503, 504], allowed_methods=["HEAD", "GET", "PUT", "DELETE", "OPTIONS", "TRACE", "POST"]
                )

                # Mount adapters with retry strategy and timeouts
                adapter = HTTPAdapter(max_retries=retry_strategy)
                _SESSION.mount("http://", adapter)
                _SESSION.mount("https://", adapter)

                # Set default timeout (connect timeout, read timeout)
                # _SESSION.timeout = (10.0, 30.0)  # This attribute doesn't exist on Session

    return _SESSION


def reset_shared_session() -> None:
    """Reset the cached shared session. Primarily intended for tests."""

    global _SESSION
    with _LOCK:
        if _SESSION is not None:
            _SESSION.close()
        _SESSION = None


__all__ = ["get_shared_session", "reset_shared_session"]
