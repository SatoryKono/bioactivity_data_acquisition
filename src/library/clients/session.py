"""Shared requests session helpers."""
from __future__ import annotations

import threading

import requests

_SESSION: requests.Session | None = None
_LOCK = threading.Lock()


def get_shared_session() -> requests.Session:
    global _SESSION
    if _SESSION is None:
        with _LOCK:
            if _SESSION is None:
                _SESSION = requests.Session()
    return _SESSION


def reset_shared_session() -> None:
    """Reset the cached shared session. Primarily intended for tests."""

    global _SESSION
    with _LOCK:
        if _SESSION is not None:
            _SESSION.close()
        _SESSION = None


__all__ = ["get_shared_session", "reset_shared_session"]
