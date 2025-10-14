"""Shared requests session helpers."""
from __future__ import annotations

import threading
from typing import Optional

import requests

_SESSION: Optional[requests.Session] = None
_LOCK = threading.Lock()


def get_shared_session() -> requests.Session:
    global _SESSION
    if _SESSION is None:
        with _LOCK:
            if _SESSION is None:
                _SESSION = requests.Session()
    return _SESSION
