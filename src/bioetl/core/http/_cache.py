from __future__ import annotations

import os
import shelve
import threading
import time
from pathlib import Path
from typing import Any, Iterable, Mapping

from bioetl.core.logging import LogEvents, UnifiedLogger


class TTLCache:
    """Файловый TTL-кэш на основе ``shelve``."""

    def __init__(self, path: str | Path | None = None) -> None:
        cache_path = Path(
            path or (Path("/tmp") / f"bioetl_http_cache_{os.getpid()}.db")
        )
        self._path = cache_path
        self._lock = threading.Lock()
        self._logger = UnifiedLogger.get(__name__).bind(component="ttl_cache")

    def _open(self) -> shelve.DbfilenameShelf:
        return shelve.open(str(self._path))

    def make_key(
        self,
        method: str,
        path: str,
        params: Mapping[str, Any] | None,
        headers: Mapping[str, str] | None,
    ) -> str:
        params_items: Iterable[tuple[str, Any]] = sorted((params or {}).items())
        header_items: Iterable[tuple[str, str]] = sorted((headers or {}).items())
        return str((method.upper(), path, tuple(params_items), tuple(header_items)))

    def get(self, key: str, ttl: float) -> bytes | None:
        now = time.time()
        with self._lock:
            with self._open() as shelf:
                if key not in shelf:
                    self._logger.debug(LogEvents.CACHE_MISS, key=key)
                    return None
                created, payload = shelf[key]
                if now - created > ttl:
                    del shelf[key]
                    self._logger.debug(LogEvents.CACHE_MISS, key=key, reason="expired")
                    return None
                self._logger.info(LogEvents.CACHE_HIT, key=key)
                return payload

    def set(self, key: str, payload: bytes) -> None:
        with self._lock:
            with self._open() as shelf:
                shelf[key] = (time.time(), payload)
