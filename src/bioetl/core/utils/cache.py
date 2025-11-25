from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any, MutableMapping

from .interfaces import CacheABC


@dataclass
class _CacheEntry:
    value: Any
    expires_at: float | None

    def is_expired(self) -> bool:
        return self.expires_at is not None and time.time() >= self.expires_at


class InMemoryCache(CacheABC):
    """Неблокирующий in-memory кэш с поддержкой TTL."""

    def __init__(self, ttl_seconds: float | None = None) -> None:
        self._storage: MutableMapping[str, _CacheEntry] = {}
        self._ttl = ttl_seconds

    def get(self, key: str) -> Any | None:
        entry = self._storage.get(key)
        if entry is None:
            return None
        if entry.is_expired():
            self._storage.pop(key, None)
            return None
        return entry.value

    def set(self, key: str, value: Any) -> None:
        expires_at = time.time() + self._ttl if self._ttl is not None else None
        self._storage[key] = _CacheEntry(value=value, expires_at=expires_at)

