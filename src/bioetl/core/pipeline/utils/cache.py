from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any, MutableMapping

from .interfaces import CacheABC


@dataclass
class CacheEntry:
    value: Any
    expires_at: float | None = None

    def is_expired(self) -> bool:
        return self.expires_at is not None and time.time() > self.expires_at


class InMemoryCache(CacheABC):
    """Простое in-memory хранилище с поддержкой TTL."""

    def __init__(self, ttl_seconds: float | None = None) -> None:
        self.ttl_seconds = ttl_seconds
        self._storage: MutableMapping[str, CacheEntry] = {}

    def get(self, key: str) -> Any | None:
        entry = self._storage.get(key)
        if entry is None:
            return None
        if entry.is_expired():
            self._storage.pop(key, None)
            return None
        return entry.value

    def set(self, key: str, value: Any) -> None:
        expires_at = time.time() + self.ttl_seconds if self.ttl_seconds else None
        self._storage[key] = CacheEntry(value=value, expires_at=expires_at)
