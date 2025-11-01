"""Lightweight fallback implementation of ``cachetools`` for environments without the dependency."""

from __future__ import annotations

import threading
import time
from collections.abc import Iterator, MutableMapping
from typing import Generic, TypeVar

KT = TypeVar("KT")
VT = TypeVar("VT")


class TTLCache(MutableMapping[KT, VT], Generic[KT, VT]):
    """Minimal TTLCache compatible with ``cachetools.TTLCache``."""

    def __init__(self, maxsize: int = 1024, ttl: float = 3600.0) -> None:
        if maxsize <= 0:
            raise ValueError("maxsize must be positive")
        if ttl <= 0:
            raise ValueError("ttl must be positive")
        self.maxsize = maxsize
        self.ttl = float(ttl)
        self._store: dict[KT, tuple[VT, float]] = {}
        self._lock = threading.Lock()

    def __repr__(self) -> str:  # pragma: no cover - debugging helper
        return f"TTLCache(maxsize={self.maxsize}, ttl={self.ttl}, size={len(self._store)})"

    def _purge_expired(self) -> None:
        now = time.monotonic()
        expired = [key for key, (_, expiry) in self._store.items() if expiry <= now]
        for key in expired:
            self._store.pop(key, None)

    def _evict_if_needed(self) -> None:
        if len(self._store) <= self.maxsize:
            return
        oldest_key: KT | None = None
        oldest_expiry = float("inf")
        for key, (_, expiry) in self._store.items():
            if expiry < oldest_expiry:
                oldest_expiry = expiry
                oldest_key = key
        if oldest_key is not None:
            self._store.pop(oldest_key, None)

    def __getitem__(self, key: KT) -> VT:
        with self._lock:
            self._purge_expired()
            value, expiry = self._store[key]
            if expiry <= time.monotonic():
                self._store.pop(key, None)
                raise KeyError(key)
            return value

    def __setitem__(self, key: KT, value: VT) -> None:
        with self._lock:
            expiry = time.monotonic() + self.ttl
            self._store[key] = (value, expiry)
            self._purge_expired()
            self._evict_if_needed()

    def __delitem__(self, key: KT) -> None:
        with self._lock:
            self._store.pop(key)

    def __iter__(self) -> Iterator[KT]:
        with self._lock:
            self._purge_expired()
            return iter(list(self._store.keys()))

    def __len__(self) -> int:
        with self._lock:
            self._purge_expired()
            return len(self._store)

    def get(self, key: KT, default: VT | None = None) -> VT | None:
        try:
            return self[key]
        except KeyError:
            return default

    def pop(self, key: KT, default: VT | None = None) -> VT | None:
        with self._lock:
            self._purge_expired()
            if key in self._store:
                value, _ = self._store.pop(key)
                return value
            if default is not None:
                return default
            raise KeyError(key)

    def clear(self) -> None:
        with self._lock:
            self._store.clear()


__all__ = ["TTLCache"]
