from __future__ import annotations

import json
import shelve
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping, MutableMapping, Protocol, runtime_checkable

import structlog


@runtime_checkable
class CacheStrategy(Protocol):
    """Контракт для кэшей HTTP-ответов."""

    @staticmethod
    def make_key(
        method: str,
        url: str,
        params: Mapping[str, Any] | None,
        headers: Mapping[str, str] | None,
    ) -> str:
        ...

    def get(self, key: str) -> bytes | None:
        ...

    def set(self, key: str, value: bytes) -> None:
        ...


@dataclass(frozen=True)
class TTLCacheConfig:
    ttl_seconds: float
    path: Path | None = None


class InMemoryTTLCacheImpl(CacheStrategy):
    """Потокобезопасный TTL-кэш в памяти или файле."""

    def __init__(self, config: TTLCacheConfig) -> None:
        self._config = config
        self._lock = threading.Lock()
        self._logger = structlog.get_logger(__name__).bind(component="ttl_cache")
        self._store: MutableMapping[str, tuple[float, bytes]] | None = None
        if config.path is None:
            self._store = {}
        else:
            config.path.parent.mkdir(parents=True, exist_ok=True)

    def _open(self) -> MutableMapping[str, tuple[float, bytes]]:
        if self._store is not None:
            return self._store
        return shelve.open(str(self._config.path), writeback=False)  # type: ignore[return-value]

    @staticmethod
    def make_key(
        method: str,
        url: str,
        params: Mapping[str, Any] | None,
        headers: Mapping[str, str] | None,
    ) -> str:
        serialized = json.dumps(
            {
                "method": method.upper(),
                "url": url,
                "params": sorted((params or {}).items()),
                "headers": sorted((headers or {}).items()),
            },
            sort_keys=True,
            separators=(",", ":"),
        )
        return serialized

    def get(self, key: str) -> bytes | None:
        now = time.monotonic()
        with self._lock:
            store = self._open()
            payload = store.get(key)
            if payload is None:
                self._logger.debug("cache_miss", key=key)
                return None
            created, data = payload
            if now - created > self._config.ttl_seconds:
                del store[key]
                self._logger.debug("cache_miss", key=key, reason="expired")
                return None
            self._logger.info("cache_hit", key=key)
            return data

    def set(self, key: str, value: bytes) -> None:
        with self._lock:
            store = self._open()
            store[key] = (time.monotonic(), value)
            if hasattr(store, "sync"):
                store.sync()  # type: ignore[operator]


__all__ = ["CacheStrategy", "InMemoryTTLCacheImpl", "TTLCacheConfig"]
