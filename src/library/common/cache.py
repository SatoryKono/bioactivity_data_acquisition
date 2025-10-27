"""Unified caching system for API clients.

This module provides a comprehensive caching solution supporting both
in-memory (TTL cache) and file-based caching strategies.
"""

from __future__ import annotations

import hashlib
import json
import logging
import threading
import time
from abc import ABC, abstractmethod
from collections.abc import Callable
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Any, Generic, TypeVar

from cachetools import TTLCache

logger = logging.getLogger(__name__)

T = TypeVar("T")


class CacheStrategy(Enum):
    """Cache storage strategies."""

    MEMORY = "memory"  # In-memory TTL cache
    FILE = "file"  # File-based cache
    HYBRID = "hybrid"  # Both memory and file


@dataclass
class CacheConfig:
    """Configuration for cache instances."""

    strategy: CacheStrategy = CacheStrategy.MEMORY
    ttl: int = 3600  # Time-to-live in seconds
    max_size: int = 1000  # Maximum number of items
    cache_dir: str | None = None  # Directory for file cache
    key_prefix: str = ""  # Prefix for cache keys


class CacheKey:
    """Utility for generating cache keys."""

    @staticmethod
    def from_endpoint(endpoint: str, params: dict[str, Any] | None = None) -> str:
        """Generate cache key from endpoint and parameters."""
        if params is None:
            params = {}

        # Create deterministic key from endpoint and sorted params
        key_data = f"{endpoint}:{json.dumps(params, sort_keys=True)}"
        return hashlib.sha256(key_data.encode()).hexdigest()

    @staticmethod
    def from_url(url: str) -> str:
        """Generate cache key from URL."""
        return hashlib.sha256(url.encode("utf-8")).hexdigest()

    @staticmethod
    def from_data(data: Any) -> str:
        """Generate cache key from arbitrary data."""
        key_data = json.dumps(data, sort_keys=True, default=str)
        return hashlib.sha256(key_data.encode()).hexdigest()


class CacheEntry(Generic[T]):
    """Cache entry with metadata."""

    def __init__(self, value: T, created_at: float | None = None):
        self.value = value
        self.created_at = created_at or time.time()
        self.access_count = 0
        self.last_accessed = self.created_at

    def is_expired(self, ttl: int) -> bool:
        """Check if entry is expired."""
        return time.time() - self.created_at > ttl

    def access(self) -> T:
        """Access the entry and update metadata."""
        self.access_count += 1
        self.last_accessed = time.time()
        return self.value


class CacheBackend(ABC, Generic[T]):
    """Abstract base class for cache backends."""

    @abstractmethod
    def get(self, key: str) -> T | None:
        """Get value from cache."""
        pass

    @abstractmethod
    def set(self, key: str, value: T, ttl: int | None = None) -> None:
        """Set value in cache."""
        pass

    @abstractmethod
    def delete(self, key: str) -> None:
        """Delete value from cache."""
        pass

    @abstractmethod
    def clear(self) -> None:
        """Clear all cache entries."""
        pass

    @abstractmethod
    def size(self) -> int:
        """Get number of entries in cache."""
        pass


class MemoryCacheBackend(CacheBackend[T]):
    """In-memory cache backend using TTLCache."""

    def __init__(self, max_size: int = 1000, ttl: int = 3600):
        self._cache: TTLCache[str, CacheEntry[T]] = TTLCache(maxsize=max_size, ttl=ttl)
        self._lock = threading.Lock()

    def get(self, key: str) -> T | None:
        """Get value from memory cache."""
        with self._lock:
            entry = self._cache.get(key)
            if entry is None:
                return None

            if entry.is_expired(int(self._cache.ttl)):
                del self._cache[key]
                return None

            return entry.access()

    def set(self, key: str, value: T, ttl: int | None = None) -> None:
        """Set value in memory cache."""
        with self._lock:
            entry = CacheEntry(value)
            self._cache[key] = entry

    def delete(self, key: str) -> None:
        """Delete value from memory cache."""
        with self._lock:
            self._cache.pop(key, None)

    def clear(self) -> None:
        """Clear all cache entries."""
        with self._lock:
            self._cache.clear()

    def size(self) -> int:
        """Get number of entries in cache."""
        return len(self._cache)


class FileCacheBackend(CacheBackend[T]):
    """File-based cache backend."""

    def __init__(self, cache_dir: str, ttl: int = 3600):
        self.cache_dir = Path(cache_dir)
        self.ttl = ttl
        self._lock = threading.Lock()

        # Create cache directory
        self.cache_dir.mkdir(parents=True, exist_ok=True)

    def _get_cache_path(self, key: str) -> Path:
        """Get cache file path for key."""
        return self.cache_dir / f"{key}.json"

    def get(self, key: str) -> T | None:
        """Get value from file cache."""
        cache_path = self._get_cache_path(key)

        if not cache_path.exists():
            return None

        try:
            with open(cache_path) as f:
                data = json.load(f)

            # Check if expired
            if time.time() - data["created_at"] > self.ttl:
                cache_path.unlink(missing_ok=True)
                return None

            return data.get("value", None)

        except Exception as e:
            logger.warning(f"Failed to read cache file {cache_path}: {e}")
            cache_path.unlink(missing_ok=True)
            return None

    def set(self, key: str, value: T, ttl: int | None = None) -> None:
        """Set value in file cache."""
        cache_path = self._get_cache_path(key)

        try:
            data = {"value": value, "created_at": time.time(), "ttl": ttl or self.ttl}

            with open(cache_path, "w") as f:
                json.dump(data, f)

        except Exception as e:
            logger.warning(f"Failed to write cache file {cache_path}: {e}")

    def delete(self, key: str) -> None:
        """Delete value from file cache."""
        cache_path = self._get_cache_path(key)
        cache_path.unlink(missing_ok=True)

    def clear(self) -> None:
        """Clear all cache entries."""
        for cache_file in self.cache_dir.glob("*.json"):
            cache_file.unlink(missing_ok=True)

    def size(self) -> int:
        """Get number of entries in cache."""
        return len(list(self.cache_dir.glob("*.json")))


class HybridCacheBackend(CacheBackend[T]):
    """Hybrid cache backend using both memory and file storage."""

    def __init__(self, cache_dir: str, max_size: int = 1000, ttl: int = 3600) -> None:
        self.memory_backend: MemoryCacheBackend[T] = MemoryCacheBackend(max_size, ttl)
        self.file_backend: FileCacheBackend[T] = FileCacheBackend(cache_dir, ttl)
        self._lock = threading.Lock()

    def get(self, key: str) -> T | None:
        """Get value from hybrid cache (memory first, then file)."""
        # Try memory first
        value = self.memory_backend.get(key)
        if value is not None:
            return value

        # Try file cache
        value = self.file_backend.get(key)
        if value is not None:
            # Promote to memory cache
            self.memory_backend.set(key, value)
            return value

        return None

    def set(self, key: str, value: T, ttl: int | None = None) -> None:
        """Set value in both memory and file cache."""
        with self._lock:
            self.memory_backend.set(key, value, ttl)
            self.file_backend.set(key, value, ttl)

    def delete(self, key: str) -> None:
        """Delete value from both caches."""
        with self._lock:
            self.memory_backend.delete(key)
            self.file_backend.delete(key)

    def clear(self) -> None:
        """Clear both caches."""
        with self._lock:
            self.memory_backend.clear()
            self.file_backend.clear()

    def size(self) -> int:
        """Get total number of entries (memory + file)."""
        return self.memory_backend.size() + self.file_backend.size()


class UnifiedCache(Generic[T]):
    """Unified cache interface supporting multiple backends."""

    def __init__(self, config: CacheConfig):
        self.config = config
        self._backend: CacheBackend[T] = self._create_backend()
        self._lock = threading.Lock()

    def _create_backend(self) -> CacheBackend[T]:
        """Create cache backend based on strategy."""
        if self.config.strategy == CacheStrategy.MEMORY:
            return MemoryCacheBackend(max_size=self.config.max_size, ttl=self.config.ttl)
        elif self.config.strategy == CacheStrategy.FILE:
            if self.config.cache_dir is None:
                raise ValueError("cache_dir is required for file strategy")
            return FileCacheBackend(cache_dir=self.config.cache_dir, ttl=self.config.ttl)
        elif self.config.strategy == CacheStrategy.HYBRID:
            if self.config.cache_dir is None:
                raise ValueError("cache_dir is required for hybrid strategy")
            return HybridCacheBackend(cache_dir=self.config.cache_dir, max_size=self.config.max_size, ttl=self.config.ttl)
        else:
            raise ValueError(f"Unknown cache strategy: {self.config.strategy}")

    def _make_key(self, key: str) -> str:
        """Add prefix to cache key."""
        if self.config.key_prefix:
            return f"{self.config.key_prefix}:{key}"
        return key

    def get(self, key: str) -> T | None:
        """Get value from cache."""
        full_key = self._make_key(key)
        return self._backend.get(full_key)

    def set(self, key: str, value: T, ttl: int | None = None) -> None:
        """Set value in cache."""
        full_key = self._make_key(key)
        self._backend.set(full_key, value, ttl)

    def delete(self, key: str) -> None:
        """Delete value from cache."""
        full_key = self._make_key(key)
        self._backend.delete(full_key)

    def clear(self) -> None:
        """Clear all cache entries."""
        self._backend.clear()

    def size(self) -> int:
        """Get number of entries in cache."""
        return self._backend.size()

    def get_or_set(self, key: str, factory: Callable[[], T], ttl: int | None = None) -> T:
        """Get value from cache or set it using factory function."""
        value = self.get(key)
        if value is None:
            value = factory()
            self.set(key, value, ttl)
        return value


# Factory functions for common cache configurations
def create_memory_cache(max_size: int = 1000, ttl: int = 3600, key_prefix: str = "") -> UnifiedCache[Any]:
    """Create an in-memory cache."""
    config = CacheConfig(strategy=CacheStrategy.MEMORY, max_size=max_size, ttl=ttl, key_prefix=key_prefix)
    return UnifiedCache(config)


def create_file_cache(cache_dir: str, ttl: int = 3600, key_prefix: str = "") -> UnifiedCache[Any]:
    """Create a file-based cache."""
    config = CacheConfig(strategy=CacheStrategy.FILE, ttl=ttl, cache_dir=cache_dir, key_prefix=key_prefix)
    return UnifiedCache(config)


def create_hybrid_cache(cache_dir: str, max_size: int = 1000, ttl: int = 3600, key_prefix: str = "") -> UnifiedCache[Any]:
    """Create a hybrid cache (memory + file)."""
    config = CacheConfig(strategy=CacheStrategy.HYBRID, max_size=max_size, ttl=ttl, cache_dir=cache_dir, key_prefix=key_prefix)
    return UnifiedCache(config)


# Global cache instances for common use cases
_global_caches: dict[str, UnifiedCache[Any]] = {}
_cache_lock = threading.Lock()


def get_global_cache(name: str, config: CacheConfig | None = None) -> UnifiedCache[Any]:
    """Get or create a global cache instance."""
    with _cache_lock:
        if name not in _global_caches:
            if config is None:
                # Default to memory cache
                config = CacheConfig(strategy=CacheStrategy.MEMORY)
            _global_caches[name] = UnifiedCache(config)
        return _global_caches[name]


def clear_global_cache(name: str) -> None:
    """Clear a global cache instance."""
    with _cache_lock:
        if name in _global_caches:
            _global_caches[name].clear()


def clear_all_global_caches() -> None:
    """Clear all global cache instances."""
    with _cache_lock:
        for cache in _global_caches.values():
            cache.clear()
        _global_caches.clear()


# Convenience functions for common cache operations
def cache_result(cache: UnifiedCache[Any], key: str, ttl: int | None = None):
    """Decorator to cache function results."""

    def decorator(func):
        def wrapper(*args, **kwargs):
            # Generate cache key from function name and arguments
            cache_key = CacheKey.from_data({"func": func.__name__, "args": args, "kwargs": kwargs})

            # Try to get from cache
            result = cache.get(cache_key)
            if result is not None:
                return result

            # Execute function and cache result
            result = func(*args, **kwargs)
            cache.set(cache_key, result, ttl)
            return result

        return wrapper

    return decorator
