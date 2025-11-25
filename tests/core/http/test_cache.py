from __future__ import annotations

import time

from bioetl.core.http import TTLCache, TTLCacheConfig


def test_ttl_cache_expiration():
    cache = TTLCache(TTLCacheConfig(ttl_seconds=0.1))
    key = cache.make_key("GET", "http://example.com", {"q": 1}, {"h": "1"})
    cache.set(key, b"payload")
    assert cache.get(key) == b"payload"
    time.sleep(0.12)
    assert cache.get(key) is None


def test_ttl_cache_persists_values():
    cache = TTLCache(TTLCacheConfig(ttl_seconds=1))
    key = cache.make_key("GET", "http://example.com", None, None)
    cache.set(key, b"payload")
    assert cache.get(key) == b"payload"
