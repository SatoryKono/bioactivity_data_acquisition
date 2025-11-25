from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Callable, Mapping

import requests

from bioetl.core.http import APIConfig, CircuitBreaker, RetryPolicy, TTLCache, TokenBucketRateLimiter, UnifiedAPIClient
from bioetl.core.http.interfaces import CacheStrategy, CircuitBreakerStrategy, RateLimiter, RetryStrategy


@dataclass
class DummyCache(CacheStrategy):
    store: dict[str, bytes]

    @staticmethod
    def make_key(method: str, url: str, params: Mapping[str, Any] | None, headers: Mapping[str, str] | None) -> str:
        return TTLCache.make_key(method, url, params, headers)

    def get(self, key: str) -> bytes | None:
        return self.store.get(key)

    def set(self, key: str, value: bytes) -> None:
        self.store[key] = value


@dataclass
class DummyRateLimiter(RateLimiter):
    acquire_calls: int = 0

    def try_acquire(self) -> bool:
        self.acquire_calls += 1
        return True

    def acquire(self, *, timeout: float | None = None) -> bool:  # noqa: ARG002 - signature dictated by protocol
        self.acquire_calls += 1
        return True


@dataclass
class DummyRetry(RetryStrategy):
    max_retries: int = 1

    def compute_backoff(self, attempt: int, retry_after: float | None = None) -> float:  # noqa: ARG002 - protocol compatibility
        return 0


@dataclass
class DummyCircuitBreaker(CircuitBreakerStrategy):
    recorded_failures: int = 0
    recorded_successes: int = 0
    call_count: int = 0

    def before_call(self) -> None:
        return None

    def record_success(self) -> None:
        self.recorded_successes += 1

    def record_failure(self) -> None:
        self.recorded_failures += 1

    def call(self, func: Callable[[], requests.Response]) -> requests.Response:  # type: ignore[override]
        self.call_count += 1
        return func()


class MockSession(requests.Session):
    def __init__(self, response: requests.Response):
        super().__init__()
        self._response = response
        self.request_calls = 0

    def request(self, *args, **kwargs):  # type: ignore[override]
        self.request_calls += 1
        return self._response


def build_api_config() -> APIConfig:
    return APIConfig(
        base_url="http://example.com/",
        timeout_sec=0.2,
        max_retries=0,
        backoff_factor=0.1,
        max_backoff_sec=5,
        rate_limit_calls=10,
        rate_limit_period_sec=1,
        cache_enabled=True,
        cache_ttl_sec=5,
        circuit_breaker_fail_max=3,
        circuit_breaker_reset_sec=0.2,
    )


def make_response(payload: Mapping[str, Any]) -> requests.Response:
    response = requests.Response()
    response.status_code = 200
    response._content = json.dumps(payload).encode("utf-8")
    response.url = "http://example.com/resource"
    response.headers = {}
    return response


def test_unified_client_composes_injected_strategies():
    response = make_response({"ok": True})
    session = MockSession(response)
    cache = DummyCache(store={})
    limiter = DummyRateLimiter()
    retry = DummyRetry()
    breaker = DummyCircuitBreaker()

    client = UnifiedAPIClient(
        build_api_config(),
        session=session,
        cache=cache,
        rate_limiter=limiter,
        retry_strategy=retry,
        circuit_breaker=breaker,
    )

    payload_first = client.get_json("/resource")
    payload_cached = client.get_json("/resource")

    assert payload_first == {"ok": True}
    assert payload_cached == {"ok": True}
    assert session.request_calls == 1  # второй вызов обслужен из кэша
    assert limiter.acquire_calls >= 1
    assert breaker.call_count == 1
    assert breaker.recorded_successes == 1
    assert cache.store


def test_default_component_factories_used_when_missing():
    response = make_response({"ok": True})
    session = MockSession(response)

    client = UnifiedAPIClient(build_api_config(), session=session)

    payload = client.get_json("/resource")

    assert payload == {"ok": True}
    assert isinstance(client._rate_limiter, TokenBucketRateLimiter)  # noqa: SLF001
    assert isinstance(client._retry_strategy, RetryPolicy)  # noqa: SLF001
    assert isinstance(client._breaker, CircuitBreaker)  # noqa: SLF001
