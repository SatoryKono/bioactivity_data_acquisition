import time
from pathlib import Path
from typing import Any

import pytest
import requests
import responses
from requests import Response
from requests.exceptions import Timeout

from bioetl.core.http.api_client import (
    APIConfig,
    CircuitBreakerConfig,
    CircuitBreakerOpenError,
    UnifiedAPIClient,
)
from bioetl.core.http._rate_limiter import RateLimiterConfig, TokenBucketRateLimiter


class FakeClock:
    def __init__(self) -> None:
        self.now = 0.0
        self.slept: list[float] = []

    def monotonic(self) -> float:
        return self.now

    def sleep(self, value: float) -> None:
        self.slept.append(value)
        self.now += value


@responses.activate
def test_retry_backoff(monkeypatch: pytest.MonkeyPatch) -> None:
    config = APIConfig(
        name="demo",
        base_url="http://example.com",
        timeout=1,
        connect_timeout=None,
        retry_max_attempts=3,
        retry_backoff_factor=0.1,
        retry_jitter=False,
        rate_limit_max_calls=10,
        rate_limit_period=1,
        rate_limit_jitter=False,
        cache_enabled=False,
        cache_ttl=0,
        headers={},
    )
    client = UnifiedAPIClient(config)
    responses.add(responses.GET, "http://example.com/status", status=500)
    responses.add(responses.GET, "http://example.com/status", status=500)
    responses.add(responses.GET, "http://example.com/status", status=200)

    sleeps: list[float] = []
    monkeypatch.setattr(time, "sleep", lambda v: sleeps.append(v))

    resp = client.get("/status")
    assert resp.status_code == 200
    assert sleeps == [0.1, 0.2]
    assert len(responses.calls) == 3


def test_rate_limiter_blocks_until_token_available() -> None:
    clock = FakeClock()
    limiter = TokenBucketRateLimiter(
        RateLimiterConfig(max_calls=2, period=1, jitter=False),
        monotonic=clock.monotonic,
        sleeper=clock.sleep,
    )
    waits = [limiter.acquire(), limiter.acquire(), limiter.acquire()]
    assert waits[0] == pytest.approx(0.0)
    assert waits[1] == pytest.approx(0.0)
    assert waits[2] == pytest.approx(0.5)
    assert clock.slept == [0.5]


@responses.activate
def test_cache_hit(tmp_path: Path) -> None:
    cache_dir = tmp_path / "cache"
    cache_dir.mkdir()
    cache_path = cache_dir / "cache.db"
    config = APIConfig(
        name="demo",
        base_url="http://example.com",
        timeout=1,
        connect_timeout=None,
        retry_max_attempts=2,
        retry_backoff_factor=0.1,
        retry_jitter=False,
        rate_limit_max_calls=10,
        rate_limit_period=1,
        rate_limit_jitter=False,
        cache_enabled=True,
        cache_ttl=60,
        headers={},
    )
    client = UnifiedAPIClient(config, cache_path=str(cache_path))
    responses.add(
        responses.GET,
        "http://example.com/data",
        json={"ok": True},
        status=200,
    )

    first = client.get("/data")
    second = client.get("/data")

    assert first.json() == {"ok": True}
    assert second.json() == {"ok": True}
    assert len(responses.calls) == 1


def test_circuit_breaker_opens_and_recovers(monkeypatch: pytest.MonkeyPatch) -> None:
    clock = FakeClock()
    config = APIConfig(
        name="demo",
        base_url="http://example.com",
        timeout=1,
        connect_timeout=None,
        retry_max_attempts=1,
        retry_backoff_factor=0.1,
        retry_jitter=False,
        rate_limit_max_calls=10,
        rate_limit_period=1,
        rate_limit_jitter=False,
        cache_enabled=False,
        cache_ttl=0,
        headers={},
    )
    cb_conf = CircuitBreakerConfig(
        failure_threshold=1, success_threshold=1, timeout_seconds=1
    )
    client = UnifiedAPIClient(config, circuit_breaker_config=cb_conf)
    client.circuit_breaker._monotonic = clock.monotonic  # type: ignore[attr-defined]

    def failing_request(*_: Any, **__: Any) -> Response:
        raise Timeout("fail")

    monkeypatch.setattr(client.session, "request", failing_request)

    with pytest.raises(Timeout):
        client.get("/boom")

    with pytest.raises(CircuitBreakerOpenError):
        client.get("/boom")

    clock.sleep(1.1)

    success_response = requests.Response()
    success_response.status_code = 200
    success_response._content = b"{}"
    success_response.url = "http://example.com/recovered"
    monkeypatch.setattr(client.session, "request", lambda *args, **kwargs: success_response)

    resp = client.get("/ok")
    assert resp.status_code == 200
