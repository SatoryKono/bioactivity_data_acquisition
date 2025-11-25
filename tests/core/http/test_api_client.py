from __future__ import annotations

import time
from unittest.mock import MagicMock

import pytest
import requests
from requests import Response
from requests.exceptions import RequestException, Timeout

from bioetl.core.http.api_client import (
    APIConfig,
    CircuitBreakerConfig,
    CircuitBreakerOpenError,
    UnifiedAPIClient,
)
from bioetl.core.http._rate_limiter import RateLimiterConfig, TokenBucketRateLimiter


class DummySession(requests.Session):
    def __init__(self, responses: list[Response | Exception]) -> None:
        super().__init__()
        self._responses = responses
        self.calls = 0

    def request(self, *args, **kwargs):  # type: ignore[override]
        self.calls += 1
        item = self._responses.pop(0)
        if isinstance(item, Exception):
            raise item
        return item


def make_response(status: int = 200, body: str = "ok", headers: dict[str, str] | None = None) -> Response:
    resp = Response()
    resp.status_code = status
    resp._content = body.encode()
    resp.headers = headers or {}
    resp.url = "http://example.com"
    resp.encoding = "utf-8"
    return resp


def base_config(**overrides):
    cfg = APIConfig(
        name="test",
        base_url="http://example.com",
        timeout=0.2,
        connect_timeout=None,
        retry_max_attempts=3,
        retry_backoff_factor=0.1,
        retry_jitter=False,
        rate_limit_max_calls=10,
        rate_limit_period=1,
        rate_limit_jitter=False,
        cache_enabled=True,
        cache_ttl=5,
        headers={},
    )
    for k, v in overrides.items():
        setattr(cfg, k, v)
    return cfg


def test_retry_backoff_without_jitter(monkeypatch):
    sleep_calls: list[float] = []
    monkeypatch.setattr(time, "sleep", lambda s: sleep_calls.append(s))

    responses: list[Response | Exception] = [Timeout("boom"), Timeout("boom"), make_response()]
    session = DummySession(responses)
    client = UnifiedAPIClient(base_config(), session=session)

    resp = client.get("/data")

    assert resp.status_code == 200
    assert sleep_calls == [0.1, 0.2]
    assert session.calls == 3


def test_rate_limiter_waits_for_token(monkeypatch):
    config = RateLimiterConfig(max_calls=2, period=0.2, jitter=False)
    limiter = TokenBucketRateLimiter(config)
    start = time.monotonic()
    limiter.acquire()
    limiter.acquire()
    limiter.acquire()
    elapsed = time.monotonic() - start
    assert elapsed >= 0.09


def test_cache_hit_avoids_network(monkeypatch):
    first = make_response(200, "payload")
    session = DummySession([first])
    client = UnifiedAPIClient(base_config(), session=session)

    resp1 = client.get("/resource")
    resp2 = client.get("/resource")

    assert resp1.content == resp2.content
    assert session.calls == 1


def test_circuit_breaker_opens_and_recovers():
    cfg = base_config(retry_max_attempts=1)
    breaker_cfg = CircuitBreakerConfig(failure_threshold=2, success_threshold=1, timeout_seconds=0.1)

    failing = DummySession([RequestException("fail"), RequestException("fail"), make_response()])
    client = UnifiedAPIClient(cfg, session=failing, circuit_breaker=breaker_cfg)

    with pytest.raises(RequestException):
        client.get("/boom")
    with pytest.raises(RequestException):
        client.get("/boom")
    with pytest.raises(CircuitBreakerOpenError):
        client.get("/boom")

    time.sleep(0.11)
    resp = client.get("/boom")
    assert resp.status_code == 200
