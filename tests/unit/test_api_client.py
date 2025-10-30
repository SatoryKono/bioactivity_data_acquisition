"""Tests for UnifiedAPIClient."""

import json
import time
from typing import Any
from unittest.mock import Mock

import pytest
import requests
from requests.structures import CaseInsensitiveDict

from bioetl.core.api_client import (
    APIConfig,
    CircuitBreaker,
    CircuitBreakerOpenError,
    RetryPolicy,
    TokenBucketLimiter,
    UnifiedAPIClient,
)


def test_circuit_breaker():
    """Test circuit breaker opening and closing."""
    cb = CircuitBreaker("test", failure_threshold=3, timeout=0.1)

    # First 2 failures should not open the circuit
    with pytest.raises(RuntimeError):
        cb.call(lambda: (_ for _ in ()).throw(RuntimeError("error")))
    assert cb.failure_count == 1
    assert cb.state == "closed"

    with pytest.raises(RuntimeError):
        cb.call(lambda: (_ for _ in ()).throw(RuntimeError("error")))
    assert cb.failure_count == 2
    assert cb.state == "closed"

    # Third failure should open the circuit
    with pytest.raises(RuntimeError):
        cb.call(lambda: (_ for _ in ()).throw(RuntimeError("error")))
    assert cb.failure_count == 3
    assert cb.state == "open"

    # Circuit should reject calls immediately
    with pytest.raises(CircuitBreakerOpenError):
        cb.call(lambda: "success")

    # After timeout, circuit should go to half-open
    time.sleep(0.15)
    with pytest.raises(RuntimeError):
        cb.call(lambda: (_ for _ in ()).throw(RuntimeError("error")))


def test_token_bucket_limiter():
    """Test token bucket rate limiter."""
    limiter = TokenBucketLimiter(max_calls=2, period=0.1, jitter=False)

    # Should work immediately for first 2 calls
    limiter.acquire()  # 1 token used
    limiter.acquire()  # 2 tokens used

    # Third call should wait
    start_time = time.time()
    limiter.acquire()
    elapsed = time.time() - start_time

    # Should have waited at least 0.1 seconds (with some tolerance)
    assert elapsed >= 0.05  # Allow for timing variance


def test_retry_policy_giveup_on_max_attempts():
    """Test retry policy giving up after max attempts."""
    policy = RetryPolicy(total=3)

    # Should give up after 3 attempts
    assert policy.should_giveup(Exception(), attempt=3) is True
    assert policy.should_giveup(Exception(), attempt=2) is False


def test_retry_policy_fail_fast_on_4xx():
    """Test retry policy fails fast on 4xx errors (except 429)."""
    policy = RetryPolicy(total=5)

    # Mock HTTPError with 400 status
    error = requests.exceptions.HTTPError()
    error.response = Mock(status_code=400)

    assert policy.should_giveup(error, attempt=1) is True

    # Mock HTTPError with 429 status (should retry)
    error429 = requests.exceptions.HTTPError()
    error429.response = Mock(status_code=429)

    assert policy.should_giveup(error429, attempt=1) is False

    # Mock HTTPError with 500 status (should retry)
    error500 = requests.exceptions.HTTPError()
    error500.response = Mock(status_code=500)

    assert policy.should_giveup(error500, attempt=1) is False


def test_retry_policy_allows_configured_status_retry():
    """Retry policy should retry for configured status codes like 404."""

    policy = RetryPolicy(total=3, status_codes=[404])

    error404 = requests.exceptions.HTTPError()
    error404.response = Mock(status_code=404)

    assert policy.should_giveup(error404, attempt=1) is False
    assert policy.should_giveup(error404, attempt=2) is False
    # Should still give up once attempts are exhausted
    assert policy.should_giveup(error404, attempt=3) is True


def test_api_client_initialization():
    """Test UnifiedAPIClient initialization."""
    config = APIConfig(
        name="test",
        base_url="https://api.example.com",
        cache_enabled=True,
        cache_ttl=3600,
    )

    client = UnifiedAPIClient(config)

    assert client.config == config
    assert client.circuit_breaker is not None
    assert client.rate_limiter is not None
    assert client.retry_policy is not None
    assert client.cache is not None


def test_api_client_cache_key():
    """Test cache key generation."""
    config = APIConfig(name="test", base_url="https://api.example.com")
    client = UnifiedAPIClient(config)

    key1 = client._cache_key("https://api.example.com/endpoint", {"param": "value"})
    key2 = client._cache_key("https://api.example.com/endpoint", {"param": "value"})
    key3 = client._cache_key("https://api.example.com/endpoint", {"other": "param"})

    # Same params should generate same key
    assert key1 == key2

    # Different params should generate different key
    assert key1 != key3


def test_retry_policy_wait_time():
    """Test retry policy wait time calculation."""
    policy = RetryPolicy(total=3, backoff_factor=2.0, backoff_max=10.0)

    # Should calculate exponential backoff
    assert policy.get_wait_time(0) == 1.0
    assert policy.get_wait_time(1) == 2.0
    assert policy.get_wait_time(2) == 4.0

    # Should cap by backoff_max
    assert policy.get_wait_time(5) == 10.0

    # Should use retry_after if provided
    assert policy.get_wait_time(1, retry_after=10.0) == 10.0


def _build_response(
    status_code: int,
    payload: dict[str, Any],
    headers: dict[str, str] | None = None,
) -> requests.Response:
    """Construct a minimal Response object for testing."""

    response = requests.Response()
    response.status_code = status_code
    response._content = json.dumps(payload).encode("utf-8")
    response.headers = CaseInsensitiveDict(headers or {})
    response.url = "https://api.example.com/test"
    response.encoding = "utf-8"
    response.reason = requests.status_codes._codes.get(status_code, [""])[0].upper()
    return response


def test_request_json_retries_configured_status(monkeypatch: pytest.MonkeyPatch) -> None:
    """UnifiedAPIClient should retry configured HTTP status codes like 404."""

    config = APIConfig(
        name="test",
        base_url="https://api.example.com",
        cache_enabled=False,
        rate_limit_max_calls=10,
        rate_limit_period=1.0,
        rate_limit_jitter=False,
        retry_total=3,
        retry_backoff_factor=1.0,
        retry_status_codes=[404],
    )

    client = UnifiedAPIClient(config)

    responses = iter(
        [
            _build_response(404, {"error": "missing"}),
            _build_response(404, {"error": "still missing"}),
            _build_response(200, {"result": "ok"}),
        ]
    )

    call_count = {"count": 0}

    def fake_request(*_: Any, **__: Any) -> requests.Response:
        call_count["count"] += 1
        return next(responses)

    monkeypatch.setattr(client.session, "request", fake_request)
    monkeypatch.setattr("bioetl.core.api_client.time.sleep", lambda _: None)

    data = client.request_json("/resource")

    assert data == {"result": "ok"}
    assert call_count["count"] == 3


def test_request_json_retries_on_timeout(monkeypatch: pytest.MonkeyPatch) -> None:
    """UnifiedAPIClient should retry transient errors like timeouts using backoff."""

    config = APIConfig(
        name="test",
        base_url="https://api.example.com",
        cache_enabled=False,
        rate_limit_max_calls=10,
        rate_limit_period=1.0,
        rate_limit_jitter=False,
        retry_total=3,
        retry_backoff_factor=1.0,
    )

    client = UnifiedAPIClient(config)

    call_count = {"count": 0}

    def fake_request(*_: Any, **__: Any) -> requests.Response:
        call_count["count"] += 1
        if call_count["count"] < 3:
            raise requests.exceptions.Timeout("timeout occurred")
        return _build_response(200, {"result": "ok"})

    monkeypatch.setattr(client.session, "request", fake_request)
    monkeypatch.setattr("bioetl.core.api_client.time.sleep", lambda _: None)
    monkeypatch.setattr("backoff._common.sleep", lambda *_: None)

    data = client.request_json("/resource")

    assert data == {"result": "ok"}
    assert call_count["count"] == 3


def test_request_json_retry_metadata_on_exhaustion(monkeypatch: pytest.MonkeyPatch) -> None:
    """UnifiedAPIClient should expose retry metadata when retries are exhausted."""

    config = APIConfig(
        name="test",
        base_url="https://api.example.com",
        cache_enabled=False,
        rate_limit_max_calls=10,
        rate_limit_period=1.0,
        rate_limit_jitter=False,
        retry_total=2,
        retry_backoff_factor=1.0,
        retry_status_codes=[404],
    )

    client = UnifiedAPIClient(config)

    responses = iter(
        [
            _build_response(404, {"error": "missing"}, headers={"Retry-After": "1"}),
            _build_response(404, {"error": "still missing"}, headers={"Retry-After": "2"}),
        ]
    )

    call_count = {"count": 0}

    def fake_request(*_: Any, **__: Any) -> requests.Response:
        call_count["count"] += 1
        return next(responses)

    monkeypatch.setattr(client.session, "request", fake_request)
    monkeypatch.setattr("bioetl.core.api_client.time.sleep", lambda _: None)

    with pytest.raises(requests.exceptions.HTTPError) as excinfo:
        client.request_json("/resource")

    assert call_count["count"] == 2

    metadata = getattr(excinfo.value, "retry_metadata", None)
    assert metadata is not None
    assert metadata["attempt"] == 2
    assert metadata["status_code"] == 404
    assert metadata["retry_after"] == "2"
    assert "still missing" in metadata["error_text"]
    assert metadata["timestamp"] <= time.time()
    assert metadata["timestamp"] > 0


def test_request_json_base_url_join(monkeypatch: pytest.MonkeyPatch) -> None:
    """request_json should normalize base URLs with and without trailing slash."""

    observed_urls: list[str] = []

    for base_url in [
        "https://api.example.com/api",
        "https://api.example.com/api/",
    ]:
        config = APIConfig(name="test", base_url=base_url, rate_limit_jitter=False)
        client = UnifiedAPIClient(config)

        def fake_request(*_: Any, **kwargs: Any) -> requests.Response:
            url = kwargs["url"]
            observed_urls.append(url)
            response = _build_response(200, {"status": "ok"})
            response.url = url
            return response

        monkeypatch.setattr(client.session, "request", fake_request)

        data = client.request_json("/activity.json")
        assert data == {"status": "ok"}

    assert len(observed_urls) == 2
    assert observed_urls[0] == observed_urls[1]


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

