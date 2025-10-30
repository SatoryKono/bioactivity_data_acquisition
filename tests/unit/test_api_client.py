"""Tests for UnifiedAPIClient."""

import json
import time
from datetime import datetime, timedelta, timezone
from email.utils import format_datetime
from typing import Any, Callable
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
    assert policy.get_wait_time(1, retry_after="15") == 15.0


def test_retry_after_parsing_numeric_and_http_date(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Retry-After header should support seconds and HTTP-date formats."""

    config = APIConfig(name="test", base_url="https://api.example.com")
    client = UnifiedAPIClient(config)

    numeric_response = _build_response(
        429,
        {"error": "rate limited"},
        headers={"Retry-After": "12"},
    )

    assert client._retry_after_seconds(numeric_response) == 12.0

    fixed_now = datetime(2024, 1, 1, 0, 0, tzinfo=timezone.utc)
    monkeypatch.setattr("bioetl.core.api_client._current_utc_time", lambda: fixed_now)

    http_date = format_datetime(fixed_now + timedelta(seconds=30))
    http_date_response = _build_response(
        429,
        {"error": "rate limited"},
        headers={"Retry-After": http_date},
    )

    wait_seconds = client._retry_after_seconds(http_date_response)
    assert wait_seconds == pytest.approx(30.0)


def test_retry_after_parsing_fixed_http_date(monkeypatch: pytest.MonkeyPatch) -> None:
    """Retry-After parser should handle canonical RFC 1123 date strings."""

    config = APIConfig(name="test", base_url="https://api.example.com")
    client = UnifiedAPIClient(config)

    # Freeze current time to ensure deterministic delta calculation.
    fixed_now = datetime(2015, 10, 21, 7, 27, tzinfo=timezone.utc)
    monkeypatch.setattr("bioetl.core.api_client._current_utc_time", lambda: fixed_now)

    http_date_header = "Wed, 21 Oct 2015 07:28:00 GMT"
    http_date_response = _build_response(
        429,
        {"error": "rate limited"},
        headers={"Retry-After": http_date_header},
    )

    wait_seconds = client._retry_after_seconds(http_date_response)
    assert wait_seconds == pytest.approx(60.0)


@pytest.mark.parametrize(
    "retry_after_builder, expected_wait",
    [
        (lambda now: "3", 3.0),
        (
            lambda now: format_datetime(now + timedelta(seconds=5)),
            5.0,
        ),
    ],
)
def test_execute_reuses_same_request_arguments_after_retry_after(
    monkeypatch: pytest.MonkeyPatch,
    retry_after_builder: Callable[[datetime], str],
    expected_wait: float,
) -> None:
    """_execute should honour Retry-After and retry with identical parameters."""

    config = APIConfig(name="test", base_url="https://api.example.com", rate_limit_jitter=False)
    client = UnifiedAPIClient(config)

    base_time = datetime(2024, 1, 1, tzinfo=timezone.utc)
    monkeypatch.setattr("bioetl.core.api_client._current_utc_time", lambda: base_time)

    retry_after_value = retry_after_builder(base_time)

    responses = iter(
        [
            _build_response(
                429,
                {"error": "rate limited"},
                headers={"Retry-After": retry_after_value},
            ),
            _build_response(200, {"result": "ok"}),
        ]
    )

    recorded_requests: list[dict[str, Any]] = []

    def fake_request(**kwargs: Any) -> requests.Response:
        recorded_requests.append(dict(kwargs))
        return next(responses)

    acquire_calls: list[object] = []

    def fake_acquire() -> None:
        acquire_calls.append(object())

    sleep_calls: list[float] = []

    monkeypatch.setattr(client.session, "request", fake_request)
    monkeypatch.setattr(client.rate_limiter, "acquire", fake_acquire)
    monkeypatch.setattr("bioetl.core.api_client.time.sleep", lambda seconds: sleep_calls.append(seconds))

    final_response = client._execute(
        method="GET",
        url="https://api.example.com/resource",
        params={"foo": "bar"},
        json={"payload": 1},
    )

    assert final_response.status_code == 200
    assert len(recorded_requests) == 2
    assert recorded_requests[0] == recorded_requests[1]
    assert sleep_calls == [pytest.approx(expected_wait)]
    assert len(acquire_calls) == 2


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


def test_request_json_uses_cache_for_idempotent_get(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Second GET request without body should be served from the cache."""

    config = APIConfig(
        name="test",
        base_url="https://api.example.com",
        cache_enabled=True,
        cache_ttl=60,
        cache_maxsize=32,
    )

    client = UnifiedAPIClient(config)

    call_count = {"count": 0}

    def fake_execute(
        *,
        method: str,
        url: str,
        params: dict[str, Any] | None = None,
        data: dict[str, Any] | None = None,
        json: dict[str, Any] | None = None,
    ) -> requests.Response:
        call_count["count"] += 1
        return _build_response(200, {"result": "ok"})

    monkeypatch.setattr(client, "_execute", fake_execute)

    first_payload = client.request_json("/resource")
    second_payload = client.request_json("/resource")

    assert first_payload == {"result": "ok"}
    assert second_payload == first_payload
    assert call_count["count"] == 1


def test_request_json_cache_returns_deepcopy(monkeypatch: pytest.MonkeyPatch) -> None:
    """Cached responses should not be mutated by callers."""

    config = APIConfig(
        name="test",
        base_url="https://api.example.com",
        cache_enabled=True,
        cache_ttl=60,
        cache_maxsize=32,
    )

    client = UnifiedAPIClient(config)

    call_count = {"count": 0}

    def fake_request(*_: Any, **__: Any) -> requests.Response:
        call_count["count"] += 1
        return _build_response(200, {"result": "ok"})

    monkeypatch.setattr(client.session, "request", fake_request)

    first_payload = client.request_json("/resource")
    first_payload["result"] = "mutated"

    second_payload = client.request_json("/resource")

    assert second_payload == {"result": "ok"}
    assert second_payload is not first_payload
    assert call_count["count"] == 1


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

    def fake_execute(
        *,
        method: str,
        url: str,
        params: dict[str, Any] | None = None,
        data: dict[str, Any] | None = None,
        json: dict[str, Any] | None = None,
    ) -> requests.Response:
        call_count["count"] += 1
        return next(responses)

    monkeypatch.setattr(client, "_execute", fake_execute)
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

    def fake_execute(
        *,
        method: str,
        url: str,
        params: dict[str, Any] | None = None,
        data: dict[str, Any] | None = None,
        json: dict[str, Any] | None = None,
    ) -> requests.Response:
        call_count["count"] += 1
        if call_count["count"] < 3:
            raise requests.exceptions.Timeout("timeout occurred")
        return _build_response(200, {"result": "ok"})

    monkeypatch.setattr(client, "_execute", fake_execute)
    monkeypatch.setattr("bioetl.core.api_client.time.sleep", lambda _: None)

    data = client.request_json("/resource")

    assert data == {"result": "ok"}
    assert call_count["count"] == 3


def test_request_json_timeout_metadata_on_exhaustion(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Timeout errors should propagate retry metadata when retries are exhausted."""

    config = APIConfig(
        name="test",
        base_url="https://api.example.com",
        cache_enabled=False,
        rate_limit_max_calls=10,
        rate_limit_period=1.0,
        rate_limit_jitter=False,
        retry_total=2,
        retry_backoff_factor=1.0,
    )

    client = UnifiedAPIClient(config)

    def fake_execute(
        *,
        method: str,
        url: str,
        params: dict[str, Any] | None = None,
        data: dict[str, Any] | None = None,
        json: dict[str, Any] | None = None,
    ) -> requests.Response:
        raise requests.exceptions.Timeout("timeout occurred")

    monkeypatch.setattr(client, "_execute", fake_execute)
    monkeypatch.setattr("bioetl.core.api_client.time.sleep", lambda _: None)

    with pytest.raises(requests.exceptions.Timeout) as excinfo:
        client.request_json("/resource")

    metadata = getattr(excinfo.value, "retry_metadata", None)
    assert metadata is not None
    assert metadata["attempt"] == 2
    assert metadata["status_code"] is None
    assert metadata["retry_after"] is None
    assert metadata["error_text"] == "timeout occurred"


def test_request_json_respects_retry_total(monkeypatch: pytest.MonkeyPatch) -> None:
    """request_json should perform at most retry_total attempts."""

    config = APIConfig(
        name="test",
        base_url="https://api.example.com",
        cache_enabled=False,
        rate_limit_max_calls=100,
        rate_limit_period=1.0,
        rate_limit_jitter=False,
        retry_total=2,
        retry_backoff_factor=0.0,
    )

    client = UnifiedAPIClient(config)

    call_count = {"count": 0}

    def fake_execute(
        self,
        *,
        method: str,
        url: str,
        params: dict[str, Any] | None = None,
        data: dict[str, Any] | None = None,
        json: dict[str, Any] | None = None,
    ) -> requests.Response:
        call_count["count"] += 1
        raise requests.exceptions.ConnectionError("network down")

    monkeypatch.setattr(UnifiedAPIClient, "_execute", fake_execute)
    monkeypatch.setattr("bioetl.core.api_client.time.sleep", lambda _: None)

    with pytest.raises(requests.exceptions.ConnectionError):
        client.request_json("/resource")

    assert call_count["count"] == 2

    client.close()


def test_request_json_connection_error_metadata(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Connection errors should include retry metadata and respect headers."""

    config = APIConfig(
        name="test",
        base_url="https://api.example.com",
        cache_enabled=False,
        rate_limit_max_calls=10,
        rate_limit_period=1.0,
        rate_limit_jitter=False,
        retry_total=2,
        retry_backoff_factor=1.0,
    )

    client = UnifiedAPIClient(config)

    error_responses = iter(
        [
            _build_response(
                503,
                {"error": "down"},
                headers={"Retry-After": "1"},
            ),
            _build_response(
                503,
                {"error": "down"},
                headers={"Retry-After": "1"},
            ),
            _build_response(
                503,
                {"error": "still down"},
                headers={"Retry-After": "2"},
            ),
            _build_response(
                503,
                {"error": "still down"},
                headers={"Retry-After": "2"},
            ),
        ]
    )

    def fake_execute(
        *,
        method: str,
        url: str,
        params: dict[str, Any] | None = None,
        data: dict[str, Any] | None = None,
        json: dict[str, Any] | None = None,
    ) -> requests.Response:
        response = next(error_responses)
        exc = requests.exceptions.ConnectionError("connection issue")
        exc.response = response
        raise exc

    monkeypatch.setattr(client, "_execute", fake_execute)
    monkeypatch.setattr("bioetl.core.api_client.time.sleep", lambda _: None)

    with pytest.raises(requests.exceptions.ConnectionError) as excinfo:
        client.request_json("/resource")

    metadata = getattr(excinfo.value, "retry_metadata", None)
    assert metadata is not None
    assert metadata["attempt"] == 2
    assert metadata["status_code"] == 503
    assert metadata["retry_after"] == "2"
    assert "still down" in metadata["error_text"]


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

    def fake_execute(
        *,
        method: str,
        url: str,
        params: dict[str, Any] | None = None,
        data: dict[str, Any] | None = None,
        json: dict[str, Any] | None = None,
    ) -> requests.Response:
        call_count["count"] += 1
        return next(responses)

    monkeypatch.setattr(client, "_execute", fake_execute)
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

        def fake_execute(
            *,
            method: str,
            url: str,
            params: dict[str, Any] | None = None,
            data: dict[str, Any] | None = None,
            json: dict[str, Any] | None = None,
        ) -> requests.Response:
            observed_urls.append(url)
            response = _build_response(200, {"status": "ok"})
            response.url = url
            return response

        monkeypatch.setattr(client, "_execute", fake_execute)

        data = client.request_json("/activity.json")
        assert data == {"status": "ok"}

    assert len(observed_urls) == 2
    assert observed_urls[0] == observed_urls[1]


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

