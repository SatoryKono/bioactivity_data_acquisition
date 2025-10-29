"""Tests for UnifiedAPIClient."""

import time
from unittest.mock import Mock, patch

import pytest
import requests

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
    policy = RetryPolicy(total=3, backoff_factor=2.0)

    # Should calculate exponential backoff
    assert policy.get_wait_time(0) == 1.0
    assert policy.get_wait_time(1) == 2.0
    assert policy.get_wait_time(2) == 4.0

    # Should use retry_after if provided
    assert policy.get_wait_time(1, retry_after=10.0) == 10.0


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

