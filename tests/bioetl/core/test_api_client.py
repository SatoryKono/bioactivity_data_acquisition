"""Unit tests for UnifiedAPIClient with HTTP mocking."""

from __future__ import annotations

import time
from typing import Any
from unittest.mock import MagicMock, patch

import pytest
import requests
from requests import Response
from requests.exceptions import ConnectionError, Timeout

from bioetl.config.models.http import (
    CircuitBreakerConfig,
    HTTPClientConfig,
    RateLimitConfig,
    RetryConfig,
)
from bioetl.core.api_client import (
    CircuitBreaker,
    CircuitBreakerOpenError,
    TokenBucketLimiter,
    UnifiedAPIClient,
)


@pytest.mark.unit
class TestTokenBucketLimiter:
    """Test suite for TokenBucketLimiter."""

    def test_init(self):
        """Test TokenBucketLimiter initialization."""
        limiter = TokenBucketLimiter(max_calls=10, period=1.0)

        assert limiter.max_calls == 10
        assert limiter.period == 1.0
        assert limiter.jitter is True

    def test_init_invalid_max_calls(self):
        """Test that invalid max_calls raises error."""
        with pytest.raises(ValueError, match="max_calls must be > 0"):
            TokenBucketLimiter(max_calls=0, period=1.0)

    def test_init_invalid_period(self):
        """Test that invalid period raises error."""
        with pytest.raises(ValueError, match="period must be > 0"):
            TokenBucketLimiter(max_calls=10, period=0.0)

    def test_acquire_no_wait(self):
        """Test token acquisition when no wait is needed."""
        limiter = TokenBucketLimiter(max_calls=10, period=1.0)

        waited = limiter.acquire()

        assert waited == 0.0

    def test_acquire_with_rate_limit(self):
        """Test token acquisition with rate limiting."""
        limiter = TokenBucketLimiter(max_calls=2, period=1.0, jitter=False)

        # Acquire first two tokens immediately
        assert limiter.acquire() == 0.0
        assert limiter.acquire() == 0.0

        # Third call should wait
        start = time.time()
        waited = limiter.acquire()
        duration = time.time() - start

        assert waited > 0.0
        assert duration >= 0.5  # Should wait at least half the period


@pytest.mark.unit
class TestUnifiedAPIClient:
    """Test suite for UnifiedAPIClient."""

    def test_init(self):
        """Test UnifiedAPIClient initialization."""
        config = HTTPClientConfig()
        client = UnifiedAPIClient(config=config)

        assert client.config == config
        assert client.name == "default"
        assert client.base_url == ""

    def test_init_with_base_url(self):
        """Test initialization with base URL."""
        config = HTTPClientConfig()
        client = UnifiedAPIClient(config=config, base_url="https://api.example.com")

        assert client.base_url == "https://api.example.com"

    def test_init_with_name(self):
        """Test initialization with custom name."""
        config = HTTPClientConfig()
        client = UnifiedAPIClient(config=config, name="custom_client")

        assert client.name == "custom_client"

    def test_close(self):
        """Test client close."""
        config = HTTPClientConfig()
        client = UnifiedAPIClient(config=config)

        # Should not raise
        client.close()

    @patch("bioetl.core.api_client.requests.Session")
    def test_get_success(self, mock_session_class: Any) -> None:
        """Test successful GET request."""
        config = HTTPClientConfig()
        mock_session = MagicMock()
        mock_response = MagicMock(spec=Response)
        mock_response.status_code = 200
        mock_response.json.return_value = {"data": "test"}
        mock_response.headers = {}
        mock_session.request.return_value = mock_response
        mock_session_class.return_value = mock_session

        client = UnifiedAPIClient(config=config, base_url="https://api.example.com")
        response = client.get("/endpoint")

        assert response.status_code == 200
        assert response.json() == {"data": "test"}
        mock_session.request.assert_called_once()

    @patch("bioetl.core.api_client.requests.Session")
    def test_get_with_params(self, mock_session_class: Any) -> None:
        """Test GET request with parameters."""
        config = HTTPClientConfig()
        mock_session = MagicMock()
        mock_response = MagicMock(spec=Response)
        mock_response.status_code = 200
        mock_response.json.return_value = {"data": "test"}
        mock_response.headers = {}
        mock_session.request.return_value = mock_response
        mock_session_class.return_value = mock_session

        client = UnifiedAPIClient(config=config)
        response = client.get("/endpoint", params={"key": "value"})

        assert response.status_code == 200
        call_kwargs = mock_session.request.call_args[1]
        assert call_kwargs["params"] == {"key": "value"}

    @patch("bioetl.core.api_client.requests.Session")
    def test_get_with_headers(self, mock_session_class: Any) -> None:
        """Test GET request with custom headers."""
        config = HTTPClientConfig()
        mock_session = MagicMock()
        mock_response = MagicMock(spec=Response)
        mock_response.status_code = 200
        mock_response.json.return_value = {"data": "test"}
        mock_response.headers = {}
        mock_session.request.return_value = mock_response
        mock_session_class.return_value = mock_session

        client = UnifiedAPIClient(config=config)
        response = client.get("/endpoint", headers={"X-Custom": "value"})

        assert response.status_code == 200

    @patch("bioetl.core.api_client.requests.Session")
    def test_get_switches_to_post_when_url_too_long(self, mock_session_class: Any) -> None:
        """GET requests exceeding the max URL length should fallback to POST."""

        config = HTTPClientConfig(max_url_length=50)
        mock_session = MagicMock()
        mock_response = MagicMock(spec=Response)
        mock_response.status_code = 200
        mock_response.json.return_value = {"data": "test"}
        mock_response.headers = {}
        mock_session.request.return_value = mock_response
        mock_session_class.return_value = mock_session

        client = UnifiedAPIClient(config=config, base_url="https://api.example.com")
        params = {"ids": ",".join(str(i) for i in range(20))}

        response = client.get("/endpoint", params=params)

        assert response.status_code == 200
        mock_session.request.assert_called_once()
        method, _url = mock_session.request.call_args[0]
        kwargs = mock_session.request.call_args[1]
        assert method == "POST"
        assert kwargs.get("data") == params
        headers = kwargs.get("headers")
        assert headers is not None
        assert headers.get("X-HTTP-Method-Override") == "GET"
        assert kwargs.get("params") in (None, {})

    @patch("bioetl.core.api_client.requests.Session")
    def test_get_retry_on_429(self, mock_session_class: Any) -> None:
        """Test retry on 429 status code."""
        config = HTTPClientConfig(
            retries=RetryConfig(total=2, backoff_multiplier=1.0, backoff_max=1.0)
        )
        mock_session = MagicMock()
        mock_response_429 = MagicMock(spec=Response)
        mock_response_429.status_code = 429
        mock_response_429.headers = {"Retry-After": "1"}
        mock_response_200 = MagicMock(spec=Response)
        mock_response_200.status_code = 200
        mock_response_200.json.return_value = {"data": "test"}
        mock_response_200.headers = {}
        mock_session.request.side_effect = [mock_response_429, mock_response_200]
        mock_session_class.return_value = mock_session

        client = UnifiedAPIClient(config=config)
        response = client.get("/endpoint")

        assert response.status_code == 200
        assert mock_session.request.call_count == 2

    @patch("bioetl.core.api_client.requests.Session")
    def test_get_retry_on_500(self, mock_session_class: Any) -> None:
        """Test retry on 500 status code."""
        config = HTTPClientConfig(
            retries=RetryConfig(total=2, backoff_multiplier=1.0, backoff_max=1.0)
        )
        mock_session = MagicMock()
        mock_response_500 = MagicMock(spec=Response)
        mock_response_500.status_code = 500
        mock_response_500.headers = {}
        mock_response_200 = MagicMock(spec=Response)
        mock_response_200.status_code = 200
        mock_response_200.json.return_value = {"data": "test"}
        mock_response_200.headers = {}
        mock_session.request.side_effect = [mock_response_500, mock_response_200]
        mock_session_class.return_value = mock_session

        client = UnifiedAPIClient(config=config)
        response = client.get("/endpoint")

        assert response.status_code == 200
        assert mock_session.request.call_count == 2

    @patch("bioetl.core.api_client.requests.Session")
    def test_get_retry_exhausted(self, mock_session_class: Any) -> None:
        """Test retry exhaustion."""
        config = HTTPClientConfig(
            retries=RetryConfig(total=2, backoff_multiplier=1.0, backoff_max=1.0)
        )
        mock_session = MagicMock()
        mock_response_500 = MagicMock(spec=Response)
        mock_response_500.status_code = 500
        mock_response_500.headers = {}
        mock_response_500.raise_for_status = MagicMock()
        mock_session.request.return_value = mock_response_500
        mock_session_class.return_value = mock_session

        client = UnifiedAPIClient(config=config)

        with pytest.raises(Timeout):
            client.get("/endpoint")

        assert mock_session.request.call_count == 3  # Initial + 2 retries

    @patch("bioetl.core.api_client.requests.Session")
    def test_get_connection_error_with_retry(self, mock_session_class: Any) -> None:
        """Test retry on connection error."""
        config = HTTPClientConfig(
            retries=RetryConfig(total=1, backoff_multiplier=1.0, backoff_max=1.0)
        )
        mock_session = MagicMock()
        mock_session.request.side_effect = [
            ConnectionError("Connection failed"),
            MagicMock(spec=Response),
        ]
        mock_response = MagicMock(spec=Response)
        mock_response.status_code = 200
        mock_response.json.return_value = {"data": "test"}
        mock_response.headers = {}
        mock_session.request.side_effect = [ConnectionError("Connection failed"), mock_response]
        mock_session_class.return_value = mock_session

        client = UnifiedAPIClient(config=config)
        response = client.get("/endpoint")

        assert response.status_code == 200
        assert mock_session.request.call_count == 2

    @patch("bioetl.core.api_client.requests.Session")
    def test_get_timeout_error(self, mock_session_class: Any) -> None:
        """Test timeout error handling."""
        config = HTTPClientConfig(
            retries=RetryConfig(total=1, backoff_multiplier=1.0, backoff_max=1.0)
        )
        mock_session = MagicMock()
        mock_session.request.side_effect = Timeout("Request timeout")
        mock_session_class.return_value = mock_session

        client = UnifiedAPIClient(config=config)

        with pytest.raises(Timeout, match="Request timeout"):
            client.get("/endpoint")

    @patch("bioetl.core.api_client.requests.Session")
    def test_get_400_error(self, mock_session_class: Any) -> None:
        """Test that 400 errors are not retried."""
        config = HTTPClientConfig()
        mock_session = MagicMock()
        mock_response = MagicMock(spec=Response)
        mock_response.status_code = 400
        mock_response.headers = {}
        mock_response.raise_for_status = MagicMock(
            side_effect=requests.exceptions.HTTPError("400 Client Error")
        )
        mock_session.request.return_value = mock_response
        mock_session_class.return_value = mock_session

        client = UnifiedAPIClient(config=config)

        with pytest.raises(requests.exceptions.HTTPError):
            client.get("/endpoint")

        assert mock_session.request.call_count == 1  # No retry for 400

    @patch("bioetl.core.api_client.requests.Session")
    def test_request_json(self, mock_session_class: Any) -> None:
        """Test request_json method."""
        config = HTTPClientConfig()
        mock_session = MagicMock()
        mock_response = MagicMock(spec=Response)
        mock_response.status_code = 200
        mock_response.json.return_value = {"data": "test"}
        mock_response.headers = {}
        mock_session.request.return_value = mock_response
        mock_session_class.return_value = mock_session

        client = UnifiedAPIClient(config=config)
        result = client.request_json("GET", "/endpoint")

        assert result == {"data": "test"}

    @patch("bioetl.core.api_client.requests.Session")
    def test_request_with_absolute_url(self, mock_session_class: Any) -> None:
        """Test request with absolute URL."""
        config = HTTPClientConfig()
        mock_session = MagicMock()
        mock_response = MagicMock(spec=Response)
        mock_response.status_code = 200
        mock_response.json.return_value = {"data": "test"}
        mock_response.headers = {}
        mock_session.request.return_value = mock_response
        mock_session_class.return_value = mock_session

        client = UnifiedAPIClient(config=config, base_url="https://api.example.com")
        response = client.get("https://other.example.com/endpoint")

        assert response.status_code == 200
        # Should use absolute URL as-is
        call_args = mock_session.request.call_args
        assert "https://other.example.com/endpoint" in str(call_args)

    @patch("bioetl.core.api_client.requests.Session")
    def test_request_with_relative_url(self, mock_session_class: Any) -> None:
        """Test request with relative URL."""
        config = HTTPClientConfig()
        mock_session = MagicMock()
        mock_response = MagicMock(spec=Response)
        mock_response.status_code = 200
        mock_response.json.return_value = {"data": "test"}
        mock_response.headers = {}
        mock_session.request.return_value = mock_response
        mock_session_class.return_value = mock_session

        client = UnifiedAPIClient(config=config, base_url="https://api.example.com")
        response = client.get("/endpoint")

        assert response.status_code == 200
        call_args = mock_session.request.call_args
        assert "https://api.example.com/endpoint" in str(call_args)

    @patch("bioetl.core.api_client.requests.Session")
    def test_rate_limiting(self, mock_session_class: Any) -> None:
        """Test rate limiting."""
        config = HTTPClientConfig(rate_limit=RateLimitConfig(max_calls=2, period=1.0))
        mock_session = MagicMock()
        mock_response = MagicMock(spec=Response)
        mock_response.status_code = 200
        mock_response.json.return_value = {"data": "test"}
        mock_response.headers = {}
        mock_session.request.return_value = mock_response
        mock_session_class.return_value = mock_session

        client = UnifiedAPIClient(config=config)

        # Make multiple requests
        start = time.time()
        for _ in range(3):
            client.get("/endpoint")
        duration = time.time() - start

        # Should have waited for rate limit
        assert duration >= 0.3  # At least some wait time
        assert mock_session.request.call_count == 3

    def test_should_retry(self) -> None:
        """Test retry decision logic."""
        config = HTTPClientConfig(retries=RetryConfig(statuses=(429, 500)))
        client = UnifiedAPIClient(config=config)

        assert client._should_retry(429) is True  # type: ignore[reportPrivateUsage]
        assert client._should_retry(500) is True  # type: ignore[reportPrivateUsage]
        assert client._should_retry(502) is True  # type: ignore[reportPrivateUsage] # 5xx range
        assert client._should_retry(400) is False  # type: ignore[reportPrivateUsage]
        assert client._should_retry(200) is False  # type: ignore[reportPrivateUsage]

    def test_compute_backoff(self) -> None:
        """Test backoff computation."""
        config = HTTPClientConfig(
            retries=RetryConfig(backoff_multiplier=2.0, backoff_max=10.0),
            rate_limit_jitter=False,
        )
        client = UnifiedAPIClient(config=config)

        from bioetl.core.api_client import _RetryState  # type: ignore[reportPrivateUsage]

        # First attempt (index 0)
        backoff = client._compute_backoff(_RetryState(attempt=1, response=None))  # type: ignore[reportPrivateUsage]
        assert backoff == 1.0  # 2^0 = 1

        # Second attempt (index 1)
        backoff = client._compute_backoff(_RetryState(attempt=2, response=None))  # type: ignore[reportPrivateUsage]
        assert backoff == 2.0  # 2^1 = 2

        # Third attempt (index 2)
        backoff = client._compute_backoff(_RetryState(attempt=3, response=None))  # type: ignore[reportPrivateUsage]
        assert backoff == 4.0  # 2^2 = 4

    def test_compute_backoff_with_retry_after(self) -> None:
        """Test backoff with Retry-After header."""
        config = HTTPClientConfig()
        client = UnifiedAPIClient(config=config)

        from bioetl.core.api_client import _RetryState  # type: ignore[reportPrivateUsage]

        backoff = client._compute_backoff(_RetryState(attempt=2, response=None, retry_after=5.0))  # type: ignore[reportPrivateUsage]
        assert backoff == 5.0  # Should use Retry-After value

    def test_resolve_url_absolute(self) -> None:
        """Test URL resolution with absolute URL."""
        config = HTTPClientConfig()
        client = UnifiedAPIClient(config=config, base_url="https://api.example.com")

        url = client._resolve_url("https://other.example.com/endpoint")  # type: ignore[reportPrivateUsage]
        assert url == "https://other.example.com/endpoint"

    def test_resolve_url_relative(self) -> None:
        """Test URL resolution with relative URL."""
        config = HTTPClientConfig()
        client = UnifiedAPIClient(config=config, base_url="https://api.example.com")

        url = client._resolve_url("/endpoint")  # type: ignore[reportPrivateUsage]
        assert url == "https://api.example.com/endpoint"

    def test_resolve_url_no_base(self) -> None:
        """Test URL resolution without base URL."""
        config = HTTPClientConfig()
        client = UnifiedAPIClient(config=config)

        url = client._resolve_url("/endpoint")  # type: ignore[reportPrivateUsage]
        assert url == "/endpoint"


@pytest.mark.unit
class TestCircuitBreaker:
    """Test suite for CircuitBreaker."""

    def test_init(self):
        """Test CircuitBreaker initialization."""
        config = CircuitBreakerConfig()
        cb = CircuitBreaker(config)

        assert cb.config == config
        assert cb.name == "default"
        assert cb.state == "closed"
        assert cb.failure_count == 0

    def test_init_with_name(self):
        """Test CircuitBreaker initialization with custom name."""
        config = CircuitBreakerConfig()
        cb = CircuitBreaker(config, name="custom_cb")

        assert cb.name == "custom_cb"

    def test_call_success_closed(self):
        """Test successful call in closed state."""
        config = CircuitBreakerConfig(failure_threshold=3)
        cb = CircuitBreaker(config)

        def func():
            return "success"

        result = cb.call(func)

        assert result == "success"
        assert cb.state == "closed"
        assert cb.failure_count == 0

    def test_call_failure_closed(self):
        """Test failure in closed state."""
        config = CircuitBreakerConfig(failure_threshold=3)
        cb = CircuitBreaker(config)

        def func():
            raise ValueError("error")

        with pytest.raises(ValueError, match="error"):
            cb.call(func)

        assert cb.state == "closed"
        assert cb.failure_count == 1

    def test_call_opens_after_threshold(self):
        """Test circuit breaker opens after threshold failures."""
        config = CircuitBreakerConfig(failure_threshold=3, timeout=1.0)
        cb = CircuitBreaker(config)

        def failing_func():
            raise ConnectionError("connection failed")

        # Fail 3 times to open circuit
        for _ in range(3):
            with pytest.raises(ConnectionError):
                cb.call(failing_func)

        assert cb.state == "open"
        assert cb.failure_count == 3

        # Next call should raise CircuitBreakerOpenError
        with pytest.raises(CircuitBreakerOpenError):
            cb.call(failing_func)

    def test_call_transitions_to_half_open(self):
        """Test circuit breaker transitions to half-open after timeout."""
        config = CircuitBreakerConfig(failure_threshold=2, timeout=0.1)
        cb = CircuitBreaker(config)

        def failing_func():
            raise ConnectionError("connection failed")

        # Open the circuit
        for _ in range(2):
            with pytest.raises(ConnectionError):
                cb.call(failing_func)

        assert cb.state == "open"

        # Wait for timeout
        time.sleep(0.15)

        # Next call should allow a trial execution (half-open) but failure reopens the circuit
        with pytest.raises(ConnectionError):
            cb.call(failing_func)

        assert cb.state == "open"

    def test_call_success_in_half_open_closes(self):
        """Test successful call in half-open state closes circuit."""
        config = CircuitBreakerConfig(failure_threshold=2, timeout=0.1)
        cb = CircuitBreaker(config)

        def failing_func():
            raise ConnectionError("connection failed")

        def success_func():
            return "success"

        # Open the circuit
        for _ in range(2):
            with pytest.raises(ConnectionError):
                cb.call(failing_func)

        assert cb.state == "open"

        # Wait for timeout
        time.sleep(0.15)

        # Successful call should close circuit
        result = cb.call(success_func)

        assert result == "success"
        assert cb.state == "closed"
        assert cb.failure_count == 0

    def test_call_failure_in_half_open_reopens(self):
        """Test failure in half-open state reopens circuit."""
        config = CircuitBreakerConfig(failure_threshold=2, timeout=0.1)
        cb = CircuitBreaker(config)

        def failing_func():
            raise ConnectionError("connection failed")

        # Open the circuit
        for _ in range(2):
            with pytest.raises(ConnectionError):
                cb.call(failing_func)

        assert cb.state == "open"

        # Wait for timeout
        time.sleep(0.15)

        # Failure in half-open should reopen circuit
        with pytest.raises(ConnectionError):
            cb.call(failing_func)

        assert cb.state == "open"


@pytest.mark.unit
class TestUnifiedAPIClientCircuitBreaker:
    """Test suite for UnifiedAPIClient with Circuit Breaker."""

    @patch("bioetl.core.api_client.requests.Session")
    def test_circuit_breaker_opens_on_failures(self, mock_session_class: Any) -> None:
        """Test circuit breaker opens after threshold failures."""
        config = HTTPClientConfig(
            circuit_breaker=CircuitBreakerConfig(failure_threshold=3, timeout=60.0),
            retries=RetryConfig(total=0),  # Disable retries for cleaner test
        )
        mock_session = MagicMock()
        mock_response_500 = MagicMock(spec=Response)
        mock_response_500.status_code = 500
        mock_response_500.headers = {}
        mock_response_500.raise_for_status = MagicMock(
            side_effect=requests.exceptions.HTTPError("500 Server Error")
        )
        mock_session.request.return_value = mock_response_500
        mock_session_class.return_value = mock_session

        client = UnifiedAPIClient(config=config)

        # Fail 3 times to open circuit
        for _ in range(3):
            with pytest.raises(requests.exceptions.HTTPError):
                client.get("/endpoint")

        # Next call should raise CircuitBreakerOpenError
        with pytest.raises(CircuitBreakerOpenError):
            client.get("/endpoint")

        assert client._circuit_breaker.state == "open"  # type: ignore[reportPrivateUsage]

    @patch("bioetl.core.api_client.requests.Session")
    def test_circuit_breaker_recovers_after_timeout(self, mock_session_class: Any) -> None:
        """Test circuit breaker recovers after timeout."""
        config = HTTPClientConfig(
            circuit_breaker=CircuitBreakerConfig(failure_threshold=2, timeout=0.1),
            retries=RetryConfig(total=0),
        )
        mock_session = MagicMock()
        mock_response_500 = MagicMock(spec=Response)
        mock_response_500.status_code = 500
        mock_response_500.headers = {}
        mock_response_500.raise_for_status = MagicMock(
            side_effect=requests.exceptions.HTTPError("500 Server Error")
        )
        mock_response_200 = MagicMock(spec=Response)
        mock_response_200.status_code = 200
        mock_response_200.json.return_value = {"data": "test"}
        mock_response_200.headers = {}
        mock_session.request.side_effect = [mock_response_500, mock_response_500, mock_response_200]
        mock_session_class.return_value = mock_session

        client = UnifiedAPIClient(config=config)

        # Open the circuit
        for _ in range(2):
            with pytest.raises(requests.exceptions.HTTPError):
                client.get("/endpoint")

        assert client._circuit_breaker.state == "open"  # type: ignore[reportPrivateUsage]

        # Wait for timeout
        time.sleep(0.15)

        # Successful call should close circuit
        response = client.get("/endpoint")
        assert response.status_code == 200
        assert client._circuit_breaker.state == "closed"  # type: ignore[reportPrivateUsage]
