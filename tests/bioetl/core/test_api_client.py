"""Unit tests for UnifiedAPIClient with HTTP mocking."""

from __future__ import annotations

import time
from typing import Any
from unittest.mock import MagicMock, patch

import pytest
import requests
from requests import Response

from bioetl.config.models.policies import (
    CircuitBreakerConfig,
    HTTPClientConfig,
    RateLimitConfig,
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
        config = HTTPClientConfig()
        client = UnifiedAPIClient(config=config)

        assert client.config == config
        assert client.name == "default"
        assert client.base_url == ""

    def test_init_with_custom_name_and_base_url(self):
        config = HTTPClientConfig()
        client = UnifiedAPIClient(
            config=config, base_url="https://api.example.com", name="custom"
        )

        assert client.base_url == "https://api.example.com"
        assert client.name == "custom"

    def test_close(self):
        config = HTTPClientConfig()
        client = UnifiedAPIClient(config=config, base_url="https://api.example.com")
        client.close()  # Should not raise

    @patch("bioetl.core.api_client.get_api_client")
    def test_get_success(self, mock_factory: Any) -> None:
        config = HTTPClientConfig()
        mock_session = MagicMock()
        mock_response = MagicMock(spec=Response)
        mock_response.status_code = 200
        mock_response.json.return_value = {"data": "ok"}
        mock_session.request.return_value = mock_response
        mock_factory.return_value = mock_session

        client = UnifiedAPIClient(config=config, base_url="https://api.example.com")
        response = client.get("/endpoint", params={"foo": "bar"}, headers={"X-ID": "123"})

        assert response.json() == {"data": "ok"}
        mock_session.request.assert_called_once()
        _, call_kwargs = mock_session.request.call_args
        assert call_kwargs["params"] == {"foo": "bar"}
        assert call_kwargs["headers"]["X-ID"] == "123"

    @patch("bioetl.core.api_client.get_api_client")
    def test_get_switches_to_post_when_url_too_long(self, mock_factory: Any) -> None:
        config = HTTPClientConfig(max_url_length=50)
        mock_session = MagicMock()
        mock_response = MagicMock(spec=Response)
        mock_response.status_code = 200
        mock_session.request.return_value = mock_response
        mock_factory.return_value = mock_session

        client = UnifiedAPIClient(config=config, base_url="https://api.example.com")
        params = {"ids": ",".join(str(i) for i in range(20))}

        response = client.get("/endpoint", params=params)

        assert response.status_code == 200
        method, _url = mock_session.request.call_args[0]
        kwargs = mock_session.request.call_args[1]
        assert method == "POST"
        assert kwargs["data"] == params
        assert kwargs["headers"]["X-HTTP-Method-Override"] == "GET"

    @patch("bioetl.core.api_client.get_api_client")
    def test_get_raises_for_client_error(self, mock_factory: Any) -> None:
        config = HTTPClientConfig()
        mock_session = MagicMock()
        mock_response = MagicMock(spec=Response)
        mock_response.status_code = 400
        mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError("bad")
        mock_session.request.return_value = mock_response
        mock_factory.return_value = mock_session

        client = UnifiedAPIClient(config=config)

        with pytest.raises(requests.exceptions.HTTPError):
            client.get("/endpoint")

    @patch("bioetl.core.api_client.get_api_client")
    def test_request_json(self, mock_factory: Any) -> None:
        config = HTTPClientConfig()
        mock_session = MagicMock()
        mock_response = MagicMock(spec=Response)
        mock_response.status_code = 200
        mock_response.json.return_value = {"key": "value"}
        mock_session.request.return_value = mock_response
        mock_factory.return_value = mock_session

        client = UnifiedAPIClient(config=config)
        payload = client.request_json("GET", "/resource")

        assert payload == {"key": "value"}

    @patch("bioetl.core.api_client.get_api_client")
    def test_request_with_absolute_url(self, mock_factory: Any) -> None:
        config = HTTPClientConfig()
        mock_session = MagicMock()
        mock_response = MagicMock(spec=Response)
        mock_response.status_code = 200
        mock_session.request.return_value = mock_response
        mock_factory.return_value = mock_session

        client = UnifiedAPIClient(config=config, base_url="https://api.example.com")
        client.get("https://other.example.com/items")

        method, url = mock_session.request.call_args[0]
        assert method == "GET"
        assert url == "https://other.example.com/items"

    @patch("bioetl.core.api_client.get_api_client")
    def test_request_with_relative_url(self, mock_factory: Any) -> None:
        config = HTTPClientConfig()
        mock_session = MagicMock()
        mock_response = MagicMock(spec=Response)
        mock_response.status_code = 200
        mock_session.request.return_value = mock_response
        mock_factory.return_value = mock_session

        client = UnifiedAPIClient(config=config, base_url="https://api.example.com")
        client.get("/items")

        method, url = mock_session.request.call_args[0]
        assert method == "GET"
        assert url == "https://api.example.com/items"

    @patch("bioetl.core.api_client.get_api_client")
    def test_rate_limiting(self, mock_factory: Any) -> None:
        config = HTTPClientConfig(rate_limit=RateLimitConfig(max_calls=2, period=1.0))
        mock_session = MagicMock()
        mock_response = MagicMock(spec=Response)
        mock_response.status_code = 200
        mock_session.request.return_value = mock_response
        mock_factory.return_value = mock_session

        client = UnifiedAPIClient(config=config)

        start = time.time()
        for _ in range(3):
            client.get("/endpoint")
        duration = time.time() - start

        assert duration >= 0.3
        assert mock_session.request.call_count == 3

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

    @patch("bioetl.core.api_client.get_api_client")
    def test_circuit_breaker_opens_on_failures(self, mock_factory: Any) -> None:
        """Test circuit breaker opens after threshold failures."""
        config = HTTPClientConfig(
            circuit_breaker=CircuitBreakerConfig(failure_threshold=3, timeout=60.0),
        )
        mock_session = MagicMock()
        mock_session.headers = {}
        mock_response_500 = MagicMock(spec=Response)
        mock_response_500.status_code = 500
        mock_response_500.headers = {}
        mock_response_500.raise_for_status = MagicMock(
            side_effect=requests.exceptions.HTTPError("500 Server Error")
        )
        mock_session.request.return_value = mock_response_500
        mock_factory.return_value = mock_session

        client = UnifiedAPIClient(config=config, base_url="https://api.example.com")

        # Fail 3 times to open circuit
        for _ in range(3):
            with pytest.raises(requests.exceptions.HTTPError):
                client.get("/endpoint")

        # Next call should raise CircuitBreakerOpenError
        with pytest.raises(CircuitBreakerOpenError):
            client.get("/endpoint")

        assert client._circuit_breaker.state == "open"  # type: ignore[reportPrivateUsage]

    @patch("bioetl.core.api_client.get_api_client")
    def test_circuit_breaker_recovers_after_timeout(self, mock_factory: Any) -> None:
        """Test circuit breaker recovers after timeout."""
        config = HTTPClientConfig(
            circuit_breaker=CircuitBreakerConfig(failure_threshold=2, timeout=0.1),
        )
        mock_session = MagicMock()
        mock_session.headers = {}
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
        mock_factory.return_value = mock_session

        client = UnifiedAPIClient(config=config, base_url="https://api.example.com")

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
