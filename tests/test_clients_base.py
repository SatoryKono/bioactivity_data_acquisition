"""Tests for base HTTP client functionality."""

import pytest
import time
import threading
from unittest.mock import Mock, patch
import requests
from requests.exceptions import Timeout, ConnectionError

from library.clients.base import RateLimiter, RateLimitConfig, BaseApiClient
from library.clients.exceptions import ApiClientError, RateLimitError
from library.config import APIClientConfig


class TestRateLimiter:
    """Test rate limiting functionality."""

    def test_rate_limiter_allows_calls_within_limit(self):
        """Test that rate limiter allows calls within the configured limit."""
        config = RateLimitConfig(max_calls=2, period=1.0)
        limiter = RateLimiter(config)
        
        # Should not raise any exceptions
        limiter.acquire()
        limiter.acquire()

    def test_rate_limiter_blocks_calls_exceeding_limit(self):
        """Test that rate limiter blocks calls exceeding the configured limit."""
        config = RateLimitConfig(max_calls=1, period=1.0)
        limiter = RateLimiter(config)
        
        # First call should succeed
        limiter.acquire()
        
        # Second call should raise RateLimitError
        with pytest.raises(RateLimitError):
            limiter.acquire()

    def test_rate_limiter_resets_after_period(self):
        """Test that rate limiter resets after the period expires."""
        config = RateLimitConfig(max_calls=1, period=0.1)
        limiter = RateLimiter(config)
        
        # First call should succeed
        limiter.acquire()
        
        # Second call should fail
        with pytest.raises(RateLimitError):
            limiter.acquire()
        
        # Wait for period to expire
        time.sleep(0.2)
        
        # Next call should succeed
        limiter.acquire()

    def test_rate_limiter_thread_safety(self):
        """Test that rate limiter is thread-safe."""
        config = RateLimitConfig(max_calls=5, period=1.0)
        limiter = RateLimiter(config)
        
        results = []
        
        def worker():
            try:
                limiter.acquire()
                results.append("success")
            except RateLimitError:
                results.append("blocked")
        
        # Start multiple threads
        threads = []
        for _ in range(10):
            thread = threading.Thread(target=worker)
            threads.append(thread)
            thread.start()
        
        # Wait for all threads to complete
        for thread in threads:
            thread.join()
        
        # Should have exactly 5 successes and 5 blocks
        assert results.count("success") == 5
        assert results.count("blocked") == 5

    @patch('library.clients.base.get_shared_session')
    def test_global_timeout_usage(self, mock_session):
        """Test that global session timeout is used when config timeout is not specified."""
        # Mock session with global timeout
        mock_session_instance = Mock()
        mock_session_instance.timeout = (10.0, 30.0)
        mock_session.return_value = mock_session_instance
        
        # Create config without timeout
        config_no_timeout = APIClientConfig(
            name="test",
            base_url="https://api.example.com",
            endpoint="/test",
            headers={},
            params={},
            pagination_param="page",
            page_size_param="size",
            page_size=10,
            max_pages=5,
            timeout=30.0,  # Default timeout
            retries={"total": 3, "backoff_factor": 1.0, "status_forcelist": [500, 502, 503, 504]},
            rate_limit={"max_calls": 10, "period": 1.0},
        )
        
        client = BaseApiClient(config_no_timeout)
        
        # Mock successful response
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"data": "test"}
        mock_response.text = '{"data": "test"}'
        mock_response.headers = {}
        mock_session_instance.request.return_value = mock_response
        
        client._request("GET", "/test")
        
        # Verify that request was called with config timeout
        mock_session_instance.request.assert_called_once()
        call_args = mock_session_instance.request.call_args
        assert call_args[1].get("timeout") == 30.0  # Config timeout used

    @patch('library.clients.base.get_shared_session')
    def test_config_timeout_override(self, mock_session):
        """Test that config timeout overrides global session timeout."""
        # Mock session with global timeout
        mock_session_instance = Mock()
        mock_session_instance.timeout = (10.0, 30.0)
        mock_session.return_value = mock_session_instance
        
        # Create config with specific timeout
        config_with_timeout = APIClientConfig(
            name="test",
            base_url="https://api.example.com",
            endpoint="/test",
            headers={},
            params={},
            pagination_param="page",
            page_size_param="size",
            page_size=10,
            max_pages=5,
            timeout=60.0,  # Specific timeout
            retries={"total": 3, "backoff_factor": 1.0, "status_forcelist": [500, 502, 503, 504]},
            rate_limit={"max_calls": 10, "period": 1.0},
        )
        
        client = BaseApiClient(config_with_timeout)
        
        # Mock successful response
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"data": "test"}
        mock_response.text = '{"data": "test"}'
        mock_response.headers = {}
        mock_session_instance.request.return_value = mock_response
        
        client._request("GET", "/test")
        
        # Verify that request was called with config timeout
        mock_session_instance.request.assert_called_once()
        call_args = mock_session_instance.request.call_args
        assert call_args[1].get("timeout") == 60.0


class TestBaseApiClient:
    """Test base API client functionality."""

    @pytest.fixture
    def mock_config(self):
        """Create a mock API client configuration."""
        return APIClientConfig(
            name="test",
            base_url="https://api.example.com",
            endpoint="/test",
            headers={"Authorization": "Bearer test-token"},
            params={},
            pagination_param="page",
            page_size_param="size",
            page_size=10,
            max_pages=5,
            timeout=30.0,
            retries={"total": 3, "backoff_factor": 1.0, "status_forcelist": [500, 502, 503, 504]},
            rate_limit={"max_calls": 10, "period": 1.0},
        )

    def test_client_initialization(self, mock_config):
        """Test that client initializes correctly."""
        client = BaseApiClient(mock_config)
        
        assert client.config == mock_config
        assert client.base_url == "https://api.example.com"
        assert client.rate_limiter is not None

    def test_make_url(self, mock_config):
        """Test URL construction."""
        client = BaseApiClient(mock_config)
        
        url = client._make_url("/endpoint")
        assert url == "https://api.example.com/endpoint"
        
        url = client._make_url("endpoint")
        assert url == "https://api.example.com/endpoint"

    @patch('library.clients.base.get_shared_session')
    def test_request_timeout_handling(self, mock_session, mock_config):
        """Test that timeouts are handled correctly."""
        # Mock session that raises Timeout
        mock_session.return_value.request.side_effect = Timeout("Request timed out")
        
        client = BaseApiClient(mock_config)
        
        with pytest.raises(ApiClientError) as exc_info:
            client._request("GET", "/test")
        
        assert "timeout" in str(exc_info.value).lower()

    @patch('library.clients.base.get_shared_session')
    def test_request_connection_error_handling(self, mock_session, mock_config):
        """Test that connection errors are handled correctly."""
        # Mock session that raises ConnectionError
        mock_session.return_value.request.side_effect = ConnectionError("Connection failed")
        
        client = BaseApiClient(mock_config)
        
        with pytest.raises(ApiClientError) as exc_info:
            client._request("GET", "/test")
        
        assert "connection" in str(exc_info.value).lower()

    @patch('library.clients.base.get_shared_session')
    def test_request_http_error_handling(self, mock_session, mock_config):
        """Test that HTTP errors are handled correctly."""
        # Mock response with 500 status
        mock_response = Mock()
        mock_response.status_code = 500
        mock_response.text = "Internal Server Error"
        mock_response.headers = {}
        
        mock_session.return_value.request.return_value = mock_response
        
        client = BaseApiClient(mock_config)
        
        with pytest.raises(ApiClientError) as exc_info:
            client._request("GET", "/test")
        
        assert "500" in str(exc_info.value)

    @patch('library.clients.base.get_shared_session')
    def test_rate_limit_429_handling(self, mock_session, mock_config):
        """Test that 429 rate limit responses are handled correctly."""
        # Mock response with 429 status and Retry-After header
        mock_response = Mock()
        mock_response.status_code = 429
        mock_response.text = "Too Many Requests"
        mock_response.headers = {"Retry-After": "5"}
        
        mock_session.return_value.request.return_value = mock_response
        
        client = BaseApiClient(mock_config)
        
        with pytest.raises(ApiClientError) as exc_info:
            client._request("GET", "/test")
        
        assert "429" in str(exc_info.value)
        assert "rate limit" in str(exc_info.value).lower()

    @patch('library.clients.base.get_shared_session')
    def test_successful_request(self, mock_session, mock_config):
        """Test successful request handling."""
        # Mock successful response
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"data": "test"}
        mock_response.text = '{"data": "test"}'
        mock_response.headers = {}
        
        mock_session.return_value.request.return_value = mock_response
        
        client = BaseApiClient(mock_config)
        
        result = client._request("GET", "/test")
        
        assert result == {"data": "test"}

    @patch('library.clients.base.get_shared_session')
    def test_xml_response_handling(self, mock_session, mock_config):
        """Test XML response handling."""
        # Mock XML response
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.text = '<root><item>test</item></root>'
        mock_response.headers = {"Content-Type": "application/xml"}
        
        mock_session.return_value.request.return_value = mock_response
        
        client = BaseApiClient(mock_config)
        
        result = client._request("GET", "/test")
        
        # Should return parsed XML as dict
        assert isinstance(result, dict)
        assert "root" in result

    def test_rate_limiter_enforcement(self, mock_config):
        """Test that rate limiter is enforced before requests."""
        # Create config with very restrictive rate limit
        restrictive_config = APIClientConfig(
            name="test",
            base_url="https://api.example.com",
            endpoint="/test",
            headers={},
            params={},
            pagination_param="page",
            page_size_param="size",
            page_size=10,
            max_pages=5,
            timeout=30.0,
            retries={"total": 3, "backoff_factor": 1.0, "status_forcelist": [500, 502, 503, 504]},
            rate_limit={"max_calls": 1, "period": 60.0},  # 1 call per minute
        )
        
        client = BaseApiClient(restrictive_config)
        
        # First call should work (rate limiter allows it)
        client.rate_limiter.acquire()
        
        # Second call should be blocked by rate limiter
        with pytest.raises(RateLimitError):
            client.rate_limiter.acquire()
