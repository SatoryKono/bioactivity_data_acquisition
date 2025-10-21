"""Tests for health checker functionality."""

from unittest.mock import Mock, patch

import responses
from pydantic import HttpUrl

from library.clients.health import (
    HealthChecker,
    HealthCheckStrategy,
    HealthStatus,
    SimpleHealthClient,
    create_health_checker_from_config,
)
from library.config import APIClientConfig


class TestHealthCheckStrategy:
    """Test health check strategy enum."""
    
    def test_strategy_values(self):
        """Test that strategy enum has expected values."""
        assert HealthCheckStrategy.BASE_URL.value == "base_url"
        assert HealthCheckStrategy.CUSTOM_ENDPOINT.value == "custom_endpoint"
        assert HealthCheckStrategy.DEFAULT_HEALTH.value == "default_health"


class TestSimpleHealthClient:
    """Test SimpleHealthClient functionality."""
    
    def test_init(self):
        """Test client initialization."""
        config = APIClientConfig(
            name="test",
            base_url=HttpUrl("https://api.example.com"),
            headers={"Authorization": "Bearer token"}
        )
        client = SimpleHealthClient(config)
        
        assert client.config == config
        assert client.base_url == "https://api.example.com"
        assert client.session.headers["Authorization"] == "Bearer token"
    
    def test_make_url(self):
        """Test URL construction."""
        config = APIClientConfig(
            name="test",
            base_url=HttpUrl("https://api.example.com")
        )
        client = SimpleHealthClient(config)
        
        assert client._make_url() == "https://api.example.com"
        assert client._make_url("health") == "https://api.example.com/health"
        assert client._make_url("/health") == "https://api.example.com/health"
        assert client._make_url("v1/status") == "https://api.example.com/v1/status"
    
    def test_get_health_check_url_with_custom_endpoint(self):
        """Test health check URL with custom endpoint."""
        config = APIClientConfig(
            name="test",
            base_url=HttpUrl("https://api.example.com"),
            health_endpoint="status"
        )
        client = SimpleHealthClient(config)
        
        assert client.get_health_check_url() == "https://api.example.com/status"
    
    def test_get_health_check_url_without_custom_endpoint(self):
        """Test health check URL without custom endpoint."""
        config = APIClientConfig(
            name="test",
            base_url=HttpUrl("https://api.example.com")
        )
        client = SimpleHealthClient(config)
        
        assert client.get_health_check_url() == "https://api.example.com"
    
    def test_get_health_check_strategy_with_custom_endpoint(self):
        """Test health check strategy with custom endpoint."""
        config = APIClientConfig(
            name="test",
            base_url=HttpUrl("https://api.example.com"),
            health_endpoint="status"
        )
        client = SimpleHealthClient(config)
        
        assert client.get_health_check_strategy() == HealthCheckStrategy.CUSTOM_ENDPOINT
    
    def test_get_health_check_strategy_without_custom_endpoint(self):
        """Test health check strategy without custom endpoint."""
        config = APIClientConfig(
            name="test",
            base_url=HttpUrl("https://api.example.com")
        )
        client = SimpleHealthClient(config)
        
        assert client.get_health_check_strategy() == HealthCheckStrategy.BASE_URL


class TestHealthChecker:
    """Test HealthChecker functionality."""
    
    def test_init(self):
        """Test HealthChecker initialization."""
        clients = {"test": Mock()}
        checker = HealthChecker(clients)
        
        assert checker.clients == clients
        assert checker.console is not None
    
    @responses.activate
    def test_check_client_health_base_url_200(self):
        """Test health check with base URL strategy returning 200."""
        config = APIClientConfig(
            name="test",
            base_url=HttpUrl("https://api.example.com")
        )
        client = SimpleHealthClient(config)
        
        # Mock successful response
        responses.add(
            responses.HEAD,
            "https://api.example.com",
            status=200
        )
        
        checker = HealthChecker({})
        status = checker._check_client_health(client, "test", 10.0)
        
        assert status.name == "test"
        assert status.is_healthy is True
        assert status.response_time_ms is not None
        assert status.error_message is None
    
    @responses.activate
    def test_check_client_health_base_url_404(self):
        """Test health check with base URL strategy returning 404 (should be healthy)."""
        config = APIClientConfig(
            name="test",
            base_url=HttpUrl("https://api.example.com")
        )
        client = SimpleHealthClient(config)
        
        # Mock 404 response (common for external APIs)
        responses.add(
            responses.HEAD,
            "https://api.example.com",
            status=404
        )
        
        checker = HealthChecker({})
        status = checker._check_client_health(client, "test", 10.0)
        
        assert status.name == "test"
        assert status.is_healthy is True  # 404 is acceptable for base URL checks
        assert status.response_time_ms is not None
        assert status.error_message is None
    
    @responses.activate
    def test_check_client_health_base_url_405(self):
        """Test health check with base URL strategy returning 405 (Method Not Allowed)."""
        config = APIClientConfig(
            name="test",
            base_url=HttpUrl("https://api.example.com")
        )
        client = SimpleHealthClient(config)
        
        # Mock 405 response (Method Not Allowed - common for HEAD requests)
        responses.add(
            responses.HEAD,
            "https://api.example.com",
            status=405
        )
        
        checker = HealthChecker({})
        status = checker._check_client_health(client, "test", 10.0)
        
        assert status.name == "test"
        assert status.is_healthy is True  # 405 is acceptable for base URL checks
        assert status.response_time_ms is not None
        assert status.error_message is None
    
    @responses.activate
    def test_check_client_health_base_url_400(self):
        """Test health check with base URL strategy returning 400 (should be unhealthy)."""
        config = APIClientConfig(
            name="test",
            base_url=HttpUrl("https://api.example.com")
        )
        client = SimpleHealthClient(config)
        
        # Mock 400 response
        responses.add(
            responses.HEAD,
            "https://api.example.com",
            status=400
        )
        
        checker = HealthChecker({})
        status = checker._check_client_health(client, "test", 10.0)
        
        assert status.name == "test"
        assert status.is_healthy is False
        assert status.response_time_ms is not None
        assert status.error_message == "HTTP 400"
    
    @responses.activate
    def test_check_client_health_base_url_500(self):
        """Test health check with base URL strategy returning 500 (should be unhealthy)."""
        config = APIClientConfig(
            name="test",
            base_url=HttpUrl("https://api.example.com")
        )
        client = SimpleHealthClient(config)
        
        # Mock 500 response
        responses.add(
            responses.HEAD,
            "https://api.example.com",
            status=500
        )
        
        checker = HealthChecker({})
        status = checker._check_client_health(client, "test", 10.0)
        
        assert status.name == "test"
        assert status.is_healthy is False
        assert status.response_time_ms is not None
        assert status.error_message == "HTTP 500"
    
    @responses.activate
    def test_check_client_health_custom_endpoint_200(self):
        """Test health check with custom endpoint strategy returning 200."""
        config = APIClientConfig(
            name="test",
            base_url=HttpUrl("https://api.example.com"),
            health_endpoint="health"
        )
        client = SimpleHealthClient(config)
        
        # Mock successful response
        responses.add(
            responses.HEAD,
            "https://api.example.com/health",
            status=200
        )
        
        checker = HealthChecker({})
        status = checker._check_client_health(client, "test", 10.0)
        
        assert status.name == "test"
        assert status.is_healthy is True
        assert status.response_time_ms is not None
        assert status.error_message is None
    
    @responses.activate
    def test_check_client_health_custom_endpoint_404(self):
        """Test health check with custom endpoint strategy returning 404 (should be unhealthy)."""
        config = APIClientConfig(
            name="test",
            base_url=HttpUrl("https://api.example.com"),
            health_endpoint="health"
        )
        client = SimpleHealthClient(config)
        
        # Mock 404 response
        responses.add(
            responses.HEAD,
            "https://api.example.com/health",
            status=404
        )
        
        checker = HealthChecker({})
        status = checker._check_client_health(client, "test", 10.0)
        
        assert status.name == "test"
        assert status.is_healthy is False  # 404 is not acceptable for custom endpoints
        assert status.response_time_ms is not None
        assert status.error_message == "HTTP 404"
    
    @responses.activate
    def test_check_client_health_timeout(self):
        """Test health check with timeout."""
        config = APIClientConfig(
            name="test",
            base_url=HttpUrl("https://api.example.com")
        )
        client = SimpleHealthClient(config)
        
        # Mock timeout
        responses.add(
            responses.HEAD,
            "https://api.example.com",
            body=Exception("Connection timeout")
        )
        
        checker = HealthChecker({})
        status = checker._check_client_health(client, "test", 0.1)  # Very short timeout
        
        assert status.name == "test"
        assert status.is_healthy is False
        assert status.response_time_ms is not None
        assert "timeout" in status.error_message.lower() or "connection" in status.error_message.lower()
    
    @responses.activate
    def test_check_client_health_connection_error(self):
        """Test health check with connection error."""
        config = APIClientConfig(
            name="test",
            base_url=HttpUrl("https://api.example.com")
        )
        client = SimpleHealthClient(config)
        
        # Mock connection error
        responses.add(
            responses.HEAD,
            "https://api.example.com",
            body=Exception("Connection refused")
        )
        
        checker = HealthChecker({})
        status = checker._check_client_health(client, "test", 10.0)
        
        assert status.name == "test"
        assert status.is_healthy is False
        assert status.response_time_ms is not None
        assert "connection" in status.error_message.lower()
    
    def test_check_client_health_circuit_breaker_open(self):
        """Test health check with circuit breaker open."""
        config = APIClientConfig(
            name="test",
            base_url=HttpUrl("https://api.example.com")
        )
        client = SimpleHealthClient(config)
        
        # Mock circuit breaker
        mock_circuit_breaker = Mock()
        mock_circuit_breaker.state.value = "open"
        client.circuit_breaker = mock_circuit_breaker
        
        checker = HealthChecker({})
        status = checker._check_client_health(client, "test", 10.0)
        
        assert status.name == "test"
        assert status.is_healthy is False
        assert status.circuit_state == "open"
        assert status.error_message == "Circuit breaker is OPEN"
    
    @responses.activate
    def test_check_all(self):
        """Test checking all clients."""
        config1 = APIClientConfig(
            name="test1",
            base_url=HttpUrl("https://api1.example.com")
        )
        config2 = APIClientConfig(
            name="test2",
            base_url=HttpUrl("https://api2.example.com")
        )
        
        client1 = SimpleHealthClient(config1)
        client2 = SimpleHealthClient(config2)
        
        # Mock responses
        responses.add(
            responses.HEAD,
            "https://api1.example.com",
            status=200
        )
        responses.add(
            responses.HEAD,
            "https://api2.example.com",
            status=404
        )
        
        checker = HealthChecker({"test1": client1, "test2": client2})
        statuses = checker.check_all(timeout=10.0)
        
        assert len(statuses) == 2
        assert all(isinstance(status, HealthStatus) for status in statuses)
        assert all(status.last_check is not None for status in statuses)
    
    def test_check_all_with_exception(self):
        """Test checking all clients with exception."""
        mock_client = Mock()
        mock_client.get_health_check_url.side_effect = Exception("Test error")
        
        checker = HealthChecker({"test": mock_client})
        statuses = checker.check_all(timeout=10.0)
        
        assert len(statuses) == 1
        assert statuses[0].name == "test"
        assert statuses[0].is_healthy is False
        assert "Test error" in statuses[0].error_message


class TestCreateHealthCheckerFromConfig:
    """Test create_health_checker_from_config function."""
    
    def test_create_health_checker_from_config(self):
        """Test creating health checker from config."""
        config1 = APIClientConfig(
            name="test1",
            base_url=HttpUrl("https://api1.example.com")
        )
        config2 = APIClientConfig(
            name="test2",
            base_url=HttpUrl("https://api2.example.com"),
            health_endpoint="status"
        )
        
        configs = {"test1": config1, "test2": config2}
        checker = create_health_checker_from_config(configs)
        
        assert isinstance(checker, HealthChecker)
        assert len(checker.clients) == 2
        assert "test1" in checker.clients
        assert "test2" in checker.clients
        
        # Check that clients are SimpleHealthClient instances
        assert isinstance(checker.clients["test1"], SimpleHealthClient)
        assert isinstance(checker.clients["test2"], SimpleHealthClient)
    
    def test_create_health_checker_from_config_with_invalid_config(self):
        """Test creating health checker with invalid config."""
        # This should not raise an exception, but log a warning
        with patch('library.clients.health.get_logger') as mock_logger:
            configs = {"invalid": "not_a_config"}
            checker = create_health_checker_from_config(configs)
            
            assert isinstance(checker, HealthChecker)
            assert len(checker.clients) == 0
            mock_logger.return_value.warning.assert_called_once()


class TestHealthStatus:
    """Test HealthStatus dataclass."""
    
    def test_health_status_creation(self):
        """Test creating HealthStatus."""
        status = HealthStatus(
            name="test",
            is_healthy=True,
            response_time_ms=100.5,
            error_message=None,
            circuit_state="closed",
            last_check=1234567890.0
        )
        
        assert status.name == "test"
        assert status.is_healthy is True
        assert status.response_time_ms == 100.5
        assert status.error_message is None
        assert status.circuit_state == "closed"
        assert status.last_check == 1234567890.0
    
    def test_health_status_minimal(self):
        """Test creating minimal HealthStatus."""
        status = HealthStatus(
            name="test",
            is_healthy=False
        )
        
        assert status.name == "test"
        assert status.is_healthy is False
        assert status.response_time_ms is None
        assert status.error_message is None
        assert status.circuit_state is None
        assert status.last_check is None
