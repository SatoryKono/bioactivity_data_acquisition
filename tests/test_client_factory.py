"""Tests for the unified API client factory."""

from unittest.mock import Mock, patch

import pytest

from library.clients.factory import create_api_client


class TestClientFactory:
    """Test cases for the unified API client factory."""

    def test_create_api_client_chembl(self):
        """Test creating a ChEMBL client."""
        # Create mock config
        config = Mock()
        config.sources = {
            "chembl": Mock()
        }
        config.sources["chembl"].http.timeout_sec = None
        config.sources["chembl"].http.base_url = None
        config.sources["chembl"].http.headers = {}
        config.sources["chembl"].http.retries = {}
        config.sources["chembl"].rate_limit = {}
        config.http.global_.timeout_sec = 30.0
        config.http.global_.headers = {}
        config.http.global_.retries.total = 5
        config.http.global_.retries.backoff_multiplier = 2.0
        
        # Mock the ChEMBLClient import and instantiation
        with patch("library.clients.factory.ChEMBLClient") as mock_client_class:
            mock_client = Mock()
            mock_client_class.return_value = mock_client
            
            # Call the factory function
            result = create_api_client("chembl", config)
            
            # Verify the client was created
            assert result == mock_client
            mock_client_class.assert_called_once()

    def test_create_api_client_unsupported_source(self):
        """Test that unsupported sources raise ValueError."""
        config = Mock()
        config.sources = {}
        
        with pytest.raises(ValueError, match="Unsupported source: invalid_source"):
            create_api_client("invalid_source", config)

    def test_create_api_client_source_not_found(self):
        """Test that missing sources raise ValueError."""
        config = Mock()
        config.sources = {}
        
        with pytest.raises(ValueError, match="Source 'chembl' not found in configuration"):
            create_api_client("chembl", config)

    def test_create_api_client_with_rate_limit(self):
        """Test creating a client with rate limiting."""
        # Create mock config with rate limiting
        config = Mock()
        config.sources = {
            "chembl": Mock()
        }
        config.sources["chembl"].http.timeout_sec = None
        config.sources["chembl"].http.base_url = None
        config.sources["chembl"].http.headers = {}
        config.sources["chembl"].http.retries = {}
        config.sources["chembl"].rate_limit = {
            "max_calls": 10,
            "period": 1.0
        }
        config.http.global_.timeout_sec = 30.0
        config.http.global_.headers = {}
        config.http.global_.retries.total = 5
        config.http.global_.retries.backoff_multiplier = 2.0
        
        # Mock the ChEMBLClient import and instantiation
        with patch("library.clients.factory.ChEMBLClient") as mock_client_class:
            mock_client = Mock()
            mock_client_class.return_value = mock_client
            
            # Call the factory function
            result = create_api_client("chembl", config)
            
            # Verify the client was created
            assert result == mock_client
            mock_client_class.assert_called_once()

    def test_create_api_client_with_headers(self):
        """Test creating a client with custom headers."""
        # Create mock config with custom headers
        config = Mock()
        config.sources = {
            "chembl": Mock()
        }
        config.sources["chembl"].http.timeout_sec = None
        config.sources["chembl"].http.base_url = None
        config.sources["chembl"].http.headers = {"X-Custom": "value"}
        config.sources["chembl"].http.retries = {}
        config.sources["chembl"].rate_limit = {}
        config.http.global_.timeout_sec = 30.0
        config.http.global_.headers = {"User-Agent": "test-agent"}
        config.http.global_.retries.total = 5
        config.http.global_.retries.backoff_multiplier = 2.0
        
        # Mock the ChEMBLClient import and instantiation
        with patch("library.clients.factory.ChEMBLClient") as mock_client_class:
            mock_client = Mock()
            mock_client_class.return_value = mock_client
            
            # Call the factory function
            result = create_api_client("chembl", config)
            
            # Verify the client was created
            assert result == mock_client
            mock_client_class.assert_called_once()

    def test_create_api_client_all_sources(self):
        """Test creating clients for all supported sources."""
        sources = ["chembl", "crossref", "openalex", "pubmed", "semantic_scholar"]
        
        for source in sources:
            # Create mock config
            config = Mock()
            config.sources = {source: Mock()}
            config.sources[source].http.timeout_sec = None
            config.sources[source].http.base_url = None
            config.sources[source].http.headers = {}
            config.sources[source].http.retries = {}
            config.sources[source].rate_limit = {}
            config.http.global_.timeout_sec = 30.0
            config.http.global_.headers = {}
            config.http.global_.retries.total = 5
            config.http.global_.retries.backoff_multiplier = 2.0
            
            # Mock the appropriate client class
            client_class_name = f"{source.title().replace('_', '')}Client"
            with patch(f"library.clients.factory.{client_class_name}") as mock_client_class:
                mock_client = Mock()
                mock_client_class.return_value = mock_client
                
                # Call the factory function
                result = create_api_client(source, config)
                
                # Verify the client was created
                assert result == mock_client
                mock_client_class.assert_called_once()


if __name__ == "__main__":
    pytest.main([__file__])
