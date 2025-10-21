"""Tests for GtoPdb enrichment functionality."""

import pytest
import pandas as pd
from unittest.mock import Mock, patch

from library.clients.gtopdb import GtoPdbClient, CircuitBreaker, RateLimiter
from library.target.gtopdb_adapter import (
    GtopdbApiCfg,
    enrich_targets_with_gtopdb,
    _extract_gtop_id,
    _extract_gtop_synonyms,
)
from library.config import APIClientConfig


class TestCircuitBreaker:
    """Test circuit breaker functionality."""
    
    def test_circuit_breaker_initial_state(self):
        """Test circuit breaker starts in closed state."""
        cb = CircuitBreaker(failure_threshold=3, holdoff_seconds=60)
        assert not cb.is_open()
        assert cb._failures == 0
    
    def test_circuit_breaker_opens_on_failures(self):
        """Test circuit breaker opens after threshold failures."""
        cb = CircuitBreaker(failure_threshold=3, holdoff_seconds=60)
        
        # Record failures up to threshold
        assert not cb.record_failure()  # 1st failure
        assert not cb.record_failure()  # 2nd failure
        assert cb.record_failure()      # 3rd failure - should open
        
        assert cb.is_open()
    
    def test_circuit_breaker_resets_on_success(self):
        """Test circuit breaker resets on successful request."""
        cb = CircuitBreaker(failure_threshold=3, holdoff_seconds=60)
        
        # Record some failures
        cb.record_failure()
        cb.record_failure()
        
        # Record success - should reset
        cb.record_success()
        assert cb._failures == 0
        assert not cb.is_open()


class TestRateLimiter:
    """Test rate limiter functionality."""
    
    def test_rate_limiter_initialization(self):
        """Test rate limiter initializes correctly."""
        rl = RateLimiter(rps=2.0, burst=3)
        assert rl.rps == 2.0
        assert rl.burst == 3
    
    def test_rate_limiter_acquire_no_delay_first_request(self):
        """Test first request has no delay."""
        rl = RateLimiter(rps=1.0, burst=1)
        import time
        
        start_time = time.time()
        rl.acquire()
        elapsed = time.time() - start_time
        
        # Should be very fast (less than 0.1 seconds)
        assert elapsed < 0.1


class TestGtopdbApiCfg:
    """Test GtoPdb API configuration."""
    
    def test_default_configuration(self):
        """Test default configuration values."""
        cfg = GtopdbApiCfg()
        
        assert cfg.base_url == "https://www.guidetopharmacology.org/services"
        assert cfg.timeout_sec == 30.0
        assert cfg.rps == 1.0
        assert cfg.burst == 2
        assert cfg.enabled is True
    
    def test_config_from_dict(self):
        """Test configuration from dictionary."""
        config_dict = {
            "enabled": False,
            "http": {
                "base_url": "https://custom.example.com",
                "timeout_sec": 60.0,
                "rate_limit": {
                    "rps": 2.0,
                    "burst": 5
                }
            },
            "circuit_breaker": {
                "failure_threshold": 10,
                "holdoff_seconds": 600
            }
        }
        
        cfg = GtopdbApiCfg(config_dict)
        
        assert not cfg.enabled
        assert cfg.base_url == "https://custom.example.com"
        assert cfg.timeout_sec == 60.0
        assert cfg.rps == 2.0
        assert cfg.burst == 5
        assert cfg.failure_threshold == 10
        assert cfg.holdoff_seconds == 600
    
    def test_to_api_client_config(self):
        """Test conversion to APIClientConfig."""
        cfg = GtopdbApiCfg()
        client_config = cfg.to_api_client_config()
        
        assert isinstance(client_config, APIClientConfig)
        assert client_config.name == "gtopdb"
        assert client_config.base_url == cfg.base_url
        assert client_config.timeout == cfg.timeout_sec


class TestGtopdbClient:
    """Test GtoPdb client functionality."""
    
    @pytest.fixture
    def mock_config(self):
        """Create mock API client configuration."""
        return APIClientConfig(
            name="gtopdb",
            base_url="https://test.example.com",
            timeout=30.0
        )
    
    @pytest.fixture
    def client(self, mock_config):
        """Create GtoPdb client with mock config."""
        return GtoPdbClient(config=mock_config)
    
    def test_client_initialization(self, client):
        """Test client initializes correctly."""
        assert client.config.name == "gtopdb"
        assert isinstance(client.circuit_breaker, CircuitBreaker)
        assert isinstance(client.rate_limiter, RateLimiter)
    
    def test_build_url(self, client):
        """Test URL building for endpoints."""
        url = client._build_url("123", "naturalLigands")
        expected = "https://test.example.com/naturalLigands/123"
        assert url == expected
    
    def test_summarize_function_empty_data(self, client):
        """Test function summarization with empty data."""
        result = client.summarize_function([])
        assert result == ""
        
        result = client.summarize_function(None)
        assert result == ""
    
    def test_summarize_function_with_data(self, client):
        """Test function summarization with actual data."""
        function_data = [
            {
                "description": "Receptor for adenosine",
                "property": "G protein-coupled receptor activity",
                "tissue": "Brain"
            }
        ]
        
        result = client.summarize_function(function_data)
        assert result == "Receptor for adenosine: G protein-coupled receptor activity"
    
    def test_summarize_function_fallback_to_description(self, client):
        """Test function summarization falls back to description."""
        function_data = [
            {
                "description": "Enzyme activity",
                "property": "",
                "tissue": "Liver"
            }
        ]
        
        result = client.summarize_function(function_data)
        assert result == "Enzyme activity"
    
    def test_summarize_function_fallback_to_tissue(self, client):
        """Test function summarization falls back to tissue."""
        function_data = [
            {
                "description": "",
                "property": "",
                "tissue": "Heart"
            }
        ]
        
        result = client.summarize_function(function_data)
        assert result == "Heart"


class TestGtopdbAdapter:
    """Test GtoPdb adapter functions."""
    
    def test_extract_gtop_id_from_guidetopharmacology(self):
        """Test GtoPdb ID extraction from GuidetoPHARMACOLOGY column."""
        row = pd.Series({
            "GuidetoPHARMACOLOGY": "1234|5678|9012",
            "gtop_target_id": "9999"
        })
        
        result = _extract_gtop_id(row)
        assert result == "1234"
    
    def test_extract_gtop_id_from_gtop_target_id(self):
        """Test GtoPdb ID extraction from gtop_target_id column."""
        row = pd.Series({
            "GuidetoPHARMACOLOGY": "",
            "gtop_target_id": "1234"
        })
        
        result = _extract_gtop_id(row)
        assert result == "1234"
    
    def test_extract_gtop_id_not_found(self):
        """Test GtoPdb ID extraction when not found."""
        row = pd.Series({
            "GuidetoPHARMACOLOGY": "",
            "gtop_target_id": ""
        })
        
        result = _extract_gtop_id(row)
        assert result == ""
    
    def test_extract_gtop_synonyms(self):
        """Test GtoPdb synonyms extraction."""
        row = pd.Series({
            "synonyms": "ADORA1|A1AR|A1 receptor",
            "target_name": "Adenosine A1 receptor"
        })
        
        result = _extract_gtop_synonyms(row)
        assert result == "ADORA1|A1AR|A1 receptor"
    
    def test_extract_gtop_synonyms_not_found(self):
        """Test GtoPdb synonyms extraction when not found."""
        row = pd.Series({
            "synonyms": "",
            "target_name": "Test receptor"
        })
        
        result = _extract_gtop_synonyms(row)
        assert result == ""
    
    @patch('library.target.gtopdb_adapter.GtoPdbClient')
    def test_enrich_targets_with_gtopdb_disabled(self, mock_client_class):
        """Test enrichment when GtoPdb is disabled."""
        config = GtopdbApiCfg({"enabled": False})
        targets_df = pd.DataFrame({
            "target_chembl_id": ["CHEMBL123"],
            "GuidetoPHARMACOLOGY": ["1234"]
        })
        
        result = enrich_targets_with_gtopdb(targets_df, config)
        
        # Should return original dataframe without calling client
        assert result.equals(targets_df)
        mock_client_class.assert_not_called()
    
    @patch('library.target.gtopdb_adapter.GtoPdbClient')
    def test_enrich_targets_with_gtopdb_empty_dataframe(self, mock_client_class):
        """Test enrichment with empty dataframe."""
        config = GtopdbApiCfg()
        targets_df = pd.DataFrame()
        
        result = enrich_targets_with_gtopdb(targets_df, config)
        
        assert result.empty
        mock_client_class.assert_not_called()
    
    @patch('library.target.gtopdb_adapter.GtoPdbClient')
    def test_enrich_targets_with_gtopdb_success(self, mock_client_class):
        """Test successful enrichment with GtoPdb data."""
        # Setup mock client
        mock_client = Mock()
        mock_client.fetch_natural_ligands.return_value = [{"id": 1}, {"id": 2}]
        mock_client.fetch_interactions.return_value = [{"id": 1}, {"id": 2}, {"id": 3}]
        mock_client.fetch_function.return_value = [{"description": "Receptor activity"}]
        mock_client.summarize_function.return_value = "Receptor activity"
        mock_client_class.return_value = mock_client
        
        config = GtopdbApiCfg()
        targets_df = pd.DataFrame({
            "target_chembl_id": ["CHEMBL123"],
            "GuidetoPHARMACOLOGY": ["1234"],
            "synonyms": "ADORA1|A1AR"
        })
        
        result = enrich_targets_with_gtopdb(targets_df, config)
        
        # Check that enrichment occurred
        assert "gtop_synonyms" in result.columns
        assert "gtop_natural_ligands_n" in result.columns
        assert "gtop_interactions_n" in result.columns
        assert "gtop_function_text_short" in result.columns
        
        # Check values
        assert result.iloc[0]["gtop_synonyms"] == "ADORA1|A1AR"
        assert result.iloc[0]["gtop_natural_ligands_n"] == "2"
        assert result.iloc[0]["gtop_interactions_n"] == "3"
        assert result.iloc[0]["gtop_function_text_short"] == "Receptor activity"
        
        # Verify client methods were called
        mock_client.fetch_natural_ligands.assert_called_once_with("1234")
        mock_client.fetch_interactions.assert_called_once_with("1234")
        mock_client.fetch_function.assert_called_once_with("1234")


if __name__ == "__main__":
    pytest.main([__file__])
