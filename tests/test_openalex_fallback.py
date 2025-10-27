"""Tests for OpenAlex fallback mechanisms."""

import pytest
from unittest.mock import Mock, patch
from library.clients.openalex import OpenAlexClient
from library.clients.fallback import OpenAlexFallbackStrategy, FallbackConfig
from library.clients.graceful_degradation import OpenAlexDegradationStrategy, DegradationConfig
from library.clients.exceptions import ApiClientError
from library.config import APIClientConfig


class TestOpenAlexTitleSearchFallback:
    """Test title-based search fallback functionality."""
    
    def test_search_by_title_success(self):
        """Test successful title search fallback."""
        config = APIClientConfig(name="openalex", base_url="https://api.openalex.org")
        client = OpenAlexClient(config)
        
        # Mock successful search response
        mock_response = Mock()
        mock_response.json.return_value = {
            "results": [{
                "id": "https://openalex.org/W123456789",
                "display_name": "Test Article Title",
                "publication_year": 2023,
                "type": "journal-article",
                "ids": {"doi": "https://doi.org/10.1000/test"},
                "authorships": [{"author": {"display_name": "John Doe"}}]
            }]
        }
        
        with patch.object(client, '_request', return_value=mock_response):
            result = client._search_by_title("Test Article Title", "test-doi")
            
            assert result["source"] == "openalex"
            assert result["openalex_title"] == "Test Article Title"
            assert result["openalex_year"] == 2023
            assert result["openalex_error"] is None
    
    def test_search_by_title_no_results(self):
        """Test title search with no results."""
        config = APIClientConfig(name="openalex", base_url="https://api.openalex.org")
        client = OpenAlexClient(config)
        
        # Mock empty search response
        mock_response = Mock()
        mock_response.json.return_value = {"results": []}
        
        with patch.object(client, '_request', return_value=mock_response):
            result = client._search_by_title("Non-existent Title", "test-doi")
            
            assert result["source"] == "openalex"
            assert result["openalex_title"] is None
            assert "Not found by title search" in result["openalex_error"]
    
    def test_search_by_title_exception(self):
        """Test title search with exception."""
        config = APIClientConfig(name="openalex", base_url="https://api.openalex.org")
        client = OpenAlexClient(config)
        
        with patch.object(client, '_request', side_effect=Exception("API Error")):
            result = client._search_by_title("Test Title", "test-doi")
            
            assert result["source"] == "openalex"
            assert result["openalex_title"] is None
            assert "Title search failed" in result["openalex_error"]
    
    def test_clean_title_for_search(self):
        """Test title cleaning functionality."""
        config = APIClientConfig(name="openalex", base_url="https://api.openalex.org")
        client = OpenAlexClient(config)
        
        # Test HTML tag removal
        dirty_title = "<b>Test</b> <i>Article</i> Title"
        clean_title = client._clean_title_for_search(dirty_title)
        assert clean_title == "Test Article Title"
        
        # Test whitespace normalization
        spaced_title = "  Test   Article   Title  "
        clean_title = client._clean_title_for_search(spaced_title)
        assert clean_title == "Test Article Title"
        
        # Test length truncation
        long_title = "A" * 300
        clean_title = client._clean_title_for_search(long_title)
        assert len(clean_title) == 200
        
        # Test empty title
        clean_title = client._clean_title_for_search("")
        assert clean_title == ""


class TestOpenAlexFetchMethodsWithTitleFallback:
    """Test fetch methods with title fallback integration."""
    
    def test_fetch_by_doi_with_title_fallback(self):
        """Test DOI fetch with title fallback."""
        config = APIClientConfig(name="openalex", base_url="https://api.openalex.org")
        client = OpenAlexClient(config)
        
        # Mock failed DOI lookup, successful title search
        mock_response = Mock()
        mock_response.json.return_value = {
            "results": [{
                "id": "https://openalex.org/W123456789",
                "display_name": "Test Article Title",
                "publication_year": 2023,
                "type": "journal-article",
                "ids": {"doi": "https://doi.org/10.1000/test"},
                "authorships": [{"author": {"display_name": "John Doe"}}]
            }]
        }
        
        with patch.object(client, '_request', side_effect=[
            ApiClientError("DOI not found", status_code=404),
            ApiClientError("Filter failed", status_code=404),
            mock_response  # Title search succeeds
        ]):
            result = client.fetch_by_doi("10.1000/test", title="Test Article Title")
            
            assert result["source"] == "openalex"
            assert result["openalex_title"] == "Test Article Title"
            assert result["openalex_year"] == 2023
    
    def test_fetch_by_pmid_with_title_fallback(self):
        """Test PMID fetch with title fallback."""
        config = APIClientConfig(name="openalex", base_url="https://api.openalex.org")
        client = OpenAlexClient(config)
        
        # Mock failed PMID lookup, successful title search
        mock_response = Mock()
        mock_response.json.return_value = {
            "results": [{
                "id": "https://openalex.org/W123456789",
                "display_name": "Test Article Title",
                "publication_year": 2023,
                "type": "journal-article",
                "ids": {"pmid": "https://pubmed.ncbi.nlm.nih.gov/12345678"},
                "authorships": [{"author": {"display_name": "John Doe"}}]
            }]
        }
        
        with patch.object(client, '_request', side_effect=[
            ApiClientError("PMID not found", status_code=404),
            ApiClientError("Filter failed", status_code=404),
            ApiClientError("Search failed", status_code=404),
            mock_response  # Title search succeeds
        ]):
            result = client.fetch_by_pmid("12345678", title="Test Article Title")
            
            assert result["source"] == "openalex"
            assert result["openalex_title"] == "Test Article Title"
            assert result["openalex_year"] == 2023


class TestOpenAlexFallbackStrategy:
    """Test OpenAlex fallback strategy."""
    
    def test_should_retry_rate_limit(self):
        """Test retry logic for rate limiting."""
        config = FallbackConfig(max_retries=1)
        strategy = OpenAlexFallbackStrategy(config)
        
        error = Mock()
        error.status_code = 429
        
        # Should retry once for rate limiting
        assert strategy.should_retry(error, 0) is True
        assert strategy.should_retry(error, 1) is False
    
    def test_should_retry_server_error(self):
        """Test retry logic for server errors."""
        config = FallbackConfig(max_retries=1)
        strategy = OpenAlexFallbackStrategy(config)
        
        error = Mock()
        error.status_code = 500
        
        # Should retry once for server errors
        assert strategy.should_retry(error, 0) is True
        assert strategy.should_retry(error, 1) is False
    
    def test_should_retry_other_errors(self):
        """Test retry logic for other errors."""
        config = FallbackConfig(max_retries=1)
        strategy = OpenAlexFallbackStrategy(config)
        
        error = Mock()
        error.status_code = 404
        
        # Should not retry for other errors
        assert strategy.should_retry(error, 0) is False
    
    def test_get_delay_rate_limit(self):
        """Test delay calculation for rate limiting."""
        config = FallbackConfig(base_delay=5.0, jitter=False)
        strategy = OpenAlexFallbackStrategy(config)
        
        error = Mock()
        error.status_code = 429
        
        delay = strategy.get_delay(error, 0)
        assert delay == 5.0  # 5.0 + (0 * 3.0)
        
        delay = strategy.get_delay(error, 1)
        assert delay == 8.0  # 5.0 + (1 * 3.0)
    
    def test_get_fallback_data(self):
        """Test fallback data generation."""
        config = FallbackConfig()
        strategy = OpenAlexFallbackStrategy(config)
        
        error = Mock()
        error.status_code = 500
        
        fallback_data = strategy.get_fallback_data(error)
        
        assert fallback_data["source"] == "fallback"
        assert fallback_data["fallback_used"] is True
        assert "OpenAlex API error: 500" in fallback_data["fallback_reason"]


class TestOpenAlexDegradationStrategy:
    """Test OpenAlex degradation strategy."""
    
    def test_should_degrade_rate_limit(self):
        """Test degradation decision for rate limiting."""
        config = DegradationConfig()
        strategy = OpenAlexDegradationStrategy(config)
        
        error = Mock()
        error.status_code = 429
        assert strategy.should_degrade(error) is True
    
    def test_should_degrade_server_error(self):
        """Test degradation decision for server errors."""
        config = DegradationConfig()
        strategy = OpenAlexDegradationStrategy(config)
        
        error = Mock()
        error.status_code = 500
        assert strategy.should_degrade(error) is True
    
    def test_should_not_degrade_other_errors(self):
        """Test degradation decision for other errors."""
        config = DegradationConfig()
        strategy = OpenAlexDegradationStrategy(config)
        
        error = Mock()
        error.status_code = 404
        assert strategy.should_degrade(error) is False
    
    def test_get_fallback_data(self):
        """Test fallback data generation."""
        config = DegradationConfig()
        strategy = OpenAlexDegradationStrategy(config)
        
        original_request = {"doi": "10.1000/test"}
        error = Mock()
        error.status_code = 500
        
        fallback_data = strategy.get_fallback_data(original_request, error)
        
        assert fallback_data["source"] == "fallback"
        assert fallback_data["api"] == "openalex"
        assert fallback_data["fallback_reason"] == "openalex_unavailable"
        assert fallback_data["degraded"] is True
        assert fallback_data["openalex_title"] is None
        assert fallback_data["openalex_error"] == "<Mock id='...'>"
    
    def test_get_degraded_response(self):
        """Test degraded response processing."""
        config = DegradationConfig()
        strategy = OpenAlexDegradationStrategy(config)
        
        partial_data = [{"openalex_title": "Test Title"}]
        error = Mock()
        error.status_code = 500
        
        degraded_data = strategy.get_degraded_response(partial_data, error)
        
        assert len(degraded_data) == 1
        assert degraded_data[0]["degradation_info"]["source"] == "openalex"
        assert degraded_data[0]["degradation_info"]["partial"] is True
        assert degraded_data[0]["degradation_info"]["error"] == "<Mock id='...'>"


class TestOpenAlexRateLimitHandling:
    """Test rate limit handling in OpenAlex."""
    
    def test_rate_limit_429_handling(self):
        """Test handling of 429 rate limit errors."""
        config = APIClientConfig(name="openalex", base_url="https://api.openalex.org")
        client = OpenAlexClient(config)
        
        error = Mock()
        error.status_code = 429
        
        with patch.object(client, '_request', side_effect=error):
            result = client.fetch_by_doi("10.1000/test")
            
            assert result["source"] == "openalex"
            assert result["openalex_title"] is None
            assert "Rate limited" in result["openalex_error"]
    
    def test_rate_limit_with_title_fallback(self):
        """Test rate limit handling with title fallback."""
        config = APIClientConfig(name="openalex", base_url="https://api.openalex.org")
        client = OpenAlexClient(config)
        
        # Mock rate limit error, then successful title search
        mock_response = Mock()
        mock_response.json.return_value = {
            "results": [{
                "id": "https://openalex.org/W123456789",
                "display_name": "Test Article Title",
                "publication_year": 2023,
                "type": "journal-article",
                "ids": {"doi": "https://doi.org/10.1000/test"},
                "authorships": [{"author": {"display_name": "John Doe"}}]
            }]
        }
        
        with patch.object(client, '_request', side_effect=[
            ApiClientError("Rate limited", status_code=429),
            ApiClientError("Rate limited", status_code=429),
            mock_response  # Title search succeeds
        ]):
            result = client.fetch_by_doi("10.1000/test", title="Test Article Title")
            
            assert result["source"] == "openalex"
            assert result["openalex_title"] == "Test Article Title"
            assert result["openalex_year"] == 2023


if __name__ == "__main__":
    pytest.main([__file__])
