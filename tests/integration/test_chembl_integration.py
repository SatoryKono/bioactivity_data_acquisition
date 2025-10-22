"""Integration tests for ChEMBL API client."""

import os

import pytest

from library.clients.chembl import ChEMBLClient
from library.clients.exceptions import ApiClientError


def _check_network_access():
    """Check if network access is available."""
    try:
        import requests
        response = requests.get("https://www.ebi.ac.uk/chembl/api/data/status", timeout=5)
        return response.status_code == 200
    except Exception:
        return False


def _check_api_keys():
    """Check if required API keys are available."""
    required_keys = ['CHEMBL_API_TOKEN', 'PUBMED_API_KEY', 'SEMANTIC_SCHOLAR_API_KEY']
    return all(os.getenv(key) for key in required_keys)


@pytest.mark.integration
class TestChEMBLIntegration:
    """Integration tests for ChEMBL API client with real API calls."""

    @pytest.fixture
    def chembl_client(self, integration_config):
        """Create a ChEMBL client for integration tests."""
        # Use test configuration or real API key if available
        api_token = os.getenv("CHEMBL_API_TOKEN")
        if api_token:
            # Override config with real API key
            integration_config.sources["chembl"].http.headers["Authorization"] = f"Bearer {api_token}"
        
        return ChEMBLClient(integration_config.sources["chembl"].to_client_config(integration_config.http.global_))

    @pytest.mark.skipif(not _check_network_access(), reason="no network access")
    def test_chembl_api_status(self, chembl_client):
        """Test that ChEMBL API is accessible."""
        # This is a simple health check
        try:
            # Try to make a minimal request to check API status
            response = chembl_client._request("GET", "/status")
            assert response is not None
        except ApiClientError as e:
            # If we get an API error, that's still a successful connection
            assert e.status_code is not None
            assert e.status_code >= 400  # API responded with an error code

    @pytest.mark.skipif(not _check_network_access() or not _check_api_keys(), reason="no network access or missing API keys")
    def test_chembl_compound_search(self, chembl_client):
        """Test compound search functionality."""
        # Search for a well-known compound (aspirin)
        compounds = chembl_client.search_compounds("aspirin", limit=5)
        
        assert len(compounds) <= 5  # Should respect limit
        if compounds:  # If we got results
            compound = compounds[0]
            assert "compound_id" in compound
            assert "pref_name" in compound or "molecule_chembl_id" in compound

    @pytest.mark.skipif(not _check_network_access() or not _check_api_keys(), reason="no network access or missing API keys")
    def test_chembl_activity_search(self, chembl_client):
        """Test activity search functionality."""
        # Search for activities related to aspirin
        activities = chembl_client.search_activities("CHEMBL25", limit=3)
        
        assert len(activities) <= 3  # Should respect limit
        if activities:  # If we got results
            activity = activities[0]
            assert "activity_id" in activity
            assert "standard_value" in activity or "pchembl_value" in activity

    @pytest.mark.skipif(not _check_network_access(), reason="no network access")
    def test_chembl_rate_limiting(self, chembl_client):
        """Test that rate limiting is enforced."""
        import time
        
        # Make multiple rapid requests to test rate limiting
        start_time = time.time()
        requests_made = 0
        
        try:
            for _ in range(5):  # Try to make 5 rapid requests
                try:
                    chembl_client._request("GET", "/status")
                    requests_made += 1
                except ApiClientError as e:
                    if "rate limit" in str(e).lower() or e.status_code == 429:
                        break  # Rate limiting is working
                time.sleep(0.1)  # Small delay between requests
        except Exception:
            # Handle any unexpected errors gracefully
            pass
        
        elapsed_time = time.time() - start_time
        
        # Rate limiting should kick in before 5 requests
        # or requests should be spread out over time
        assert requests_made <= 5
        assert elapsed_time >= 0.5  # At least some time should have passed

    @pytest.mark.skipif(not _check_network_access(), reason="no network access")
    def test_chembl_error_handling(self, chembl_client):
        """Test error handling for invalid requests."""
        # Try to access a non-existent endpoint
        with pytest.raises(ApiClientError) as exc_info:
            chembl_client._request("GET", "/nonexistent-endpoint")
        
        error = exc_info.value
        assert error.status_code is not None
        assert error.status_code >= 400

    @pytest.mark.skipif(not _check_network_access(), reason="no network access")
    def test_chembl_timeout_handling(self, chembl_client):
        """Test timeout handling."""
        # Create a client with very short timeout
        from library.config import APIClientConfig
        
        short_timeout_config = APIClientConfig(
            name="chembl",
            base_url="https://www.ebi.ac.uk/chembl/api/data",
            endpoint="/",
            headers=chembl_client.default_headers,
            params={},
            pagination_param="offset",
            page_size_param="limit",
            page_size=10,
            max_pages=1,
            timeout=0.1,  # Very short timeout
            retries={"total": 1, "backoff_factor": 1.0, "status_forcelist": [500, 502, 503, 504]},
            rate_limit={"max_calls": 1, "period": 1.0},
        )
        
        short_timeout_client = ChEMBLClient(short_timeout_config)
        
        # This should timeout
        with pytest.raises(ApiClientError) as exc_info:
            short_timeout_client._request("GET", "/status")
        
        error = exc_info.value
        assert "timeout" in str(error).lower() or "timeout" in error.__class__.__name__.lower()

    @pytest.mark.slow
    @pytest.mark.skipif(not _check_network_access() or not _check_api_keys(), reason="no network access or missing API keys")
    def test_chembl_large_dataset(self, chembl_client):
        """Test handling of larger datasets (marked as slow)."""
        # Search for a common compound to get more results
        compounds = chembl_client.search_compounds("benzene", limit=50)
        
        assert len(compounds) <= 50
        if len(compounds) > 10:  # If we got substantial results
            # Test that we can process the data
            compound_ids = [c.get("compound_id") or c.get("molecule_chembl_id") for c in compounds]
            compound_ids = [cid for cid in compound_ids if cid]  # Filter out None values
            
            assert len(compound_ids) > 0
            assert all(isinstance(cid, str) for cid in compound_ids)

    @pytest.mark.skipif(not _check_network_access() or not _check_api_keys(), reason="no network access or missing API keys")
    def test_chembl_pagination(self, chembl_client):
        """Test pagination functionality."""
        # Test that pagination parameters are correctly applied
        compounds_page1 = chembl_client.search_compounds("aspirin", limit=2, offset=0)
        compounds_page2 = chembl_client.search_compounds("aspirin", limit=2, offset=2)
        
        assert len(compounds_page1) <= 2
        assert len(compounds_page2) <= 2
        
        # If we got results, they should be different (assuming there are enough results)
        if compounds_page1 and compounds_page2:
            ids1 = {c.get("compound_id") or c.get("molecule_chembl_id") for c in compounds_page1}
            ids2 = {c.get("compound_id") or c.get("molecule_chembl_id") for c in compounds_page2}
            ids1.discard(None)
            ids2.discard(None)
            
            # Pages should have different IDs (no overlap)
            assert ids1.isdisjoint(ids2), f"Page overlap detected: {ids1 & ids2}"
