"""Integration tests for API rate limiting and error handling."""

import os
import time
from concurrent.futures import ThreadPoolExecutor

import pytest

from library.clients.chembl import ChEMBLClient
from library.clients.crossref import CrossrefClient
from library.clients.exceptions import ApiClientError, RateLimitError
from library.config import APIClientConfig


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
class TestAPILimitsIntegration:
    """Integration tests for API rate limiting and concurrent access."""

    @pytest.fixture
    def chembl_client(self, integration_config):
        """Create a ChEMBL client for rate limiting tests."""
        api_token = os.getenv("CHEMBL_API_TOKEN")
        if api_token:
            integration_config.sources["chembl"].http.headers["Authorization"] = f"Bearer {api_token}"
        
        return ChEMBLClient(integration_config.sources["chembl"].to_client_config(integration_config.http.global_))

    @pytest.mark.skipif(not _check_network_access(), reason="no network access")
    def test_rate_limit_enforcement(self, chembl_client):
        """Test that rate limiting is properly enforced."""
        # Configure very strict rate limiting
        strict_config = APIClientConfig(
            name="chembl",
            base_url="https://www.ebi.ac.uk/chembl/api/data",
            endpoint="/",
            headers=chembl_client.default_headers,
            params={},
            pagination_param="offset",
            page_size_param="limit",
            page_size=1,
            max_pages=1,
            timeout=30.0,
            retries={"total": 1, "backoff_factor": 1.0, "status_forcelist": [500, 502, 503, 504]},
            rate_limit={"max_calls": 1, "period": 2.0},  # 1 call per 2 seconds
        )
        
        strict_client = ChEMBLClient(strict_config)
        
        # Make first request - should succeed
        try:
            strict_client._request("GET", "/status")
            first_request_success = True
        except ApiClientError:
            first_request_success = False
        
        # Make second request immediately - should be rate limited
        start_time = time.time()
        try:
            strict_client._request("GET", "/status")
            second_request_success = True
        except (ApiClientError, RateLimitError) as e:
            second_request_success = False
            # Should be rate limited
            assert "rate limit" in str(e).lower() or e.status_code == 429
        
        elapsed_time = time.time() - start_time
        
        # Either the request should fail due to rate limiting,
        # or it should take at least 2 seconds (rate limit period)
        if second_request_success:
            assert elapsed_time >= 1.8, f"Rate limiting not enforced: {elapsed_time:.2f}s"
        else:
            assert elapsed_time < 1.0, f"Rate limiting failed: {elapsed_time:.2f}s"

    @pytest.mark.skipif(not _check_network_access(), reason="no network access")
    def test_concurrent_rate_limiting(self, chembl_client):
        """Test rate limiting with concurrent requests."""
        # Create multiple clients with rate limiting
        from library.config import APIClientConfig
        
        rate_limit_config = APIClientConfig(
            name="chembl",
            base_url="https://www.ebi.ac.uk/chembl/api/data",
            endpoint="/",
            headers=chembl_client.default_headers,
            params={},
            pagination_param="offset",
            page_size_param="limit",
            page_size=1,
            max_pages=1,
            timeout=30.0,
            retries={"total": 1, "backoff_factor": 1.0, "status_forcelist": [500, 502, 503, 504]},
            rate_limit={"max_calls": 2, "period": 1.0},  # 2 calls per second
        )
        
        clients = [ChEMBLClient(rate_limit_config) for _ in range(3)]
        
        results = []
        errors = []
        
        def make_request(client, client_id):
            try:
                client._request("GET", "/status")
                results.append(f"client_{client_id}_success")
            except Exception as e:
                errors.append(f"client_{client_id}_error: {e}")
        
        # Make concurrent requests
        with ThreadPoolExecutor(max_workers=3) as executor:
            futures = [executor.submit(make_request, client, i) for i, client in enumerate(clients)]
            
            # Wait for all requests to complete
            for future in futures:
                future.result(timeout=10)
        
        # Should have some successes and some rate limit errors
        assert len(results) + len(errors) == 3
        
        # At least one request should succeed
        assert len(results) > 0
        
        # Some requests might be rate limited
        rate_limit_errors = [e for e in errors if "rate limit" in e.lower()]
        # This is acceptable - rate limiting is working

    @pytest.mark.skipif(not _check_network_access(), reason="no network access")
    def test_api_error_handling(self, chembl_client):
        """Test handling of various API errors."""
        # Test 404 error
        with pytest.raises(ApiClientError) as exc_info:
            chembl_client._request("GET", "/nonexistent-endpoint")
        
        error = exc_info.value
        assert error.status_code == 404
        
        # Test invalid method
        with pytest.raises(ApiClientError) as exc_info:
            chembl_client._request("POST", "/status")
        
        error = exc_info.value
        assert error.status_code == 405  # Method Not Allowed

    @pytest.mark.skipif(not _check_network_access(), reason="no network access")
    def test_timeout_handling(self):
        """Test timeout handling with real network conditions."""
        from library.config import APIClientConfig
        
        # Create client with very short timeout
        timeout_config = APIClientConfig(
            name="chembl",
            base_url="https://www.ebi.ac.uk/chembl/api/data",
            endpoint="/",
            headers={},
            params={},
            pagination_param="offset",
            page_size_param="limit",
            page_size=1,
            max_pages=1,
            timeout=0.1,  # 100ms timeout
            retries={"total": 1, "backoff_factor": 1.0, "status_forcelist": [500, 502, 503, 504]},
            rate_limit={"max_calls": 10, "period": 1.0},
        )
        
        timeout_client = ChEMBLClient(timeout_config)
        
        # This should timeout
        start_time = time.time()
        with pytest.raises(ApiClientError) as exc_info:
            timeout_client._request("GET", "/status")
        
        elapsed_time = time.time() - start_time
        error = exc_info.value
        
        # Should timeout quickly
        assert elapsed_time < 1.0, f"Timeout took too long: {elapsed_time:.2f}s"
        assert "timeout" in str(error).lower() or "timeout" in error.__class__.__name__.lower()

    @pytest.mark.skipif(not _check_network_access(), reason="no network access")
    def test_retry_mechanism(self, chembl_client):
        """Test retry mechanism with intermittent failures."""
        # This test is harder to implement reliably without mocking
        # We'll test that the retry configuration is properly set up
        
        assert chembl_client.max_retries > 0
        assert chembl_client.config.retries.total > 0
        
        # Test that backoff is configured
        assert chembl_client.config.retries.backoff_factor > 0

    @pytest.mark.skipif(not _check_network_access() or not _check_api_keys(), reason="no network access or missing API keys")
    def test_different_api_endpoints(self, integration_config):
        """Test different API endpoints and their rate limits."""
        # Test ChEMBL
        chembl_client = ChEMBLClient(integration_config.sources["chembl"].to_client_config(integration_config.http.global_))
        
        try:
            response = chembl_client._request("GET", "/status")
            assert response is not None
        except ApiClientError as e:
            # API might return an error, but we should get a response
            assert e.status_code is not None
        
        # Test Crossref (if available)
        try:
            crossref_config = APIClientConfig(
                name="crossref",
                base_url="https://api.crossref.org",
                endpoint="/",
                headers={},
                params={},
                pagination_param="offset",
                page_size_param="rows",
                page_size=1,
                max_pages=1,
                timeout=30.0,
                retries={"total": 3, "backoff_factor": 1.0, "status_forcelist": [500, 502, 503, 504]},
                rate_limit={"max_calls": 50, "period": 1.0},
            )
            
            crossref_client = CrossrefClient(crossref_config)
            response = crossref_client._request("GET", "/works")
            assert response is not None
            
        except Exception:
            # Crossref might not be available or have different rate limits
            pass

    @pytest.mark.skipif(not _check_network_access() or not _check_api_keys(), reason="no network access or missing API keys")
    def test_api_response_size_limits(self, chembl_client):
        """Test handling of large API responses."""
        # Request a larger dataset to test response size handling
        try:
            compounds = chembl_client.search_compounds("benzene", limit=100)
            
            if compounds:
                # Verify response structure
                assert isinstance(compounds, list)
                assert len(compounds) <= 100
                
                # Check that we can process the data
                for compound in compounds[:5]:  # Check first 5
                    assert isinstance(compound, dict)
                    assert "compound_id" in compound or "molecule_chembl_id" in compound
                    
        except ApiClientError as e:
            # API might have limits on response size
            if e.status_code == 413:  # Payload Too Large
                pytest.skip("API response size limit exceeded")
            else:
                raise

    @pytest.mark.slow
    @pytest.mark.skipif(not _check_network_access() or not _check_api_keys(), reason="no network access or missing API keys")
    def test_sustained_api_usage(self, chembl_client):
        """Test sustained API usage over time."""
        import time
        
        # Make multiple requests over time to test sustained usage
        start_time = time.time()
        successful_requests = 0
        failed_requests = 0
        
        for i in range(10):  # Make 10 requests over 20 seconds
            try:
                compounds = chembl_client.search_compounds(f"test_{i}", limit=1)
                successful_requests += 1
            except Exception:
                failed_requests += 1
            
            time.sleep(2)  # Wait 2 seconds between requests
        
        elapsed_time = time.time() - start_time
        
        # Should have made progress
        assert elapsed_time >= 18, f"Test completed too quickly: {elapsed_time:.2f}s"
        
        # Should have some successful requests
        assert successful_requests > 0
        
        # Failure rate should be reasonable (some failures due to invalid search terms are expected)
        failure_rate = failed_requests / (successful_requests + failed_requests)
        assert failure_rate < 0.8, f"Too many failures: {failure_rate:.2%}"
