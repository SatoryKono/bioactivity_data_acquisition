"""Tests for ChEMBL status client."""

from unittest.mock import patch

from library.clients.chembl_client import ChEMBLStatusClient
from library.config import APIClientConfig


class TestChEMBLStatusClient:
    """Test cases for ChEMBLStatusClient."""
    
    def test_init(self):
        """Test client initialization."""
        config = APIClientConfig(
            base_url="https://www.ebi.ac.uk/chembl/api/data",
            timeout=30,
            retries=3
        )
        client = ChEMBLStatusClient(config)
        assert client is not None
    
    @patch('library.clients.chembl_client.ChEMBLStatusClient._request')
    def test_get_chembl_status_success(self, mock_request):
        """Test successful ChEMBL status retrieval."""
        # Mock successful response
        mock_request.return_value = {
            "chembl_release": "ChEMBL_33 (2024-01-15)",
            "chembl_db_version": "ChEMBL_33",
            "status": "ok"
        }
        
        config = APIClientConfig(
            base_url="https://www.ebi.ac.uk/chembl/api/data",
            timeout=30,
            retries=3
        )
        client = ChEMBLStatusClient(config)
        
        result = client.get_chembl_status()
        
        assert result["chembl_db_version"] == "ChEMBL_33"
        assert result["chembl_release_date"] == "2024-01-15"
        assert result["status"] == "ok"
        assert "timestamp" in result
        mock_request.assert_called_once_with("GET", "status")
    
    @patch('library.clients.chembl_client.ChEMBLStatusClient._request')
    def test_get_chembl_status_without_date(self, mock_request):
        """Test ChEMBL status retrieval without release date."""
        # Mock response without date
        mock_request.return_value = {
            "chembl_release": "ChEMBL_33",
            "chembl_db_version": "ChEMBL_33",
            "status": "ok"
        }
        
        config = APIClientConfig(
            base_url="https://www.ebi.ac.uk/chembl/api/data",
            timeout=30,
            retries=3
        )
        client = ChEMBLStatusClient(config)
        
        result = client.get_chembl_status()
        
        assert result["chembl_db_version"] == "ChEMBL_33"
        assert result["chembl_release_date"] is None
        assert result["status"] == "ok"
    
    @patch('library.clients.chembl_client.ChEMBLStatusClient._request')
    def test_get_chembl_status_error(self, mock_request):
        """Test ChEMBL status retrieval with error."""
        # Mock error response
        mock_request.side_effect = Exception("API error")
        
        config = APIClientConfig(
            base_url="https://www.ebi.ac.uk/chembl/api/data",
            timeout=30,
            retries=3
        )
        client = ChEMBLStatusClient(config)
        
        result = client.get_chembl_status()
        
        assert result["chembl_db_version"] is None
        assert result["chembl_release_date"] is None
        assert result["status"] == "error"
        assert "error" in result
        assert result["error"] == "API error"
    
    @patch('library.clients.chembl_client.ChEMBLStatusClient._request')
    def test_get_chembl_status_empty_response(self, mock_request):
        """Test ChEMBL status retrieval with empty response."""
        # Mock empty response
        mock_request.return_value = {}
        
        config = APIClientConfig(
            base_url="https://www.ebi.ac.uk/chembl/api/data",
            timeout=30,
            retries=3
        )
        client = ChEMBLStatusClient(config)
        
        result = client.get_chembl_status()
        
        assert result["chembl_db_version"] is None
        assert result["chembl_release_date"] is None
        assert result["status"] == "unknown"
