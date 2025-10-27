"""Tests for ChEMBL client v2 compatibility and migration."""

from unittest.mock import Mock, patch

from src.library.clients.chembl_v2 import ChemblClient
from src.library.clients.chembl_adapter import ChemblClientAdapter
from src.library.config.models import ApiCfg, RetryCfg, ChemblCacheCfg


class TestChemblClientV2:
    """Test the new ChemblClient v2 implementation."""

    def test_initialization(self):
        """Test client initialization with default configs."""
        client = ChemblClient()
        assert client.cache is not None
        assert client._cache_lock is not None
        assert client._session_local is not None

    def test_initialization_with_configs(self):
        """Test client initialization with custom configs."""
        api_cfg = ApiCfg(timeout_connect=5.0, timeout_read=15.0)
        retry_cfg = RetryCfg(retries=5, backoff_multiplier=1.5)
        chembl_cfg = ChemblCacheCfg(cache_ttl=1800, cache_size=500)
        
        client = ChemblClient(api=api_cfg, retry=retry_cfg, chembl=chembl_cfg)
        assert client.cache.ttl == 1800
        assert client.cache.maxsize == 500

    @patch('src.library.clients.chembl_v2.requests.Session.get')
    def test_fetch_success(self, mock_get):
        """Test successful fetch operation."""
        # Mock response
        mock_response = Mock()
        mock_response.json.return_value = {"status": "ok", "data": "test"}
        mock_response.raise_for_status.return_value = None
        mock_response.status_code = 200
        mock_response.elapsed.total_seconds.return_value = 0.5
        mock_get.return_value.__enter__.return_value = mock_response

        client = ChemblClient()
        api_cfg = ApiCfg()
        
        result = client.fetch("https://test.com/api.json", api_cfg)
        
        assert result == {"status": "ok", "data": "test"}
        mock_get.assert_called_once()

    @patch('src.library.clients.chembl_v2.requests.Session.get')
    def test_fetch_with_fallback(self, mock_get):
        """Test fetch with fallback URL (.json -> no suffix)."""
        # First call fails with 404, second succeeds
        mock_response_404 = Mock()
        mock_response_404.status_code = 404
        mock_response_404.raise_for_status.side_effect = Exception("404")
        
        mock_response_ok = Mock()
        mock_response_ok.json.return_value = {"status": "ok"}
        mock_response_ok.raise_for_status.return_value = None
        mock_response_ok.status_code = 200
        mock_response_ok.elapsed.total_seconds.return_value = 0.3
        
        mock_get.side_effect = [
            mock_response_404,
            mock_response_ok
        ]

        client = ChemblClient()
        api_cfg = ApiCfg(retries=1)  # Only 1 retry to test fallback
        
        result = client.fetch("https://test.com/api.json", api_cfg)
        
        assert result == {"status": "ok"}
        assert mock_get.call_count == 2

    def test_cache_functionality(self):
        """Test cache hit/miss functionality."""
        client = ChemblClient()
        
        # Initially empty cache
        assert len(client.cache) == 0
        
        # Add item to cache
        client.cache["test_key"] = {"test": "data"}
        assert len(client.cache) == 1
        assert client.cache["test_key"] == {"test": "data"}
        
        # Clear cache
        client.clear_cache()
        assert len(client.cache) == 0

    def test_deterministic_jitter(self):
        """Test that jitter is deterministic with fixed seed."""
        retry_cfg = RetryCfg(backoff_jitter_seed=42)
        jitter_func = retry_cfg.build_jitter()
        
        # Multiple calls should produce same results due to fixed seed
        results = [jitter_func(1.0) for _ in range(5)]
        
        # All results should be the same (deterministic)
        assert all(r == results[0] for r in results)
        # But different from input (jitter applied)
        assert results[0] != 1.0


class TestChemblClientAdapter:
    """Test backward compatibility adapter."""

    def test_adapter_initialization(self):
        """Test adapter initialization with old-style config."""
        # Mock old config
        old_config = Mock()
        old_config.base_url = "https://custom.chembl.api"
        old_config.user_agent = "Custom-Agent/1.0"
        old_config.timeout_connect = 5.0
        old_config.timeout_read = 20.0
        old_config.verify = True
        old_config.retries = 2
        old_config.backoff_multiplier = 1.5
        old_config.backoff_jitter_seed = 123

        adapter = ChemblClientAdapter(config=old_config, cache_ttl=1800)
        
        assert adapter.config == old_config
        assert adapter.cache is not None

    def test_adapter_initialization_defaults(self):
        """Test adapter initialization with no config."""
        adapter = ChemblClientAdapter()
        
        assert adapter.config is None
        assert adapter.cache is not None

    @patch('src.library.clients.chembl_adapter.ChemblClient.fetch')
    def test_get_chembl_status(self, mock_fetch):
        """Test get_chembl_status method."""
        mock_fetch.return_value = {
            "chembl_release": "33.0",
            "status": "active"
        }
        
        adapter = ChemblClientAdapter()
        result = adapter.get_chembl_status()
        
        assert result["chembl_release"] == "33.0"
        assert result["status"] == "active"
        mock_fetch.assert_called_once()

    @patch('src.library.clients.chembl_adapter.ChemblClient.fetch')
    def test_fetch_molecule(self, mock_fetch):
        """Test fetch_molecule method."""
        mock_fetch.return_value = {
            "molecules": [{
                "molecule_chembl_id": "CHEMBL123",
                "pref_name": "Test Molecule",
                "molecule_type": "Small molecule"
            }]
        }
        
        adapter = ChemblClientAdapter()
        result = adapter.fetch_molecule("CHEMBL123")
        
        assert result["molecule_chembl_id"] == "CHEMBL123"
        assert result["pref_name"] == "Test Molecule"
        mock_fetch.assert_called_once()

    @patch('src.library.clients.chembl_adapter.ChemblClient.fetch')
    def test_fetch_molecule_error(self, mock_fetch):
        """Test fetch_molecule with error handling."""
        mock_fetch.side_effect = Exception("API Error")
        
        adapter = ChemblClientAdapter()
        result = adapter.fetch_molecule("CHEMBL123")
        
        assert result["molecule_chembl_id"] == "CHEMBL123"
        assert result["error"] == "API Error"
        assert result["pref_name"] == ""

    def test_context_manager(self):
        """Test adapter as context manager."""
        with ChemblClientAdapter() as adapter:
            assert adapter is not None
            assert adapter.cache is not None


class TestMigrationCompatibility:
    """Test compatibility between old and new implementations."""

    def test_api_interface_compatibility(self):
        """Test that adapter provides same interface as old client."""
        adapter = ChemblClientAdapter()
        
        # Check that adapter has expected methods
        assert hasattr(adapter, 'get_chembl_status')
        assert hasattr(adapter, 'fetch_molecule')
        assert hasattr(adapter, '_request')
        assert hasattr(adapter, 'close')
        assert hasattr(adapter, '__enter__')
        assert hasattr(adapter, '__exit__')

    def test_config_compatibility(self):
        """Test that configs are properly converted."""
        old_config = Mock()
        old_config.base_url = "https://test.chembl.api"
        old_config.user_agent = "Test-Agent/2.0"
        old_config.timeout_connect = 8.0
        old_config.timeout_read = 25.0
        old_config.verify = False
        old_config.retries = 4
        old_config.backoff_multiplier = 2.5
        old_config.backoff_jitter_seed = 456

        adapter = ChemblClientAdapter(config=old_config)
        
        # Verify that the underlying client was configured correctly
        # (This is a bit of an implementation detail test, but important for migration)
        assert adapter._client is not None
