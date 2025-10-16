"""Tests for document pipeline configuration usage."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import Mock, patch

import pytest
import yaml

from library.documents.config import DocumentConfig, load_document_config
from library.documents.pipeline import _create_api_client, DocumentValidationError


@pytest.fixture()
def custom_headers_config_yaml(tmp_path: Path) -> Path:
    """Create a test configuration with custom headers and settings."""
    config_path = tmp_path / "custom_config.yaml"
    config_data = {
        "http": {
            "global": {
                "timeout_sec": 45.0,
                "retries": {
                    "total": 3,
                    "backoff_multiplier": 1.5
                },
                "headers": {
                    "User-Agent": "custom-user-agent/1.0",
                    "X-Custom-Header": "global-value"
                }
            }
        },
        "sources": {
            "chembl": {
                "enabled": True,
                "http": {
                    "base_url": "https://custom.chembl.api/",
                    "timeout_sec": 90.0,
                    "headers": {
                        "Authorization": "Bearer custom-token",
                        "X-Custom-Header": "chembl-override"
                    },
                    "retries": {
                        "total": 7,
                        "backoff_multiplier": 2.5
                    }
                },
                "rate_limit": {
                    "requests_per_second": 0.5,
                    "requests_per_minute": 30,
                    "burst_size": 5
                }
            },
            "crossref": {
                "enabled": True,
                "http": {
                    "base_url": "https://custom.crossref.api/",
                    "headers": {
                        "X-API-Key": "custom-crossref-key"
                    }
                }
            },
            "pubmed": {
                "enabled": True,
                "http": {
                    "timeout_sec": 120.0,
                    "headers": {
                        "X-API-Key": "custom-pubmed-key"
                    },
                    "retries": {
                        "total": 10
                    }
                }
            }
        }
    }
    config_path.write_text(yaml.safe_dump(config_data), encoding="utf-8")
    return config_path


def test_create_api_client_uses_source_config(custom_headers_config_yaml: Path) -> None:
    """Test that _create_api_client uses source-specific configuration."""
    
    config = load_document_config(custom_headers_config_yaml)
    
    # Test ChEMBL client creation
    with patch('library.documents.pipeline.ChEMBLClient') as mock_client:
        client = _create_api_client("chembl", config)
        
        # Verify client was created
        mock_client.assert_called_once()
        call_args = mock_client.call_args
        
        # Check APIClientConfig was created with correct values
        api_config = call_args[0][0]  # First positional argument
        
        assert api_config.name == "chembl"
        assert str(api_config.base_url) == "https://custom.chembl.api/"
        assert api_config.timeout == 90.0  # Source-specific timeout
        
        # Check headers are merged correctly (global + source-specific)
        expected_headers = {
            "User-Agent": "custom-user-agent/1.0",
            "X-Custom-Header": "chembl-override",  # Source overrides global
            "Authorization": "Bearer custom-token"
        }
        assert api_config.headers == expected_headers
        
        # Check retry settings
        assert api_config.retries.total == 7  # Source-specific
        assert api_config.retries.backoff_multiplier == 2.5  # Source-specific
        
        # Check rate limit settings
        assert api_config.rate_limit is not None
        assert api_config.rate_limit.requests_per_second == 0.5
        assert api_config.rate_limit.requests_per_minute == 30
        assert api_config.rate_limit.burst_size == 5


def test_create_api_client_fallback_to_global_config(custom_headers_config_yaml: Path) -> None:
    """Test that _create_api_client falls back to global config when source config is missing."""
    
    config = load_document_config(custom_headers_config_yaml)
    
    # Test Crossref client creation (uses global timeout, partial headers)
    with patch('library.documents.pipeline.CrossrefClient') as mock_client:
        client = _create_api_client("crossref", config)
        
        mock_client.assert_called_once()
        call_args = mock_client.call_args
        api_config = call_args[0][0]
        
        assert api_config.name == "crossref"
        assert str(api_config.base_url) == "https://custom.crossref.api/"
        assert api_config.timeout == 45.0  # Falls back to global
        
        # Check headers are merged correctly
        expected_headers = {
            "User-Agent": "custom-user-agent/1.0",
            "X-Custom-Header": "global-value",  # Global value (not overridden)
            "X-API-Key": "custom-crossref-key"
        }
        assert api_config.headers == expected_headers
        
        # Check retry settings fall back to global
        assert api_config.retries.total == 3  # Global value
        assert api_config.retries.backoff_multiplier == 1.5  # Global value
        
        # No rate limit configured
        assert api_config.rate_limit is None


def test_create_api_client_partial_retry_config(custom_headers_config_yaml: Path) -> None:
    """Test that _create_api_client handles partial retry configuration."""
    
    config = load_document_config(custom_headers_config_yaml)
    
    # Test PubMed client creation (partial retry config)
    with patch('library.documents.pipeline.PubMedClient') as mock_client:
        client = _create_api_client("pubmed", config)
        
        mock_client.assert_called_once()
        call_args = mock_client.call_args
        api_config = call_args[0][0]
        
        assert api_config.name == "pubmed"
        assert api_config.timeout == 120.0  # Source-specific timeout
        
        # Check retry settings: source total, global backoff_multiplier
        assert api_config.retries.total == 10  # Source-specific
        assert api_config.retries.backoff_multiplier == 1.5  # Falls back to global


def test_create_api_client_unknown_source() -> None:
    """Test that _create_api_client raises error for unknown source."""
    
    config = DocumentConfig()
    
    with pytest.raises(DocumentValidationError, match="Source 'unknown' not found"):
        _create_api_client("unknown", config)


def test_create_api_client_uses_default_urls_when_not_configured() -> None:
    """Test that _create_api_client uses default URLs when not configured in source."""
    
    config = DocumentConfig()
    
    # Test with a source that has no custom base_url
    with patch('library.documents.pipeline.ChEMBLClient') as mock_client:
        client = _create_api_client("chembl", config)
        
        mock_client.assert_called_once()
        call_args = mock_client.call_args
        api_config = call_args[0][0]
        
        # Should use default URL from _get_base_url
        assert str(api_config.base_url) == "https://www.ebi.ac.uk/chembl/api/data"
        
        # Should use global timeout
        assert api_config.timeout == 30.0  # Default global timeout
        
        # Should use global headers
        assert api_config.headers == {"User-Agent": "bioactivity-data-acquisition/0.1.0"}


def test_create_api_client_chembl_minimum_timeout() -> None:
    """Test that ChEMBL gets minimum 60 second timeout even if configured lower."""
    
    config_data = {
        "http": {
            "global": {
                "timeout_sec": 30.0
            }
        },
        "sources": {
            "chembl": {
                "enabled": True,
                "http": {
                    "timeout_sec": 45.0  # Less than 60, should be increased
                }
            }
        }
    }
    
    config = DocumentConfig.model_validate(config_data)
    
    with patch('library.documents.pipeline.ChEMBLClient') as mock_client:
        client = _create_api_client("chembl", config)
        
        mock_client.assert_called_once()
        call_args = mock_client.call_args
        api_config = call_args[0][0]
        
        # Should be increased to minimum 60 seconds
        assert api_config.timeout == 60.0


def test_create_api_client_all_sources() -> None:
    """Test that _create_api_client works for all supported sources."""
    
    config = DocumentConfig()
    
    sources_and_clients = [
        ("chembl", "ChEMBLClient"),
        ("crossref", "CrossrefClient"),
        ("openalex", "OpenAlexClient"),
        ("pubmed", "PubMedClient"),
        ("semantic_scholar", "SemanticScholarClient")
    ]
    
    for source, client_class in sources_and_clients:
        with patch(f'library.documents.pipeline.{client_class}') as mock_client:
            client = _create_api_client(source, config)
            mock_client.assert_called_once()
            
            # Verify the client was created with APIClientConfig
            call_args = mock_client.call_args
            api_config = call_args[0][0]
            assert api_config.name == source
