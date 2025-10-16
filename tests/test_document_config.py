"""Tests for document configuration loading and validation."""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from library.documents.config import (
    DocumentConfig,
    DocumentSourceSettings,
    DocumentSourceHTTPSettings,
    DocumentSourcePaginationSettings,
    load_document_config,
    ConfigLoadError,
)


@pytest.fixture()
def document_config_yaml(tmp_path: Path) -> Path:
    """Create a test document configuration YAML file."""
    config_path = tmp_path / "document_config.yaml"
    config_data = {
        "http": {
            "global": {
                "timeout_sec": 30.0,
                "retries": {
                    "total": 5,
                    "backoff_multiplier": 2.0
                },
                "headers": {
                    "User-Agent": "bioactivity-data-acquisition/0.1.0"
                }
            }
        },
        "sources": {
            "chembl": {
                "name": "chembl",
                "enabled": True,
                "endpoint": "document",
                "params": {
                    "document_type": "article"
                },
                "pagination": {
                    "page_param": "page",
                    "size_param": "page_size",
                    "size": 200,
                    "max_pages": 10
                },
                "http": {
                    "base_url": "https://www.ebi.ac.uk/chembl/api/data",
                    "timeout_sec": 60.0,
                    "headers": {
                        "Accept": "application/json",
                        "Authorization": "Bearer {chembl_api_token}"
                    },
                    "retries": {
                        "total": 10,
                        "backoff_multiplier": 3.0
                    }
                },
                "rate_limit": {
                    "max_calls": 3,
                    "period": 1.0
                }
            },
            "crossref": {
                "name": "crossref",
                "enabled": True,
                "params": {
                    "query": "chembl",
                    "select": "DOI,title"
                },
                "pagination": {
                    "page_param": "cursor",
                    "size_param": "rows",
                    "size": 100,
                    "max_pages": 5
                },
                "http": {
                    "base_url": "https://api.crossref.org/works",
                    "timeout_sec": 30.0,
                    "headers": {
                        "Accept": "application/json",
                        "Crossref-Plus-API-Token": "{crossref_api_key}"
                    }
                }
            },
            "pubmed": {
                "name": "pubmed",
                "enabled": True,
                "rate_limit": {
                    "max_calls": 2,
                    "period": 1.0
                },
                "http": {
                    "base_url": "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/",
                    "timeout_sec": 60.0,
                    "headers": {
                        "Accept": "application/json",
                        "User-Agent": "bioactivity-data-acquisition/0.1.0",
                        "api_key": "{PUBMED_API_KEY}"
                    },
                    "retries": {
                        "total": 10,
                        "backoff_multiplier": 3.0
                    }
                }
            }
        },
        "io": {
            "input": {
                "documents_csv": "data/input/documents.csv"
            },
            "output": {
                "dir": "data/output/documents"
            }
        },
        "runtime": {
            "workers": 4,
            "limit": None,
            "dry_run": False
        }
    }
    config_path.write_text(yaml.safe_dump(config_data), encoding="utf-8")
    return config_path


def test_load_document_config_preserves_nested_dicts(document_config_yaml: Path) -> None:
    """Test that load_document_config preserves nested dictionaries from YAML."""
    
    config = load_document_config(document_config_yaml)
    
    # Проверяем, что источники загружены как DocumentSourceSettings
    assert isinstance(config.sources["chembl"], DocumentSourceSettings)
    assert isinstance(config.sources["crossref"], DocumentSourceSettings)
    assert isinstance(config.sources["pubmed"], DocumentSourceSettings)
    
    # Проверяем, что все поля доступны
    chembl_source = config.sources["chembl"]
    assert chembl_source.enabled is True
    assert chembl_source.name == "chembl"
    assert chembl_source.endpoint == "document"
    assert chembl_source.params == {"document_type": "article"}
    assert chembl_source.rate_limit == {"max_calls": 3, "period": 1.0}
    
    # Проверяем HTTP настройки
    assert isinstance(chembl_source.http, DocumentSourceHTTPSettings)
    assert chembl_source.http.base_url == "https://www.ebi.ac.uk/chembl/api/data"
    assert chembl_source.http.timeout_sec == 60.0
    assert chembl_source.http.headers == {
        "Accept": "application/json",
        "Authorization": "Bearer {chembl_api_token}"
    }
    assert chembl_source.http.retries == {
        "total": 10,
        "backoff_multiplier": 3.0
    }
    
    # Проверяем настройки пагинации
    assert isinstance(chembl_source.pagination, DocumentSourcePaginationSettings)
    assert chembl_source.pagination.page_param == "page"
    assert chembl_source.pagination.size_param == "page_size"
    assert chembl_source.pagination.size == 200
    assert chembl_source.pagination.max_pages == 10


def test_document_config_headers_and_timeouts_accessible(document_config_yaml: Path) -> None:
    """Test that headers and timeouts from YAML are accessible after loading."""
    
    config = load_document_config(document_config_yaml)
    
    # Проверяем глобальные HTTP настройки
    assert config.http.global_.timeout_sec == 30.0
    assert config.http.global_.retries.total == 5
    assert config.http.global_.retries.backoff_multiplier == 2.0
    assert config.http.global_.headers == {
        "User-Agent": "bioactivity-data-acquisition/0.1.0"
    }
    
    # Проверяем специфичные для источника HTTP настройки
    chembl_source = config.sources["chembl"]
    assert chembl_source.http.timeout_sec == 60.0  # Переопределен для ChEMBL
    assert chembl_source.http.headers["Accept"] == "application/json"
    assert chembl_source.http.headers["Authorization"] == "Bearer {chembl_api_token}"
    
    crossref_source = config.sources["crossref"]
    assert crossref_source.http.timeout_sec == 30.0  # Использует глобальное значение
    assert crossref_source.http.headers["Crossref-Plus-API-Token"] == "{crossref_api_key}"
    
    pubmed_source = config.sources["pubmed"]
    assert pubmed_source.http.timeout_sec == 60.0  # Переопределен для PubMed
    assert pubmed_source.http.headers["api_key"] == "{PUBMED_API_KEY}"


def test_document_config_default_values() -> None:
    """Test that default values are properly set for sources."""
    
    config = DocumentConfig()
    
    # Проверяем, что все источники созданы с правильными значениями по умолчанию
    for source_name in ["chembl", "crossref", "openalex", "pubmed", "semantic_scholar"]:
        source = config.sources[source_name]
        assert isinstance(source, DocumentSourceSettings)
        assert source.enabled is True
        assert source.name == source_name
        assert source.params == {}
        assert source.rate_limit == {}
        
        # Проверяем HTTP настройки по умолчанию
        assert isinstance(source.http, DocumentSourceHTTPSettings)
        assert source.http.base_url is None
        assert source.http.timeout_sec is None
        assert source.http.headers == {}
        assert source.http.retries == {}
        
        # Проверяем настройки пагинации по умолчанию
        assert isinstance(source.pagination, DocumentSourcePaginationSettings)
        assert source.pagination.page_param is None
        assert source.pagination.size_param is None
        assert source.pagination.size is None
        assert source.pagination.max_pages is None


def test_document_config_boolean_toggle() -> None:
    """Test that boolean toggles still work for backward compatibility."""
    
    config_data = {
        "sources": {
            "chembl": True,
            "crossref": False,
            "pubmed": True
        }
    }
    
    config = DocumentConfig.model_validate(config_data)
    
    assert config.sources["chembl"].enabled is True
    assert config.sources["chembl"].name == "chembl"
    assert config.sources["crossref"].enabled is False
    assert config.sources["crossref"].name == "crossref"
    assert config.sources["pubmed"].enabled is True
    assert config.sources["pubmed"].name == "pubmed"


def test_document_config_enabled_sources() -> None:
    """Test that enabled_sources method works with new model."""
    
    config_data = {
        "sources": {
            "chembl": {"enabled": True},
            "crossref": {"enabled": False},
            "openalex": {"enabled": True},
            "pubmed": {"enabled": False},
            "semantic_scholar": {"enabled": True}
        }
    }
    
    config = DocumentConfig.model_validate(config_data)
    enabled = config.enabled_sources()
    
    assert enabled == ["chembl", "openalex", "semantic_scholar"]


def test_document_config_validation_errors() -> None:
    """Test that validation errors are properly raised."""
    
    # Тест с неподдерживаемым источником
    with pytest.raises(ValueError, match="Unsupported source"):
        DocumentConfig.model_validate({
            "sources": {
                "invalid_source": {"enabled": True}
            }
        })
    
    # Тест с невалидным значением для источника
    with pytest.raises(ValueError, match="Invalid configuration"):
        DocumentConfig.model_validate({
            "sources": {
                "chembl": "invalid_value"
            }
        })
    
    # Тест с отключенными всеми источниками
    with pytest.raises(ValueError, match="At least one source must be enabled"):
        DocumentConfig.model_validate({
            "sources": {
                "chembl": {"enabled": False},
                "crossref": {"enabled": False},
                "openalex": {"enabled": False},
                "pubmed": {"enabled": False},
                "semantic_scholar": {"enabled": False}
            }
        })


def test_document_config_load_error_handling(tmp_path: Path) -> None:
    """Test error handling in load_document_config."""
    
    # Тест с несуществующим файлом
    with pytest.raises(ConfigLoadError, match="Configuration file not found"):
        load_document_config(tmp_path / "nonexistent.yaml")
    
    # Тест с невалидным YAML
    invalid_yaml_path = tmp_path / "invalid.yaml"
    invalid_yaml_path.write_text("invalid: yaml: content: [", encoding="utf-8")
    
    with pytest.raises(ConfigLoadError, match="Failed to parse configuration file"):
        load_document_config(invalid_yaml_path)
    
    # Тест с невалидной конфигурацией
    invalid_config_path = tmp_path / "invalid_config.yaml"
    invalid_config_path.write_text(yaml.safe_dump({
        "sources": {
            "invalid_source": {"enabled": True}
        }
    }), encoding="utf-8")
    
    with pytest.raises(ConfigLoadError, match="Unsupported source"):
        load_document_config(invalid_config_path)
