"""Tests for configuration schema validation."""

import tempfile
from pathlib import Path

import pytest
from pydantic import ValidationError

from library.config import Config


class TestConfigSchemaValidation:
    """Test JSON Schema validation for configuration files."""

    def test_valid_config_passes_schema_validation(self):
        """Test that a valid configuration passes schema validation."""
        valid_config = {
            "http": {
                "global": {
                    "timeout_sec": 30.0,
                    "retries": {
                        "total": 3,
                        "backoff_factor": 1.0
                    }
                }
            },
            "sources": {
                "chembl": {
                    "name": "chembl",
                    "enabled": True,
                    "http": {
                        "base_url": "https://api.example.com",
                        "rate_limit": {
                            "max_calls": 10,
                            "period": 1.0
                        }
                    },
                    "params": {},
                    "pagination": {
                        "page_param": "page",
                        "size_param": "size",
                        "size": 100,
                        "max_pages": 10
                    }
                }
            },
            "io": {
                "input": {
                    "documents_csv": "data/input/documents.csv"
                },
                "output": {
                    "data_path": "data/output/data.csv",
                    "qc_report_path": "data/output/qc_report.csv",
                    "correlation_path": "data/output/correlation.csv"
                }
            },
            "runtime": {
                "workers": 4,
                "limit": 1000
            },
            "logging": {
                "level": "INFO"
            },
            "validation": {
                "qc": {
                    "min_fill_rate": 0.8
                }
            }
        }
        
        # Should not raise any exceptions
        config = Config.model_validate(valid_config)
        assert config is not None

    def test_invalid_config_fails_schema_validation(self):
        """Test that an invalid configuration fails schema validation."""
        invalid_config = {
            "http": {
                "global": {
                    "timeout_sec": -1.0,  # Invalid: negative timeout
                    "retries": {
                        "total": 3
                    }
                }
            },
            "sources": {
                "chembl": {
                    "name": "chembl",  # Required field
                    "enabled": True,
                    "http": {
                        "base_url": "not-a-valid-url",  # Invalid: not a URI
                        "rate_limit": {
                            "max_calls": 0,  # Invalid: must be >= 1
                            "period": 1.0
                        }
                    }
                }
            },
            "io": {
                "input": {
                    "documents_csv": "data/input/documents.csv"
                },
                "output": {
                    "data_path": "data/output/data.csv",  # Required field
                    "qc_report_path": "data/output/qc_report.csv",  # Required field
                    "correlation_path": "data/output/correlation.csv"  # Required field
                }
            },
            "runtime": {
                "workers": 4,
                "limit": 1000
            },
            "logging": {
                "level": "INFO"
            },
            "validation": {
                "qc": {
                    "max_missing_fraction": 0.2,  # Use new field name
                    "max_duplicate_fraction": 0.1
                }
            }
        }
        
        # Should raise ValidationError due to Pydantic validation failure
        with pytest.raises(ValidationError):
            Config.model_validate(invalid_config)

    def test_missing_required_fields_fails_validation(self):
        """Test that missing required fields fail validation."""
        incomplete_config = {
            "http": {
                "global": {
                    "timeout_sec": 30.0
                    # Missing required 'retries' field
                }
            },
            "sources": {
                "chembl": {
                    "name": "chembl",  # Required field
                    "enabled": True,
                    "http": {
                        "base_url": "https://api.example.com"
                    }
                }
            }
            # Missing required 'io', 'logging' fields
        }
        
        with pytest.raises(ValidationError):
            Config.model_validate(incomplete_config)

    def test_invalid_enum_values_fail_validation(self):
        """Test that invalid enum values fail validation."""
        invalid_enum_config = {
            "http": {
                "global": {
                    "timeout_sec": 30.0,
                    "retries": {
                        "total": 3
                    }
                }
            },
            "sources": {
                "chembl": {
                    "name": "chembl",  # Required field
                    "enabled": True,
                    "http": {
                        "base_url": "https://api.example.com"
                    }
                }
            },
            "io": {
                "input": {
                    "documents_csv": "data/input/documents.csv"
                },
                "output": {
                    "data_path": "data/output/data.csv",  # Required field
                    "qc_report_path": "data/output/qc_report.csv",  # Required field
                    "correlation_path": "data/output/correlation.csv"  # Required field
                }
            },
            "runtime": {
                "workers": 4,
                "limit": 1000
            },
            "logging": {
                "level": "INVALID_LEVEL"  # Invalid: not in enum
            },
            "validation": {
                "qc": {
                    "max_missing_fraction": 0.2,  # Use new field name
                    "max_duplicate_fraction": 0.1
                }
            }
        }
        
        with pytest.raises(ValidationError):
            Config.model_validate(invalid_enum_config)

    def test_yaml_file_validation_with_schema(self):
        """Test that YAML file loading uses schema validation."""
        valid_yaml_content = """
http:
  global:
    timeout_sec: 30.0
    retries:
      total: 3
      backoff_factor: 1.0

sources:
  chembl:
    name: chembl
    enabled: true
    http:
      base_url: "https://api.example.com"
      rate_limit:
        max_calls: 10
        period: 1.0
    params: {}
    pagination:
      page_param: "page"
      size_param: "size"
      size: 100
      max_pages: 10

io:
  input:
    documents_csv: "data/input/documents.csv"
  output:
    data_path: "data/output/data.csv"
    qc_report_path: "data/output/qc_report.csv"
    correlation_path: "data/output/correlation.csv"

runtime:
  workers: 4
  limit: 1000

logging:
  level: "INFO"

validation:
  qc:
    max_missing_fraction: 0.2
    max_duplicate_fraction: 0.1
"""
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            f.write(valid_yaml_content)
            temp_path = Path(f.name)
        
        try:
            # Should not raise any exceptions
            config = Config.load(temp_path)
            assert config is not None
            assert config.http.global_.timeout_sec == 30.0
            assert config.sources["chembl"].enabled is True
        finally:
            temp_path.unlink()

    def test_invalid_yaml_file_fails_validation(self):
        """Test that invalid YAML file fails schema validation."""
        invalid_yaml_content = """
http:
  global:
    timeout_sec: -1.0  # Invalid: negative
    retries:
      total: 3

sources:
  chembl:
    name: chembl
    enabled: true
    http:
      base_url: "not-a-valid-url"  # Invalid: not a URI

io:
  input:
    documents_csv: "data/input/documents.csv"
  output:
    data_path: "data/output/data.csv"
    qc_report_path: "data/output/qc_report.csv"
    correlation_path: "data/output/correlation.csv"

runtime:
  workers: 4
  limit: 1000

logging:
  level: "INFO"

validation:
  qc:
    max_missing_fraction: 0.2
    max_duplicate_fraction: 0.1
"""
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            f.write(invalid_yaml_content)
            temp_path = Path(f.name)
        
        try:
            # Should raise ValueError due to schema validation failure
            with pytest.raises(ValueError, match="Configuration validation failed"):
                Config.load(temp_path)
        finally:
            temp_path.unlink()

    def test_schema_file_not_found_error(self):
        """Test error handling when schema file is not found."""
        # Temporarily rename schema file to simulate missing file
        schema_path = Path(__file__).parent.parent / "configs" / "schema.json"
        backup_path = schema_path.with_suffix('.json.backup')
        
        try:
            if schema_path.exists():
                schema_path.rename(backup_path)
            
            with pytest.raises(FileNotFoundError, match="Schema file not found"):
                Config._load_schema()
        finally:
            # Restore schema file
            if backup_path.exists():
                backup_path.rename(schema_path)
