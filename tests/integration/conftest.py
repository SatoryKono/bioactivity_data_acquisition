"""Configuration for integration tests."""

import pytest
import os
import tempfile
from pathlib import Path
from typing import Generator

from library.config import Config
from library.documents.config import load_document_config


@pytest.fixture
def integration_config() -> Config:
    """Create a test configuration for integration tests."""
    # Use a minimal configuration for integration tests
    config_data = {
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
                    "base_url": "https://www.ebi.ac.uk/chembl/api/data",
                    "rate_limit": {
                        "max_calls": 1,
                        "period": 1.0  # Conservative for testing
                    }
                },
                "params": {},
                "pagination": {
                    "page_param": "offset",
                    "size_param": "limit",
                    "size": 10,  # Small page size for testing
                    "max_pages": 1  # Limit to 1 page for testing
                }
            }
        },
        "io": {
            "input": {
                "documents_csv": "data/input/test_documents.csv"
            },
            "output": {
                "data_path": "data/output/test_data.csv",
                "qc_report_path": "data/output/test_qc_report.csv",
                "correlation_path": "data/output/test_correlation.csv"
            }
        },
        "runtime": {
            "workers": 1,  # Single worker for testing
            "limit": 10  # Limit to 10 records for testing
        },
        "logging": {
            "level": "INFO"
        },
        "validation": {
            "qc": {
                "min_fill_rate": 0.5  # Lower threshold for testing
            }
        }
    }
    
    return Config.model_validate(config_data)


@pytest.fixture
def temp_output_dir() -> Generator[Path, None, None]:
    """Create a temporary output directory for integration tests."""
    with tempfile.TemporaryDirectory() as temp_dir:
        output_dir = Path(temp_dir) / "output"
        output_dir.mkdir(parents=True)
        yield output_dir


@pytest.fixture
def test_documents_csv(temp_output_dir: Path) -> Generator[Path, None, None]:
    """Create a test documents CSV file for integration tests."""
    import pandas as pd
    
    # Create minimal test data
    test_data = pd.DataFrame({
        'document_id': ['CHEMBL12345', 'CHEMBL67890'],
        'title': ['Test Document 1', 'Test Document 2'],
        'doi': ['10.1234/test1', '10.1234/test2'],
        'pmid': ['12345', '67890'],
    })
    
    input_dir = temp_output_dir.parent / "input"
    input_dir.mkdir(parents=True)
    csv_path = input_dir / "test_documents.csv"
    
    test_data.to_csv(csv_path, index=False)
    
    yield csv_path
    
    # Cleanup
    csv_path.unlink(missing_ok=True)


@pytest.fixture
def skip_if_no_api_key():
    """Skip test if API keys are not available."""
    required_keys = ['CHEMBL_API_TOKEN', 'PUBMED_API_KEY', 'SEMANTIC_SCHOLAR_API_KEY']
    missing_keys = [key for key in required_keys if not os.getenv(key)]
    
    if missing_keys:
        pytest.skip(f"Skipping integration test: missing API keys: {', '.join(missing_keys)}")


@pytest.fixture
def skip_if_no_network():
    """Skip test if network access is not available."""
    try:
        import requests
        response = requests.get("https://www.ebi.ac.uk/chembl/api/data/status", timeout=5)
        if response.status_code != 200:
            pytest.skip("Skipping integration test: ChEMBL API not accessible")
    except Exception:
        pytest.skip("Skipping integration test: no network access")


def pytest_configure(config):
    """Configure pytest for integration tests."""
    config.addinivalue_line(
        "markers", "integration: mark test as integration test requiring real API access"
    )


def pytest_collection_modifyitems(config, items):
    """Modify test collection to handle integration tests."""
    if config.getoption("--run-integration"):
        # Run integration tests
        return
    
    # Skip integration tests by default
    skip_integration = pytest.mark.skip(reason="need --run-integration option to run")
    for item in items:
        if "integration" in item.keywords:
            item.add_marker(skip_integration)


def pytest_addoption(parser):
    """Add command line options for integration tests."""
    parser.addoption(
        "--run-integration",
        action="store_true",
        default=False,
        help="run integration tests that require real API access"
    )
