"""Shared pytest fixtures and configuration."""

import os
import sys
import tempfile
from collections.abc import Generator
from pathlib import Path
from types import ModuleType

import pandas as pd
import pytest

try:
    from faker import Faker
except ModuleNotFoundError as exc:  # pragma: no cover - import guard
    raise RuntimeError(
        "Faker is required for test fixtures. Install it via `pip install faker` "
        "or include the `bioetl[dev]` extra before running the test suite."
    ) from exc

# Add src to path so imports work
project_root = Path(__file__).parent.parent
src_path = project_root / "src"
if str(src_path) not in sys.path:
    sys.path.insert(0, str(src_path))

# Configure Faker for consistent test data
fake = Faker()
Faker.seed(42)  # For reproducible test data


class _DummyTTLCache(dict):
    """Minimal TTLCache stub used when ``cachetools`` is unavailable."""

    def __init__(self, maxsize, ttl):  # noqa: D401 - simple stub
        super().__init__()
        self.maxsize = maxsize
        self.ttl = ttl


def _register_cachetools_stub() -> None:
    """Ensure a ``cachetools`` stub with ``TTLCache`` is available for tests."""

    module = sys.modules.get("cachetools")
    if module is None:
        stub = ModuleType("cachetools")
        stub.TTLCache = _DummyTTLCache
        sys.modules["cachetools"] = stub
        return

    if not hasattr(module, "TTLCache"):
        module.TTLCache = _DummyTTLCache


_register_cachetools_stub()


@pytest.fixture
def temp_dir() -> Generator[Path, None, None]:
    """Create temporary directory for test files."""
    with tempfile.TemporaryDirectory() as tmp_dir:
        yield Path(tmp_dir)


@pytest.fixture
def sample_chembl_data() -> pd.DataFrame:
    """Sample ChEMBL data for testing."""
    return pd.DataFrame({
        "assay_chembl_id": ["CHEMBL1", "CHEMBL2", "CHEMBL3"],
        "assay_type": ["B", "F", "B"],
        "assay_category": ["SINGLE", "SINGLE", "SINGLE"],
        "assay_tax_id": [9606, 10090, 9606],
        "assay_organism": ["Homo sapiens", "Mus musculus", "Homo sapiens"],
        "assay_strain": [None, "C57BL/6", None],
        "assay_tissue": ["Brain", "Liver", "Heart"],
        "assay_cell_type": [None, "Hepatocyte", None],
        "assay_subcellular_fraction": [None, "Membrane", None],
        "confidence_score": [9, 8, 7],
        "src_id": [1, 2, 3],
        "chembl_id": ["CHEMBL1", "CHEMBL2", "CHEMBL3"],
        "species_group_flag": [0, 0, 0],
        "tax_id": [9606, 10090, 9606],
        "organism": ["Homo sapiens", "Mus musculus", "Homo sapiens"],
        "component_type": ["PROTEIN", "PROTEIN", "PROTEIN"],
        "component_name": ["Protein A", "Protein B", "Protein C"],
        "component_class": ["Enzyme", "Receptor", "Enzyme"],
        "component_description": ["Test protein A", "Test protein B", "Test protein C"],
        "component_synonym": ["Synonym A", "Synonym B", "Synonym C"],
        "component_count": [1, 1, 1],
        "assay_class_id": [1, 2, 1],
        "assay_class": ["Binding", "Functional", "Binding"],
        "assay_class_description": ["Test binding", "Test functional", "Test binding"],
        "variant_id": [1, 2, 1],
        "variant_name": ["Variant A", "Variant B", "Variant A"],
        "variant_description": ["Test variant A", "Test variant B", "Test variant A"],
        "assay_param_type": ["IC50", "EC50", "IC50"],
        "assay_param_value": [100.0, 200.0, 150.0],
        "assay_param_standard_value": [100.0, 200.0, 150.0],
        "assay_param_unit": ["nM", "nM", "nM"],
        "assay_param_relation": ["=", "=", "="],
        "assay_param_text_value": [None, None, None],
        "assay_param_standard_relation": ["=", "=", "="],
        "assay_param_standard_text_value": [None, None, None],
        "assay_param_standard_flag": [0, 0, 0],
        "assay_param_standard_type": ["IC50", "EC50", "IC50"],
        "assay_param_standard_unit": ["nM", "nM", "nM"],
        "assay_param_standard_relation_flag": [0, 0, 0],
        "assay_param_standard_text_value_flag": [0, 0, 0],
        "fallback_retry_after_sec": [60, 60, 60],
    })


@pytest.fixture
def sample_activity_data() -> pd.DataFrame:
    """Sample activity data for testing."""
    return pd.DataFrame({
        "activity_id": [1, 2, 3],
        "assay_chembl_id": ["CHEMBL1", "CHEMBL2", "CHEMBL3"],
        "molecule_chembl_id": ["CHEMBL100", "CHEMBL101", "CHEMBL102"],
        "standard_type": ["IC50", "EC50", "IC50"],
        "standard_value": [100.0, 200.0, 150.0],
        "standard_units": ["nM", "nM", "nM"],
        "standard_relation": ["=", "=", "="],
        "standard_flag": [0, 0, 0],
        "standard_text_value": [None, None, None],
        "standard_upper_value": [None, None, None],
        "standard_lower_value": [None, None, None],
        "standard_upper_units": [None, None, None],
        "standard_lower_units": [None, None, None],
        "standard_upper_relation": [None, None, None],
        "standard_lower_relation": [None, None, None],
        "standard_upper_flag": [0, 0, 0],
        "standard_lower_flag": [0, 0, 0],
        "standard_upper_text_value": [None, None, None],
        "standard_lower_text_value": [None, None, None],
        "activity_comment": ["Test comment 1", "Test comment 2", "Test comment 3"],
        "data_validity_comment": [None, None, None],
        "potential_duplicate": [0, 0, 0],
        "pchembl_value": [6.0, 5.7, 5.8],
        "bao_endpoint": ["BAO_0000004", "BAO_0000004", "BAO_0000004"],
        "uo_units": ["UO_0000065", "UO_0000065", "UO_0000065"],
        "qudt_units": ["QUDT_0000065", "QUDT_0000065", "QUDT_0000065"],
        "normalized_value": [100.0, 200.0, 150.0],
        "normalized_units": ["nM", "nM", "nM"],
        "normalized_type": ["IC50", "EC50", "IC50"],
        "normalized_relation": ["=", "=", "="],
        "normalized_text_value": [None, None, None],
        "normalized_upper_value": [None, None, None],
        "normalized_lower_value": [None, None, None],
        "normalized_upper_units": [None, None, None],
        "normalized_lower_units": [None, None, None],
        "normalized_upper_relation": [None, None, None],
        "normalized_lower_relation": [None, None, None],
        "normalized_upper_flag": [0, 0, 0],
        "normalized_lower_flag": [0, 0, 0],
        "normalized_upper_text_value": [None, None, None],
        "normalized_lower_text_value": [None, None, None],
        "fallback_retry_after_sec": [60, 60, 60],
    })


@pytest.fixture
def sample_document_data() -> pd.DataFrame:
    """Sample document data for testing."""
    return pd.DataFrame({
        "document_chembl_id": ["CHEMBL1137491", "CHEMBL1155082", "CHEMBL4000255"],
        "pubmed_id": [17827018, 18578478, 28337320],
        "doi": [
            "10.1016/j.bmc.2007.08.038",
            "10.1021/jm800092x",
            "10.1021/acsmedchemlett.6b00465",
        ],
        "title": [
            "Click chemistry based solid phase supported synthesis",
            "Chemo-enzymatic synthesis of glutamate analogues",
            "Discovery of ABBV-075 (mivebresib)",
        ],
        "abstract": ["Abstract 1", "Abstract 2", "Abstract 3"],
        "authors": ["Author 1", "Author 2", "Author 3"],
        "journal": ["J. Med. Chem.", "J. Med. Chem.", "ACS Med. Chem. Lett."],
        "year": [2008, 2008, 2017],
        "classification": ["Journal Article", "Journal Article", "Journal Article"],
        "document_contains_external_links": [True, False, True],
        "is_experimental_doc": [True, True, True],
    })


@pytest.fixture
def mock_config():
    """Mock configuration for testing."""
    from bioetl.config import Config

    return Config(
        version=1,
        pipeline={"name": "test", "entity": "test"},
        http={
            "global": {
                "timeout_sec": 60.0,
                "retries": {"total": 3, "backoff_multiplier": 2.0, "backoff_max": 120.0},
                "rate_limit": {"max_calls": 5, "period": 15.0},
            }
        },
        cache={"enabled": True, "directory": "data/cache", "ttl": 86400, "release_scoped": True},
        paths={"input_root": "data/input", "output_root": "data/output"},
        determinism={"sort": {"by": [], "ascending": []}, "column_order": []},
        postprocess={},
        qc={"enabled": True, "severity_threshold": "warning"},
        cli={},
    )


@pytest.fixture(autouse=True)
def reset_environment():
    """Reset environment variables after each test."""
    original_env = os.environ.copy()
    yield
    os.environ.clear()
    os.environ.update(original_env)


@pytest.fixture
def mock_requests(monkeypatch):
    """Mock requests library for API testing."""
    from unittest.mock import Mock

    import requests

    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.json.return_value = {"data": []}
    mock_response.text = '{"data": []}'

    mock_get = Mock(return_value=mock_response)
    mock_post = Mock(return_value=mock_response)

    monkeypatch.setattr(requests, "get", mock_get)
    monkeypatch.setattr(requests, "post", mock_post)

    return {"get": mock_get, "post": mock_post}

