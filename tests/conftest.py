"""Shared pytest fixtures for BioETL tests."""

# pyright: reportMissingImports=false
from __future__ import annotations

import sys
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

# Fix sys.path to prioritize current project over old one
# This ensures we import from bioactivity_data_acquisition2, not bioactivity_data_acquisition
_current_project_src = Path(__file__).parent.parent / "src"
_old_project_src = Path(__file__).parent.parent.parent / "bioactivity_data_acquisition" / "src"
if str(_current_project_src) in sys.path and str(_old_project_src) in sys.path:
    # Remove old path if it exists
    if str(_old_project_src) in sys.path:
        sys.path.remove(str(_old_project_src))
    # Ensure current path is first
    if str(_current_project_src) in sys.path:
        sys.path.remove(str(_current_project_src))
    sys.path.insert(0, str(_current_project_src))
elif str(_current_project_src) not in sys.path:
    sys.path.insert(0, str(_current_project_src))

import pandas as pd  # noqa: E402
import pytest  # noqa: E402

from bioetl.config import PipelineConfig  # noqa: E402
from bioetl.config.models import (  # noqa: E402, type: ignore[attr-defined]  # noqa: ISC001
    CLIConfig,
    DeterminismConfig,
    DeterminismHashingConfig,
    DeterminismSortingConfig,
    HTTPClientConfig,
    HTTPConfig,
    MaterializationConfig,
    PipelineMetadata,
    PostprocessConfig,
    RetryConfig,
    SourceConfig,
    ValidationConfig,
)
from bioetl.core.api_client import UnifiedAPIClient  # noqa: E402

# Models will be imported inside fixtures to avoid import errors


@pytest.fixture  # type: ignore[misc]
def tmp_output_dir(tmp_path: Path) -> Path:
    """Temporary directory for pipeline output artifacts."""
    output_dir = tmp_path / "output"
    output_dir.mkdir(parents=True, exist_ok=True)
    return output_dir


@pytest.fixture  # type: ignore[misc]
def tmp_logs_dir(tmp_path: Path) -> Path:
    """Temporary directory for pipeline logs."""
    logs_dir = tmp_path / "logs"
    logs_dir.mkdir(parents=True, exist_ok=True)
    return logs_dir


@pytest.fixture  # type: ignore[misc]
def golden_dir(tmp_path: Path) -> Path:
    """Directory for golden test snapshots."""
    golden = tmp_path / "golden"
    golden.mkdir(parents=True, exist_ok=True)
    return golden


@pytest.fixture  # type: ignore[misc]
def sample_activity_data() -> pd.DataFrame:
    """Sample activity DataFrame for testing."""
    import json

    # Правильный формат activity_properties: массив объектов с ключами из ACTIVITY_PROPERTY_KEYS
    activity_properties_valid = json.dumps([
        {
            "type": "IC50",
            "relation": "=",
            "units": "nM",
            "value": 10.5,
            "text_value": None,
            "result_flag": True
        }
    ])

    df = pd.DataFrame(
        {
            "activity_id": [1, 2, 3],
            "assay_chembl_id": ["CHEMBL100", "CHEMBL101", "CHEMBL102"],
            "assay_type": ["B", "F", None],
            "assay_description": ["Binding assay", "Functional assay", None],
            "assay_organism": ["Homo sapiens", "Mus musculus", None],
            "assay_tax_id": pd.Series([9606, 10090, None], dtype="Int64"),
            "testitem_chembl_id": ["CHEMBL1", "CHEMBL2", "CHEMBL3"],
            "molecule_chembl_id": ["CHEMBL1", "CHEMBL2", "CHEMBL3"],
            "parent_molecule_chembl_id": [None, "CHEMBL1", None],
            "molecule_pref_name": ["Molecule 1", "Molecule 2", None],
            "target_chembl_id": ["CHEMBL200", "CHEMBL201", "CHEMBL202"],
            "target_pref_name": ["Target 1", "Target 2", None],
            "document_chembl_id": ["CHEMBL300", "CHEMBL301", "CHEMBL302"],
            "record_id": pd.Series([100, 101, None], dtype="Int64"),
            "src_id": pd.Series([1, 2, None], dtype="Int64"),
            "type": ["IC50", "EC50", "Ki"],
            "relation": ["=", ">", "<="],
            "value": [10.5, 20.0, 5.3],
            "units": ["nM", "μM", "mM"],
            "standard_type": ["IC50", "EC50", "Ki"],
            "standard_relation": ["=", ">", "<="],
            "standard_value": [10.5, 20.0, 5.3],
            "standard_upper_value": [None, 25.0, None],
            "standard_units": ["nM", "μM", "mM"],
            "standard_text_value": [None, None, "5.3"],
            "standard_flag": [0, 1, 0],
            "upper_value": [None, 25.0, None],
            "lower_value": [None, 15.0, None],
            "pchembl_value": [7.98, 6.70, 8.28],
            "published_type": ["IC50", None, "Ki"],
            "published_relation": ["=", None, "<="],
            "published_value": [10.5, None, 5.3],
            "published_units": ["nM", None, "mM"],
            "uo_units": [None, "UO_0000001", None],
            "qudt_units": [None, None, "QUDT_0000001"],
            "text_value": [None, "Text value", None],
            "activity_comment": [None, "Test comment", None],
            "bao_endpoint": ["BAO_0000001", "BAO_0000002", None],
            "bao_format": ["BAO_0000003", None, "BAO_0000004"],
            "bao_label": ["Binding", "Functional", None],
            "canonical_smiles": ["CCO", "CCN", "CCC"],
            "ligand_efficiency": [None, '{"LE": 0.5}', None],
            "target_organism": ["Homo sapiens", "Mus musculus", None],
            "target_tax_id": pd.Series([9606, 10090, 9606], dtype="Int64"),
            "data_validity_comment": [None, None, "Validated"],
            "data_validity_description": [None, "Validated description", None],
            "potential_duplicate": [False, True, None],
            "activity_properties": [None, activity_properties_valid, None],
            "compound_key": ["key1", "key2", "key3"],
        }
    )
    df["target_tax_id"] = pd.Series([9606, 10090, 9606], dtype="Int64")
    df["assay_tax_id"] = pd.Series([9606, 10090, None], dtype="Int64")
    df["record_id"] = pd.Series([100, 101, None], dtype="Int64")
    df["src_id"] = pd.Series([1, 2, None], dtype="Int64")
    return df


@pytest.fixture  # type: ignore[misc]
def sample_activity_data_raw() -> list[dict[str, Any]]:
    """Raw activity data as it would come from ChEMBL API."""
    return [
        {
            "activity_id": 1,
            "molecule_chembl_id": "CHEMBL1",
            "assay_chembl_id": "CHEMBL100",
            "assay_type": "B",
            "assay_description": "Binding assay",
            "assay_organism": "Homo sapiens",
            "assay_tax_id": 9606,
            "testitem_chembl_id": "CHEMBL1",
            "target_chembl_id": "CHEMBL200",
            "target_pref_name": "Target 1",
            "document_chembl_id": "CHEMBL300",
            "record_id": 100,
            "src_id": 1,
            "standard_type": "IC50",
            "standard_relation": "=",
            "standard_value": 10.5,
            "standard_upper_value": None,
            "standard_units": "nM",
            "pchembl_value": 7.98,
            "published_type": "IC50",
            "published_relation": "=",
            "published_value": 10.5,
            "published_units": "nM",
            "bao_endpoint": "BAO_0000001",
            "bao_format": "BAO_0000003",
            "bao_label": "Binding",
            "canonical_smiles": "CCO",
            "target_organism": "Homo sapiens",
            "target_tax_id": 9606,
            "data_validity_description": None,
            "potential_duplicate": 0,
        },
        {
            "activity_id": 2,
            "molecule_chembl_id": "CHEMBL2",
            "parent_molecule_chembl_id": "CHEMBL1",
            "molecule_pref_name": "Molecule 2",
            "assay_chembl_id": "CHEMBL101",
            "assay_type": "F",
            "assay_description": "Functional assay",
            "assay_organism": "Mus musculus",
            "assay_tax_id": 10090,
            "testitem_chembl_id": "CHEMBL2",
            "target_chembl_id": "CHEMBL201",
            "target_pref_name": "Target 2",
            "document_chembl_id": "CHEMBL301",
            "record_id": 101,
            "src_id": 2,
            "standard_type": "EC50",
            "standard_relation": ">",
            "standard_value": 20.0,
            "standard_upper_value": 25.0,
            "standard_units": "μM",
            "pchembl_value": 6.70,
            "published_type": None,
            "published_relation": None,
            "published_value": None,
            "published_units": None,
            "uo_units": "UO_0000001",
            "text_value": "Text value",
            "bao_endpoint": "BAO_0000002",
            "bao_label": "Functional",
            "ligand_efficiency": {"LE": 0.5},
            "target_organism": "Mus musculus",
            "target_tax_id": 10090,
            "data_validity_description": "Validated description",
            "activity_properties": {"property": "value"},
            "potential_duplicate": 1,
        },
    ]


@pytest.fixture  # type: ignore[misc]
def pipeline_config_fixture(tmp_output_dir: Path) -> PipelineConfig:
    """Sample PipelineConfig for testing."""
    return PipelineConfig(  # type: ignore[call-arg]
        version=1,
        pipeline=PipelineMetadata(  # type: ignore[call-arg]
            name="activity_chembl",
            version="1.0.0",
            description="Test activity pipeline",
        ),
        http=HTTPConfig(
            default=HTTPClientConfig(
                timeout_sec=30.0,
                connect_timeout_sec=10.0,
                read_timeout_sec=30.0,
                retries=RetryConfig(total=3, backoff_multiplier=2.0, backoff_max=10.0),
            ),
        ),
        materialization=MaterializationConfig(root=str(tmp_output_dir)),
        determinism=DeterminismConfig(  # type: ignore[call-arg]
            sort=DeterminismSortingConfig(
                by=[],
                ascending=[],
            ),
            hashing=DeterminismHashingConfig(
                business_key_fields=(),
            ),
        ),
        validation=ValidationConfig(
            schema_out=None,
            strict=True,
            coerce=True,
        ),
        postprocess=PostprocessConfig(),
        sources={
            "chembl": SourceConfig(  # type: ignore[call-arg,dict-item]
                enabled=True,
                parameters={"base_url": "https://www.ebi.ac.uk/chembl/api/data"},
            ),
        },
        cli=CLIConfig(date_tag="20240101"),  # type: ignore[attr-defined]
    )


@pytest.fixture  # type: ignore[misc]
def mock_api_client() -> MagicMock:
    """Mock UnifiedAPIClient for testing."""
    mock_client = MagicMock(spec=UnifiedAPIClient)
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = {"data": "test"}
    mock_response.headers = {}
    mock_client.get.return_value = mock_response
    mock_client.request.return_value = mock_response
    mock_client.request_json.return_value = {"data": "test"}
    return mock_client


@pytest.fixture  # type: ignore[misc]
def mock_http_response() -> MagicMock:
    """Mock HTTP response for testing."""
    response = MagicMock()
    response.status_code = 200
    response.json.return_value = {}
    response.headers = {}
    response.raise_for_status = MagicMock()
    return response


@pytest.fixture  # type: ignore[misc]
def httpserver() -> Any:  # type: ignore[valid-type,empty-body]
    """HTTPServer fixture for integration tests (from pytest-httpserver)."""
    # This fixture is provided by pytest-httpserver plugin
    # We just declare it here for type hints
    pass  # pragma: no cover  # type: ignore[empty-body]


@pytest.fixture  # type: ignore[misc]
def run_id() -> str:
    """Sample run_id for testing."""
    return "test-run-12345"


@pytest.fixture  # type: ignore[misc]
def sample_chembl_api_response() -> dict[str, Any]:
    """Sample ChEMBL API response structure."""
    return {
        "page_meta": {
            "offset": 0,
            "limit": 25,
            "count": 2,
            "next": None,
        },
        "activities": [
            {
                "activity_id": 1,
                "molecule_chembl_id": "CHEMBL1",
                "assay_chembl_id": "CHEMBL100",
                "target_chembl_id": "CHEMBL200",
                "document_chembl_id": "CHEMBL300",
                "standard_type": "IC50",
                "standard_relation": "=",
                "standard_value": 10.5,
                "standard_units": "nM",
            },
            {
                "activity_id": 2,
                "molecule_chembl_id": "CHEMBL2",
                "assay_chembl_id": "CHEMBL101",
                "target_chembl_id": "CHEMBL201",
                "document_chembl_id": "CHEMBL301",
                "standard_type": "EC50",
                "standard_relation": ">",
                "standard_value": 20.0,
                "standard_units": "μM",
            },
        ],
    }


@pytest.fixture  # type: ignore[misc]
def sample_chembl_status_response() -> dict[str, Any]:
    """Sample ChEMBL status API response."""
    return {
        "chembl_release": "33",
        "chembl_db_version": "33",
        "release_date": "2024-01-01",
    }


@pytest.fixture  # type: ignore[misc]
def mock_chembl_api_client() -> MagicMock:
    """Mock ChEMBL API client with standard responses."""
    mock_client = MagicMock(spec=UnifiedAPIClient)
    
    # Create standard status response
    mock_status_response = MagicMock()
    mock_status_response.json.return_value = {"chembl_release": "33", "chembl_db_version": "33", "api_version": "1.0"}
    mock_status_response.status_code = 200
    mock_status_response.headers = {}
    
    # Create empty data response by default
    mock_data_response = MagicMock()
    mock_data_response.json.return_value = {
        "page_meta": {"offset": 0, "limit": 25, "count": 0, "next": None},
        "activities": [],
    }
    mock_data_response.status_code = 200
    mock_data_response.headers = {}
    
    # Default side_effect: status first, then data
    mock_client.get.side_effect = [mock_status_response, mock_data_response]
    
    return mock_client


@pytest.fixture  # type: ignore[misc]
def mock_api_client_factory_patch(mock_chembl_api_client: MagicMock) -> Any:
    """Pytest fixture that provides a context manager for mocking APIClientFactory.for_source."""
    from contextlib import contextmanager
    from unittest.mock import patch
    
    @contextmanager
    def _factory(mock_client: MagicMock | None = None):
        """Context manager for patching APIClientFactory.for_source."""
        client = mock_client or mock_chembl_api_client
        with patch("bioetl.core.client_factory.APIClientFactory.for_source") as mock_factory:
            mock_factory.return_value = client
            yield mock_factory
    
    return _factory


@pytest.fixture  # type: ignore[misc]
def mock_chembl_client_with_data(sample_activity_data_raw: list[dict[str, Any]]) -> MagicMock:
    """Mock ChEMBL API client with sample activity data."""
    mock_client = MagicMock(spec=UnifiedAPIClient)
    
    # Status response
    mock_status_response = MagicMock()
    mock_status_response.json.return_value = {"chembl_release": "33", "chembl_db_version": "33"}
    mock_status_response.status_code = 200
    mock_status_response.headers = {}
    
    # Activity data response
    mock_activity_response = MagicMock()
    mock_activity_response.json.return_value = {
        "page_meta": {"offset": 0, "limit": 25, "count": len(sample_activity_data_raw), "next": None},
        "activities": sample_activity_data_raw,
    }
    mock_activity_response.status_code = 200
    mock_activity_response.headers = {}
    
    mock_client.get.side_effect = [mock_status_response, mock_activity_response]
    
    return mock_client


@pytest.fixture  # type: ignore[misc]
def mock_chembl_responses_for_endpoint() -> Any:
    """Factory fixture to create mock responses for specific ChEMBL endpoints."""
    def _create_responses(
        endpoint_data: dict[str, Any] | list[dict[str, Any]],
        endpoint_type: str = "activities",
        count: int | None = None,
    ) -> tuple[MagicMock, MagicMock]:
        """Create mock status and data responses.
        
        Args:
            endpoint_data: Data for the endpoint (dict for single, list for multiple)
            endpoint_type: Type of endpoint (activities, assays, targets, etc.)
            count: Number of items (auto-detected if None)
        
        Returns:
            Tuple of (status_response, data_response)
        """
        # Status response
        mock_status_response = MagicMock()
        mock_status_response.json.return_value = {"chembl_release": "33", "chembl_db_version": "33"}
        mock_status_response.status_code = 200
        mock_status_response.headers = {}
        
        # Determine count
        if count is None:
            if isinstance(endpoint_data, list):
                count = len(endpoint_data)
            else:
                count = 1
                endpoint_data = [endpoint_data]
        elif isinstance(endpoint_data, dict):
            endpoint_data = [endpoint_data]
        
        # Data response
        mock_data_response = MagicMock()
        mock_data_response.json.return_value = {
            "page_meta": {"offset": 0, "limit": 25, "count": count, "next": None},
            endpoint_type: endpoint_data if isinstance(endpoint_data, list) else [endpoint_data],
        }
        mock_data_response.status_code = 200
        mock_data_response.headers = {}
        
        return mock_status_response, mock_data_response
    
    return _create_responses