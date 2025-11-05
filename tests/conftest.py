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
    df = pd.DataFrame(
        {
            "activity_id": [1, 2, 3],
            "assay_chembl_id": ["CHEMBL100", "CHEMBL101", "CHEMBL102"],
            "testitem_chembl_id": ["CHEMBL1", "CHEMBL2", "CHEMBL3"],
            "molecule_chembl_id": ["CHEMBL1", "CHEMBL2", "CHEMBL3"],
            "target_chembl_id": ["CHEMBL200", "CHEMBL201", "CHEMBL202"],
            "document_chembl_id": ["CHEMBL300", "CHEMBL301", "CHEMBL302"],
            "standard_type": ["IC50", "EC50", "Ki"],
            "standard_relation": ["=", ">", "<="],
            "standard_value": [10.5, 20.0, 5.3],
            "standard_units": ["nM", "μM", "mM"],
            "pchembl_value": [7.98, 6.70, 8.28],
            "bao_endpoint": ["BAO_0000001", "BAO_0000002", None],
            "bao_format": ["BAO_0000003", None, "BAO_0000004"],
            "bao_label": ["Binding", "Functional", None],
            "canonical_smiles": ["CCO", "CCN", "CCC"],
            "ligand_efficiency": [None, '{"LE": 0.5}', None],
            "target_organism": ["Homo sapiens", "Mus musculus", None],
            "target_tax_id": pd.Series([9606, 10090, 9606], dtype="Int64"),
            "data_validity_comment": [None, None, "Validated"],
            "potential_duplicate": [False, True, None],
            "activity_properties": [None, '{"property": "value"}', None],
            "compound_key": ["key1", "key2", "key3"],
        }
    )
    df["target_tax_id"] = pd.Series([9606, 10090, 9606], dtype="Int64")
    return df


@pytest.fixture  # type: ignore[misc]
def sample_activity_data_raw() -> list[dict[str, Any]]:
    """Raw activity data as it would come from ChEMBL API."""
    return [
        {
            "activity_id": 1,
            "molecule_chembl_id": "CHEMBL1",
            "assay_chembl_id": "CHEMBL100",
            "testitem_chembl_id": "CHEMBL1",
            "target_chembl_id": "CHEMBL200",
            "document_chembl_id": "CHEMBL300",
            "standard_type": "IC50",
            "standard_relation": "=",
            "standard_value": 10.5,
            "standard_units": "nM",
            "pchembl_value": 7.98,
            "bao_endpoint": "BAO_0000001",
            "bao_format": "BAO_0000003",
            "bao_label": "Binding",
            "canonical_smiles": "CCO",
            "target_organism": "Homo sapiens",
            "target_tax_id": 9606,
            "potential_duplicate": 0,
        },
        {
            "activity_id": 2,
            "molecule_chembl_id": "CHEMBL2",
            "assay_chembl_id": "CHEMBL101",
            "testitem_chembl_id": "CHEMBL2",
            "target_chembl_id": "CHEMBL201",
            "document_chembl_id": "CHEMBL301",
            "standard_type": "EC50",
            "standard_relation": ">",
            "standard_value": 20.0,
            "standard_units": "μM",
            "pchembl_value": 6.70,
            "bao_endpoint": "BAO_0000002",
            "bao_label": "Functional",
            "ligand_efficiency": {"LE": 0.5},
            "target_organism": "Mus musculus",
            "target_tax_id": 10090,
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

