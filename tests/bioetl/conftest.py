"""Shared pytest fixtures for BioETL tests."""

from __future__ import annotations

import json
import sys
from collections.abc import Callable, Generator
from contextlib import AbstractContextManager, contextmanager
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest  # type: ignore[reportMissingImports]

from bioetl.config import PipelineConfig
from bioetl.config.models.base import PipelineMetadata
from bioetl.config.models.cli import CLIConfig
from bioetl.config.models.determinism import (
    DeterminismConfig,
    DeterminismHashingConfig,
    DeterminismSortingConfig,
)
from bioetl.config.models.http import HTTPClientConfig, HTTPConfig, RetryConfig
from bioetl.config.models.paths import MaterializationConfig
from bioetl.config.models.postprocess import PostprocessConfig
from bioetl.config.models.source import SourceConfig
from bioetl.config.models.validation import ValidationConfig
from bioetl.core.api_client import UnifiedAPIClient

PROJECT_ROOT = Path(__file__).resolve().parents[2]
SRC_DIR = PROJECT_ROOT / "src"

if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

DATA_DIR = Path(__file__).parent / "data"


def _load_json(relative_path: str) -> Any:
    """Загружает JSON из каталога ``tests/bioetl/data`` с каноничными параметрами."""

    data_path = DATA_DIR / relative_path
    with data_path.open("r", encoding="utf-8") as stream:
        return json.load(stream)


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
def sample_activity_data_raw() -> list[dict[str, Any]]:
    """Raw activity data as it would come from ChEMBL API."""

    raw_payload = _load_json("sample_activity_data_raw.json")
    return list(raw_payload)


@pytest.fixture  # type: ignore[misc]
def sample_activity_data() -> pd.DataFrame:
    """Sample activity DataFrame for testing."""

    dataset: dict[str, list[Any]] = _load_json("sample_activity_data.json")
    frame = pd.DataFrame(dataset)

    # Explicit nullable integer columns for deterministic schema assertions.
    nullable_columns = ("assay_tax_id", "record_id", "src_id", "target_tax_id")
    for column in nullable_columns:
        frame[column] = pd.Series(dataset[column], dtype="Int64")

    return frame


@pytest.fixture  # type: ignore[misc]
def sample_chembl_api_response() -> dict[str, Any]:
    """Sample ChEMBL API paginated response."""

    payload = _load_json("sample_chembl_api_response.json")
    return dict(payload)


@pytest.fixture  # type: ignore[misc]
def sample_chembl_status_response() -> dict[str, Any]:
    """Sample ChEMBL status API response."""

    payload = _load_json("sample_chembl_status_response.json")
    return dict(payload)


@pytest.fixture  # type: ignore[misc]
def pipeline_config_fixture(tmp_output_dir: Path) -> PipelineConfig:
    """Sample ``PipelineConfig`` for pipeline unit tests."""

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
            sort=DeterminismSortingConfig(by=[], ascending=[]),
            hashing=DeterminismHashingConfig(business_key_fields=()),
        ),
        validation=ValidationConfig(schema_out=None, strict=True, coerce=True),
        postprocess=PostprocessConfig(),
        sources={
            "chembl": SourceConfig(  # type: ignore[call-arg,dict-item]
                enabled=True,
                parameters={"base_url": "https://www.ebi.ac.uk/chembl/api/data"},
            )
        },
        cli=CLIConfig(date_tag="20240101"),  # type: ignore[attr-defined]
    )


@pytest.fixture  # type: ignore[misc]
def run_id() -> str:
    """Deterministic run identifier for tests."""

    return "test-run-12345"


@pytest.fixture  # type: ignore[misc]
def mock_api_client() -> MagicMock:
    """Mock ``UnifiedAPIClient`` with deterministic defaults."""

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
    """Mock HTTP response object."""

    response = MagicMock()
    response.status_code = 200
    response.json.return_value = {}
    response.headers = {}
    response.raise_for_status = MagicMock()
    return response


@pytest.fixture  # type: ignore[misc]
def mock_chembl_api_client() -> MagicMock:
    """Mock ChEMBL API client returning status and empty data."""

    mock_client = MagicMock(spec=UnifiedAPIClient)

    mock_status_response = MagicMock()
    mock_status_response.json.return_value = {
        "chembl_release": "33",
        "chembl_db_version": "33",
        "api_version": "1.0",
    }
    mock_status_response.status_code = 200
    mock_status_response.headers = {}

    mock_data_response = MagicMock()
    mock_data_response.json.return_value = {
        "page_meta": {"offset": 0, "limit": 25, "count": 0, "next": None},
        "activities": [],
    }
    mock_data_response.status_code = 200
    mock_data_response.headers = {}

    mock_client.get.side_effect = [mock_status_response, mock_data_response]

    return mock_client


@pytest.fixture  # type: ignore[misc]
def mock_api_client_factory_patch(
    mock_chembl_api_client: MagicMock,
) -> Callable[[MagicMock | None], AbstractContextManager[MagicMock]]:
    """Context manager fixture for patching ``APIClientFactory.for_source``."""

    def _factory(mock_client: MagicMock | None = None) -> Generator[MagicMock, None, None]:
        client = mock_client or mock_chembl_api_client
        with patch("bioetl.core.client_factory.APIClientFactory.for_source") as mock_factory:
            mock_factory.return_value = client
            yield mock_factory

    return contextmanager(_factory)


@pytest.fixture  # type: ignore[misc]
def mock_chembl_client_with_data(
    sample_activity_data_raw: list[dict[str, Any]],
) -> MagicMock:
    """Mock ChEMBL client preloaded with sample activity payload."""

    mock_client = MagicMock(spec=UnifiedAPIClient)

    mock_status_response = MagicMock()
    mock_status_response.json.return_value = {
        "chembl_release": "33",
        "chembl_db_version": "33",
    }
    mock_status_response.status_code = 200
    mock_status_response.headers = {}

    mock_activity_response = MagicMock()
    mock_activity_response.json.return_value = {
        "page_meta": {
            "offset": 0,
            "limit": 25,
            "count": len(sample_activity_data_raw),
            "next": None,
        },
        "activities": sample_activity_data_raw,
    }
    mock_activity_response.status_code = 200
    mock_activity_response.headers = {}

    mock_client.get.side_effect = [mock_status_response, mock_activity_response]

    return mock_client


@pytest.fixture  # type: ignore[misc]
def mock_chembl_responses_for_endpoint() -> (
    Callable[
        [dict[str, Any] | list[dict[str, Any]], str, int | None],
        tuple[MagicMock, MagicMock],
    ]
):
    """Factory fixture for constructing ChEMBL endpoint responses."""

    def _create_responses(
        endpoint_data: dict[str, Any] | list[dict[str, Any]],
        endpoint_type: str = "activities",
        count: int | None = None,
    ) -> tuple[MagicMock, MagicMock]:
        mock_status_response = MagicMock()
        mock_status_response.json.return_value = {
            "chembl_release": "33",
            "chembl_db_version": "33",
        }
        mock_status_response.status_code = 200
        mock_status_response.headers = {}

        items: list[dict[str, Any]]
        if isinstance(endpoint_data, dict):
            items = [endpoint_data]
        else:
            items = list(endpoint_data)

        effective_count = len(items) if count is None else count

        mock_data_response = MagicMock()
        mock_data_response.json.return_value = {
            "page_meta": {
                "offset": 0,
                "limit": 25,
                "count": effective_count,
                "next": None,
            },
            endpoint_type: items,
        }
        mock_data_response.status_code = 200
        mock_data_response.headers = {}

        return mock_status_response, mock_data_response

    return _create_responses
