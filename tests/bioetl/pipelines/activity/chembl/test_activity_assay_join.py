"""Integration tests for Activity assay join in extract stage."""

from __future__ import annotations

from unittest.mock import MagicMock

import pandas as pd
import pytest

from bioetl.clients.client_chembl_common import ChemblClient
from bioetl.config import PipelineConfig
from bioetl.config.models.base import PipelineMetadata
from bioetl.config.models.cache import CacheConfig
from bioetl.config.models.http import HTTPClientConfig, HTTPConfig, RetryConfig
from bioetl.config.models.paths import PathsConfig
from bioetl.config.models.validation import ValidationConfig
from bioetl.core.http.api_client import UnifiedAPIClient
from bioetl.core.logging import UnifiedLogger
from bioetl.pipelines.chembl.activity.run import ChemblActivityPipeline


@pytest.fixture
def mock_chembl_client() -> ChemblClient:
    """Create a mock ChemblClient for testing."""
    mock_api_client = MagicMock(spec=UnifiedAPIClient)
    return ChemblClient(mock_api_client)


@pytest.fixture
def sample_activity_df() -> pd.DataFrame:
    """Sample activity DataFrame for extract testing."""
    return pd.DataFrame(
        {
            "activity_id": [1, 2, 3, 4, 5],
            "assay_chembl_id": ["CHEMBL100", "CHEMBL101", "CHEMBL102", "CHEMBL103", None],
            "molecule_chembl_id": ["CHEMBL1", "CHEMBL2", "CHEMBL3", "CHEMBL4", "CHEMBL5"],
            "testitem_chembl_id": ["CHEMBL1", "CHEMBL2", "CHEMBL3", "CHEMBL4", "CHEMBL5"],
        }
    )


@pytest.fixture
def mock_pipeline_config() -> PipelineConfig:
    """Create a mock PipelineConfig for testing."""
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
        sources={},
        paths=PathsConfig(output_root="data/output", cache_root="data/cache"),
        cache=CacheConfig(enabled=False),
        validation=ValidationConfig(
            schema_out="bioetl.schemas.chembl_activity_schema.ActivitySchema"
        ),
    )


@pytest.fixture
def mock_pipeline(mock_pipeline_config: PipelineConfig) -> ChemblActivityPipeline:
    """Create a mock ChemblActivityPipeline for testing."""
    return ChemblActivityPipeline(mock_pipeline_config, run_id="test-run-123")


@pytest.mark.integration
class TestActivityAssayJoin:
    """Test suite for activity assay join in extract stage."""

    def test_extract_assay_fields_adds_columns(
        self,
        mock_pipeline: ChemblActivityPipeline,
        mock_chembl_client: ChemblClient,
        sample_activity_df: pd.DataFrame,
    ) -> None:
        """Test that _extract_assay_fields adds expected columns."""
        # Mock fetch_assays_by_ids to return sample data
        mock_chembl_client.fetch_assays_by_ids = MagicMock(  # type: ignore[method-assign]
            return_value={
                "CHEMBL100": {
                    "assay_chembl_id": "CHEMBL100",
                    "assay_organism": "Homo sapiens",
                    "assay_tax_id": "9606",
                },
                "CHEMBL101": {
                    "assay_chembl_id": "CHEMBL101",
                    "assay_organism": "Mus musculus",
                    "assay_tax_id": "10090",
                },
            }
        )

        log = UnifiedLogger.get(__name__).bind(component="test")
        result = mock_pipeline._extract_assay_fields(sample_activity_df, mock_chembl_client, log)  # type: ignore[reportPrivateUsage]

        # Check that new columns are present
        assert "assay_organism" in result.columns
        assert "assay_tax_id" in result.columns

        # Check that original columns are preserved
        assert "activity_id" in result.columns
        assert "assay_chembl_id" in result.columns

    def test_extract_assay_fields_matches_records(
        self,
        mock_pipeline: ChemblActivityPipeline,
        mock_chembl_client: ChemblClient,
        sample_activity_df: pd.DataFrame,
    ) -> None:
        """Test that _extract_assay_fields correctly matches records by assay_chembl_id."""
        mock_chembl_client.fetch_assays_by_ids = MagicMock(  # type: ignore[method-assign]
            return_value={
                "CHEMBL100": {
                    "assay_chembl_id": "CHEMBL100",
                    "assay_organism": "Homo sapiens",
                    "assay_tax_id": "9606",
                },
                "CHEMBL102": {
                    "assay_chembl_id": "CHEMBL102",
                    "assay_organism": "Rattus norvegicus",
                    "assay_tax_id": "10116",
                },
            }
        )

        log = UnifiedLogger.get(__name__).bind(component="test")
        result = mock_pipeline._extract_assay_fields(sample_activity_df, mock_chembl_client, log)  # type: ignore[reportPrivateUsage]

        # First row (CHEMBL100) should have matched data
        first_row_organism = result.iloc[0]["assay_organism"]
        assert not pd.isna(first_row_organism), f"Expected non-NA value, got {first_row_organism}"
        assert first_row_organism == "Homo sapiens"

        first_row_tax_id = result.iloc[0]["assay_tax_id"]
        assert not pd.isna(first_row_tax_id), f"Expected non-NA value, got {first_row_tax_id}"
        assert first_row_tax_id == 9606

        # Third row (CHEMBL102) should have matched data
        third_row_organism = result.iloc[2]["assay_organism"]
        assert not pd.isna(third_row_organism), f"Expected non-NA value, got {third_row_organism}"
        assert third_row_organism == "Rattus norvegicus"

        third_row_tax_id = result.iloc[2]["assay_tax_id"]
        assert not pd.isna(third_row_tax_id), f"Expected non-NA value, got {third_row_tax_id}"
        assert third_row_tax_id == 10116

        # Second row (CHEMBL101) should have NA (no match)
        assert pd.isna(result.iloc[1]["assay_organism"])
        assert pd.isna(result.iloc[1]["assay_tax_id"])

        # Last row (None assay_chembl_id) should have NA
        assert pd.isna(result.iloc[4]["assay_organism"])
        assert pd.isna(result.iloc[4]["assay_tax_id"])

    def test_extract_assay_fields_handles_missing_records(
        self,
        mock_pipeline: ChemblActivityPipeline,
        mock_chembl_client: ChemblClient,
        sample_activity_df: pd.DataFrame,
    ) -> None:
        """Test that _extract_assay_fields handles missing assay records gracefully."""
        mock_chembl_client.fetch_assays_by_ids = MagicMock(  # type: ignore[method-assign]
            return_value={}
        )

        log = UnifiedLogger.get(__name__).bind(component="test")
        result = mock_pipeline._extract_assay_fields(sample_activity_df, mock_chembl_client, log)  # type: ignore[reportPrivateUsage]

        # All enrichment columns should be present but filled with NA
        assert all(pd.isna(result["assay_organism"]))
        assert all(pd.isna(result["assay_tax_id"]))

    def test_extract_assay_fields_handles_none_values(
        self,
        mock_pipeline: ChemblActivityPipeline,
        mock_chembl_client: ChemblClient,
        sample_activity_df: pd.DataFrame,
    ) -> None:
        """Test that _extract_assay_fields handles None/NA values in assay_chembl_id."""
        mock_chembl_client.fetch_assays_by_ids = MagicMock(  # type: ignore[method-assign]
            return_value={
                "CHEMBL100": {
                    "assay_chembl_id": "CHEMBL100",
                    "assay_organism": "Homo sapiens",
                    "assay_tax_id": "9606",
                },
            }
        )

        log = UnifiedLogger.get(__name__).bind(component="test")
        result = mock_pipeline._extract_assay_fields(sample_activity_df, mock_chembl_client, log)  # type: ignore[reportPrivateUsage]

        # Should not crash and should have NA for rows with None assay_chembl_id
        assert len(result) == len(sample_activity_df)
        # Last row has None assay_chembl_id, should have NA
        assert pd.isna(result.iloc[4]["assay_organism"])
        assert pd.isna(result.iloc[4]["assay_tax_id"])

    def test_extract_assay_fields_preserves_row_order(
        self,
        mock_pipeline: ChemblActivityPipeline,
        mock_chembl_client: ChemblClient,
        sample_activity_df: pd.DataFrame,
    ) -> None:
        """Test that _extract_assay_fields preserves the original row order."""
        mock_chembl_client.fetch_assays_by_ids = MagicMock(  # type: ignore[method-assign]
            return_value={
                "CHEMBL102": {
                    "assay_chembl_id": "CHEMBL102",
                    "assay_organism": "Rattus norvegicus",
                    "assay_tax_id": "10116",
                },
            }
        )

        log = UnifiedLogger.get(__name__).bind(component="test")
        result = mock_pipeline._extract_assay_fields(sample_activity_df, mock_chembl_client, log)  # type: ignore[reportPrivateUsage]

        # Check that activity_ids are in the same order
        assert list(result["activity_id"]) == list(sample_activity_df["activity_id"])
        # Third row (index 2) should have the matched data
        third_row_organism = result.iloc[2]["assay_organism"]
        assert not pd.isna(third_row_organism), f"Expected non-NA value, got {third_row_organism}"
        assert third_row_organism == "Rattus norvegicus"

    def test_extract_assay_fields_validates_types(
        self,
        mock_pipeline: ChemblActivityPipeline,
        mock_chembl_client: ChemblClient,
        sample_activity_df: pd.DataFrame,
    ) -> None:
        """Test that _extract_assay_fields validates types correctly."""
        mock_chembl_client.fetch_assays_by_ids = MagicMock(  # type: ignore[method-assign]
            return_value={
                "CHEMBL100": {
                    "assay_chembl_id": "CHEMBL100",
                    "assay_organism": "Homo sapiens",
                    "assay_tax_id": "9606",  # String, should be converted to Int64
                },
            }
        )

        log = UnifiedLogger.get(__name__).bind(component="test")
        result = mock_pipeline._extract_assay_fields(sample_activity_df, mock_chembl_client, log)  # type: ignore[reportPrivateUsage]

        # Check types
        assert result["assay_organism"].dtype == "string"
        assert result["assay_tax_id"].dtype == "Int64"

        # Check values
        first_row_tax_id = result.iloc[0]["assay_tax_id"]
        assert not pd.isna(first_row_tax_id), f"Expected non-NA value, got {first_row_tax_id}"
        assert first_row_tax_id == 9606
        assert isinstance(first_row_tax_id, (int, pd.Int64Dtype().type))

    def test_extract_assay_fields_validates_tax_id_range(
        self,
        mock_pipeline: ChemblActivityPipeline,
        mock_chembl_client: ChemblClient,
        sample_activity_df: pd.DataFrame,
    ) -> None:
        """Test that _extract_assay_fields validates tax_id range (>= 1)."""
        mock_chembl_client.fetch_assays_by_ids = MagicMock(  # type: ignore[method-assign]
            return_value={
                "CHEMBL100": {
                    "assay_chembl_id": "CHEMBL100",
                    "assay_organism": "Homo sapiens",
                    "assay_tax_id": "9606",  # Valid: >= 1
                },
                "CHEMBL101": {
                    "assay_chembl_id": "CHEMBL101",
                    "assay_organism": "Mus musculus",
                    "assay_tax_id": "0",  # Invalid: < 1, should be converted to NA
                },
            }
        )

        log = UnifiedLogger.get(__name__).bind(component="test")
        result = mock_pipeline._extract_assay_fields(sample_activity_df, mock_chembl_client, log)  # type: ignore[reportPrivateUsage]

        # First row should have valid tax_id
        first_row_tax_id = result.iloc[0]["assay_tax_id"]
        assert not pd.isna(first_row_tax_id), f"Expected non-NA value, got {first_row_tax_id}"
        assert first_row_tax_id >= 1

        # Second row should have NA for invalid tax_id
        assert pd.isna(result.iloc[1]["assay_tax_id"])

    def test_extract_assay_fields_handles_missing_tax_id(
        self,
        mock_pipeline: ChemblActivityPipeline,
        mock_chembl_client: ChemblClient,
        sample_activity_df: pd.DataFrame,
    ) -> None:
        """Test that _extract_assay_fields handles missing tax_id (NA)."""
        mock_chembl_client.fetch_assays_by_ids = MagicMock(  # type: ignore[method-assign]
            return_value={
                "CHEMBL100": {
                    "assay_chembl_id": "CHEMBL100",
                    "assay_organism": "Homo sapiens",
                    "assay_tax_id": None,  # Missing tax_id
                },
            }
        )

        log = UnifiedLogger.get(__name__).bind(component="test")
        result = mock_pipeline._extract_assay_fields(sample_activity_df, mock_chembl_client, log)  # type: ignore[reportPrivateUsage]

        # First row should have organism but NA tax_id
        first_row_organism = result.iloc[0]["assay_organism"]
        assert not pd.isna(first_row_organism), f"Expected non-NA value, got {first_row_organism}"
        assert first_row_organism == "Homo sapiens"

        assert pd.isna(result.iloc[0]["assay_tax_id"])

    def test_extract_assay_fields_handles_empty_dataframe(
        self,
        mock_pipeline: ChemblActivityPipeline,
        mock_chembl_client: ChemblClient,
    ) -> None:
        """Test that _extract_assay_fields handles empty DataFrame."""
        empty_df = pd.DataFrame(columns=["activity_id", "assay_chembl_id"])

        log = UnifiedLogger.get(__name__).bind(component="test")
        result = mock_pipeline._extract_assay_fields(empty_df, mock_chembl_client, log)  # type: ignore[reportPrivateUsage]

        # When DataFrame is empty, method should return empty DataFrame
        assert result.empty

    def test_extract_assay_fields_handles_missing_assay_chembl_id_column(
        self,
        mock_pipeline: ChemblActivityPipeline,
        mock_chembl_client: ChemblClient,
    ) -> None:
        """Test that _extract_assay_fields handles missing assay_chembl_id column."""
        df_without_assay_id = pd.DataFrame(
            {
                "activity_id": [1, 2, 3],
                "molecule_chembl_id": ["CHEMBL1", "CHEMBL2", "CHEMBL3"],
            }
        )

        log = UnifiedLogger.get(__name__).bind(component="test")
        result = mock_pipeline._extract_assay_fields(df_without_assay_id, mock_chembl_client, log)  # type: ignore[reportPrivateUsage]

        # Should add columns with NA values
        assert "assay_organism" in result.columns
        assert "assay_tax_id" in result.columns
        assert all(pd.isna(result["assay_organism"]))
        assert all(pd.isna(result["assay_tax_id"]))

    def test_extract_assay_fields_handles_fetch_error(
        self,
        mock_pipeline: ChemblActivityPipeline,
        mock_chembl_client: ChemblClient,
        sample_activity_df: pd.DataFrame,
    ) -> None:
        """Test that _extract_assay_fields handles fetch errors gracefully."""
        mock_chembl_client.fetch_assays_by_ids = MagicMock(  # type: ignore[method-assign]
            side_effect=Exception("API error")
        )

        log = UnifiedLogger.get(__name__).bind(component="test")
        result = mock_pipeline._extract_assay_fields(sample_activity_df, mock_chembl_client, log)  # type: ignore[reportPrivateUsage]

        # Should not crash and should have NA for all enrichment columns
        assert len(result) == len(sample_activity_df)
        assert "assay_organism" in result.columns
        assert "assay_tax_id" in result.columns
        assert all(pd.isna(result["assay_organism"]))
        assert all(pd.isna(result["assay_tax_id"]))
