"""Integration tests for Activity compound_record enrichment."""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock

import pandas as pd
import pytest

from bioetl.clients.chembl import ChemblClient
from bioetl.core.api_client import UnifiedAPIClient
from bioetl.pipelines.activity.activity_enrichment import enrich_with_compound_record


@pytest.fixture
def mock_chembl_client() -> ChemblClient:
    """Create a mock ChemblClient for testing."""
    mock_api_client = MagicMock(spec=UnifiedAPIClient)
    return ChemblClient(mock_api_client)


@pytest.fixture
def sample_activity_df() -> pd.DataFrame:
    """Sample activity DataFrame for enrichment testing."""
    return pd.DataFrame({
        "activity_id": [1, 2, 3, 4, 5],
        "molecule_chembl_id": ["CHEMBL1", "CHEMBL2", "CHEMBL3", "CHEMBL4", "CHEMBL5"],
        "document_chembl_id": ["CHEMBL100", "CHEMBL100", "CHEMBL101", "CHEMBL101", None],
        "assay_chembl_id": ["CHEMBL200", "CHEMBL201", "CHEMBL202", "CHEMBL203", "CHEMBL204"],
        "testitem_chembl_id": ["CHEMBL1", "CHEMBL2", "CHEMBL3", "CHEMBL4", "CHEMBL5"],
    })


@pytest.fixture
def enrichment_config() -> dict[str, Any]:
    """Configuration for enrichment."""
    return {
        "enabled": True,
        "fields": [
            "compound_name",
            "compound_key",
            "curated",
            "removed",
            "molecule_chembl_id",
            "document_chembl_id",
        ],
        "page_limit": 1000,
    }


@pytest.mark.integration
class TestActivityCompoundRecordEnrichment:
    """Test suite for compound_record enrichment."""

    def test_enrichment_adds_columns(
        self,
        mock_chembl_client: ChemblClient,
        sample_activity_df: pd.DataFrame,
        enrichment_config: dict[str, Any],
    ) -> None:
        """Test that enrichment adds expected columns."""
        # Mock fetch_compound_records_by_pairs to return sample data
        mock_chembl_client.fetch_compound_records_by_pairs = MagicMock(  # type: ignore[method-assign]
            return_value={
                ("CHEMBL1", "CHEMBL100"): {
                    "compound_name": "Test Compound 1",
                    "compound_key": "CID1",
                    "curated": True,
                    "removed": False,
                    "molecule_chembl_id": "CHEMBL1",
                    "document_chembl_id": "CHEMBL100",
                },
                ("CHEMBL2", "CHEMBL100"): {
                    "compound_name": "Test Compound 2",
                    "compound_key": "CID2",
                    "curated": False,
                    "removed": False,
                    "molecule_chembl_id": "CHEMBL2",
                    "document_chembl_id": "CHEMBL100",
                },
            }
        )

        result = enrich_with_compound_record(
            sample_activity_df,
            mock_chembl_client,
            enrichment_config,
        )

        # Check that new columns are present
        assert "compound_name" in result.columns
        assert "compound_key" in result.columns
        assert "curated" in result.columns
        assert "removed" in result.columns

        # Check that original columns are preserved
        assert "activity_id" in result.columns
        assert "molecule_chembl_id" in result.columns
        assert "document_chembl_id" in result.columns

    def test_enrichment_matches_records(
        self,
        mock_chembl_client: ChemblClient,
        sample_activity_df: pd.DataFrame,
        enrichment_config: dict[str, Any],
    ) -> None:
        """Test that enrichment correctly matches records by (molecule, document) pair."""
        mock_chembl_client.fetch_compound_records_by_pairs = MagicMock(  # type: ignore[method-assign]
            return_value={
                ("CHEMBL1", "CHEMBL100"): {
                    "compound_name": "Matched Compound",
                    "compound_key": "CID1",
                    "curated": True,
                    "removed": False,
                    "molecule_chembl_id": "CHEMBL1",
                    "document_chembl_id": "CHEMBL100",
                },
            }
        )

        result = enrich_with_compound_record(
            sample_activity_df,
            mock_chembl_client,
            enrichment_config,
        )

        # First row should have matched data
        assert result.iloc[0]["compound_name"] == "Matched Compound"
        assert result.iloc[0]["compound_key"] == "CID1"
        assert result.iloc[0]["curated"] == True  # noqa: E712
        assert result.iloc[0]["removed"] == False  # noqa: E712

        # Second row (CHEMBL2, CHEMBL100) should have NA (no match)
        assert pd.isna(result.iloc[1]["compound_name"])

    def test_enrichment_handles_missing_records(
        self,
        mock_chembl_client: ChemblClient,
        sample_activity_df: pd.DataFrame,
        enrichment_config: dict[str, Any],
    ) -> None:
        """Test that enrichment handles missing compound_record gracefully."""
        mock_chembl_client.fetch_compound_records_by_pairs = MagicMock(  # type: ignore[method-assign]
            return_value={}
        )

        result = enrich_with_compound_record(
            sample_activity_df,
            mock_chembl_client,
            enrichment_config,
        )

        # All enrichment columns should be present but filled with NA
        assert all(pd.isna(result["compound_name"]))
        assert all(pd.isna(result["compound_key"]))
        assert all(pd.isna(result["curated"]))
        assert all(pd.isna(result["removed"]))

    def test_enrichment_handles_none_values(
        self,
        mock_chembl_client: ChemblClient,
        sample_activity_df: pd.DataFrame,
        enrichment_config: dict[str, Any],
    ) -> None:
        """Test that enrichment handles None/NA values in molecule/document IDs."""
        # Last row has None document_chembl_id
        mock_chembl_client.fetch_compound_records_by_pairs = MagicMock(  # type: ignore[method-assign]
            return_value={}
        )

        result = enrich_with_compound_record(
            sample_activity_df,
            mock_chembl_client,
            enrichment_config,
        )

        # Should not crash and should have NA for all enrichment columns
        assert len(result) == len(sample_activity_df)
        assert all(pd.isna(result["compound_name"]))

    def test_enrichment_preserves_row_order(
        self,
        mock_chembl_client: ChemblClient,
        sample_activity_df: pd.DataFrame,
        enrichment_config: dict[str, Any],
    ) -> None:
        """Test that enrichment preserves the original row order."""
        mock_chembl_client.fetch_compound_records_by_pairs = MagicMock(  # type: ignore[method-assign]
            return_value={
                ("CHEMBL3", "CHEMBL101"): {
                    "compound_name": "Third",
                    "compound_key": "CID3",
                    "curated": True,
                    "removed": False,
                    "molecule_chembl_id": "CHEMBL3",
                    "document_chembl_id": "CHEMBL101",
                },
            }
        )

        result = enrich_with_compound_record(
            sample_activity_df,
            mock_chembl_client,
            enrichment_config,
        )

        # Check that activity_ids are in the same order
        assert list(result["activity_id"]) == list(sample_activity_df["activity_id"])
        # Third row (index 2) should have the matched data
        assert result.iloc[2]["compound_name"] == "Third"

    def test_enrichment_handles_curated_priority(
        self,
        mock_chembl_client: ChemblClient,
        sample_activity_df: pd.DataFrame,
        enrichment_config: dict[str, Any],
    ) -> None:
        """Test that enrichment correctly handles curated=True priority."""
        # Note: Priority handling is done in fetch_compound_records_by_pairs,
        # but we test that the result is correctly applied
        mock_chembl_client.fetch_compound_records_by_pairs = MagicMock(  # type: ignore[method-assign]
            return_value={
                ("CHEMBL1", "CHEMBL100"): {
                    "compound_name": "Curated Compound",
                    "compound_key": "CID1",
                    "curated": True,  # Curated=True should be preferred
                    "removed": False,
                    "molecule_chembl_id": "CHEMBL1",
                    "document_chembl_id": "CHEMBL100",
                },
            }
        )

        result = enrich_with_compound_record(
            sample_activity_df,
            mock_chembl_client,
            enrichment_config,
        )

        assert result.iloc[0]["curated"] == True  # noqa: E712

    def test_enrichment_with_empty_dataframe(
        self,
        mock_chembl_client: ChemblClient,
        enrichment_config: dict[str, Any],
    ) -> None:
        """Test that enrichment handles empty DataFrame."""
        empty_df = pd.DataFrame(columns=["activity_id", "molecule_chembl_id", "document_chembl_id"])

        result = enrich_with_compound_record(
            empty_df,
            mock_chembl_client,
            enrichment_config,
        )

        # When DataFrame is empty, enrichment is skipped and original DataFrame is returned
        assert result.empty
        # Enrichment columns are not added when DataFrame is empty (enrichment is skipped)
        # This is expected behavior - no columns are added for empty DataFrame

