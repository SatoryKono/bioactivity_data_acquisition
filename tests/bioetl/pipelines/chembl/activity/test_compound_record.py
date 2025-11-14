"""Integration tests for Activity compound_record enrichment."""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock

import pandas as pd
import pytest

from bioetl.clients.client_chembl_common import ChemblClient
from bioetl.core.http.api_client import UnifiedAPIClient
from bioetl.pipelines.chembl.activity import normalize


@pytest.fixture
def mock_chembl_client() -> ChemblClient:
    """Create a mock ChemblClient for testing."""
    mock_api_client = MagicMock(spec=UnifiedAPIClient)
    return ChemblClient(mock_api_client)


@pytest.fixture
def sample_activity_df() -> pd.DataFrame:
    """Sample activity DataFrame for enrichment testing."""
    return pd.DataFrame(
        {
            "activity_id": [1, 2, 3, 4, 5],
            "molecule_chembl_id": ["CHEMBL1", "CHEMBL2", "CHEMBL3", "CHEMBL4", "CHEMBL5"],
            "document_chembl_id": ["CHEMBL100", "CHEMBL100", "CHEMBL101", "CHEMBL101", None],
            "assay_chembl_id": ["CHEMBL200", "CHEMBL201", "CHEMBL202", "CHEMBL203", "CHEMBL204"],
            "testitem_chembl_id": ["CHEMBL1", "CHEMBL2", "CHEMBL3", "CHEMBL4", "CHEMBL5"],
        }
    )


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

        result = normalize.enrich_with_compound_record(
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
        # Keys in mock should match normalized keys (upper, strip) used in enrichment
        # The enrichment normalizes keys before lookup, so mock should use normalized keys
        mock_chembl_client.fetch_compound_records_by_pairs = MagicMock(  # type: ignore[method-assign]
            return_value={
                ("CHEMBL1", "CHEMBL100"): {
                    "compound_name": "Matched Compound",
                    "compound_key": "CID1",
                    "curated": True,
                    "removed": False,
                    "molecule_chembl_id": "CHEMBL1",
                    "document_chembl_id": "CHEMBL100",
                    # Also include fields that might be used for mapping
                    "PREF_NAME": "Matched Compound",
                    "CHEMBL_ID": "CHEMBL1",
                    "STANDARD_INCHI_KEY": "CID1",
                },
            }
        )

        result = normalize.enrich_with_compound_record(
            sample_activity_df,
            mock_chembl_client,
            enrichment_config,
        )

        # First row should have matched data
        # Use proper comparison that handles NA values
        first_row_name = result.iloc[0]["compound_name"]
        assert not pd.isna(first_row_name), f"Expected non-NA value, got {first_row_name}"
        assert first_row_name == "Matched Compound"

        first_row_key = result.iloc[0]["compound_key"]
        assert not pd.isna(first_row_key), f"Expected non-NA value, got {first_row_key}"
        assert first_row_key == "CID1"

        # For boolean columns, check for NA first, then compare
        # Note: pandas boolean types use np.True_/np.False_, so use == instead of 'is'
        first_row_curated = result.iloc[0]["curated"]
        assert not pd.isna(first_row_curated), f"Expected non-NA value, got {first_row_curated}"
        assert first_row_curated == True  # noqa: E712  # Use == for pandas boolean comparison

        first_row_removed = result.iloc[0]["removed"]
        # removed is always NA at this stage, so check for NA
        assert pd.isna(first_row_removed)

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

        result = normalize.enrich_with_compound_record(
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

        result = normalize.enrich_with_compound_record(
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
                    # Also include fields that might be used for mapping
                    "PREF_NAME": "Third",
                    "CHEMBL_ID": "CHEMBL3",
                    "STANDARD_INCHI_KEY": "CID3",
                },
            }
        )

        result = normalize.enrich_with_compound_record(
            sample_activity_df,
            mock_chembl_client,
            enrichment_config,
        )

        # Check that activity_ids are in the same order
        assert list(result["activity_id"]) == list(sample_activity_df["activity_id"])
        # Third row (index 2) should have the matched data
        third_row_name = result.iloc[2]["compound_name"]
        assert not pd.isna(third_row_name), f"Expected non-NA value, got {third_row_name}"
        assert third_row_name == "Third"

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
                    # Also include fields that might be used for mapping
                    "PREF_NAME": "Curated Compound",
                    "CHEMBL_ID": "CHEMBL1",
                    "STANDARD_INCHI_KEY": "CID1",
                },
            }
        )

        result = normalize.enrich_with_compound_record(
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

        result = normalize.enrich_with_compound_record(
            empty_df,
            mock_chembl_client,
            enrichment_config,
        )

        # When DataFrame is empty, enrichment is skipped and original DataFrame is returned
        assert result.empty
        # Enrichment columns are not added when DataFrame is empty (enrichment is skipped)
        # This is expected behavior - no columns are added for empty DataFrame

    def test_enrichment_preserves_row_order_exact(
        self,
        mock_chembl_client: ChemblClient,
        sample_activity_df: pd.DataFrame,
        enrichment_config: dict[str, Any],
    ) -> None:
        """Test that enrichment preserves exact row order (indices should match)."""
        # Create a DataFrame with specific order
        df_with_order = pd.DataFrame(
            {
                "activity_id": [10, 20, 30, 40, 50],
                "molecule_chembl_id": ["CHEMBL1", "CHEMBL2", "CHEMBL3", "CHEMBL4", "CHEMBL5"],
                "document_chembl_id": ["CHEMBL100", "CHEMBL101", "CHEMBL100", "CHEMBL101", None],
            }
        )

        mock_chembl_client.fetch_compound_records_by_pairs = MagicMock(  # type: ignore[method-assign]
            return_value={
                ("CHEMBL2", "CHEMBL101"): {
                    "PREF_NAME": "Second",
                    "STANDARD_INCHI_KEY": "KEY2",
                    "curated": True,
                },
                ("CHEMBL3", "CHEMBL100"): {
                    "PREF_NAME": "Third",
                    "STANDARD_INCHI_KEY": "KEY3",
                    "curated": False,
                },
            }
        )

        result = normalize.enrich_with_compound_record(
            df_with_order,
            mock_chembl_client,
            enrichment_config,
        )

        # Check that activity_ids are in the exact same order
        assert list(result["activity_id"]) == list(df_with_order["activity_id"])
        # Check that indices match
        assert list(result.index) == list(df_with_order.index)
        # Second row (index 1) should have matched data
        assert not pd.isna(result.iloc[1]["compound_name"])
        assert result.iloc[1]["compound_name"] == "Second"
        # Third row (index 2) should have matched data
        assert not pd.isna(result.iloc[2]["compound_name"])
        assert result.iloc[2]["compound_name"] == "Third"

    def test_enrichment_coalescence_fills_na(
        self,
        mock_chembl_client: ChemblClient,
        enrichment_config: dict[str, Any],
    ) -> None:
        """Test that coalescence fills NA values from enrichment."""
        # Create DataFrame with existing NA values in compound_name
        df_with_na = pd.DataFrame(
            {
                "activity_id": [1, 2],
                "molecule_chembl_id": ["CHEMBL1", "CHEMBL2"],
                "document_chembl_id": ["CHEMBL100", "CHEMBL100"],
                "compound_name": [pd.NA, "Existing Name"],  # First row has NA, second has value
                "compound_key": [pd.NA, pd.NA],
                "curated": [pd.NA, pd.NA],
            }
        )

        mock_chembl_client.fetch_compound_records_by_pairs = MagicMock(  # type: ignore[method-assign]
            return_value={
                ("CHEMBL1", "CHEMBL100"): {
                    "PREF_NAME": "Enriched Name",
                    "STANDARD_INCHI_KEY": "Enriched Key",
                    "curated": True,
                },
                ("CHEMBL2", "CHEMBL100"): {
                    "PREF_NAME": "Should Not Override",
                    "STANDARD_INCHI_KEY": "Should Not Override",
                    "curated": False,
                },
            }
        )

        result = normalize.enrich_with_compound_record(
            df_with_na,
            mock_chembl_client,
            enrichment_config,
        )

        # First row: NA should be filled from enrichment
        assert not pd.isna(result.iloc[0]["compound_name"])
        assert result.iloc[0]["compound_name"] == "Enriched Name"
        assert not pd.isna(result.iloc[0]["compound_key"])
        assert result.iloc[0]["compound_key"] == "Enriched Key"
        assert not pd.isna(result.iloc[0]["curated"])
        assert result.iloc[0]["curated"] == True  # noqa: E712

        # Second row: existing value should be preserved (not overridden)
        assert not pd.isna(result.iloc[1]["compound_name"])
        assert result.iloc[1]["compound_name"] == "Existing Name"

    def test_enrichment_key_normalization(
        self,
        mock_chembl_client: ChemblClient,
        enrichment_config: dict[str, Any],
    ) -> None:
        """Test that key normalization works: pairs form identically for chembl25 and CHEMBL25."""
        # Create DataFrame with mixed case and whitespace
        df_mixed_case = pd.DataFrame(
            {
                "activity_id": [1, 2],
                "molecule_chembl_id": ["chembl25", "  CHEMBL26  "],  # Lowercase and with spaces
                "document_chembl_id": ["chembl100", "  CHEMBL101  "],  # Lowercase and with spaces
            }
        )

        # Mock should use normalized keys (uppercase, stripped)
        mock_chembl_client.fetch_compound_records_by_pairs = MagicMock(  # type: ignore[method-assign]
            return_value={
                ("CHEMBL25", "CHEMBL100"): {  # Normalized keys
                    "PREF_NAME": "Normalized Match",
                    "STANDARD_INCHI_KEY": "KEY25",
                    "curated": True,
                },
                ("CHEMBL26", "CHEMBL101"): {  # Normalized keys
                    "PREF_NAME": "Normalized Match 2",
                    "STANDARD_INCHI_KEY": "KEY26",
                    "curated": False,
                },
            }
        )

        result = normalize.enrich_with_compound_record(
            df_mixed_case,
            mock_chembl_client,
            enrichment_config,
        )

        # Both rows should match despite different case/spacing in input
        assert not pd.isna(result.iloc[0]["compound_name"])
        assert result.iloc[0]["compound_name"] == "Normalized Match"
        assert not pd.isna(result.iloc[1]["compound_name"])
        assert result.iloc[1]["compound_name"] == "Normalized Match 2"

    def test_enrichment_removed_always_na(
        self,
        mock_chembl_client: ChemblClient,
        sample_activity_df: pd.DataFrame,
        enrichment_config: dict[str, Any],
    ) -> None:
        """Test that removed is always pd.NA (dtype=boolean)."""
        mock_chembl_client.fetch_compound_records_by_pairs = MagicMock(  # type: ignore[method-assign]
            return_value={
                ("CHEMBL1", "CHEMBL100"): {
                    "PREF_NAME": "Test Compound",
                    "STANDARD_INCHI_KEY": "KEY1",
                    "curated": True,
                    "removed": False,  # Even if client returns False, should be NA
                },
            }
        )

        result = normalize.enrich_with_compound_record(
            sample_activity_df,
            mock_chembl_client,
            enrichment_config,
        )

        # removed should always be pd.NA regardless of what client returns
        assert all(pd.isna(result["removed"]))
        # dtype should be boolean
        assert result["removed"].dtype == "boolean"

    def test_enrichment_client_error_handling(
        self,
        mock_chembl_client: ChemblClient,
        sample_activity_df: pd.DataFrame,
        enrichment_config: dict[str, Any],
    ) -> None:
        """Test that client errors are handled: function returns frame with empty columns of correct types."""
        # Mock client to raise an exception
        mock_chembl_client.fetch_compound_records_by_pairs = MagicMock(  # type: ignore[method-assign]
            side_effect=Exception("API error")
        )

        result = normalize.enrich_with_compound_record(
            sample_activity_df,
            mock_chembl_client,
            enrichment_config,
        )

        # Should not crash and should have all enrichment columns with correct types
        assert len(result) == len(sample_activity_df)
        assert "compound_name" in result.columns
        assert "compound_key" in result.columns
        assert "curated" in result.columns
        assert "removed" in result.columns

        # All columns should be NA
        assert all(pd.isna(result["compound_name"]))
        assert all(pd.isna(result["compound_key"]))
        assert all(pd.isna(result["curated"]))
        assert all(pd.isna(result["removed"]))

        # Check types
        assert result["compound_name"].dtype == "string"
        assert result["compound_key"].dtype == "string"
        assert result["curated"].dtype == "boolean"
        assert result["removed"].dtype == "boolean"

    def test_enrichment_records_matched_count(
        self,
        mock_chembl_client: ChemblClient,
        sample_activity_df: pd.DataFrame,
        enrichment_config: dict[str, Any],
    ) -> None:
        """Test that records_matched count reflects only actually found matches."""
        # Mock to return some records and some None values
        mock_chembl_client.fetch_compound_records_by_pairs = MagicMock(  # type: ignore[method-assign]
            return_value={
                ("CHEMBL1", "CHEMBL100"): {
                    "PREF_NAME": "Found 1",
                    "STANDARD_INCHI_KEY": "KEY1",
                    "curated": True,
                },
                ("CHEMBL2", "CHEMBL100"): None,  # Explicitly None
                ("CHEMBL3", "CHEMBL101"): {
                    "PREF_NAME": "Found 3",
                    "STANDARD_INCHI_KEY": "KEY3",
                    "curated": False,
                },
            }
        )

        result = normalize.enrich_with_compound_record(
            sample_activity_df,
            mock_chembl_client,
            enrichment_config,
        )

        # Should have matched 2 records (CHEMBL1 and CHEMBL3), not 3
        # First row should have data
        assert not pd.isna(result.iloc[0]["compound_name"])
        assert result.iloc[0]["compound_name"] == "Found 1"
        # Second row should have NA (None in mock)
        assert pd.isna(result.iloc[1]["compound_name"])
        # Third row should have data
        assert not pd.isna(result.iloc[2]["compound_name"])
        assert result.iloc[2]["compound_name"] == "Found 3"
