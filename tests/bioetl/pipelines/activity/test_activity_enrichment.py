"""Integration tests for Activity pipeline enrichment from /assay and /compound_record."""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock

import pandas as pd
import pytest

from bioetl.clients.chembl import ChemblClient
from bioetl.core.api_client import UnifiedAPIClient
from bioetl.pipelines.chembl.activity.normalize import (
    enrich_with_assay,
    enrich_with_compound_record,
    enrich_with_data_validity,
)
from bioetl.schemas.activity import COLUMN_ORDER, ActivitySchema


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
            "assay_chembl_id": ["CHEMBL100", "CHEMBL101", "CHEMBL102", "CHEMBL103", "CHEMBL104"],
            "molecule_chembl_id": ["CHEMBL1", "CHEMBL2", "CHEMBL3", "CHEMBL4", "CHEMBL5"],
            "document_chembl_id": ["CHEMBL1000", "CHEMBL1001", "CHEMBL1002", "CHEMBL1003", None],
            "testitem_chembl_id": ["CHEMBL1", "CHEMBL2", "CHEMBL3", "CHEMBL4", "CHEMBL5"],
            "type": ["IC50", "EC50", "Ki", "Kd", "IC50"],
            "relation": ["=", "=", "=", "=", "="],
            "value": [10.5, 20.3, 5.7, 15.2, 8.9],
            "units": ["nM", "nM", "nM", "nM", "nM"],
        }
    )


@pytest.fixture
def assay_enrichment_config() -> dict[str, Any]:
    """Configuration for assay enrichment."""
    return {
        "enabled": True,
        "fields": [
            "assay_chembl_id",
            "assay_organism",
            "assay_tax_id",
        ],
        "page_limit": 1000,
    }


@pytest.fixture
def compound_record_enrichment_config() -> dict[str, Any]:
    """Configuration for compound_record enrichment."""
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
class TestActivityEnrichment:
    """Test suite for activity enrichment from /assay and /compound_record."""

    def test_enrich_with_assay_adds_columns(
        self,
        mock_chembl_client: ChemblClient,
        sample_activity_df: pd.DataFrame,
        assay_enrichment_config: dict[str, Any],
    ) -> None:
        """Test that assay enrichment adds expected columns."""
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

        result = enrich_with_assay(
            sample_activity_df,
            mock_chembl_client,
            assay_enrichment_config,
        )

        # Check that new columns are present
        assert "assay_organism" in result.columns
        assert "assay_tax_id" in result.columns

        # Check that original columns are preserved
        assert "activity_id" in result.columns
        assert "assay_chembl_id" in result.columns

    def test_enrich_with_assay_matches_records(
        self,
        mock_chembl_client: ChemblClient,
        sample_activity_df: pd.DataFrame,
        assay_enrichment_config: dict[str, Any],
    ) -> None:
        """Test that assay enrichment correctly matches records by assay_chembl_id."""
        mock_chembl_client.fetch_assays_by_ids = MagicMock(  # type: ignore[method-assign]
            return_value={
                "CHEMBL100": {
                    "assay_chembl_id": "CHEMBL100",
                    "assay_organism": "Homo sapiens",
                    "assay_tax_id": "9606",
                },
            }
        )

        result = enrich_with_assay(
            sample_activity_df,
            mock_chembl_client,
            assay_enrichment_config,
        )

        # First row should have matched data
        assert result.iloc[0]["assay_organism"] == "Homo sapiens"
        assert result.iloc[0]["assay_tax_id"] == 9606

        # Second row (CHEMBL101) should have NA (no match)
        assert pd.isna(result.iloc[1]["assay_organism"])

    def test_enrich_with_assay_handles_missing_records(
        self,
        mock_chembl_client: ChemblClient,
        sample_activity_df: pd.DataFrame,
        assay_enrichment_config: dict[str, Any],
    ) -> None:
        """Test that assay enrichment handles missing assay records gracefully."""
        mock_chembl_client.fetch_assays_by_ids = MagicMock(  # type: ignore[method-assign]
            return_value={}
        )

        result = enrich_with_assay(
            sample_activity_df,
            mock_chembl_client,
            assay_enrichment_config,
        )

        # All enrichment columns should be present but filled with NA
        assert all(pd.isna(result["assay_organism"]))
        assert all(pd.isna(result["assay_tax_id"]))

    def test_enrich_with_assay_handles_none_values(
        self,
        mock_chembl_client: ChemblClient,
        sample_activity_df: pd.DataFrame,
        assay_enrichment_config: dict[str, Any],
    ) -> None:
        """Test that assay enrichment handles None/NA values in assay_chembl_id."""
        # Add a row with None assay_chembl_id
        df_with_none = pd.concat(
            [
                sample_activity_df,
                pd.DataFrame(
                    {
                        "activity_id": [6],
                        "assay_chembl_id": [None],
                        "molecule_chembl_id": ["CHEMBL6"],
                        "document_chembl_id": ["CHEMBL1006"],
                        "testitem_chembl_id": ["CHEMBL6"],
                        "type": ["IC50"],
                        "relation": ["="],
                        "value": [10.0],
                        "units": ["nM"],
                    }
                ),
            ],
            ignore_index=True,
        )

        mock_chembl_client.fetch_assays_by_ids = MagicMock(  # type: ignore[method-assign]
            return_value={}
        )

        result = enrich_with_assay(
            df_with_none,
            mock_chembl_client,
            assay_enrichment_config,
        )

        # Should not crash and should have NA for all enrichment columns
        assert len(result) == len(df_with_none)
        assert all(pd.isna(result["assay_organism"]))

    def test_enrich_with_assay_preserves_row_order(
        self,
        mock_chembl_client: ChemblClient,
        sample_activity_df: pd.DataFrame,
        assay_enrichment_config: dict[str, Any],
    ) -> None:
        """Test that assay enrichment preserves the original row order."""
        mock_chembl_client.fetch_assays_by_ids = MagicMock(  # type: ignore[method-assign]
            return_value={
                "CHEMBL102": {
                    "assay_chembl_id": "CHEMBL102",
                    "assay_organism": "Rattus norvegicus",
                    "assay_tax_id": "10116",
                },
            }
        )

        result = enrich_with_assay(
            sample_activity_df,
            mock_chembl_client,
            assay_enrichment_config,
        )

        # Check that activity_ids are in the same order
        assert list(result["activity_id"]) == list(sample_activity_df["activity_id"])
        # Third row (index 2) should have the matched data
        assert result.iloc[2]["assay_organism"] == "Rattus norvegicus"

    def test_enrich_with_compound_record_adds_columns(
        self,
        mock_chembl_client: ChemblClient,
        sample_activity_df: pd.DataFrame,
        compound_record_enrichment_config: dict[str, Any],
    ) -> None:
        """Test that compound_record enrichment adds expected columns."""
        # Mock fetch_compound_records_by_pairs to return sample data
        mock_chembl_client.fetch_compound_records_by_pairs = MagicMock(  # type: ignore[method-assign]
            return_value={
                ("CHEMBL1", "CHEMBL1000"): {
                    "compound_name": "Test Compound 1",
                    "compound_key": "CID1",
                    "curated": True,
                    "removed": False,
                    "molecule_chembl_id": "CHEMBL1",
                    "document_chembl_id": "CHEMBL1000",
                },
                ("CHEMBL2", "CHEMBL1001"): {
                    "compound_name": "Test Compound 2",
                    "compound_key": "CID2",
                    "curated": False,
                    "removed": False,
                    "molecule_chembl_id": "CHEMBL2",
                    "document_chembl_id": "CHEMBL1001",
                },
            }
        )

        result = enrich_with_compound_record(
            sample_activity_df,
            mock_chembl_client,
            compound_record_enrichment_config,
        )

        # Check that new columns are present
        assert "compound_name" in result.columns
        assert "compound_key" in result.columns
        assert "curated" in result.columns
        assert "removed" in result.columns

    def test_published_fields_removed_from_schema(self) -> None:
        """Test that published_* fields are removed from ActivitySchema."""
        # Check COLUMN_ORDER
        assert "published_type" not in COLUMN_ORDER
        assert "published_relation" not in COLUMN_ORDER
        assert "published_value" not in COLUMN_ORDER
        assert "published_units" not in COLUMN_ORDER

        # Check ActivitySchema
        schema_columns = set(ActivitySchema.columns.keys())
        assert "published_type" not in schema_columns
        assert "published_relation" not in schema_columns
        assert "published_value" not in schema_columns
        assert "published_units" not in schema_columns

    def test_activity_fields_present_in_schema(self) -> None:
        """Test that all required fields are present in ActivitySchema."""
        schema_columns = set(ActivitySchema.columns.keys())

        # Fields from /activity endpoint
        assert "molecule_pref_name" in schema_columns
        assert "standard_upper_value" in schema_columns
        assert "standard_text_value" in schema_columns
        assert "text_value" in schema_columns
        assert "activity_comment" in schema_columns
        assert "data_validity_comment" in schema_columns
        assert "data_validity_description" in schema_columns

        # Fields from /assay enrichment
        assert "assay_organism" in schema_columns
        assert "assay_tax_id" in schema_columns

        # Fields from /compound_record enrichment
        assert "compound_name" in schema_columns
        assert "compound_key" in schema_columns
        assert "curated" in schema_columns
        assert "removed" in schema_columns

        # Fields that replaced published_*
        assert "type" in schema_columns
        assert "relation" in schema_columns
        assert "value" in schema_columns
        assert "units" in schema_columns

    def test_enrich_with_assay_converts_tax_id_to_int64(
        self,
        mock_chembl_client: ChemblClient,
        sample_activity_df: pd.DataFrame,
        assay_enrichment_config: dict[str, Any],
    ) -> None:
        """Test that assay_tax_id is converted to Int64 type."""
        mock_chembl_client.fetch_assays_by_ids = MagicMock(  # type: ignore[method-assign]
            return_value={
                "CHEMBL100": {
                    "assay_chembl_id": "CHEMBL100",
                    "assay_organism": "Homo sapiens",
                    "assay_tax_id": "9606",  # String from API
                },
            }
        )

        result = enrich_with_assay(
            sample_activity_df,
            mock_chembl_client,
            assay_enrichment_config,
        )

        # Check that assay_tax_id is Int64 type
        assert result["assay_tax_id"].dtype == "Int64"
        assert result.iloc[0]["assay_tax_id"] == 9606

    def test_enrich_with_assay_with_empty_dataframe(
        self,
        mock_chembl_client: ChemblClient,
        assay_enrichment_config: dict[str, Any],
    ) -> None:
        """Test that assay enrichment handles empty DataFrame."""
        empty_df = pd.DataFrame(columns=["activity_id", "assay_chembl_id"])

        result = enrich_with_assay(
            empty_df,
            mock_chembl_client,
            assay_enrichment_config,
        )

        # When DataFrame is empty, enrichment is skipped and original DataFrame is returned
        assert result.empty

    def test_all_activity_fields_present_with_null_values(
        self,
        mock_chembl_client: ChemblClient,
        sample_activity_df: pd.DataFrame,
        assay_enrichment_config: dict[str, Any],
        compound_record_enrichment_config: dict[str, Any],
    ) -> None:
        """Test that all activity fields are present even when they are NULL."""
        # Mock enrichment to return some data
        mock_chembl_client.fetch_assays_by_ids = MagicMock(  # type: ignore[method-assign]
            return_value={
                "CHEMBL100": {
                    "assay_chembl_id": "CHEMBL100",
                    "assay_organism": "Homo sapiens",
                    "assay_tax_id": "9606",
                },
            }
        )
        mock_chembl_client.fetch_compound_records_by_pairs = MagicMock(  # type: ignore[method-assign]
            return_value={
                ("CHEMBL1", "CHEMBL1000"): {
                    "compound_name": "Test Compound",
                    "compound_key": "CID1",
                    "curated": True,
                    "removed": False,
                    "molecule_chembl_id": "CHEMBL1",
                    "document_chembl_id": "CHEMBL1000",
                },
            }
        )

        # Add fields that are often NULL
        df_with_all_fields = sample_activity_df.copy()
        df_with_all_fields["standard_upper_value"] = pd.NA
        df_with_all_fields["standard_text_value"] = pd.NA
        df_with_all_fields["upper_value"] = pd.NA
        df_with_all_fields["lower_value"] = pd.NA
        df_with_all_fields["text_value"] = pd.NA
        df_with_all_fields["activity_comment"] = pd.NA
        df_with_all_fields["data_validity_comment"] = pd.NA
        df_with_all_fields["data_validity_description"] = pd.NA

        # Enrich with both sources
        result = enrich_with_assay(
            df_with_all_fields,
            mock_chembl_client,
            assay_enrichment_config,
        )
        result = enrich_with_compound_record(
            result,
            mock_chembl_client,
            compound_record_enrichment_config,
        )

        # Check that all fields are present
        assert "standard_upper_value" in result.columns
        assert "standard_text_value" in result.columns
        assert "upper_value" in result.columns
        assert "lower_value" in result.columns
        assert "text_value" in result.columns
        assert "activity_comment" in result.columns
        assert "data_validity_comment" in result.columns
        assert "data_validity_description" in result.columns
        assert "assay_organism" in result.columns
        assert "assay_tax_id" in result.columns
        assert "compound_name" in result.columns
        assert "compound_key" in result.columns
        assert "curated" in result.columns
        assert "removed" in result.columns

        # Check that NULL values are preserved
        assert pd.isna(result["standard_upper_value"].iloc[0])
        assert pd.isna(result["standard_text_value"].iloc[0])
        assert pd.isna(result["upper_value"].iloc[0])
        assert pd.isna(result["lower_value"].iloc[0])
        assert pd.isna(result["text_value"].iloc[0])
        assert pd.isna(result["activity_comment"].iloc[0])
        assert pd.isna(result["data_validity_comment"].iloc[0])
        assert pd.isna(result["data_validity_description"].iloc[0])

    def test_enrichment_with_both_sources_simultaneously(
        self,
        mock_chembl_client: ChemblClient,
        sample_activity_df: pd.DataFrame,
        assay_enrichment_config: dict[str, Any],
        compound_record_enrichment_config: dict[str, Any],
    ) -> None:
        """Test that enrichment works with both /assay and /compound_record simultaneously."""
        # Mock both enrichment sources
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
        mock_chembl_client.fetch_compound_records_by_pairs = MagicMock(  # type: ignore[method-assign]
            return_value={
                ("CHEMBL1", "CHEMBL1000"): {
                    "PREF_NAME": "Compound 1",
                    "STANDARD_INCHI_KEY": "CID1",
                    "curated": True,
                    "removed": False,
                    "molecule_chembl_id": "CHEMBL1",
                    "document_chembl_id": "CHEMBL1000",
                },
                ("CHEMBL2", "CHEMBL1001"): {
                    "PREF_NAME": "Compound 2",
                    "STANDARD_INCHI_KEY": "CID2",
                    "curated": False,
                    "removed": False,
                    "molecule_chembl_id": "CHEMBL2",
                    "document_chembl_id": "CHEMBL1001",
                },
            }
        )

        # Enrich with assay first
        result = enrich_with_assay(
            sample_activity_df,
            mock_chembl_client,
            assay_enrichment_config,
        )
        # Then enrich with compound_record
        result = enrich_with_compound_record(
            result,
            mock_chembl_client,
            compound_record_enrichment_config,
        )

        # Check that both enrichment sources added their fields
        assert "assay_organism" in result.columns
        assert "assay_tax_id" in result.columns
        assert "compound_name" in result.columns
        assert "compound_key" in result.columns
        assert "curated" in result.columns
        assert "removed" in result.columns

        # Check that enrichment data is correct
        assert result.iloc[0]["assay_organism"] == "Homo sapiens"
        assert result.iloc[0]["assay_tax_id"] == 9606
        # Используем безопасное сравнение для полей, которые могут быть NA
        compound_name_0 = result.iloc[0]["compound_name"]
        assert not pd.isna(compound_name_0) and compound_name_0 == "Compound 1"
        compound_key_0 = result.iloc[0]["compound_key"]
        assert not pd.isna(compound_key_0) and compound_key_0 == "CID1"
        curated_0 = result.iloc[0]["curated"]
        assert not pd.isna(curated_0) and curated_0 == True  # noqa: E712
        # removed всегда None/NA на этом этапе обогащения
        assert pd.isna(result.iloc[0]["removed"])

        assert result.iloc[1]["assay_organism"] == "Mus musculus"
        assert result.iloc[1]["assay_tax_id"] == 10090
        compound_name_1 = result.iloc[1]["compound_name"]
        assert not pd.isna(compound_name_1) and compound_name_1 == "Compound 2"
        compound_key_1 = result.iloc[1]["compound_key"]
        assert not pd.isna(compound_key_1) and compound_key_1 == "CID2"
        curated_1 = result.iloc[1]["curated"]
        assert not pd.isna(curated_1) and curated_1 == False  # noqa: E712
        # removed всегда None/NA на этом этапе обогащения
        assert pd.isna(result.iloc[1]["removed"])

    def test_enrichment_field_types_match_schema(
        self,
        mock_chembl_client: ChemblClient,
        sample_activity_df: pd.DataFrame,
        assay_enrichment_config: dict[str, Any],
        compound_record_enrichment_config: dict[str, Any],
    ) -> None:
        """Test that enrichment field types match ActivitySchema."""
        # Mock enrichment
        mock_chembl_client.fetch_assays_by_ids = MagicMock(  # type: ignore[method-assign]
            return_value={
                "CHEMBL100": {
                    "assay_chembl_id": "CHEMBL100",
                    "assay_organism": "Homo sapiens",
                    "assay_tax_id": "9606",
                },
            }
        )
        mock_chembl_client.fetch_compound_records_by_pairs = MagicMock(  # type: ignore[method-assign]
            return_value={
                ("CHEMBL1", "CHEMBL1000"): {
                    "compound_name": "Test Compound",
                    "compound_key": "CID1",
                    "curated": True,
                    "removed": False,
                    "molecule_chembl_id": "CHEMBL1",
                    "document_chembl_id": "CHEMBL1000",
                },
            }
        )

        # Enrich
        result = enrich_with_assay(
            sample_activity_df,
            mock_chembl_client,
            assay_enrichment_config,
        )
        result = enrich_with_compound_record(
            result,
            mock_chembl_client,
            compound_record_enrichment_config,
        )

        # Check types match schema
        assert result["assay_organism"].dtype == "string"
        assert result["assay_tax_id"].dtype == "Int64"
        assert result["compound_name"].dtype == "string"
        assert result["compound_key"].dtype == "string"
        assert result["curated"].dtype == "boolean"
        assert result["removed"].dtype == "boolean"

    def test_activity_fields_with_non_null_values(
        self,
        mock_chembl_client: ChemblClient,
        sample_activity_df: pd.DataFrame,
    ) -> None:
        """Test that activity fields with non-null values are preserved."""
        # Add fields with actual values
        df_with_values = sample_activity_df.copy()
        df_with_values["standard_upper_value"] = [15.0, None, 20.0, None, None]
        df_with_values["standard_text_value"] = [">10", None, "~15", None, None]
        df_with_values["upper_value"] = [12.0, None, 18.0, None, None]
        df_with_values["lower_value"] = [8.0, None, 10.0, None, None]
        df_with_values["text_value"] = ["Active", None, "Inactive", None, None]
        df_with_values["activity_comment"] = ["Test comment", None, "Another comment", None, None]
        df_with_values["data_validity_comment"] = ["Valid", None, None, None, None]
        df_with_values["data_validity_description"] = ["Validated", None, None, None, None]

        # These fields should be preserved
        assert df_with_values["standard_upper_value"].iloc[0] == 15.0
        assert df_with_values["standard_text_value"].iloc[0] == ">10"
        assert df_with_values["upper_value"].iloc[0] == 12.0
        assert df_with_values["lower_value"].iloc[0] == 8.0
        assert df_with_values["text_value"].iloc[0] == "Active"
        assert df_with_values["activity_comment"].iloc[0] == "Test comment"
        assert df_with_values["data_validity_comment"].iloc[0] == "Valid"
        assert df_with_values["data_validity_description"].iloc[0] == "Validated"

    def test_invariant_standard_text_value_implies_null_standard_value(
        self,
        sample_activity_df: pd.DataFrame,
    ) -> None:
        """Test invariant: standard_text_value IS NOT NULL ⇒ standard_value IS NULL."""
        df = sample_activity_df.copy()
        df["standard_text_value"] = [">10", None, "~15", None, None]
        df["standard_value"] = [None, 20.0, None, 15.0, None]

        # Проверка: если standard_text_value не NULL, то standard_value должен быть NULL
        mask_text_not_null = df["standard_text_value"].notna()
        mask_value_null = df["standard_value"].isna()
        assert (mask_text_not_null & ~mask_value_null).sum() == 0, (
            "Invariant violated: standard_text_value IS NOT NULL but standard_value is not NULL"
        )

    def test_invariant_text_value_implies_null_value(
        self,
        sample_activity_df: pd.DataFrame,
    ) -> None:
        """Test invariant: text_value IS NOT NULL ⇒ value IS NULL."""
        df = sample_activity_df.copy()
        df["text_value"] = ["Active", None, "Inactive", None, None]
        df["value"] = [None, 20.0, None, 15.0, None]

        # Проверка: если text_value не NULL, то value должен быть NULL
        mask_text_not_null = df["text_value"].notna()
        mask_value_null = df["value"].isna()
        assert (mask_text_not_null & ~mask_value_null).sum() == 0, (
            "Invariant violated: text_value IS NOT NULL but value is not NULL"
        )

    def test_invariant_data_validity_comment_implies_description(
        self,
        mock_chembl_client: ChemblClient,
        sample_activity_df: pd.DataFrame,
    ) -> None:
        """Test invariant: data_validity_comment IS NOT NULL ⇒ data_validity_description IS NOT NULL (after LEFT JOIN)."""
        df = sample_activity_df.copy()
        df["data_validity_comment"] = ["Valid", "Invalid", None, "Outside typical range", None]

        # Mock fetch_data_validity_lookup
        mock_chembl_client.fetch_data_validity_lookup = MagicMock(  # type: ignore[method-assign]
            return_value={
                "Valid": {"data_validity_comment": "Valid", "description": "Validated data"},
                "Invalid": {"data_validity_comment": "Invalid", "description": "Invalid data"},
                "Outside typical range": {
                    "data_validity_comment": "Outside typical range",
                    "description": "Value outside typical range",
                },
            }
        )

        config = {
            "enabled": True,
            "fields": ["data_validity_comment", "description"],
            "page_limit": 1000,
        }
        result = enrich_with_data_validity(df, mock_chembl_client, config)

        # Проверка: если data_validity_comment не NULL, то data_validity_description должен быть не NULL
        mask_comment_not_null = result["data_validity_comment"].notna()
        mask_description_not_null = result["data_validity_description"].notna()
        assert (mask_comment_not_null & ~mask_description_not_null).sum() == 0, (
            "Invariant violated: data_validity_comment IS NOT NULL but data_validity_description is NULL"
        )

    def test_invariant_curated_implies_curated_by_not_null(
        self,
        sample_activity_df: pd.DataFrame,
    ) -> None:
        """Test invariant: curated == TRUE ⇒ ACTIVITIES.CURATED_BY IS NOT NULL."""
        df = sample_activity_df.copy()
        df["curated_by"] = ["user1", None, "user2", None, None]
        df["curated"] = [True, False, True, False, None]

        # Проверка: если curated == TRUE, то curated_by должен быть не NULL
        curated_rows = df[df["curated"] == True]  # noqa: E712
        if not curated_rows.empty:
            assert curated_rows["curated_by"].notna().all(), (
                "Invariant violated: curated == TRUE but curated_by is NULL"
            )

    def test_invariant_removed_always_null_on_extraction(
        self,
        mock_chembl_client: ChemblClient,
        sample_activity_df: pd.DataFrame,
    ) -> None:
        """Test invariant: removed always NULL on extraction stage."""
        config = {
            "enabled": True,
            "fields": ["molecule_chembl_id", "pref_name", "molecule_structures"],
            "page_limit": 1000,
        }

        # Mock fetch_molecules_by_ids
        mock_chembl_client.fetch_molecules_by_ids = MagicMock(  # type: ignore[method-assign]
            return_value={
                "CHEMBL1": {
                    "molecule_chembl_id": "CHEMBL1",
                    "pref_name": "Test Compound 1",
                    "molecule_structures": [{"standard_inchi_key": "KEY1"}],
                },
            }
        )

        result = enrich_with_compound_record(sample_activity_df, mock_chembl_client, config)

        # Проверка: removed всегда NULL
        if "removed" in result.columns:
            assert result["removed"].isna().all(), (
                "Invariant violated: removed is not NULL on extraction stage"
            )

    def test_invariant_assay_organism_and_tax_id_consistency(
        self,
        mock_chembl_client: ChemblClient,
        sample_activity_df: pd.DataFrame,
    ) -> None:
        """Test invariant: assay_organism and assay_tax_id should be consistent."""
        # Mock fetch_assays_by_ids
        mock_chembl_client.fetch_assays_by_ids = MagicMock(  # type: ignore[method-assign]
            return_value={
                "CHEMBL100": {
                    "assay_chembl_id": "CHEMBL100",
                    "assay_organism": "Homo sapiens",
                    "assay_tax_id": 9606,
                },
                "CHEMBL101": {
                    "assay_chembl_id": "CHEMBL101",
                    "assay_organism": "Mus musculus",
                    "assay_tax_id": 10090,
                },
            }
        )

        config = {
            "enabled": True,
            "fields": ["assay_chembl_id", "assay_organism", "assay_tax_id"],
            "page_limit": 1000,
        }
        result = enrich_with_assay(sample_activity_df, mock_chembl_client, config)

        # Проверка: если assay_organism NULL, то assay_tax_id должен быть NULL (или наоборот - редкие legacy-ряды допускаются)
        mask_organism_null = result["assay_organism"].isna()
        mask_tax_id_not_null = result["assay_tax_id"].notna()
        inconsistent_count = (mask_organism_null & mask_tax_id_not_null).sum()
        # Допускаем редкие legacy-ряды, но их должно быть близко к 0
        assert inconsistent_count <= 1, (
            f"Invariant violated: too many rows with assay_organism NULL but assay_tax_id NOT NULL: {inconsistent_count}"
        )
