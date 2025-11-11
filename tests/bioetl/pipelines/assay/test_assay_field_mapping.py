"""Unit tests for assay field mapping and extraction correctness."""

from __future__ import annotations

import pandas as pd
import pytest

from bioetl.config import PipelineConfig
from bioetl.config.models.base import PipelineMetadata
from bioetl.config.models.http import HTTPClientConfig, HTTPConfig
from bioetl.config.models.transform import TransformConfig
from bioetl.config.models.validation import ValidationConfig
from bioetl.pipelines.chembl.assay import run as assay_run


def _create_minimal_config() -> PipelineConfig:
    """Create minimal pipeline config for testing."""
    return PipelineConfig(
        version=1,
        pipeline=PipelineMetadata(name="assay_chembl", version="1.0.0"),
        transform=TransformConfig(
            arrays_to_header_rows=["assay_classifications", "assay_parameters"]
        ),
        sources={},
        http=HTTPConfig(default=HTTPClientConfig()),
        validation=ValidationConfig(schema_out="bioetl.schemas.assay.assay_chembl.AssaySchema"),
    )


@pytest.mark.unit
class TestAssayFieldMapping:
    """Test suite for assay field mapping correctness."""

    def test_direct_fields_extracted_from_api(self) -> None:
        """Test that direct fields are extracted directly from API response."""
        config = _create_minimal_config()
        pipeline = assay_run.ChemblAssayPipeline(config, "test_run_id")

        # Mock data with direct fields from API
        df = pd.DataFrame(
            {
                "assay_chembl_id": ["CHEMBL100"],
                "assay_category": ["ADMET"],  # Direct from ASSAYS.ASSAY_CATEGORY
                "assay_strain": ["E. coli"],  # Direct from ASSAYS.ASSAY_STRAIN
                "src_assay_id": ["AID123"],  # Direct from ASSAYS.SRC_ASSAY_ID
                "src_id": ["1"],  # Direct from ASSAYS.SRC_ID
                "assay_group": ["Group1"],  # Direct from ASSAYS.ASSAY_GROUP
                "assay_type": ["B"],  # Separate field, not used as surrogate
                "assay_organism": ["Homo sapiens"],  # Separate field, not used as surrogate
            }
        )

        # Use transform method which internally calls _normalize_string_fields
        df = pipeline.transform(df)  # type: ignore[arg-type]

        # Verify fields are preserved as-is
        assert df["assay_category"].iloc[0] == "ADMET"
        assert df["assay_strain"].iloc[0] == "E. coli"
        assert df["src_assay_id"].iloc[0] == "AID123"
        assert df["assay_group"].iloc[0] == "Group1"
        # Verify these are NOT used as surrogates
        assert df["assay_type"].iloc[0] == "B"
        assert df["assay_organism"].iloc[0] == "Homo sapiens"

    def test_no_surrogate_extraction(self) -> None:
        """Test that fields are NOT computed from surrogates."""
        config = _create_minimal_config()
        pipeline = assay_run.ChemblAssayPipeline(config, "test_run_id")

        # Mock data with surrogates but missing direct fields
        df = pd.DataFrame(
            {
                "assay_chembl_id": ["CHEMBL100"],
                "assay_type": ["B"],  # Surrogate - should NOT be used
                "assay_organism": ["Homo sapiens"],  # Surrogate - should NOT be used
                "confidence_score": [8],  # Surrogate - should NOT be used
                # Direct fields missing
            }
        )

        # Use transform method which internally calls _normalize_string_fields
        df = pipeline.transform(df)  # type: ignore[arg-type]

        # Verify direct fields are NULL (not computed from surrogates)
        assert pd.isna(df["assay_category"].iloc[0]) if "assay_category" in df.columns else True
        assert pd.isna(df["assay_strain"].iloc[0]) if "assay_strain" in df.columns else True
        assert pd.isna(df["src_assay_id"].iloc[0]) if "src_assay_id" in df.columns else True
        assert pd.isna(df["assay_group"].iloc[0]) if "assay_group" in df.columns else True

    def test_curation_level_handling(self) -> None:
        """Test that curation_level is handled correctly."""
        config = _create_minimal_config()
        pipeline = assay_run.ChemblAssayPipeline(config, "test_run_id")

        # Test 1: curation_level present in API
        df1 = pd.DataFrame(
            {
                "assay_chembl_id": ["CHEMBL100"],
                "curation_level": ["Expert curated"],
            }
        )
        df1 = pipeline.transform(df1)  # type: ignore[arg-type]
        assert df1["curation_level"].iloc[0] == "Expert curated"

        # Test 2: curation_level missing - should be NULL
        df2 = pd.DataFrame(
            {
                "assay_chembl_id": ["CHEMBL100"],
            }
        )
        df2 = pipeline.transform(df2)  # type: ignore[arg-type]
        assert pd.isna(df2["curation_level"].iloc[0])

    def test_missing_columns_handling(self) -> None:
        """Test handling of missing columns for ChEMBL versioning (v34/v35)."""
        config = _create_minimal_config()
        pipeline = assay_run.ChemblAssayPipeline(config, "test_run_id")

        # DataFrame without optional columns
        df = pd.DataFrame(
            {
                "assay_chembl_id": ["CHEMBL100"],
                "assay_category": ["ADMET"],
            }
        )

        # Use transform which will call _check_missing_columns internally
        # Note: This is tested indirectly through transform, as _check_missing_columns
        # is called during extract stage. For unit test, we verify the behavior
        # through the transform method which ensures columns exist.
        df = pipeline.transform(df)  # type: ignore[arg-type]

        # Verify missing columns are added with NULL (handled in _ensure_schema_columns)
        assert "assay_strain" in df.columns
        assert "assay_group" in df.columns
        assert "curation_level" in df.columns

    def test_no_bao_format_surrogate(self) -> None:
        """Test that assay_class_id is NOT extracted from bao_format surrogate."""
        config = _create_minimal_config()
        pipeline = assay_run.ChemblAssayPipeline(config, "test_run_id")

        # Mock data with bao_format but no assay_class_id
        df = pd.DataFrame(
            {
                "assay_chembl_id": ["CHEMBL100"],
                "bao_format": ["BAO_0000015"],  # Surrogate - should NOT be used
                "assay_class_id": [None],
            }
        )

        # Use transform method which internally calls _normalize_nested_structures
        df = pipeline.transform(df)  # type: ignore[arg-type]

        # Verify assay_class_id remains NULL (not extracted from bao_format)
        assert pd.isna(df["assay_class_id"].iloc[0])

    def test_src_id_and_src_assay_id_preserved(self) -> None:
        """Test that both src_id and src_assay_id are preserved for traceability."""
        config = _create_minimal_config()
        pipeline = assay_run.ChemblAssayPipeline(config, "test_run_id")

        df = pd.DataFrame(
            {
                "assay_chembl_id": ["CHEMBL100"],
                "src_id": ["1"],
                "src_assay_id": ["AID123"],
            }
        )

        # Use transform method which internally calls _normalize_string_fields
        df = pipeline.transform(df)  # type: ignore[arg-type]

        # Verify both fields are preserved
        assert df["src_id"].iloc[0] == "1"
        assert df["src_assay_id"].iloc[0] == "AID123"

    def test_assay_classifications_not_from_surrogates(self) -> None:
        """Test that assay_classifications are NOT extracted from surrogate sources."""
        config = _create_minimal_config()
        pipeline = assay_run.ChemblAssayPipeline(config, "test_run_id")

        # Mock data with surrogates but no classifications
        df = pd.DataFrame(
            {
                "assay_chembl_id": ["CHEMBL100"],
                "bao_format": ["BAO_0000015"],  # Surrogate
                "assay_type": ["B"],  # Surrogate
                "assay_classifications": [None],
            }
        )

        # Use transform method which internally calls _normalize_nested_structures
        df = pipeline.transform(df)  # type: ignore[arg-type]

        # Verify classifications remain NULL (not computed from surrogates)
        assert pd.isna(df["assay_classifications"].iloc[0])
