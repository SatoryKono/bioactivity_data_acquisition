"""Integration tests for assay pipeline array serialization."""

from __future__ import annotations

import re
from pathlib import Path

import pandas as pd
import pytest

from bioetl.config import load_config
from bioetl.pipelines.chembl.assay import ChemblAssayPipeline


@pytest.mark.integration
class TestAssayPipelineSmoke:
    """Smoke tests for assay pipeline array serialization."""

    def test_assay_pipeline_serializes_array_fields(self, tmp_path: Path) -> None:
        """Test that assay pipeline serializes array fields to strings."""
        config_path = Path(__file__).parent.parent.parent / "configs" / "pipelines" / "chembl" / "assay.yaml"
        config = load_config(config_path)

        pipeline = ChemblAssayPipeline(config, run_id="test_run")

        # Extract a small sample (limit to 5 records)
        config.cli.limit = 5
        df = pipeline.extract()
        df = pipeline.transform(df)

        # Check that array fields are present
        assert "assay_classifications" in df.columns or "assay_parameters" in df.columns, "Array fields missing"

        # Check that array fields are strings (not lists)
        if "assay_classifications" in df.columns:
            classifications = df["assay_classifications"]
            for value in classifications.dropna():
                assert isinstance(value, str), f"assay_classifications should be string, got {type(value)}"
                # Check pattern: header+rows format (header/row1/row2/...)
                # Empty string is valid, or should match pattern ^[^/]+(/.+)?$
                if value:
                    assert re.match(
                        r"^[^/]+(/.+)?$", value
                    ), f"assay_classifications should match header+rows pattern, got: {value[:100]}"

        if "assay_parameters" in df.columns:
            parameters = df["assay_parameters"]
            for value in parameters.dropna():
                assert isinstance(value, str), f"assay_parameters should be string, got {type(value)}"
                # Check pattern: header+rows format
                if value:
                    assert re.match(
                        r"^[^/]+(/.+)?$", value
                    ), f"assay_parameters should match header+rows pattern, got: {value[:100]}"

    def test_assay_pipeline_has_all_required_fields(self, tmp_path: Path) -> None:
        """Test that assay pipeline extracts all required scalar fields."""
        config_path = Path(__file__).parent.parent.parent / "configs" / "pipelines" / "chembl" / "assay.yaml"
        config = load_config(config_path)

        pipeline = ChemblAssayPipeline(config, run_id="test_run")

        # Extract a small sample
        config.cli.limit = 5
        df = pipeline.extract()
        df = pipeline.transform(df)

        # Check for required scalar fields
        required_fields = [
            "assay_chembl_id",
            "description",
            "assay_type",
            "assay_type_description",
            "assay_test_type",
            "assay_category",
            "assay_organism",
            "assay_tax_id",
            "assay_strain",
            "assay_tissue",
            "assay_cell_type",
            "assay_subcellular_fraction",
            "target_chembl_id",
            "document_chembl_id",
            "src_id",
            "src_assay_id",
            "cell_chembl_id",
            "tissue_chembl_id",
            "assay_group",
            "confidence_score",
            "confidence_description",
            "variant_sequence",
        ]

        for field in required_fields:
            assert field in df.columns, f"Required field {field} missing from output"

    def test_assay_pipeline_array_fields_format(self, tmp_path: Path) -> None:
        """Test that array fields follow header+rows format."""
        config_path = Path(__file__).parent.parent.parent / "configs" / "pipelines" / "chembl" / "assay.yaml"
        config = load_config(config_path)

        pipeline = ChemblAssayPipeline(config, run_id="test_run")

        # Extract a small sample
        config.cli.limit = 10
        df = pipeline.extract()
        df = pipeline.transform(df)

        # Check format of array fields
        if "assay_classifications" in df.columns:
            classifications = df["assay_classifications"].dropna()
            for value in classifications:
                if value:  # Non-empty string
                    # Should have format: header/row1/row2/...
                    parts = value.split("/", 1)
                    assert len(parts) >= 1, f"Should have at least header, got: {value[:100]}"
                    header = parts[0]
                    # Header should have pipe-separated keys
                    assert "|" in header or len(header) == 0, f"Header should have keys, got: {header}"

        if "assay_parameters" in df.columns:
            parameters = df["assay_parameters"].dropna()
            for value in parameters:
                if value:  # Non-empty string
                    # Should have format: header/row1/row2/...
                    parts = value.split("/", 1)
                    assert len(parts) >= 1, f"Should have at least header, got: {value[:100]}"
                    header = parts[0]
                    # Header should have pipe-separated keys
                    assert "|" in header or len(header) == 0, f"Header should have keys, got: {header}"

