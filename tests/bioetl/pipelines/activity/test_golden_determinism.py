"""Golden tests for deterministic output verification."""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest  # type: ignore[reportMissingImports]

from bioetl.config import PipelineConfig
from bioetl.pipelines.activity.activity import ChemblActivityPipeline


@pytest.mark.golden  # type: ignore[attr-defined]
@pytest.mark.determinism  # type: ignore[attr-defined]
class TestGoldenDeterminism:
    """Test suite for golden file determinism checks."""

    def test_csv_artifact_determinism(
        self,
        pipeline_config_fixture: PipelineConfig,
        run_id: str,
        sample_activity_data: pd.DataFrame,
        golden_dir: Path,
    ):
        """Test that CSV artifacts are deterministic."""
        pipeline_config_fixture.validation.schema_out = "bioetl.schemas.activity:ActivitySchema"
        pipeline_config_fixture.determinism.sort.by = ["activity_id"]
        pipeline_config_fixture.determinism.sort.ascending = [True]
        pipeline_config_fixture.determinism.hashing.business_key_fields = ("activity_id",)

        pipeline = ChemblActivityPipeline(config=pipeline_config_fixture, run_id=run_id)
        artifacts = pipeline.plan_run_artifacts(
            run_id,
            include_metadata=True,
            include_qc_metrics=True,
        )

        # Run pipeline twice
        result1 = pipeline.write(sample_activity_data, artifacts.run_directory)

        # Create new pipeline instance with same config
        pipeline2 = ChemblActivityPipeline(config=pipeline_config_fixture, run_id=run_id)
        artifacts2 = pipeline2.plan_run_artifacts(
            run_id,
            include_metadata=True,
            include_qc_metrics=True,
        )
        result2 = pipeline2.write(sample_activity_data, artifacts2.run_directory)

        # Compare CSV files byte-by-byte
        csv1_content = result1.write_result.dataset.read_bytes()
        csv2_content = result2.write_result.dataset.read_bytes()

        assert csv1_content == csv2_content, "CSV artifacts should be bit-identical"

    def test_meta_yaml_structure(
        self,
        pipeline_config_fixture: PipelineConfig,
        run_id: str,
        sample_activity_data: pd.DataFrame,
    ):
        """Test that meta.yaml has required structure."""
        import yaml

        pipeline_config_fixture.validation.schema_out = "bioetl.schemas.activity:ActivitySchema"
        pipeline_config_fixture.determinism.sort.by = ["activity_id"]
        pipeline_config_fixture.determinism.sort.ascending = [True]

        pipeline = ChemblActivityPipeline(config=pipeline_config_fixture, run_id=run_id)
        artifacts = pipeline.plan_run_artifacts(
            run_id,
            include_metadata=True,
            include_qc_metrics=True,
        )

        result = pipeline.write(sample_activity_data, artifacts.run_directory, extended=True)

        # Load and verify meta.yaml structure
        assert (
            result.write_result.metadata is not None
        ), "metadata should be created with extended=True"
        meta_content = yaml.safe_load(result.write_result.metadata.read_text())

        # Verify required fields based on actual metadata structure
        assert "pipeline" in meta_content
        assert "row_count" in meta_content
        assert "generated_at_utc" in meta_content
        assert "columns" in meta_content
        assert "sorting" in meta_content
        assert "hashing" in meta_content

    def test_column_order_stability(
        self,
        pipeline_config_fixture: PipelineConfig,
        run_id: str,
        sample_activity_data: pd.DataFrame,
    ):
        """Test that column order is stable."""
        pipeline_config_fixture.validation.schema_out = "bioetl.schemas.activity:ActivitySchema"
        pipeline_config_fixture.determinism.sort.by = ["activity_id"]
        pipeline_config_fixture.determinism.sort.ascending = [True]

        pipeline = ChemblActivityPipeline(config=pipeline_config_fixture, run_id=run_id)
        artifacts = pipeline.plan_run_artifacts(
            run_id,
            include_metadata=True,
            include_qc_metrics=True,
        )

        # Transform data to add row_subtype and row_index columns
        transformed_data = pipeline.transform(sample_activity_data)
        validated_data = pipeline.validate(transformed_data)
        result = pipeline.write(validated_data, artifacts.run_directory)

        # Read CSV and verify column order
        df = pd.read_csv(result.write_result.dataset)  # type: ignore[reportUnknownMemberType]

        # First column should be activity_id (from schema COLUMN_ORDER)
        assert df.columns[0] == "activity_id"

        # Verify column order matches schema
        # Note: hash_row and hash_business_key are added after schema columns
        # Note: enrichment columns (compound_name, curated, removed) are only added if enrichment is enabled
        from bioetl.schemas.activity import COLUMN_ORDER

        # Filter out enrichment columns that may not be present if enrichment is disabled
        enrichment_cols = {"compound_name", "curated", "removed"}
        schema_cols = [col for col in COLUMN_ORDER if col not in enrichment_cols]

        # Get schema columns that are actually present in the output
        df_schema_cols = [col for col in df.columns if col in schema_cols]

        # Check that schema columns (excluding enrichment) are in the correct order
        assert (
            df_schema_cols == schema_cols
        ), "Schema columns (excluding enrichment) must be in the correct order before hash columns"

        # Hash columns should be at the end (if present)
        if "hash_row" in df.columns:
            assert (
                df.columns[-2] == "hash_row" or df.columns[-1] == "hash_row"
            ), "hash_row should be at the end"
        if "hash_business_key" in df.columns:
            assert (
                df.columns[-1] == "hash_business_key"
            ), "hash_business_key should be the last column"
