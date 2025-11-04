"""Golden tests for deterministic output verification."""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest  # type: ignore[reportMissingImports]

from bioetl.config import PipelineConfig
from bioetl.pipelines.chembl.activity import ChemblActivityPipeline


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
        result1 = pipeline.write(sample_activity_data, artifacts)

        # Create new pipeline instance with same config
        pipeline2 = ChemblActivityPipeline(config=pipeline_config_fixture, run_id=run_id)
        artifacts2 = pipeline2.plan_run_artifacts(
            run_id,
            include_metadata=True,
            include_qc_metrics=True,
        )
        result2 = pipeline2.write(sample_activity_data, artifacts2)

        # Compare CSV files byte-by-byte
        csv1_content = result1.dataset.read_bytes()
        csv2_content = result2.dataset.read_bytes()

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

        result = pipeline.write(sample_activity_data, artifacts)

        # Load and verify meta.yaml structure
        meta_content = yaml.safe_load(result.metadata.read_text())

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

        result = pipeline.write(sample_activity_data, artifacts)

        # Read CSV and verify column order
        df = pd.read_csv(result.dataset)  # type: ignore[reportUnknownMemberType]

        # First column should be activity_id (from schema COLUMN_ORDER)
        assert df.columns[0] == "activity_id"

        # Verify column order matches schema
        from bioetl.schemas.activity import COLUMN_ORDER

        for idx, expected_col in enumerate(COLUMN_ORDER):
            if idx < len(df.columns):
                assert df.columns[idx] == expected_col, f"Column at index {idx} should be {expected_col}"

