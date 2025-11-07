"""QC tests for determinism verification."""

from __future__ import annotations

import hashlib

import pandas as pd
import pytest

from bioetl.config import PipelineConfig
from bioetl.pipelines.activity.activity import ChemblActivityPipeline


@pytest.mark.qc
@pytest.mark.determinism
class TestDeterminismQC:
    """Test suite for determinism QC checks."""

    def test_hash_row_consistency(
        self,
        pipeline_config_fixture: PipelineConfig,
        run_id: str,
        sample_activity_data: pd.DataFrame,
    ):
        """Test that hash_row is consistent across runs."""
        pipeline_config_fixture.validation.schema_out = "bioetl.schemas.activity_chembl:ActivitySchema"
        pipeline_config_fixture.determinism.sort.by = ["activity_id"]
        pipeline_config_fixture.determinism.hashing.row_fields = ["activity_id", "molecule_chembl_id"]
        pipeline_config_fixture.determinism.hashing.business_key_fields = ("activity_id",)

        pipeline = ChemblActivityPipeline(config=pipeline_config_fixture, run_id=run_id)
        artifacts = pipeline.plan_run_artifacts(run_id)

        result1 = pipeline.write(sample_activity_data, artifacts.run_directory)

        # Read CSV and check hash_row column
        df1: pd.DataFrame = pd.read_csv(result1.write_result.dataset)  # type: ignore[assignment]

        if "hash_row" in df1.columns:
            # Hash should be consistent for same data
            hash1 = df1["hash_row"].iloc[0]

            # Run again
            pipeline2 = ChemblActivityPipeline(config=pipeline_config_fixture, run_id=run_id)
            artifacts2 = pipeline2.plan_run_artifacts(run_id)
            result2 = pipeline2.write(sample_activity_data, artifacts2.run_directory)

            df2: pd.DataFrame = pd.read_csv(result2.write_result.dataset)  # type: ignore[assignment]
            hash2 = df2["hash_row"].iloc[0]

            assert hash1 == hash2, "hash_row should be consistent across runs"

    def test_hash_business_key_consistency(
        self,
        pipeline_config_fixture: PipelineConfig,
        run_id: str,
        sample_activity_data: pd.DataFrame,
    ):
        """Test that hash_business_key is consistent."""
        pipeline_config_fixture.validation.schema_out = "bioetl.schemas.activity_chembl:ActivitySchema"
        pipeline_config_fixture.determinism.sort.by = ["activity_id"]
        pipeline_config_fixture.determinism.hashing.business_key_fields = ("activity_id",)

        pipeline = ChemblActivityPipeline(config=pipeline_config_fixture, run_id=run_id)
        artifacts = pipeline.plan_run_artifacts(run_id)

        result1 = pipeline.write(sample_activity_data, artifacts.run_directory)
        df1: pd.DataFrame = pd.read_csv(result1.write_result.dataset)  # type: ignore[assignment]

        if "hash_business_key" in df1.columns:
            hash1 = df1["hash_business_key"].iloc[0]

            # Run again
            pipeline2 = ChemblActivityPipeline(config=pipeline_config_fixture, run_id=run_id)
            artifacts2 = pipeline2.plan_run_artifacts(run_id)
            result2 = pipeline2.write(sample_activity_data, artifacts2.run_directory)

            df2: pd.DataFrame = pd.read_csv(result2.write_result.dataset)  # type: ignore[assignment]
            hash2 = df2["hash_business_key"].iloc[0]

            assert hash1 == hash2, "hash_business_key should be consistent"

    def test_sort_order_stability(
        self,
        pipeline_config_fixture: PipelineConfig,
        run_id: str,
        sample_activity_data: pd.DataFrame,
    ):
        """Test that sort order is stable."""
        pipeline_config_fixture.validation.schema_out = "bioetl.schemas.activity_chembl:ActivitySchema"
        pipeline_config_fixture.determinism.sort.by = ["activity_id"]
        pipeline_config_fixture.determinism.sort.ascending = [True]

        pipeline = ChemblActivityPipeline(config=pipeline_config_fixture, run_id=run_id)
        artifacts = pipeline.plan_run_artifacts(run_id)

        result1 = pipeline.write(sample_activity_data, artifacts.run_directory)
        df1: pd.DataFrame = pd.read_csv(result1.write_result.dataset)  # type: ignore[assignment]

        # Run again
        pipeline2 = ChemblActivityPipeline(config=pipeline_config_fixture, run_id=run_id)
        artifacts2 = pipeline2.plan_run_artifacts(run_id)
        result2 = pipeline2.write(sample_activity_data, artifacts2.run_directory)

        df2: pd.DataFrame = pd.read_csv(result2.write_result.dataset)  # type: ignore[assignment]

        # Row order should be identical
        assert df1["activity_id"].tolist() == df2["activity_id"].tolist()

    def test_csv_checksum_consistency(
        self,
        pipeline_config_fixture: PipelineConfig,
        run_id: str,
        sample_activity_data: pd.DataFrame,
    ):
        """Test that CSV file checksums are consistent."""
        pipeline_config_fixture.validation.schema_out = "bioetl.schemas.activity_chembl:ActivitySchema"
        pipeline_config_fixture.determinism.sort.by = ["activity_id"]

        pipeline = ChemblActivityPipeline(config=pipeline_config_fixture, run_id=run_id)
        artifacts = pipeline.plan_run_artifacts(run_id)

        result1 = pipeline.write(sample_activity_data, artifacts.run_directory)
        content1 = result1.write_result.dataset.read_bytes()
        checksum1 = hashlib.sha256(content1).hexdigest()

        # Run again
        pipeline2 = ChemblActivityPipeline(config=pipeline_config_fixture, run_id=run_id)
        artifacts2 = pipeline2.plan_run_artifacts(run_id)
        result2 = pipeline2.write(sample_activity_data, artifacts2.run_directory)

        content2 = result2.write_result.dataset.read_bytes()
        checksum2 = hashlib.sha256(content2).hexdigest()

        assert checksum1 == checksum2, "CSV file checksums should be identical"

