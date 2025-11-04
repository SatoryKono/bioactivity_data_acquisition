"""Unit tests for PipelineBase orchestration."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

import pandas as pd
import pytest

from bioetl.pipelines.base import PipelineBase, RunArtifacts


class TestPipeline(PipelineBase):
    """Test pipeline implementation for testing PipelineBase."""

    def extract(self, *args: object, **kwargs: object) -> pd.DataFrame:
        """Return sample DataFrame."""
        return pd.DataFrame({"id": [1, 2, 3], "value": [10, 20, 30]})

    def transform(self, payload: object) -> pd.DataFrame:
        """Transform payload."""
        if isinstance(payload, pd.DataFrame):
            df = payload.copy()
            df["value_doubled"] = df["value"] * 2
            return df
        return pd.DataFrame()


@pytest.mark.unit
class TestPipelineBase:
    """Test suite for PipelineBase."""

    def test_init(self, pipeline_config_fixture, run_id: str):
        """Test PipelineBase initialization."""
        pipeline = TestPipeline(config=pipeline_config_fixture, run_id=run_id)

        assert pipeline.config == pipeline_config_fixture
        assert pipeline.run_id == run_id
        assert pipeline.pipeline_code == "activity"
        assert pipeline.pipeline_directory.exists()
        assert pipeline.logs_directory.exists()

    def test_ensure_pipeline_directory(self, pipeline_config_fixture, run_id: str):
        """Test pipeline directory creation."""
        pipeline = TestPipeline(config=pipeline_config_fixture, run_id=run_id)

        assert pipeline.pipeline_directory.exists()
        assert pipeline.pipeline_directory.name == "_activity"
        assert pipeline.pipeline_directory.is_dir()

    def test_ensure_logs_directory(self, pipeline_config_fixture, run_id: str):
        """Test logs directory creation."""
        pipeline = TestPipeline(config=pipeline_config_fixture, run_id=run_id)

        assert pipeline.logs_directory.exists()
        assert pipeline.logs_directory.name == "activity"
        assert pipeline.logs_directory.is_dir()

    def test_derive_trace_and_span(self, pipeline_config_fixture):
        """Test trace and span ID derivation."""
        pipeline = TestPipeline(config=pipeline_config_fixture, run_id="test-123")

        trace_id, span_id = pipeline._derive_trace_and_span()

        assert len(trace_id) == 32
        assert len(span_id) == 16
        assert trace_id.isalnum() or "0" in trace_id
        assert span_id.isalnum() or "0" in span_id

    def test_build_run_stem(self, pipeline_config_fixture, run_id: str):
        """Test run stem generation."""
        pipeline = TestPipeline(config=pipeline_config_fixture, run_id=run_id)

        stem = pipeline.build_run_stem("20240101")
        assert stem == "activity_20240101"

        stem_with_mode = pipeline.build_run_stem("20240101", mode="full")
        assert stem_with_mode == "activity_full_20240101"

    def test_plan_run_artifacts(self, pipeline_config_fixture, run_id: str):
        """Test artifact planning."""
        pipeline = TestPipeline(config=pipeline_config_fixture, run_id=run_id)

        artifacts = pipeline.plan_run_artifacts("20240101")

        assert isinstance(artifacts, RunArtifacts)
        assert artifacts.write.dataset.exists() is False  # File not created yet
        assert artifacts.write.dataset.name == "activity_20240101.csv"
        assert artifacts.write.metadata.name == "activity_20240101_meta.yaml"
        assert artifacts.write.quality_report is not None
        assert artifacts.write.correlation_report is None  # Default is False
        assert artifacts.write.qc_metrics is not None

    def test_plan_run_artifacts_with_correlation(self, pipeline_config_fixture, run_id: str):
        """Test artifact planning with correlation report."""
        pipeline = TestPipeline(config=pipeline_config_fixture, run_id=run_id)

        artifacts = pipeline.plan_run_artifacts("20240101", include_correlation=True)

        assert artifacts.write.correlation_report is not None
        assert artifacts.write.correlation_report.name == "activity_20240101_correlation_report.csv"

    def test_list_run_stems(self, pipeline_config_fixture, run_id: str, tmp_output_dir: Path):
        """Test listing run stems."""
        pipeline = TestPipeline(config=pipeline_config_fixture, run_id=run_id)

        # Create mock meta files
        pipeline_dir = pipeline.pipeline_directory
        meta_file1 = pipeline_dir / "activity_20240101_meta.yaml"
        meta_file2 = pipeline_dir / "activity_20240102_meta.yaml"
        meta_file1.touch()
        meta_file2.touch()

        stems = pipeline.list_run_stems()

        assert len(stems) >= 2
        assert "activity_20240101" in stems
        assert "activity_20240102" in stems

    def test_apply_retention_policy(self, pipeline_config_fixture, run_id: str):
        """Test retention policy application."""
        pipeline = TestPipeline(config=pipeline_config_fixture, run_id=run_id)
        pipeline.retention_runs = 2

        # Create more runs than retention limit
        pipeline_dir = pipeline.pipeline_directory
        for i in range(5):
            meta_file = pipeline_dir / f"activity_run{i}_meta.yaml"
            dataset_file = pipeline_dir / f"activity_run{i}.csv"
            meta_file.touch()
            dataset_file.touch()

        pipeline.apply_retention_policy()

        # Check that only 2 most recent runs remain
        remaining_meta = list(pipeline_dir.glob("activity_*_meta.yaml"))
        assert len(remaining_meta) <= 2

    def test_apply_retention_policy_disabled(self, pipeline_config_fixture, run_id: str):
        """Test retention policy when disabled."""
        pipeline = TestPipeline(config=pipeline_config_fixture, run_id=run_id)
        pipeline.retention_runs = 0

        # Create some runs
        pipeline_dir = pipeline.pipeline_directory
        for i in range(3):
            meta_file = pipeline_dir / f"activity_run{i}_meta.yaml"
            meta_file.touch()

        pipeline.apply_retention_policy()

        # All runs should remain
        remaining_meta = list(pipeline_dir.glob("activity_*_meta.yaml"))
        assert len(remaining_meta) == 3

    def test_register_client(self, pipeline_config_fixture, run_id: str):
        """Test client registration."""
        pipeline = TestPipeline(config=pipeline_config_fixture, run_id=run_id)

        mock_client = MagicMock()
        mock_client.close = MagicMock()

        pipeline.register_client("test_client", mock_client)

        # Should be able to register
        assert "test_client" in pipeline._registered_clients

    def test_register_client_callable(self, pipeline_config_fixture, run_id: str):
        """Test registering a callable client."""
        pipeline = TestPipeline(config=pipeline_config_fixture, run_id=run_id)

        def close_client() -> None:
            pass

        pipeline.register_client("callable_client", close_client)

        assert "callable_client" in pipeline._registered_clients

    def test_register_client_duplicate(self, pipeline_config_fixture, run_id: str):
        """Test that registering duplicate client raises error."""
        pipeline = TestPipeline(config=pipeline_config_fixture, run_id=run_id)

        mock_client = MagicMock()
        mock_client.close = MagicMock()

        pipeline.register_client("test_client", mock_client)

        with pytest.raises(ValueError, match="already registered"):
            pipeline.register_client("test_client", mock_client)

    def test_register_client_invalid(self, pipeline_config_fixture, run_id: str):
        """Test that registering invalid client raises error."""
        pipeline = TestPipeline(config=pipeline_config_fixture, run_id=run_id)

        invalid_client = object()  # No close method

        with pytest.raises(TypeError, match="does not expose callable"):
            pipeline.register_client("invalid_client", invalid_client)

    def test_validate_with_schema(self, pipeline_config_fixture, run_id: str, sample_activity_data: pd.DataFrame):
        """Test validation with Pandera schema."""
        # Update config to use activity schema
        pipeline_config_fixture.validation.schema_out = "bioetl.schemas.activity:ActivitySchema"
        pipeline_config_fixture.determinism.sort.by = ["activity_id"]
        pipeline_config_fixture.determinism.sort.ascending = [True]

        pipeline = TestPipeline(config=pipeline_config_fixture, run_id=run_id)

        # Override transform to return sample activity data
        pipeline.transform = lambda payload: sample_activity_data

        validated = pipeline.validate(sample_activity_data)

        assert isinstance(validated, pd.DataFrame)
        assert len(validated) == len(sample_activity_data)
        assert pipeline._validation_schema is not None
        assert pipeline._validation_summary is not None

    def test_validate_without_schema(self, pipeline_config_fixture, run_id: str):
        """Test validation without schema."""
        pipeline_config_fixture.validation.schema_out = None
        pipeline = TestPipeline(config=pipeline_config_fixture, run_id=run_id)

        df = pd.DataFrame({"id": [1, 2], "value": [10, 20]})
        validated = pipeline.validate(df)

        assert validated is df  # Should return unchanged when no schema
        assert pipeline._validation_schema is None

    def test_write(self, pipeline_config_fixture, run_id: str, sample_activity_data: pd.DataFrame):
        """Test write stage."""
        pipeline_config_fixture.validation.schema_out = "bioetl.schemas.activity:ActivitySchema"
        pipeline_config_fixture.determinism.sort.by = ["activity_id"]
        pipeline_config_fixture.determinism.sort.ascending = [True]
        pipeline_config_fixture.determinism.hashing.business_key_fields = ("activity_id",)

        pipeline = TestPipeline(config=pipeline_config_fixture, run_id=run_id)

        artifacts = pipeline.plan_run_artifacts(run_id)

        write_result = pipeline.write(sample_activity_data, artifacts)

        assert write_result.dataset.exists()
        assert write_result.metadata.exists()
        assert write_result.quality_report is not None
        assert write_result.quality_report.exists()

    def test_write_invalid_payload(self, pipeline_config_fixture, run_id: str):
        """Test write with invalid payload type."""
        pipeline = TestPipeline(config=pipeline_config_fixture, run_id=run_id)
        artifacts = pipeline.plan_run_artifacts(run_id)

        with pytest.raises(TypeError, match="expects a pandas DataFrame"):
            pipeline.write("invalid_payload", artifacts)

    def test_run_lifecycle(self, pipeline_config_fixture, run_id: str):
        """Test full pipeline lifecycle."""
        pipeline = TestPipeline(config=pipeline_config_fixture, run_id=run_id)

        result = pipeline.run()

        assert result.run_id == run_id
        assert result.write_result.dataset.exists()
        assert result.write_result.metadata.exists()
        assert "extract" in result.stage_durations_ms
        assert "transform" in result.stage_durations_ms
        assert "validate" in result.stage_durations_ms
        assert "write" in result.stage_durations_ms

    def test_run_with_mode(self, pipeline_config_fixture, run_id: str):
        """Test pipeline run with mode."""
        pipeline = TestPipeline(config=pipeline_config_fixture, run_id=run_id)

        result = pipeline.run(mode="full")

        assert result.write_result.dataset.name == f"activity_full_{run_id}.csv"

    def test_run_error_handling(self, pipeline_config_fixture, run_id: str):
        """Test error handling in pipeline run."""

        class FailingPipeline(TestPipeline):
            def extract(self, *args: object, **kwargs: object) -> pd.DataFrame:
                raise ValueError("Extraction failed")

        pipeline = FailingPipeline(config=pipeline_config_fixture, run_id=run_id)

        with pytest.raises(ValueError, match="Extraction failed"):
            pipeline.run()

        # Clients should be cleaned up even on error
        assert len(pipeline._registered_clients) == 0

    def test_build_quality_report(self, pipeline_config_fixture, run_id: str, sample_activity_data: pd.DataFrame):
        """Test quality report building."""
        pipeline_config_fixture.determinism.hashing.business_key_fields = ("activity_id",)
        pipeline = TestPipeline(config=pipeline_config_fixture, run_id=run_id)

        report = pipeline.build_quality_report(sample_activity_data)

        assert report is not None
        assert isinstance(report, (pd.DataFrame, dict))

    def test_build_correlation_report(self, pipeline_config_fixture, run_id: str, sample_activity_data: pd.DataFrame):
        """Test correlation report building."""
        pipeline = TestPipeline(config=pipeline_config_fixture, run_id=run_id)

        report = pipeline.build_correlation_report(sample_activity_data)

        assert report is not None
        assert isinstance(report, (pd.DataFrame, dict))

    def test_build_qc_metrics(self, pipeline_config_fixture, run_id: str, sample_activity_data: pd.DataFrame):
        """Test QC metrics building."""
        pipeline_config_fixture.determinism.hashing.business_key_fields = ("activity_id",)
        pipeline = TestPipeline(config=pipeline_config_fixture, run_id=run_id)

        metrics = pipeline.build_qc_metrics(sample_activity_data)

        assert metrics is not None
        assert isinstance(metrics, (pd.DataFrame, dict))

    def test_augment_metadata(self, pipeline_config_fixture, run_id: str, sample_activity_data: pd.DataFrame):
        """Test metadata augmentation."""
        pipeline = TestPipeline(config=pipeline_config_fixture, run_id=run_id)

        metadata = {"test": "value"}
        augmented = pipeline.augment_metadata(metadata, sample_activity_data)

        assert augmented == metadata  # Default implementation returns unchanged

    def test_close_resources(self, pipeline_config_fixture, run_id: str):
        """Test resource cleanup."""
        pipeline = TestPipeline(config=pipeline_config_fixture, run_id=run_id)

        # Should not raise
        pipeline.close_resources()

