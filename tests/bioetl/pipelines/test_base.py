"""Unit tests for PipelineBase orchestration."""

from __future__ import annotations

from collections.abc import Sequence
from pathlib import Path
from unittest.mock import MagicMock

import pandas as pd
import pytest

from bioetl.config.models.models import PipelineConfig
from bioetl.core.pipeline import PipelineBase, RunArtifacts
from tests.support.qc_assertions import assert_qc_artifact_set


class TestPipeline(PipelineBase):
    """Test pipeline implementation for testing PipelineBase."""

    def extract(self, *args: object, **kwargs: object) -> pd.DataFrame:
        """Return sample DataFrame."""
        return pd.DataFrame({"id": [1, 2, 3], "value": [10, 20, 30]})

    def extract_all(self) -> pd.DataFrame:
        """Extract all records - returns sample DataFrame for testing."""
        return pd.DataFrame({"id": [1, 2, 3], "value": [10, 20, 30]})

    def extract_by_ids(self, ids: Sequence[str]) -> pd.DataFrame:
        """Extract records by IDs - returns filtered sample DataFrame for testing."""
        base_df = pd.DataFrame({"id": [1, 2, 3], "value": [10, 20, 30]})
        id_ints: list[int] = [int(id_val) for id_val in ids if id_val.isdigit()]
        if id_ints:
            return base_df[base_df["id"].isin(id_ints)]  # type: ignore[arg-type]
        return pd.DataFrame({"id": [], "value": []})

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """Transform payload."""
        df = df.copy()
        df["value_doubled"] = df["value"] * 2
        if "id" in df.columns and "activity_id" not in df.columns:
            df["activity_id"] = df["id"]
        return df


@pytest.mark.unit
class TestPipelineBase:
    """Test suite for PipelineBase."""

    def test_init(self, pipeline_config_fixture: PipelineConfig, run_id: str) -> None:
        """Test PipelineBase initialization."""
        pipeline = TestPipeline(config=pipeline_config_fixture, run_id=run_id)

        assert pipeline.config == pipeline_config_fixture
        assert pipeline.run_id == run_id
        assert pipeline.pipeline_code == "activity_chembl"
        # Directory should not be created on initialization
        assert not pipeline.pipeline_directory.exists()
        assert pipeline.logs_directory.exists()

    def test_ensure_pipeline_directory(
        self, pipeline_config_fixture: PipelineConfig, run_id: str
    ) -> None:
        """Test pipeline directory path without creation."""
        pipeline = TestPipeline(config=pipeline_config_fixture, run_id=run_id)

        # Directory should not exist on initialization
        assert not pipeline.pipeline_directory.exists()
        assert pipeline.pipeline_directory.name == "_activity_chembl"
        # Directory should be created when needed
        created_dir = pipeline._ensure_pipeline_directory_exists()  # type: ignore[reportPrivateUsage]
        assert created_dir.exists()
        assert created_dir.is_dir()

    def test_ensure_logs_directory(
        self, pipeline_config_fixture: PipelineConfig, run_id: str
    ) -> None:
        """Test logs directory creation."""
        pipeline = TestPipeline(config=pipeline_config_fixture, run_id=run_id)

        assert pipeline.logs_directory.exists()
        assert pipeline.logs_directory.name == "activity_chembl"
        assert pipeline.logs_directory.is_dir()

    def test_derive_trace_and_span(self, pipeline_config_fixture: PipelineConfig) -> None:
        """Test trace and span ID derivation."""
        pipeline = TestPipeline(config=pipeline_config_fixture, run_id="test-123")

        trace_id, span_id = pipeline._derive_trace_and_span()  # type: ignore[reportPrivateUsage]

        assert len(trace_id) == 32
        assert len(span_id) == 16
        assert trace_id.isalnum() or "0" in trace_id
        assert span_id.isalnum() or "0" in span_id

    def test_build_run_stem(self, pipeline_config_fixture: PipelineConfig, run_id: str) -> None:
        """Test run stem generation."""
        pipeline = TestPipeline(config=pipeline_config_fixture, run_id=run_id)

        stem = pipeline.build_run_stem("20240101")
        assert stem == "activity_chembl_20240101"

        stem_with_mode = pipeline.build_run_stem("20240101", mode="full")
        assert stem_with_mode == "activity_chembl_full_20240101"

    def test_plan_run_artifacts(self, pipeline_config_fixture: PipelineConfig, run_id: str) -> None:
        """Test artifact planning."""
        pipeline = TestPipeline(config=pipeline_config_fixture, run_id=run_id)

        artifacts = pipeline.plan_run_artifacts("20240101")

        assert isinstance(artifacts, RunArtifacts)
        assert artifacts.write.dataset.exists() is False  # File not created yet
        assert artifacts.write.dataset.name == "activity_chembl_20240101.csv"
        assert artifacts.write.metadata is None
        assert artifacts.write.quality_report is not None
        assert artifacts.write.correlation_report is None  # Default is False
        assert artifacts.write.qc_metrics is None

    def test_plan_run_artifacts_with_correlation(
        self, pipeline_config_fixture: PipelineConfig, run_id: str
    ) -> None:
        """Test artifact planning with correlation report."""
        pipeline = TestPipeline(config=pipeline_config_fixture, run_id=run_id)

        artifacts = pipeline.plan_run_artifacts("20240101", include_correlation=True)

        assert artifacts.write.correlation_report is not None
        assert (
            artifacts.write.correlation_report.name
            == "activity_chembl_20240101_correlation_report.csv"
        )

    def test_list_run_stems(
        self, pipeline_config_fixture: PipelineConfig, run_id: str, tmp_output_dir: Path
    ) -> None:
        """Test listing run stems."""
        pipeline = TestPipeline(config=pipeline_config_fixture, run_id=run_id)

        # Create mock dataset files - need to create directory first
        pipeline_dir = pipeline._ensure_pipeline_directory_exists()  # type: ignore[reportPrivateUsage]
        dataset_file1 = pipeline_dir / "activity_chembl_20240101.csv"
        dataset_file2 = pipeline_dir / "activity_chembl_20240102.csv"
        dataset_file1.touch()
        dataset_file2.touch()

        stems = pipeline.list_run_stems()

        assert len(stems) >= 2
        assert "activity_chembl_20240101" in stems
        assert "activity_chembl_20240102" in stems

    def test_apply_retention_policy(
        self, pipeline_config_fixture: PipelineConfig, run_id: str
    ) -> None:
        """Test retention policy application."""
        pipeline = TestPipeline(config=pipeline_config_fixture, run_id=run_id)
        pipeline.retention_runs = 2

        # Create more runs than retention limit - need to create directory first
        pipeline_dir = pipeline._ensure_pipeline_directory_exists()  # type: ignore[reportPrivateUsage]
        for i in range(5):
            dataset_file = pipeline_dir / f"activity_chembl_run{i}.csv"
            dataset_file.touch()

        pipeline.apply_retention_policy()

        # Check that only 2 most recent runs remain
        remaining_datasets = list(pipeline_dir.glob("activity_chembl_*.csv"))
        assert len(remaining_datasets) <= 2

    def test_apply_retention_policy_disabled(
        self, pipeline_config_fixture: PipelineConfig, run_id: str
    ) -> None:
        """Test retention policy when disabled."""
        pipeline = TestPipeline(config=pipeline_config_fixture, run_id=run_id)
        pipeline.retention_runs = 0

        # Create some runs - need to create directory first
        pipeline_dir = pipeline._ensure_pipeline_directory_exists()  # type: ignore[reportPrivateUsage]
        for i in range(3):
            dataset_file = pipeline_dir / f"activity_chembl_run{i}.csv"
            dataset_file.touch()

        pipeline.apply_retention_policy()

        # All runs should remain
        remaining_datasets = list(pipeline_dir.glob("activity_chembl_*.csv"))
        assert len(remaining_datasets) == 3

    def test_register_client(self, pipeline_config_fixture: PipelineConfig, run_id: str) -> None:
        """Test client registration."""
        pipeline = TestPipeline(config=pipeline_config_fixture, run_id=run_id)

        mock_client = MagicMock()
        mock_client.close = MagicMock()

        pipeline.register_client("test_client", mock_client)

        # Should be able to register
        assert "test_client" in pipeline._registered_clients  # type: ignore[reportPrivateUsage]

    def test_register_client_callable(
        self, pipeline_config_fixture: PipelineConfig, run_id: str
    ) -> None:
        """Test registering a callable client."""
        pipeline = TestPipeline(config=pipeline_config_fixture, run_id=run_id)

        def close_client() -> None:
            pass

        pipeline.register_client("callable_client", close_client)

        assert "callable_client" in pipeline._registered_clients  # type: ignore[reportPrivateUsage]

    def test_register_client_duplicate(
        self, pipeline_config_fixture: PipelineConfig, run_id: str
    ) -> None:
        """Test that registering duplicate client raises error."""
        pipeline = TestPipeline(config=pipeline_config_fixture, run_id=run_id)

        mock_client = MagicMock()
        mock_client.close = MagicMock()

        pipeline.register_client("test_client", mock_client)

        with pytest.raises(ValueError, match="already registered"):
            pipeline.register_client("test_client", mock_client)

    def test_register_client_invalid(
        self, pipeline_config_fixture: PipelineConfig, run_id: str
    ) -> None:
        """Test that registering invalid client raises error."""
        pipeline = TestPipeline(config=pipeline_config_fixture, run_id=run_id)

        invalid_client = object()  # No close method

        with pytest.raises(TypeError, match="does not expose callable"):
            pipeline.register_client("invalid_client", invalid_client)

    def test_validate_with_schema(
        self,
        pipeline_config_fixture: PipelineConfig,
        run_id: str,
        sample_activity_data: pd.DataFrame,
    ) -> None:
        """Test validation with Pandera schema."""
        # Update config to use activity schema
        pipeline_config_fixture.validation.schema_out = (
            "bioetl.schemas.chembl_activity_schema:ActivitySchema"
        )
        pipeline_config_fixture.determinism.sort.by = ["activity_id"]
        pipeline_config_fixture.determinism.sort.ascending = [True]

        pipeline = TestPipeline(config=pipeline_config_fixture, run_id=run_id)

        # Override transform to return sample activity data
        valid_sample = sample_activity_data.copy()
        if "target_tax_id" in valid_sample.columns:
            valid_sample["target_tax_id"] = valid_sample["target_tax_id"].fillna(1).astype(int)  # type: ignore[arg-type]
        pipeline.transform = lambda df: valid_sample

        validated = pipeline.validate(valid_sample)

        assert isinstance(validated, pd.DataFrame)
        assert len(validated) == len(valid_sample)
        assert pipeline._validation_schema is not None  # type: ignore[reportPrivateUsage]
        assert pipeline._validation_summary is not None  # type: ignore[reportPrivateUsage]
        assert pipeline._validation_summary.get("schema_valid") is True  # type: ignore[reportPrivateUsage]

    def test_validate_without_schema(
        self, pipeline_config_fixture: PipelineConfig, run_id: str
    ) -> None:
        """Test validation without schema."""
        pipeline_config_fixture.validation.schema_out = None
        pipeline = TestPipeline(config=pipeline_config_fixture, run_id=run_id)

        df = pd.DataFrame({"id": [1, 2], "value": [10, 20]})
        validated = pipeline.validate(df)

        assert validated is df  # Should return unchanged when no schema
        assert pipeline._validation_schema is None  # type: ignore[reportPrivateUsage]
        assert pipeline._validation_summary is None  # type: ignore[reportPrivateUsage]

    def test_validate_fail_open_schema_errors(
        self,
        pipeline_config_fixture: PipelineConfig,
        run_id: str,
        sample_activity_data: pd.DataFrame,
    ) -> None:
        """Schema errors are logged and execution continues when fail-open is enabled."""

        pipeline_config_fixture.validation.schema_out = (
            "bioetl.schemas.chembl_activity_schema:ActivitySchema"
        )
        pipeline_config_fixture.determinism.sort.by = ["activity_id"]
        pipeline_config_fixture.determinism.sort.ascending = [True]

        pipeline = TestPipeline(config=pipeline_config_fixture, run_id=run_id)
        pipeline.config.cli.fail_on_schema_drift = False

        faulty = sample_activity_data.copy()
        faulty.loc[:, "activity_id"] = "invalid"

        validated = pipeline.validate(faulty)

        assert isinstance(validated, pd.DataFrame)
        assert pipeline._validation_summary is not None  # type: ignore[reportPrivateUsage]
        assert pipeline._validation_summary.get("schema_valid") is False  # type: ignore[reportPrivateUsage]
        assert "error" in pipeline._validation_summary  # type: ignore[reportPrivateUsage]

    def test_write(
        self,
        pipeline_config_fixture: PipelineConfig,
        run_id: str,
        sample_activity_data: pd.DataFrame,
    ) -> None:
        """Test write stage."""
        pipeline_config_fixture.validation.schema_out = (
            "bioetl.schemas.chembl_activity_schema:ActivitySchema"
        )
        pipeline_config_fixture.determinism.sort.by = ["activity_id"]
        pipeline_config_fixture.determinism.sort.ascending = [True]
        pipeline_config_fixture.determinism.hashing.business_key_fields = ("activity_id",)

        pipeline = TestPipeline(config=pipeline_config_fixture, run_id=run_id)

        artifacts = pipeline.plan_run_artifacts(
            run_id,
            include_metadata=True,
            include_qc_metrics=True,
        )

        # Use output_path from artifacts for write()
        result = pipeline.write(sample_activity_data, artifacts.write.dataset.parent, extended=True)

        assert_qc_artifact_set(
            result.write_result,
            expect_metrics=True,
        )

    def test_write_invalid_payload(
        self, pipeline_config_fixture: PipelineConfig, run_id: str, tmp_output_dir: Path
    ) -> None:
        """Test write with invalid payload type."""
        pipeline = TestPipeline(config=pipeline_config_fixture, run_id=run_id)

        with pytest.raises(TypeError, match="expects a pandas DataFrame"):
            pipeline.write("invalid_payload", tmp_output_dir)  # type: ignore[arg-type]

    def test_run_lifecycle(
        self, pipeline_config_fixture: PipelineConfig, run_id: str, tmp_output_dir: Path
    ) -> None:
        """Test full pipeline lifecycle."""
        pipeline_config_fixture.validation.schema_out = None
        pipeline = TestPipeline(config=pipeline_config_fixture, run_id=run_id)

        result = pipeline.run(Path(tmp_output_dir))

        assert result.run_id == run_id
        assert result.write_result.dataset.exists()
        assert result.write_result.metadata is None
        assert result.manifest is None
        assert "extract" in result.stage_durations_ms
        assert "transform" in result.stage_durations_ms
        assert "validate" in result.stage_durations_ms
        assert "write" in result.stage_durations_ms

    def test_cli_sample_application(
        self, pipeline_config_fixture: PipelineConfig, run_id: str
    ) -> None:
        """Deterministic sampling is applied when configured via CLI."""
        pipeline_config_fixture.validation.schema_out = None
        pipeline_config_fixture.cli.sample = 2

        pipeline = TestPipeline(config=pipeline_config_fixture, run_id=run_id)
        df = pd.DataFrame({"id": list(range(10)), "value": list(range(10))})

        sampled = pipeline._apply_cli_sample(df)  # type: ignore[reportPrivateUsage]
        assert len(sampled) == 2

        sampled_again = pipeline._apply_cli_sample(df)  # type: ignore[reportPrivateUsage]
        pd.testing.assert_frame_equal(sampled, sampled_again)

    def test_run_with_mode(
        self, pipeline_config_fixture: PipelineConfig, run_id: str, tmp_output_dir: Path
    ) -> None:
        """Test pipeline run with extended mode."""
        pipeline_config_fixture.validation.schema_out = None
        pipeline = TestPipeline(config=pipeline_config_fixture, run_id=run_id)

        result = pipeline.run(Path(tmp_output_dir), extended=True)

        # The dataset name depends on the date tag, which is derived from config
        assert "activity_chembl_extended_" in result.write_result.dataset.name

    def test_run_error_handling(
        self, pipeline_config_fixture: PipelineConfig, run_id: str, tmp_output_dir: Path
    ) -> None:
        """Test error handling in pipeline run."""
        pipeline_config_fixture.validation.schema_out = None

        class FailingPipeline(TestPipeline):
            def extract(self, *args: object, **kwargs: object) -> pd.DataFrame:
                raise ValueError("Extraction failed")

        pipeline = FailingPipeline(config=pipeline_config_fixture, run_id=run_id)

        with pytest.raises(ValueError, match="Extraction failed"):
            pipeline.run(Path(tmp_output_dir))

        # Clients should be cleaned up even on error
        assert len(pipeline._registered_clients) == 0  # type: ignore[reportPrivateUsage]

    def test_build_quality_report(
        self,
        pipeline_config_fixture: PipelineConfig,
        run_id: str,
        sample_activity_data: pd.DataFrame,
    ) -> None:
        """Test quality report building."""
        pipeline_config_fixture.determinism.hashing.business_key_fields = ("activity_id",)
        pipeline = TestPipeline(config=pipeline_config_fixture, run_id=run_id)

        report = pipeline.build_quality_report(sample_activity_data)

        assert report is not None
        assert isinstance(report, (pd.DataFrame, dict))

    def test_build_correlation_report(
        self,
        pipeline_config_fixture: PipelineConfig,
        run_id: str,
        sample_activity_data: pd.DataFrame,
    ) -> None:
        """Test correlation report building."""
        pipeline = TestPipeline(config=pipeline_config_fixture, run_id=run_id)

        report = pipeline.build_correlation_report(sample_activity_data)

        assert report is not None
        assert isinstance(report, (pd.DataFrame, dict))

    def test_build_qc_metrics(
        self,
        pipeline_config_fixture: PipelineConfig,
        run_id: str,
        sample_activity_data: pd.DataFrame,
    ) -> None:
        """Test QC metrics building."""
        pipeline_config_fixture.determinism.hashing.business_key_fields = ("activity_id",)
        pipeline = TestPipeline(config=pipeline_config_fixture, run_id=run_id)

        metrics = pipeline.build_qc_metrics(sample_activity_data)

        assert metrics is not None
        assert isinstance(metrics, (pd.DataFrame, dict))

    def test_augment_metadata(
        self,
        pipeline_config_fixture: PipelineConfig,
        run_id: str,
        sample_activity_data: pd.DataFrame,
    ) -> None:
        """Test metadata augmentation."""
        pipeline = TestPipeline(config=pipeline_config_fixture, run_id=run_id)

        metadata = {"test": "value"}
        augmented = pipeline.augment_metadata(metadata, sample_activity_data)

        assert augmented == metadata  # Default implementation returns unchanged

    def test_close_resources(self, pipeline_config_fixture: PipelineConfig, run_id: str) -> None:
        """Test resource cleanup."""
        pipeline = TestPipeline(config=pipeline_config_fixture, run_id=run_id)

        # Should not raise
        pipeline.close_resources()

    def test_pipeline_directory_lazy_creation(
        self,
        pipeline_config_fixture: PipelineConfig,
        run_id: str,
        sample_activity_data: pd.DataFrame,
    ) -> None:
        """Test that pipeline directory is created only when writing files."""
        pipeline_config_fixture.validation.schema_out = (
            "bioetl.schemas.chembl_activity_schema:ActivitySchema"
        )
        pipeline_config_fixture.determinism.sort.by = ["activity_id"]
        pipeline_config_fixture.determinism.sort.ascending = [True]
        pipeline_config_fixture.determinism.hashing.business_key_fields = ("activity_id",)

        pipeline = TestPipeline(config=pipeline_config_fixture, run_id=run_id)

        # Directory should not exist after initialization
        assert not pipeline.pipeline_directory.exists()

        # Directory should not exist after planning artifacts
        pipeline.plan_run_artifacts(run_id, include_metadata=True)
        assert not pipeline.pipeline_directory.exists()

        # Directory should be created when writing to pipeline_directory
        # Use pipeline_directory directly as output_path
        output_file = pipeline.pipeline_directory / "test_output.csv"
        result = pipeline.write(sample_activity_data, output_file, extended=True)
        assert pipeline.pipeline_directory.exists()
        assert result.write_result.dataset.exists()

    def test_list_run_stems_empty_directory(
        self, pipeline_config_fixture: PipelineConfig, run_id: str
    ) -> None:
        """Test that list_run_stems returns empty list when directory doesn't exist."""
        pipeline = TestPipeline(config=pipeline_config_fixture, run_id=run_id)

        # Directory should not exist
        assert not pipeline.pipeline_directory.exists()

        # Should return empty list
        stems = pipeline.list_run_stems()
        assert stems == []
