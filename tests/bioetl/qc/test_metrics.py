"""QC tests for quality control metrics."""

from __future__ import annotations

import pandas as pd
import pytest

from bioetl.config import PipelineConfig
from bioetl.pipelines.chembl.activity.run import ChemblActivityPipeline
from bioetl.qc.metrics import compute_correlation_matrix, compute_missingness
from bioetl.qc.report import build_qc_metrics_payload, build_quality_report


@pytest.mark.qc
class TestQCMetrics:
    """Test suite for QC metrics."""

    def test_build_quality_report(self, sample_activity_data: pd.DataFrame):
        """Test quality report building."""
        report = build_quality_report(sample_activity_data, business_key_fields=["activity_id"])

        assert report is not None
        assert not report.empty
        # Should have metrics columns
        assert "section" in report.columns or "metric" in report.columns
        # Distribution sections should be present for relation and units fields
        assert (
            report["metric"].eq("relation_distribution").any()
            or report["metric"].eq("units_distribution").any()
        )

    def test_build_qc_metrics(self, sample_activity_data: pd.DataFrame):
        """Test QC metrics payload building."""
        metrics = build_qc_metrics_payload(
            sample_activity_data, business_key_fields=["activity_id"]
        )

        assert metrics is not None
        assert isinstance(metrics, dict)

        # Should have key metrics
        assert "row_count" in metrics or "total_rows" in metrics
        assert "duplicate_count" in metrics or "unique_count" in metrics
        # New distribution metrics should be part of the payload
        assert "units_distribution" in metrics
        assert "relation_distribution" in metrics
        assert "iqr_outliers" in metrics

    def test_quality_report_detects_outliers(self):
        """Ensure that simple outliers are surfaced in the report."""

        df = pd.DataFrame(
            {
                "activity_id": [1, 2, 3, 4],
                "value": [10.0, 11.0, 12.0, 500.0],  # 500 should be flagged as outlier
                "units": ["nM", "nM", "nM", "nM"],
                "relation": ["=", "=", "=", "="],
            }
        )

        report = build_quality_report(df, business_key_fields=["activity_id"])

        outlier_rows = report[report["metric"] == "iqr_outlier_count"]
        assert not outlier_rows.empty
        assert set(outlier_rows["column"]) == {"value"}

    def test_quality_report_duplicate_detection(self):
        """Test that quality report detects duplicates."""
        df = pd.DataFrame(
            {
                "activity_id": [1, 2, 1],  # Duplicate
                "molecule_chembl_id": ["CHEMBL1", "CHEMBL2", "CHEMBL1"],
            }
        )

        report = build_quality_report(df, business_key_fields=["activity_id"])

        assert report is not None
        # Should detect duplicates
        duplicate_metrics = report[report["metric"].str.contains("duplicate", case=False, na=False)]
        assert not duplicate_metrics.empty

    def test_qc_metrics_with_empty_dataframe(self):
        """Test QC metrics with empty DataFrame."""
        df = pd.DataFrame()

        metrics = build_qc_metrics_payload(df, business_key_fields=[])

        assert metrics is not None
        assert isinstance(metrics, dict)

    def test_compute_missingness_is_sorted(self) -> None:
        """Missingness stats should be sorted deterministically."""
        df = pd.DataFrame(
            {
                "b": [1, None, 3],
                "a": [None, None, 3],
                "c": [1, 2, 3],
            }
        )

        result = compute_missingness(df)
        reordered = compute_missingness(df[["c", "b", "a"]])

        assert result.equals(reordered)
        assert result["column"].tolist() == ["a", "b", "c"]
        assert str(result["column"].dtype) == "string[python]"

    def test_compute_correlation_matrix_sorted_axes(self) -> None:
        """Correlation matrix should sort axes alphabetically."""
        df = pd.DataFrame(
            {
                "z": [1, 2, 3, 4],
                "a": [2, 4, 6, 8],
                "m": [5, 6, 7, 8],
            }
        )

        correlation = compute_correlation_matrix(df)
        assert correlation is not None
        assert list(correlation.columns) == ["a", "m", "z"]
        assert list(correlation.index) == ["a", "m", "z"]

    def test_pipeline_qc_artifacts(
        self,
        pipeline_config_fixture: PipelineConfig,
        run_id: str,
        sample_activity_data: pd.DataFrame,
    ):
        """Test that pipeline creates QC artifacts."""
        pipeline_config_fixture.validation.schema_out = "bioetl.schemas.chembl_activity_schema:ActivitySchema"  # type: ignore[attr-defined]
        pipeline_config_fixture.determinism.sort.by = ["activity_id"]  # type: ignore[attr-defined]
        pipeline_config_fixture.determinism.hashing.business_key_fields = ("activity_id",)  # type: ignore[attr-defined]

        pipeline = ChemblActivityPipeline(config=pipeline_config_fixture, run_id=run_id)  # type: ignore[arg-type]
        artifacts = pipeline.plan_run_artifacts(run_id)

        result = pipeline.write(sample_activity_data, artifacts.run_directory)

        # Should have QC artifacts
        assert result.write_result.quality_report is not None
        assert result.write_result.quality_report.exists()

        # Verify QC report is readable
        report_df = pd.read_csv(result.write_result.quality_report)  # type: ignore[arg-type]
        assert not report_df.empty
