"""Unit tests for QC report builders."""

from __future__ import annotations

import pandas as pd
import pytest
from pandas.testing import assert_frame_equal

from bioetl.qc.plan import QCPlan, QCMetricsExecutor
from bioetl.qc.report import (
    build_correlation_report,
    build_qc_metrics_payload,
    build_quality_report,
)
from tests.support.qc_assertions import (
    assert_quality_report_structure,
    assert_qc_metrics_payload_structure,
)


@pytest.mark.unit
class TestQCReport:
    """Test suite for QC report builders."""

    def test_build_quality_report_basic(self) -> None:
        """Test building a basic quality report."""
        df = pd.DataFrame(
            {
                "id": [1, 2, 3, 4, 5],
                "value": [10, 20, 30, 40, 50],
                "name": ["A", "B", "C", "D", "E"],
            }
        )

        report = build_quality_report(df)

        assert_quality_report_structure(report, required_sections=("summary",))
        summary = report[report["section"] == "summary"]
        assert len(summary) > 0

    def test_build_quality_report_with_duplicates(self) -> None:
        """Test building quality report with duplicates."""
        df = pd.DataFrame(
            {
                "id": [1, 2, 2, 3, 3, 3],
                "value": [10, 20, 20, 30, 30, 30],
            }
        )

        report = build_quality_report(df)

        assert_quality_report_structure(report, required_sections=("summary",))
        summary = report[report["section"] == "summary"]
        assert len(summary) > 0

    def test_build_quality_report_with_missing(self) -> None:
        """Test building quality report with missing values."""
        df = pd.DataFrame(
            {
                "id": [1, 2, 3, 4, 5],
                "value": [10, 20, None, 40, None],
                "name": ["A", "B", "C", None, "E"],
            }
        )

        report = build_quality_report(df)

        assert_quality_report_structure(report, required_sections=("missing",))
        missing = report[report["section"] == "missing"]
        assert len(missing) > 0
        assert "column" in missing.columns
        assert "ratio" in missing.columns

    def test_build_quality_report_with_business_key(self) -> None:
        """Test building quality report with business key fields."""
        df = pd.DataFrame(
            {
                "id": [1, 2, 3, 4, 5],
                "value": [10, 20, 30, 40, 50],
            }
        )

        report = build_quality_report(df, business_key_fields=("id",))

        assert_quality_report_structure(report, required_sections=("summary",))
        summary = report[report["section"] == "summary"]
        assert len(summary) > 0

    def test_build_correlation_report_with_numeric(self) -> None:
        """Test building correlation report with numeric data."""
        df = pd.DataFrame(
            {
                "col1": [1, 2, 3, 4, 5],
                "col2": [2, 4, 6, 8, 10],
                "col3": [1, 1, 1, 1, 1],  # No variance
            }
        )

        report = build_correlation_report(df)

        assert report is not None
        assert isinstance(report, pd.DataFrame)
        assert "feature" in report.columns

    def test_build_correlation_report_with_no_variance(self) -> None:
        """Test building correlation report with no variance."""
        df = pd.DataFrame(
            {
                "col1": [1, 1, 1, 1, 1],
                "col2": [2, 2, 2, 2, 2],
            }
        )

        report = build_correlation_report(df)

        # When no variance, correlation matrix may still be created but with NaN values
        # or may return None if correlation computation fails
        if report is not None:
            # If report exists, check it has expected structure
            assert isinstance(report, pd.DataFrame)
            assert "feature" in report.columns

    def test_build_correlation_report_with_strings(self) -> None:
        """Test building correlation report with string data."""
        df = pd.DataFrame(
            {
                "col1": ["A", "B", "C", "D", "E"],
                "col2": [1, 2, 3, 4, 5],
            }
        )

        report = build_correlation_report(df)

        # Should only correlate numeric columns
        if report is not None:
            assert isinstance(report, pd.DataFrame)

    def test_build_qc_metrics_payload_basic(self) -> None:
        """Test building QC metrics payload."""
        df = pd.DataFrame(
            {
                "id": [1, 2, 3, 4, 5],
                "value": [10, 20, 30, 40, 50],
            }
        )

        payload = build_qc_metrics_payload(df)

        assert_qc_metrics_payload_structure(payload)

    def test_build_qc_metrics_payload_with_missing(self) -> None:
        """Test building QC metrics payload with missing values."""
        df = pd.DataFrame(
            {
                "id": [1, 2, 3, 4, 5],
                "value": [10, 20, None, 40, None],
                "name": ["A", "B", "C", None, "E"],
            }
        )

        payload = build_qc_metrics_payload(df)

        assert_qc_metrics_payload_structure(payload)
        assert payload["total_missing_values"] > 0
        assert payload["columns_with_missing"] != ""

    def test_build_qc_metrics_payload_with_business_key(self) -> None:
        """Test building QC metrics payload with business key fields."""
        df = pd.DataFrame(
            {
                "id": [1, 2, 3, 4, 5],
                "value": [10, 20, 30, 40, 50],
            }
        )

        payload = build_qc_metrics_payload(df, business_key_fields=("id",))

        assert_qc_metrics_payload_structure(payload, required_keys=("business_key_fields",))
        assert payload["business_key_fields"] == ["id"]

    def test_build_qc_metrics_payload_no_missing(self) -> None:
        """Test building QC metrics payload with no missing values."""
        df = pd.DataFrame(
            {
                "id": [1, 2, 3, 4, 5],
                "value": [10, 20, 30, 40, 50],
            }
        )

        payload = build_qc_metrics_payload(df)

        assert_qc_metrics_payload_structure(payload)
        assert payload["total_missing_values"] == 0
        assert payload["columns_with_missing"] == ""

    def test_build_quality_report_is_deterministic(self) -> None:
        """Quality report should be identical across repeated invocations."""
        df = pd.DataFrame(
            {
                "activity_id": [1, 2, 2, 4],
                "value": [1.0, None, 3.0, 4.0],
                "value_units": ["nM", "uM", "nM", "nM"],
                "value_relation": ["=", ">", "<", "="],
            }
        )

        report1 = build_quality_report(df, business_key_fields=("activity_id",))
        report2 = build_quality_report(df.copy(), business_key_fields=("activity_id",))

        assert_frame_equal(report1, report2, check_dtype=False, check_like=False)

    def test_build_qc_metrics_payload_sorted_keys(self) -> None:
        """QC payload should expose sorted keys for nested structures."""
        df = pd.DataFrame(
            {
                "id": [1, 2, 3, 4],
                "measurement_units": ["mM", "nM", "uM", "nM"],
                "measurement_relation": ["<", "=", ">", "="],
            }
        )

        payload = build_qc_metrics_payload(df)

        units_distribution_keys = list(payload["units_distribution"]["measurement_units"].keys())
        relation_distribution_keys = list(
            payload["relation_distribution"]["measurement_relation"].keys()
        )

        assert units_distribution_keys == sorted(units_distribution_keys)
        assert relation_distribution_keys == sorted(relation_distribution_keys)

    def test_build_correlation_report_has_sorted_columns(self) -> None:
        """Correlation report should include feature column followed by sorted metrics."""
        df = pd.DataFrame(
            {
                "b": [1, 2, 3, 4, 5],
                "a": [2, 3, 4, 5, 6],
                "c": [5, 4, 3, 2, 1],
            }
        )

        report = build_correlation_report(df)

        assert report is not None
        assert list(report.columns) == ["feature", "a", "b", "c"]

    def test_build_qc_metrics_payload_limits_distribution(self) -> None:
        """QC payload should aggregate rare categories into the other bucket."""

        units = [f"unit_{index:02d}" for index in range(25)]
        df = pd.DataFrame(
            {
                "measurement_units": units,
                "measurement_relation": ["="] * len(units),
            }
        )

        payload = build_qc_metrics_payload(df)
        units_distribution = payload["units_distribution"]["measurement_units"]

        assert "__other__" in units_distribution
        assert len(units_distribution) == 21  # top 20 + other bucket
        assert units_distribution["__other__"]["count"] == 5
        assert units_distribution["__other__"]["ratio"] == 0.2

    def test_build_quality_report_from_bundle(self) -> None:
        """Quality report should accept a precomputed bundle."""
        df = pd.DataFrame(
            {
                "activity_id": [1, 2, 3],
                "value": [1.0, 2.0, 3.0],
                "value_units": ["nM", "nM", "uM"],
            }
        )
        executor = QCMetricsExecutor()
        bundle = executor.execute(df)

        report = build_quality_report(bundle=bundle)

        assert_quality_report_structure(report)

    def test_build_correlation_report_respects_plan(self) -> None:
        """Correlation builder should follow plan toggles."""
        df = pd.DataFrame(
            {
                "col1": [1, 2, 3],
                "col2": [2, 4, 6],
            }
        )
        executor = QCMetricsExecutor()
        plan = QCPlan(correlation=False)
        bundle = executor.execute(df, plan=plan)

        report = build_correlation_report(bundle=bundle, plan=plan)

        assert report is None