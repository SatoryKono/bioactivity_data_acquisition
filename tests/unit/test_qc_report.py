"""Unit tests for QC report builders."""

from __future__ import annotations

import pandas as pd
import pytest

from bioetl.qc.report import build_correlation_report, build_qc_metrics_payload, build_quality_report


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

        assert isinstance(report, pd.DataFrame)
        assert "section" in report.columns
        assert "metric" in report.columns
        assert "value" in report.columns
        # Should have summary metrics
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

        assert isinstance(report, pd.DataFrame)
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

        assert isinstance(report, pd.DataFrame)
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

        assert isinstance(report, pd.DataFrame)
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

        assert isinstance(payload, dict)
        assert "row_count" in payload
        assert "total_missing_values" in payload
        assert "columns_with_missing" in payload

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

        assert "business_key_fields" in payload
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

        assert payload["total_missing_values"] == 0
        assert payload["columns_with_missing"] == ""

