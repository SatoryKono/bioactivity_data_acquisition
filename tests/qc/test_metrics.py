"""Tests for QC metrics and metric registry."""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from bioetl.qc.metrics import (
    DEFAULT_REGISTRY,
    MetricRegistry,
    QCFailureException,
    QCMetricResult,
    metric_distribution_summary,
    metric_null_percentage,
    metric_pchembl_consistency,
    metric_pka_range,
    metric_row_count,
    metric_unique_count,
)
from bioetl.qc.plan import MetricSpec


class TestQCMetricResult:
    """Test suite for QCMetricResult."""

    def test_to_payload_basic(self) -> None:
        """Test basic payload conversion."""
        result = QCMetricResult(
            name="test",
            metric_type="count",
            value=42,
            threshold=50.0,
            status="PASS",
            message="All good"
        )
        
        payload = result.to_payload()
        
        assert payload["name"] == "test"
        assert payload["metric_type"] == "count"
        assert payload["value"] == 42
        assert payload["threshold"] == 50.0
        assert payload["status"] == "PASS"
        assert payload["message"] == "All good"
        assert payload["details"] is None

    def test_to_payload_with_dataframe_details(self) -> None:
        """Test payload conversion with DataFrame details."""
        details = pd.DataFrame([{"col": "A", "ratio": 0.5}])
        result = QCMetricResult(
            name="test",
            metric_type="ratio",
            value=0.5,
            details=details
        )
        
        payload = result.to_payload()
        
        assert payload["details"] == [{"col": "A", "ratio": 0.5}]


class TestQCFailureException:
    """Test suite for QCFailureException."""

    def test_exception_creation(self) -> None:
        """Test exception creation with failures."""
        failures = {"metric1": QCMetricResult("test", "count", 10)}
        exc = QCFailureException(failures)
        
        assert str(exc) == "QC thresholds violated"
        assert exc.failures == failures


class TestMetricRegistry:
    """Test suite for MetricRegistry."""

    def test_init_empty(self) -> None:
        """Test empty registry initialization."""
        registry = MetricRegistry()
        assert registry.list_metrics() == ()

    def test_register_metric(self) -> None:
        """Test metric registration."""
        registry = MetricRegistry()
        
        def dummy_metric(df, spec):
            return QCMetricResult("test", "count", 0)
        
        registry.register("test_metric", dummy_metric)
        
        assert "test_metric" in registry.list_metrics()
        assert registry.get("test_metric") == dummy_metric

    def test_register_duplicate_error(self) -> None:
        """Test error on duplicate registration."""
        registry = MetricRegistry()
        
        def dummy_metric(df, spec):
            return QCMetricResult("test", "count", 0)
        
        registry.register("test_metric", dummy_metric)
        
        with pytest.raises(ValueError, match="metric 'test_metric' already registered"):
            registry.register("test_metric", dummy_metric)

    def test_register_duplicate_with_override(self) -> None:
        """Test successful duplicate registration with override."""
        registry = MetricRegistry()
        
        def dummy_metric(df, spec):
            return QCMetricResult("test", "count", 0)
        
        def dummy_metric2(df, spec):
            return QCMetricResult("test", "count", 1)
        
        registry.register("test_metric", dummy_metric)
        registry.register("test_metric", dummy_metric2, override=True)
        
        assert registry.get("test_metric") == dummy_metric2

    def test_register_empty_name_error(self) -> None:
        """Test error on empty metric name."""
        registry = MetricRegistry()
        
        def dummy_metric(df, spec):
            return QCMetricResult("test", "count", 0)
        
        with pytest.raises(ValueError, match="metric name must be provided"):
            registry.register("", dummy_metric)

    def test_get_nonexistent_metric_error(self) -> None:
        """Test error on getting nonexistent metric."""
        registry = MetricRegistry()
        
        with pytest.raises(KeyError, match="metric 'nonexistent' is not registered"):
            registry.get("nonexistent")

    def test_list_metrics_sorted(self) -> None:
        """Test that metrics are listed in sorted order."""
        registry = MetricRegistry()
        
        def dummy_metric(df, spec):
            return QCMetricResult("test", "count", 0)
        
        registry.register("z_metric", dummy_metric)
        registry.register("a_metric", dummy_metric)
        registry.register("m_metric", dummy_metric)
        
        assert registry.list_metrics() == ("a_metric", "m_metric", "z_metric")


class TestMetricFunctions:
    """Test suite for metric functions."""

    def test_metric_row_count(self) -> None:
        """Test row count metric."""
        df = pd.DataFrame([{"a": 1}, {"b": 2}, {"c": 3}])
        spec = MetricSpec(name="row_count", type="count")
        
        result = metric_row_count(df, spec)
        
        assert result.name == "row_count"
        assert result.metric_type == "count"
        assert result.value == 3

    def test_metric_row_count_empty(self) -> None:
        """Test row count metric with empty DataFrame."""
        df = pd.DataFrame()
        spec = MetricSpec(name="row_count", type="count")
        
        result = metric_row_count(df, spec)
        
        assert result.value == 0

    def test_metric_null_percentage_normal(self) -> None:
        """Test null percentage metric with normal data."""
        df = pd.DataFrame({
            "a": [1, 2, None],
            "b": [None, None, None],
            "c": [1, 2, 3]
        })
        spec = MetricSpec(name="null_percentage", type="ratio")
        
        result = metric_null_percentage(df, spec)
        
        assert result.name == "null_percentage"
        assert result.metric_type == "ratio"
        assert result.value == 1.0  # column b is 100% null
        
        details = result.details
        assert len(details) == 3
        assert details.loc[details["column"] == "a", "null_ratio"].iloc[0] == 1/3
        assert details.loc[details["column"] == "b", "null_ratio"].iloc[0] == 1.0
        assert details.loc[details["column"] == "c", "null_ratio"].iloc[0] == 0.0

    def test_metric_null_percentage_empty(self) -> None:
        """Test null percentage metric with empty DataFrame."""
        df = pd.DataFrame()
        spec = MetricSpec(name="null_percentage", type="ratio")
        
        result = metric_null_percentage(df, spec)
        
        assert result.value == 0.0
        assert result.details.columns.tolist() == ["column", "null_ratio"]
        assert len(result.details) == 0

    def test_metric_unique_count_success(self) -> None:
        """Test unique count metric success case."""
        df = pd.DataFrame({"col": [1, 2, 2, 3, 3, 3]})
        spec = MetricSpec(name="unique_count", type="count", params={"column": "col"})
        
        result = metric_unique_count(df, spec)
        
        assert result.name == "unique_count"
        assert result.metric_type == "count"
        assert result.value == 3  # 1, 2, 3

    def test_metric_unique_count_no_column_param(self) -> None:
        """Test unique count metric without column parameter."""
        df = pd.DataFrame({"col": [1, 2, 3]})
        spec = MetricSpec(name="unique_count", type="count")
        
        result = metric_unique_count(df, spec)
        
        assert result.value == 0
        assert "column parameter is not configured" in result.message

    def test_metric_unique_count_column_not_present(self) -> None:
        """Test unique count metric with missing column."""
        df = pd.DataFrame({"other": [1, 2, 3]})
        spec = MetricSpec(name="unique_count", type="count", params={"column": "missing"})
        
        result = metric_unique_count(df, spec)
        
        assert result.value == 0
        assert "column 'missing' not present" in result.message

    def test_metric_unique_count_with_nulls(self) -> None:
        """Test unique count metric handles null values correctly."""
        df = pd.DataFrame({"col": [1, 2, None, 2, None]})
        spec = MetricSpec(name="unique_count", type="count", params={"column": "col"})
        
        result = metric_unique_count(df, spec)
        
        assert result.value == 2  # 1, 2 (nulls excluded)

    def test_metric_distribution_summary_numeric(self) -> None:
        """Test distribution summary with numeric data."""
        df = pd.DataFrame({
            "a": [1, 2, 3, 4, 5],
            "b": [10.0, 20.0, 30.0, 40.0, 50.0],
            "c": ["x", "y", "z", "x", "y"]  # non-numeric
        })
        spec = MetricSpec(name="distribution", type="summary")
        
        result = metric_distribution_summary(df, spec)
        
        assert result.name == "distribution"
        assert result.metric_type == "summary"
        assert result.value == 2  # 2 numeric columns
        
        details = result.details
        assert len(details) == 2
        assert set(details["column"]) == {"a", "b"}
        assert "mean" in details.columns
        assert "std" in details.columns

    def test_metric_distribution_summary_no_numeric(self) -> None:
        """Test distribution summary with no numeric data."""
        df = pd.DataFrame({"a": ["x", "y"], "b": ["z", "w"]})
        spec = MetricSpec(name="distribution", type="summary")
        
        result = metric_distribution_summary(df, spec)
        
        assert result.value == 0
        assert len(result.details) == 0

    def test_metric_distribution_summary_empty(self) -> None:
        """Test distribution summary with empty DataFrame."""
        df = pd.DataFrame()
        spec = MetricSpec(name="distribution", type="summary")
        
        result = metric_distribution_summary(df, spec)
        
        assert result.value == 0
        assert len(result.details) == 0

    def test_metric_pka_range_normal(self) -> None:
        """Test pKa range metric with normal data."""
        df = pd.DataFrame({"pka": [1.0, 7.0, 14.0, 8.0]})
        spec = MetricSpec(
            name="pka_range",
            type="ratio",
            params={"column": "pka", "min": 2.0, "max": 12.0}
        )
        
        result = metric_pka_range(df, spec)
        
        assert result.name == "pka_range"
        assert result.metric_type == "ratio"
        assert result.value == 0.5  # 1.0 and 14.0 are out of range (2/4)

    def test_metric_pka_range_default_params(self) -> None:
        """Test pKa range metric with default parameters."""
        df = pd.DataFrame({"pka": [7.0, 8.0]})
        spec = MetricSpec(name="pka_range", type="ratio")
        
        result = metric_pka_range(df, spec)
        
        assert result.value == 0.0  # Both within default range 0-14

    def test_metric_pka_range_missing_column(self) -> None:
        """Test pKa range metric with missing column."""
        df = pd.DataFrame({"other": [1, 2, 3]})
        spec = MetricSpec(name="pka_range", type="ratio")
        
        result = metric_pka_range(df, spec)
        
        assert result.value == 0.0
        assert "column 'pka' not present" in result.message

    def test_metric_pka_range_all_nulls(self) -> None:
        """Test pKa range metric with all null values."""
        df = pd.DataFrame({"pka": [None, None]})
        spec = MetricSpec(name="pka_range", type="ratio")
        
        result = metric_pka_range(df, spec)
        
        assert result.value == 0.0

    def test_metric_pchembl_consistency_normal(self) -> None:
        """Test pChembl consistency metric with normal data."""
        df = pd.DataFrame({
            "pchembl_value": [5.0, 6.0, 7.0],
            "standard_value": [5.1, 6.0, 7.2]
        })
        spec = MetricSpec(
            name="pchembl_consistency",
            type="ratio",
            params={"tolerance": 0.1}
        )
        
        result = metric_pchembl_consistency(df, spec)
        
        assert result.name == "pchembl_consistency"
        assert result.metric_type == "ratio"
        assert result.value == 1/3  # Only 7.2 exceeds tolerance

    def test_metric_pchembl_consistency_default_params(self) -> None:
        """Test pChembl consistency with default parameters."""
        df = pd.DataFrame({
            "pchembl_value": [5.0, 6.0],
            "standard_value": [5.0, 6.0]
        })
        spec = MetricSpec(name="pchembl_consistency", type="ratio")
        
        result = metric_pchembl_consistency(df, spec)
        
        assert result.value == 0.0  # Perfect consistency with tolerance 0

    def test_metric_pchembl_consistency_missing_columns(self) -> None:
        """Test pChembl consistency with missing columns."""
        df = pd.DataFrame({"other": [1, 2, 3]})
        spec = MetricSpec(name="pchembl_consistency", type="ratio")
        
        result = metric_pchembl_consistency(df, spec)
        
        assert result.value == 0.0
        assert "required columns missing" in result.message

    def test_metric_pchembl_consistency_no_comparable_data(self) -> None:
        """Test pChembl consistency with no comparable data."""
        df = pd.DataFrame({
            "pchembl_value": [None, None],
            "standard_value": [None, None]
        })
        spec = MetricSpec(name="pchembl_consistency", type="ratio")
        
        result = metric_pchembl_consistency(df, spec)
        
        assert result.value == 0.0


class TestDefaultRegistry:
    """Test suite for default registry."""

    def test_default_registry_populated(self) -> None:
        """Test that default registry is populated with expected metrics."""
        expected_metrics = {
            "row_count",
            "null_percentage", 
            "unique_count",
            "distribution_summary",
            "pka_range",
            "pchembl_consistency"
        }
        
        actual_metrics = set(DEFAULT_REGISTRY.list_metrics())
        
        assert expected_metrics.issubset(actual_metrics)

    def test_default_registry_metrics_callable(self) -> None:
        """Test that all registered metrics are callable."""
        for metric_name in DEFAULT_REGISTRY.list_metrics():
            metric_func = DEFAULT_REGISTRY.get(metric_name)
            assert callable(metric_func)

    def test_default_registry_metrics_work(self) -> None:
        """Test that all registered metrics work with minimal data."""
        df = pd.DataFrame({"test": [1, 2, 3]})
        
        for metric_name in DEFAULT_REGISTRY.list_metrics():
            metric_func = DEFAULT_REGISTRY.get(metric_name)
            spec = MetricSpec(name=metric_name, type="test")
            
            result = metric_func(df, spec)
            
            assert isinstance(result, QCMetricResult)
            assert result.name == metric_name
            assert result.metric_type == "test"
