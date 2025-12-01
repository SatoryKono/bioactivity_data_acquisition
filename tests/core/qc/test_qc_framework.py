from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from bioetl.qc.executor import QCMetricsExecutor
from bioetl.qc.metrics import QCFailureException, metric_null_percentage
from bioetl.qc.plan import MetricSpec, QCPlan
from bioetl.qc.report import build_correlation_report, golden_test_compare


def sample_df() -> pd.DataFrame:
    return pd.DataFrame({"a": [1, 2, 2, 100], "b": [1.0, None, 3.5, 4.5]})


def test_null_percentage_metric() -> None:
    df = sample_df()
    spec = MetricSpec(name="nulls", type="null_percentage")
    result = metric_null_percentage(df, spec)

    assert result.value == pytest.approx(0.25)
    assert not result.details.empty
    assert set(result.details.columns) == {"column", "null_ratio"}


def test_executor_threshold_violation() -> None:
    plan = QCPlan(
        metrics=[MetricSpec(name="rows", type="row_count")],
        thresholds={"rows": 2},
        fail_on_threshold_violation=True,
    )
    executor = QCMetricsExecutor()

    with pytest.raises(QCFailureException):
        executor.execute(sample_df(), plan)


def test_quality_and_correlation_reports(tmp_path: Path) -> None:
    df = sample_df()
    plan = QCPlan(metrics=[MetricSpec(name="rows", type="row_count")])
    executor = QCMetricsExecutor()

    quality_report, payload = executor.execute(df, plan, dataset_name="demo")

    assert not quality_report.empty
    assert set(quality_report.columns) >= {"metric", "status", "dataset"}
    assert "rows" in payload

    correlation = build_correlation_report(df, df.copy())
    assert not correlation.empty
    assert set(correlation.columns) == {"column", "correlation"}

    diff = golden_test_compare(quality_report, quality_report.copy())
    assert diff.empty
