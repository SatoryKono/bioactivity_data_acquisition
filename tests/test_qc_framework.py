from __future__ import annotations

from pathlib import Path

import pandas as pd

from bioetl.qc.plan import (
    QCPlan,
    QCMetricsExecutor,
    QCMetricResult,
    register_qc_metric,
)


def sample_df() -> pd.DataFrame:
    return pd.DataFrame({"a": [1, 2, 2, 100], "b": [1.0, None, 3.5, 4.5]})


def test_qc_executor_writes_reports(tmp_path: Path):
    df = sample_df()
    executor = QCMetricsExecutor()
    bundle = executor.execute(df, dataset_name="demo", output_dir=tmp_path)

    assert bundle.report_paths is not None
    assert (tmp_path / "demo_quality_report.csv").exists()
    assert (tmp_path / "demo_qc.json").exists()
    assert (tmp_path / "demo_correlation_report.csv").exists()


def test_custom_metric_registry(tmp_path: Path):
    def simple_metric(df: pd.DataFrame, ctx: object) -> QCMetricResult:  # type: ignore[arg-type]
        return QCMetricResult(name="row_count", payload=len(df))

    register_qc_metric("row_count", simple_metric, override=True)

    executor = QCMetricsExecutor()
    plan = QCPlan(custom_metrics=("row_count",))
    bundle = executor.execute(sample_df(), plan=plan, output_dir=tmp_path)

    assert "row_count" in bundle.custom
    assert bundle.custom["row_count"].payload == 4
