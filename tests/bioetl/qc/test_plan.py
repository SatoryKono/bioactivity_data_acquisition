"""Tests for QC plan execution and custom metric registry."""

from __future__ import annotations

import pandas as pd
import pytest

from bioetl.qc.plan import (
    QC_METRIC_REGISTRY,
    QCExecutionContext,
    QCMetricResult,
    QCMetricsExecutor,
    QCPlan,
    register_qc_metric,
)
from bioetl.qc.report import build_qc_metrics_payload, build_quality_report


@pytest.mark.unit
def test_qc_plan_disables_missingness() -> None:
    """Executor should respect QCPlan toggles."""

    df = pd.DataFrame(
        {
            "id": [1, 2, 2],
            "value": [10.0, 20.0, None],
        }
    )
    plan = QCPlan(
        duplicates=True,
        missingness=False,
        units_distribution=False,
        relation_distribution=False,
        correlation=False,
        outliers=False,
    )
    executor = QCMetricsExecutor()

    bundle = executor.execute(df, plan=plan)

    assert bundle.missingness is None
    assert bundle.duplicates is not None


@pytest.mark.unit
def test_qc_plan_custom_metric_registration() -> None:
    """Registered custom metrics should feed both report and payload outputs."""

    df = pd.DataFrame(
        {
            "id": [1, 2, 3],
            "value": [1.0, 2.0, 3.0],
        }
    )
    metric_name = "test::custom_ratio"

    def _custom_metric(payload: pd.DataFrame, _: QCExecutionContext) -> QCMetricResult:
        total = int(payload.shape[0])
        rows = (
            {
                "section": "custom:test",
                "metric": "custom_ratio",
                "value": total,
            },
        )
        return QCMetricResult(
            name="custom_ratio",
            payload={"total": total},
            section="custom:test",
            rows=rows,
        )

    register_qc_metric(metric_name, _custom_metric, override=True)
    plan = QCPlan(custom_metrics=(metric_name,))
    executor = QCMetricsExecutor()

    try:
        bundle = executor.execute(df, plan=plan)
        assert "custom_ratio" in bundle.custom

        report = build_quality_report(bundle=bundle, plan=plan)
        assert (report["metric"] == "custom_ratio").any()

        payload = build_qc_metrics_payload(bundle=bundle, plan=plan)
        assert payload["custom_metrics"]["custom_ratio"]["total"] == 3
    finally:
        QC_METRIC_REGISTRY.unregister(metric_name)

