"""Composite QC report builders."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any, Literal

import pandas as pd

from bioetl.qc.plan import (
    QCPlan,
    QC_PLAN_DEFAULT,
    QCMetricsBundle,
    QCMetricsExecutor,
)

__all__ = [
    "build_quality_report",
    "build_correlation_report",
    "build_qc_metrics_payload",
]


SummaryMetricKey = Literal[
    "row_count",
    "deduplicated_count",
    "duplicate_count",
    "duplicate_ratio",
]


def build_quality_report(
    df: pd.DataFrame | None = None,
    *,
    business_key_fields: Sequence[str] | None = None,
    plan: QCPlan | None = None,
    bundle: QCMetricsBundle | None = None,
    executor: QCMetricsExecutor | None = None,
) -> pd.DataFrame:
    """Return a deterministic tabular quality report for downstream persistence."""

    metrics_bundle, _ = _ensure_bundle(
        df,
        bundle=bundle,
        plan=plan,
        executor=executor,
        business_key_fields=business_key_fields,
    )
    rows: list[dict[str, Any]] = []

    summary_order: tuple[SummaryMetricKey, ...] = (
        "row_count",
        "deduplicated_count",
        "duplicate_count",
        "duplicate_ratio",
    )

    if metrics_bundle.duplicates is not None:
        duplicates = metrics_bundle.duplicates
        for metric_name in summary_order:
            metric_value = getattr(duplicates, metric_name)
            summary_row: dict[str, Any] = {"section": "summary", "metric": metric_name}
            if isinstance(metric_value, float):
                summary_row["ratio"] = float(metric_value)
            else:
                summary_row["count"] = int(metric_value)
            rows.append(summary_row)

    missing = metrics_bundle.missingness
    if missing is not None and not missing.table.empty:
        missing_records = missing.table.to_dict(orient="records")
        for record in missing_records:
            rows.append(
                {
                    "section": "missing",
                    "metric": "missing_count",
                    "column": str(record["column"]),
                    "count": int(record["missing_count"]),
                    "ratio": float(record["missing_ratio"]),
                }
            )

    for column, distributions in sorted(
        (metrics_bundle.units_distribution or {}).items(),
        key=lambda item: item[0],
    ):
        for value, info in distributions.items():
            rows.append(
                {
                    "section": "distribution",
                    "metric": "units_distribution",
                    "column": column,
                    "value": value,
                    "count": info["count"],
                    "ratio": info["ratio"],
                }
            )

    for column, distributions in sorted(
        (metrics_bundle.relation_distribution or {}).items(),
        key=lambda item: item[0],
    ):
        for value, info in distributions.items():
            rows.append(
                {
                    "section": "distribution",
                    "metric": "relation_distribution",
                    "column": column,
                    "value": value,
                    "count": info["count"],
                    "ratio": info["ratio"],
                }
            )

    for outlier in metrics_bundle.outliers:
        rows.append(
            {
                "section": "outliers",
                "metric": "iqr_outlier_count",
                "column": outlier.column,
                "count": outlier.count,
                "lower_bound": outlier.lower_bound,
                "upper_bound": outlier.upper_bound,
            }
        )

    if metrics_bundle.custom:
        for name in sorted(metrics_bundle.custom.keys()):
            result = metrics_bundle.custom[name]
            for row in result.rows:
                normalized = dict(row)
                normalized.setdefault("section", result.section)
                normalized.setdefault("metric", name)
                rows.append(normalized)

    quality_df = pd.DataFrame(rows, dtype="object")
    ordered_columns = [
        "section",
        "metric",
        "column",
        "value",
        "count",
        "ratio",
        "lower_bound",
        "upper_bound",
    ]
    quality_df = quality_df.reindex(columns=ordered_columns)
    return quality_df.reset_index(drop=True)


def build_correlation_report(
    df: pd.DataFrame | None = None,
    *,
    business_key_fields: Sequence[str] | None = None,
    plan: QCPlan | None = None,
    bundle: QCMetricsBundle | None = None,
    executor: QCMetricsExecutor | None = None,
) -> pd.DataFrame | None:
    """Return a correlation matrix suitable for deterministic persistence."""

    metrics_bundle, _ = _ensure_bundle(
        df,
        bundle=bundle,
        plan=plan,
        executor=executor,
        business_key_fields=business_key_fields,
    )
    correlation = metrics_bundle.correlation
    if correlation is None:
        return None
    correlation_df = correlation.table.reset_index(names="feature").rename_axis(columns=None)
    ordered_columns = ["feature"] + [
        column for column in correlation_df.columns if column != "feature"
    ]
    return correlation_df.loc[:, ordered_columns]


def build_qc_metrics_payload(
    df: pd.DataFrame | None = None,
    *,
    business_key_fields: Sequence[str] | None = None,
    plan: QCPlan | None = None,
    bundle: QCMetricsBundle | None = None,
    executor: QCMetricsExecutor | None = None,
) -> Mapping[str, Any]:
    """Return a flattened, deterministic mapping of QC metrics."""

    metrics_bundle, _ = _ensure_bundle(
        df,
        bundle=bundle,
        plan=plan,
        executor=executor,
        business_key_fields=business_key_fields,
    )

    payload: dict[str, Any] = {}
    duplicates = metrics_bundle.duplicates
    if duplicates is not None:
        payload.update(
            {
                "row_count": duplicates.row_count,
                "duplicate_count": duplicates.duplicate_count,
                "duplicate_ratio": duplicates.duplicate_ratio,
                "deduplicated_count": duplicates.deduplicated_count,
            }
        )

    missing = metrics_bundle.missingness
    if missing is not None:
        payload["total_missing_values"] = missing.total_missing()
        payload["columns_with_missing"] = ", ".join(missing.columns_with_missing())
    else:
        payload["total_missing_values"] = 0
        payload["columns_with_missing"] = ""

    if metrics_bundle.units_distribution is not None:
        payload["units_distribution"] = {
            column: distributions
            for column, distributions in sorted(
                metrics_bundle.units_distribution.items(),
                key=lambda item: item[0],
            )
        }
    else:
        payload["units_distribution"] = {}

    if metrics_bundle.relation_distribution is not None:
        payload["relation_distribution"] = {
            column: distributions
            for column, distributions in sorted(
                metrics_bundle.relation_distribution.items(),
                key=lambda item: item[0],
            )
        }
    else:
        payload["relation_distribution"] = {}

    if metrics_bundle.outliers:
        payload["iqr_outliers"] = {
            entry.column: {
                "count": entry.count,
                "lower_bound": entry.lower_bound,
                "upper_bound": entry.upper_bound,
            }
            for entry in sorted(metrics_bundle.outliers, key=lambda item: item.column)
        }
    else:
        payload["iqr_outliers"] = {}

    if metrics_bundle.business_key_fields:
        payload["business_key_fields"] = list(metrics_bundle.business_key_fields)

    if metrics_bundle.custom:
        payload["custom_metrics"] = {
            name: metrics_bundle.custom[name].payload
            for name in sorted(metrics_bundle.custom.keys())
        }

    return payload


def _ensure_bundle(
    df: pd.DataFrame | None,
    *,
    bundle: QCMetricsBundle | None,
    plan: QCPlan | None,
    executor: QCMetricsExecutor | None,
    business_key_fields: Sequence[str] | None = None,
) -> tuple[QCMetricsBundle, QCPlan]:
    if bundle is None:
        if df is None:
            msg = "df or bundle must be provided"
            raise ValueError(msg)
        effective_plan = plan or QC_PLAN_DEFAULT
        effective_executor = executor or QCMetricsExecutor()
        computed = effective_executor.execute(
            df,
            plan=effective_plan,
            business_key_fields=business_key_fields,
        )
        return computed, effective_plan
    effective_plan = plan or QC_PLAN_DEFAULT
    return bundle, effective_plan
