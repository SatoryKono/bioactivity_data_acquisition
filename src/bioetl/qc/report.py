"""Composite QC report builders."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any, Literal, cast

import pandas as pd

from bioetl.core.qc.units import QCUnits

from .metrics import (
    DuplicateStats,
    compute_correlation_matrix,
    compute_duplicate_stats,
    compute_missingness,
)

__all__ = [
    "build_quality_report",
    "build_correlation_report",
    "build_qc_metrics_payload",
]


def _detect_simple_outliers(df: pd.DataFrame) -> dict[str, dict[str, float | int]]:
    """Return IQR-based outlier counts for numeric columns in ``df``."""

    outliers: dict[str, dict[str, float | int]] = {}
    numeric_df = df.select_dtypes(include=["number"])
    for column in sorted(numeric_df.columns):
        series = numeric_df[column].dropna()
        if series.empty:
            continue

        q1 = series.quantile(0.25)
        q3 = series.quantile(0.75)
        iqr = float(q3 - q1)
        if iqr == 0:
            continue

        lower_bound = float(q1 - 1.5 * iqr)
        upper_bound = float(q3 + 1.5 * iqr)
        mask = (series < lower_bound) | (series > upper_bound)
        count = int(mask.sum())
        if count == 0:
            continue

        outliers[column] = {
            "count": count,
            "lower_bound": lower_bound,
            "upper_bound": upper_bound,
        }
    return outliers


SummaryMetricKey = Literal[
    "row_count",
    "deduplicated_count",
    "duplicate_count",
    "duplicate_ratio",
]


def build_quality_report(
    df: pd.DataFrame,
    *,
    business_key_fields: Sequence[str] | None = None,
) -> pd.DataFrame:
    """Return a deterministic tabular quality report for downstream persistence."""

    duplicates = compute_duplicate_stats(df, business_key_fields=business_key_fields)
    rows: list[dict[str, Any]] = []

    summary_order: tuple[SummaryMetricKey, ...] = (
        "row_count",
        "deduplicated_count",
        "duplicate_count",
        "duplicate_ratio",
    )
    for metric_name in summary_order:
        metric_value = duplicates[metric_name]
        summary_row: dict[str, Any] = {"section": "summary", "metric": metric_name}
        if isinstance(metric_value, float):
            summary_row["ratio"] = float(metric_value)
        else:
            summary_row["count"] = int(metric_value)
        rows.append(summary_row)

    missing = compute_missingness(df)
    if not missing.empty:
        missing_records = cast(Sequence[Mapping[str, Any]], missing.to_dict(orient="records"))
        for record in missing_records:
            rows.append(
                {
                    "section": "missing",  # column-level metrics
                    "metric": "missing_count",
                    "column": str(record["column"]),
                    "count": int(record["missing_count"]),
                    "ratio": float(record["missing_ratio"]),
                }
            )

    units_distribution = QCUnits.for_units(df)
    for column in sorted(units_distribution.keys()):
        for value, info in units_distribution[column].items():
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

    relation_distribution = QCUnits.for_relation(df)
    for column in sorted(relation_distribution.keys()):
        for value, info in relation_distribution[column].items():
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

    for column, info in _detect_simple_outliers(df).items():
        rows.append(
            {
                "section": "outliers",
                "metric": "iqr_outlier_count",
                "column": column,
                "count": info["count"],
                "lower_bound": info["lower_bound"],
                "upper_bound": info["upper_bound"],
            }
        )
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


def build_correlation_report(df: pd.DataFrame) -> pd.DataFrame | None:
    """Return a correlation matrix suitable for deterministic persistence."""

    correlation = compute_correlation_matrix(df)
    if correlation is None:
        return None
    correlation = correlation.reset_index(names="feature").rename_axis(columns=None)
    ordered_columns = ["feature"] + [column for column in correlation.columns if column != "feature"]
    return correlation.loc[:, ordered_columns]


def build_qc_metrics_payload(
    df: pd.DataFrame,
    *,
    business_key_fields: Sequence[str] | None = None,
) -> Mapping[str, Any]:
    """Return a flattened, deterministic mapping of QC metrics."""

    duplicates: DuplicateStats = compute_duplicate_stats(
        df,
        business_key_fields=business_key_fields,
    )
    missing = compute_missingness(df)
    columns_with_missing = (
        missing.loc[missing["missing_count"] > 0, "column"].astype(str).tolist()
        if not missing.empty
        else []
    )
    total_missing = int(missing["missing_count"].sum()) if not missing.empty else 0

    units_distribution = QCUnits.for_units(df)
    relation_distribution = QCUnits.for_relation(df)
    numeric_outliers = _detect_simple_outliers(df)

    payload: dict[str, Any] = {
        "row_count": duplicates["row_count"],
        "duplicate_count": duplicates["duplicate_count"],
        "duplicate_ratio": duplicates["duplicate_ratio"],
        "deduplicated_count": duplicates["deduplicated_count"],
        "total_missing_values": total_missing,
        "columns_with_missing": ", ".join(sorted(columns_with_missing)),
        "units_distribution": {
            column: distributions
            for column, distributions in sorted(units_distribution.items(), key=lambda item: item[0])
        },
        "relation_distribution": {
            column: distributions
            for column, distributions in sorted(relation_distribution.items(), key=lambda item: item[0])
        },
        "iqr_outliers": {
            column: numeric_outliers[column] for column in sorted(numeric_outliers.keys())
        },
    }
    if business_key_fields:
        payload["business_key_fields"] = list(business_key_fields)
    return payload
