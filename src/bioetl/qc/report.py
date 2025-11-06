"""Composite QC report builders."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any, Mapping

import pandas as pd

from .metrics import compute_correlation_matrix, compute_duplicate_stats, compute_missingness

__all__ = [
    "build_quality_report",
    "build_correlation_report",
    "build_qc_metrics_payload",
    "compute_units_distribution",
    "compute_relation_distribution",
    "detect_simple_outliers",
]


def _normalize_value(value: Any) -> str:
    """Return a human-readable representation for categorical values."""

    if pd.isna(value):
        return "null"
    return str(value)


def compute_units_distribution(df: pd.DataFrame) -> dict[str, dict[str, dict[str, float | int]]]:
    """Return value counts for all ``*_units`` columns in ``df``."""

    distributions: dict[str, dict[str, dict[str, float | int]]] = {}
    unit_columns = [column for column in df.columns if column.endswith("units")]
    for column in unit_columns:
        series = df[column]
        # Skip entirely empty columns to avoid noisy metrics
        if series.notna().sum() == 0:
            continue
        value_counts = series.astype("object").value_counts(dropna=False)
        total = int(value_counts.sum())
        distributions[column] = {
            _normalize_value(value): {
                "count": int(count),
                "ratio": float(count / total) if total else 0.0,
            }
            for value, count in value_counts.items()
        }
    return distributions


def compute_relation_distribution(df: pd.DataFrame) -> dict[str, dict[str, dict[str, float | int]]]:
    """Return value counts for all ``*_relation`` columns in ``df``."""

    distributions: dict[str, dict[str, dict[str, float | int]]] = {}
    relation_columns = [column for column in df.columns if column.endswith("relation")]
    for column in relation_columns:
        series = df[column]
        if series.notna().sum() == 0:
            continue
        value_counts = series.astype("object").value_counts(dropna=False)
        total = int(value_counts.sum())
        distributions[column] = {
            _normalize_value(value): {
                "count": int(count),
                "ratio": float(count / total) if total else 0.0,
            }
            for value, count in value_counts.items()
        }
    return distributions


def detect_simple_outliers(df: pd.DataFrame) -> dict[str, dict[str, float | int]]:
    """Return IQR-based outlier counts for numeric columns in ``df``."""

    outliers: dict[str, dict[str, float | int]] = {}
    numeric_df = df.select_dtypes(include=["number"])
    for column in numeric_df.columns:
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


def build_quality_report(
    df: pd.DataFrame,
    *,
    business_key_fields: Sequence[str] | None = None,
) -> pd.DataFrame:
    """Return a tabular quality report with duplicate and missingness stats."""

    duplicates = compute_duplicate_stats(df, business_key_fields=business_key_fields)
    rows: list[dict[str, Any]] = [
        {"section": "summary", "metric": key, "value": value}
        for key, value in duplicates.items()
    ]

    missing = compute_missingness(df)
    if not missing.empty:
        for _, record in missing.iterrows():
            rows.append(
                {
                    "section": "missing",  # column-level metrics
                    "metric": "missing_count",
                    "column": record["column"],
                    "value": int(record["missing_count"]),
                    "ratio": float(record["missing_ratio"]),
                }
            )

    for column, distribution in compute_units_distribution(df).items():
        for value, info in distribution.items():
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

    for column, distribution in compute_relation_distribution(df).items():
        for value, info in distribution.items():
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

    for column, info in detect_simple_outliers(df).items():
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
    return pd.DataFrame(rows)


def build_correlation_report(df: pd.DataFrame) -> pd.DataFrame | None:
    """Return a correlation matrix suitable for persistence."""

    correlation = compute_correlation_matrix(df)
    if correlation is None:
        return None
    return correlation.reset_index(names="feature").rename_axis(columns=None)


def build_qc_metrics_payload(
    df: pd.DataFrame,
    *,
    business_key_fields: Sequence[str] | None = None,
) -> Mapping[str, Any]:
    """Return a flattened mapping of QC metrics for metadata and persistence."""

    duplicates = compute_duplicate_stats(df, business_key_fields=business_key_fields)
    missing = compute_missingness(df)
    columns_with_missing = (
        missing.loc[missing["missing_count"] > 0, "column"].astype(str).tolist()
        if not missing.empty
        else []
    )
    total_missing = int(missing["missing_count"].sum()) if not missing.empty else 0

    units_distribution = compute_units_distribution(df)
    relation_distribution = compute_relation_distribution(df)
    numeric_outliers = detect_simple_outliers(df)

    payload: dict[str, Any] = {
        **duplicates,
        "total_missing_values": total_missing,
        "columns_with_missing": ", ".join(sorted(columns_with_missing)),
        "units_distribution": units_distribution,
        "relation_distribution": relation_distribution,
        "iqr_outliers": numeric_outliers,
    }
    if business_key_fields:
        payload["business_key_fields"] = list(business_key_fields)
    return payload
