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
]


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

    payload: dict[str, Any] = {
        **duplicates,
        "total_missing_values": total_missing,
        "columns_with_missing": ", ".join(sorted(columns_with_missing)),
    }
    if business_key_fields:
        payload["business_key_fields"] = list(business_key_fields)
    return payload
