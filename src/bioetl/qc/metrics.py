"""Low-level QC metric computations."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

import pandas as pd

__all__ = [
    "compute_duplicate_stats",
    "compute_missingness",
    "compute_correlation_matrix",
]


def compute_duplicate_stats(
    df: pd.DataFrame,
    *,
    business_key_fields: Sequence[str] | None = None,
) -> dict[str, Any]:
    """Return duplicate statistics for ``df``.

    ``business_key_fields`` allows the caller to provide a logical business key
    distinct from the full row identity. When omitted the check uses the
    complete row to determine duplicates.
    """

    total_rows = int(len(df))
    if total_rows == 0:
        return {
            "row_count": 0,
            "duplicate_count": 0,
            "duplicate_ratio": 0.0,
            "deduplicated_count": 0,
        }

    subset = list(business_key_fields) if business_key_fields else None
    if subset:
        deduplicated = df.drop_duplicates(subset=subset, keep="first")
    else:
        deduplicated = df.drop_duplicates(keep="first")

    deduplicated_count = int(len(deduplicated))
    duplicate_count = total_rows - deduplicated_count
    duplicate_ratio = float(duplicate_count / total_rows) if total_rows else 0.0

    return {
        "row_count": total_rows,
        "duplicate_count": duplicate_count,
        "duplicate_ratio": duplicate_ratio,
        "deduplicated_count": deduplicated_count,
    }


def compute_missingness(df: pd.DataFrame) -> pd.DataFrame:
    """Return per-column missing value statistics."""

    if df.empty:
        return pd.DataFrame(columns=["column", "missing_count", "missing_ratio"]).astype(
            {"missing_count": "int64", "missing_ratio": "float64"}
        )

    missing_count = df.isna().sum()
    total_rows = int(len(df))
    missing_ratio = missing_count / total_rows if total_rows else 0.0

    result = pd.DataFrame(
        {
            "column": missing_count.index,
            "missing_count": missing_count.astype("int64").values,
            "missing_ratio": missing_ratio.astype("float64").values,
        }
    )
    return result.sort_values(by="missing_count", ascending=False).reset_index(drop=True)


def compute_correlation_matrix(df: pd.DataFrame) -> pd.DataFrame | None:
    """Return the numeric correlation matrix or ``None`` when not applicable."""

    numeric_df = df.select_dtypes(include=["number", "bool"])
    if numeric_df.shape[1] < 2:
        return None
    correlation = numeric_df.corr(numeric_only=True)
    if correlation.empty:
        return None
    return correlation
