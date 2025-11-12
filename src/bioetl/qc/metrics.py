"""Low-level QC metric computations."""

from __future__ import annotations

from collections.abc import Sequence
from typing import TypedDict

import pandas as pd


def _string_dtype_repr(self: pd.StringDtype) -> str:
    """Patch pandas ``StringDtype`` to expose storage in its textual form."""

    storage = getattr(self, "storage", None)
    normalized = storage or getattr(self, "_storage", None)
    if normalized:
        return f"string[{normalized}]"
    return "string"


# Ensure deterministic ``string[python]`` representation even on older pandas.
pd.StringDtype.__repr__ = _string_dtype_repr  # type: ignore[assignment]
pd.StringDtype.__str__ = _string_dtype_repr  # type: ignore[assignment]

__all__ = [
    "compute_duplicate_stats",
    "compute_missingness",
    "compute_correlation_matrix",
]


class DuplicateStats(TypedDict):
    """Structured duplicate statistics returned by :func:`compute_duplicate_stats`."""

    row_count: int
    duplicate_count: int
    duplicate_ratio: float
    deduplicated_count: int


def compute_duplicate_stats(
    df: pd.DataFrame,
    *,
    business_key_fields: Sequence[str] | None = None,
) -> DuplicateStats:
    """Return deterministic duplicate statistics for ``df``.

    ``business_key_fields`` allows the caller to provide a logical business key
    distinct from the full row identity. When omitted the check uses the
    complete row to determine duplicates.
    """

    total_rows = int(len(df))
    if total_rows == 0:
        return DuplicateStats(
            row_count=0,
            duplicate_count=0,
            duplicate_ratio=0.0,
            deduplicated_count=0,
        )

    subset = list(business_key_fields) if business_key_fields else None
    if subset:
        deduplicated = df.drop_duplicates(subset=subset, keep="first")
    else:
        deduplicated = df.drop_duplicates(keep="first")

    deduplicated_count = int(len(deduplicated))
    duplicate_count = total_rows - deduplicated_count
    duplicate_ratio = float(duplicate_count / total_rows) if total_rows else 0.0

    return DuplicateStats(
        row_count=total_rows,
        duplicate_count=duplicate_count,
        duplicate_ratio=duplicate_ratio,
        deduplicated_count=deduplicated_count,
    )


def compute_missingness(df: pd.DataFrame) -> pd.DataFrame:
    """Return per-column missing value statistics with stable ordering."""

    if df.empty:
        result = pd.DataFrame(
            {
                "column": pd.Series(dtype="string[python]"),
                "missing_count": pd.Series(dtype="int64"),
                "missing_ratio": pd.Series(dtype="float64"),
            }
        )
        return result.astype(
            {
                "column": "string[python]",
                "missing_count": "int64",
                "missing_ratio": "float64",
            }
        )

    column_dtype = pd.StringDtype(storage="python")

    missing_count = df.isna().sum()
    total_rows = int(len(df))
    missing_count_int = missing_count.astype("int64")
    missing_ratio = (missing_count / total_rows).astype("float64")

    column_labels: list[str] = [str(label) for label in missing_count.index]
    column_array = pd.array(column_labels, dtype=column_dtype)

    result = pd.DataFrame(
        {
            "column": column_array,
            "missing_count": pd.Series(missing_count_int.values, dtype="int64"),
            "missing_ratio": pd.Series(missing_ratio.values, dtype="float64"),
        }
    )
    result = result.astype(
        {
            "column": column_dtype,
            "missing_count": "int64",
            "missing_ratio": "float64",
        }
    )
    result = result.sort_values(
        by=["missing_count", "column"],
        ascending=[False, True],
        kind="mergesort",
    ).reset_index(drop=True)
    return result


def compute_correlation_matrix(df: pd.DataFrame) -> pd.DataFrame | None:
    """Return the numeric correlation matrix or ``None`` when not applicable.

    Columns are sorted alphabetically to guarantee deterministic output.
    """

    numeric_df = df.select_dtypes(include=["number", "bool"])
    if numeric_df.shape[1] < 2:
        return None
    ordered_columns = sorted(numeric_df.columns)
    numeric_df = numeric_df.loc[:, ordered_columns]
    correlation = numeric_df.corr(numeric_only=True)
    if correlation.empty:
        return None
    return correlation.reindex(index=ordered_columns, columns=ordered_columns)
