from __future__ import annotations

from pathlib import Path
from typing import Any, Mapping

import pandas as pd


def build_quality_report(
    df: pd.DataFrame,
    metrics_results: Mapping[str, Any],
    *,
    dataset_name: str = "dataset",
) -> pd.DataFrame:
    """Build tabular quality report summarising metric outcomes."""

    rows: list[dict[str, Any]] = []
    for name, result in metrics_results.items():
        rows.append(
            {
                "dataset": dataset_name,
                "metric": name,
                "metric_type": getattr(result, "metric_type", "custom"),
                "value": getattr(result, "value", None),
                "threshold": getattr(result, "threshold", None),
                "status": getattr(result, "status", "PASS"),
                "message": getattr(result, "message", None),
            }
        )
    return pd.DataFrame(rows)


def build_correlation_report(
    df_primary: pd.DataFrame,
    df_secondary: pd.DataFrame,
    *,
    method: str = "pearson",
) -> pd.DataFrame:
    """Compute column-wise correlations between two datasets."""

    numeric_primary = df_primary.select_dtypes(include=["number"])
    numeric_secondary = df_secondary.select_dtypes(include=["number"])
    common = [col for col in numeric_primary.columns if col in numeric_secondary.columns]
    if not common:
        return pd.DataFrame(columns=["column", "correlation"])

    min_len = min(len(numeric_primary), len(numeric_secondary))
    aligned_primary = numeric_primary[common].iloc[:min_len]
    aligned_secondary = numeric_secondary[common].iloc[:min_len]
    correlations = aligned_primary.corrwith(aligned_secondary, method=method)
    return correlations.reset_index().rename(columns={"index": "column", 0: "correlation"})


def golden_test_compare(current: pd.DataFrame, golden: pd.DataFrame | Path | str) -> pd.DataFrame:
    """Compare current QC output against a golden baseline."""

    golden_df = (
        golden
        if isinstance(golden, pd.DataFrame)
        else pd.read_csv(Path(golden))
    )
    current_sorted = current.sort_index(axis=1)
    golden_sorted = golden_df.sort_index(axis=1)
    diff = current_sorted.compare(golden_sorted, keep_shape=True, keep_equal=False)
    diff = diff.dropna(axis=0, how="all").dropna(axis=1, how="all")
    return diff


__all__ = [
    "build_quality_report",
    "build_correlation_report",
    "golden_test_compare",
]
