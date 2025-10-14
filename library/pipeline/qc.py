"""Quality control utilities."""

from __future__ import annotations

import pandas as pd


def build_qc_report(df: pd.DataFrame) -> pd.DataFrame:
    """Generate a QC report with basic completeness metrics."""

    metrics = {
        "row_count": len(df),
        "missing_compound_id": int(df["compound_id"].isna().sum()) if "compound_id" in df else 0,
        "missing_target": int(df["target"].isna().sum()) if "target" in df else 0,
        "duplicates": int(df.duplicated().sum()),
    }
    return pd.DataFrame(
        [{"metric": key, "value": value} for key, value in metrics.items()]
    )


def build_correlation_matrix(df: pd.DataFrame) -> pd.DataFrame:
    """Compute correlation matrix for numeric columns."""

    numeric = df.select_dtypes(include="number")
    if numeric.empty:
        return pd.DataFrame()
    return numeric.corr().fillna(0.0)


__all__ = ["build_correlation_matrix", "build_qc_report"]
