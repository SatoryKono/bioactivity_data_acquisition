"""Common helpers for building QC reports across modules.

This module provides a minimal, consistent QC format with at least the
following metrics:
  - row_count: total number of records in the dataset
  - missing_data: mapping of column -> {missing_count, missing_percentage}

Modules can add their own domain-specific metrics on top. This helper will
merge module-specific QC rows with the common baseline and ensure a stable
shape: a two-column DataFrame with columns ["metric", "value"].
"""

from __future__ import annotations

from typing import Any

import pandas as pd


def _build_missing_data_summary(df: pd.DataFrame) -> dict[str, dict[str, float | int]]:
    summary: dict[str, dict[str, float | int]] = {}
    total = len(df)
    if total <= 0:
        return summary
    for column_name in df.columns:
        missing_count = int(df[column_name].isna().sum())
        if missing_count > 0:
            summary[column_name] = {
                "missing_count": missing_count,
                "missing_percentage": float(missing_count / total * 100.0),
            }
    return summary


def ensure_common_qc(data_frame: pd.DataFrame, qc_frame: pd.DataFrame | None, _module_name: str) -> pd.DataFrame:
    """Return QC DataFrame in a unified two-column format.

    Args:
        data_frame: Source data used to compute row_count/missing_data.
        qc_frame: Optional existing QC metrics to merge.
        module_name: Logical module name (activity/assay/target/testitem/documents).

    Returns:
        A DataFrame with columns ["metric", "value"].
    """
    rows: list[dict[str, Any]] = []

    # Baseline metrics (always present)
    rows.append({"metric": "row_count", "value": int(len(data_frame))})

    missing = _build_missing_data_summary(data_frame)
    if missing:
        rows.append({"metric": "missing_data", "value": missing})

    # Merge with existing QC (if any)
    if isinstance(qc_frame, pd.DataFrame) and not qc_frame.empty:
        # Normalize to metric/value columns if needed
        if set(qc_frame.columns) == {"metric", "value"}:
            merged = pd.DataFrame(rows)
            qc_normalized = qc_frame.copy()
        else:
            # Fallback: flatten any simple key/value-like structures
            qc_normalized = pd.DataFrame()
            try:
                if "metric" in qc_frame.columns and "value" in qc_frame.columns:
                    qc_normalized = qc_frame[["metric", "value"]].copy()
                else:
                    # Attempt to coerce by stacking non-numeric summaries
                    tmp_rows: list[dict[str, Any]] = []
                    for col in qc_frame.columns:
                        tmp_rows.append({"metric": str(col), "value": qc_frame[col].tolist()})
                    qc_normalized = pd.DataFrame(tmp_rows)
            except Exception:
                qc_normalized = pd.DataFrame()

        merged = pd.concat([pd.DataFrame(rows), qc_normalized], ignore_index=True)
        # Drop exact duplicates just in case
        merged = merged.drop_duplicates(subset=["metric"]).reset_index(drop=True)
        return merged

    # No existing QC provided → return baseline only
    return pd.DataFrame(rows)


__all__ = [
    "ensure_common_qc",
]


