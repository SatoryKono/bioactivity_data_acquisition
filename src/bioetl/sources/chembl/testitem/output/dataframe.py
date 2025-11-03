"""Output helpers for TestItem pipeline quality control."""

from __future__ import annotations

from typing import Any

import pandas as pd

from bioetl.utils.qc import duplicate_summary

__all__ = [
    "build_duplicate_summary",
    "calculate_fallback_stats",
]


def build_duplicate_summary(
    df: pd.DataFrame,
    *,
    field: str,
    initial_rows: int,
    threshold: float | None,
) -> dict[str, Any]:
    """Return QC summary for duplicate business keys."""

    duplicate_count = 0
    if field in df.columns:
        duplicate_count = int(df.duplicated(subset=[field]).sum())

    return duplicate_summary(initial_rows, duplicate_count, field=field, threshold=threshold)


def calculate_fallback_stats(
    df: pd.DataFrame,
    *,
    field: str = "fallback_error_code",
) -> tuple[int, float]:
    """Calculate fallback count/ratio for QC metrics."""

    if df.empty or field not in df.columns:
        return 0, 0.0

    fallback_count = int(df[field].notna().sum())
    fallback_ratio = fallback_count / len(df) if len(df) else 0.0
    return fallback_count, fallback_ratio
