"""Quality control utilities."""

from __future__ import annotations

import pandas as pd
from structlog.stdlib import BoundLogger
from typing import Optional

from .enhanced_qc import build_enhanced_qc_summary, build_enhanced_qc_detailed
from .enhanced_correlation import (
    build_enhanced_correlation_analysis,
    build_enhanced_correlation_reports,
    build_correlation_insights
)


def build_qc_report(df: pd.DataFrame) -> pd.DataFrame:
    """Generate a QC report with basic completeness metrics."""

    # Проверяем дубликаты только по основным колонкам (избегаем нехешируемых типов)
    key_columns = ["compound_id", "target", "activity_value", "source"]
    available_key_columns = [col for col in key_columns if col in df.columns]
    
    metrics = {
        "row_count": len(df),
        "missing_compound_id": int(df["compound_id"].isna().sum()) if "compound_id" in df else 0,
        "missing_target": int(df["target"].isna().sum()) if "target" in df else 0,
        "duplicates": int(df[available_key_columns].duplicated().sum()) if available_key_columns else 0,
    }
    return pd.DataFrame(
        [{"metric": key, "value": value} for key, value in metrics.items()]
    )


def build_enhanced_qc_report(df: pd.DataFrame, logger: Optional[BoundLogger] = None) -> pd.DataFrame:
    """Generate an enhanced QC report with detailed quality metrics."""
    return build_enhanced_qc_summary(df, logger=logger)


def build_enhanced_qc_detailed_reports(df: pd.DataFrame, logger: Optional[BoundLogger] = None) -> dict[str, pd.DataFrame]:
    """Generate detailed QC reports including summary, top values, and pattern coverage."""
    return build_enhanced_qc_detailed(df, logger=logger)


def build_enhanced_correlation_analysis_report(df: pd.DataFrame, logger: Optional[BoundLogger] = None) -> dict[str, Any]:
    """Generate enhanced correlation analysis with multiple correlation types."""
    return build_enhanced_correlation_analysis(df, logger=logger)


def build_enhanced_correlation_reports_df(df: pd.DataFrame, logger: Optional[BoundLogger] = None) -> dict[str, pd.DataFrame]:
    """Generate enhanced correlation reports as DataFrames."""
    return build_enhanced_correlation_reports(df, logger=logger)


def build_correlation_insights_report(df: pd.DataFrame, logger: Optional[BoundLogger] = None) -> list[dict[str, Any]]:
    """Generate correlation insights and recommendations."""
    return build_correlation_insights(df, logger=logger)


def build_correlation_matrix(df: pd.DataFrame) -> pd.DataFrame:
    """Compute correlation matrix for numeric columns."""

    numeric = df.select_dtypes(include="number")
    if numeric.empty:
        return pd.DataFrame()
    return numeric.corr().fillna(0.0)


__all__ = [
    "build_correlation_matrix", 
    "build_qc_report", 
    "build_enhanced_qc_report", 
    "build_enhanced_qc_detailed_reports",
    "build_enhanced_correlation_analysis_report",
    "build_enhanced_correlation_reports_df",
    "build_correlation_insights_report"
]
