"""Quality control utilities."""

from __future__ import annotations

import warnings
from typing import Any

import numpy as np
import pandas as pd
from structlog.stdlib import BoundLogger

# Подавляем предупреждения о делении на ноль в NumPy
warnings.filterwarnings("ignore", category=RuntimeWarning, message="invalid value encountered in divide")

from .enhanced_correlation import (
    build_correlation_insights,
    build_enhanced_correlation_analysis,
    build_enhanced_correlation_reports,
)
from .enhanced_qc import build_enhanced_qc_detailed, build_enhanced_qc_summary


def build_qc_report(df: pd.DataFrame) -> pd.DataFrame:
    """Generate a QC report with basic completeness metrics."""

    # Определяем тип данных по наличию ключевых колонок
    if "document_chembl_id" in df.columns:
        # Это данные документов
        key_columns = ["document_chembl_id", "doi", "title"]
        available_key_columns = [col for col in key_columns if col in df.columns]

        metrics = {
            "row_count": len(df),
            "missing_document_chembl_id": (int(df["document_chembl_id"].isna().sum()) if "document_chembl_id" in df else 0),
            "missing_doi": (int(df["doi"].isna().sum()) if "doi" in df else 0),
            "missing_title": (int(df["title"].isna().sum()) if "title" in df else 0),
            "duplicates": (int(df[available_key_columns].duplicated().sum()) if available_key_columns else 0),
        }
    else:
        # Общие метрики для неизвестного типа данных
        metrics = {
            "row_count": len(df),
            "total_columns": len(df.columns),
            "duplicates": int(df.duplicated().sum()),
        }

    return pd.DataFrame([{"metric": key, "value": value} for key, value in metrics.items()])


def build_enhanced_qc_report(df: pd.DataFrame, logger: BoundLogger | None = None) -> pd.DataFrame:
    """Generate an enhanced QC report with detailed quality metrics."""
    return build_enhanced_qc_summary(df, logger=logger)


def build_enhanced_qc_detailed_reports(df: pd.DataFrame, logger: BoundLogger | None = None) -> dict[str, pd.DataFrame]:
    """Generate detailed QC reports including summary, top values, and pattern coverage."""
    return build_enhanced_qc_detailed(df, logger=logger)


def build_enhanced_correlation_analysis_report(df: pd.DataFrame, logger: BoundLogger | None = None) -> dict[str, Any]:
    """Generate enhanced correlation analysis with multiple correlation types."""
    return build_enhanced_correlation_analysis(df, logger=logger)


def build_enhanced_correlation_reports_df(df: pd.DataFrame, logger: BoundLogger | None = None) -> dict[str, pd.DataFrame]:
    """Generate enhanced correlation reports as DataFrames."""
    return build_enhanced_correlation_reports(df, logger=logger)


def build_correlation_insights_report(df: pd.DataFrame, logger: BoundLogger | None = None) -> list[dict[str, Any]]:
    """Generate correlation insights and recommendations."""
    return build_correlation_insights(df, logger=logger)


def build_correlation_matrix(df: pd.DataFrame) -> pd.DataFrame:
    """Compute correlation matrix for numeric columns."""

    numeric = df.select_dtypes(include="number")
    if numeric.empty or len(numeric) < 2:
        return pd.DataFrame()

    try:
        corr_matrix = numeric.corr()
        # Заменяем NaN и inf на 0
        return corr_matrix.fillna(0.0).replace([np.inf, -np.inf], 0.0)
    except (ValueError, TypeError, MemoryError) as e:
        # Логируем конкретные ошибки для отладки
        import warnings

        warnings.warn(f"Ошибка при вычислении корреляционной матрицы: {e}", stacklevel=2)
        return pd.DataFrame()


__all__ = [
    "build_correlation_matrix",
    "build_qc_report",
    "build_enhanced_qc_report",
    "build_enhanced_qc_detailed_reports",
    "build_enhanced_correlation_analysis_report",
    "build_enhanced_correlation_reports_df",
    "build_correlation_insights_report",
]
