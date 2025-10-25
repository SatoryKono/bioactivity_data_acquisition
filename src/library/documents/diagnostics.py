"""Модуль для диагностики и анализа пустых колонок в документах."""

from __future__ import annotations

import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd

logger = logging.getLogger(__name__)


class DocumentDiagnostics:
    """Класс для анализа и диагностики пустых колонок в документах."""

    def __init__(self, output_dir: str = "data/output/documents"):
        """Инициализация диагностики.

        Args:
            output_dir: Директория для сохранения отчетов
        """
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def analyze_dataframe(self, df: pd.DataFrame) -> dict[str, Any]:
        """Анализирует DataFrame на пустые колонки и генерирует отчет.

        Args:
            df: DataFrame для анализа

        Returns:
            Словарь с результатами анализа
        """
        logger.info(f"Analyzing DataFrame with {len(df)} records and {len(df.columns)} columns")

        # Анализ заполненности колонок
        column_analysis = {}
        for col in df.columns:
            total_count = len(df)
            non_null_count = df[col].notna().sum()
            non_empty_count = (df[col].notna() & (df[col] != "") & (df[col] != "nan")).sum()

            column_analysis[col] = {
                "total_records": total_count,
                "non_null_count": int(non_null_count),
                "non_empty_count": int(non_empty_count),
                "null_percentage": round((total_count - non_null_count) / total_count * 100, 2),
                "empty_percentage": round((total_count - non_empty_count) / total_count * 100, 2),
                "fill_rate": round(non_empty_count / total_count * 100, 2),
            }

        # Анализ ошибок по источникам
        error_analysis = self._analyze_errors(df)

        # Анализ источников данных
        source_analysis = self._analyze_sources(df)

        # Создаем отчет
        report = {
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "summary": {
                "total_records": len(df),
                "total_columns": len(df.columns),
                "columns_with_data": sum(1 for col_analysis in column_analysis.values() if col_analysis["fill_rate"] > 0),
                "columns_empty": sum(1 for col_analysis in column_analysis.values() if col_analysis["fill_rate"] == 0),
            },
            "column_analysis": column_analysis,
            "error_analysis": error_analysis,
            "source_analysis": source_analysis,
            "recommendations": self._generate_recommendations(column_analysis, error_analysis),
        }

        return report

    def _analyze_errors(self, df: pd.DataFrame) -> dict[str, Any]:
        """Анализирует ошибки по источникам."""
        error_columns = [col for col in df.columns if col.endswith("_error")]
        error_analysis = {}

        for error_col in error_columns:
            source_name = error_col.replace("_error", "")
            error_records = df[df[error_col].notna() & (df[error_col] != "")]

            if len(error_records) > 0:
                # Категоризируем ошибки
                error_categories = {}
                for error_msg in error_records[error_col].unique():
                    if pd.isna(error_msg) or error_msg == "":
                        continue

                    error_msg_str = str(error_msg).lower()
                    if "404" in error_msg_str or "not found" in error_msg_str:
                        category = "not_found"
                    elif "timeout" in error_msg_str:
                        category = "timeout"
                    elif "rate limit" in error_msg_str or "429" in error_msg_str:
                        category = "rate_limit"
                    elif "parse" in error_msg_str or "json" in error_msg_str:
                        category = "parse_error"
                    else:
                        category = "other"

                    error_categories[category] = error_categories.get(category, 0) + 1

                error_analysis[source_name] = {
                    "total_errors": len(error_records),
                    "error_percentage": round(len(error_records) / len(df) * 100, 2),
                    "error_categories": error_categories,
                }

        return error_analysis

    def _analyze_sources(self, df: pd.DataFrame) -> dict[str, Any]:
        """Анализирует заполненность данных по источникам."""
        sources = ["chembl", "crossref", "openalex", "pubmed", "semantic_scholar"]
        source_analysis = {}

        for source in sources:
            # Находим колонки этого источника
            source_columns = [col for col in df.columns if col.startswith(f"{source}_") and not col.endswith("_error")]

            if source_columns:
                # Анализируем заполненность
                source_data_count = 0
                for col in source_columns:
                    non_empty_count = (df[col].notna() & (df[col] != "") & (df[col] != "nan")).sum()
                    if non_empty_count > 0:
                        source_data_count += 1

                source_analysis[source] = {
                    "total_columns": len(source_columns),
                    "columns_with_data": source_data_count,
                    "data_percentage": round(source_data_count / len(source_columns) * 100, 2),
                }

        return source_analysis

    def _generate_recommendations(self, column_analysis: dict, error_analysis: dict) -> list[str]:
        """Генерирует рекомендации на основе анализа."""
        recommendations = []

        # Анализ пустых колонок
        empty_columns = [col for col, analysis in column_analysis.items() if analysis["fill_rate"] == 0]
        if empty_columns:
            recommendations.append(f"Колонки полностью пустые: {', '.join(empty_columns[:5])}{'...' if len(empty_columns) > 5 else ''}")

        # Анализ ошибок
        for source, errors in error_analysis.items():
            if errors["error_percentage"] > 50:
                recommendations.append(f"Высокий процент ошибок для {source}: {errors['error_percentage']}%")

            if "rate_limit" in errors["error_categories"]:
                recommendations.append(f"Обнаружены rate limit ошибки для {source}. Рассмотрите увеличение задержек между запросами.")

            if "not_found" in errors["error_categories"]:
                recommendations.append(f"Много 404 ошибок для {source}. Проверьте корректность идентификаторов.")

        return recommendations

    def save_report(self, report: dict[str, Any], filename: str | None = None) -> Path:
        """Сохраняет отчет в JSON файл.

        Args:
            report: Отчет для сохранения
            filename: Имя файла (если None, генерируется автоматически)

        Returns:
            Путь к сохраненному файлу
        """
        if filename is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"diagnostics_{timestamp}.json"

        file_path = self.output_dir / filename

        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(report, f, indent=2, ensure_ascii=False)

        logger.info(f"Diagnostic report saved to {file_path}")
        return file_path

    def generate_summary_report(self, df: pd.DataFrame) -> str:
        """Генерирует краткий текстовый отчет.

        Args:
            df: DataFrame для анализа

        Returns:
            Краткий отчет в виде строки
        """
        total_records = len(df)
        total_columns = len(df.columns)

        # Подсчитываем колонки с данными
        columns_with_data = 0
        for col in df.columns:
            non_empty_count = (df[col].notna() & (df[col] != "") & (df[col] != "nan")).sum()
            if non_empty_count > 0:
                columns_with_data += 1

        # Анализируем ошибки
        error_columns = [col for col in df.columns if col.endswith("_error")]
        total_errors = 0
        for error_col in error_columns:
            error_count = (df[error_col].notna() & (df[error_col] != "")).sum()
            total_errors += error_count

        summary = f"""
=== DIAGNOSTIC SUMMARY ===
Total records: {total_records}
Total columns: {total_columns}
Columns with data: {columns_with_data}
Empty columns: {total_columns - columns_with_data}
Total errors: {total_errors}
Error rate: {round(total_errors / (total_records * len(error_columns)) * 100, 2) if error_columns else 0}%

=== SOURCE ANALYSIS ===
"""

        # Анализ по источникам
        sources = ["chembl", "crossref", "openalex", "pubmed", "semantic_scholar"]
        for source in sources:
            source_columns = [col for col in df.columns if col.startswith(f"{source}_") and not col.endswith("_error")]
            if source_columns:
                source_data_count = 0
                for col in source_columns:
                    non_empty_count = (df[col].notna() & (df[col] != "") & (df[col] != "nan")).sum()
                    if non_empty_count > 0:
                        source_data_count += 1

                error_col = f"{source}_error"
                error_count = (df[error_col].notna() & (df[error_col] != "")).sum() if error_col in df.columns else 0

                summary += f"{source.upper()}: {source_data_count}/{len(source_columns)} columns with data, {error_count} errors\n"

        return summary
