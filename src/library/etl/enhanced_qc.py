"""Расширенная система оценки качества данных с детальными метриками."""

from __future__ import annotations

import re
from typing import Any, Optional

import numpy as np
import pandas as pd
from structlog.stdlib import BoundLogger


class EnhancedTableQualityProfiler:
    """Расширенный профайлер качества данных с детальными метриками."""

    def __init__(self, logger: Optional[BoundLogger] = None):
        """Инициализация профайлера качества данных."""
        self.logger = logger
        self._patterns = {
            "doi": re.compile(r"10\.\d{4,9}/[-._;()/:A-Z0-9]+", re.IGNORECASE),
            "issn": re.compile(r"\b\d{4}-\d{3}[\dxX]\b"),
            "isbn": re.compile(r"\b(?:ISBN[- ]?(?:13|10)?:? )?(?:97[89][- ]?)?\d{1,5}[- ]?\d{1,7}[- ]?\d{1,6}[- ]?\d\b"),
            "url": re.compile(r'https?://[^\s<>"{}|\\^`\[\]]+', re.IGNORECASE),
            "email": re.compile(r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b"),
        }

    def consume(self, df: pd.DataFrame) -> dict[str, Any]:
        """Анализ DataFrame и генерация расширенных метрик качества."""
        if self.logger:
            self.logger.info(f"Начинаем анализ качества данных: {len(df)} строк, {len(df.columns)} колонок")

        quality_report = {
            "table_summary": self._analyze_table_summary(df),
            "column_analysis": self._analyze_columns(df),
            "data_patterns": self._analyze_patterns(df),
            "statistical_summary": self._analyze_statistics(df),
            "data_roles": self._analyze_data_roles(df),
        }

        if self.logger:
            self.logger.info("Анализ качества данных завершен")

        return quality_report

<<<<<<< Updated upstream
=======
    def consume_with_pipeline_specific_metrics(self, df: pd.DataFrame, pipeline_type: str) -> dict[str, Any]:
        """Анализ DataFrame с учетом специфичных метрик для конкретного пайплайна."""
        if self.logger:
            self.logger.info(f"Начинаем анализ качества данных для пайплайна {pipeline_type}: {len(df)} строк, {len(df.columns)} колонок")

        # Базовые метрики
        quality_report = self.consume(df)

        # Добавляем специфичные метрики для пайплайна
        pipeline_specific_metrics = self._get_pipeline_specific_metrics(df, pipeline_type)
        quality_report["pipeline_specific"] = pipeline_specific_metrics

        if self.logger:
            self.logger.info(f"Анализ качества данных для пайплайна {pipeline_type} завершен")

        return quality_report

    def _get_pipeline_specific_metrics(self, df: pd.DataFrame, pipeline_type: str) -> dict[str, Any]:
        """Получает специфичные метрики для конкретного пайплайна."""
        if pipeline_type == "documents":
            return self._get_documents_metrics(df)
        elif pipeline_type == "testitem":
            return self._get_testitem_metrics(df)
        elif pipeline_type == "target":
            return self._get_target_metrics(df)
        else:
            return {}

>>>>>>> Stashed changes
    def _analyze_table_summary(self, df: pd.DataFrame) -> dict[str, Any]:
        """Анализ общей информации о таблице."""
        return {
            "total_rows": len(df),
            "total_columns": len(df.columns),
            "memory_usage_bytes": df.memory_usage(deep=True).sum(),
            "dtypes_count": df.dtypes.value_counts().to_dict(),
        }

    def _analyze_columns(self, df: pd.DataFrame) -> dict[str, dict[str, Any]]:
        """Детальный анализ каждой колонки."""
        column_analysis = {}

        for col in df.columns:
            series = df[col]
<<<<<<< Updated upstream
            analysis = {
                'non_null': int(series.notna().sum()),
                'non_empty': int(series.notna().sum() - (series.astype(str).str.strip() == '').sum()),
                'empty_pct': float((series.isna().sum() + (series.astype(str).str.strip() == '').sum()) / len(series) * 100),
                'unique_cnt': int(series.nunique()),
                'unique_pct_of_non_empty': float(series.nunique() / series.notna().sum() * 100) if series.notna().sum() > 0 else 0.0,
                'dtype': str(series.dtype),
=======
            # Безопасное вычисление non_empty без использования оператора - для numpy boolean
            non_null_count = series.notna().sum()
            empty_string_count = (series.astype(str).str.strip() == "").sum()
            non_empty_count = non_null_count - empty_string_count

            analysis = {
                "non_null": int(non_null_count),
                "non_empty": int(non_empty_count),
                "empty_pct": float((series.isna().sum() + empty_string_count) / len(series) * 100),
                "unique_cnt": int(series.nunique()),
                "unique_pct_of_non_empty": float(series.nunique() / series.notna().sum() * 100) if series.notna().sum() > 0 else 0.0,
                "dtype": str(series.dtype),
>>>>>>> Stashed changes
            }

            # Анализ паттернов для текстовых данных
            if series.dtype == "object":
                text_series = series.astype(str)
                analysis.update(self._analyze_text_patterns(text_series))

            # Статистический анализ для числовых данных
            if pd.api.types.is_numeric_dtype(series):
                analysis.update(self._analyze_numeric_series(series))

            # Анализ дат
            if pd.api.types.is_datetime64_any_dtype(series):
                analysis.update(self._analyze_datetime_series(series))

            # Анализ длины текста
            if series.dtype == "object":
                text_lengths = series.astype(str).str.len()
                non_empty_lengths = text_lengths.dropna()
                if len(non_empty_lengths) > 0:
                    analysis.update(
                        {
                            "text_len_min": int(non_empty_lengths.min()),
                            "text_len_p50": int(non_empty_lengths.median()),
                            "text_len_p95": int(non_empty_lengths.quantile(0.95)),
                            "text_len_max": int(non_empty_lengths.max()),
                        }
                    )
                else:
                    analysis.update(
                        {
                            "text_len_min": 0,
                            "text_len_p50": 0,
                            "text_len_p95": 0,
                            "text_len_max": 0,
                        }
                    )

            # Топ значения
            value_counts = series.value_counts().head(10)
            analysis["top_values"] = {str(k): int(v) for k, v in value_counts.items()}

            column_analysis[col] = analysis

        return column_analysis

    def _analyze_text_patterns(self, text_series: pd.Series) -> dict[str, float]:
        """Анализ паттернов в текстовых данных."""
        patterns = {}
        total_non_empty = len(text_series[text_series.str.strip() != ""])

        if total_non_empty == 0:
            return {f"pattern_cov_{pattern}": 0.0 for pattern in self._patterns.keys()}

        for pattern_name, pattern_regex in self._patterns.items():
            matches = text_series.str.contains(pattern_regex, na=False)
            coverage = float(matches.sum() / total_non_empty * 100)
            patterns[f"pattern_cov_{pattern_name}"] = coverage

        # Анализ булевых значений
        bool_pattern = re.compile(r"^(true|false|yes|no|1|0|y|n)$", re.IGNORECASE)
        bool_matches = text_series.str.match(bool_pattern, na=False)
        patterns["bool_like_cov"] = float(bool_matches.sum() / total_non_empty * 100) if total_non_empty > 0 else 0.0

        return patterns

    def _analyze_numeric_series(self, series: pd.Series) -> dict[str, float]:
        """Анализ числовых данных."""
        numeric_data = series.dropna()

        if len(numeric_data) == 0:
            return {
                "numeric_cov": 0.0,
                "numeric_min": 0.0,
                "numeric_p50": 0.0,
                "numeric_p95": 0.0,
                "numeric_max": 0.0,
                "numeric_mean": 0.0,
                "numeric_std": 0.0,
            }

        return {
            "numeric_cov": float(len(numeric_data) / len(series) * 100),
            "numeric_min": float(numeric_data.min()),
            "numeric_p50": float(numeric_data.median()),
            "numeric_p95": float(numeric_data.quantile(0.95)),
            "numeric_max": float(numeric_data.max()),
            "numeric_mean": float(numeric_data.mean()),
            "numeric_std": float(numeric_data.std()) if len(numeric_data) > 1 else 0.0,
        }

    def _analyze_datetime_series(self, series: pd.Series) -> dict[str, Any]:
        """Анализ временных данных."""
        datetime_data = pd.to_datetime(series, errors="coerce").dropna()

        if len(datetime_data) == 0:
            return {
                "date_cov": 0.0,
                "date_min": None,
                "date_p50": None,
                "date_max": None,
            }

        return {
            "date_cov": float(len(datetime_data) / len(series) * 100),
            "date_min": datetime_data.min().isoformat(),
            "date_p50": datetime_data.median().isoformat(),
            "date_max": datetime_data.max().isoformat(),
        }

    def _analyze_patterns(self, df: pd.DataFrame) -> dict[str, dict[str, float]]:
        """Анализ паттернов по всей таблице."""
        pattern_summary = {}

        for col in df.columns:
            if df[col].dtype == "object":
                text_series = df[col].astype(str)
                col_patterns = {}

                total_non_empty = len(text_series[text_series.str.strip() != ""])
                if total_non_empty > 0:
                    for pattern_name, pattern_regex in self._patterns.items():
                        matches = text_series.str.contains(pattern_regex, na=False)
                        coverage = float(matches.sum() / total_non_empty * 100)
                        col_patterns[f"pattern_cov_{pattern_name}"] = coverage

                pattern_summary[col] = col_patterns

        return pattern_summary

    def _analyze_statistics(self, df: pd.DataFrame) -> dict[str, Any]:
        """Общая статистическая информация."""
        numeric_cols = df.select_dtypes(include=[np.number]).columns
        categorical_cols = df.select_dtypes(include=["object"]).columns

        return {
            "numeric_columns": list(numeric_cols),
            "categorical_columns": list(categorical_cols),
            "datetime_columns": list(df.select_dtypes(include=["datetime64"]).columns),
            "missing_data_matrix": df.isnull().sum().to_dict(),
            "duplicate_rows": int(df.duplicated().sum()),
            "memory_per_row": float(df.memory_usage(deep=True).sum() / len(df)) if len(df) > 0 else 0.0,
        }

    def _analyze_data_roles(self, df: pd.DataFrame) -> dict[str, str]:
        """Автоматическое определение ролей колонок."""
        roles = {}

        for col in df.columns:
            series = df[col]

            # Проверка на ID
            if col.lower().endswith("_id") or col.lower().endswith("id") or series.nunique() == len(series) and series.notna().sum() > 0:
                roles[col] = "identifier"

            # Проверка на дату
            elif pd.api.types.is_datetime64_any_dtype(series):
                roles[col] = "datetime"

            # Проверка на числовые метрики
            elif pd.api.types.is_numeric_dtype(series):
                roles[col] = "numeric_metric"

            # Проверка на категориальные данные
            elif series.dtype == "object":
                unique_ratio = series.nunique() / len(series)
                if unique_ratio < 0.1:  # Менее 10% уникальных значений
                    roles[col] = "categorical"
                else:
                    roles[col] = "text"

            else:
                roles[col] = "unknown"

        return roles

    def generate_summary_report(self, quality_report: dict[str, Any]) -> pd.DataFrame:
        """Генерация сводного отчета по качеству данных."""
        summary_data = []

        for col, analysis in quality_report["column_analysis"].items():
            row = {
                "column": col,
                "dtype": analysis.get("dtype", "unknown"),
                "role": quality_report["data_roles"].get(col, "unknown"),
                "non_null": analysis.get("non_null", 0),
                "non_empty": analysis.get("non_empty", 0),
                "empty_pct": analysis.get("empty_pct", 0.0),
                "unique_cnt": analysis.get("unique_cnt", 0),
                "unique_pct_of_non_empty": analysis.get("unique_pct_of_non_empty", 0.0),
                "pattern_cov_doi": analysis.get("pattern_cov_doi", 0.0),
                "pattern_cov_issn": analysis.get("pattern_cov_issn", 0.0),
                "pattern_cov_isbn": analysis.get("pattern_cov_isbn", 0.0),
                "pattern_cov_url": analysis.get("pattern_cov_url", 0.0),
                "pattern_cov_email": analysis.get("pattern_cov_email", 0.0),
                "bool_like_cov": analysis.get("bool_like_cov", 0.0),
                "numeric_cov": analysis.get("numeric_cov", 0.0),
                "numeric_min": analysis.get("numeric_min", 0.0),
                "numeric_p50": analysis.get("numeric_p50", 0.0),
                "numeric_p95": analysis.get("numeric_p95", 0.0),
                "numeric_max": analysis.get("numeric_max", 0.0),
                "numeric_mean": analysis.get("numeric_mean", 0.0),
                "numeric_std": analysis.get("numeric_std", 0.0),
                "date_cov": analysis.get("date_cov", 0.0),
                "date_min": analysis.get("date_min"),
                "date_p50": analysis.get("date_p50"),
                "date_max": analysis.get("date_max"),
                "text_len_min": analysis.get("text_len_min", 0),
                "text_len_p50": analysis.get("text_len_p50", 0),
                "text_len_p95": analysis.get("text_len_p95", 0),
                "text_len_max": analysis.get("text_len_max", 0),
                "top_values": str(analysis.get("top_values", {})),
            }
            summary_data.append(row)

        return pd.DataFrame(summary_data)

    def generate_detailed_report(self, quality_report: dict[str, Any]) -> dict[str, pd.DataFrame]:
        """Генерация детального отчета с отдельными таблицами."""
        reports = {}

        # Сводная таблица по колонкам
        reports["column_summary"] = self.generate_summary_report(quality_report)

        # Таблица с топ значениями
        top_values_data = []
        for col, analysis in quality_report["column_analysis"].items():
            top_values = analysis.get("top_values", {})
            for value, count in top_values.items():
                top_values_data.append({"column": col, "value": value, "count": count, "percentage": float(count / quality_report["table_summary"]["total_rows"] * 100)})

        if top_values_data:
            reports["top_values"] = pd.DataFrame(top_values_data)

        # Таблица с паттернами
        pattern_data = []
        for col, patterns in quality_report["data_patterns"].items():
            for pattern, coverage in patterns.items():
                pattern_data.append({"column": col, "pattern": pattern, "coverage_pct": coverage})

        if pattern_data:
            reports["pattern_coverage"] = pd.DataFrame(pattern_data)

        return reports

<<<<<<< Updated upstream

def build_enhanced_qc_report(df: pd.DataFrame, logger: Optional[BoundLogger] = None) -> dict[str, Any]:
=======
    def _get_documents_metrics(self, df: pd.DataFrame) -> dict[str, Any]:
        """Специфичные метрики для пайплайна документов."""
        metrics = {}

        # DOI валидация
        if "doi" in df.columns:
            doi_valid = df["doi"].apply(lambda x: bool(self._patterns["doi"].match(str(x))) if pd.notna(x) else False)
            metrics["doi_validity_rate"] = doi_valid.sum() / len(df) if len(df) > 0 else 0
            metrics["doi_coverage"] = df["doi"].notna().sum() / len(df) if len(df) > 0 else 0

        # PubMed ID валидация
        if "document_pubmed_id" in df.columns:
            pmid_valid = df["document_pubmed_id"].apply(lambda x: str(x).isdigit() if pd.notna(x) else False)
            metrics["pmid_validity_rate"] = pmid_valid.sum() / len(df) if len(df) > 0 else 0
            metrics["pmid_coverage"] = df["document_pubmed_id"].notna().sum() / len(df) if len(df) > 0 else 0

        # Журнальная информация
        if "journal" in df.columns:
            metrics["journal_coverage"] = df["journal"].notna().sum() / len(df) if len(df) > 0 else 0
            metrics["unique_journals"] = df["journal"].nunique()

        # Год публикации
        if "year" in df.columns:
            current_year = pd.Timestamp.now().year
            valid_years = df["year"].between(1800, current_year + 1)
            metrics["year_validity_rate"] = valid_years.sum() / len(df) if len(df) > 0 else 0
            metrics["year_coverage"] = df["year"].notna().sum() / len(df) if len(df) > 0 else 0

        # Мульти-источниковые данные
        source_columns = ["chembl_title", "crossref_title", "openalex_title", "pubmed_title", "semantic_scholar_title"]
        available_sources = [col for col in source_columns if col in df.columns]
        if available_sources:
            multi_source_count = 0
            for _, row in df.iterrows():
                source_count = sum(1 for col in available_sources if pd.notna(row.get(col)))
                if source_count >= 2:
                    multi_source_count += 1
            metrics["multi_source_coverage"] = multi_source_count / len(df) if len(df) > 0 else 0

        return metrics

    def _get_testitem_metrics(self, df: pd.DataFrame) -> dict[str, Any]:
        """Специфичные метрики для пайплайна testitem."""
        metrics = {}

        # ChEMBL ID валидация
        if "molecule_chembl_id" in df.columns:
            chembl_valid = df["molecule_chembl_id"].apply(lambda x: str(x).startswith("CHEMBL") if pd.notna(x) else False)
            metrics["chembl_id_validity_rate"] = chembl_valid.sum() / len(df) if len(df) > 0 else 0
            metrics["chembl_id_coverage"] = df["molecule_chembl_id"].notna().sum() / len(df) if len(df) > 0 else 0

        # Молекулярные свойства
        molecular_props = ["mw_freebase", "alogp", "hba", "hbd", "psa"]
        for prop in molecular_props:
            if prop in df.columns:
                prop_valid = df[prop].apply(lambda x: isinstance(x, (int, float)) and x > 0 if pd.notna(x) else False)
                metrics[f"{prop}_validity_rate"] = prop_valid.sum() / len(df) if len(df) > 0 else 0
                metrics[f"{prop}_coverage"] = df[prop].notna().sum() / len(df) if len(df) > 0 else 0

        # SMILES валидация
        if "standardized_smiles" in df.columns:
            smiles_coverage = df["standardized_smiles"].notna().sum() / len(df) if len(df) > 0 else 0
            metrics["smiles_coverage"] = smiles_coverage

        # InChI валидация
        if "standardized_inchi" in df.columns:
            inchi_coverage = df["standardized_inchi"].notna().sum() / len(df) if len(df) > 0 else 0
            metrics["inchi_coverage"] = inchi_coverage

        # Lipinski's Rule of Five
        if all(col in df.columns for col in ["mw_freebase", "alogp", "hba", "hbd"]):
            lipinski_violations = 0
            for _, row in df.iterrows():
                violations = 0
                if pd.notna(row["mw_freebase"]) and row["mw_freebase"] > 500:
                    violations += 1
                if pd.notna(row["alogp"]) and row["alogp"] > 5:
                    violations += 1
                if pd.notna(row["hba"]) and row["hba"] > 10:
                    violations += 1
                if pd.notna(row["hbd"]) and row["hbd"] > 5:
                    violations += 1
                if violations > 1:
                    lipinski_violations += 1
            metrics["lipinski_rule_violations"] = lipinski_violations / len(df) if len(df) > 0 else 0

        return metrics

    def _get_target_metrics(self, df: pd.DataFrame) -> dict[str, Any]:
        """Специфичные метрики для пайплайна target."""
        metrics = {}

        # ChEMBL ID валидация
        if "target_chembl_id" in df.columns:
            chembl_valid = df["target_chembl_id"].apply(lambda x: str(x).startswith("CHEMBL") if pd.notna(x) else False)
            metrics["chembl_id_validity_rate"] = chembl_valid.sum() / len(df) if len(df) > 0 else 0
            metrics["chembl_id_coverage"] = df["target_chembl_id"].notna().sum() / len(df) if len(df) > 0 else 0

        # UniProt ID валидация
        if "uniprot_id" in df.columns:
            uniprot_pattern = re.compile(r"^[OPQ][0-9][A-Z0-9]{3}[0-9]|[A-NR-Z][0-9]([A-Z][A-Z0-9]{2}[0-9]){1,2}$")
            uniprot_valid = df["uniprot_id"].apply(lambda x: bool(uniprot_pattern.match(str(x))) if pd.notna(x) else False)
            metrics["uniprot_id_validity_rate"] = uniprot_valid.sum() / len(df) if len(df) > 0 else 0
            metrics["uniprot_id_coverage"] = df["uniprot_id"].notna().sum() / len(df) if len(df) > 0 else 0

        # Организм и таксономия
        if "unified_organism" in df.columns:
            metrics["organism_coverage"] = df["unified_organism"].notna().sum() / len(df) if len(df) > 0 else 0
            metrics["unique_organisms"] = df["unified_organism"].nunique()

        if "unified_tax_id" in df.columns:
            tax_valid = df["unified_tax_id"].apply(lambda x: isinstance(x, (int, float)) and x > 0 if pd.notna(x) else False)
            metrics["tax_id_validity_rate"] = tax_valid.sum() / len(df) if len(df) > 0 else 0
            metrics["tax_id_coverage"] = df["unified_tax_id"].notna().sum() / len(df) if len(df) > 0 else 0

        # Тип мишени
        if "unified_target_type" in df.columns:
            valid_target_types = {"SINGLE PROTEIN", "PROTEIN COMPLEX", "PROTEIN FAMILY", "PROTEIN-PROTEIN INTERACTION", "NUCLEIC ACID", "UNKNOWN"}
            target_type_valid = df["unified_target_type"].apply(lambda x: x in valid_target_types if pd.notna(x) else False)
            metrics["target_type_validity_rate"] = target_type_valid.sum() / len(df) if len(df) > 0 else 0
            metrics["target_type_coverage"] = df["unified_target_type"].notna().sum() / len(df) if len(df) > 0 else 0
            metrics["unique_target_types"] = df["unified_target_type"].nunique()

        # Мульти-источниковые данные
        source_columns = ["has_chembl_data", "has_uniprot_data", "has_iuphar_data", "has_gtopdb_data"]
        available_sources = [col for col in source_columns if col in df.columns]
        if available_sources:
            for col in available_sources:
                metrics[f"{col}_rate"] = df[col].sum() / len(df) if len(df) > 0 else 0

            # Мульти-источниковая валидация
            if "multi_source_validated" in df.columns:
                metrics["multi_source_validation_rate"] = df["multi_source_validated"].sum() / len(df) if len(df) > 0 else 0

        return metrics


def build_enhanced_qc_summary(df: pd.DataFrame, pipeline_type: str = "generic") -> pd.DataFrame:
    """Строит краткий QC отчет с базовыми метриками."""
    profiler = EnhancedTableQualityProfiler()

    if pipeline_type in ["documents", "testitem", "target"]:
        quality_report = profiler.consume_with_pipeline_specific_metrics(df, pipeline_type)
    else:
        quality_report = profiler.consume(df)

    # Извлекаем основные метрики
    summary_metrics = []

    # Базовые метрики
    table_summary = quality_report.get("table_summary", {})

    # Конвертируем numpy типы в Python типы
    total_rows = table_summary.get("total_rows", 0)
    if hasattr(total_rows, "item"):
        total_rows = total_rows.item()

    total_columns = table_summary.get("total_columns", 0)
    if hasattr(total_columns, "item"):
        total_columns = total_columns.item()

    memory_usage_bytes = table_summary.get("memory_usage_bytes", 0)
    if hasattr(memory_usage_bytes, "item"):
        memory_usage_bytes = memory_usage_bytes.item()

    summary_metrics.extend(
        [
            {"metric": "total_rows", "value": total_rows},
            {"metric": "total_columns", "value": total_columns},
            {"metric": "memory_usage_mb", "value": round(memory_usage_bytes / 1024 / 1024, 2)},
        ]
    )

    # Метрики полноты данных
    column_analysis = quality_report.get("column_analysis", {})
    for col_name, col_info in column_analysis.items():
        # Вычисляем completeness как процент непустых значений
        empty_pct = col_info.get("empty_pct", 100)
        if hasattr(empty_pct, "item"):  # numpy scalar
            empty_pct = empty_pct.item()
        completeness = (100 - empty_pct) / 100
        unique_cnt = col_info.get("unique_cnt", 0)
        if hasattr(unique_cnt, "item"):
            unique_cnt = unique_cnt.item()

        summary_metrics.extend(
            [
                {"metric": f"{col_name}_completeness", "value": round(completeness, 3)},
                {"metric": f"{col_name}_unique_values", "value": unique_cnt},
            ]
        )

    # Специфичные метрики пайплайна
    pipeline_specific = quality_report.get("pipeline_specific", {})
    for metric_name, metric_value in pipeline_specific.items():
        if isinstance(metric_value, (int, float)):
            summary_metrics.append({"metric": f"pipeline_{metric_name}", "value": round(metric_value, 3) if isinstance(metric_value, float) else metric_value})

    return pd.DataFrame(summary_metrics)


def build_enhanced_qc_detailed(df: pd.DataFrame, pipeline_type: str = "generic") -> dict[str, Any]:
    """Строит детальный QC отчет со всеми метриками."""
    profiler = EnhancedTableQualityProfiler()

    if pipeline_type in ["documents", "testitem", "target"]:
        return profiler.consume_with_pipeline_specific_metrics(df, pipeline_type)
    else:
        return profiler.consume(df)


def build_enhanced_qc_report(df: pd.DataFrame, logger: BoundLogger | None = None) -> dict[str, Any]:
>>>>>>> Stashed changes
    """Создание расширенного отчета о качестве данных."""
    profiler = EnhancedTableQualityProfiler(logger=logger)
    return profiler.consume(df)


<<<<<<< Updated upstream
def build_enhanced_qc_summary(df: pd.DataFrame, logger: Optional[BoundLogger] = None) -> pd.DataFrame:
    """Создание сводного отчета о качестве данных."""
    profiler = EnhancedTableQualityProfiler(logger=logger)
    quality_report = profiler.consume(df)
    return profiler.generate_summary_report(quality_report)


def build_enhanced_qc_detailed(df: pd.DataFrame, logger: Optional[BoundLogger] = None) -> dict[str, pd.DataFrame]:
    """Создание детального отчета о качестве данных."""
    profiler = EnhancedTableQualityProfiler(logger=logger)
    quality_report = profiler.consume(df)
    return profiler.generate_detailed_report(quality_report)


__all__ = [
    'EnhancedTableQualityProfiler',
    'build_enhanced_qc_report',
    'build_enhanced_qc_summary',
    'build_enhanced_qc_detailed',
=======
class NormalizationReporter:
    """Репортер для отслеживания и агрегации изменений данных при нормализации."""

    def __init__(self, logger: BoundLogger | None = None):
        """Инициализация репортера нормализации."""
        self.logger = logger
        self.changes: dict[str, list[dict[str, Any]]] = defaultdict(list)
        self.field_stats: dict[str, dict[str, int]] = defaultdict(lambda: {"total_records": 0, "changed_count": 0, "discarded_count": 0, "success_count": 0})

    def track_normalization(self, field: str, original_value: Any, normalized_value: Any, normalizer_name: str | None = None) -> None:
        """Отслеживает изменение значения при нормализации.

        Args:
            field: Название поля
            original_value: Исходное значение
            normalized_value: Нормализованное значение
            normalizer_name: Название примененного нормализатора
        """
        # Обновляем статистику поля
        stats = self.field_stats[field]
        stats["total_records"] += 1

        # Определяем тип изменения
        if normalized_value is None and original_value is not None:
            stats["discarded_count"] += 1
            change_type = "discarded"
        elif original_value != normalized_value:
            stats["changed_count"] += 1
            change_type = "changed"
        else:
            stats["success_count"] += 1
            change_type = "unchanged"

        # Сохраняем детали изменения (только для измененных значений)
        if change_type in ["changed", "discarded"]:
            self.changes[field].append(
                {
                    "original_value": str(original_value) if original_value is not None else None,
                    "normalized_value": str(normalized_value) if normalized_value is not None else None,
                    "change_type": change_type,
                    "normalizer_name": normalizer_name,
                }
            )

    def aggregate_changes(self) -> dict[str, dict[str, Any]]:
        """Агрегирует изменения по полям.

        Returns:
            Словарь с агрегированной статистикой по каждому полю
        """
        aggregated = {}

        for field, stats in self.field_stats.items():
            total = stats["total_records"]
            if total == 0:
                continue

            success_rate = (stats["success_count"] / total) * 100 if total > 0 else 0

            # Получаем примеры изменений
            examples = self.changes[field][:5] if field in self.changes else []

            aggregated[field] = {
                "total_records": total,
                "changed_count": stats["changed_count"],
                "discarded_count": stats["discarded_count"],
                "success_count": stats["success_count"],
                "success_rate": round(success_rate, 2),
                "examples": examples,
            }

        return aggregated

    def generate_normalization_report(self) -> pd.DataFrame:
        """Генерирует детальный отчет о нормализации в формате DataFrame.

        Returns:
            DataFrame с отчетом по каждому полю
        """
        aggregated = self.aggregate_changes()

        if not aggregated:
            return pd.DataFrame()

        report_data = []
        for field, stats in aggregated.items():
            # Получаем примеры изменений
            examples = stats["examples"]
            example_before = examples[0]["original_value"] if examples else None
            example_after = examples[0]["normalized_value"] if examples else None

            report_data.append(
                {
                    "field_name": field,
                    "total_records": stats["total_records"],
                    "changed_count": stats["changed_count"],
                    "discarded_count": stats["discarded_count"],
                    "success_rate": stats["success_rate"],
                    "example_before": example_before,
                    "example_after": example_after,
                }
            )

        df = pd.DataFrame(report_data)

        # Сортируем по количеству изменений (по убыванию)
        df = df.sort_values("changed_count", ascending=False)

        if self.logger:
            self.logger.info(f"Сгенерирован отчет нормализации: {len(df)} полей")

        return df

    def save_report_to_csv(self, filepath: str) -> None:
        """Сохраняет отчет нормализации в CSV файл.

        Args:
            filepath: Путь к файлу для сохранения
        """
        report_df = self.generate_normalization_report()
        if not report_df.empty:
            report_df.to_csv(filepath, index=False)
            if self.logger:
                self.logger.info(f"Отчет нормализации сохранен в {filepath}")
        else:
            if self.logger:
                self.logger.warning("Нет данных для сохранения отчета нормализации")

    def get_summary_stats(self) -> dict[str, Any]:
        """Получает сводную статистику по всем полям.

        Returns:
            Словарь со сводной статистикой
        """
        aggregated = self.aggregate_changes()

        if not aggregated:
            return {"total_fields": 0, "total_records": 0, "total_changed": 0, "total_discarded": 0, "overall_success_rate": 0.0}

        total_fields = len(aggregated)
        total_records = sum(stats["total_records"] for stats in aggregated.values())
        total_changed = sum(stats["changed_count"] for stats in aggregated.values())
        total_discarded = sum(stats["discarded_count"] for stats in aggregated.values())
        total_success = sum(stats["success_count"] for stats in aggregated.values())

        overall_success_rate = (total_success / total_records) * 100 if total_records > 0 else 0

        return {
            "total_fields": total_fields,
            "total_records": total_records,
            "total_changed": total_changed,
            "total_discarded": total_discarded,
            "total_success": total_success,
            "overall_success_rate": round(overall_success_rate, 2),
        }

    def reset(self) -> None:
        """Сбрасывает все накопленные данные."""
        self.changes.clear()
        self.field_stats.clear()
        if self.logger:
            self.logger.info("Данные репортера нормализации сброшены")


__all__ = [
    "EnhancedTableQualityProfiler",
    "NormalizationReporter",
    "build_enhanced_qc_report",
    "build_enhanced_qc_summary",
    "build_enhanced_qc_detailed",
>>>>>>> Stashed changes
]
