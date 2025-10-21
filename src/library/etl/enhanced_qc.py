"""Расширенная система оценки качества данных с детальными метриками."""

from __future__ import annotations

import re
from typing import Any

import numpy as np
import pandas as pd
from structlog.stdlib import BoundLogger


class EnhancedTableQualityProfiler:
    """Расширенный профайлер качества данных с детальными метриками."""

    def __init__(self, logger: BoundLogger | None = None):
        """Инициализация профайлера качества данных."""
        self.logger = logger
        self._patterns = {
            'doi': re.compile(r'10\.\d{4,9}/[-._;()/:A-Z0-9]+', re.IGNORECASE),
            'issn': re.compile(r'\b\d{4}-\d{3}[\dxX]\b'),
            'isbn': re.compile(r'\b(?:ISBN[- ]?(?:13|10)?:? )?(?:97[89][- ]?)?\d{1,5}[- ]?\d{1,7}[- ]?\d{1,6}[- ]?\d\b'),
            'url': re.compile(r'https?://[^\s<>"{}|\\^`\[\]]+', re.IGNORECASE),
            'email': re.compile(r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b'),
        }

    def consume(self, df: pd.DataFrame) -> dict[str, Any]:
        """Анализ DataFrame и генерация расширенных метрик качества."""
        if self.logger:
            self.logger.info(f"Начинаем анализ качества данных: {len(df)} строк, {len(df.columns)} колонок")

        quality_report = {
            'table_summary': self._analyze_table_summary(df),
            'column_analysis': self._analyze_columns(df),
            'data_patterns': self._analyze_patterns(df),
            'statistical_summary': self._analyze_statistics(df),
            'data_roles': self._analyze_data_roles(df),
        }

        if self.logger:
            self.logger.info("Анализ качества данных завершен")
        
        return quality_report

    def _analyze_table_summary(self, df: pd.DataFrame) -> dict[str, Any]:
        """Анализ общей информации о таблице."""
        return {
            'total_rows': len(df),
            'total_columns': len(df.columns),
            'memory_usage_bytes': df.memory_usage(deep=True).sum(),
            'dtypes_count': df.dtypes.value_counts().to_dict(),
        }

    def _analyze_columns(self, df: pd.DataFrame) -> dict[str, dict[str, Any]]:
        """Детальный анализ каждой колонки."""
        column_analysis = {}
        
        for col in df.columns:
            series = df[col]
            analysis = {
                'non_null': int(series.notna().sum()),
                'non_empty': int(series.notna().sum() - (series.astype(str).str.strip() == '').sum()),
                'empty_pct': float((series.isna().sum() + (series.astype(str).str.strip() == '').sum()) / len(series) * 100),
                'unique_cnt': int(series.nunique()),
                'unique_pct_of_non_empty': float(series.nunique() / series.notna().sum() * 100) if series.notna().sum() > 0 else 0.0,
                'dtype': str(series.dtype),
            }
            
            # Анализ паттернов для текстовых данных
            if series.dtype == 'object':
                text_series = series.astype(str)
                analysis.update(self._analyze_text_patterns(text_series))
            
            # Статистический анализ для числовых данных
            if pd.api.types.is_numeric_dtype(series):
                analysis.update(self._analyze_numeric_series(series))
            
            # Анализ дат
            if pd.api.types.is_datetime64_any_dtype(series):
                analysis.update(self._analyze_datetime_series(series))
            
            # Анализ длины текста
            if series.dtype == 'object':
                text_lengths = series.astype(str).str.len()
                non_empty_lengths = text_lengths.dropna()
                if len(non_empty_lengths) > 0:
                    analysis.update({
                        'text_len_min': int(non_empty_lengths.min()),
                        'text_len_p50': int(non_empty_lengths.median()),
                        'text_len_p95': int(non_empty_lengths.quantile(0.95)),
                        'text_len_max': int(non_empty_lengths.max()),
                    })
                else:
                    analysis.update({
                        'text_len_min': 0,
                        'text_len_p50': 0,
                        'text_len_p95': 0,
                        'text_len_max': 0,
                    })
            
            # Топ значения
            value_counts = series.value_counts().head(10)
            analysis['top_values'] = {
                str(k): int(v) for k, v in value_counts.items()
            }
            
            column_analysis[col] = analysis
        
        return column_analysis

    def _analyze_text_patterns(self, text_series: pd.Series) -> dict[str, float]:
        """Анализ паттернов в текстовых данных."""
        patterns = {}
        total_non_empty = len(text_series[text_series.str.strip() != ''])
        
        if total_non_empty == 0:
            return {f'pattern_cov_{pattern}': 0.0 for pattern in self._patterns.keys()}
        
        for pattern_name, pattern_regex in self._patterns.items():
            matches = text_series.str.contains(pattern_regex, na=False)
            coverage = float(matches.sum() / total_non_empty * 100)
            patterns[f'pattern_cov_{pattern_name}'] = coverage
        
        # Анализ булевых значений
        bool_pattern = re.compile(r'^(true|false|yes|no|1|0|y|n)$', re.IGNORECASE)
        bool_matches = text_series.str.match(bool_pattern, na=False)
        patterns['bool_like_cov'] = float(bool_matches.sum() / total_non_empty * 100) if total_non_empty > 0 else 0.0
        
        return patterns

    def _analyze_numeric_series(self, series: pd.Series) -> dict[str, float]:
        """Анализ числовых данных."""
        numeric_data = series.dropna()
        
        if len(numeric_data) == 0:
            return {
                'numeric_cov': 0.0,
                'numeric_min': 0.0,
                'numeric_p50': 0.0,
                'numeric_p95': 0.0,
                'numeric_max': 0.0,
                'numeric_mean': 0.0,
                'numeric_std': 0.0,
            }
        
        return {
            'numeric_cov': float(len(numeric_data) / len(series) * 100),
            'numeric_min': float(numeric_data.min()),
            'numeric_p50': float(numeric_data.median()),
            'numeric_p95': float(numeric_data.quantile(0.95)),
            'numeric_max': float(numeric_data.max()),
            'numeric_mean': float(numeric_data.mean()),
            'numeric_std': float(numeric_data.std()) if len(numeric_data) > 1 else 0.0,
        }

    def _analyze_datetime_series(self, series: pd.Series) -> dict[str, Any]:
        """Анализ временных данных."""
        datetime_data = pd.to_datetime(series, errors='coerce').dropna()
        
        if len(datetime_data) == 0:
            return {
                'date_cov': 0.0,
                'date_min': None,
                'date_p50': None,
                'date_max': None,
            }
        
        return {
            'date_cov': float(len(datetime_data) / len(series) * 100),
            'date_min': datetime_data.min().isoformat(),
            'date_p50': datetime_data.median().isoformat(),
            'date_max': datetime_data.max().isoformat(),
        }

    def _analyze_patterns(self, df: pd.DataFrame) -> dict[str, dict[str, float]]:
        """Анализ паттернов по всей таблице."""
        pattern_summary = {}
        
        for col in df.columns:
            if df[col].dtype == 'object':
                text_series = df[col].astype(str)
                col_patterns = {}
                
                total_non_empty = len(text_series[text_series.str.strip() != ''])
                if total_non_empty > 0:
                    for pattern_name, pattern_regex in self._patterns.items():
                        matches = text_series.str.contains(pattern_regex, na=False)
                        coverage = float(matches.sum() / total_non_empty * 100)
                        col_patterns[f'pattern_cov_{pattern_name}'] = coverage
                
                pattern_summary[col] = col_patterns
        
        return pattern_summary

    def _analyze_statistics(self, df: pd.DataFrame) -> dict[str, Any]:
        """Общая статистическая информация."""
        numeric_cols = df.select_dtypes(include=[np.number]).columns
        categorical_cols = df.select_dtypes(include=['object']).columns
        
        return {
            'numeric_columns': list(numeric_cols),
            'categorical_columns': list(categorical_cols),
            'datetime_columns': list(df.select_dtypes(include=['datetime64']).columns),
            'missing_data_matrix': df.isnull().sum().to_dict(),
            'duplicate_rows': int(df.duplicated().sum()),
            'memory_per_row': float(df.memory_usage(deep=True).sum() / len(df)) if len(df) > 0 else 0.0,
        }

    def _analyze_data_roles(self, df: pd.DataFrame) -> dict[str, str]:
        """Автоматическое определение ролей колонок."""
        roles = {}
        
        for col in df.columns:
            series = df[col]
            
            # Проверка на ID
            if (col.lower().endswith('_id') or col.lower().endswith('id') or 
                series.nunique() == len(series) and series.notna().sum() > 0):
                roles[col] = 'identifier'
            
            # Проверка на дату
            elif pd.api.types.is_datetime64_any_dtype(series):
                roles[col] = 'datetime'
            
            # Проверка на числовые метрики
            elif pd.api.types.is_numeric_dtype(series):
                roles[col] = 'numeric_metric'
            
            # Проверка на категориальные данные
            elif series.dtype == 'object':
                unique_ratio = series.nunique() / len(series)
                if unique_ratio < 0.1:  # Менее 10% уникальных значений
                    roles[col] = 'categorical'
                else:
                    roles[col] = 'text'
            
            else:
                roles[col] = 'unknown'
        
        return roles

    def generate_summary_report(self, quality_report: dict[str, Any]) -> pd.DataFrame:
        """Генерация сводного отчета по качеству данных."""
        summary_data = []
        
        for col, analysis in quality_report['column_analysis'].items():
            row = {
                'column': col,
                'dtype': analysis.get('dtype', 'unknown'),
                'role': quality_report['data_roles'].get(col, 'unknown'),
                'non_null': analysis.get('non_null', 0),
                'non_empty': analysis.get('non_empty', 0),
                'empty_pct': analysis.get('empty_pct', 0.0),
                'unique_cnt': analysis.get('unique_cnt', 0),
                'unique_pct_of_non_empty': analysis.get('unique_pct_of_non_empty', 0.0),
                'pattern_cov_doi': analysis.get('pattern_cov_doi', 0.0),
                'pattern_cov_issn': analysis.get('pattern_cov_issn', 0.0),
                'pattern_cov_isbn': analysis.get('pattern_cov_isbn', 0.0),
                'pattern_cov_url': analysis.get('pattern_cov_url', 0.0),
                'pattern_cov_email': analysis.get('pattern_cov_email', 0.0),
                'bool_like_cov': analysis.get('bool_like_cov', 0.0),
                'numeric_cov': analysis.get('numeric_cov', 0.0),
                'numeric_min': analysis.get('numeric_min', 0.0),
                'numeric_p50': analysis.get('numeric_p50', 0.0),
                'numeric_p95': analysis.get('numeric_p95', 0.0),
                'numeric_max': analysis.get('numeric_max', 0.0),
                'numeric_mean': analysis.get('numeric_mean', 0.0),
                'numeric_std': analysis.get('numeric_std', 0.0),
                'date_cov': analysis.get('date_cov', 0.0),
                'date_min': analysis.get('date_min'),
                'date_p50': analysis.get('date_p50'),
                'date_max': analysis.get('date_max'),
                'text_len_min': analysis.get('text_len_min', 0),
                'text_len_p50': analysis.get('text_len_p50', 0),
                'text_len_p95': analysis.get('text_len_p95', 0),
                'text_len_max': analysis.get('text_len_max', 0),
                'top_values': str(analysis.get('top_values', {})),
            }
            summary_data.append(row)
        
        return pd.DataFrame(summary_data)

    def generate_detailed_report(self, quality_report: dict[str, Any]) -> dict[str, pd.DataFrame]:
        """Генерация детального отчета с отдельными таблицами."""
        reports = {}
        
        # Сводная таблица по колонкам
        reports['column_summary'] = self.generate_summary_report(quality_report)
        
        # Таблица с топ значениями
        top_values_data = []
        for col, analysis in quality_report['column_analysis'].items():
            top_values = analysis.get('top_values', {})
            for value, count in top_values.items():
                top_values_data.append({
                    'column': col,
                    'value': value,
                    'count': count,
                    'percentage': float(count / quality_report['table_summary']['total_rows'] * 100)
                })
        
        if top_values_data:
            reports['top_values'] = pd.DataFrame(top_values_data)
        
        # Таблица с паттернами
        pattern_data = []
        for col, patterns in quality_report['data_patterns'].items():
            for pattern, coverage in patterns.items():
                pattern_data.append({
                    'column': col,
                    'pattern': pattern,
                    'coverage_pct': coverage
                })
        
        if pattern_data:
            reports['pattern_coverage'] = pd.DataFrame(pattern_data)
        
        return reports


def build_enhanced_qc_report(df: pd.DataFrame, logger: BoundLogger | None = None) -> dict[str, Any]:
    """Создание расширенного отчета о качестве данных."""
    profiler = EnhancedTableQualityProfiler(logger=logger)
    return profiler.consume(df)


def build_enhanced_qc_summary(df: pd.DataFrame, logger: BoundLogger | None = None) -> pd.DataFrame:
    """Создание сводного отчета о качестве данных."""
    profiler = EnhancedTableQualityProfiler(logger=logger)
    quality_report = profiler.consume(df)
    return profiler.generate_summary_report(quality_report)


def build_enhanced_qc_detailed(df: pd.DataFrame, logger: BoundLogger | None = None) -> dict[str, pd.DataFrame]:
    """Создание детального отчета о качестве данных."""
    profiler = EnhancedTableQualityProfiler(logger=logger)
    quality_report = profiler.consume(df)
    return profiler.generate_detailed_report(quality_report)


__all__ = [
    'EnhancedTableQualityProfiler',
    'build_enhanced_qc_report',
    'build_enhanced_qc_summary',
    'build_enhanced_qc_detailed',
]
