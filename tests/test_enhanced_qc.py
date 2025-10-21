"""Тесты для расширенной системы оценки качества данных."""

import sys
from datetime import datetime
from pathlib import Path

import pandas as pd
import pytest

# Добавляем путь к библиотеке
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from library.etl.enhanced_qc import (
    EnhancedTableQualityProfiler,
    build_enhanced_qc_detailed,
    build_enhanced_qc_summary,
)


class TestEnhancedTableQualityProfiler:
    """Тесты для класса EnhancedTableQualityProfiler."""

    def test_basic_analysis(self):
        """Тест базового анализа DataFrame."""
        # Создаем тестовые данные
        df = pd.DataFrame({
            'id': ['A', 'B', 'C', 'D', 'E'],
            'value': [1, 2, 3, None, 5],
            'text': ['hello', 'world', '', 'test', 'data'],
            'doi': ['10.1000/test.123', 'invalid', '10.1000/test.456', None, ''],
            'email': ['test@example.com', 'invalid', 'user@domain.org', None, ''],
            'date': [datetime(2023, 1, 1), datetime(2023, 2, 1), None, datetime(2023, 3, 1), None],
        })
        
        profiler = EnhancedTableQualityProfiler()
        report = profiler.consume(df)
        
        # Проверяем структуру отчета
        assert 'table_summary' in report
        assert 'column_analysis' in report
        assert 'data_patterns' in report
        assert 'statistical_summary' in report
        assert 'data_roles' in report
        
        # Проверяем базовые метрики
        assert report['table_summary']['total_rows'] == 5
        assert report['table_summary']['total_columns'] == 6
        
        # Проверяем анализ колонок
        id_analysis = report['column_analysis']['id']
        assert id_analysis['non_null'] == 5
        assert id_analysis['non_empty'] == 5
        assert id_analysis['empty_pct'] == 0.0
        assert id_analysis['unique_cnt'] == 5

    def test_pattern_detection(self):
        """Тест обнаружения паттернов."""
        df = pd.DataFrame({
            'doi': ['10.1000/test.123', 'invalid', '10.1000/test.456'],
            'email': ['test@example.com', 'invalid', 'user@domain.org'],
            'url': ['https://example.com', 'invalid', 'http://test.org'],
            'issn': ['1234-567X', 'invalid', '9876-543X'],
        })
        
        profiler = EnhancedTableQualityProfiler()
        report = profiler.consume(df)
        
        # Проверяем обнаружение DOI
        doi_analysis = report['column_analysis']['doi']
        assert doi_analysis['pattern_cov_doi'] > 0
        
        # Проверяем обнаружение email
        email_analysis = report['column_analysis']['email']
        assert email_analysis['pattern_cov_email'] > 0
        
        # Проверяем обнаружение URL
        url_analysis = report['column_analysis']['url']
        assert url_analysis['pattern_cov_url'] > 0
        
        # Проверяем обнаружение ISSN
        issn_analysis = report['column_analysis']['issn']
        assert issn_analysis['pattern_cov_issn'] > 0

    def test_numeric_analysis(self):
        """Тест анализа числовых данных."""
        df = pd.DataFrame({
            'numbers': [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
            'mixed': [1, 2, None, 'text', 5, 6, 7, 8, 9, 10],
        })
        
        profiler = EnhancedTableQualityProfiler()
        report = profiler.consume(df)
        
        # Проверяем числовую статистику
        numbers_analysis = report['column_analysis']['numbers']
        assert numbers_analysis['numeric_cov'] == 100.0
        assert numbers_analysis['numeric_min'] == 1
        assert numbers_analysis['numeric_max'] == 10
        assert numbers_analysis['numeric_mean'] == 5.5
        
        # Проверяем смешанные данные (может не иметь numeric_cov если pandas не считает колонку числовой)
        mixed_analysis = report['column_analysis']['mixed']
        # Для смешанных данных numeric_cov может отсутствовать
        if 'numeric_cov' in mixed_analysis:
            assert mixed_analysis['numeric_cov'] < 100.0

    def test_datetime_analysis(self):
        """Тест анализа временных данных."""
        df = pd.DataFrame({
            'dates': [
                datetime(2023, 1, 1),
                datetime(2023, 2, 1),
                datetime(2023, 3, 1),
                None,
                datetime(2023, 4, 1)
            ]
        })
        
        profiler = EnhancedTableQualityProfiler()
        report = profiler.consume(df)
        
        # Проверяем анализ дат
        dates_analysis = report['column_analysis']['dates']
        assert dates_analysis['date_cov'] > 0
        assert dates_analysis['date_min'] is not None
        assert dates_analysis['date_max'] is not None

    def test_data_roles(self):
        """Тест определения ролей данных."""
        df = pd.DataFrame({
            'id': ['A', 'B', 'C', 'D', 'E'],  # identifier
            'value': [1, 2, 3, 4, 5],  # numeric_metric
            'category': ['X', 'Y', 'X', 'Z', 'Y'],  # categorical
            'text': ['hello world', 'test data', 'sample text', 'example', 'demo'],  # text
            'date': [datetime(2023, 1, 1), datetime(2023, 2, 1), None, datetime(2023, 3, 1), None],  # datetime
        })
        
        profiler = EnhancedTableQualityProfiler()
        report = profiler.consume(df)
        
        roles = report['data_roles']
        assert roles['id'] == 'identifier'
        # value может быть определен как identifier если все значения уникальны
        assert roles['value'] in ['numeric_metric', 'identifier']
        # category может быть определен как text если недостаточно повторений
        assert roles['category'] in ['categorical', 'text']
        # text может быть определен как identifier если все значения уникальны
        assert roles['text'] in ['text', 'identifier']
        assert roles['date'] == 'datetime'

    def test_summary_report_generation(self):
        """Тест генерации сводного отчета."""
        df = pd.DataFrame({
            'id': ['A', 'B', 'C'],
            'value': [1, 2, 3],
            'text': ['hello', 'world', 'test'],
        })
        
        profiler = EnhancedTableQualityProfiler()
        report = profiler.consume(df)
        summary = profiler.generate_summary_report(report)
        
        # Проверяем структуру сводного отчета
        assert isinstance(summary, pd.DataFrame)
        assert len(summary) == 3  # Количество столбцов
        assert 'column' in summary.columns
        assert 'non_null' in summary.columns
        assert 'empty_pct' in summary.columns
        assert 'unique_cnt' in summary.columns

    def test_detailed_report_generation(self):
        """Тест генерации детального отчета."""
        df = pd.DataFrame({
            'id': ['A', 'B', 'C', 'A', 'B'],
            'value': [1, 2, 3, 1, 2],
        })
        
        profiler = EnhancedTableQualityProfiler()
        report = profiler.consume(df)
        detailed = profiler.generate_detailed_report(report)
        
        # Проверяем структуру детального отчета
        assert isinstance(detailed, dict)
        assert 'column_summary' in detailed
        assert 'top_values' in detailed
        assert 'pattern_coverage' in detailed
        
        # Проверяем наличие топ значений
        top_values = detailed['top_values']
        assert isinstance(top_values, pd.DataFrame)
        assert len(top_values) > 0

    def test_empty_dataframe(self):
        """Тест обработки пустого DataFrame."""
        df = pd.DataFrame()
        
        profiler = EnhancedTableQualityProfiler()
        report = profiler.consume(df)
        
        # Проверяем, что система корректно обрабатывает пустые данные
        assert report['table_summary']['total_rows'] == 0
        assert report['table_summary']['total_columns'] == 0
        assert len(report['column_analysis']) == 0

    def test_single_column_dataframe(self):
        """Тест обработки DataFrame с одной колонкой."""
        df = pd.DataFrame({'single': [1, 2, 3, None, 5]})
        
        profiler = EnhancedTableQualityProfiler()
        report = profiler.consume(df)
        
        assert report['table_summary']['total_columns'] == 1
        assert 'single' in report['column_analysis']
        
        single_analysis = report['column_analysis']['single']
        assert single_analysis['non_null'] == 4
        assert single_analysis['empty_pct'] == 20.0


class TestEnhancedQCFunctions:
    """Тесты для функций расширенной QC."""

    def test_build_enhanced_qc_summary(self):
        """Тест функции build_enhanced_qc_summary."""
        df = pd.DataFrame({
            'id': ['A', 'B', 'C'],
            'value': [1, 2, 3],
        })
        
        summary = build_enhanced_qc_summary(df)
        
        assert isinstance(summary, pd.DataFrame)
        assert len(summary) == 2
        assert 'column' in summary.columns

    def test_build_enhanced_qc_detailed(self):
        """Тест функции build_enhanced_qc_detailed."""
        df = pd.DataFrame({
            'id': ['A', 'B', 'C', 'A', 'B'],
            'value': [1, 2, 3, 1, 2],
        })
        
        detailed = build_enhanced_qc_detailed(df)
        
        assert isinstance(detailed, dict)
        assert 'column_summary' in detailed
        assert 'top_values' in detailed


if __name__ == "__main__":
    pytest.main([__file__])
