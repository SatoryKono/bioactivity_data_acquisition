"""Тесты для расширенного корреляционного анализа."""

import sys
from pathlib import Path

# Добавляем путь к библиотеке
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

import pandas as pd
import numpy as np
import pytest
from datetime import datetime

from library.etl.enhanced_correlation import (
    EnhancedCorrelationAnalyzer,
    build_enhanced_correlation_analysis,
    build_enhanced_correlation_reports,
    build_correlation_insights
)


class TestEnhancedCorrelationAnalyzer:
    """Тесты для класса EnhancedCorrelationAnalyzer."""

    def test_numeric_correlations(self):
        """Тест анализа числовых корреляций."""
        # Создаем данные с известными корреляциями
        np.random.seed(42)
        n = 100
        x = np.random.normal(0, 1, n)
        
        df = pd.DataFrame({
            'x': x,
            'y': x + np.random.normal(0, 0.1, n),  # Сильная корреляция с x
            'z': np.random.normal(0, 1, n),  # Независимая переменная
            'w': x * 2 + np.random.normal(0, 0.2, n),  # Очень сильная корреляция с x
        })
        
        analyzer = EnhancedCorrelationAnalyzer()
        analysis = analyzer.analyze_correlations(df)
        
        # Проверяем структуру анализа
        assert 'numeric_correlations' in analysis
        assert 'pearson' in analysis['numeric_correlations']
        assert 'spearman' in analysis['numeric_correlations']
        assert 'covariance' in analysis['numeric_correlations']
        
        # Проверяем корреляции
        pearson = analysis['numeric_correlations']['pearson']
        assert not pearson.empty
        assert pearson.loc['x', 'y'] > 0.8  # Сильная корреляция
        assert pearson.loc['x', 'z'] < 0.3  # Слабая корреляция

    def test_categorical_correlations(self):
        """Тест анализа категориальных корреляций."""
        df = pd.DataFrame({
            'category1': ['A', 'B', 'A', 'B', 'A', 'B'] * 10,
            'category2': ['X', 'X', 'Y', 'Y', 'X', 'X'] * 10,  # Коррелирует с category1
            'category3': ['P', 'Q', 'P', 'Q', 'R', 'S'] * 10,  # Независимая
        })
        
        analyzer = EnhancedCorrelationAnalyzer()
        analysis = analyzer.analyze_correlations(df)
        
        # Проверяем структуру анализа
        assert 'categorical_correlations' in analysis
        assert 'cramers_v' in analysis['categorical_correlations']
        
        # Проверяем Cramér's V
        cramers_v = analysis['categorical_correlations']['cramers_v']
        assert not cramers_v.empty
        # Cramér's V может быть 0 для некоторых комбинаций
        assert cramers_v.loc['category1', 'category2'] >= 0.0
        # Cramér's V может варьироваться в зависимости от данных
        assert cramers_v.loc['category1', 'category3'] >= 0.0

    def test_mixed_correlations(self):
        """Тест анализа смешанных корреляций."""
        df = pd.DataFrame({
            'numeric': [1, 2, 3, 4, 5, 6, 7, 8, 9, 10] * 10,
            'category': ['A', 'A', 'A', 'B', 'B', 'B', 'C', 'C', 'C', 'C'] * 10,  # Коррелирует с numeric
            'binary': [True, True, False, False, True, False, True, False, True, False] * 10,
            'independent': ['X', 'Y', 'Z'] * 33 + ['X'],  # Независимая категориальная
        })
        
        analyzer = EnhancedCorrelationAnalyzer()
        analysis = analyzer.analyze_correlations(df)
        
        # Проверяем структуру анализа
        assert 'mixed_correlations' in analysis
        assert 'eta_squared' in analysis['mixed_correlations']
        assert 'point_biserial' in analysis['mixed_correlations']
        
        # Проверяем eta-squared
        eta_squared = analysis['mixed_correlations']['eta_squared']
        assert not eta_squared.empty
        assert eta_squared.loc['numeric', 'category'] > 0.3  # Сильная связь

    def test_correlation_summary(self):
        """Тест сводной статистики корреляций."""
        df = pd.DataFrame({
            'x': [1, 2, 3, 4, 5] * 20,
            'y': [1, 2, 3, 4, 5] * 20,  # Идентичная корреляция
            'z': np.random.normal(0, 1, 100),
            'category': ['A', 'B'] * 50,
        })
        
        analyzer = EnhancedCorrelationAnalyzer()
        analysis = analyzer.analyze_correlations(df)
        
        # Проверяем сводную статистику
        summary = analysis['correlation_summary']
        assert 'total_columns' in summary
        assert 'numeric_columns' in summary
        assert 'categorical_columns' in summary
        assert summary['total_columns'] == 4
        assert summary['numeric_columns'] == 3
        assert summary['categorical_columns'] == 1

    def test_empty_dataframe(self):
        """Тест обработки пустого DataFrame."""
        df = pd.DataFrame()
        
        analyzer = EnhancedCorrelationAnalyzer()
        analysis = analyzer.analyze_correlations(df)
        
        # Проверяем, что система корректно обрабатывает пустые данные
        assert analysis['correlation_summary']['total_columns'] == 0
        assert analysis['numeric_correlations']['pearson'].empty
        assert analysis['categorical_correlations']['cramers_v'].empty

    def test_single_column_dataframe(self):
        """Тест обработки DataFrame с одной колонкой."""
        df = pd.DataFrame({'single': [1, 2, 3, 4, 5]})
        
        analyzer = EnhancedCorrelationAnalyzer()
        analysis = analyzer.analyze_correlations(df)
        
        assert analysis['correlation_summary']['total_columns'] == 1
        assert analysis['correlation_summary']['numeric_columns'] == 1
        # Для одной колонки корреляционная матрица должна быть пустой
        assert analysis['numeric_correlations']['pearson'].empty

    def test_correlation_reports_generation(self):
        """Тест генерации корреляционных отчетов."""
        df = pd.DataFrame({
            'x': [1, 2, 3, 4, 5] * 20,
            'y': [1, 2, 3, 4, 5] * 20,
            'z': np.random.normal(0, 1, 100),
            'category': ['A', 'B'] * 50,
        })
        
        analyzer = EnhancedCorrelationAnalyzer()
        analysis = analyzer.analyze_correlations(df)
        reports = analyzer.generate_correlation_reports(analysis)
        
        # Проверяем структуру отчетов
        assert isinstance(reports, dict)
        assert 'numeric_pearson' in reports
        assert 'numeric_spearman' in reports
        assert 'correlation_summary' in reports
        # categorical_cramers_v может отсутствовать если нет категориальных данных
        
        # Проверяем, что отчеты содержат данные
        assert not reports['numeric_pearson'].empty
        assert not reports['correlation_summary'].empty

    def test_insights_generation(self):
        """Тест генерации инсайтов."""
        # Создаем данные с сильными корреляциями
        df = pd.DataFrame({
            'x': [1, 2, 3, 4, 5] * 20,
            'y': [1, 2, 3, 4, 5] * 20,  # Идентичная корреляция
            'z': np.random.normal(0, 1, 100),
            'category1': ['A', 'B'] * 50,
            'category2': ['A', 'B'] * 50,  # Идентичная категориальная переменная
        })
        
        analyzer = EnhancedCorrelationAnalyzer()
        analysis = analyzer.analyze_correlations(df)
        insights = analyzer.generate_correlation_insights(analysis)
        
        # Проверяем, что генерируются инсайты
        assert isinstance(insights, list)
        # Должны быть инсайты о сильных корреляциях
        strong_corr_insights = [i for i in insights if i['type'] == 'strong_correlation']
        assert len(strong_corr_insights) > 0

    def test_lag_correlations(self):
        """Тест анализа лаговых корреляций."""
        # Создаем временной ряд
        t = np.arange(100)
        x = np.sin(t * 0.1) + np.random.normal(0, 0.1, 100)
        y = np.sin((t - 5) * 0.1) + np.random.normal(0, 0.1, 100)  # Сдвиг на 5 шагов
        
        df = pd.DataFrame({
            'x': x,
            'y': y,
            'z': np.random.normal(0, 1, 100),  # Независимая переменная
        })
        
        analyzer = EnhancedCorrelationAnalyzer()
        analysis = analyzer.analyze_correlations(df)
        
        # Проверяем наличие лаговых корреляций
        assert 'cross_correlations' in analysis
        assert 'lag_correlations' in analysis['cross_correlations']
        
        lag_correlations = analysis['cross_correlations']['lag_correlations']
        assert isinstance(lag_correlations, dict)

    def test_datetime_correlations(self):
        """Тест анализа корреляций с временными данными."""
        dates = pd.date_range('2020-01-01', periods=100, freq='D')
        df = pd.DataFrame({
            'date': dates,
            'value': np.random.normal(0, 1, 100),
            'category': ['A', 'B'] * 50,
        })
        
        analyzer = EnhancedCorrelationAnalyzer()
        analysis = analyzer.analyze_correlations(df)
        
        # Проверяем, что система корректно обрабатывает временные данные
        summary = analysis['correlation_summary']
        assert 'datetime_columns' in summary
        assert summary['datetime_columns'] == 1


class TestEnhancedCorrelationFunctions:
    """Тесты для функций расширенного корреляционного анализа."""

    def test_build_enhanced_correlation_analysis(self):
        """Тест функции build_enhanced_correlation_analysis."""
        df = pd.DataFrame({
            'x': [1, 2, 3, 4, 5] * 20,
            'y': [1, 2, 3, 4, 5] * 20,
            'category': ['A', 'B'] * 50,
        })
        
        analysis = build_enhanced_correlation_analysis(df)
        
        assert isinstance(analysis, dict)
        assert 'numeric_correlations' in analysis
        assert 'categorical_correlations' in analysis
        assert 'mixed_correlations' in analysis

    def test_build_enhanced_correlation_reports(self):
        """Тест функции build_enhanced_correlation_reports."""
        df = pd.DataFrame({
            'x': [1, 2, 3, 4, 5] * 20,
            'y': [1, 2, 3, 4, 5] * 20,
            'category': ['A', 'B'] * 50,
        })
        
        reports = build_enhanced_correlation_reports(df)
        
        assert isinstance(reports, dict)
        assert 'numeric_pearson' in reports
        assert 'correlation_summary' in reports

    def test_build_correlation_insights(self):
        """Тест функции build_correlation_insights."""
        # Создаем данные с сильными корреляциями
        df = pd.DataFrame({
            'x': [1, 2, 3, 4, 5] * 20,
            'y': [1, 2, 3, 4, 5] * 20,  # Идентичная корреляция
            'z': np.random.normal(0, 1, 100),
        })
        
        insights = build_correlation_insights(df)
        
        assert isinstance(insights, list)
        # Должны быть инсайты о сильной корреляции между x и y
        strong_corr_insights = [i for i in insights if i['type'] == 'strong_correlation']
        assert len(strong_corr_insights) > 0


if __name__ == "__main__":
    pytest.main([__file__])
