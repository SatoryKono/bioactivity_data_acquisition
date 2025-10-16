"""Расширенный анализ корреляций для оценки качества данных."""

from __future__ import annotations

import warnings
import numpy as np
import pandas as pd
from scipy.stats import chi2_contingency, pearsonr
from sklearn.preprocessing import LabelEncoder
from structlog.stdlib import BoundLogger
from typing import Any, Dict, List, Optional

# Подавляем предупреждения о делении на ноль в NumPy
warnings.filterwarnings('ignore', category=RuntimeWarning, message='invalid value encountered in divide')


class EnhancedCorrelationAnalyzer:
    """Расширенный анализатор корреляций с поддержкой различных типов данных."""

    def __init__(self, logger: Optional[BoundLogger] = None):
        """Инициализация анализатора корреляций."""
        self.logger = logger

    def analyze_correlations(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Комплексный анализ корреляций для DataFrame."""
        if self.logger:
            self.logger.info("Начинаем расширенный анализ корреляций", 
                           rows=len(df), columns=len(df.columns))

        correlation_analysis = {
            'numeric_correlations': self._analyze_numeric_correlations(df),
            'categorical_correlations': self._analyze_categorical_correlations(df),
            'mixed_correlations': self._analyze_mixed_correlations(df),
            'cross_correlations': self._analyze_cross_correlations(df),
            'correlation_summary': self._analyze_correlation_summary(df),
        }

        if self.logger:
            self.logger.info("Расширенный анализ корреляций завершен")
        
        return correlation_analysis

    def _analyze_numeric_correlations(self, df: pd.DataFrame) -> Dict[str, pd.DataFrame]:
        """Анализ корреляций между числовыми столбцами."""
        numeric_cols = df.select_dtypes(include=[np.number]).columns
        
        if len(numeric_cols) < 2:
            return {
                'pearson': pd.DataFrame(),
                'spearman': pd.DataFrame(),
                'covariance': pd.DataFrame(),
            }

        numeric_df = df[numeric_cols].dropna()

        correlations = {}
        
        # Корреляция Пирсона
        try:
            # Проверяем, что у нас есть достаточно данных
            if len(numeric_df) > 1:
                pearson_corr = numeric_df.corr(method='pearson')
                # Заменяем NaN и inf на 0
                correlations['pearson'] = pearson_corr.fillna(0.0).replace([np.inf, -np.inf], 0.0)
            else:
                correlations['pearson'] = pd.DataFrame()
        except Exception as e:
            if self.logger:
                self.logger.warning("Ошибка при вычислении корреляции Пирсона", error=str(e))
            correlations['pearson'] = pd.DataFrame()

        # Корреляция Спирмена
        try:
            # Проверяем, что у нас есть достаточно данных
            if len(numeric_df) > 1:
                spearman_corr = numeric_df.corr(method='spearman')
                # Заменяем NaN и inf на 0
                correlations['spearman'] = spearman_corr.fillna(0.0).replace([np.inf, -np.inf], 0.0)
            else:
                correlations['spearman'] = pd.DataFrame()
        except Exception as e:
            if self.logger:
                self.logger.warning("Ошибка при вычислении корреляции Спирмена", error=str(e))
            correlations['spearman'] = pd.DataFrame()

        # Ковариационная матрица
        try:
            # Проверяем, что у нас есть достаточно данных
            if len(numeric_df) > 1:
                covariance = numeric_df.cov()
                # Заменяем NaN и inf на 0
                correlations['covariance'] = covariance.fillna(0.0).replace([np.inf, -np.inf], 0.0)
            else:
                correlations['covariance'] = pd.DataFrame()
        except Exception as e:
            if self.logger:
                self.logger.warning("Ошибка при вычислении ковариации", error=str(e))
            correlations['covariance'] = pd.DataFrame()

        return correlations

    def _analyze_categorical_correlations(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Анализ корреляций между категориальными столбцами."""
        categorical_cols = df.select_dtypes(include=['object', 'category']).columns
        
        if len(categorical_cols) < 2:
            return {
                'cramers_v': pd.DataFrame(),
                'contingency_tables': {},
                'chi2_stats': {},
            }

        correlations = {}
        
        # Cramér's V для категориальных данных
        cramers_v_matrix = pd.DataFrame(
            index=categorical_cols, 
            columns=categorical_cols, 
            dtype=float
        )
        
        contingency_tables = {}
        chi2_stats = {}

        for col1 in categorical_cols:
            for col2 in categorical_cols:
                if col1 == col2:
                    cramers_v_matrix.loc[col1, col2] = 1.0
                    continue

                try:
                    # Создаем таблицу сопряженности
                    contingency = pd.crosstab(df[col1], df[col2])
                    
                    # Вычисляем Cramér's V с проверкой деления на ноль
                    chi2, p_value, dof, expected = chi2_contingency(contingency)
                    n = contingency.sum().sum()
                    
                    # Проверяем, что n > 0 и есть вариация в данных
                    if n > 0 and min(contingency.shape) > 1:
                        denominator = n * (min(contingency.shape) - 1)
                        if denominator > 0:
                            cramers_v = np.sqrt(chi2 / denominator)
                        else:
                            cramers_v = 0.0
                    else:
                        cramers_v = 0.0
                    
                    cramers_v_matrix.loc[col1, col2] = cramers_v
                    
                    # Сохраняем таблицу сопряженности для сильных корреляций
                    if cramers_v > 0.3:
                        contingency_tables[f"{col1}_vs_{col2}"] = contingency
                        chi2_stats[f"{col1}_vs_{col2}"] = {
                            'chi2': chi2,
                            'p_value': p_value,
                            'dof': dof,
                            'cramers_v': cramers_v
                        }

                except Exception as e:
                    if self.logger:
                        self.logger.warning("Ошибка при анализе категориальных корреляций", 
                                          col1=col1, col2=col2, error=str(e))
                    cramers_v_matrix.loc[col1, col2] = 0.0

        correlations['cramers_v'] = cramers_v_matrix.fillna(0.0)
        correlations['contingency_tables'] = contingency_tables
        correlations['chi2_stats'] = chi2_stats

        return correlations

    def _analyze_mixed_correlations(self, df: pd.DataFrame) -> Dict[str, pd.DataFrame]:
        """Анализ корреляций между числовыми и категориальными столбцами."""
        numeric_cols = df.select_dtypes(include=[np.number]).columns
        categorical_cols = df.select_dtypes(include=['object', 'category']).columns
        
        if len(numeric_cols) == 0 or len(categorical_cols) == 0:
            return {
                'eta_squared': pd.DataFrame(),
                'point_biserial': pd.DataFrame(),
            }

        correlations = {}
        
        # Eta-squared для смешанных корреляций
        eta_squared_matrix = pd.DataFrame(
            index=numeric_cols,
            columns=categorical_cols,
            dtype=float
        )

        # Point-biserial correlation для бинарных категориальных переменных
        point_biserial_matrix = pd.DataFrame(
            index=numeric_cols,
            columns=categorical_cols,
            dtype=float
        )

        for num_col in numeric_cols:
            for cat_col in categorical_cols:
                try:
                    # Eta-squared
                    grouped = df.groupby(cat_col)[num_col]
                    n_groups = grouped.ngroups
                    if n_groups > 1:
                        # Вычисляем eta-squared
                        ss_between = grouped.apply(lambda x: (x.mean() - df[num_col].mean())**2 * len(x)).sum()
                        ss_total = ((df[num_col] - df[num_col].mean())**2).sum()
                        
                        # Проверяем деление на ноль для eta-squared
                        if ss_total > 0 and not np.isnan(ss_total):
                            eta_squared = ss_between / ss_total
                            # Ограничиваем значение от 0 до 1
                            eta_squared = max(0.0, min(1.0, eta_squared))
                        else:
                            eta_squared = 0.0
                        eta_squared_matrix.loc[num_col, cat_col] = eta_squared
                    else:
                        eta_squared_matrix.loc[num_col, cat_col] = 0.0

                    # Point-biserial correlation для бинарных переменных
                    unique_values = df[cat_col].dropna().unique()
                    if len(unique_values) == 2:
                        # Кодируем категориальные значения как 0 и 1
                        le = LabelEncoder()
                        encoded = le.fit_transform(df[cat_col].fillna('missing'))
                        if len(np.unique(encoded)) == 2:
                            # Проверяем, что у нас есть вариация в данных
                            num_data = df[num_col].fillna(0)
                            if num_data.std() > 0:  # Есть вариация в числовых данных
                                correlation, _ = pearsonr(num_data, encoded)
                                # Проверяем на NaN и inf
                                if not np.isnan(correlation) and not np.isinf(correlation):
                                    point_biserial_matrix.loc[num_col, cat_col] = correlation
                                else:
                                    point_biserial_matrix.loc[num_col, cat_col] = 0.0
                            else:
                                point_biserial_matrix.loc[num_col, cat_col] = 0.0
                        else:
                            point_biserial_matrix.loc[num_col, cat_col] = 0.0
                    else:
                        point_biserial_matrix.loc[num_col, cat_col] = 0.0

                except Exception as e:
                    if self.logger:
                        self.logger.warning("Ошибка при анализе смешанных корреляций", 
                                          num_col=num_col, cat_col=cat_col, error=str(e))
                    eta_squared_matrix.loc[num_col, cat_col] = 0.0
                    point_biserial_matrix.loc[num_col, cat_col] = 0.0

        correlations['eta_squared'] = eta_squared_matrix.fillna(0.0)
        correlations['point_biserial'] = point_biserial_matrix.fillna(0.0)

        return correlations

    def _analyze_cross_correlations(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Анализ кросс-корреляций и лагов."""
        numeric_cols = df.select_dtypes(include=[np.number]).columns
        
        if len(numeric_cols) < 2:
            return {
                'lag_correlations': {},
                'rolling_correlations': {},
            }

        correlations = {}
        
        # Лаговые корреляции для временных рядов
        lag_correlations = {}
        max_lags = min(10, len(df) // 4)  # Максимум 10 лагов или 1/4 от длины данных
        
        for i, col1 in enumerate(numeric_cols):
            for j, col2 in enumerate(numeric_cols):
                if i >= j:  # Избегаем дублирования
                    continue
                
                try:
                    # Вычисляем корреляции для разных лагов
                    lag_corr = {}
                    for lag in range(0, max_lags + 1):
                        if lag == 0:
                            # Нулевой лаг - обычная корреляция
                            corr = df[col1].corr(df[col2])
                        else:
                            # Положительный лаг - col1 опережает col2
                            if len(df) > lag:
                                corr = df[col1].iloc[:-lag].corr(df[col2].iloc[lag:])
                            else:
                                corr = np.nan
                        
                        if not np.isnan(corr):
                            lag_corr[f"lag_{lag}"] = corr
                    
                    if lag_corr:
                        lag_correlations[f"{col1}_vs_{col2}"] = lag_corr

                except Exception as e:
                    if self.logger:
                        self.logger.warning("Ошибка при вычислении лаговых корреляций", 
                                          col1=col1, col2=col2, error=str(e))

        correlations['lag_correlations'] = lag_correlations

        # Скользящие корреляции
        rolling_correlations = {}
        window_size = min(50, len(df) // 10)  # Окно для скользящих корреляций
        
        if window_size > 10:
            for i, col1 in enumerate(numeric_cols):
                for j, col2 in enumerate(numeric_cols):
                    if i >= j:
                        continue
                    
                    try:
                        rolling_corr = df[col1].rolling(window=window_size).corr(df[col2])
                        rolling_correlations[f"{col1}_vs_{col2}"] = rolling_corr.dropna().to_dict()
                    except Exception as e:
                        if self.logger:
                            self.logger.warning("Ошибка при вычислении скользящих корреляций", 
                                              col1=col1, col2=col2, error=str(e))

        correlations['rolling_correlations'] = rolling_correlations

        return correlations

    def _analyze_correlation_summary(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Сводная статистика по корреляциям."""
        numeric_cols = df.select_dtypes(include=[np.number]).columns
        categorical_cols = df.select_dtypes(include=['object', 'category']).columns
        
        summary = {
            'total_columns': len(df.columns),
            'numeric_columns': len(numeric_cols),
            'categorical_columns': len(categorical_cols),
            'datetime_columns': len(df.select_dtypes(include=['datetime64']).columns),
        }

        if len(numeric_cols) > 1:
            try:
                numeric_df = df[numeric_cols].dropna()
                if len(numeric_df) > 0:
                    corr_matrix = numeric_df.corr()
                    # Находим сильные корреляции (|r| > 0.7)
                    strong_correlations = []
                    for i in range(len(corr_matrix.columns)):
                        for j in range(i+1, len(corr_matrix.columns)):
                            corr_val = corr_matrix.iloc[i, j]
                            if abs(corr_val) > 0.7:
                                strong_correlations.append({
                                    'col1': corr_matrix.columns[i],
                                    'col2': corr_matrix.columns[j],
                                    'correlation': corr_val
                                })
                    
                    summary['strong_numeric_correlations'] = strong_correlations
                    summary['max_correlation'] = corr_matrix.abs().max().max()
                    summary['mean_correlation'] = corr_matrix.abs().mean().mean()
            except Exception as e:
                if self.logger:
                    self.logger.warning("Ошибка при анализе сводной статистики корреляций", error=str(e))

        return summary

    def generate_correlation_reports(self, correlation_analysis: Dict[str, Any]) -> Dict[str, pd.DataFrame]:
        """Генерация отчетов по корреляциям."""
        reports = {}
        
        # Отчет по числовым корреляциям
        numeric_corr = correlation_analysis['numeric_correlations']
        if 'pearson' in numeric_corr and not numeric_corr['pearson'].empty:
            reports['numeric_pearson'] = numeric_corr['pearson']
        if 'spearman' in numeric_corr and not numeric_corr['spearman'].empty:
            reports['numeric_spearman'] = numeric_corr['spearman']
        if 'covariance' in numeric_corr and not numeric_corr['covariance'].empty:
            reports['numeric_covariance'] = numeric_corr['covariance']

        # Отчет по категориальным корреляциям
        categorical_corr = correlation_analysis['categorical_correlations']
        if 'cramers_v' in categorical_corr and not categorical_corr['cramers_v'].empty:
            reports['categorical_cramers_v'] = categorical_corr['cramers_v']

        # Отчет по смешанным корреляциям
        mixed_corr = correlation_analysis['mixed_correlations']
        if 'eta_squared' in mixed_corr and not mixed_corr['eta_squared'].empty:
            reports['mixed_eta_squared'] = mixed_corr['eta_squared']
        if 'point_biserial' in mixed_corr and not mixed_corr['point_biserial'].empty:
            reports['mixed_point_biserial'] = mixed_corr['point_biserial']

        # Сводный отчет
        summary = correlation_analysis['correlation_summary']
        summary_df = pd.DataFrame([summary])
        reports['correlation_summary'] = summary_df

        return reports

    def generate_correlation_insights(self, correlation_analysis: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Генерация инсайтов и рекомендаций на основе корреляций."""
        insights = []
        
        # Анализ сильных числовых корреляций
        numeric_corr = correlation_analysis['numeric_correlations']
        if 'pearson' in numeric_corr and not numeric_corr['pearson'].empty:
            corr_matrix = numeric_corr['pearson']
            for i in range(len(corr_matrix.columns)):
                for j in range(i+1, len(corr_matrix.columns)):
                    corr_val = corr_matrix.iloc[i, j]
                    if abs(corr_val) > 0.8:
                        insights.append({
                            'type': 'strong_correlation',
                            'severity': 'high',
                            'message': f"Сильная корреляция между {corr_matrix.columns[i]} и {corr_matrix.columns[j]}: {corr_val:.3f}",
                            'recommendation': "Рассмотрите возможность удаления одного из столбцов или создания составного признака"
                        })
                    elif abs(corr_val) > 0.7:
                        insights.append({
                            'type': 'moderate_correlation',
                            'severity': 'medium',
                            'message': f"Умеренная корреляция между {corr_matrix.columns[i]} и {corr_matrix.columns[j]}: {corr_val:.3f}",
                            'recommendation': "Мониторьте мультиколлинеарность при построении моделей"
                        })

        # Анализ категориальных корреляций
        categorical_corr = correlation_analysis['categorical_correlations']
        if 'cramers_v' in categorical_corr and not categorical_corr['cramers_v'].empty:
            cramers_matrix = categorical_corr['cramers_v']
            for i in range(len(cramers_matrix.columns)):
                for j in range(i+1, len(cramers_matrix.columns)):
                    cramers_val = cramers_matrix.iloc[i, j]
                    if cramers_val > 0.5:
                        insights.append({
                            'type': 'categorical_association',
                            'severity': 'medium',
                            'message': f"Сильная ассоциация между категориальными переменными {cramers_matrix.columns[i]} и {cramers_matrix.columns[j]}: {cramers_val:.3f}",
                            'recommendation': "Проанализируйте логическую связь между этими переменными"
                        })

        return insights


def build_enhanced_correlation_analysis(df: pd.DataFrame, logger: Optional[BoundLogger] = None) -> Dict[str, Any]:
    """Создание расширенного анализа корреляций."""
    analyzer = EnhancedCorrelationAnalyzer(logger=logger)
    return analyzer.analyze_correlations(df)


def build_enhanced_correlation_reports(df: pd.DataFrame, logger: Optional[BoundLogger] = None) -> Dict[str, pd.DataFrame]:
    """Создание отчетов по расширенным корреляциям."""
    analyzer = EnhancedCorrelationAnalyzer(logger=logger)
    analysis = analyzer.analyze_correlations(df)
    return analyzer.generate_correlation_reports(analysis)


def build_correlation_insights(df: pd.DataFrame, logger: Optional[BoundLogger] = None) -> List[Dict[str, Any]]:
    """Создание инсайтов и рекомендаций по корреляциям."""
    analyzer = EnhancedCorrelationAnalyzer(logger=logger)
    analysis = analyzer.analyze_correlations(df)
    return analyzer.generate_correlation_insights(analysis)


__all__ = [
    'EnhancedCorrelationAnalyzer',
    'build_enhanced_correlation_analysis',
    'build_enhanced_correlation_reports',
    'build_correlation_insights',
]
