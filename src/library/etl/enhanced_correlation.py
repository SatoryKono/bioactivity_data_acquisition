"""Расширенный анализ корреляций для оценки качества данных."""

from __future__ import annotations

import warnings
from typing import Any

import numpy as np
import pandas as pd
from scipy.stats import chi2_contingency, pearsonr
from sklearn.preprocessing import LabelEncoder
from structlog.stdlib import BoundLogger

# Подавляем предупреждения о делении на ноль в NumPy
warnings.filterwarnings("ignore", category=RuntimeWarning, message="invalid value encountered in divide")


class EnhancedCorrelationAnalyzer:
    """Расширенный анализатор корреляций с поддержкой различных типов данных."""

    def __init__(self, logger: BoundLogger | None = None):
        """Инициализация анализатора корреляций."""
        self.logger = logger

    def analyze_correlations(self, df: pd.DataFrame) -> dict[str, Any]:
        """Комплексный анализ корреляций для DataFrame."""
        _log_info(self.logger, "Начинаем расширенный анализ корреляций", rows=len(df), columns=len(df.columns))

        correlation_analysis = {
            "numeric_correlations": self._analyze_numeric_correlations(df),
            "categorical_correlations": self._analyze_categorical_correlations(df),
            "mixed_correlations": self._analyze_mixed_correlations(df),
            "cross_correlations": self._analyze_cross_correlations(df),
            "correlation_summary": self._analyze_correlation_summary(df),
        }

        if self.logger:
            self.logger.info("Расширенный анализ корреляций завершен")

        return correlation_analysis

    def _analyze_numeric_correlations(self, df: pd.DataFrame) -> dict[str, pd.DataFrame]:
        """Анализ корреляций между числовыми столбцами."""
        numeric_cols = df.select_dtypes(include=[np.number]).columns

        if len(numeric_cols) < 2:
            return {
                "pearson": pd.DataFrame(),
                "spearman": pd.DataFrame(),
                "covariance": pd.DataFrame(),
            }

        numeric_df = df[numeric_cols].dropna()

        correlations = {}

        # Корреляция Пирсона
        try:
            # Проверяем, что у нас есть достаточно данных
            if len(numeric_df) > 1:
                pearson_corr = numeric_df.corr(method="pearson")
                # Заменяем NaN и inf на 0
                correlations["pearson"] = pearson_corr.fillna(0.0).replace([np.inf, -np.inf], 0.0)
            else:
                correlations["pearson"] = pd.DataFrame()
        except Exception as e:
            _log_warning(self.logger, "Ошибка при вычислении корреляции Пирсона", error=str(e))
            correlations["pearson"] = pd.DataFrame()

        # Корреляция Спирмена
        try:
            # Проверяем, что у нас есть достаточно данных
            if len(numeric_df) > 1:
                spearman_corr = numeric_df.corr(method="spearman")
                # Заменяем NaN и inf на 0
                correlations["spearman"] = spearman_corr.fillna(0.0).replace([np.inf, -np.inf], 0.0)
            else:
                correlations["spearman"] = pd.DataFrame()
        except Exception as e:
            _log_warning(self.logger, "Ошибка при вычислении корреляции Спирмена", error=str(e))
            correlations["spearman"] = pd.DataFrame()

        # Ковариационная матрица
        try:
            # Проверяем, что у нас есть достаточно данных
            if len(numeric_df) > 1:
                covariance = numeric_df.cov()
                # Заменяем NaN и inf на 0
                correlations["covariance"] = covariance.fillna(0.0).replace([np.inf, -np.inf], 0.0)
            else:
                correlations["covariance"] = pd.DataFrame()
        except Exception as e:
            _log_warning(self.logger, "Ошибка при вычислении ковариации", error=str(e))
            correlations["covariance"] = pd.DataFrame()

        return correlations

    def _analyze_categorical_correlations(self, df: pd.DataFrame) -> dict[str, Any]:
        """Анализ корреляций между категориальными столбцами."""
        categorical_cols = df.select_dtypes(include=["object", "category"]).columns

        if len(categorical_cols) < 2:
            return {
                "cramers_v": pd.DataFrame(),
                "contingency_tables": {},
                "chi2_stats": {},
            }

        correlations = {}

        # Cramér's V для категориальных данных
        cramers_v_matrix = pd.DataFrame(index=categorical_cols, columns=categorical_cols, dtype=float)

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
                        chi2_stats[f"{col1}_vs_{col2}"] = {"chi2": chi2, "p_value": p_value, "dof": dof, "cramers_v": cramers_v}

                except Exception as e:
                    _log_warning(self.logger, "Ошибка при анализе категориальных корреляций", col1=col1, col2=col2, error=str(e))
                    cramers_v_matrix.loc[col1, col2] = 0.0

        correlations["cramers_v"] = cramers_v_matrix.fillna(0.0)
        correlations["contingency_tables"] = contingency_tables
        correlations["chi2_stats"] = chi2_stats

        return correlations

    def _analyze_mixed_correlations(self, df: pd.DataFrame) -> dict[str, pd.DataFrame]:
        """Анализ корреляций между числовыми и категориальными столбцами."""
        numeric_cols = df.select_dtypes(include=[np.number]).columns
        categorical_cols = df.select_dtypes(include=["object", "category"]).columns

        if len(numeric_cols) == 0 or len(categorical_cols) == 0:
            return {
                "eta_squared": pd.DataFrame(),
                "point_biserial": pd.DataFrame(),
            }

        correlations = {}

        # Eta-squared для смешанных корреляций
        eta_squared_matrix = pd.DataFrame(index=numeric_cols, columns=categorical_cols, dtype=float)

        # Point-biserial correlation для бинарных категориальных переменных
        point_biserial_matrix = pd.DataFrame(index=numeric_cols, columns=categorical_cols, dtype=float)

        for num_col in numeric_cols:
            for cat_col in categorical_cols:
                try:
                    # Eta-squared
                    grouped = df.groupby(cat_col)[num_col]
                    n_groups = grouped.ngroups
                    if n_groups > 1:
                        # Вычисляем eta-squared
                        num_col_mean = df[num_col].mean()

                        def calculate_ss_between(mean_val):
                            def inner(x):
                                return (x.mean() - mean_val) ** 2 * len(x)

                            return inner

                        ss_between = grouped.apply(calculate_ss_between(num_col_mean)).sum()
                        ss_total = ((df[num_col] - df[num_col].mean()) ** 2).sum()

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
                        encoded = le.fit_transform(df[cat_col].fillna("missing"))
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
                    _log_warning(self.logger, "Ошибка при анализе смешанных корреляций", num_col=num_col, cat_col=cat_col, error=str(e))
                    eta_squared_matrix.loc[num_col, cat_col] = 0.0
                    point_biserial_matrix.loc[num_col, cat_col] = 0.0

        correlations["eta_squared"] = eta_squared_matrix.fillna(0.0)
        correlations["point_biserial"] = point_biserial_matrix.fillna(0.0)

        return correlations

    def _analyze_cross_correlations(self, df: pd.DataFrame) -> dict[str, Any]:
        """Анализ кросс-корреляций и лагов."""
        numeric_cols = df.select_dtypes(include=[np.number]).columns

        if len(numeric_cols) < 2:
            return {
                "lag_correlations": {},
                "rolling_correlations": {},
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
                    _log_warning(self.logger, "Ошибка при вычислении лаговых корреляций", col1=col1, col2=col2, error=str(e))

        correlations["lag_correlations"] = lag_correlations

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
                        _log_warning(self.logger, "Ошибка при вычислении скользящих корреляций", col1=col1, col2=col2, error=str(e))

        correlations["rolling_correlations"] = rolling_correlations

        return correlations

    def _analyze_correlation_summary(self, df: pd.DataFrame) -> dict[str, Any]:
        """Сводная статистика по корреляциям."""
        numeric_cols = df.select_dtypes(include=[np.number]).columns
        categorical_cols = df.select_dtypes(include=["object", "category"]).columns

        summary = {
            "total_columns": len(df.columns),
            "numeric_columns": len(numeric_cols),
            "categorical_columns": len(categorical_cols),
            "datetime_columns": len(df.select_dtypes(include=["datetime64"]).columns),
        }

        if len(numeric_cols) > 1:
            try:
                numeric_df = df[numeric_cols].dropna()
                if len(numeric_df) > 0:
                    corr_matrix = numeric_df.corr()
                    # Находим сильные корреляции (|r| > 0.7)
                    strong_correlations = []
                    for i in range(len(corr_matrix.columns)):
                        for j in range(i + 1, len(corr_matrix.columns)):
                            corr_val = corr_matrix.iloc[i, j]
                            if abs(corr_val) > 0.7:
                                strong_correlations.append({"col1": corr_matrix.columns[i], "col2": corr_matrix.columns[j], "correlation": corr_val})

                    summary["strong_numeric_correlations"] = strong_correlations
                    summary["max_correlation"] = corr_matrix.abs().max().max()
                    summary["mean_correlation"] = corr_matrix.abs().mean().mean()
            except Exception as e:
                _log_warning(self.logger, "Ошибка при анализе сводной статистики корреляций", error=str(e))

        return summary

    def generate_correlation_reports(self, correlation_analysis: dict[str, Any]) -> dict[str, pd.DataFrame]:
        """Генерация отчетов по корреляциям."""
        reports = {}

        # Отчет по числовым корреляциям
        numeric_corr = correlation_analysis["numeric_correlations"]
        if "pearson" in numeric_corr and not numeric_corr["pearson"].empty:
            reports["numeric_pearson"] = numeric_corr["pearson"]
        if "spearman" in numeric_corr and not numeric_corr["spearman"].empty:
            reports["numeric_spearman"] = numeric_corr["spearman"]
        if "covariance" in numeric_corr and not numeric_corr["covariance"].empty:
            reports["numeric_covariance"] = numeric_corr["covariance"]

        # Отчет по категориальным корреляциям
        categorical_corr = correlation_analysis["categorical_correlations"]
        if "cramers_v" in categorical_corr and not categorical_corr["cramers_v"].empty:
            reports["categorical_cramers_v"] = categorical_corr["cramers_v"]

        # Отчет по смешанным корреляциям
        mixed_corr = correlation_analysis["mixed_correlations"]
        if "eta_squared" in mixed_corr and not mixed_corr["eta_squared"].empty:
            reports["mixed_eta_squared"] = mixed_corr["eta_squared"]
        if "point_biserial" in mixed_corr and not mixed_corr["point_biserial"].empty:
            reports["mixed_point_biserial"] = mixed_corr["point_biserial"]

        # Сводный отчет
        summary = correlation_analysis["correlation_summary"]
        # Convert numpy types to Python types for proper CSV serialization
        summary_clean = {}
        for key, value in summary.items():
            if isinstance(value, (np.integer, np.floating)):
                summary_clean[key] = value.item()
            elif isinstance(value, list):
                # Convert numpy types in lists
                summary_clean[key] = [
                    {k: v.item() if isinstance(v, (np.integer, np.floating)) else v for k, v in item.items()} if isinstance(item, dict) else item for item in value
                ]
            else:
                summary_clean[key] = value

        summary_df = pd.DataFrame([summary_clean])
        reports["correlation_summary"] = summary_df

        return reports

    def generate_correlation_insights(self, correlation_analysis: dict[str, Any]) -> list[dict[str, Any]]:
        """Генерация инсайтов и рекомендаций на основе корреляций."""
        insights = []

        # Анализ сильных числовых корреляций
        numeric_corr = correlation_analysis["numeric_correlations"]
        if "pearson" in numeric_corr and not numeric_corr["pearson"].empty:
            corr_matrix = numeric_corr["pearson"]
            for i in range(len(corr_matrix.columns)):
                for j in range(i + 1, len(corr_matrix.columns)):
                    corr_val = corr_matrix.iloc[i, j]
                    if abs(corr_val) > 0.8:
                        insights.append(
                            {
                                "type": "strong_correlation",
                                "severity": "high",
                                "message": f"Сильная корреляция между {corr_matrix.columns[i]} и {corr_matrix.columns[j]}: {corr_val:.3f}",
                                "recommendation": "Рассмотрите возможность удаления одного из столбцов или создания составного признака",
                            }
                        )
                    elif abs(corr_val) > 0.7:
                        insights.append(
                            {
                                "type": "moderate_correlation",
                                "severity": "medium",
                                "message": f"Умеренная корреляция между {corr_matrix.columns[i]} и {corr_matrix.columns[j]}: {corr_val:.3f}",
                                "recommendation": "Мониторьте мультиколлинеарность при построении моделей",
                            }
                        )

        # Анализ категориальных корреляций
        categorical_corr = correlation_analysis["categorical_correlations"]
        if "cramers_v" in categorical_corr and not categorical_corr["cramers_v"].empty:
            cramers_matrix = categorical_corr["cramers_v"]
            for i in range(len(cramers_matrix.columns)):
                for j in range(i + 1, len(cramers_matrix.columns)):
                    cramers_val = cramers_matrix.iloc[i, j]
                    if cramers_val > 0.5:
                        insights.append(
                            {
                                "type": "categorical_association",
                                "severity": "medium",
                                "message": f"Сильная ассоциация между категориальными переменными {cramers_matrix.columns[i]} и {cramers_matrix.columns[j]}: {cramers_val:.3f}",
                                "recommendation": "Проанализируйте логическую связь между этими переменными",
                            }
                        )

        return insights


def _log_info(logger: BoundLogger | None, message: str, **kwargs) -> None:
    """Вспомогательная функция для логирования с поддержкой разных типов логгеров."""
    if logger:
        if hasattr(logger, "info") and hasattr(logger.info, "__code__") and "extra" in logger.info.__code__.co_varnames:
            # Структурированный логгер (structlog)
            logger.info(message, **kwargs)
        else:
            # Стандартный Python logger
            if kwargs:
                kwargs_str = ", ".join([f"{k}={v}" for k, v in kwargs.items()])
                logger.info(f"{message}: {kwargs_str}")
            else:
                logger.info(message)


def _log_warning(logger: BoundLogger | None, message: str, **kwargs) -> None:
    """Вспомогательная функция для логирования предупреждений с поддержкой разных типов логгеров."""
    if logger:
        if hasattr(logger, "warning") and hasattr(logger.warning, "__code__") and "extra" in logger.warning.__code__.co_varnames:
            # Структурированный логгер (structlog)
            logger.warning(message, **kwargs)
        else:
            # Стандартный Python logger
            if kwargs:
                kwargs_str = ", ".join([f"{k}={v}" for k, v in kwargs.items()])
                logger.warning(f"{message}: {kwargs_str}")
            else:
                logger.warning(message)


def prepare_data_for_correlation_analysis(df: pd.DataFrame, data_type: str = "general", logger: BoundLogger | None = None) -> pd.DataFrame:
    """
    Подготовка данных для корреляционного анализа.

    Args:
        df: Исходный DataFrame
        data_type: Тип данных ("documents", "assays", "general")
        logger: Логгер для записи информации

    Returns:
        Подготовленный DataFrame для корреляционного анализа
    """
    _log_info(logger, "Подготовка данных для корреляционного анализа", data_type=data_type, original_rows=len(df), original_columns=len(df.columns))

    analysis_df = df.copy()

    # Удаляем колонки с ошибками (общее для всех типов)
    error_columns = [col for col in analysis_df.columns if col.endswith("_error")]
    if error_columns:
        analysis_df = analysis_df.drop(columns=error_columns)
        _log_info(logger, "Удалены колонки с ошибками", removed_columns=error_columns)

    # Специфичная обработка для разных типов данных
    if data_type == "documents":
        # Для документов: используем все доступные колонки
        # Удаляем только технические колонки
        technical_columns = ["retrieved_at", "extracted_at", "hash_row", "hash_business_key"]
        technical_columns = [col for col in technical_columns if col in analysis_df.columns]
        if technical_columns:
            analysis_df = analysis_df.drop(columns=technical_columns)
            _log_info(logger, "Удалены технические колонки", removed_columns=technical_columns)

    elif data_type == "assays":
        # Для ассеев: конвертируем категориальные колонки в числовые
        categorical_columns = ["assay_type", "relationship_type", "src_id", "assay_organism", "assay_tissue"]
        for col in categorical_columns:
            if col in analysis_df.columns:
                # Convert to numeric codes
                analysis_df[f"{col}_numeric"] = pd.Categorical(analysis_df[col]).codes
                _log_info(logger, "Конвертирована категориальная колонка", column=col)

        # Добавляем текстовые признаки
        if "description" in analysis_df.columns:
            analysis_df["description_length"] = analysis_df["description"].fillna("").str.len()
            _log_info(logger, "Добавлен признак длины описания")

        # Добавляем хеш-признаки
        if "hash_row" in analysis_df.columns:
            analysis_df["hash_numeric"] = analysis_df["hash_row"].fillna("").str[:8].apply(lambda x: int(x, 16) if x and len(x) == 8 else 0)
            _log_info(logger, "Добавлен числовой хеш-признак")

        # Выбираем только числовые колонки (включая созданные выше)
        numeric_columns = analysis_df.select_dtypes(include=[np.number]).columns
        analysis_df = analysis_df[numeric_columns]
        _log_info(logger, "Выбраны только числовые колонки", numeric_columns=list(numeric_columns))

    elif data_type == "testitems":
        # Для testitems: конвертируем категориальные колонки в числовые
        categorical_columns = [
            "molecule_type",
            "chirality",
            "prodrug",
            "inorganic_flag",
            "polymer_flag",
            "drug_type",
            "drug_substance_flag",
            "drug_indication_flag",
            "drug_antibacterial_flag",
            "drug_antiviral_flag",
            "drug_antifungal_flag",
            "drug_antiparasitic_flag",
            "drug_antineoplastic_flag",
            "drug_immunosuppressant_flag",
            "drug_antiinflammatory_flag",
            "withdrawn_flag",
            "direct_interaction",
            "molecular_mechanism",
            "oral",
            "parenteral",
            "topical",
            "black_box_warning",
            "natural_product",
            "first_in_class",
            "atc_level1",
            "atc_level2",
            "atc_level3",
            "atc_level4",
            "atc_level5",
        ]

        for col in categorical_columns:
            if col in analysis_df.columns:
                # Convert to numeric codes
                analysis_df[f"{col}_numeric"] = pd.Categorical(analysis_df[col]).codes
                _log_info(logger, "Конвертирована категориальная колонка", column=col)

        # Добавляем текстовые признаки
        text_columns = ["pref_name", "molecule_chembl_id", "drug_name", "mechanism_of_action"]
        for col in text_columns:
            if col in analysis_df.columns:
                analysis_df[f"{col}_length"] = analysis_df[col].fillna("").str.len()
                _log_info(logger, "Добавлен признак длины текста", column=col)

        # Добавляем хеш-признаки
        if "hash_row" in analysis_df.columns:
            analysis_df["hash_numeric"] = analysis_df["hash_row"].fillna("").str[:8].apply(lambda x: int(x, 16) if x and len(x) == 8 else 0)
            _log_info(logger, "Добавлен числовой хеш-признак")

        # Выбираем только числовые колонки (включая созданные выше)
        numeric_columns = analysis_df.select_dtypes(include=[np.number]).columns
        analysis_df = analysis_df[numeric_columns]
        _log_info(logger, "Выбраны только числовые колонки", numeric_columns=list(numeric_columns))

    # Общая обработка: удаляем колонки с высоким процентом пропущенных значений
    missing_threshold = 0.5
    columns_to_keep = analysis_df.columns[analysis_df.isnull().mean() < missing_threshold]
    removed_columns = set(analysis_df.columns) - set(columns_to_keep)
    if removed_columns:
        analysis_df = analysis_df[columns_to_keep]
        _log_info(logger, "Удалены колонки с высоким процентом пропущенных значений", removed_columns=list(removed_columns), threshold=missing_threshold)

    # Удаляем колонки с нулевой дисперсией (константные значения)
    constant_columns = []
    for col in analysis_df.columns:
        try:
            if analysis_df[col].nunique() <= 1:
                constant_columns.append(col)
        except (TypeError, ValueError) as e:
            # Пропускаем колонки с нехешируемыми типами данных (например, списки)
            _log_info(logger, f"Пропущена колонка {col} из-за нехешируемого типа: {e}")
            constant_columns.append(col)

    if constant_columns:
        analysis_df = analysis_df.drop(columns=constant_columns)
        _log_info(logger, "Удалены константные колонки", removed_columns=constant_columns)

    _log_info(logger, "Подготовка данных завершена", final_rows=len(analysis_df), final_columns=len(analysis_df.columns), columns=list(analysis_df.columns))

    return analysis_df


def build_enhanced_correlation_analysis(df: pd.DataFrame, logger: BoundLogger | None = None) -> dict[str, Any]:
    """Создание расширенного анализа корреляций."""
    analyzer = EnhancedCorrelationAnalyzer(logger=logger)
    return analyzer.analyze_correlations(df)


def build_enhanced_correlation_reports(df: pd.DataFrame, logger: BoundLogger | None = None) -> dict[str, pd.DataFrame]:
    """Создание отчетов по расширенным корреляциям."""
    analyzer = EnhancedCorrelationAnalyzer(logger=logger)
    analysis = analyzer.analyze_correlations(df)
    return analyzer.generate_correlation_reports(analysis)


def build_correlation_insights(df: pd.DataFrame, logger: BoundLogger | None = None) -> list[dict[str, Any]]:
    """Создание инсайтов и рекомендаций по корреляциям."""
    analyzer = EnhancedCorrelationAnalyzer(logger=logger)
    analysis = analyzer.analyze_correlations(df)
    return analyzer.generate_correlation_insights(analysis)


__all__ = [
    "EnhancedCorrelationAnalyzer",
    "prepare_data_for_correlation_analysis",
    "build_enhanced_correlation_analysis",
    "build_enhanced_correlation_reports",
    "build_correlation_insights",
]
