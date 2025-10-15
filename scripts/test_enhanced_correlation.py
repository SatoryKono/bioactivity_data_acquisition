#!/usr/bin/env python3
"""Тестовый скрипт для демонстрации расширенного корреляционного анализа."""

import sys
from pathlib import Path

# Добавляем путь к библиотеке
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from structlog import get_logger

from library.etl.enhanced_correlation import (
    EnhancedCorrelationAnalyzer,
    build_enhanced_correlation_analysis,
    build_enhanced_correlation_reports,
    build_correlation_insights
)


def create_test_data_with_correlations() -> pd.DataFrame:
    """Создание тестового набора данных с различными типами корреляций."""
    
    np.random.seed(42)
    n_rows = 1000
    
    # Создаем базовые переменные
    base_value = np.random.normal(100, 20, n_rows)
    
    data = {
        # Сильно коррелированные числовые переменные
        'activity_ic50': base_value + np.random.normal(0, 5, n_rows),
        'activity_ec50': base_value * 0.8 + np.random.normal(0, 3, n_rows),  # Сильная корреляция с IC50
        'activity_ki': base_value * 1.2 + np.random.normal(0, 8, n_rows),   # Умеренная корреляция
        
        # Независимые числовые переменные
        'molecular_weight': np.random.normal(400, 100, n_rows),
        'logp': np.random.normal(2.5, 1.5, n_rows),
        'tpsa': np.random.normal(80, 30, n_rows),
        
        # Категориальные переменные с корреляциями
        'assay_type': np.random.choice(['binding', 'functional', 'adme', 'safety'], n_rows, 
                                     p=[0.4, 0.3, 0.2, 0.1]),
        'organism': np.random.choice(['human', 'mouse', 'rat', 'other'], n_rows, 
                                   p=[0.5, 0.2, 0.2, 0.1]),
        'target_class': np.random.choice(['GPCR', 'Enzyme', 'Ion Channel', 'Nuclear Receptor'], n_rows,
                                       p=[0.3, 0.4, 0.2, 0.1]),
        
        # Связанные категориальные переменные
        'high_activity': ['high' if x > 120 else 'low' for x in base_value],
        'activity_category': ['very_high' if x > 130 else 'high' if x > 110 else 'medium' if x > 90 else 'low' 
                            for x in base_value],
        
        # Булевы переменные
        'is_drug_like': np.random.choice([True, False], n_rows, p=[0.7, 0.3]),
        'has_side_effects': np.random.choice([True, False], n_rows, p=[0.4, 0.6]),
        
        # Временные данные
        'publication_year': np.random.randint(2015, 2024, n_rows),
        'assay_date': [datetime(2020, 1, 1) + timedelta(days=np.random.randint(0, 365*4)) 
                      for _ in range(n_rows)],
        
        # Смешанные корреляции (числовые vs категориальные)
        'efficacy_score': base_value / 100 + np.random.normal(0, 0.1, n_rows),  # Коррелирует с активностью
        'safety_score': np.random.normal(0.8, 0.2, n_rows),
        
        # Дополнительные переменные для анализа
        'compound_id': [f"CHEMBL{i:06d}" for i in range(1, n_rows + 1)],
        'doi': [f"10.1000/journal.{i}.{np.random.randint(1000, 9999)}" 
               if np.random.random() > 0.2 else None 
               for i in range(1, n_rows + 1)],
        'journal_impact_factor': np.random.exponential(3, n_rows),
    }
    
    df = pd.DataFrame(data)
    
    # Добавляем некоторые пропущенные значения
    df.loc[df.sample(frac=0.05, random_state=42).index, 'efficacy_score'] = None
    df.loc[df.sample(frac=0.03, random_state=43).index, 'safety_score'] = None
    
    # Создаем искусственные корреляции между категориальными переменными
    # GPCR чаще встречается с binding assay
    gpcr_mask = df['target_class'] == 'GPCR'
    df.loc[gpcr_mask, 'assay_type'] = np.random.choice(['binding', 'functional'], 
                                                      size=gpcr_mask.sum(), 
                                                      p=[0.7, 0.3])
    
    # Enzyme чаще встречается с functional assay
    enzyme_mask = df['target_class'] == 'Enzyme'
    df.loc[enzyme_mask, 'assay_type'] = np.random.choice(['functional', 'binding'], 
                                                        size=enzyme_mask.sum(), 
                                                        p=[0.8, 0.2])
    
    return df


def main():
    """Основная функция для демонстрации расширенного корреляционного анализа."""
    
    # Настройка логирования
    logger = get_logger()
    logger.info("Создание тестового набора данных с корреляциями")
    
    # Создание тестовых данных
    df = create_test_data_with_correlations()
    logger.info("Тестовые данные созданы", rows=len(df), columns=len(df.columns))
    
    # Инициализация анализатора
    analyzer = EnhancedCorrelationAnalyzer(logger=logger)
    
    # Анализ корреляций
    logger.info("Начинаем расширенный корреляционный анализ")
    correlation_analysis = analyzer.analyze_correlations(df)
    
    # Генерация отчетов
    correlation_reports = analyzer.generate_correlation_reports(correlation_analysis)
    
    # Генерация инсайтов
    insights = analyzer.generate_correlation_insights(correlation_analysis)
    
    # Сохранение результатов
    output_dir = Path("reports")
    output_dir.mkdir(exist_ok=True)
    
    # Сохранение корреляционных отчетов
    for report_name, report_df in correlation_reports.items():
        report_path = output_dir / f"enhanced_correlation_{report_name}.csv"
        report_df.to_csv(report_path, index=True)
        logger.info("Корреляционный отчет сохранен", report_name=report_name, path=str(report_path))
    
    # Сохранение инсайтов
    if insights:
        insights_df = pd.DataFrame(insights)
        insights_path = output_dir / "correlation_insights.csv"
        insights_df.to_csv(insights_path, index=False)
        logger.info("Отчет с инсайтами сохранен", path=str(insights_path))
    
    # Сохранение детального анализа в JSON
    import json
    analysis_path = output_dir / "correlation_analysis.json"
    
    def convert_numpy(obj):
        """Конвертация numpy типов в JSON-совместимые."""
        if isinstance(obj, np.integer):
            return int(obj)
        elif isinstance(obj, np.floating):
            return float(obj)
        elif isinstance(obj, np.ndarray):
            return obj.tolist()
        elif isinstance(obj, pd.DataFrame):
            return obj.to_dict()
        return obj
    
    with open(analysis_path, 'w', encoding='utf-8') as f:
        json.dump(correlation_analysis, f, default=convert_numpy, ensure_ascii=False, indent=2)
    
    logger.info("Детальный анализ сохранен", path=str(analysis_path))
    
    # Вывод ключевых результатов
    print("\n=== РЕЗУЛЬТАТЫ РАСШИРЕННОГО КОРРЕЛЯЦИОННОГО АНАЛИЗА ===")
    
    summary = correlation_analysis['correlation_summary']
    print(f"Общее количество столбцов: {summary['total_columns']}")
    print(f"Числовые столбцы: {summary['numeric_columns']}")
    print(f"Категориальные столбцы: {summary['categorical_columns']}")
    print(f"Временные столбцы: {summary['datetime_columns']}")
    
    if 'max_correlation' in summary:
        print(f"Максимальная корреляция: {summary['max_correlation']:.3f}")
        print(f"Средняя корреляция: {summary['mean_correlation']:.3f}")
    
    if 'strong_numeric_correlations' in summary:
        strong_corr = summary['strong_numeric_correlations']
        print(f"\n=== СИЛЬНЫЕ КОРРЕЛЯЦИИ (|r| > 0.7) ===")
        for corr in strong_corr:
            print(f"{corr['col1']} <-> {corr['col2']}: {corr['correlation']:.3f}")
    
    # Анализ числовых корреляций
    numeric_corr = correlation_analysis['numeric_correlations']
    if 'pearson' in numeric_corr and not numeric_corr['pearson'].empty:
        print(f"\n=== ЧИСЛОВЫЕ КОРРЕЛЯЦИИ (ПИРСОН) ===")
        pearson_df = numeric_corr['pearson']
        print("Топ-5 самых сильных корреляций:")
        # Находим самые сильные корреляции
        corr_pairs = []
        for i in range(len(pearson_df.columns)):
            for j in range(i+1, len(pearson_df.columns)):
                corr_val = pearson_df.iloc[i, j]
                corr_pairs.append((pearson_df.columns[i], pearson_df.columns[j], abs(corr_val), corr_val))
        
        corr_pairs.sort(key=lambda x: x[2], reverse=True)
        for col1, col2, abs_corr, corr in corr_pairs[:5]:
            print(f"{col1} <-> {col2}: {corr:.3f}")
    
    # Анализ категориальных корреляций
    categorical_corr = correlation_analysis['categorical_correlations']
    if 'cramers_v' in categorical_corr and not categorical_corr['cramers_v'].empty:
        print(f"\n=== КАТЕГОРИАЛЬНЫЕ КОРРЕЛЯЦИИ (CRAMER'S V) ===")
        cramers_df = categorical_corr['cramers_v']
        print("Топ-5 самых сильных ассоциаций:")
        # Находим самые сильные ассоциации
        assoc_pairs = []
        for i in range(len(cramers_df.columns)):
            for j in range(i+1, len(cramers_df.columns)):
                assoc_val = cramers_df.iloc[i, j]
                assoc_pairs.append((cramers_df.columns[i], cramers_df.columns[j], assoc_val))
        
        assoc_pairs.sort(key=lambda x: x[2], reverse=True)
        for col1, col2, assoc in assoc_pairs[:5]:
            print(f"{col1} <-> {col2}: {assoc:.3f}")
    
    # Анализ смешанных корреляций
    mixed_corr = correlation_analysis['mixed_correlations']
    if 'eta_squared' in mixed_corr and not mixed_corr['eta_squared'].empty:
        print(f"\n=== СМЕШАННЫЕ КОРРЕЛЯЦИИ (ETA-SQUARED) ===")
        eta_df = mixed_corr['eta_squared']
        print("Топ-5 самых сильных связей числовых и категориальных переменных:")
        # Находим самые сильные связи
        mixed_pairs = []
        for num_col in eta_df.index:
            for cat_col in eta_df.columns:
                eta_val = eta_df.loc[num_col, cat_col]
                mixed_pairs.append((num_col, cat_col, eta_val))
        
        mixed_pairs.sort(key=lambda x: x[2], reverse=True)
        for num_col, cat_col, eta in mixed_pairs[:5]:
            print(f"{num_col} <-> {cat_col}: {eta:.3f}")
    
    # Вывод инсайтов
    if insights:
        print(f"\n=== ИНСАЙТЫ И РЕКОМЕНДАЦИИ ===")
        for i, insight in enumerate(insights[:10], 1):  # Показываем первые 10 инсайтов
            print(f"{i}. [{insight['severity'].upper()}] {insight['message']}")
            print(f"   Рекомендация: {insight['recommendation']}")
            print()
    
    logger.info("Расширенный корреляционный анализ завершен успешно")


if __name__ == "__main__":
    main()
