#!/usr/bin/env python3
"""Тестовый скрипт для демонстрации расширенной системы оценки качества данных."""

import sys
from pathlib import Path

# Добавляем путь к библиотеке
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from structlog import get_logger

from library.etl.enhanced_qc import EnhancedTableQualityProfiler, build_enhanced_qc_summary


def create_test_data() -> pd.DataFrame:
    """Создание тестового набора данных для демонстрации."""
    
    np.random.seed(42)
    n_rows = 1000
    
    data = {
        # Идентификаторы
        'compound_id': [f"CHEMBL{i:06d}" for i in range(1, n_rows + 1)],
        'target_id': np.random.choice([f"TARGET_{i:03d}" for i in range(1, 51)], n_rows),
        
        # DOI и публикации
        'doi': [
            f"10.1000/journal.{i}.{np.random.randint(1000, 9999)}" 
            if np.random.random() > 0.2 else None 
            for i in range(1, n_rows + 1)
        ],
        'pmid': [
            f"{np.random.randint(10000000, 99999999)}" 
            if np.random.random() > 0.15 else None 
            for i in range(n_rows)
        ],
        'issn': [
            f"{np.random.randint(1000, 9999)}-{np.random.randint(100, 999)}X" 
            if np.random.random() > 0.7 else None 
            for i in range(n_rows)
        ],
        'isbn': [
            f"978-{np.random.randint(100, 999)}-{np.random.randint(10000, 99999)}-{np.random.randint(0, 9)}" 
            if np.random.random() > 0.8 else None 
            for i in range(n_rows)
        ],
        
        # URL и email
        'url': [
            f"https://example.com/paper/{i}" 
            if np.random.random() > 0.6 else None 
            for i in range(n_rows)
        ],
        'email': [
            f"researcher{i}@university.edu" 
            if np.random.random() > 0.9 else None 
            for i in range(n_rows)
        ],
        
        # Числовые данные
        'activity_value': np.random.normal(7.5, 2.0, n_rows),
        'standard_value': np.random.normal(100.0, 50.0, n_rows),
        'confidence_score': np.random.uniform(0.0, 1.0, n_rows),
        
        # Булевы значения
        'is_active': np.random.choice([True, False, None], n_rows, p=[0.6, 0.3, 0.1]),
        'has_structure': np.random.choice(['true', 'false', 'yes', 'no'], n_rows, p=[0.4, 0.3, 0.2, 0.1]),
        
        # Даты
        'publication_date': [
            datetime(2020, 1, 1) + timedelta(days=np.random.randint(0, 365*4)) 
            if np.random.random() > 0.1 else None 
            for i in range(n_rows)
        ],
        'assay_date': [
            datetime(2022, 1, 1) + timedelta(days=np.random.randint(0, 365*2)) 
            if np.random.random() > 0.05 else None 
            for i in range(n_rows)
        ],
        
        # Текстовые данные
        'title': [
            f"Research Paper {i}: Study of Compound Activity" 
            if np.random.random() > 0.05 else None 
            for i in range(n_rows)
        ],
        'abstract': [
            f"This is a detailed abstract for research paper {i} that describes the methodology and results." 
            if np.random.random() > 0.2 else None 
            for i in range(n_rows)
        ],
        
        # Категориальные данные
        'assay_type': np.random.choice(['binding', 'functional', 'adme', 'safety'], n_rows, p=[0.4, 0.3, 0.2, 0.1]),
        'organism': np.random.choice(['human', 'mouse', 'rat', 'other'], n_rows, p=[0.5, 0.2, 0.2, 0.1]),
        
        # Данные с пропусками
        'missing_data': [None if np.random.random() > 0.5 else f"value_{i}" for i in range(n_rows)],
        'empty_strings': ["" if np.random.random() > 0.3 else f"data_{i}" for i in range(n_rows)],
    }
    
    df = pd.DataFrame(data)
    
    # Добавляем несколько дублированных строк
    duplicates = df.sample(n=50, random_state=42)
    df = pd.concat([df, duplicates], ignore_index=True)
    
    return df


def main():
    """Основная функция для демонстрации расширенной QC системы."""
    
    # Настройка логирования
    logger = get_logger()
    logger.info("Создание тестового набора данных")
    
    # Создание тестовых данных
    df = create_test_data()
    logger.info("Тестовые данные созданы", rows=len(df), columns=len(df.columns))
    
    # Инициализация профайлера
    profiler = EnhancedTableQualityProfiler(logger=logger)
    
    # Анализ качества данных
    logger.info("Начинаем расширенный анализ качества данных")
    quality_report = profiler.consume(df)
    
    # Генерация сводного отчета
    summary_report = profiler.generate_summary_report(quality_report)
    
    # Сохранение результатов
    output_dir = Path("reports")
    output_dir.mkdir(exist_ok=True)
    
    # Сводный отчет
    summary_path = output_dir / "enhanced_qc_summary.csv"
    summary_report.to_csv(summary_path, index=False)
    logger.info("Сводный отчет сохранен", path=str(summary_path))
    
    # Детальные отчеты
    detailed_reports = profiler.generate_detailed_report(quality_report)
    for report_name, report_df in detailed_reports.items():
        report_path = output_dir / f"enhanced_qc_{report_name}.csv"
        report_df.to_csv(report_path, index=False)
        logger.info("Детальный отчет сохранен", report_name=report_name, path=str(report_path))
    
    # Вывод ключевых метрик
    print("\n=== РЕЗУЛЬТАТЫ АНАЛИЗА КАЧЕСТВА ДАННЫХ ===")
    print(f"Общее количество строк: {quality_report['table_summary']['total_rows']}")
    print(f"Общее количество колонок: {quality_report['table_summary']['total_columns']}")
    print(f"Использование памяти: {quality_report['table_summary']['memory_usage_bytes'] / 1024 / 1024:.2f} MB")
    
    print("\n=== АНАЛИЗ ПО КОЛОНКАМ ===")
    for col, analysis in list(quality_report['column_analysis'].items())[:10]:  # Показываем первые 10 колонок
        print(f"\n{col}:")
        print(f"  - Непустые значения: {analysis['non_null']} ({100 - analysis['empty_pct']:.1f}%)")
        print(f"  - Уникальные значения: {analysis['unique_cnt']} ({analysis['unique_pct_of_non_empty']:.1f}%)")
        print(f"  - Роль данных: {quality_report['data_roles'][col]}")
        
        # Показываем паттерны для текстовых данных
        if 'pattern_cov_doi' in analysis:
            patterns = [k for k, v in analysis.items() if k.startswith('pattern_cov_') and v > 0]
            if patterns:
                print(f"  - Обнаруженные паттерны: {', '.join([p.replace('pattern_cov_', '') for p in patterns])}")
        
        # Показываем статистики для числовых данных
        if 'numeric_mean' in analysis and analysis['numeric_mean'] > 0:
            print(f"  - Среднее значение: {analysis['numeric_mean']:.2f}")
            print(f"  - Стандартное отклонение: {analysis['numeric_std']:.2f}")
    
    print(f"\n=== ОБНАРУЖЕННЫЕ ПАТТЕРНЫ ===")
    for col, patterns in quality_report['data_patterns'].items():
        if patterns:
            print(f"{col}: {patterns}")
    
    print(f"\n=== СТАТИСТИЧЕСКАЯ ИНФОРМАЦИЯ ===")
    stats = quality_report['statistical_summary']
    print(f"Числовые колонки: {len(stats['numeric_columns'])}")
    print(f"Категориальные колонки: {len(stats['categorical_columns'])}")
    print(f"Колонки с датами: {len(stats['datetime_columns'])}")
    print(f"Дублированные строки: {stats['duplicate_rows']}")
    
    logger.info("Анализ качества данных завершен успешно")


if __name__ == "__main__":
    main()
