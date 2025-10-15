#!/usr/bin/env python3
"""Анализ результатов исправленного пайплайна."""

import sys
import pandas as pd
from pathlib import Path

def analyze_results(csv_path: str):
    """Анализирует результаты исправленного пайплайна."""
    
    print(f"Анализ исправленного пайплайна: {csv_path}")
    print("=" * 60)
    
    # Загружаем данные
    try:
        df = pd.read_csv(csv_path)
    except Exception as e:
        print(f"Ошибка загрузки файла: {e}")
        return
    
    total_rows = len(df)
    total_columns = len(df.columns)
    
    print(f"Всего строк: {total_rows}")
    print(f"Всего колонок: {total_columns}")
    print()
    
    # Анализ по источникам
    print("АНАЛИЗ ПО ИСТОЧНИКАМ:")
    print("-" * 60)
    
    source_columns = {
        'ChEMBL': [col for col in df.columns if col.startswith('chembl_')],
        'Crossref': [col for col in df.columns if col.startswith('crossref_')],
        'OpenAlex': [col for col in df.columns if col.startswith('openalex_')],
        'PubMed': [col for col in df.columns if col.startswith('pubmed_')],
        'Semantic Scholar': [col for col in df.columns if col.startswith('semantic_scholar_')],
        'Основные': ['document_chembl_id', 'title', 'doi', 'pubmed_id', 'journal', 'year', 'volume', 'issue', 'first_page', 'last_page', 'month', 'abstract', 'authors']
    }
    
    for source, columns in source_columns.items():
        if not columns:
            continue
            
        filled_count = 0
        total_cells = 0
        for col in columns:
            if col in df.columns:
                filled_count += df[col].notna().sum()
                total_cells += total_rows
        
        if total_cells > 0:
            fill_percentage = (filled_count / total_cells) * 100
            print(f"{source:<20} {filled_count:4d}/{total_cells:4d} ячеек ({fill_percentage:5.1f}%)")
    
    # Общая статистика
    print()
    print("ОБЩАЯ СТАТИСТИКА:")
    print("-" * 60)
    
    total_cells = total_rows * total_columns
    filled_cells = df.notna().sum().sum()
    overall_fill_percentage = (filled_cells / total_cells) * 100
    
    print(f"Всего ячеек: {total_cells:,}")
    print(f"Заполненных ячеек: {filled_cells:,}")
    print(f"Общий процент заполнения: {overall_fill_percentage:.1f}%")
    
    # Проверяем, есть ли полностью заполненные строки
    fully_filled_rows = df.notna().all(axis=1).sum()
    print(f"Полностью заполненных строк: {fully_filled_rows}/{total_rows} ({fully_filled_rows/total_rows*100:.1f}%)")
    
    # Проверяем строки с минимальным заполнением
    min_fill_per_row = df.notna().sum(axis=1).min()
    max_fill_per_row = df.notna().sum(axis=1).max()
    avg_fill_per_row = df.notna().sum(axis=1).mean()
    
    print(f"Минимальное заполнение в строке: {min_fill_per_row}/{total_columns} ({min_fill_per_row/total_columns*100:.1f}%)")
    print(f"Максимальное заполнение в строке: {max_fill_per_row}/{total_columns} ({max_fill_per_row/total_columns*100:.1f}%)")
    print(f"Среднее заполнение в строке: {avg_fill_per_row:.1f}/{total_columns} ({avg_fill_per_row/total_columns*100:.1f}%)")
    
    # Показываем примеры заполненных полей
    print()
    print("ПРИМЕРЫ ЗАПОЛНЕННЫХ ПОЛЕЙ:")
    print("-" * 60)
    
    for source, columns in source_columns.items():
        if not columns:
            continue
            
        filled_columns = []
        for col in columns:
            if col in df.columns and df[col].notna().sum() > 0:
                filled_columns.append(col)
        
        if filled_columns:
            print(f"{source}: {', '.join(filled_columns[:5])}{'...' if len(filled_columns) > 5 else ''}")

def main():
    if len(sys.argv) != 2:
        print("Использование: python analyze_fixed_results.py <путь_к_csv_файлу>")
        sys.exit(1)
    
    csv_path = sys.argv[1]
    if not Path(csv_path).exists():
        print(f"Файл не найден: {csv_path}")
        sys.exit(1)
    
    analyze_results(csv_path)

if __name__ == "__main__":
    main()
