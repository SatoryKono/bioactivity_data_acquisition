#!/usr/bin/env python3
"""
Скрипт для анализа выходного CSV файла документов.
Проверяет количество строк, колонки CHEMBL.SOURCE.* и другие метрики.
"""

import pandas as pd
import sys
from pathlib import Path

def analyze_document_csv(file_path: str):
    """Анализирует CSV файл с документами."""
    print(f"Анализ файла: {file_path}")
    print("=" * 60)
    
    try:
        # Загружаем CSV
        df = pd.read_csv(file_path)
        
        # Основная статистика
        print(f"Общее количество строк: {len(df)}")
        print(f"Общее количество колонок: {len(df.columns)}")
        print()
        
        # Анализ CHEMBL.SOURCE.* колонок
        source_cols = [c for c in df.columns if 'CHEMBL.SOURCE' in c]
        print(f"Колонки CHEMBL.SOURCE.*: {len(source_cols)}")
        for col in source_cols:
            non_null = df[col].notna().sum()
            null_count = df[col].isna().sum()
            print(f"  {col}:")
            print(f"    - Непустых значений: {non_null}")
            print(f"    - Пустых значений: {null_count}")
            if non_null > 0:
                unique_vals = df[col].dropna().unique()
                print(f"    - Уникальных значений: {len(unique_vals)}")
                if len(unique_vals) <= 5:
                    print(f"    - Значения: {list(unique_vals)}")
        print()
        
        # Анализ CHEMBL.DOCS.* колонок
        docs_cols = [c for c in df.columns if 'CHEMBL.DOCS' in c]
        print(f"Колонки CHEMBL.DOCS.*: {len(docs_cols)}")
        for col in docs_cols:
            non_null = df[col].notna().sum()
            print(f"  {col}: {non_null} непустых значений")
        print()
        
        # Анализ других колонок
        other_cols = [c for c in df.columns if 'CHEMBL' not in c]
        print(f"Другие колонки: {len(other_cols)}")
        for col in other_cols[:10]:  # Показываем первые 10
            non_null = df[col].notna().sum()
            print(f"  {col}: {non_null} непустых значений")
        if len(other_cols) > 10:
            print(f"  ... и еще {len(other_cols) - 10} колонок")
        print()
        
        # Проверка на дубликаты
        if 'document_chembl_id' in df.columns:
            duplicates = df['document_chembl_id'].duplicated().sum()
            print(f"Дубликаты document_chembl_id: {duplicates}")
            if duplicates > 0:
                print("Дубликаты:")
                dup_ids = df[df['document_chembl_id'].duplicated(keep=False)]['document_chembl_id'].unique()
                for dup_id in dup_ids[:5]:
                    print(f"  {dup_id}")
        print()
        
        # Проверка ошибок
        error_cols = [c for c in df.columns if 'error' in c.lower()]
        if error_cols:
            print(f"Колонки с ошибками: {error_cols}")
            for col in error_cols:
                errors = df[col].notna().sum()
                print(f"  {col}: {errors} ошибок")
                if errors > 0:
                    error_types = df[col].dropna().value_counts()
                    print(f"    Типы ошибок: {dict(error_types.head())}")
        print()
        
        # Общая статистика по заполненности
        print("Статистика заполненности колонок:")
        for col in df.columns:
            non_null = df[col].notna().sum()
            percentage = (non_null / len(df)) * 100
            print(f"  {col}: {non_null}/{len(df)} ({percentage:.1f}%)")
            
    except Exception as e:
        print(f"Ошибка при анализе файла: {e}")
        return False
    
    return True

def main():
    """Главная функция."""
    if len(sys.argv) != 2:
        print("Использование: python analyze_document_output.py <путь_к_csv_файлу>")
        sys.exit(1)
    
    file_path = sys.argv[1]
    if not Path(file_path).exists():
        print(f"Файл не найден: {file_path}")
        sys.exit(1)
    
    success = analyze_document_csv(file_path)
    sys.exit(0 if success else 1)

if __name__ == "__main__":
    main()
