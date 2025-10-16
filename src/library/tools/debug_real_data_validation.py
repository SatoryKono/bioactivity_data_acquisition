#!/usr/bin/env python3
"""Диагностика валидации с реальными данными."""

import os
import sys

import pandas as pd

sys.path.append(os.path.join(os.path.dirname(__file__), 'src', 'library', 'tools'))

from data_validator import validate_doi_fields


def debug_real_data_validation():
    """Диагностика валидации с реальными данными."""
    
    print("=== ДИАГНОСТИКА ВАЛИДАЦИИ С РЕАЛЬНЫМИ ДАННЫМИ ===")
    print()
    
    # Читаем реальный выходной файл
    real_file = "data/output/full/documents_20251016.csv"
    if not os.path.exists(real_file):
        print(f"Файл {real_file} не найден!")
        return
    
    try:
        df = pd.read_csv(real_file)
        print(f"1. Файл {real_file} загружен: {df.shape}")
    except Exception as e:
        print(f"Ошибка при загрузке файла: {e}")
        return
    
    # Ищем колонки DOI
    doi_cols = [col for col in df.columns if 'doi' in col.lower() and col not in ['invalid_doi', 'valid_doi']]
    print(f"2. Колонки DOI: {doi_cols}")
    
    # Проверяем значения DOI
    print("3. Значения DOI:")
    for col in doi_cols:
        non_null_count = df[col].notna().sum()
        print(f"   {col}: {non_null_count} непустых значений")
        if non_null_count > 0:
            print(f"      Примеры: {df[col].dropna().head(3).tolist()}")
    
    # Создаем DataFrame только с DOI колонками для валидации
    doi_data = df[doi_cols].copy()
    print(f"\n4. Данные для валидации: {doi_data.shape}")
    print(doi_data.head())
    
    # Валидируем DOI
    print("\n5. Валидация DOI:")
    try:
        result = validate_doi_fields(doi_data)
        print("   Валидация прошла успешно!")
        print(f"   invalid_doi: {result['invalid_doi'].tolist()}")
        print(f"   Тип invalid_doi: {result['invalid_doi'].dtype}")
        print(f"   valid_doi: {result['valid_doi'].tolist()}")
        
        # Проверяем, есть ли nan значения
        nan_count = result['invalid_doi'].isna().sum()
        print(f"   Количество nan значений: {nan_count}")
        
    except Exception as e:
        print(f"   ОШИБКА при валидации: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    debug_real_data_validation()
