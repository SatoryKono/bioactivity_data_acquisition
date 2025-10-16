#!/usr/bin/env python3
"""Диагностика реальных DOI данных."""

import pandas as pd

def debug_real_doi_data():
    """Диагностика реальных DOI данных."""
    
    print("=== ДИАГНОСТИКА РЕАЛЬНЫХ DOI ДАННЫХ ===")
    print()
    
    # Читаем реальный файл
    real_file = "data/output/full/documents_20251016.csv"
    try:
        df = pd.read_csv(real_file)
        print(f"1. Файл {real_file} загружен")
        print(f"   Размер: {df.shape}")
    except Exception as e:
        print(f"1. Ошибка при загрузке файла: {e}")
        return
    
    # Ищем колонки DOI
    doi_cols = [col for col in df.columns if 'doi' in col.lower()]
    print(f"2. Колонки DOI: {doi_cols}")
    print()
    
    # Проверяем значения DOI
    print("3. Значения DOI:")
    for col in doi_cols:
        values = df[col].tolist()
        non_null_count = df[col].notna().sum()
        print(f"   {col}: {non_null_count} непустых значений")
        print(f"      Значения: {values}")
        print(f"      Тип: {df[col].dtype}")
        print()
    
    # Проверяем колонки валидации
    print("4. Колонки валидации:")
    validation_cols = [col for col in df.columns if 'invalid_' in col or 'valid_' in col]
    for col in validation_cols:
        values = df[col].tolist()
        non_null_count = df[col].notna().sum()
        print(f"   {col}: {non_null_count} непустых значений")
        print(f"      Значения: {values}")
        print(f"      Тип: {df[col].dtype}")
        print()

if __name__ == "__main__":
    debug_real_doi_data()
