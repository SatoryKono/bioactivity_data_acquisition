#!/usr/bin/env python3
"""Диагностика вызова валидации в пайплайне."""

import os
import sys

import pandas as pd

sys.path.append(os.path.join(os.path.dirname(__file__), 'src', 'library', 'tools'))

# Мокаем функцию валидации, чтобы проверить, вызывается ли она
original_validate_all_fields = None

def mock_validate_all_fields(df):
    """Мок функция для проверки вызова валидации."""
    print("=== ВАЛИДАЦИЯ ВЫЗВАНА! ===")
    print(f"Размер данных: {df.shape}")
    print(f"Колонки: {list(df.columns)}")
    
    # Ищем колонки DOI
    doi_columns = [col for col in df.columns if 'doi' in col.lower()]
    print(f"Колонки DOI: {doi_columns}")
    
    # Проверяем, есть ли данные в колонках DOI
    for col in doi_columns:
        if col in df.columns:
            non_null_count = df[col].notna().sum()
            print(f"  {col}: {non_null_count} непустых значений")
    
    # Вызываем оригинальную функцию
    if original_validate_all_fields:
        result = original_validate_all_fields(df)
        print("Валидация завершена успешно!")
        return result
    else:
        # Возвращаем данные как есть, но добавляем колонки валидации
        result = df.copy()
        result['invalid_doi'] = False
        result['valid_doi'] = df.get('doi', pd.NA)
        return result

def test_validation_call():
    """Тест вызова валидации."""
    
    print("=== ТЕСТ ВЫЗОВА ВАЛИДАЦИИ ===")
    print()
    
    # Импортируем оригинальную функцию
    global original_validate_all_fields
    try:
        from data_validator import validate_all_fields
        original_validate_all_fields = validate_all_fields
        print("1. Оригинальная функция валидации импортирована успешно")
    except Exception as e:
        print(f"1. Ошибка импорта функции валидации: {e}")
        return
    
    # Создаем тестовые данные
    test_data = pd.DataFrame({
        'document_chembl_id': ['CHEMBL1', 'CHEMBL2'],
        'doi': ['10.1000/test1', '10.1000/test2'],
        'chembl_doi': ['10.1000/test1', '10.1000/test2'],
        'crossref_doi': ['10.1000/test1', '10.1000/test2'],
        'openalex_doi': [None, '10.1000/test2'],
    })
    
    print("2. Тестовые данные созданы")
    print(f"   Размер: {test_data.shape}")
    print(f"   Колонки: {list(test_data.columns)}")
    print()
    
    # Тестируем мок функцию
    print("3. Тестируем мок функцию валидации:")
    try:
        result = mock_validate_all_fields(test_data)
        print("   Мок функция работает!")
        print(f"   Результат: {result.shape}")
        if 'invalid_doi' in result.columns:
            print(f"   Колонка invalid_doi: {result['invalid_doi'].tolist()}")
        else:
            print("   Колонка invalid_doi не найдена!")
    except Exception as e:
        print(f"   Ошибка в мок функции: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_validation_call()
