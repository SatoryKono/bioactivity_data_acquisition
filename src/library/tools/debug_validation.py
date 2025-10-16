#!/usr/bin/env python3
"""Диагностика проблемы с валидацией DOI."""

import pandas as pd
import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), 'src', 'library', 'tools'))

from data_validator import validate_doi_fields

def debug_validation():
    """Диагностика валидации DOI."""
    
    print("=== ДИАГНОСТИКА ВАЛИДАЦИИ DOI ===")
    print()
    
    # Создаем тестовые данные с разными сценариями
    test_data = pd.DataFrame({
        'chembl_doi': ['10.1000/test1', '10.1000/test2', None, '10.1000/test4', '10.1000/test5'],
        'crossref_doi': ['10.1000/test1', '10.1000/test2', '10.1000/test3', None, '10.1000/test6'],
        'openalex_doi': [None, '10.1000/test2', '10.1000/test3', '10.1000/test4', None],
        'pubmed_doi': [None, None, None, None, None],
        'semantic_scholar_doi': [None, None, None, None, None]
    })
    
    print("1. Исходные данные:")
    print(test_data)
    print()
    
    # Проверяем, какие поля есть в данных
    print("2. Поля в DataFrame:")
    for col in test_data.columns:
        print(f"   {col}: {test_data[col].dtype}")
    print()
    
    # Проверяем, есть ли пустые значения
    print("3. Пустые значения:")
    for col in test_data.columns:
        null_count = test_data[col].isnull().sum()
        print(f"   {col}: {null_count} пустых значений")
    print()
    
    # Валидируем DOI
    print("4. Валидация DOI:")
    try:
        result = validate_doi_fields(test_data)
        print("   Валидация прошла успешно!")
        print()
        
        print("5. Результат валидации:")
        print(result[['chembl_doi', 'crossref_doi', 'openalex_doi', 'invalid_doi', 'valid_doi']])
        print()
        
        # Проверяем, есть ли колонки валидации
        print("6. Колонки валидации:")
        if 'invalid_doi' in result.columns:
            print(f"   invalid_doi: найдена, тип {result['invalid_doi'].dtype}")
            print(f"   Значения: {result['invalid_doi'].tolist()}")
        else:
            print("   invalid_doi: НЕ НАЙДЕНА!")
            
        if 'valid_doi' in result.columns:
            print(f"   valid_doi: найдена, тип {result['valid_doi'].dtype}")
            print(f"   Значения: {result['valid_doi'].tolist()}")
        else:
            print("   valid_doi: НЕ НАЙДЕНА!")
            
    except Exception as e:
        print(f"   ОШИБКА при валидации: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    debug_validation()
