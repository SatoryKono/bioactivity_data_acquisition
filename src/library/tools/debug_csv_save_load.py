#!/usr/bin/env python3
"""Диагностика сохранения и загрузки CSV с булевыми значениями."""

import pandas as pd
import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), 'src', 'library', 'tools'))

from data_validator import validate_doi_fields

def debug_csv_save_load():
    """Диагностика сохранения и загрузки CSV."""
    
    print("=== ДИАГНОСТИКА СОХРАНЕНИЯ И ЗАГРУЗКИ CSV ===")
    print()
    
    # Создаем тестовые данные
    test_data = pd.DataFrame({
        'chembl_doi': ['10.1021/jm00178a007', '10.1021/jm00182a017'],
        'crossref_doi': ['10.1021/jm00178a007', '10.1021/jm00182a017'],
        'openalex_doi': [None, None],
        'pubmed_doi': [None, None],
        'semantic_scholar_doi': [None, None]
    })
    
    print("1. Исходные данные:")
    print(test_data)
    print()
    
    # Валидируем данные
    print("2. Валидация данных:")
    validated_data = validate_doi_fields(test_data)
    print(f"   invalid_doi: {validated_data['invalid_doi'].tolist()}")
    print(f"   Тип invalid_doi: {validated_data['invalid_doi'].dtype}")
    print()
    
    # Сохраняем в CSV
    print("3. Сохранение в CSV:")
    test_file = "test_validation.csv"
    try:
        validated_data.to_csv(test_file, index=False)
        print(f"   Файл {test_file} сохранен успешно")
    except Exception as e:
        print(f"   Ошибка при сохранении: {e}")
        return
    
    # Загружаем из CSV
    print("4. Загрузка из CSV:")
    try:
        loaded_data = pd.read_csv(test_file)
        print(f"   Файл {test_file} загружен успешно")
        print(f"   Размер: {loaded_data.shape}")
        print(f"   invalid_doi: {loaded_data['invalid_doi'].tolist()}")
        print(f"   Тип invalid_doi: {loaded_data['invalid_doi'].dtype}")
        
        # Проверяем, есть ли nan значения
        nan_count = loaded_data['invalid_doi'].isna().sum()
        print(f"   Количество nan значений: {nan_count}")
        
    except Exception as e:
        print(f"   Ошибка при загрузке: {e}")
        return
    
    # Очищаем тестовый файл
    try:
        os.remove(test_file)
        print(f"   Тестовый файл {test_file} удален")
    except:
        pass
    
    print()
    
    # Проверяем реальный файл
    print("5. Проверка реального файла:")
    real_file = "data/output/full/documents_20251016.csv"
    if os.path.exists(real_file):
        try:
            real_data = pd.read_csv(real_file)
            print(f"   Файл {real_file} загружен")
            print(f"   invalid_doi: {real_data['invalid_doi'].tolist()}")
            print(f"   Тип invalid_doi: {real_data['invalid_doi'].dtype}")
            
            # Проверяем, есть ли nan значения
            nan_count = real_data['invalid_doi'].isna().sum()
            print(f"   Количество nan значений: {nan_count}")
            
            # Проверяем, есть ли False значения
            false_count = (real_data['invalid_doi'] == False).sum()
            print(f"   Количество False значений: {false_count}")
            
            # Проверяем, есть ли True значения
            true_count = (real_data['invalid_doi'] == True).sum()
            print(f"   Количество True значений: {true_count}")
            
        except Exception as e:
            print(f"   Ошибка при загрузке реального файла: {e}")
    else:
        print(f"   Файл {real_file} не найден")

if __name__ == "__main__":
    debug_csv_save_load()
