#!/usr/bin/env python3
"""Диагностика вызова валидации в пайплайне."""

import pandas as pd
import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

def debug_pipeline_validation_call():
    """Диагностика вызова валидации в пайплайне."""
    
    print("=== ДИАГНОСТИКА ВЫЗОВА ВАЛИДАЦИИ В ПАЙПЛАЙНЕ ===")
    print()
    
    # Проверяем, есть ли проблема с импортом
    print("1. Проверка импорта валидатора:")
    try:
        from library.tools.data_validator import validate_all_fields
        print("   Импорт validate_all_fields: OK")
    except Exception as e:
        print(f"   Ошибка импорта validate_all_fields: {e}")
        return
    
    # Проверяем, есть ли проблема с импортом пайплайна
    print("2. Проверка импорта пайплайна:")
    try:
        print("   Импорт write_document_outputs: OK")
    except Exception as e:
        print(f"   Ошибка импорта write_document_outputs: {e}")
        return
    
    # Создаем тестовые данные
    print("3. Создание тестовых данных:")
    test_data = pd.DataFrame({
        'document_chembl_id': ['CHEMBL1', 'CHEMBL2'],
        'doi': ['10.1021/jm00178a007', '10.1021/jm00182a017'],
        'chembl_doi': ['10.1021/jm00178a007', '10.1021/jm00182a017'],
        'crossref_doi': ['10.1021/jm00178a007', '10.1021/jm00182a017'],
        'openalex_doi': ['10.1021/jm00178a007', '10.1021/jm00182a017'],
        'pubmed_doi': ['10.1021/jm00178a007', '10.1021/jm00182a017'],
        'semantic_scholar_doi': ['10.1021/jm00178a007', '10.1021/jm00182a017']
    })
    print(f"   Тестовые данные созданы: {test_data.shape}")
    
    # Тестируем валидацию напрямую
    print("4. Тест валидации напрямую:")
    try:
        validated_data = validate_all_fields(test_data)
        print("   Валидация прошла успешно!")
        print(f"   invalid_doi: {validated_data['invalid_doi'].tolist()}")
        print(f"   Тип invalid_doi: {validated_data['invalid_doi'].dtype}")
    except Exception as e:
        print(f"   Ошибка при валидации: {e}")
        import traceback
        traceback.print_exc()
        return
    
    print()
    
    # Проверяем, есть ли проблема с сохранением
    print("5. Тест сохранения данных:")
    test_file = "test_pipeline_validation.csv"
    try:
        validated_data.to_csv(test_file, index=False)
        print(f"   Файл {test_file} сохранен успешно")
        
        # Загружаем и проверяем
        loaded_data = pd.read_csv(test_file)
        print(f"   Файл загружен: {loaded_data.shape}")
        print(f"   invalid_doi: {loaded_data['invalid_doi'].tolist()}")
        print(f"   Тип invalid_doi: {loaded_data['invalid_doi'].dtype}")
        
        # Очищаем тестовый файл
        os.remove(test_file)
        print("   Тестовый файл удален")
        
    except Exception as e:
        print(f"   Ошибка при сохранении/загрузке: {e}")
        import traceback
        traceback.print_exc()
    
    print()
    
    # Проверяем, есть ли проблема с реальным пайплайном
    print("6. Проверка реального пайплайна:")
    
    # Проверяем, есть ли ошибки в логах или исключения
    print("   Проверяем возможные проблемы...")
    
    # Проверяем, есть ли проблема с типами данных в реальном файле
    real_file = "data/output/full/documents_20251016.csv"
    if os.path.exists(real_file):
        try:
            real_data = pd.read_csv(real_file)
            print(f"   Реальный файл загружен: {real_data.shape}")
            
            # Проверяем, есть ли проблема с типами данных
            print("   Проверка типов данных:")
            for col in ['invalid_doi', 'invalid_journal', 'invalid_year']:
                if col in real_data.columns:
                    print(f"     {col}: {real_data[col].dtype}")
                    print(f"       Значения: {real_data[col].tolist()}")
            
        except Exception as e:
            print(f"   Ошибка при загрузке реального файла: {e}")

if __name__ == "__main__":
    debug_pipeline_validation_call()