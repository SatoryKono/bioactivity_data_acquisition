#!/usr/bin/env python3
"""Диагностика проблемы с валидацией DOI."""

import pandas as pd
import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), 'src', 'library', 'tools'))

from data_validator import validate_doi_fields

def debug_doi_validation_issue():
    """Диагностика проблемы с валидацией DOI."""
    
    print("=== ДИАГНОСТИКА ПРОБЛЕМЫ С ВАЛИДАЦИЕЙ DOI ===")
    print()
    
    # Создаем тестовые данные, точно такие же, как в реальном файле
    test_data = pd.DataFrame({
        'chembl_doi': ['10.1021/jm00178a007', '10.1021/jm00182a017'],
        'crossref_doi': ['10.1021/jm00178a007', '10.1021/jm00182a017'],
        'openalex_doi': ['10.1021/jm00178a007', '10.1021/jm00182a017'],
        'pubmed_doi': ['10.1021/jm00178a007', '10.1021/jm00182a017'],
        'semantic_scholar_doi': ['10.1021/jm00178a007', '10.1021/jm00182a017']
    })
    
    print("1. Тестовые данные (как в реальном файле):")
    print(test_data)
    print()
    
    # Проверяем типы данных
    print("2. Типы данных:")
    for col in test_data.columns:
        print(f"   {col}: {test_data[col].dtype}")
    print()
    
    # Валидируем данные
    print("3. Валидация данных:")
    try:
        result = validate_doi_fields(test_data)
        print("   Валидация прошла успешно!")
        print(f"   invalid_doi: {result['invalid_doi'].tolist()}")
        print(f"   Тип invalid_doi: {result['invalid_doi'].dtype}")
        print(f"   valid_doi: {result['valid_doi'].tolist()}")
        print(f"   Тип valid_doi: {result['valid_doi'].dtype}")
    except Exception as e:
        print(f"   ОШИБКА при валидации: {e}")
        import traceback
        traceback.print_exc()
        return
    
    print()
    
    # Проверяем, что происходит при сохранении и загрузке
    print("4. Тест сохранения и загрузки:")
    test_file = "test_doi_validation.csv"
    try:
        result.to_csv(test_file, index=False)
        loaded = pd.read_csv(test_file)
        print("   Файл сохранен и загружен")
        print(f"   invalid_doi после загрузки: {loaded['invalid_doi'].tolist()}")
        print(f"   Тип invalid_doi после загрузки: {loaded['invalid_doi'].dtype}")
        
        # Очищаем тестовый файл
        os.remove(test_file)
    except Exception as e:
        print(f"   Ошибка при сохранении/загрузке: {e}")
    
    print()
    
    # Проверяем, есть ли проблема с логикой валидации
    print("5. Проверка логики валидации:")
    
    # Импортируем функции валидации
    from data_validator import _is_empty_value, _normalize_value
    
    for i, row in test_data.iterrows():
        print(f"   Строка {i}:")
        
        # Проверяем, какие DOI не пустые
        doi_fields = ['chembl_doi', 'crossref_doi', 'openalex_doi', 'pubmed_doi', 'semantic_scholar_doi']
        non_empty_dois = []
        for field in doi_fields:
            if field in row and not _is_empty_value(row[field]):
                non_empty_dois.append(_normalize_value(row[field]))
        
        print(f"     Непустые DOI: {non_empty_dois}")
        print(f"     Количество непустых DOI: {len(non_empty_dois)}")
        
        if len(non_empty_dois) == 0:
            print("     Результат: invalid_doi=True, valid_doi=NA")
        elif len(non_empty_dois) == 1:
            print(f"     Результат: invalid_doi=False, valid_doi={non_empty_dois[0]}")
        else:
            # Несколько DOI - проверяем совпадения
            chembl_doi = _normalize_value(row['chembl_doi']) if not _is_empty_value(row['chembl_doi']) else None
            print(f"     ChEMBL DOI (нормализованный): {repr(chembl_doi)}")
            
            matches = 0
            mismatches = 0
            
            for doi in non_empty_dois:
                if chembl_doi is not None and doi == chembl_doi:
                    matches += 1
                elif chembl_doi is not None:
                    mismatches += 1
            
            print(f"     Совпадений: {matches}, Несовпадений: {mismatches}")
            
            if matches <= mismatches:
                print("     Результат: invalid_doi=True, valid_doi=NA")
            else:
                print(f"     Результат: invalid_doi=False, valid_doi={row['chembl_doi']}")
        print()

if __name__ == "__main__":
    debug_doi_validation_issue()
