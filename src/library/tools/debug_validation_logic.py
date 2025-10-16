#!/usr/bin/env python3
"""Диагностика логики валидации."""

import pandas as pd
import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), 'src', 'library', 'tools'))

from data_validator import validate_doi_fields, _is_empty_value, _normalize_value

def debug_validation_logic():
    """Диагностика логики валидации."""
    
    print("=== ДИАГНОСТИКА ЛОГИКИ ВАЛИДАЦИИ ===")
    print()
    
    # Создаем тестовые данные, аналогичные реальным
    test_data = pd.DataFrame({
        'chembl_doi': ['10.1021/jm00178a007', '10.1021/jm00182a017'],
        'crossref_doi': ['10.1021/jm00178a007', '10.1021/jm00182a017'],
        'openalex_doi': [None, None],
        'pubmed_doi': [None, None],
        'semantic_scholar_doi': [None, None]
    })
    
    print("1. Тестовые данные:")
    print(test_data)
    print()
    
    # Проверяем функции _is_empty_value и _normalize_value
    print("2. Тестируем функции _is_empty_value и _normalize_value:")
    test_values = ['10.1021/jm00178a007', None, pd.NA, '', '   ']
    for val in test_values:
        is_empty = _is_empty_value(val)
        normalized = _normalize_value(val)
        print(f"   Значение: {repr(val)} -> is_empty: {is_empty}, normalized: {repr(normalized)}")
    print()
    
    # Проверяем логику валидации пошагово
    print("3. Пошаговая диагностика валидации:")
    
    for i, row in test_data.iterrows():
        print(f"   Строка {i}:")
        print(f"     chembl_doi: {repr(row['chembl_doi'])}")
        print(f"     crossref_doi: {repr(row['crossref_doi'])}")
        
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
    
    # Тестируем полную валидацию
    print("4. Полная валидация:")
    try:
        result = validate_doi_fields(test_data)
        print("   Валидация прошла успешно!")
        print(f"   invalid_doi: {result['invalid_doi'].tolist()}")
        print(f"   valid_doi: {result['valid_doi'].tolist()}")
    except Exception as e:
        print(f"   ОШИБКА при валидации: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    debug_validation_logic()
