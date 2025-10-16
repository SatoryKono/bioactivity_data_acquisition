#!/usr/bin/env python3
"""Диагностика обогащенных данных для валидации DOI."""

import os
import sys

import pandas as pd

sys.path.append(os.path.join(os.path.dirname(__file__), 'src', 'library', 'tools'))

from data_validator import validate_doi_fields


def debug_enriched_data():
    """Диагностика обогащенных данных."""
    
    print("=== ДИАГНОСТИКА ОБОГАЩЕННЫХ ДАННЫХ ===")
    print()
    
    # Создаем тестовые данные, имитирующие обогащенные данные
    test_data = pd.DataFrame({
        # Исходные поля
        'document_chembl_id': ['CHEMBL1', 'CHEMBL2', 'CHEMBL3', 'CHEMBL4'],
        'doi': ['10.1000/test1', '10.1000/test2', '10.1000/test3', '10.1000/test4'],
        'document_pubmed_id': ['123', '456', '789', '101'],
        
        # ChEMBL поля
        'chembl_doi': ['10.1000/test1', '10.1000/test2', None, '10.1000/test4'],
        'chembl_title': ['Title 1', 'Title 2', 'Title 3', 'Title 4'],
        
        # Crossref поля
        'crossref_doi': ['10.1000/test1', '10.1000/test2', '10.1000/test3', None],
        'crossref_title': ['Title 1', 'Title 2', 'Title 3', 'Title 4'],
        
        # OpenAlex поля
        'openalex_doi': [None, '10.1000/test2', '10.1000/test3', '10.1000/test4'],
        'openalex_title': ['Title 1', 'Title 2', 'Title 3', 'Title 4'],
        
        # PubMed поля
        'pubmed_doi': [None, None, None, None],
        'pubmed_title': [None, None, None, None],
        
        # Semantic Scholar поля
        'semantic_scholar_doi': [None, None, None, None],
        'semantic_scholar_title': [None, None, None, None],
    })
    
    print("1. Тестовые обогащенные данные:")
    print(test_data[['document_chembl_id', 'doi', 'chembl_doi', 'crossref_doi', 'openalex_doi', 'pubmed_doi', 'semantic_scholar_doi']])
    print()
    
    # Проверяем, какие поля DOI есть в данных
    doi_columns = [col for col in test_data.columns if 'doi' in col.lower()]
    print("2. Колонки DOI в данных:")
    for col in doi_columns:
        non_null_count = test_data[col].notna().sum()
        print(f"   {col}: {non_null_count} непустых значений")
    print()
    
    # Валидируем DOI
    print("3. Валидация DOI:")
    try:
        result = validate_doi_fields(test_data)
        print("   Валидация прошла успешно!")
        print()
        
        print("4. Результат валидации:")
        print(result[['chembl_doi', 'crossref_doi', 'openalex_doi', 'invalid_doi', 'valid_doi']])
        print()
        
        # Проверяем статистику
        if 'invalid_doi' in result.columns:
            invalid_count = result['invalid_doi'].sum()
            total_count = len(result)
            print("5. Статистика валидации:")
            print(f"   Всего записей: {total_count}")
            print(f"   Невалидных DOI: {invalid_count}")
            print(f"   Валидных DOI: {total_count - invalid_count}")
            print(f"   Процент невалидных: {invalid_count/total_count*100:.1f}%")
        else:
            print("   Колонка invalid_doi не найдена!")
            
    except Exception as e:
        print(f"   ОШИБКА при валидации: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    debug_enriched_data()
