#!/usr/bin/env python3
"""
Тест синхронизации схемы testitem с column_order.
"""

import pandas as pd
import sys
import os

# Добавляем src в путь
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

from library.schemas.testitem_schema import TestitemNormalizedSchema
from library.testitem.normalize import TestitemNormalizer

def test_schema_sync():
    """Тест синхронизации схемы."""
    print("=== Тест синхронизации схемы testitem ===")
    
    # Создаем тестовые данные
    test_data = pd.DataFrame({
        'molecule_chembl_id': ['CHEMBL129530', 'CHEMBL81421', 'CHEMBL283509'],
        'nstereo': [1, 1, 1]
    })
    
    print(f"Исходные данные: {test_data.shape}")
    print(f"Исходные колонки: {list(test_data.columns)}")
    
    # Тестируем нормализацию
    normalizer = TestitemNormalizer({})
    normalized_data = normalizer.normalize_testitems(test_data)
    
    print(f"\nПосле нормализации: {normalized_data.shape}")
    print(f"Колонки после нормализации: {list(normalized_data.columns)}")
    
    # Проверяем наличие ключевых колонок из column_order
    expected_columns = [
        'molecule_chembl_id', 'molregno', 'pref_name', 'parent_chembl_id',
        'max_phase', 'therapeutic_flag', 'oral', 'parenteral', 'topical',
        'drug_chembl_id', 'drug_name', 'pubchem_cid', 'pubchem_molecular_formula',
        'standardized_inchi', 'standardized_smiles', 'index', 'pipeline_version',
        'source_system', 'extracted_at', 'hash_row', 'hash_business_key'
    ]
    
    missing_columns = []
    for col in expected_columns:
        if col not in normalized_data.columns:
            missing_columns.append(col)
    
    print(f"\nПроверка ключевых колонок:")
    if missing_columns:
        print(f"❌ Отсутствующие колонки: {missing_columns}")
    else:
        print("✅ Все ключевые колонки присутствуют")
    
    # Проверяем дубликаты колонок
    duplicate_columns = []
    seen_columns = set()
    for col in normalized_data.columns:
        if col in seen_columns:
            duplicate_columns.append(col)
        else:
            seen_columns.add(col)
    
    print(f"\nПроверка дубликатов:")
    if duplicate_columns:
        print(f"❌ Дубликаты колонок: {duplicate_columns}")
    else:
        print("✅ Дубликатов колонок нет")
    
    # Тестируем схему Pandera
    try:
        schema = TestitemNormalizedSchema.get_schema()
        validated_data = schema.validate(normalized_data)
        print("✅ Pandera схема валидна")
    except Exception as e:
        print(f"❌ Ошибка валидации Pandera: {e}")
    
    print(f"\nИтоговые колонки ({len(normalized_data.columns)}):")
    for i, col in enumerate(normalized_data.columns):
        print(f"{i+1:3d}. {col}")
    
    return normalized_data

if __name__ == "__main__":
    result = test_schema_sync()
    print(f"\n✅ Тест завершен. Результат: {result.shape}")