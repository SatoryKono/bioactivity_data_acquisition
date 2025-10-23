#!/usr/bin/env python3
"""
Отладочный скрипт для отслеживания изменения типов данных в pipeline.
"""

import pandas as pd
import numpy as np
from src.library.testitem.pipeline import TestitemPipeline
from src.library.testitem.normalize import TestitemNormalizer

def debug_dtype_flow():
    """Отладка изменения типов данных."""
    
    # Создаем mock config
    class MockConfig:
        def __init__(self):
            self.sources = {}
            self.pipeline = {'version': '2.0.0'}
    
    # Тестовые данные
    test_data = pd.DataFrame({
        "molecule_chembl_id": ["CHEMBL123"],
        "num_ro5_violations": [1.0]  # float64
    })
    
    print("=== ИСХОДНЫЕ ДАННЫЕ ===")
    print(f"num_ro5_violations dtype: {test_data['num_ro5_violations'].dtype}")
    print(f"num_ro5_violations value: {test_data['num_ro5_violations'].iloc[0]}")
    print(f"num_ro5_violations type: {type(test_data['num_ro5_violations'].iloc[0])}")
    
    # Тестируем merge
    pipeline = TestitemPipeline(MockConfig())
    chembl_data = pd.DataFrame({
        "molecule_chembl_id": ["CHEMBL123"],
        "num_ro5_violations": ["2"]  # string
    })
    
    merged = pipeline._merge_chembl_data(test_data, chembl_data)
    
    print("\n=== ПОСЛЕ MERGE ===")
    print(f"num_ro5_violations dtype: {merged['num_ro5_violations'].dtype}")
    print(f"num_ro5_violations value: {merged['num_ro5_violations'].iloc[0]}")
    print(f"num_ro5_violations type: {type(merged['num_ro5_violations'].iloc[0])}")
    
    # Тестируем нормализатор
    normalizer = TestitemNormalizer(MockConfig())
    
    # Добавляем недостающие колонки
    merged = normalizer._add_missing_columns(merged)
    
    print("\n=== ПОСЛЕ _add_missing_columns ===")
    print(f"num_ro5_violations dtype: {merged['num_ro5_violations'].dtype}")
    print(f"num_ro5_violations value: {merged['num_ro5_violations'].iloc[0]}")
    print(f"num_ro5_violations type: {type(merged['num_ro5_violations'].iloc[0])}")
    
    # Тестируем нормализацию молекулярных данных
    normalized = normalizer._normalize_molecule_data(merged)
    
    print("\n=== ПОСЛЕ _normalize_molecule_data ===")
    print(f"num_ro5_violations dtype: {normalized['num_ro5_violations'].dtype}")
    print(f"num_ro5_violations value: {normalized['num_ro5_violations'].iloc[0]}")
    print(f"num_ro5_violations type: {type(normalized['num_ro5_violations'].iloc[0])}")
    
    # Проверяем, что происходит с normalize_numeric_field
    from src.library.utils.empty_value_handler import normalize_numeric_field
    
    test_values = [1.0, "2", "3.0", None, np.nan]
    print("\n=== ТЕСТ normalize_numeric_field ===")
    for val in test_values:
        result = normalize_numeric_field(val)
        print(f"Input: {val} ({type(val)}) -> Output: {result} ({type(result)})")

if __name__ == "__main__":
    debug_dtype_flow()
