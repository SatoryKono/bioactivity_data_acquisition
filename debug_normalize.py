#!/usr/bin/env python3
"""
Отладочный скрипт для проверки нормализации.
"""

import pandas as pd

from src.library.testitem.normalize import TestitemNormalizer
from src.library.utils.empty_value_handler import normalize_numeric_field


def debug_normalize():
    """Отладка нормализации."""
    
    # Создаем mock config
    class MockConfig:
        def __init__(self):
            self.sources = {}
            self.pipeline = {'version': '2.0.0'}
    
    config = MockConfig()
    TestitemNormalizer({
        'sources': config.sources,
        'pipeline': config.pipeline
    })
    
    # Тестовые данные
    test_data = pd.DataFrame({
        "molecule_chembl_id": ["CHEMBL123"],
        "num_ro5_violations": [1.0]  # float64
    })
    
    print("Before normalization:")
    print(test_data.dtypes)
    print(test_data)
    
    # Тестируем функцию нормализации
    test_value = test_data['num_ro5_violations'].iloc[0]
    print(f"\nTest value: {test_value}, type: {type(test_value)}")
    
    normalized_value = normalize_numeric_field(test_value)
    print(f"Normalized value: {normalized_value}, type: {type(normalized_value)}")
    
    # Тестируем на строке
    test_string = "1"
    print(f"\nTest string: {test_string}, type: {type(test_string)}")
    normalized_string = normalize_numeric_field(test_string)
    print(f"Normalized string: {normalized_string}, type: {type(normalized_string)}")

if __name__ == "__main__":
    debug_normalize()
