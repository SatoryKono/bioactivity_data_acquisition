#!/usr/bin/env python3
"""
Отладочный скрипт для проверки merge логики.
"""

import pandas as pd
import numpy as np
from src.library.testitem.pipeline import TestitemPipeline

def debug_merge():
    """Отладка merge логики."""
    
    # Создаем mock config
    class MockConfig:
        def __init__(self):
            self.sources = {}
    
    pipeline = TestitemPipeline(MockConfig())
    
    # Тестовые данные
    base_data = pd.DataFrame({
        "molecule_chembl_id": ["CHEMBL123"],
        "num_ro5_violations": [0.0]  # float64
    })
    
    chembl_data = pd.DataFrame({
        "molecule_chembl_id": ["CHEMBL123"],
        "num_ro5_violations": ["1"]  # string
    })
    
    print("Base data types:")
    print(base_data.dtypes)
    print("\nChEMBL data types:")
    print(chembl_data.dtypes)
    
    # Вызываем merge
    merged = pipeline._merge_chembl_data(base_data, chembl_data)
    
    print("\nMerged data types:")
    print(merged.dtypes)
    print("\nMerged data:")
    print(merged)
    
    # Проверяем конкретное значение
    print(f"\nnum_ro5_violations value: {merged['num_ro5_violations'].iloc[0]}")
    print(f"num_ro5_violations type: {type(merged['num_ro5_violations'].iloc[0])}")

if __name__ == "__main__":
    debug_merge()
