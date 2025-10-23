#!/usr/bin/env python3
"""
Тест для проверки исправления типов данных.
"""

import pandas as pd
from src.library.utils.empty_value_handler import normalize_numeric_field

def test_dtype_fix():
    """Тест исправления типов данных."""
    
    # Создаем DataFrame с числовой колонкой
    df = pd.DataFrame({
        "num_ro5_violations": [1.0, 2.0, 3.0]
    })
    
    print("До нормализации:")
    print(f"dtype: {df['num_ro5_violations'].dtype}")
    print(f"values: {df['num_ro5_violations'].tolist()}")
    
    # Применяем нормализацию
    df['num_ro5_violations'] = df['num_ro5_violations'].apply(normalize_numeric_field)
    
    print("\nПосле нормализации:")
    print(f"dtype: {df['num_ro5_violations'].dtype}")
    print(f"values: {df['num_ro5_violations'].tolist()}")
    
    # Проверяем, что тип остался float64
    assert df['num_ro5_violations'].dtype == 'float64', f"Expected float64, got {df['num_ro5_violations'].dtype}"
    
    print("\n✅ Тест прошел успешно!")

if __name__ == "__main__":
    test_dtype_fix()
