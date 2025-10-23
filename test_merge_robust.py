#!/usr/bin/env python3
"""
Тест для проверки исправленной логики merge с различными типами данных.
"""

import pandas as pd
import numpy as np
from src.library.testitem.pipeline import TestitemPipeline

def test_merge_robust():
    """Тест исправленной логики merge с различными типами данных."""
    
    # Создаем mock config
    from src.library.testitem.config import TestitemConfig
    
    class MockConfig(TestitemConfig):
        def __init__(self):
            super().__init__()
            self.sources = {}
    
    pipeline = TestitemPipeline(MockConfig())
    
    # Тестовые данные с различными типами
    base_data = pd.DataFrame({
        "molecule_chembl_id": ["CHEMBL123", "CHEMBL456"],
        "canonical_smiles": ["CCO", "CCN"],
        "chirality": [1, 0],
        "mw_freebase": [46.07, 45.08]
    })
    
    chembl_data = pd.DataFrame({
        "molecule_chembl_id": ["CHEMBL123", "CHEMBL456"],
        "molregno": [12345, 67890],
        "pref_name": ["Test Drug 1", "Test Drug 2"],
        "canonical_smiles": ["CCO", "CCN"],
        "chirality": [1, 0],
        "mw_freebase": [46.07, 45.08],
        "parent_chembl_id": ["CHEMBL123", "CHEMBL456"],
        "alogp": [0.31, 0.17],
        # Добавляем проблемные значения
        "empty_array": [np.array([]), np.array([])],
        "empty_string": ["", ""],
        "none_value": [None, None],
        "empty_list": [[], []]
    })
    
    # Вызываем merge
    merged = pipeline._merge_chembl_data(base_data, chembl_data)
    
    # Проверяем, что нет дублирующих колонок с суффиксами
    duplicate_columns = [col for col in merged.columns if '_x' in col or '_y' in col]
    assert len(duplicate_columns) == 0, f"Найдены дублирующие колонки: {duplicate_columns}"
    
    # Проверяем, что все ожидаемые колонки присутствуют
    expected_columns = [
        "molecule_chembl_id", "canonical_smiles", "chirality", "mw_freebase",
        "molregno", "pref_name", "parent_chembl_id", "alogp"
    ]
    
    for col in expected_columns:
        assert col in merged.columns, f"Колонка {col} отсутствует в результате merge"
    
    # Проверяем, что данные корректно объединены
    assert len(merged) == 2
    assert merged["molregno"].tolist() == [12345, 67890]
    assert merged["pref_name"].tolist() == ["Test Drug 1", "Test Drug 2"]
    assert merged["parent_chembl_id"].tolist() == ["CHEMBL123", "CHEMBL456"]
    assert merged["alogp"].tolist() == [0.31, 0.17]
    
    print("✅ Тест исправленной логики merge с различными типами данных прошел успешно!")
    print(f"Колонки в результате: {list(merged.columns)}")

def test_is_valid_value():
    """Тест метода _is_valid_value."""
    
    from src.library.testitem.config import TestitemConfig
    
    class MockConfig(TestitemConfig):
        def __init__(self):
            super().__init__()
            self.sources = {}
    
    pipeline = TestitemPipeline(MockConfig())
    
    # Тестируем различные значения
    assert pipeline._is_valid_value("valid_string")
    assert pipeline._is_valid_value(123)
    assert pipeline._is_valid_value(0.5)
    assert pipeline._is_valid_value(True)
    assert pipeline._is_valid_value([1, 2, 3])
    assert pipeline._is_valid_value(np.array([1, 2, 3]))
    
    # Тестируем невалидные значения
    assert not pipeline._is_valid_value(None)
    assert not pipeline._is_valid_value("")
    assert not pipeline._is_valid_value("   ")
    assert not pipeline._is_valid_value([])
    assert not pipeline._is_valid_value(np.array([]))
    assert not pipeline._is_valid_value(pd.NA)
    
    print("✅ Тест метода _is_valid_value прошел успешно!")

if __name__ == "__main__":
    print("Запуск тестов исправленной логики merge...")
    
    try:
        test_merge_robust()
        test_is_valid_value()
        print("\n🎉 Все тесты прошли успешно!")
        
    except Exception as e:
        print(f"\n❌ Тест не прошел: {e}")
        raise
