#!/usr/bin/env python3
"""Тест для проверки нормализации данных с сохранением регистра."""

import pandas as pd
from library.etl.load import _normalize_dataframe
from library.config import DeterminismSettings

def test_case_preservation():
    """Тест сохранения регистра в SMILES и других полях."""
    # Создаем тестовые данные с разным регистром
    df = pd.DataFrame({
        'smiles': ['CCO', 'CCN', 'c1ccccc1'],  # SMILES с разным регистром
        'target': ['ProteinA', 'ProteinB', 'EnzymeC'],  # Названия белков
        'compound_id': ['CHEMBL1', 'CHEMBL2', 'CHEMBL3'],  # ID соединений
        'activity_unit': ['nM', 'uM', 'pM'],  # Единицы измерения
        'source': ['ChEMBL', 'PubChem', 'BindingDB']  # Источники данных
    })
    
    print("Исходные данные:")
    print(df)
    print()
    
    # Тест 1: Без приведения к нижнему регистру (по умолчанию)
    print("Тест 1: Без приведения к нижнему регистру")
    determinism_empty = DeterminismSettings(lowercase_columns=[])
    result_empty = _normalize_dataframe(df, determinism_empty)
    print("SMILES:", result_empty['smiles'].tolist())
    print("Target:", result_empty['target'].tolist())
    print("Compound ID:", result_empty['compound_id'].tolist())
    print("Activity Unit:", result_empty['activity_unit'].tolist())
    print("Source:", result_empty['source'].tolist())
    print()
    
    # Тест 2: Селективное приведение к нижнему регистру только для source
    print("Тест 2: Селективное приведение к нижнему регистру (только source)")
    determinism_selective = DeterminismSettings(lowercase_columns=['source'])
    result_selective = _normalize_dataframe(df, determinism_selective)
    print("SMILES:", result_selective['smiles'].tolist())
    print("Target:", result_selective['target'].tolist())
    print("Compound ID:", result_selective['compound_id'].tolist())
    print("Activity Unit:", result_selective['activity_unit'].tolist())
    print("Source:", result_selective['source'].tolist())
    print()
    
    # Проверки
    print("Проверки:")
    
    # В обоих случаях SMILES должны сохранить регистр
    assert result_empty['smiles'].tolist() == ['CCO', 'CCN', 'c1ccccc1'], f"SMILES в тесте 1: {result_empty['smiles'].tolist()}"
    assert result_selective['smiles'].tolist() == ['CCO', 'CCN', 'c1ccccc1'], f"SMILES в тесте 2: {result_selective['smiles'].tolist()}"
    print("✓ SMILES сохранили регистр в обоих тестах")
    
    # Target должен сохранить регистр в обоих случаях
    assert result_empty['target'].tolist() == ['ProteinA', 'ProteinB', 'EnzymeC'], f"Target в тесте 1: {result_empty['target'].tolist()}"
    assert result_selective['target'].tolist() == ['ProteinA', 'ProteinB', 'EnzymeC'], f"Target в тесте 2: {result_selective['target'].tolist()}"
    print("✓ Target сохранил регистр в обоих тестах")
    
    # Source должен сохранить регистр в тесте 1, но быть приведен к нижнему в тесте 2
    assert result_empty['source'].tolist() == ['ChEMBL', 'PubChem', 'BindingDB'], f"Source в тесте 1: {result_empty['source'].tolist()}"
    assert result_selective['source'].tolist() == ['chembl', 'pubchem', 'bindingdb'], f"Source в тесте 2: {result_selective['source'].tolist()}"
    print("✓ Source правильно обработан в обоих тестах")
    
    print("\n🎉 Все тесты прошли успешно! Регистр сохраняется корректно.")

if __name__ == "__main__":
    test_case_preservation()
