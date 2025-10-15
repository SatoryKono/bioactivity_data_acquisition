"""Простой тест для проверки нормализации данных."""

import sys
from pathlib import Path

# Добавляем путь к библиотеке
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

import pandas as pd
import numpy as np
import pytest
from pathlib import Path
import tempfile
import os

from library.etl.load import write_deterministic_csv, _normalize_dataframe


def test_string_normalization_simple():
    """Простой тест нормализации строковых данных."""
    # Создаем тестовые данные
    df = pd.DataFrame({
        'text_col': ['  Hello World  ', 'TEST STRING', '  mixed Case  '],
        'empty_string': ['', '   ', 'normal'],
        'none_value': [None, 'value', None]
    })
    
    # Нормализуем данные
    normalized_df = _normalize_dataframe(df)
    
    print("Исходные данные:")
    print(df)
    print("\nНормализованные данные:")
    print(normalized_df)
    
    # Проверяем основные случаи
    assert normalized_df['text_col'].iloc[0] == 'hello world'
    assert normalized_df['text_col'].iloc[1] == 'test string'
    assert normalized_df['text_col'].iloc[2] == 'mixed case'
    
    # Проверяем пустые строки
    assert pd.isna(normalized_df['empty_string'].iloc[0])
    assert pd.isna(normalized_df['empty_string'].iloc[1])
    assert normalized_df['empty_string'].iloc[2] == 'normal'
    
    # Проверяем None значения
    assert pd.isna(normalized_df['none_value'].iloc[0])
    assert normalized_df['none_value'].iloc[1] == 'value'
    assert pd.isna(normalized_df['none_value'].iloc[2])
    
    print("Тест нормализации строковых данных прошел успешно!")


def test_numeric_normalization_simple():
    """Простой тест нормализации числовых данных."""
    # Создаем тестовые данные
    df = pd.DataFrame({
        'int_col': [1, 2, np.nan, 4],
        'float_col': [1.5, 2.7, np.nan, np.inf]
    })
    
    # Нормализуем данные
    normalized_df = _normalize_dataframe(df)
    
    print("Исходные числовые данные:")
    print(df)
    print("\nНормализованные числовые данные:")
    print(normalized_df)
    
    # Проверяем, что обычные числа остались без изменений
    assert normalized_df['int_col'].iloc[0] == 1
    assert normalized_df['int_col'].iloc[1] == 2
    assert normalized_df['int_col'].iloc[3] == 4
    
    # Проверяем, что NaN и inf заменены на pd.NA
    assert pd.isna(normalized_df['int_col'].iloc[2])
    assert pd.isna(normalized_df['float_col'].iloc[2])
    assert pd.isna(normalized_df['float_col'].iloc[3])
    
    print("Тест нормализации числовых данных прошел успешно!")


def test_index_column_preserved():
    """Тест, что столбец index не нормализуется."""
    # Создаем данные с index (одинаковая длина)
    df = pd.DataFrame({
        'index': [0, 1, 2, 3],
        'text_col': ['  HELLO  ', 'World', '  Test  ', 'Another'],
        'numeric_col': [1, 2.5, 3, 4.0]
    })
    
    # Нормализуем данные
    normalized_df = _normalize_dataframe(df)
    
    print("Исходные данные с index:")
    print(df)
    print("\nНормализованные данные с index:")
    print(normalized_df)
    
    # Проверяем, что index остался без изменений
    assert normalized_df['index'].tolist() == [0, 1, 2, 3]
    
    # Проверяем, что остальные колонки нормализованы
    assert normalized_df['text_col'].iloc[0] == 'hello'
    assert normalized_df['text_col'].iloc[1] == 'world'
    assert normalized_df['text_col'].iloc[2] == 'test'
    assert normalized_df['text_col'].iloc[3] == 'another'
    
    print("Тест сохранения столбца index прошел успешно!")


def test_full_pipeline_with_normalization():
    """Тест полного пайплайна с нормализацией."""
    # Создаем тестовые данные
    df = pd.DataFrame({
        'compound_id': ['  CHEMBL1  ', 'CHEMBL2', 'chembl3'],
        'target': ['TARGET1', '  target2  ', 'TARGET3'],
        'activity_value': [5.2, np.nan, 7.1],
        'is_active': [True, False, True]
    })
    
    # Создаем временный файл
    with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as tmp_file:
        tmp_path = Path(tmp_file.name)
    
    try:
        # Записываем данные с нормализацией
        write_deterministic_csv(
            df,
            tmp_path,
            determinism=None,
            output=None
        )
        
        # Читаем обратно и проверяем
        result_df = pd.read_csv(tmp_path)
        
        print("Результат полного пайплайна:")
        print(result_df)
        
        # Проверяем, что index добавлен
        assert 'index' in result_df.columns
        assert result_df.columns[0] == 'index'
        
        # Проверяем нормализацию строк
        assert result_df['compound_id'].iloc[0] == 'chembl1'
        assert result_df['compound_id'].iloc[1] == 'chembl2'
        assert result_df['compound_id'].iloc[2] == 'chembl3'
        
        assert result_df['target'].iloc[0] == 'target1'
        assert result_df['target'].iloc[1] == 'target2'
        assert result_df['target'].iloc[2] == 'target3'
        
        # Проверяем числовые данные
        assert result_df['activity_value'].iloc[0] == 5.2
        assert pd.isna(result_df['activity_value'].iloc[1])  # NaN -> NA
        assert result_df['activity_value'].iloc[2] == 7.1
        
        print("Тест полного пайплайна прошел успешно!")
        
    finally:
        # Удаляем временный файл
        if tmp_path.exists():
            os.unlink(tmp_path)


if __name__ == "__main__":
    test_string_normalization_simple()
    test_numeric_normalization_simple()
    test_index_column_preserved()
    test_full_pipeline_with_normalization()
    print("\nВсе тесты нормализации прошли успешно!")
