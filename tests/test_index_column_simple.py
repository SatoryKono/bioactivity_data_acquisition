"""Простой тест для проверки добавления столбца index."""

import sys
from pathlib import Path

# Добавляем путь к библиотеке
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

import pandas as pd
import pytest
from pathlib import Path
import tempfile
import os

from library.etl.load import write_deterministic_csv


def test_index_column_added_simple():
    """Простой тест добавления столбца index в CSV файл."""
    # Создаем тестовые данные
    df = pd.DataFrame({
        'id': ['A', 'B', 'C', 'D'],
        'value': [1, 2, 3, 4],
        'text': ['hello', 'world', 'test', 'data'],
    })
    
    # Создаем временный файл
    with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as tmp_file:
        tmp_path = Path(tmp_file.name)
    
    try:
        # Записываем данные без настроек детерминированного порядка
        write_deterministic_csv(
            df,
            tmp_path,
            determinism=None,
            output=None
        )
        
        # Читаем обратно и проверяем
        result_df = pd.read_csv(tmp_path)
        
        # Проверяем, что столбец index добавлен
        assert 'index' in result_df.columns
        
        # Проверяем, что index является первым столбцом
        assert result_df.columns[0] == 'index'
        
        # Проверяем, что значения index корректны (0, 1, 2, 3)
        expected_index = list(range(len(df)))
        assert result_df['index'].tolist() == expected_index
        
        # Проверяем, что остальные данные сохранены
        assert len(result_df) == len(df)
        assert 'id' in result_df.columns
        assert 'value' in result_df.columns
        assert 'text' in result_df.columns
        
        print("Тест добавления столбца index прошел успешно!")
        print(f"Столбцы в результате: {list(result_df.columns)}")
        print(f"Значения index: {result_df['index'].tolist()}")
        
    finally:
        # Удаляем временный файл
        if tmp_path.exists():
            os.unlink(tmp_path)


def test_index_column_empty_dataframe_simple():
    """Тест обработки пустого DataFrame."""
    # Создаем пустой DataFrame
    df = pd.DataFrame()
    
    # Создаем временный файл
    with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as tmp_file:
        tmp_path = Path(tmp_file.name)
    
    try:
        # Записываем данные
        write_deterministic_csv(
            df,
            tmp_path,
            determinism=None,
            output=None
        )
        
        # Для пустого DataFrame столбец index не должен добавляться
        # и файл должен быть пустым или содержать только заголовок
        try:
            if tmp_path.stat().st_size > 0:
                result_df = pd.read_csv(tmp_path)
                assert len(result_df) == 0
        except pd.errors.EmptyDataError:
            # Файл пустой, что тоже корректно для пустого DataFrame
            pass
        
        print("Тест обработки пустого DataFrame прошел успешно!")
        
    finally:
        # Удаляем временный файл
        if tmp_path.exists():
            os.unlink(tmp_path)


if __name__ == "__main__":
    test_index_column_added_simple()
    test_index_column_empty_dataframe_simple()
    print("\nВсе тесты прошли успешно!")
