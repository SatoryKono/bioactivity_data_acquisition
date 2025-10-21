"""Тесты для проверки нормализации данных."""

import sys
from pathlib import Path

# Добавляем путь к библиотеке
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

import os
import tempfile
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from library.etl.load import _normalize_dataframe, write_deterministic_csv


class TestDataNormalization:
    """Тесты для проверки нормализации данных."""

    def test_string_normalization(self):
        """Тест нормализации строковых данных."""
        # Создаем тестовые данные со строками разного регистра и пробелами
        df = pd.DataFrame({
            'text_col': ['  Hello World  ', 'TEST STRING', '  mixed Case  ', '', '   ', None],
            'another_text': ['UPPERCASE', 'lowercase', 'Mixed Case', '', None, 'Normal Text']
        })
        
        # Нормализуем данные
        normalized_df = _normalize_dataframe(df)
        
        # Проверяем, что строки приведены к нижнему регистру и обрезаны
        expected_text = ['hello world', 'test string', 'mixed case', pd.NA, pd.NA, pd.NA]
        expected_another = ['uppercase', 'lowercase', 'mixed case', pd.NA, pd.NA, 'normal text']
        
        # Проверяем первую колонку
        result_text = normalized_df['text_col'].tolist()
        for i, (actual, expected) in enumerate(zip(result_text, expected_text, strict=False)):
            if pd.isna(expected):
                assert pd.isna(actual), f"Позиция {i}: ожидался NA, получено {actual}"
            else:
                assert actual == expected, f"Позиция {i}: ожидалось '{expected}', получено '{actual}'"
        
        # Проверяем вторую колонку
        result_another = normalized_df['another_text'].tolist()
        for i, (actual, expected) in enumerate(zip(result_another, expected_another, strict=False)):
            if pd.isna(expected):
                assert pd.isna(actual), f"Позиция {i}: ожидался NA, получено {actual}"
            else:
                assert actual == expected, f"Позиция {i}: ожидалось '{expected}', получено '{actual}'"

    def test_numeric_normalization(self):
        """Тест нормализации числовых данных."""
        # Создаем тестовые данные с различными числовыми значениями
        df = pd.DataFrame({
            'int_col': [1, 2, np.nan, 4, 5],
            'float_col': [1.5, 2.7, np.nan, np.inf, -np.inf],
            'mixed_numeric': [1, 2.5, np.nan, 4, 5.0]
        })
        
        # Нормализуем данные
        normalized_df = _normalize_dataframe(df)
        
        # Проверяем, что NaN, inf и -inf заменены на pd.NA
        assert pd.isna(normalized_df['int_col'].iloc[2]), "NaN должен быть заменен на pd.NA"
        assert pd.isna(normalized_df['float_col'].iloc[2]), "NaN должен быть заменен на pd.NA"
        assert pd.isna(normalized_df['float_col'].iloc[3]), "inf должен быть заменен на pd.NA"
        assert pd.isna(normalized_df['float_col'].iloc[4]), "-inf должен быть заменен на pd.NA"
        
        # Проверяем, что обычные числа остались без изменений
        assert normalized_df['int_col'].iloc[0] == 1
        assert normalized_df['int_col'].iloc[1] == 2
        assert normalized_df['float_col'].iloc[0] == 1.5
        assert normalized_df['float_col'].iloc[1] == 2.7

    def test_boolean_normalization(self):
        """Тест нормализации логических данных."""
        # Создаем тестовые данные с логическими значениями
        df = pd.DataFrame({
            'bool_col': [True, False, np.nan, True, False],
            'mixed_bool': [True, False, None, 1, 0]
        })
        
        # Нормализуем данные
        normalized_df = _normalize_dataframe(df)
        
        # Проверяем, что NaN заменен на pd.NA
        assert pd.isna(normalized_df['bool_col'].iloc[2]), "NaN должен быть заменен на pd.NA"
        
        # Проверяем, что остальные значения остались корректными
        assert normalized_df['bool_col'].iloc[0] == True
        assert normalized_df['bool_col'].iloc[1] == False
        assert normalized_df['bool_col'].iloc[3] == True
        assert normalized_df['bool_col'].iloc[4] == False

    def test_empty_dataframe_normalization(self):
        """Тест нормализации пустого DataFrame."""
        # Создаем пустой DataFrame
        df = pd.DataFrame()
        
        # Нормализуем данные
        normalized_df = _normalize_dataframe(df)
        
        # Проверяем, что пустой DataFrame остается пустым
        assert len(normalized_df) == 0
        assert len(normalized_df.columns) == 0

    def test_mixed_data_types_normalization(self):
        """Тест нормализации смешанных типов данных."""
        # Создаем DataFrame со смешанными типами данных
        df = pd.DataFrame({
            'string_col': ['  HELLO  ', 'World', '', None],
            'numeric_col': [1, 2.5, np.nan, 4],
            'bool_col': [True, False, np.nan, True],
            'index': [0, 1, 2, 3]  # Столбец index
        })
        
        # Нормализуем данные
        normalized_df = _normalize_dataframe(df)
        
        # Проверяем строковую колонку
        assert normalized_df['string_col'].iloc[0] == 'hello'
        assert normalized_df['string_col'].iloc[1] == 'world'
        assert pd.isna(normalized_df['string_col'].iloc[2])
        assert pd.isna(normalized_df['string_col'].iloc[3])
        
        # Проверяем числовую колонку
        assert normalized_df['numeric_col'].iloc[0] == 1
        assert normalized_df['numeric_col'].iloc[1] == 2.5
        assert pd.isna(normalized_df['numeric_col'].iloc[2])
        assert normalized_df['numeric_col'].iloc[3] == 4
        
        # Проверяем логическую колонку
        assert normalized_df['bool_col'].iloc[0] == True
        assert normalized_df['bool_col'].iloc[1] == False
        assert pd.isna(normalized_df['bool_col'].iloc[2])
        assert normalized_df['bool_col'].iloc[3] == True
        
        # Проверяем, что index остался без изменений
        assert normalized_df['index'].tolist() == [0, 1, 2, 3]

    def test_normalization_with_write_deterministic_csv(self):
        """Тест интеграции нормализации с write_deterministic_csv."""
        # Создаем тестовые данные
        df = pd.DataFrame({
            'compound_id': ['  CHEMBL1  ', 'CHEMBL2', '', None],
            'target': ['TARGET1', '  target2  ', 'TARGET3', ''],
            'activity_value': [5.2, np.nan, 7.1, 9.4],
            'is_active': [True, False, np.nan, True]
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
            
            # Проверяем, что index добавлен
            assert 'index' in result_df.columns
            assert result_df.columns[0] == 'index'
            
            # Проверяем нормализацию строковых данных
            assert result_df['compound_id'].iloc[0] == 'chembl1'  # обрезано и в нижнем регистре
            assert result_df['compound_id'].iloc[1] == 'chembl2'  # в нижнем регистре
            assert pd.isna(result_df['compound_id'].iloc[2])  # пустая строка -> NA
            assert pd.isna(result_df['compound_id'].iloc[3])  # None -> NA
            
            assert result_df['target'].iloc[0] == 'target1'  # в нижнем регистре
            assert result_df['target'].iloc[1] == 'target2'  # обрезано и в нижнем регистре
            assert result_df['target'].iloc[2] == 'target3'  # в нижнем регистре
            assert pd.isna(result_df['target'].iloc[3])  # пустая строка -> NA
            
            # Проверяем нормализацию числовых данных
            assert result_df['activity_value'].iloc[0] == 5.2
            assert pd.isna(result_df['activity_value'].iloc[1])  # NaN -> NA
            assert result_df['activity_value'].iloc[2] == 7.1
            assert result_df['activity_value'].iloc[3] == 9.4
            
            # Проверяем нормализацию логических данных
            assert result_df['is_active'].iloc[0] == True
            assert result_df['is_active'].iloc[1] == False
            assert pd.isna(result_df['is_active'].iloc[2])  # NaN -> NA
            assert result_df['is_active'].iloc[3] == True
            
        finally:
            # Удаляем временный файл
            if tmp_path.exists():
                os.unlink(tmp_path)

    def test_special_string_values_normalization(self):
        """Тест нормализации специальных строковых значений."""
        # Создаем тестовые данные со специальными значениями (одинаковой длины)
        df = pd.DataFrame({
            'special_strings': ['nan', 'None', 'NONE', 'null', 'NULL', '  NAN  '],
            'normal_strings': ['Hello', 'WORLD', '  Test  ', 'Normal', 'String', 'Values']
        })
        
        # Нормализуем данные
        normalized_df = _normalize_dataframe(df)
        
        # Проверяем, что специальные значения заменены на pd.NA
        for i in range(6):  # Первые 6 значений должны быть NA
            assert pd.isna(normalized_df['special_strings'].iloc[i]), f"Позиция {i} должна быть NA"
        
        # Проверяем нормализацию обычных строк
        assert normalized_df['normal_strings'].iloc[0] == 'hello'
        assert normalized_df['normal_strings'].iloc[1] == 'world'
        assert normalized_df['normal_strings'].iloc[2] == 'test'


if __name__ == "__main__":
    pytest.main([__file__])
