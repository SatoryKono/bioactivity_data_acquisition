"""
Тесты для нормализаторов числовых данных.
"""

import pytest
from src.library.normalizers.numeric_normalizers import (
    normalize_int,
    normalize_float,
    normalize_int_positive,
    normalize_int_range,
    normalize_float_precision,
    normalize_year,
    normalize_month,
    normalize_day,
    normalize_molecular_weight,
    normalize_pchembl_range,
)


class TestNumericNormalizers:
    """Тесты для нормализаторов числовых данных."""
    
    @pytest.mark.parametrize("input_val,expected", [
        # Позитивные тесты
        (123, 123),
        (123.0, 123),
        ("123", 123),
        ("123.0", 123),
        (0, 0),
        # Граничные случаи
        (None, None),
        ("", None),
        ("invalid", None),
        (float('nan'), None),
        (float('inf'), None),
    ])
    def test_normalize_int(self, input_val, expected):
        """Тест нормализации целых чисел."""
        assert normalize_int(input_val) == expected
    
    @pytest.mark.parametrize("input_val,expected", [
        # Позитивные тесты
        (123.45, 123.45),
        (123, 123.0),
        ("123.45", 123.45),
        ("123", 123.0),
        (0.0, 0.0),
        # Граничные случаи
        (None, None),
        ("", None),
        ("invalid", None),
        (float('nan'), None),
        (float('inf'), None),
    ])
    def test_normalize_float(self, input_val, expected):
        """Тест нормализации чисел с плавающей точкой."""
        assert normalize_float(input_val) == expected
    
    @pytest.mark.parametrize("input_val,expected", [
        # Позитивные тесты
        (123, 123),
        (1, 1),
        ("123", 123),
        # Негативные тесты
        (0, None),
        (-1, None),
        (None, None),
        ("", None),
        ("invalid", None),
    ])
    def test_normalize_int_positive(self, input_val, expected):
        """Тест нормализации положительных целых чисел."""
        assert normalize_int_positive(input_val) == expected
    
    @pytest.mark.parametrize("input_val,min_val,max_val,expected", [
        # Позитивные тесты
        (5, 1, 10, 5),
        (1, 1, 10, 1),
        (10, 1, 10, 10),
        # Негативные тесты
        (0, 1, 10, None),
        (11, 1, 10, None),
        (None, 1, 10, None),
    ])
    def test_normalize_int_range(self, input_val, min_val, max_val, expected):
        """Тест нормализации целых чисел в диапазоне."""
        assert normalize_int_range(input_val, min_val, max_val) == expected
    
    @pytest.mark.parametrize("input_val,precision,expected", [
        # Позитивные тесты
        (123.456789, 2, 123.46),
        (123.456789, 3, 123.457),
        (123.456789, 0, 123.0),
        # Граничные случаи
        (None, 2, None),
        ("invalid", 2, None),
    ])
    def test_normalize_float_precision(self, input_val, precision, expected):
        """Тест нормализации точности чисел с плавающей точкой."""
        assert normalize_float_precision(input_val, precision) == expected
    
    @pytest.mark.parametrize("input_val,expected", [
        # Позитивные тесты
        (2023, 2023),
        (1800, 1800),
        (2024, 2024),
        # Негативные тесты
        (1799, None),  # Слишком старый
        (2030, None),  # Слишком новый (если текущий год 2024)
        (None, None),
        ("invalid", None),
    ])
    def test_normalize_year(self, input_val, expected):
        """Тест нормализации года."""
        # Мокаем текущий год для теста
        import datetime
        from unittest.mock import patch
        
        with patch('datetime.datetime') as mock_datetime:
            mock_datetime.now.return_value = datetime.datetime(2024, 1, 1)
            assert normalize_year(input_val) == expected
    
    @pytest.mark.parametrize("input_val,expected", [
        # Позитивные тесты
        (1, 1),
        (6, 6),
        (12, 12),
        # Негативные тесты
        (0, None),
        (13, None),
        (None, None),
        ("invalid", None),
    ])
    def test_normalize_month(self, input_val, expected):
        """Тест нормализации месяца."""
        assert normalize_month(input_val) == expected
    
    @pytest.mark.parametrize("input_val,expected", [
        # Позитивные тесты
        (1, 1),
        (15, 15),
        (31, 31),
        # Негативные тесты
        (0, None),
        (32, None),
        (None, None),
        ("invalid", None),
    ])
    def test_normalize_day(self, input_val, expected):
        """Тест нормализации дня."""
        assert normalize_day(input_val) == expected
    
    @pytest.mark.parametrize("input_val,expected", [
        # Позитивные тесты
        (100.0, 100.0),
        (500.0, 500.0),
        (1000.0, 1000.0),
        # Негативные тесты
        (49.0, None),  # Слишком маленький
        (2001.0, None),  # Слишком большой
        (None, None),
        ("invalid", None),
    ])
    def test_normalize_molecular_weight(self, input_val, expected):
        """Тест нормализации молекулярной массы."""
        assert normalize_molecular_weight(input_val) == expected
    
    @pytest.mark.parametrize("input_val,expected", [
        # Позитивные тесты
        (7.0, 7.0),
        (0.0, 0.0),
        (14.0, 14.0),
        (7.1234, 7.123),  # Округление до 3 знаков
        # Негативные тесты
        (-1.0, None),  # Слишком маленький
        (15.0, None),  # Слишком большой
        (None, None),
        ("invalid", None),
    ])
    def test_normalize_pchembl_range(self, input_val, expected):
        """Тест нормализации pChEMBL диапазона."""
        assert normalize_pchembl_range(input_val) == expected
