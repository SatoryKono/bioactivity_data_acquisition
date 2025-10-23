"""
Тесты для нормализаторов булевых данных.
"""

import pytest
from src.library.normalizers.boolean_normalizers import (
    normalize_boolean,
    normalize_boolean_strict,
    normalize_boolean_from_numeric,
    normalize_boolean_preserve_none,
)


class TestBooleanNormalizers:
    """Тесты для нормализаторов булевых данных."""
    
    @pytest.mark.parametrize("input_val,expected", [
        # Позитивные тесты - True значения
        (True, True),
        (1, True),
        (1.0, True),
        ("true", True),
        ("True", True),
        ("TRUE", True),
        ("t", True),
        ("T", True),
        ("1", True),
        ("yes", True),
        ("Yes", True),
        ("YES", True),
        ("y", True),
        ("Y", True),
        ("on", True),
        ("On", True),
        ("ON", True),
        ("enabled", True),
        ("Enabled", True),
        ("ENABLED", True),
        ("active", True),
        ("Active", True),
        ("ACTIVE", True),
        
        # Позитивные тесты - False значения
        (False, False),
        (0, False),
        (0.0, False),
        ("false", False),
        ("False", False),
        ("FALSE", False),
        ("f", False),
        ("F", False),
        ("0", False),
        ("no", False),
        ("No", False),
        ("NO", False),
        ("n", False),
        ("N", False),
        ("off", False),
        ("Off", False),
        ("OFF", False),
        ("disabled", False),
        ("Disabled", False),
        ("DISABLED", False),
        ("inactive", False),
        ("Inactive", False),
        ("INACTIVE", False),
        
        # Граничные случаи
        (None, None),
        ("", None),
        ("invalid", None),
        (2, None),
        (-1, None),
    ])
    def test_normalize_boolean(self, input_val, expected):
        """Тест нормализации булевых значений."""
        assert normalize_boolean(input_val) == expected
    
    @pytest.mark.parametrize("input_val,expected", [
        # Позитивные тесты - строгие True значения
        (True, True),
        (1, True),
        ("true", True),
        ("1", True),
        
        # Позитивные тесты - строгие False значения
        (False, False),
        (0, False),
        ("false", False),
        ("0", False),
        
        # Негативные тесты - нестрогие значения
        (2, None),
        (-1, None),
        ("yes", None),
        ("no", None),
        ("on", None),
        ("off", None),
        (None, None),
        ("", None),
        ("invalid", None),
    ])
    def test_normalize_boolean_strict(self, input_val, expected):
        """Тест строгой нормализации булевых значений."""
        assert normalize_boolean_strict(input_val) == expected
    
    @pytest.mark.parametrize("input_val,threshold,expected", [
        # Позитивные тесты с порогом 0
        (1, 0, True),
        (0.1, 0, True),
        (0, 0, False),
        (-1, 0, False),
        
        # Позитивные тесты с порогом 5
        (6, 5, True),
        (5, 5, False),
        (4, 5, False),
        
        # Граничные случаи
        (None, 0, None),
        ("invalid", 0, None),
        ("5", 0, True),  # Строка "5" > 0
        ("0", 0, False),  # Строка "0" = 0
    ])
    def test_normalize_boolean_from_numeric(self, input_val, threshold, expected):
        """Тест нормализации булевых значений из числовых по порогу."""
        assert normalize_boolean_from_numeric(input_val, threshold) == expected
    
    @pytest.mark.parametrize("input_val,expected", [
        # Позитивные тесты
        (True, True),
        (False, False),
        (1, True),
        (0, False),
        ("true", True),
        ("false", False),
        
        # Особый случай - None остается None
        (None, None),
        
        # Граничные случаи
        ("", None),
        ("invalid", None),
    ])
    def test_normalize_boolean_preserve_none(self, input_val, expected):
        """Тест нормализации булевых значений с сохранением None."""
        assert normalize_boolean_preserve_none(input_val) == expected
