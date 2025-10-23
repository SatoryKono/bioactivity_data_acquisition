"""
Тесты для нормализаторов единиц измерения.
"""

import pytest
from src.library.normalizers.units_normalizers import (
    normalize_units,
    normalize_pchembl,
    normalize_activity_value,
    normalize_concentration,
    normalize_percentage,
)


class TestUnitsNormalizers:
    """Тесты для нормализаторов единиц измерения."""
    
    @pytest.mark.parametrize("input_val,expected", [
        # Концентрации - наномоляр
        ("nm", "nm"),
        ("nanomolar", "nm"),
        ("nanomol/l", "nm"),
        ("nanomol/liter", "nm"),
        ("nmol/l", "nm"),
        ("nmol/liter", "nm"),
        
        # Концентрации - микромоляр
        ("um", "μm"),
        ("μm", "μm"),
        ("micromolar", "μm"),
        ("micromol/l", "μm"),
        ("micromol/liter", "μm"),
        ("μmol/l", "μm"),
        ("μmol/liter", "μm"),
        ("umol/l", "μm"),
        ("umol/liter", "μm"),
        
        # Концентрации - миллимоляр
        ("mm", "mm"),
        ("millimolar", "mm"),
        ("millimol/l", "mm"),
        ("millimol/liter", "mm"),
        ("mmol/l", "mm"),
        ("mmol/liter", "mm"),
        
        # Концентрации - моляр
        ("m", "m"),
        ("molar", "m"),
        ("mol/l", "m"),
        ("mol/liter", "m"),
        
        # Массы
        ("ng/ml", "ng/ml"),
        ("nanogram/ml", "ng/ml"),
        ("nanogram/milliliter", "ng/ml"),
        ("ug/ml", "μg/ml"),
        ("μg/ml", "μg/ml"),
        ("microgram/ml", "μg/ml"),
        ("microgram/milliliter", "μg/ml"),
        ("mg/ml", "mg/ml"),
        ("milligram/ml", "mg/ml"),
        ("milligram/milliliter", "mg/ml"),
        ("g/ml", "g/ml"),
        ("gram/ml", "g/ml"),
        ("gram/milliliter", "g/ml"),
        
        # Проценты
        ("%", "%"),
        ("percent", "%"),
        ("pct", "%"),
        
        # Другие единицы
        ("ic50", "ic50"),
        ("ec50", "ec50"),
        ("ki", "ki"),
        ("kd", "kd"),
        ("ic90", "ic90"),
        ("ec90", "ec90"),
        
        # Граничные случаи
        ("", ""),
        (None, None),
        ("unknown_unit", "unknown_unit"),  # Неизвестная единица возвращается как есть
    ])
    def test_normalize_units(self, input_val, expected):
        """Тест нормализации единиц измерения."""
        assert normalize_units(input_val) == expected
    
    @pytest.mark.parametrize("input_val,expected", [
        # Позитивные тесты
        (7.0, 7.0),
        (0.0, 0.0),
        (14.0, 14.0),
        (7.1234, 7.123),  # Округление до 3 знаков
        (7.1236, 7.124),  # Округление до 3 знаков
        (7.1235, 7.124),  # Округление до 3 знаков
        
        # Граничные случаи
        (None, None),
        ("invalid", None),
        (-1.0, None),  # Слишком маленький
        (15.0, None),  # Слишком большой
    ])
    def test_normalize_pchembl(self, input_val, expected):
        """Тест нормализации pChEMBL значений."""
        assert normalize_pchembl(input_val) == expected
    
    @pytest.mark.parametrize("input_val,expected", [
        # Позитивные тесты
        (100.0, 100.0),
        (0.1, 0.1),
        (1000.0, 1000.0),
        (123.456789, 123.456789),  # Округление до 6 знаков
        
        # Граничные случаи
        (None, None),
        ("invalid", None),
        (0.0, None),  # Должно быть > 0
        (-1.0, None),  # Должно быть > 0
    ])
    def test_normalize_activity_value(self, input_val, expected):
        """Тест нормализации значений активности."""
        assert normalize_activity_value(input_val) == expected
    
    @pytest.mark.parametrize("input_val,unit,expected", [
        # Позитивные тесты без единиц
        (100.0, None, 100.0),
        (0.1, None, 0.1),
        
        # Позитивные тесты с единицами
        (1000.0, "nm", 0.000001),  # 1000 nM = 1e-6 M
        (1.0, "μm", 0.000001),  # 1 μM = 1e-6 M
        (1.0, "mm", 0.001),  # 1 mM = 1e-3 M
        (1.0, "m", 1.0),  # 1 M = 1 M
        
        # Граничные случаи
        (None, None, None),
        ("invalid", None, None),
        (0.0, None, None),  # Должно быть > 0
        (-1.0, None, None),  # Должно быть > 0
    ])
    def test_normalize_concentration(self, input_val, unit, expected):
        """Тест нормализации концентрации."""
        assert normalize_concentration(input_val, unit) == expected
    
    @pytest.mark.parametrize("input_val,expected", [
        # Позитивные тесты
        (50.0, 50.0),
        (0.0, 0.0),
        (100.0, 100.0),
        (50.123, 50.12),  # Округление до 2 знаков
        ("50%", 50.0),  # Строка с символом %
        ("50", 50.0),  # Строка без символа %
        
        # Граничные случаи
        (None, None),
        ("invalid", None),
        (-1.0, None),  # Слишком маленький
        (101.0, None),  # Слишком большой
        ("", None),
    ])
    def test_normalize_percentage(self, input_val, expected):
        """Тест нормализации процентных значений."""
        assert normalize_percentage(input_val) == expected
