"""
Тесты для нормализаторов химических структур.
"""

import pytest

from src.library.normalizers.chemistry_normalizers import (
    normalize_inchi,
    normalize_inchi_key,
    normalize_molecular_formula,
    normalize_smiles,
    normalize_smiles_canonical,
)


class TestChemistryNormalizers:
    """Тесты для нормализаторов химических структур."""
    
    @pytest.mark.parametrize("input_val,expected", [
        # Позитивные тесты
        ("CCO", "CCO"),  # Этанол
        ("C[C@H](O)Cl", "C[C@H](O)Cl"),  # Хиральный центр
        ("c1ccccc1", "c1ccccc1"),  # Бензол
        ("C1=CC=CC=C1", "C1=CC=CC=C1"),  # Бензол (альтернативная запись)
        # Граничные случаи
        ("", None),
        (None, None),
        ("invalid", None),
        ("C[invalid]", None),  # Недопустимые символы
        ("C(C", None),  # Несбалансированные скобки
        ("C)C", None),  # Несбалансированные скобки
    ])
    def test_normalize_smiles(self, input_val, expected):
        """Тест нормализации SMILES."""
        assert normalize_smiles(input_val) == expected
    
    @pytest.mark.parametrize("input_val,expected", [
        # Позитивные тесты
        ("InChI=1S/CH4/h1H4", "InChI=1S/CH4/h1H4"),  # Метан
        ("InChI=1S/C2H6O/c1-2-3/h3H,2H2,1H3", "InChI=1S/C2H6O/c1-2-3/h3H,2H2,1H3"),  # Этанол
        # Граничные случаи
        ("", None),
        (None, None),
        ("invalid", None),
        ("InChI=invalid", None),  # Неправильный формат
        ("InChI=1S", None),  # Неполный InChI
    ])
    def test_normalize_inchi(self, input_val, expected):
        """Тест нормализации InChI."""
        assert normalize_inchi(input_val) == expected
    
    @pytest.mark.parametrize("input_val,expected", [
        # Позитивные тесты
        ("BSYNRYMUTXBXSQ-UHFFFAOYSA-N", "BSYNRYMUTXBXSQ-UHFFFAOYSA-N"),  # Валидный InChI Key
        ("VNWKTOKETHGBQD-UHFFFAOYSA-N", "VNWKTOKETHGBQD-UHFFFAOYSA-N"),  # Валидный InChI Key
        # Граничные случаи
        ("", None),
        (None, None),
        ("invalid", None),
        ("BSYNRYMUTXBXSQ-UHFFFAOYSA", None),  # Неполный ключ
        ("bsynrymutxbxsq-uhfffaoysa-n", "BSYNRYMUTXBXSQ-UHFFFAOYSA-N"),  # Приводится к верхнему регистру
    ])
    def test_normalize_inchi_key(self, input_val, expected):
        """Тест нормализации InChI Key."""
        assert normalize_inchi_key(input_val) == expected
    
    @pytest.mark.parametrize("input_val,expected", [
        # Позитивные тесты
        ("C2H6O", "C2H6O"),  # Этанол
        ("CH4", "CH4"),  # Метан
        ("C6H6", "C6H6"),  # Бензол
        ("H2O", "H2O"),  # Вода
        # Граничные случаи
        ("", None),
        (None, None),
        ("invalid", None),
        ("C2H6O2", "C2H6O2"),  # Сложная формула
    ])
    def test_normalize_molecular_formula(self, input_val, expected):
        """Тест нормализации молекулярной формулы."""
        assert normalize_molecular_formula(input_val) == expected
    
    def test_normalize_smiles_canonical(self):
        """Тест канонизации SMILES."""
        # Пока что это просто обертка над normalize_smiles
        assert normalize_smiles_canonical("CCO") == "CCO"
        assert normalize_smiles_canonical("") is None
        assert normalize_smiles_canonical(None) is None
