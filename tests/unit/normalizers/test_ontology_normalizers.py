"""
Тесты для нормализаторов онтологических данных.
"""

import pytest

from src.library.normalizers.ontology_normalizers import normalize_bao_id
from src.library.normalizers.ontology_normalizers import normalize_bao_label
from src.library.normalizers.ontology_normalizers import normalize_ec_number
from src.library.normalizers.ontology_normalizers import normalize_go_id
from src.library.normalizers.ontology_normalizers import normalize_hgnc_id


class TestOntologyNormalizers:
    """Тесты для нормализаторов онтологических данных."""
    
    @pytest.mark.parametrize("input_val,expected", [
        # Позитивные тесты
        ("BAO_0000357", "BAO_0000357"),
        ("bao:0000357", "BAO_0000357"),
        ("BAO0000357", "BAO_0000357"),
        ("0000357", "BAO_0000357"),
        
        # Граничные случаи
        ("", None),
        (None, None),
        ("invalid", None),
        ("BAO_invalid", None),
        ("BAO_", None),
    ])
    def test_normalize_bao_id(self, input_val, expected):
        """Тест нормализации BAO ID."""
        assert normalize_bao_id(input_val) == expected
    
    @pytest.mark.parametrize("input_val,expected", [
        # Позитивные тесты
        ("binding assay", "Binding Assay"),
        ("cell-based assay", "Cell-Based Assay"),
        ("enzyme assay", "Enzyme Assay"),
        ("test", "Test"),
        
        # Граничные случаи
        ("", ""),
        (None, None),
    ])
    def test_normalize_bao_label(self, input_val, expected):
        """Тест нормализации BAO label."""
        assert normalize_bao_label(input_val) == expected
    
    @pytest.mark.parametrize("input_val,expected", [
        # Позитивные тесты
        ("GO:0000001", "GO:0000001"),
        ("GO0000001", "GO:0000001"),
        ("0000001", "GO:0000001"),
        
        # Граничные случаи
        ("", None),
        (None, None),
        ("invalid", None),
        ("GO:123456", None),  # Слишком короткий
        ("GO:123456789", None),  # Слишком длинный
    ])
    def test_normalize_go_id(self, input_val, expected):
        """Тест нормализации GO ID."""
        assert normalize_go_id(input_val) == expected
    
    @pytest.mark.parametrize("input_val,expected", [
        # Позитивные тесты
        ("1.1.1.1", "1.1.1.1"),
        ("EC 1.1.1.1", "1.1.1.1"),
        ("EC:1.1.1.1", "1.1.1.1"),
        ("EC1.1.1.1", "1.1.1.1"),
        ("1.1.1.-", "1.1.1.-"),
        ("1.1.-.-", "1.1.-.-"),
        ("1.-.-.-", "1.-.-.-"),
        
        # Граничные случаи
        ("", None),
        (None, None),
        ("invalid", None),
        ("1.1.1", None),  # Неполный номер
        ("1.1.1.1.1", None),  # Слишком длинный
    ])
    def test_normalize_ec_number(self, input_val, expected):
        """Тест нормализации EC номеров."""
        assert normalize_ec_number(input_val) == expected
    
    @pytest.mark.parametrize("input_val,expected", [
        # Позитивные тесты
        ("HGNC:1234", "HGNC:1234"),
        ("HGNC1234", "HGNC:1234"),
        ("1234", "HGNC:1234"),
        
        # Граничные случаи
        ("", None),
        (None, None),
        ("invalid", None),
        ("HGNC:", None),  # Пустой номер
    ])
    def test_normalize_hgnc_id(self, input_val, expected):
        """Тест нормализации HGNC ID."""
        assert normalize_hgnc_id(input_val) == expected
