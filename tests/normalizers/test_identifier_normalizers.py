"""
Тесты для нормализаторов идентификаторов.
"""

import pytest

from src.library.normalizers.identifier_normalizers import (
    normalize_chembl_id,
    normalize_doi,
    normalize_iuphar_id,
    normalize_pmid,
    normalize_pubchem_cid,
    normalize_uniprot_id,
)


class TestIdentifierNormalizers:
    """Тесты для нормализаторов идентификаторов."""
    
    @pytest.mark.parametrize("input_val,expected", [
        # DOI нормализация
        ("https://doi.org/10.1021/acs.jmedchem.0c01234", "10.1021/acs.jmedchem.0c01234"),
        ("DOI:10.1021/acs.jmedchem.0c01234", "10.1021/acs.jmedchem.0c01234"),
        ("10.1021/acs.jmedchem.0c01234", "10.1021/acs.jmedchem.0c01234"),
        ("http://dx.doi.org/10.1021/acs.jmedchem.0c01234", "10.1021/acs.jmedchem.0c01234"),
        ("https://dx.doi.org/10.1021/acs.jmedchem.0c01234", "10.1021/acs.jmedchem.0c01234"),
        ("urn:doi:10.1021/acs.jmedchem.0c01234", "10.1021/acs.jmedchem.0c01234"),
        ("info:doi/10.1021/acs.jmedchem.0c01234", "10.1021/acs.jmedchem.0c01234"),
        # Негативные тесты
        ("invalid", None),
        ("", None),
        (None, None),
        ("10.1021/invalid", None),
    ])
    def test_normalize_doi(self, input_val, expected):
        """Тест нормализации DOI."""
        assert normalize_doi(input_val) == expected
    
    @pytest.mark.parametrize("input_val,expected", [
        # ChEMBL ID нормализация
        ("chembl25", "CHEMBL25"),
        ("CHEMBL25", "CHEMBL25"),
        ("25", "CHEMBL25"),
        ("chembl123456", "CHEMBL123456"),
        # Негативные тесты
        ("invalid", None),
        ("", None),
        (None, None),
        ("chembl", None),
    ])
    def test_normalize_chembl_id(self, input_val, expected):
        """Тест нормализации ChEMBL ID."""
        assert normalize_chembl_id(input_val) == expected
    
    @pytest.mark.parametrize("input_val,expected", [
        # UniProt ID нормализация
        ("P12345", "P12345"),
        ("p12345", "P12345"),
        ("O12345", "O12345"),
        ("Q12345", "Q12345"),
        # Негативные тесты
        ("invalid", None),
        ("", None),
        (None, None),
        ("P1234", None),  # Слишком короткий
    ])
    def test_normalize_uniprot_id(self, input_val, expected):
        """Тест нормализации UniProt ID."""
        assert normalize_uniprot_id(input_val) == expected
    
    @pytest.mark.parametrize("input_val,expected", [
        # IUPHAR ID нормализация
        ("GTOPDB:1234", "1234"),
        ("1234", "1234"),
        ("IUPHAR:5678", "5678"),
        ("iuphar:9999", "9999"),
        # Негативные тесты
        ("invalid", None),
        ("", None),
        (None, None),
        ("GTOPDB:invalid", None),
    ])
    def test_normalize_iuphar_id(self, input_val, expected):
        """Тест нормализации IUPHAR ID."""
        assert normalize_iuphar_id(input_val) == expected
    
    @pytest.mark.parametrize("input_val,expected", [
        # PubChem CID нормализация
        ("CID:2244", "2244"),
        ("2244", "2244"),
        ("https://pubchem.ncbi.nlm.nih.gov/compound/2244", "2244"),
        ("http://pubchem.ncbi.nlm.nih.gov/compound/2244", "2244"),
        # Негативные тесты
        ("invalid", None),
        ("", None),
        (None, None),
        ("CID:invalid", None),
        ("0", None),  # Должно быть > 0
    ])
    def test_normalize_pubchem_cid(self, input_val, expected):
        """Тест нормализации PubChem CID."""
        assert normalize_pubchem_cid(input_val) == expected
    
    @pytest.mark.parametrize("input_val,expected", [
        # PMID нормализация
        ("12345678", "12345678"),
        ("1", "1"),
        ("999999999", "999999999"),
        # Негативные тесты
        ("invalid", None),
        ("", None),
        (None, None),
        ("0", None),  # Должно быть > 0
        ("-1", None),  # Должно быть > 0
    ])
    def test_normalize_pmid(self, input_val, expected):
        """Тест нормализации PMID."""
        assert normalize_pmid(input_val) == expected
