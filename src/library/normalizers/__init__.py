"""
Централизованный модуль нормализаторов данных.

Предоставляет функции нормализации для всех типов данных в системе:
- Стандартные типы: string, numeric, datetime, boolean
- Специфические типы: DOI, ChEMBL ID, SMILES, InChI, BAO, единицы измерения
"""

# Импортируем все модули для регистрации нормализаторов
from . import boolean_normalizers  # noqa: F401
from . import chemistry_normalizers  # noqa: F401
from . import datetime_normalizers  # noqa: F401
from . import identifier_normalizers  # noqa: F401
from . import numeric_normalizers  # noqa: F401
from . import ontology_normalizers  # noqa: F401
from . import string_normalizers  # noqa: F401
from . import units_normalizers  # noqa: F401
from .base import NormalizationError
from .base import get_normalizer
from .boolean_normalizers import normalize_boolean
from .boolean_normalizers import normalize_boolean_strict
from .chemistry_normalizers import normalize_inchi
from .chemistry_normalizers import normalize_inchi_key
from .chemistry_normalizers import normalize_smiles
from .datetime_normalizers import normalize_datetime_iso8601
from .datetime_normalizers import normalize_datetime_precision
from .datetime_normalizers import normalize_datetime_validate
from .identifier_normalizers import normalize_chembl_id
from .identifier_normalizers import normalize_doi
from .identifier_normalizers import normalize_iuphar_id
from .identifier_normalizers import normalize_pubchem_cid
from .identifier_normalizers import normalize_uniprot_id
from .numeric_normalizers import normalize_float
from .numeric_normalizers import normalize_float_precision
from .numeric_normalizers import normalize_int
from .numeric_normalizers import normalize_int_positive
from .numeric_normalizers import normalize_int_range
from .ontology_normalizers import normalize_bao_id
from .ontology_normalizers import normalize_bao_label
from .string_normalizers import normalize_empty_to_null
from .string_normalizers import normalize_string_lower
from .string_normalizers import normalize_string_nfc
from .string_normalizers import normalize_string_strip
from .string_normalizers import normalize_string_titlecase
from .string_normalizers import normalize_string_upper
from .string_normalizers import normalize_string_whitespace
from .units_normalizers import normalize_pchembl
from .units_normalizers import normalize_units

__all__ = [
    # Base
    "get_normalizer",
    "NormalizationError",
    
    # String normalizers
    "normalize_string_strip",
    "normalize_string_upper", 
    "normalize_string_lower",
    "normalize_string_titlecase",
    "normalize_string_nfc",
    "normalize_string_whitespace",
    "normalize_empty_to_null",
    
    # Numeric normalizers
    "normalize_int",
    "normalize_float",
    "normalize_int_positive",
    "normalize_int_range",
    "normalize_float_precision",
    
    # Datetime normalizers
    "normalize_datetime_iso8601",
    "normalize_datetime_validate",
    "normalize_datetime_precision",
    
    # Boolean normalizers
    "normalize_boolean",
    "normalize_boolean_strict",
    
    # Identifier normalizers
    "normalize_doi",
    "normalize_chembl_id",
    "normalize_uniprot_id",
    "normalize_iuphar_id",
    "normalize_pubchem_cid",
    
    # Chemistry normalizers
    "normalize_smiles",
    "normalize_inchi",
    "normalize_inchi_key",
    
    # Ontology normalizers
    "normalize_bao_id",
    "normalize_bao_label",
    
    # Units normalizers
    "normalize_units",
    "normalize_pchembl",
]
