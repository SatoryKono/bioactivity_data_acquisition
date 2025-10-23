"""
Централизованный модуль нормализаторов данных.

Предоставляет функции нормализации для всех типов данных в системе:
- Стандартные типы: string, numeric, datetime, boolean
- Специфические типы: DOI, ChEMBL ID, SMILES, InChI, BAO, единицы измерения
"""

# Импортируем все модули для регистрации нормализаторов
from . import string_normalizers
from . import numeric_normalizers
from . import datetime_normalizers
from . import boolean_normalizers
from . import identifier_normalizers
from . import chemistry_normalizers
from . import ontology_normalizers
from . import units_normalizers

from .base import get_normalizer, NormalizationError
from .string_normalizers import (
    normalize_string_strip,
    normalize_string_upper,
    normalize_string_lower,
    normalize_string_titlecase,
    normalize_string_nfc,
    normalize_string_whitespace,
    normalize_empty_to_null,
)
from .numeric_normalizers import (
    normalize_int,
    normalize_float,
    normalize_int_positive,
    normalize_int_range,
    normalize_float_precision,
)
from .datetime_normalizers import (
    normalize_datetime_iso8601,
    normalize_datetime_validate,
    normalize_datetime_precision,
)
from .boolean_normalizers import (
    normalize_boolean,
    normalize_boolean_strict,
)
from .identifier_normalizers import (
    normalize_doi,
    normalize_chembl_id,
    normalize_uniprot_id,
    normalize_iuphar_id,
    normalize_pubchem_cid,
)
from .chemistry_normalizers import (
    normalize_smiles,
    normalize_inchi,
    normalize_inchi_key,
)
from .ontology_normalizers import (
    normalize_bao_id,
    normalize_bao_label,
)
from .units_normalizers import (
    normalize_units,
    normalize_pchembl,
)

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
