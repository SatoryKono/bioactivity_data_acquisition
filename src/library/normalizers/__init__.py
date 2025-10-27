"""
Централизованный модуль нормализаторов данных.

Предоставляет функции нормализации для всех типов данных в системе:
- Стандартные типы: string, numeric, datetime, boolean
- Специфические типы: DOI, ChEMBL ID, SMILES, InChI, BAO, единицы измерения
"""

# Импортируем все модули для регистрации нормализаторов
from . import assay_normalizers  # noqa: F401
from . import boolean_normalizers  # noqa: F401
from . import chemistry_normalizers  # noqa: F401
from . import dataframe_normalizers  # noqa: F401
from . import datetime_normalizers  # noqa: F401
from . import identifier_normalizers  # noqa: F401
from . import json_normalizers  # noqa: F401
from . import numeric_normalizers  # noqa: F401
from . import ontology_normalizers  # noqa: F401
from . import string_normalizers  # noqa: F401
from . import units_normalizers  # noqa: F401
from .assay_normalizers import (normalize_assay_class_hierarchy,
                                normalize_assay_class_type,
                                normalize_assay_description,
                                normalize_assay_param_type,
                                normalize_assay_param_units,
                                normalize_bao_id, normalize_chembl_release,
                                normalize_protein_sequence, normalize_relation,
                                normalize_uniprot_accession,
                                normalize_variant_accession_reported,
                                normalize_variant_mutation)
from .base import NormalizationError, NormalizationPipeline, get_normalizer
from .boolean_normalizers import normalize_boolean, normalize_boolean_strict
from .chemistry_normalizers import (normalize_inchi, normalize_inchi_key,
                                    normalize_smiles)
from .datetime_normalizers import (normalize_datetime_iso8601,
                                   normalize_datetime_precision,
                                   normalize_datetime_validate)
from .identifier_normalizers import (normalize_chembl_id, normalize_doi,
                                     normalize_iuphar_id,
                                     normalize_pubchem_cid,
                                     normalize_uniprot_id)
from .json_normalizers import (normalize_day, normalize_json_keys,
                               normalize_json_structure, normalize_month,
                               normalize_pmid, normalize_year)
from .numeric_normalizers import (normalize_float, normalize_float_precision,
                                  normalize_int, normalize_int_positive,
                                  normalize_int_range)
from .ontology_normalizers import normalize_bao_id, normalize_bao_label
from .string_normalizers import (normalize_empty_to_null,
                                 normalize_string_lower, normalize_string_nfc,
                                 normalize_string_strip,
                                 normalize_string_titlecase,
                                 normalize_string_upper,
                                 normalize_string_whitespace)
from .units_normalizers import normalize_pchembl, normalize_units
from .dataframe_normalizers import normalize_assay_dataframe

__all__ = [
    # Base
    "get_normalizer",
    "NormalizationError",
    "NormalizationPipeline",
    
    # Assay normalizers
    "normalize_assay_description",
    "normalize_bao_id",
    "normalize_relation",
    "normalize_uniprot_accession",
    "normalize_protein_sequence",
    "normalize_assay_param_type",
    "normalize_assay_param_units",
    "normalize_assay_class_type",
    "normalize_assay_class_hierarchy",
    "normalize_variant_mutation",
    "normalize_variant_accession_reported",
    "normalize_chembl_release",
    
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
    "normalize_pmid",
    
    # JSON normalizers
    "normalize_json_keys",
    "normalize_json_structure",
    "normalize_year",
    "normalize_month",
    "normalize_day",
    
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
    
    # DataFrame normalizers
    "normalize_assay_dataframe",
]
