"""
Нормализаторы для данных assays.

Предоставляет специализированные методы нормализации для полей assays,
включая ASSAY_PARAMETERS, ASSAY_CLASS, VARIANT_SEQUENCES.
"""

import re
from typing import Any

from .base import ensure_string, is_empty_value, register_normalizer, safe_normalize


@safe_normalize
def normalize_assay_description(value: Any) -> str | None:
    """Strip HTML, trim, max 4000 chars.
    
    Args:
        value: Значение для нормализации
        
    Returns:
        Нормализованное описание assay или None
    """
    if is_empty_value(value):
        return None
    
    text = ensure_string(value)
    if text is None:
        return None
    
    # Strip HTML tags
    text = re.sub(r'<[^>]+>', '', text)
    text = text.strip()
    return text[:4000] if len(text) > 4000 else text


@safe_normalize
def normalize_bao_id(value: Any) -> str | None:
    """Uppercase, validate BAO_\\d{7} format.
    
    Args:
        value: Значение для нормализации
        
    Returns:
        Нормализованный BAO ID или None
    """
    if is_empty_value(value):
        return None
    
    text = ensure_string(value)
    if text is None:
        return None
    
    text = text.strip().upper()
    if re.match(r'^BAO_\d{7}$', text):
        return text
    return None


@safe_normalize
def normalize_relation(value: Any) -> str | None:
    """Normalize relation to standard set.
    
    Args:
        value: Значение для нормализации
        
    Returns:
        Нормализованное отношение или None
    """
    if is_empty_value(value):
        return None
    
    text = ensure_string(value)
    if text is None:
        return None
    
    relation_map = {
        "=": "=", "eq": "=",
        ">": ">", "gt": ">",
        ">=": ">=", "gte": ">=",
        "<": "<", "lt": "<",
        "<=": "<=", "lte": "<=",
        "~": "~", "approx": "~"
    }
    normalized = relation_map.get(text.strip().lower())
    return normalized


@safe_normalize
def normalize_uniprot_accession(value: Any) -> str | None:
    """Validate UniProt accession format.
    
    Args:
        value: Значение для нормализации
        
    Returns:
        Нормализованный UniProt accession или None
    """
    if is_empty_value(value):
        return None
    
    text = ensure_string(value)
    if text is None:
        return None
    
    text = text.strip().upper()
    # UniProt regex: [OPQ][0-9][A-Z0-9]{3}[0-9](-\d+)?
    if re.match(r'^[OPQ][0-9][A-Z0-9]{3}[0-9](-\d+)?$', text):
        return text
    return None


@safe_normalize
def normalize_protein_sequence(value: Any) -> str | None:
    """Validate protein sequence (A-Z and *).
    
    Args:
        value: Значение для нормализации
        
    Returns:
        Нормализованная белковая последовательность или None
    """
    if is_empty_value(value):
        return None
    
    text = ensure_string(value)
    if text is None:
        return None
    
    text = text.strip().upper()
    if re.match(r'^[A-Z\*]+$', text):
        return text
    return None


@safe_normalize
def normalize_assay_param_type(value: Any) -> str | None:
    """Normalize assay parameter type.
    
    Args:
        value: Значение для нормализации
        
    Returns:
        Нормализованный тип параметра assay или None
    """
    if is_empty_value(value):
        return None
    
    text = ensure_string(value)
    if text is None:
        return None
    
    text = text.strip().upper()
    # Common assay parameter types
    known_types = {
        "PH", "TEMPERATURE", "TIME", "CONCENTRATION", "VOLUME", 
        "PRESSURE", "RATIO", "PERCENTAGE", "MOLARITY", "NORMALITY"
    }
    if text in known_types:
        return text
    return text  # Return as-is if not in known types


@safe_normalize
def normalize_assay_param_units(value: Any) -> str | None:
    """Normalize assay parameter units to lowercase.
    
    Args:
        value: Значение для нормализации
        
    Returns:
        Нормализованные единицы измерения или None
    """
    if is_empty_value(value):
        return None
    
    text = ensure_string(value)
    if text is None:
        return None
    
    return text.strip().lower()


@safe_normalize
def normalize_assay_class_type(value: Any) -> str | None:
    """Normalize assay class type to lowercase.
    
    Args:
        value: Значение для нормализации
        
    Returns:
        Нормализованный тип класса assay или None
    """
    if is_empty_value(value):
        return None
    
    text = ensure_string(value)
    if text is None:
        return None
    
    return text.strip().lower()


@safe_normalize
def normalize_assay_class_hierarchy(value: Any) -> str | None:
    """Normalize assay class hierarchy levels (l1, l2, l3) to lowercase.
    
    Args:
        value: Значение для нормализации
        
    Returns:
        Нормализованный уровень иерархии или None
    """
    if is_empty_value(value):
        return None
    
    text = ensure_string(value)
    if text is None:
        return None
    
    return text.strip().lower()


@safe_normalize
def normalize_variant_mutation(value: Any) -> str | None:
    """Normalize variant mutation to uppercase.
    
    Args:
        value: Значение для нормализации
        
    Returns:
        Нормализованная мутация или None
    """
    if is_empty_value(value):
        return None
    
    text = ensure_string(value)
    if text is None:
        return None
    
    return text.strip().upper()


@safe_normalize
def normalize_variant_accession_reported(value: Any) -> str | None:
    """Normalize variant accession reported to uppercase.
    
    Args:
        value: Значение для нормализации
        
    Returns:
        Нормализованный accession или None
    """
    if is_empty_value(value):
        return None
    
    text = ensure_string(value)
    if text is None:
        return None
    
    return text.strip().upper()


@safe_normalize
def normalize_chembl_release(value: Any) -> str | None:
    """Normalize ChEMBL release to uppercase and validate format.
    
    Args:
        value: Значение для нормализации
        
    Returns:
        Нормализованный релиз ChEMBL или None
    """
    if is_empty_value(value):
        return None
    
    text = ensure_string(value)
    if text is None:
        return None
    
    text = text.strip().upper()
    # Validate CHEMBL_XX format
    if re.match(r'^CHEMBL_\d+$', text):
        return text
    return None


# Регистрация всех нормализаторов
register_normalizer("normalize_assay_description", normalize_assay_description)
register_normalizer("normalize_bao_id", normalize_bao_id)
register_normalizer("normalize_relation", normalize_relation)
register_normalizer("normalize_uniprot_accession", normalize_uniprot_accession)
register_normalizer("normalize_protein_sequence", normalize_protein_sequence)
register_normalizer("normalize_assay_param_type", normalize_assay_param_type)
register_normalizer("normalize_assay_param_units", normalize_assay_param_units)
register_normalizer("normalize_assay_class_type", normalize_assay_class_type)
register_normalizer("normalize_assay_class_hierarchy", normalize_assay_class_hierarchy)
register_normalizer("normalize_variant_mutation", normalize_variant_mutation)
register_normalizer("normalize_variant_accession_reported", normalize_variant_accession_reported)
register_normalizer("normalize_chembl_release", normalize_chembl_release)
