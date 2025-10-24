"""
Нормализация данных для assays.

Предоставляет специализированные методы нормализации для полей assays,
включая ASSAY_PARAMETERS, ASSAY_CLASS, VARIANT_SEQUENCES.
"""

import re

import pandas as pd

from library.normalize.base import BaseNormalizer

logger = __name__


class AssayNormalizer(BaseNormalizer):
    """Normalizer for assay-specific fields."""
    
    @staticmethod
    def normalize_assay_description(value: str | None) -> str | None:
        """Strip HTML, trim, max 4000 chars."""
        if not value:
            return None
        # Strip HTML tags
        value = re.sub(r'<[^>]+>', '', str(value))
        value = value.strip()
        return value[:4000] if len(value) > 4000 else value
    
    @staticmethod
    def normalize_bao_id(value: str | None) -> str | None:
        """Uppercase, validate BAO_\\d{7} format."""
        if not value:
            return None
        value = str(value).strip().upper()
        if re.match(r'^BAO_\d{7}$', value):
            return value
        return None
    
    @staticmethod
    def normalize_relation(value: str | None) -> str | None:
        """Normalize relation to standard set."""
        if not value:
            return None
        relation_map = {
            "=": "=", "eq": "=",
            ">": ">", "gt": ">",
            ">=": ">=", "gte": ">=",
            "<": "<", "lt": "<",
            "<=": "<=", "lte": "<=",
            "~": "~", "approx": "~"
        }
        normalized = relation_map.get(str(value).strip().lower())
        return normalized
    
    @staticmethod
    def normalize_uniprot_accession(value: str | None) -> str | None:
        """Validate UniProt accession format."""
        if not value:
            return None
        value = str(value).strip().upper()
        # UniProt regex: [OPQ][0-9][A-Z0-9]{3}[0-9](-\d+)?
        if re.match(r'^[OPQ][0-9][A-Z0-9]{3}[0-9](-\d+)?$', value):
            return value
        return None
    
    @staticmethod
    def normalize_protein_sequence(value: str | None) -> str | None:
        """Validate protein sequence (A-Z and *)."""
        if not value:
            return None
        value = str(value).strip().upper()
        if re.match(r'^[A-Z\*]+$', value):
            return value
        return None
    
    @staticmethod
    def normalize_assay_param_type(value: str | None) -> str | None:
        """Normalize assay parameter type."""
        if not value:
            return None
        value = str(value).strip().upper()
        # Common assay parameter types
        known_types = {
            "PH", "TEMPERATURE", "TIME", "CONCENTRATION", "VOLUME", 
            "PRESSURE", "RATIO", "PERCENTAGE", "MOLARITY", "NORMALITY"
        }
        if value in known_types:
            return value
        return value  # Return as-is if not in known types
    
    @staticmethod
    def normalize_assay_param_units(value: str | None) -> str | None:
        """Normalize assay parameter units to lowercase."""
        if not value:
            return None
        return str(value).strip().lower()
    
    @staticmethod
    def normalize_assay_class_type(value: str | None) -> str | None:
        """Normalize assay class type to lowercase."""
        if not value:
            return None
        return str(value).strip().lower()
    
    @staticmethod
    def normalize_assay_class_hierarchy(value: str | None) -> str | None:
        """Normalize assay class hierarchy levels (l1, l2, l3) to lowercase."""
        if not value:
            return None
        return str(value).strip().lower()
    
    @staticmethod
    def normalize_variant_mutation(value: str | None) -> str | None:
        """Normalize variant mutation to uppercase."""
        if not value:
            return None
        return str(value).strip().upper()
    
    @staticmethod
    def normalize_variant_accession_reported(value: str | None) -> str | None:
        """Normalize variant accession reported to uppercase."""
        if not value:
            return None
        return str(value).strip().upper()
    
    @staticmethod
    def normalize_chembl_release(value: str | None) -> str | None:
        """Normalize ChEMBL release to uppercase and validate format."""
        if not value:
            return None
        value = str(value).strip().upper()
        # Validate CHEMBL_XX format
        if re.match(r'^CHEMBL_\d+$', value):
            return value
        return None


def normalize_assay_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """Apply assay-specific normalization to a DataFrame."""
    if df.empty:
        return df
    
    normalizer = AssayNormalizer()
    
    # Normalize assay description
    if 'assay_description' in df.columns:
        df['assay_description'] = df['assay_description'].apply(
            normalizer.normalize_assay_description
        )
    
    # Normalize BAO fields
    for field in ['bao_endpoint', 'bao_format', 'assay_class_bao_id']:
        if field in df.columns:
            df[field] = df[field].apply(normalizer.normalize_bao_id)
    
    # Normalize assay parameters
    if 'assay_param_relation' in df.columns:
        df['assay_param_relation'] = df['assay_param_relation'].apply(
            normalizer.normalize_relation
        )
    
    if 'assay_param_type' in df.columns:
        df['assay_param_type'] = df['assay_param_type'].apply(
            normalizer.normalize_assay_param_type
        )
    
    if 'assay_param_units' in df.columns:
        df['assay_param_units'] = df['assay_param_units'].apply(
            normalizer.normalize_assay_param_units
        )
    
    # Normalize assay class fields
    if 'assay_class_type' in df.columns:
        df['assay_class_type'] = df['assay_class_type'].apply(
            normalizer.normalize_assay_class_type
        )
    
    for field in ['assay_class_l1', 'assay_class_l2', 'assay_class_l3']:
        if field in df.columns:
            df[field] = df[field].apply(normalizer.normalize_assay_class_hierarchy)
    
    # Normalize variant fields
    if 'variant_base_accession' in df.columns:
        df['variant_base_accession'] = df['variant_base_accession'].apply(
            normalizer.normalize_uniprot_accession
        )
    
    if 'variant_mutation' in df.columns:
        df['variant_mutation'] = df['variant_mutation'].apply(
            normalizer.normalize_variant_mutation
        )
    
    if 'variant_sequence' in df.columns:
        df['variant_sequence'] = df['variant_sequence'].apply(
            normalizer.normalize_protein_sequence
        )
    
    if 'variant_accession_reported' in df.columns:
        df['variant_accession_reported'] = df['variant_accession_reported'].apply(
            normalizer.normalize_variant_accession_reported
        )
    
    # Normalize ChEMBL release
    if 'chembl_release' in df.columns:
        df['chembl_release'] = df['chembl_release'].apply(
            normalizer.normalize_chembl_release
        )
    
    return df
