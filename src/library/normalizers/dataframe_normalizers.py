"""
Нормализаторы для DataFrame данных.

Предоставляет функции для применения нормализации к целым DataFrame.
"""

import pandas as pd
from typing import Any

from .assay_normalizers import (
    normalize_assay_class_hierarchy,
    normalize_assay_class_type,
    normalize_assay_description,
    normalize_assay_param_type,
    normalize_assay_param_units,
    normalize_bao_id,
    normalize_chembl_release,
    normalize_protein_sequence,
    normalize_relation,
    normalize_uniprot_accession,
    normalize_variant_accession_reported,
    normalize_variant_mutation,
)


def normalize_assay_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """Apply assay-specific normalization to a DataFrame.
    
    Args:
        df: DataFrame для нормализации
        
    Returns:
        Нормализованный DataFrame
    """
    if df.empty:
        return df
    
    # Normalize assay description
    if 'assay_description' in df.columns:
        df['assay_description'] = df['assay_description'].apply(
            normalize_assay_description
        )
    
    # Normalize BAO fields
    for field in ['bao_endpoint', 'bao_format', 'assay_class_bao_id']:
        if field in df.columns:
            df[field] = df[field].apply(normalize_bao_id)
    
    # Normalize assay parameters
    if 'assay_param_relation' in df.columns:
        df['assay_param_relation'] = df['assay_param_relation'].apply(
            normalize_relation
        )
    
    if 'assay_param_type' in df.columns:
        df['assay_param_type'] = df['assay_param_type'].apply(
            normalize_assay_param_type
        )
    
    if 'assay_param_units' in df.columns:
        df['assay_param_units'] = df['assay_param_units'].apply(
            normalize_assay_param_units
        )
    
    # Normalize assay class fields
    if 'assay_class_type' in df.columns:
        df['assay_class_type'] = df['assay_class_type'].apply(
            normalize_assay_class_type
        )
    
    for field in ['assay_class_l1', 'assay_class_l2', 'assay_class_l3']:
        if field in df.columns:
            df[field] = df[field].apply(normalize_assay_class_hierarchy)
    
    # Normalize variant fields
    if 'variant_base_accession' in df.columns:
        df['variant_base_accession'] = df['variant_base_accession'].apply(
            normalize_uniprot_accession
        )
    
    if 'variant_mutation' in df.columns:
        df['variant_mutation'] = df['variant_mutation'].apply(
            normalize_variant_mutation
        )
    
    if 'variant_sequence' in df.columns:
        df['variant_sequence'] = df['variant_sequence'].apply(
            normalize_protein_sequence
        )
    
    if 'variant_accession_reported' in df.columns:
        df['variant_accession_reported'] = df['variant_accession_reported'].apply(
            normalize_variant_accession_reported
        )
    
    # Normalize ChEMBL release
    if 'chembl_release' in df.columns:
        df['chembl_release'] = df['chembl_release'].apply(
            normalize_chembl_release
        )
    
    return df
