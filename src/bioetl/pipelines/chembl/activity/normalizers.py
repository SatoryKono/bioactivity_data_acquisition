from __future__ import annotations

"""Normalization logic for ChEMBL activity records."""

import pandas as pd

from bioetl.schemas.chembl_activity_schema import (
    ChEMBLActivityColumns,
    ChEMBLActivitySchema,
)
from bioetl.clients.chembl import (
    BaseChemblNormalizer,
    ColumnNormalizationSpec,
)


_ACTIVITY_NORMALIZER = BaseChemblNormalizer(
    business_key_column="activity_id",
    schema=ChEMBLActivitySchema,
    columns=ChEMBLActivityColumns,
    column_specs=[
        # Core activity fields
        ColumnNormalizationSpec("activity_id", dtype="string"),
        ColumnNormalizationSpec("row_subtype", dtype="string", default=pd.NA),
        ColumnNormalizationSpec("row_index", dtype="int", default=pd.NA),
        
        # Assay information
        ColumnNormalizationSpec("assay_chembl_id", dtype="string"),
        ColumnNormalizationSpec("assay_type", dtype="string", default=pd.NA),
        ColumnNormalizationSpec("assay_description", dtype="string", default=pd.NA),
        ColumnNormalizationSpec("assay_organism", dtype="string", default=pd.NA),
        ColumnNormalizationSpec("assay_tax_id", dtype="string", default=pd.NA),
        
        # Test item/molecule information
        ColumnNormalizationSpec("testitem_chembl_id", dtype="string", default=pd.NA),
        ColumnNormalizationSpec("molecule_chembl_id", dtype="string", default=pd.NA),
        ColumnNormalizationSpec("parent_molecule_chembl_id", dtype="string", default=pd.NA),
        ColumnNormalizationSpec("molecule_pref_name", dtype="string", default=pd.NA),
        
        # Target information
        ColumnNormalizationSpec("target_chembl_id", dtype="string", default=pd.NA),
        ColumnNormalizationSpec("target_pref_name", dtype="string", default=pd.NA),
        ColumnNormalizationSpec("target_organism", dtype="string", default=pd.NA),
        ColumnNormalizationSpec("target_tax_id", dtype="string", default=pd.NA),
        
        # Document and source information
        ColumnNormalizationSpec("document_chembl_id", dtype="string", default=pd.NA),
        ColumnNormalizationSpec("record_id", dtype="string", default=pd.NA),
        ColumnNormalizationSpec("src_id", dtype="string", default=pd.NA),
        
        # Activity measurements
        ColumnNormalizationSpec("type", dtype="string", default=pd.NA),
        ColumnNormalizationSpec("relation", dtype="string", default=pd.NA),
        ColumnNormalizationSpec(
            "value", dtype="float", transformer=lambda s: pd.to_numeric(s, errors="coerce")
        ),
        ColumnNormalizationSpec("units", dtype="string", default=pd.NA),
        ColumnNormalizationSpec("standard_type", dtype="string", default=pd.NA),
        ColumnNormalizationSpec("standard_relation", dtype="string", default=pd.NA),
        ColumnNormalizationSpec(
            "standard_value", dtype="float", transformer=lambda s: pd.to_numeric(s, errors="coerce")
        ),
        ColumnNormalizationSpec("standard_units", dtype="string", default=pd.NA),
        ColumnNormalizationSpec("standard_flag", dtype="bool", default=pd.NA),
        ColumnNormalizationSpec(
            "pchembl_value", dtype="float", transformer=lambda s: pd.to_numeric(s, errors="coerce")
        ),
        ColumnNormalizationSpec("uo_units", dtype="string", default=pd.NA),
        ColumnNormalizationSpec("qudt_units", dtype="string", default=pd.NA),
        
        # Comments and annotations
        ColumnNormalizationSpec("activity_comment", dtype="string", default=pd.NA),
        ColumnNormalizationSpec("bao_endpoint", dtype="string", default=pd.NA),
        ColumnNormalizationSpec("bao_format", dtype="string", default=pd.NA),
        ColumnNormalizationSpec("bao_label", dtype="string", default=pd.NA),
        
        # Compound information
        ColumnNormalizationSpec("canonical_smiles", dtype="string", default=pd.NA),
        ColumnNormalizationSpec("ligand_efficiency", dtype="string", default=pd.NA),
        ColumnNormalizationSpec("compound_key", dtype="string", default=pd.NA),
        ColumnNormalizationSpec("compound_name", dtype="string", default=pd.NA),
        
        # Data validity and curation
        ColumnNormalizationSpec("data_validity_comment", dtype="string", default=pd.NA),
        ColumnNormalizationSpec("data_validity_description", dtype="string", default=pd.NA),
        ColumnNormalizationSpec("potential_duplicate", dtype="bool", default=pd.NA),
        ColumnNormalizationSpec("curated", dtype="bool", default=pd.NA),
        ColumnNormalizationSpec("removed", dtype="bool", default=pd.NA),
        
        # Activity properties (JSON string)
        ColumnNormalizationSpec("activity_properties", dtype="string", default=pd.NA),
    ],
)


class ActivityNormalizer:
    """Apply domain normalization and schema alignment for activity rows."""

    def normalize(self, df_raw: pd.DataFrame) -> pd.DataFrame:
        return _ACTIVITY_NORMALIZER.normalize(df_raw)


__all__ = ["ActivityNormalizer"]
