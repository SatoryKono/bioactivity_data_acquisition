"""Pandera schemas for target data validation."""

from __future__ import annotations

import importlib.util

from pandera.typing import Series

_PANDERA_PANDAS_SPEC = importlib.util.find_spec("pandera.pandas")
if _PANDERA_PANDAS_SPEC is not None:  # pragma: no cover - import side effect
    import pandera.pandas as pa  # type: ignore[no-redef]
else:  # pragma: no cover - import side effect
    import pandera as pa


class TargetInputSchema(pa.DataFrameModel):
    """Schema for input target data from CSV files."""

    target_chembl_id: Series[str] = pa.Field(description="ChEMBL target identifier")

    class Config:
        strict = False  # Allow extra columns
        coerce = True


class TargetNormalizedSchema(pa.DataFrameModel):
    """Schema for normalized target data after enrichment."""

    # Business key - only required field
    target_chembl_id: Series[str] = pa.Field(nullable=False)
    
    # HGNC enrichment fields (из ChEMBL cross-references)
    hgnc_id: Series[str] = pa.Field(
        nullable=True, 
        description="HGNC identifier from ChEMBL cross-references"
    )
    hgnc_name: Series[str] = pa.Field(
        nullable=True, 
        description="HGNC gene name from ChEMBL cross-references"
    )
    
    # Gene symbol (из UniProt API)
    gene_symbol: Series[str] = pa.Field(
        nullable=True, 
        description="Primary gene symbol from UniProt"
    )
    
    # GtoPdb enrichment fields
    gtop_synonyms: Series[str] = pa.Field(nullable=True, description="Guide to Pharmacology synonyms")
    gtop_natural_ligands_n: Series[str] = pa.Field(nullable=True, description="Number of natural ligands")
    gtop_interactions_n: Series[str] = pa.Field(nullable=True, description="Number of interactions")
    gtop_function_text_short: Series[str] = pa.Field(nullable=True, description="Short function description")

    class Config:
        strict = False  # allow extra columns from enrichments
        coerce = True


__all__ = ["TargetInputSchema", "TargetNormalizedSchema"]


