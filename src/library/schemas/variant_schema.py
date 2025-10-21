"""Pandera schemas for variant sequence data validation."""

from __future__ import annotations

import importlib.util

from pandera.typing import Series

_PANDERA_PANDAS_SPEC = importlib.util.find_spec("pandera.pandas")
if _PANDERA_PANDAS_SPEC is not None:  # pragma: no cover - import side effect
    import pandera.pandas as pa  # type: ignore[no-redef]
else:  # pragma: no cover - import side effect
    import pandera as pa


class VariantSequenceDimSchema(pa.DataFrameModel):
    """Schema for variant sequences dimension table.
    
    This schema validates the variant_sequences_dim reference table
    containing normalized variant sequence data from ChEMBL.
    """

    # Required fields
    variant_id: Series[int] = pa.Field(
        description="ChEMBL variant identifier",
        unique=True,
        nullable=False
    )
    extracted_at: Series[object] = pa.Field(
        description="Timestamp when data was retrieved from API",
        nullable=False
    )
    
    # Optional fields - nullable=True for missing data
    variant_accession: Series[str] = pa.Field(
        description="UniProt accession of variant",
        nullable=True
    )
    variant_sequence: Series[str] = pa.Field(
        description="Protein sequence of variant",
        nullable=True
    )
    variant_organism: Series[str] = pa.Field(
        description="Organism of variant",
        nullable=True
    )
    mutation: Series[str] = pa.Field(
        description="Description of mutations in variant",
        nullable=True
    )

    class Config:
        strict = True  # STRICT MODE: No additional columns allowed
        coerce = True  # Allow type coercion for data cleaning


__all__ = ["VariantSequenceDimSchema"]
