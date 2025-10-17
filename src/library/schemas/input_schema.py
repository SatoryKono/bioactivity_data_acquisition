"""Pandera schemas describing the raw API payload."""

from __future__ import annotations

import importlib.util

import pandas as pd
from pandera.typing import Series

_PANDERA_PANDAS_SPEC = importlib.util.find_spec("pandera.pandas")
if _PANDERA_PANDAS_SPEC is not None:  # pragma: no cover - import side effect
    import pandera.pandas as pa  # type: ignore[no-redef]
else:  # pragma: no cover - import side effect
    import pandera as pa


class RawBioactivitySchema(pa.DataFrameModel):
    """Schema for raw bioactivity records fetched from the API.
    
    This schema validates raw data from external APIs before normalization.
    Strict mode ensures no unexpected columns are present.
    """

    # Required fields - must be present in all records
    source: Series[str] = pa.Field(
        description="Data source identifier (e.g., 'chembl', 'crossref')",
        nullable=False
    )
    retrieved_at: Series[pd.Timestamp] = pa.Field(
        description="Timestamp when data was retrieved from API",
        nullable=False
    )
    
    # Core bioactivity fields - nullable=True because APIs may not always provide them
    target_pref_name: Series[str] = pa.Field(
        description="Preferred name of the biological target",
        nullable=True
    )
    standard_value: Series[float] = pa.Field(
        description="Bioactivity value (IC50, Ki, etc.)",
        nullable=True
    )
    standard_units: Series[str] = pa.Field(
        description="Units for the bioactivity value (nM, uM, etc.)",
        nullable=True
    )
    canonical_smiles: Series[str] = pa.Field(
        description="Canonical SMILES representation of the compound",
        nullable=True
    )
    
    # ChEMBL-specific fields - nullable because not all sources provide them
    activity_id: Series[int] = pa.Field(
        description="Unique activity identifier from ChEMBL",
        nullable=True
    )
    assay_chembl_id: Series[str] = pa.Field(
        description="ChEMBL assay identifier",
        nullable=True
    )
    document_chembl_id: Series[str] = pa.Field(
        description="ChEMBL document identifier",
        nullable=True
    )
    standard_type: Series[str] = pa.Field(
        description="Type of bioactivity measurement (IC50, Ki, etc.)",
        nullable=True
    )
    standard_relation: Series[str] = pa.Field(
        description="Relation to the standard value (=, <, >, etc.)",
        nullable=True
    )
    target_chembl_id: Series[str] = pa.Field(
        description="ChEMBL target identifier",
        nullable=True
    )
    target_organism: Series[str] = pa.Field(
        description="Organism of the biological target",
        nullable=True
    )
    target_tax_id: Series[str] = pa.Field(
        description="Taxonomic identifier of the target organism",
        nullable=True
    )

    class Config:
        strict = True  # STRICT MODE: No additional columns allowed
        coerce = True  # Allow type coercion for data cleaning


__all__ = ["RawBioactivitySchema"]
