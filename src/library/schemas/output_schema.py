"""Pandera schema for normalized bioactivity tables."""

from __future__ import annotations

import importlib.util

import pandas as pd
from pandera.typing import Series

_PANDERA_PANDAS_SPEC = importlib.util.find_spec("pandera.pandas")
if _PANDERA_PANDAS_SPEC is not None:  # pragma: no cover - import side effect
    import pandera.pandas as pa  # type: ignore[no-redef]
else:  # pragma: no cover - import side effect
    import pandera as pa


class NormalizedBioactivitySchema(pa.DataFrameModel):
    """Schema for normalized bioactivity data ready for export.
    
    This schema validates normalized data after ETL processing.
    All data should be clean and consistent for downstream analysis.
    """

    # Required fields - must be present in all normalized records
    source: Series[str] = pa.Field(
        description="Data source identifier (normalized)",
        nullable=False
    )
    retrieved_at: Series[pd.Timestamp] = pa.Field(
        description="Timestamp when data was retrieved from API",
        nullable=False
    )
    
    # Core bioactivity fields - nullable=True for missing data
    target: Series[str] = pa.Field(
        description="Normalized target name",
        nullable=True
    )
    activity_value: Series[float] = pa.Field(
        description="Normalized bioactivity value (converted to nM)",
        nullable=True
    )
    activity_unit: Series[str] = pa.Field(
        description="Normalized activity unit (should be nM after conversion)",
        nullable=True
    )
    smiles: Series[str] = pa.Field(
        description="Canonical SMILES representation (normalized)",
        nullable=True
    )

    class Config:
        strict = True  # STRICT MODE: No additional columns allowed
        coerce = True  # Allow type coercion for data cleaning


__all__ = ["NormalizedBioactivitySchema"]
