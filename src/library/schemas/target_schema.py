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

    class Config:
        strict = False  # allow extra columns from enrichments
        coerce = True


__all__ = ["TargetInputSchema", "TargetNormalizedSchema"]


