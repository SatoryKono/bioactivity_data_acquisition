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
    """Schema for normalized bioactivity data ready for export."""

    compound_id: Series[str]
    target: Series[str]
    activity_value: Series[float] = pa.Field(gt=0)
    activity_unit: Series[str] = pa.Field(isin=["nM"])
    source: Series[str]
    retrieved_at: Series[pd.Timestamp]  # type: ignore[type-var]
    smiles: Series[str] = pa.Field(nullable=True)

    class Config:
        strict = True
        coerce = True


__all__ = ["NormalizedBioactivitySchema"]
