"""Pandera schemas describing the raw API payload."""

from __future__ import annotations

import importlib.util

import pandas as pd

_PANDERA_PANDAS_SPEC = importlib.util.find_spec("pandera.pandas")
if _PANDERA_PANDAS_SPEC is not None:  # pragma: no cover - import side effect
    import pandera.pandas as pa  # type: ignore[no-redef]
else:  # pragma: no cover - import side effect
    import pandera as pa
from pandera.typing import Series


class RawBioactivitySchema(pa.DataFrameModel):
    """Schema for raw bioactivity records fetched from the API."""

    compound_id: Series[str]
    target_pref_name: Series[str]
    activity_value: Series[float] = pa.Field(gt=0)
    activity_units: Series[str] = pa.Field(isin=["nM", "uM", "pM"])
    source: Series[str]
    retrieved_at: Series[pd.Timestamp]  # type: ignore[type-var]
    smiles: Series[str] = pa.Field(nullable=True)

    class Config:
        strict = True
        coerce = True


__all__ = ["RawBioactivitySchema"]
